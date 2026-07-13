from __future__ import annotations

import contextlib
import socket
import ssl
import threading
import time
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import replace
from functools import partial
from typing import Any, cast
from urllib.parse import unquote, urlparse

from ferricstore.batch_core import (
    require_batch_items,
)
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    DeferredCallbackFuture,
    RetryableResourceSet,
    close_resources_sync,
    raise_primary_with_cleanup,
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.protocol_commands import (
    _compact_flow_many_payloads_from_raw,
    _compact_pipeline_payload_from_raw,
    build_protocol_command,
)
from ferricstore.protocol_common import (
    _command_name,
    _encode_request_body,
    _flow_wake_payload,
    _normalize_protocol_url_kwargs,
    _notify_event_listeners,
    _protocol_collection_limit,
    _protocol_lane_count,
    _request_body_byte_limit,
    _response_identity_map,
    _response_item_count_map,
    _set_wire_future_sources,
    _sync_adapter_deadline,
    _timeout_with_deadline,
    _validate_pending_response_identity,
    _validated_route_lane,
)
from ferricstore.protocol_constants import (
    _DEFAULT_MAX_DECODED_COLLECTION_ITEMS,
    _DEFAULT_MAX_DECOMPRESSED_RESPONSE_BYTES,
    _DEFAULT_MAX_EVENT_QUEUE_SIZE,
    _DEFAULT_MAX_RESPONSE_BYTES,
    _DEFAULT_MAX_RESPONSE_CHUNKS,
    _DEFAULT_PROTOCOL_LANES,
    _FLAG_COMPRESSED,
    _FLAG_CUSTOM_PAYLOAD,
    _FLAG_MORE_CHUNKS,
    _FLAG_TRACE,
    _HEADER,
    _MAGIC,
    _OP_AUTH,
    _OP_FLOW_VALUE_MGET,
    _OP_MGET,
    _OP_PIPELINE,
    _OP_STARTUP,
    _OP_SUBSCRIBE_EVENTS,
    _OPCODES,
    _REQUEST_VERSION,
    _RESPONSE_VERSION,
    _SUPPORTED_SCHEMES,
    _TLS_SCHEMES,
    _USE_ADAPTER_TIMEOUT,
    ProtocolCommand,
    ProtocolResponse,
)
from ferricstore.protocol_framing import (
    ResponseBodyAccumulator,
    ResponseFrameBudget,
    ResponseIdentity,
    frame_parts,
    validate_response_identity,
    validated_optional_nonnegative_int,
    validated_response_chunk_limit,
)
from ferricstore.protocol_framing import (
    send_frames as _send_frames,
)
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_INFLIGHT_REQUESTS,
    DEFAULT_MAX_PENDING_REQUEST_BYTES,
    PendingRequestBudget,
    PendingRequestCapacityError,
    SyncDeadlineScheduler,
    validated_pending_limit,
)
from ferricstore.protocol_pipeline_codec import (
    _blocks_forever,
    _compact_kv_keys_payload,
    _compact_kv_set_keys_value_payload,
    _compact_kv_set_pairs_payload,
    _compact_pipeline_payload,
    _expected_command_collection_items,
    _expected_payload_collection_items,
    _pipeline_frame_supported,
)
from ferricstore.protocol_pipelines import ProtocolPipeline
from ferricstore.protocol_responses import (
    _batch_item_value,
    _decode_protocol_response,
    _flow_many_group_values,
    _ok_scalar,
    _pipeline_pair_list,
    _response_value,
)


class ProtocolAdapter:
    """FerricStore protocol TCP adapter for the sync SDK.

    The adapter accepts the small `execute_command(*args)` SDK executor shape.
    It encodes supported FerricStore and FerricFlow commands into native protocol
    typed frames.
    """

    client: ProtocolAdapter
    supports_concurrent_fanout = True

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6388,
        *,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        timeout: float | None = 30.0,
        client_name: str | None = "ferricstore-python",
        compression: str = "none",
        lanes: int = _DEFAULT_PROTOCOL_LANES,
        ssl_context: ssl.SSLContext | None = None,
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float | None = 30.0,
        max_response_bytes: int | None = _DEFAULT_MAX_RESPONSE_BYTES,
        max_response_chunks: int | None = _DEFAULT_MAX_RESPONSE_CHUNKS,
        max_decompressed_response_bytes: int | None = _DEFAULT_MAX_DECOMPRESSED_RESPONSE_BYTES,
        max_event_queue_size: int | None = _DEFAULT_MAX_EVENT_QUEUE_SIZE,
        max_decoded_collection_items: int | None = _DEFAULT_MAX_DECODED_COLLECTION_ITEMS,
        max_inflight_requests: int | None = DEFAULT_MAX_INFLIGHT_REQUESTS,
        max_pending_request_bytes: int | None = DEFAULT_MAX_PENDING_REQUEST_BYTES,
        _fixed_lane_id: int | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username or "default"
        self.password = password
        self.tls = tls
        self.timeout = timeout
        self.client_name = client_name
        self.compression = compression
        self.lanes = _protocol_lane_count(lanes)
        self.ssl_context = ssl_context
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.max_response_bytes = validated_optional_nonnegative_int(
            max_response_bytes,
            name="max_response_bytes",
        )
        self.max_response_chunks = validated_response_chunk_limit(max_response_chunks)
        self.max_decompressed_response_bytes = validated_optional_nonnegative_int(
            max_decompressed_response_bytes,
            name="max_decompressed_response_bytes",
        )
        self.max_event_queue_size = validated_optional_nonnegative_int(
            max_event_queue_size,
            name="max_event_queue_size",
        )
        self.max_decoded_collection_items = _protocol_collection_limit(max_decoded_collection_items)
        self.max_inflight_requests = validated_pending_limit(
            max_inflight_requests,
            name="max_inflight_requests",
        )
        self.max_pending_request_bytes = validated_pending_limit(
            max_pending_request_bytes,
            name="max_pending_request_bytes",
        )
        self._fixed_lane_id = _validated_route_lane(_fixed_lane_id)
        self.client = self
        self._lock = threading.Lock()
        self._connect_lock = threading.Lock()
        self._transport_state_lock = threading.Lock()
        self._deadline_context = threading.local()
        self._request_id = 0
        self._lane_cursor = 0
        self._sock: socket.socket | ssl.SSLSocket | None = None
        self._reader_thread: threading.Thread | None = None
        self._pending_lock = threading.Lock()
        self._pending: dict[int, Future[ProtocolResponse]] = {}
        self._pending_budget = PendingRequestBudget(
            max_requests=self.max_inflight_requests,
            max_bytes=self.max_pending_request_bytes,
        )
        self._deadline_scheduler = SyncDeadlineScheduler(
            self._expire_pending_request,
            thread_name="ferricstore-protocol-deadlines",
        )
        self._pending_traces: dict[int, dict[str, Any]] = {}
        self._pending_response_item_counts: dict[int, int] = {}
        self._pending_response_identities: dict[int, ResponseIdentity] = {}
        self._pending_transport_bindings: dict[
            int, tuple[int, socket.socket | ssl.SSLSocket | None]
        ] = {}
        self._transport_generation = 0
        self._events: deque[Any] = deque()
        self._events_cv = threading.Condition()
        self._event_error: BaseException | None = None
        self._event_listeners: list[Callable[[], None]] = []
        self._idle_listeners: list[Callable[[], None]] = []
        self._subscription_lock = threading.Lock()
        self._flow_wake_subscriptions: list[dict[str, Any]] = []
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_stop: threading.Event | None = None
        self._heartbeat_pause_count = 0
        self._last_activity = time.monotonic()
        self._closed = False
        self._connecting = False
        self._retired_sockets = RetryableResourceSet(())
        self._transport_close_lock = threading.Lock()
        self._connect()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> ProtocolAdapter:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        tls = scheme in _TLS_SCHEMES
        if scheme not in _SUPPORTED_SCHEMES:
            raise ValueError(f"unsupported FerricStore URL scheme: {parsed.scheme}")

        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (6389 if tls else 6388)
        username = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None
        _normalize_protocol_url_kwargs(kwargs)
        kwargs.setdefault("username", username)
        kwargs.setdefault("password", password)
        kwargs.setdefault("tls", tls)
        return cls(host, port, **kwargs)

    @property
    def events(self) -> list[Any]:
        with self._events_cv:
            return list(self._events)

    @property
    def backpressure_scope(self) -> tuple[str, bool, str, int, str]:
        return ("protocol", self.tls, self.host.lower(), self.port, self.username)

    @property
    def pending_request_count(self) -> int:
        with self._pending_state_lock():
            return self._pending_request_budget().count

    @property
    def pending_request_bytes(self) -> int:
        with self._pending_state_lock():
            return self._pending_request_budget().total_bytes

    def add_event_listener(self, listener: Callable[[], None]) -> None:
        with self._events_cv:
            if listener not in self._event_listeners:
                self._event_listeners.append(listener)

    def remove_event_listener(self, listener: Callable[[], None]) -> None:
        with self._events_cv, contextlib.suppress(ValueError):
            self._event_listeners.remove(listener)

    def add_idle_listener(self, listener: Callable[[], None]) -> None:
        with self._events_cv:
            if listener not in self._idle_listeners:
                self._idle_listeners.append(listener)

    def remove_idle_listener(self, listener: Callable[[], None]) -> None:
        with self._events_cv, contextlib.suppress(ValueError):
            self._idle_listeners.remove(listener)

    def _enqueue_event(self, value: Any) -> None:
        listeners: list[Callable[[], None]]
        error: FerricStoreError | None = None
        with self._events_cv:
            limit = self.max_event_queue_size
            if limit is not None and len(self._events) >= limit:
                error = FerricStoreError("protocol event queue exceeds max_event_queue_size")
                self._event_error = error
            else:
                self._events.append(value)
            listeners = list(self._event_listeners)
            self._events_cv.notify_all()
        _notify_event_listeners(listeners)
        if error is not None:
            raise error

    def wait_event(self, timeout: float | None = None) -> Any | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._events_cv:
            while not self._events and self._event_error is None:
                if timeout == 0.0:
                    return None
                wait_for: float | None = None
                if deadline is not None:
                    wait_for = deadline - time.monotonic()
                    if wait_for <= 0:
                        return None
                self._events_cv.wait(wait_for)
            if self._events:
                return self._events.popleft()
            if self._event_error is not None:
                raise self._event_error
            return None

    def pause_heartbeat(self) -> None:
        self._heartbeat_pause_count += 1

    def resume_heartbeat(self) -> None:
        self._heartbeat_pause_count = max(self._heartbeat_pause_count - 1, 0)

    def close(self) -> None:
        try:
            self._close_transport(mark_closed=True)
        finally:
            scheduler = getattr(self, "_deadline_scheduler", None)
            if scheduler is not None:
                scheduler.close()

    def invalidate(self) -> None:
        """Drop connection-local state while keeping this adapter reusable."""
        self._close_transport(
            FerricStoreError("protocol connection was invalidated"),
            mark_closed=False,
        )

    def acquire_session(self) -> ProtocolAdapter:
        """Open a dedicated connection for connection-affine commands."""
        return self._new_session_adapter(fixed_lane_id=None)

    def acquire_session_on_lane(self, lane_id: int) -> ProtocolAdapter:
        """Open a dedicated connection pinned to one topology route lane."""
        return self._new_session_adapter(fixed_lane_id=lane_id)

    def _new_session_adapter(self, *, fixed_lane_id: int | None) -> ProtocolAdapter:
        return ProtocolAdapter(
            self.host,
            self.port,
            username=self.username,
            password=self.password,
            tls=self.tls,
            timeout=self.timeout,
            client_name=self.client_name,
            compression=self.compression,
            lanes=self.lanes,
            ssl_context=self.ssl_context,
            heartbeat_interval=self.heartbeat_interval,
            heartbeat_timeout=self.heartbeat_timeout,
            max_response_bytes=self.max_response_bytes,
            max_response_chunks=self.max_response_chunks,
            max_decompressed_response_bytes=self.max_decompressed_response_bytes,
            max_event_queue_size=self.max_event_queue_size,
            max_decoded_collection_items=self.max_decoded_collection_items,
            max_inflight_requests=self.max_inflight_requests,
            max_pending_request_bytes=self.max_pending_request_bytes,
            _fixed_lane_id=fixed_lane_id,
        )

    def _pending_state_lock(self) -> threading.Lock:
        lock = getattr(self, "_pending_lock", None)
        if lock is None:
            lock = threading.Lock()
            self._pending_lock = lock
        return lock

    def _pending_request_budget(self) -> PendingRequestBudget:
        budget = getattr(self, "_pending_budget", None)
        if budget is None:
            budget = PendingRequestBudget(
                max_requests=getattr(
                    self,
                    "max_inflight_requests",
                    DEFAULT_MAX_INFLIGHT_REQUESTS,
                ),
                max_bytes=getattr(
                    self,
                    "max_pending_request_bytes",
                    DEFAULT_MAX_PENDING_REQUEST_BYTES,
                ),
            )
            self._pending_budget = budget
        return cast(PendingRequestBudget, budget)

    def _pending_deadline_scheduler(self) -> SyncDeadlineScheduler:
        scheduler = getattr(self, "_deadline_scheduler", None)
        if scheduler is None:
            scheduler = SyncDeadlineScheduler(
                self._expire_pending_request,
                thread_name="ferricstore-protocol-deadlines",
            )
            self._deadline_scheduler = scheduler
        return cast(SyncDeadlineScheduler, scheduler)

    def _release_pending_lifecycle_locked(self, request_id: int) -> None:
        self._pending_request_budget().release(request_id)
        scheduler = getattr(self, "_deadline_scheduler", None)
        if scheduler is not None:
            scheduler.cancel(request_id)

    def _set_pending_request_size(self, request_id: int, size: int) -> None:
        with self._pending_state_lock():
            try:
                self._pending_request_budget().set_size(request_id, size)
            except PendingRequestCapacityError as exc:
                raise FerricStoreError(str(exc)) from exc
            except KeyError as exc:
                raise FerricStoreError("protocol request timed out") from exc

    def _pending_request_body_limit(self, request_id: int) -> int | None:
        with self._pending_state_lock():
            return _request_body_byte_limit(self._pending_request_budget(), request_id)

    def _encode_pending_request_body(
        self,
        request_id: int,
        payload: dict[str, Any] | bytes,
    ) -> tuple[bytes, bool]:
        budget = self._pending_request_budget()
        try:
            return _encode_request_body(
                payload,
                compression=self.compression,
                max_body_bytes=self._pending_request_body_limit(request_id),
                pending_limit=getattr(self, "max_pending_request_bytes", budget.max_bytes),
            )
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc

    def _expire_pending_request(self, request_id: int) -> None:
        future, _trace = self._discard_pending_request(request_id)
        if future is None:
            return
        try_set_future_exception(future, FerricStoreError("protocol request timed out"))
        self._notify_idle_if_needed()

    def _current_transport_binding(
        self,
    ) -> tuple[int, socket.socket | ssl.SSLSocket | None]:
        transport_state_lock = getattr(self, "_transport_state_lock", None)
        if transport_state_lock is None:
            transport_state_lock = threading.Lock()
            self._transport_state_lock = transport_state_lock
        with transport_state_lock:
            return (
                int(getattr(self, "_transport_generation", 0)),
                cast(
                    socket.socket | ssl.SSLSocket | None,
                    getattr(self, "_sock", None),
                ),
            )

    def _register_pending_request(
        self,
        request_id: int,
        future: Future[ProtocolResponse],
        *,
        expected_collection_items: int | None = None,
        client_trace: dict[str, Any] | None = None,
        binding: tuple[int, socket.socket | ssl.SSLSocket | None] | None = None,
        expires_at: float | None = None,
        response_identity: ResponseIdentity | None = None,
    ) -> tuple[int, socket.socket | ssl.SSLSocket | None]:
        transport_state_lock = getattr(self, "_transport_state_lock", None)
        if transport_state_lock is None:
            transport_state_lock = threading.Lock()
            self._transport_state_lock = transport_state_lock

        def register(
            selected_binding: tuple[int, socket.socket | ssl.SSLSocket | None],
        ) -> tuple[int, socket.socket | ssl.SSLSocket | None]:
            with self._pending_state_lock():
                budget = self._pending_request_budget()
                try:
                    budget.reserve(request_id)
                except PendingRequestCapacityError as exc:
                    raise FerricStoreError(str(exc)) from exc
                try:
                    self._pending[request_id] = future
                    bindings = getattr(self, "_pending_transport_bindings", None)
                    if bindings is None:
                        bindings = {}
                        self._pending_transport_bindings = bindings
                    bindings[request_id] = selected_binding
                    if response_identity is not None:
                        _response_identity_map(self)[request_id] = response_identity
                    if expected_collection_items is not None:
                        _response_item_count_map(self)[request_id] = expected_collection_items
                    if client_trace is not None:
                        self._pending_traces[request_id] = client_trace
                    if expires_at is not None:
                        self._pending_deadline_scheduler().schedule(request_id, expires_at)
                except BaseException:
                    self._pending.pop(request_id, None)
                    bindings = getattr(self, "_pending_transport_bindings", None)
                    if bindings is not None:
                        bindings.pop(request_id, None)
                    _response_identity_map(self).pop(request_id, None)
                    _response_item_count_map(self).pop(request_id, None)
                    self._pending_traces.pop(request_id, None)
                    budget.release(request_id)
                    raise
            return selected_binding

        if binding is not None:
            return register(binding)
        with transport_state_lock:
            selected = (
                int(getattr(self, "_transport_generation", 0)),
                cast(
                    socket.socket | ssl.SSLSocket | None,
                    getattr(self, "_sock", None),
                ),
            )
            return register(selected)

    def _discard_pending_request(
        self,
        request_id: int,
        *,
        expected_future: Future[ProtocolResponse] | None = None,
    ) -> tuple[Future[ProtocolResponse] | None, dict[str, Any] | None]:
        with self._pending_state_lock():
            future = self._pending.get(request_id)
            if expected_future is not None and future is not expected_future:
                return None, None
            future = self._pending.pop(request_id, None)
            trace = self._pending_traces.pop(request_id, None)
            _response_item_count_map(self).pop(request_id, None)
            _response_identity_map(self).pop(request_id, None)
            bindings = getattr(self, "_pending_transport_bindings", None)
            if bindings is not None:
                bindings.pop(request_id, None)
            self._release_pending_lifecycle_locked(request_id)
            return future, trace

    def _take_pending_response(
        self,
        request_id: int,
        sock: socket.socket | ssl.SSLSocket | None,
    ) -> tuple[Future[ProtocolResponse] | None, dict[str, Any] | None]:
        with self._pending_state_lock():
            bindings = getattr(self, "_pending_transport_bindings", None)
            binding = None if bindings is None else bindings.get(request_id)
            if binding is not None and binding[1] is not None and binding[1] is not sock:
                return None, None
            future = self._pending.pop(request_id, None)
            trace = self._pending_traces.pop(request_id, None)
            if bindings is not None:
                bindings.pop(request_id, None)
            _response_identity_map(self).pop(request_id, None)
            self._release_pending_lifecycle_locked(request_id)
            return future, trace

    def _pending_socket(self, request_id: int) -> socket.socket | ssl.SSLSocket:
        with self._pending_state_lock():
            bindings = getattr(self, "_pending_transport_bindings", None)
            binding = None if bindings is None else bindings.get(request_id)
            if binding is not None and binding[1] is not None:
                return cast(socket.socket | ssl.SSLSocket, binding[1])
        return self._require_socket()

    def _close_transport(
        self,
        exc: BaseException | None = None,
        *,
        mark_closed: bool = False,
        expected_sock: socket.socket | ssl.SSLSocket | None = None,
    ) -> None:
        transport_state_lock = getattr(self, "_transport_state_lock", None)
        if transport_state_lock is None:
            transport_state_lock = threading.Lock()
            self._transport_state_lock = transport_state_lock
        with transport_state_lock:
            if mark_closed:
                self._closed = True
            if expected_sock is not None and self._sock is not expected_sock:
                return
            sock = self._sock
            transport_generation = int(getattr(self, "_transport_generation", 0))
            self._sock = None
        retired_sockets = getattr(self, "_retired_sockets", None)
        if retired_sockets is None:
            retired_sockets = RetryableResourceSet(())
            self._retired_sockets = retired_sockets
        if sock is not None:
            retired_sockets.add(sock)
        heartbeat_stop = getattr(self, "_heartbeat_stop", None)
        self._heartbeat_stop = None
        if heartbeat_stop is not None:
            heartbeat_stop.set()
        listeners: list[Callable[[], None]] = []
        with self._events_cv:
            if exc is not None or mark_closed:
                self._event_error = exc or FerricStoreError("protocol connection is closed")
                listeners = list(self._event_listeners)
            self._events_cv.notify_all()
        _notify_event_listeners(listeners)
        close_error = exc or FerricStoreError("protocol connection is closed")

        def close_socket(resource: Any) -> None:
            with contextlib.suppress(OSError):
                resource.shutdown(socket.SHUT_RDWR)
            resource.close()
            retired_sockets.complete(resource)

        transport_close_lock = getattr(self, "_transport_close_lock", None)
        if transport_close_lock is None:
            transport_close_lock = threading.Lock()
            self._transport_close_lock = transport_close_lock

        def close_retired_sockets() -> None:
            with transport_close_lock:
                pending_sockets = retired_sockets.snapshot()
                close_resources_sync(
                    [
                        partial(close_socket, resource)
                        for resource in pending_sockets
                        if retired_sockets.contains(resource)
                    ]
                )

        try:
            close_resources_sync(
                [
                    partial(
                        self._fail_pending,
                        close_error,
                        transport_generation=None if mark_closed else transport_generation,
                    ),
                    close_retired_sockets,
                ]
            )
        finally:
            self._notify_idle_if_needed()

    def _notify_idle_if_needed(self) -> None:
        if getattr(self, "_pending", None) or getattr(self, "_connecting", False):
            return
        idle_listeners = getattr(self, "_idle_listeners", ())
        if not idle_listeners:
            return
        events_cv = getattr(self, "_events_cv", None)
        if events_cv is None:
            listeners = list(idle_listeners)
        else:
            with events_cv:
                listeners = list(idle_listeners)
        _notify_event_listeners(listeners)

    def pipeline(self, transaction: bool = False) -> ProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return ProtocolPipeline(self)

    def execute_command(self, *args: Any) -> Any:
        expected_collection_items = _expected_command_collection_items(args)
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            response = self._request(
                opcode,
                1,
                payload,
                flags,
                expected_collection_items=expected_collection_items,
            )
            return self._response_value(response)

        command = build_protocol_command(*args)
        if _blocks_forever(args):
            response = self._request(
                command.opcode,
                command.lane_id,
                command.payload,
                command.flags,
                timeout=None,
                expected_collection_items=expected_collection_items,
            )
        else:
            response = self._request(
                command.opcode,
                command.lane_id,
                command.payload,
                command.flags,
                expected_collection_items=expected_collection_items,
            )
        return self._response_value(response)

    def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        """Execute a routed command on the exact topology lane."""
        expected_collection_items = _expected_command_collection_items(args)
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            return self._response_value(
                self._request_on_lane(
                    opcode,
                    lane_id,
                    payload,
                    flags,
                    expected_collection_items=expected_collection_items,
                )
            )

        command = build_protocol_command(*args)
        if _blocks_forever(args):
            response = self._request_on_lane(
                command.opcode,
                lane_id,
                command.payload,
                command.flags,
                timeout=None,
                expected_collection_items=expected_collection_items,
            )
        else:
            response = self._request_on_lane(
                command.opcode,
                lane_id,
                command.payload,
                command.flags,
                expected_collection_items=expected_collection_items,
            )
        return self._response_value(response)

    def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        command = build_protocol_command(*args)
        response = self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            expected_collection_items=_expected_command_collection_items(args),
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    def execute_command_with_trace_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> dict[str, Any]:
        command = build_protocol_command(*args)
        response = self._request_on_lane(
            command.opcode,
            lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            expected_collection_items=_expected_command_collection_items(args),
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    def submit_command(self, *args: Any) -> Future[Any]:
        expected_collection_items = _expected_command_collection_items(args)
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            _request_id, response_future = self._submit_request(
                opcode,
                1,
                payload,
                flags,
                expected_collection_items=expected_collection_items,
            )
            return self._value_future(response_future)

        command = build_protocol_command(*args)
        _request_id, response_future = self._submit_request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags,
            expected_collection_items=expected_collection_items,
        )
        return self._value_future(response_future)

    def submit_command_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> Future[Any]:
        opcode: int
        payload: dict[str, Any] | bytes
        flags: int
        expected_collection_items = _expected_command_collection_items(args)
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
        else:
            command = build_protocol_command(*args)
            opcode, payload, flags = command.opcode, command.payload, command.flags
        _request_id, response_future = self._submit_request_on_lane(
            opcode,
            lane_id,
            payload,
            flags,
            expected_collection_items=expected_collection_items,
        )
        return self._value_future(response_future)

    def submit_mget(self, keys: Sequence[Any]) -> Future[Any]:
        payload = _compact_kv_keys_payload(keys, 2)
        if payload is None:
            raise InvalidCommandError("MGET requires one or more string/binary keys")
        return self.submit_mget_payload(payload)

    def submit_mget_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MGET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request(
            _OP_MGET, 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_flow_value_mget_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "FLOW.VALUE.MGET payload must be a non-empty compact binary payload"
            )
        _request_id, response_future = self._submit_request(
            _OP_FLOW_VALUE_MGET, 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_flow_value_mget_payload_on_lane(
        self,
        payload: bytes,
        lane_id: int,
    ) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "FLOW.VALUE.MGET payload must be a non-empty compact binary payload"
            )
        _request_id, response_future = self._submit_request_on_lane(
            _OP_FLOW_VALUE_MGET,
            lane_id,
            payload,
            _FLAG_CUSTOM_PAYLOAD,
        )
        return self._value_future(response_future)

    def submit_mset_same_value(self, keys: Sequence[Any], value: Any) -> Future[Any]:
        payload = _compact_kv_set_keys_value_payload(keys, value)
        if payload is None:
            raise InvalidCommandError(
                "MSET requires one or more string/binary keys and a string/binary value"
            )
        return self.submit_mset_payload(payload)

    def submit_mset_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MSET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request(
            _OPCODES["MSET"], 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_mset_payload_on_lane(self, payload: bytes, lane_id: int) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MSET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request_on_lane(
            _OPCODES["MSET"],
            lane_id,
            payload,
            _FLAG_CUSTOM_PAYLOAD,
        )
        return self._value_future(response_future)

    def submit_pipeline_payload(self, payload: bytes, count: int) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("PIPELINE payload must be a non-empty compact binary payload")
        if count < 0:
            raise InvalidCommandError("PIPELINE payload count must be non-negative")

        future: Future[list[Any]] = Future()
        response_future = self._submit_pipeline_payload(payload, count)
        self._complete_batch_future(response_future, count, future)
        return future

    def submit_pipeline_payload_on_lane(
        self,
        payload: bytes,
        count: int,
        lane_id: int,
    ) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("PIPELINE payload must be a non-empty compact binary payload")
        if count < 0:
            raise InvalidCommandError("PIPELINE payload count must be non-negative")
        future: Future[list[Any]] = Future()
        response_future = self._submit_pipeline_payload(
            payload,
            count,
            routed_lane=lane_id,
        )
        self._complete_batch_future(response_future, count, future)
        return future

    def submit_flow_many_payload(
        self, command: str, payload: bytes, count: int
    ) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "Flow many payload must be a non-empty compact binary payload"
            )
        if count < 0:
            raise InvalidCommandError("Flow many payload count must be non-negative")

        name = _command_name(command)
        if name not in {
            "FLOW.CREATE_MANY",
            "FLOW.COMPLETE_MANY",
            "FLOW.TRANSITION_MANY",
            "FLOW.RETRY_MANY",
            "FLOW.FAIL_MANY",
            "FLOW.CANCEL_MANY",
        }:
            raise InvalidCommandError(f"{name} does not support direct Flow many payload submit")

        future: Future[list[Any]] = Future()
        self._submit_flow_many_batch([(_OPCODES[name], payload, count)], count, future)
        return future

    def submit_flow_many_payload_on_lane(
        self,
        command: str,
        payload: bytes,
        count: int,
        lane_id: int,
    ) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "Flow many payload must be a non-empty compact binary payload"
            )
        if count < 0:
            raise InvalidCommandError("Flow many payload count must be non-negative")
        name = _command_name(command)
        if name not in {
            "FLOW.CREATE_MANY",
            "FLOW.COMPLETE_MANY",
            "FLOW.TRANSITION_MANY",
            "FLOW.RETRY_MANY",
            "FLOW.FAIL_MANY",
            "FLOW.CANCEL_MANY",
        }:
            raise InvalidCommandError(f"{name} does not support direct Flow many payload submit")
        future: Future[list[Any]] = Future()
        self._submit_flow_many_batch(
            [(_OPCODES[name], payload, count)],
            count,
            future,
            routed_lane=lane_id,
        )
        return future

    def _fast_bulk_kv_request(self, args: tuple[Any, ...]) -> tuple[int, bytes, int] | None:
        if not args:
            return None

        try:
            name = _command_name(args[0])
        except Exception:
            return None

        command_args = args[1:]
        if name == "MGET":
            payload = _compact_kv_keys_payload(command_args, 2)
            return (
                (_OPCODES["MGET"], payload, _FLAG_CUSTOM_PAYLOAD) if payload is not None else None
            )
        if name == "MSET":
            payload = _compact_kv_set_pairs_payload(command_args)
            return (
                (_OPCODES["MSET"], payload, _FLAG_CUSTOM_PAYLOAD) if payload is not None else None
            )
        return None

    def submit_commands(self, commands: list[tuple[Any, ...]]) -> list[Future[Any]]:
        return self._submit_commands(commands, routed_lane=None)

    def submit_commands_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Future[Any]]:
        return self._submit_commands(commands, routed_lane=lane_id)

    def _submit_commands(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
    ) -> list[Future[Any]]:
        if not commands:
            return []

        compact_payload = _compact_pipeline_payload_from_raw(commands, values_only=False)
        if compact_payload is not None:
            compact_response_future = self._submit_pipeline_payload(
                compact_payload,
                len(commands),
                routed_lane=routed_lane,
            )
            return self._pipeline_item_futures(compact_response_future, len(commands))

        protocol_commands = [build_protocol_command(*command) for command in commands]
        if _pipeline_frame_supported(protocol_commands):
            return self._submit_pipeline(
                protocol_commands,
                compact=False,
                routed_lane=routed_lane,
            )

        pending: list[tuple[int, Future[ProtocolResponse]]] = []
        frames: list[bytes] = []
        expires_at = None if self.timeout is None else time.monotonic() + max(0.0, self.timeout)

        self._ensure_connected()
        with self._lock:
            binding = self._current_transport_binding()
            try:
                for raw_command, command in zip(commands, protocol_commands, strict=True):
                    request_id = self._next_request_id()
                    lane_id = (
                        self._next_lane_id(command.lane_id) if routed_lane is None else routed_lane
                    )
                    response_future: Future[ProtocolResponse] = Future()
                    expected_collection_items = _expected_command_collection_items(raw_command)
                    self._register_pending_request(
                        request_id,
                        response_future,
                        expected_collection_items=expected_collection_items,
                        binding=binding,
                        expires_at=expires_at,
                        response_identity=ResponseIdentity(
                            lane_id=lane_id,
                            opcode=command.opcode,
                            request_id=request_id,
                        ),
                    )
                    pending.append((request_id, response_future))

                    body, compressed = self._encode_pending_request_body(
                        request_id,
                        command.payload,
                    )
                    flags = command.flags
                    if compressed:
                        flags |= _FLAG_COMPRESSED
                    header = _HEADER.pack(
                        _MAGIC,
                        _REQUEST_VERSION,
                        flags,
                        lane_id,
                        command.opcode,
                        request_id,
                        len(body),
                    )
                    self._set_pending_request_size(request_id, len(header) + len(body))
                    frames.extend(frame_parts(header, body))

                sock = binding[1] or self._require_socket()
                try:
                    _send_frames(
                        sock,
                        frames,
                        timeout=self.timeout,
                    )
                except BaseException as write_error:
                    try:
                        self._close_transport(
                            FerricStoreError("protocol write failed", raw=write_error),
                            mark_closed=False,
                            expected_sock=sock,
                        )
                    except BaseException as cleanup_error:
                        raise_primary_with_cleanup(
                            write_error,
                            write_error.__traceback__,
                            cleanup_error,
                        )
                    raise
                self._last_activity = time.monotonic()
            except BaseException as exc:
                for request_id, response_future in pending:
                    self._discard_pending_request(
                        request_id,
                        expected_future=response_future,
                    )
                    try_set_future_exception(response_future, exc)
                raise

        return [self._value_future(response_future) for _request_id, response_future in pending]

    def submit_batch(self, commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
        return self._submit_batch(commands, routed_lane=None)

    def submit_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> Future[list[Any]]:
        return self._submit_batch(commands, routed_lane=lane_id)

    def _submit_batch(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
    ) -> Future[list[Any]]:
        future: Future[list[Any]] = Future()
        if not commands:
            future.set_result([])
            return future

        compact_payload = _compact_pipeline_payload_from_raw(commands, values_only=True)
        if compact_payload is not None:
            response_future = self._submit_pipeline_payload(
                compact_payload,
                len(commands),
                routed_lane=routed_lane,
            )
            self._complete_batch_future(response_future, len(commands), future)
            return future

        flow_many_payloads = _compact_flow_many_payloads_from_raw(commands)
        if flow_many_payloads is not None:
            self._submit_flow_many_batch(
                flow_many_payloads,
                len(commands),
                future,
                routed_lane=routed_lane,
            )
            return future

        protocol_commands = [build_protocol_command(*command) for command in commands]
        if not _pipeline_frame_supported(protocol_commands):
            item_futures = self._submit_commands(commands, routed_lane=routed_lane)
            _set_wire_future_sources(
                future,
                [
                    source
                    for item in item_futures
                    for source in getattr(item, "_ferricstore_sources", (item,))
                ],
            )
            lock = threading.Lock()
            results: list[Any] = [None] * len(item_futures)
            remaining = len(item_futures)

            def complete_items(index: int, item_future: Future[Any]) -> None:
                nonlocal remaining
                try:
                    value = item_future.result()
                except Exception as exc:
                    try_set_future_exception(future, exc)
                    return

                with lock:
                    if future.done():
                        return
                    results[index] = value
                    remaining -= 1
                    if remaining == 0:
                        try_set_future_result(future, results)

            for index, item in enumerate(item_futures):
                item.add_done_callback(partial(complete_items, index))
            return future

        response_future = self._submit_pipeline_request(
            protocol_commands,
            compact=False,
            values_only=True,
            routed_lane=routed_lane,
        )
        self._complete_batch_future(response_future, len(protocol_commands), future)
        return future

    def _submit_flow_many_batch(
        self,
        payloads: list[tuple[int, bytes, int]],
        expected_count: int,
        future: Future[list[Any]],
        *,
        routed_lane: int | None = None,
    ) -> None:
        pending: list[tuple[Future[ProtocolResponse], int]] = []
        try:
            for opcode, payload, count in payloads:
                submit = (
                    self._submit_request if routed_lane is None else self._submit_request_on_lane
                )
                _request_id, response_future = submit(
                    opcode,
                    1 if routed_lane is None else routed_lane,
                    payload,
                    _FLAG_CUSTOM_PAYLOAD,
                )
                pending.append((response_future, count))
        except Exception as exc:
            _set_wire_future_sources(
                future,
                [response_future for response_future, _count in pending],
            )
            try_set_future_exception(future, exc)
            return

        _set_wire_future_sources(
            future,
            [response_future for response_future, _count in pending],
        )

        results: list[list[Any] | None] = [None] * len(pending)
        remaining = len(pending)
        lock = threading.Lock()

        def complete(index: int, response_future: Future[ProtocolResponse], count: int) -> None:
            nonlocal remaining
            try:
                value = self._response_value(response_future.result())
                group_values = _flow_many_group_values(value, count)
            except Exception as exc:
                try_set_future_exception(future, exc)
                return

            with lock:
                if future.done():
                    return
                results[index] = group_values
                remaining -= 1
                if remaining == 0:
                    merged = [item for group in results if group is not None for item in group]
                    if len(merged) != expected_count:
                        try_set_future_exception(
                            future,
                            FerricStoreError(
                                "protocol Flow many returned invalid result", raw=merged
                            ),
                        )
                    else:
                        try_set_future_result(future, merged)

        for index, (response_future, count) in enumerate(pending):
            response_future.add_done_callback(partial(complete, index, count=count))

    def _complete_batch_future(
        self,
        response_future: Future[ProtocolResponse],
        expected_count: int,
        future: Future[list[Any]],
        *,
        allow_scalar_ok: bool = False,
    ) -> None:

        _set_wire_future_sources(future, [response_future])

        def complete(source_future: Future[ProtocolResponse]) -> None:
            try:
                value = self._response_value(source_future.result())
                if allow_scalar_ok and _ok_scalar(value):
                    try_set_future_result(future, [value] * expected_count)
                    return
                if not isinstance(value, list) or len(value) != expected_count:
                    raise FerricStoreError("protocol PIPELINE returned invalid result", raw=value)
                if _pipeline_pair_list(value):
                    result = [self._batch_item_value(item) for item in value]
                else:
                    result = value
                try_set_future_result(future, result)
            except Exception as exc:
                try_set_future_exception(future, exc)

        response_future.add_done_callback(complete)

    def _submit_pipeline_payload(
        self,
        payload: bytes,
        _expected_count: int,
        *,
        routed_lane: int | None = None,
    ) -> Future[ProtocolResponse]:
        submit = self._submit_request if routed_lane is None else self._submit_request_on_lane
        _request_id, response_future = submit(
            _OP_PIPELINE,
            1 if routed_lane is None else routed_lane,
            payload,
            _FLAG_CUSTOM_PAYLOAD,
        )
        return response_future

    def _submit_pipeline(
        self,
        commands: list[ProtocolCommand],
        *,
        raw_commands: list[tuple[Any, ...]] | None = None,
        compact: bool = True,
        routed_lane: int | None = None,
    ) -> list[Future[Any]]:
        response_future = self._submit_pipeline_request(
            commands,
            raw_commands=raw_commands,
            compact=compact,
            routed_lane=routed_lane,
        )
        return self._pipeline_item_futures(response_future, len(commands))

    def _pipeline_item_futures(
        self,
        response_future: Future[ProtocolResponse],
        count: int,
    ) -> list[Future[Any]]:
        futures: list[Future[Any]] = [DeferredCallbackFuture() for _ in range(count)]

        def complete(source_future: Future[ProtocolResponse]) -> None:
            deferred = [future for future in futures if isinstance(future, DeferredCallbackFuture)]
            for future in deferred:
                future.defer_callbacks()
            try:
                value = self._response_value(source_future.result())
                if not isinstance(value, list) or len(value) != len(futures):
                    raise FerricStoreError("protocol PIPELINE returned invalid result", raw=value)
                decoded = [self._batch_item_value(item) for item in value]
                for target, item in zip(futures, decoded, strict=True):
                    try_set_future_result(target, item)
            except Exception as exc:
                for target in futures:
                    try_set_future_exception(target, exc)
            finally:
                for future in deferred:
                    future.release_callbacks()

        response_future.add_done_callback(complete)
        return futures

    def _submit_pipeline_request(
        self,
        commands: list[ProtocolCommand],
        *,
        raw_commands: list[tuple[Any, ...]] | None = None,
        compact: bool = True,
        values_only: bool = False,
        routed_lane: int | None = None,
    ) -> Future[ProtocolResponse]:
        if not compact:
            compact_payload = None
        elif raw_commands is not None:
            compact_payload = _compact_pipeline_payload_from_raw(
                raw_commands,
                values_only=values_only,
            )
        else:
            compact_payload = _compact_pipeline_payload(commands, values_only=values_only)
        flags = _FLAG_CUSTOM_PAYLOAD if compact_payload is not None else 0
        payload: dict[str, Any] | bytes

        if compact_payload is not None:
            payload = compact_payload
        else:
            pipeline_commands = [
                {
                    "opcode": command.opcode,
                    "lane_id": command.lane_id if routed_lane is None else routed_lane,
                    "request_id": idx + 1,
                    "body": command.payload,
                }
                for idx, command in enumerate(commands)
            ]
            payload = {"atomicity": "none", "commands": pipeline_commands, "return": "compact"}

        submit = self._submit_request if routed_lane is None else self._submit_request_on_lane
        _request_id, response_future = submit(
            _OP_PIPELINE,
            1 if routed_lane is None else routed_lane,
            payload,
            flags,
        )
        return response_future

    def _value_future(self, response_future: Future[ProtocolResponse]) -> Future[Any]:
        value_future: Future[Any] = Future()
        _set_wire_future_sources(value_future, [response_future])

        def complete(source: Future[ProtocolResponse]) -> None:
            try:
                value = self._response_value(source.result())
            except Exception as exc:
                try_set_future_exception(value_future, exc)
            else:
                try_set_future_result(value_future, value)

        response_future.add_done_callback(complete)
        return value_future

    def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | None = None,
        partition_keys: list[str] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        payload = _flow_wake_payload(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        response = self._request(
            _OP_SUBSCRIBE_EVENTS,
            0,
            payload,
        )
        value = self._response_value(response)
        self.register_flow_wake_subscription(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        return value

    def register_flow_wake_subscription(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | None = None,
        partition_keys: list[str] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> None:
        payload = _flow_wake_payload(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        with self._subscription_lock:
            self._flow_wake_subscriptions[:] = [payload]

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return self._execute_batch(commands, routed_lane=None)

    def execute_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        return self._execute_batch(commands, routed_lane=lane_id)

    def _execute_batch(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
    ) -> list[Any]:
        if not commands:
            return []

        lane_id = 1 if routed_lane is None else routed_lane
        request = self._request if routed_lane is None else self._request_on_lane

        flow_many_payloads = _compact_flow_many_payloads_from_raw(commands)
        if flow_many_payloads is not None:
            values: list[Any] = []
            for opcode, payload, count in flow_many_payloads:
                response = request(
                    opcode,
                    lane_id,
                    payload,
                    _FLAG_CUSTOM_PAYLOAD,
                )
                group_values = self._response_value(response)
                values.extend(_flow_many_group_values(group_values, count))
            return values

        compact_payload = _compact_pipeline_payload_from_raw(commands, values_only=True)
        if compact_payload is not None:
            response = request(
                _OP_PIPELINE,
                lane_id,
                compact_payload,
                _FLAG_CUSTOM_PAYLOAD,
            )
            values = require_batch_items(
                self._response_value(response),
                len(commands),
                operation="protocol PIPELINE",
            )
            if _pipeline_pair_list(values):
                return [self._batch_item_value(item) for item in values]
            return values

        protocol_commands = [build_protocol_command(*command) for command in commands]
        if not _pipeline_frame_supported(protocol_commands):
            if routed_lane is None:
                return [self.execute_command(*command) for command in commands]
            values = []
            for raw_command, command in zip(commands, protocol_commands, strict=True):
                command_lane = command.lane_id if routed_lane is None else routed_lane
                if _blocks_forever(raw_command):
                    response = request(
                        command.opcode,
                        command_lane,
                        command.payload,
                        command.flags,
                        timeout=None,
                    )
                else:
                    response = request(
                        command.opcode,
                        command_lane,
                        command.payload,
                        command.flags,
                    )
                values.append(self._response_value(response))
            return values

        batch_commands = [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id if routed_lane is None else routed_lane,
                "request_id": idx + 1,
                "body": command.payload,
            }
            for idx, command in enumerate(protocol_commands)
        ]
        response = request(
            _OP_PIPELINE,
            lane_id,
            {"atomicity": "none", "commands": batch_commands, "return": "compact"},
        )

        values = require_batch_items(
            self._response_value(response),
            len(commands),
            operation="protocol PIPELINE",
        )

        return [self._batch_item_value(item) for item in values]

    def _connect(self) -> None:
        try:
            self._connect_without_idle_notification()
        finally:
            self._notify_idle_if_needed()

    def _connect_without_idle_notification(self) -> None:
        last_error: BaseException | None = None
        deadline = _sync_adapter_deadline(self)

        for attempt in range(3):
            try:
                self._connecting = True
                try:
                    self._connect_once()
                    if self.password is not None:
                        self._response_value(
                            self._request(
                                _OP_AUTH,
                                0,
                                {"username": self.username, "password": self.password},
                            )
                        )
                    with self._subscription_lock:
                        subscriptions = list(self._flow_wake_subscriptions)
                    for payload in subscriptions:
                        self._response_value(self._request(_OP_SUBSCRIBE_EVENTS, 0, payload))
                    with self._events_cv:
                        self._event_error = None
                finally:
                    self._connecting = False
                break
            except (ConnectionError, OSError, TimeoutError, FerricStoreError) as exc:
                self._close_transport(exc, mark_closed=False)
                if not self._startup_retryable(exc):
                    raise
                last_error = exc
                if attempt == 2:
                    raise
                delay = 0.02 * (attempt + 1)
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise FutureTimeoutError from exc
                    delay = min(delay, remaining)
                time.sleep(delay)
        else:
            if last_error is not None:
                raise last_error

        try:
            self._start_heartbeat()
        except BaseException as start_error:
            try:
                self._close_transport(
                    FerricStoreError("protocol heartbeat thread failed to start", raw=start_error),
                    mark_closed=False,
                )
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(
                    start_error,
                    start_error.__traceback__,
                    cleanup_error,
                )
            raise

    @staticmethod
    def _startup_retryable(exc: BaseException) -> bool:
        if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
            return True
        if isinstance(exc, FerricStoreError):
            message = str(exc).lower()
            return "timed out" in message or "closed" in message or "reset" in message
        return False

    def _connect_once(self) -> None:
        deadline = _sync_adapter_deadline(self)
        connect_timeout = _timeout_with_deadline(self.timeout, deadline)
        raw_sock = socket.create_connection((self.host, self.port), timeout=connect_timeout)
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connected_sock: socket.socket | ssl.SSLSocket
        if self.tls:
            context = self.ssl_context or ssl.create_default_context()
            try:
                raw_sock.settimeout(_timeout_with_deadline(self.timeout, deadline))
                connected_sock = context.wrap_socket(raw_sock, server_hostname=self.host)
            except BaseException:
                raw_sock.close()
                raise
            connected_sock.settimeout(None)
        else:
            raw_sock.settimeout(None)
            connected_sock = raw_sock
        with self._transport_state_lock:
            rejected = self._closed
            self._transport_generation = int(getattr(self, "_transport_generation", 0)) + 1
            self._sock = connected_sock
        if rejected:
            error = FerricStoreError("protocol connection is closed")
            try:
                self._close_transport(error, mark_closed=True, expected_sock=connected_sock)
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(error, error.__traceback__, cleanup_error)
            raise error
        reader_thread = threading.Thread(
            target=self._reader_loop,
            args=(connected_sock,),
            daemon=True,
        )
        self._reader_thread = reader_thread
        try:
            reader_thread.start()
        except BaseException as start_error:
            if reader_thread.ident is None:
                self._reader_thread = None
            try:
                self._close_transport(
                    FerricStoreError("protocol reader thread failed to start", raw=start_error),
                    mark_closed=False,
                    expected_sock=connected_sock,
                )
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(
                    start_error,
                    start_error.__traceback__,
                    cleanup_error,
                )
            raise

        startup: dict[str, Any] = {
            "compression": self.compression,
            "compact_flow_responses": True,
        }
        if self.client_name is not None:
            startup["client_name"] = self.client_name
            startup["driver_name"] = self.client_name
        self._response_value(self._request(_OP_STARTUP, 0, startup))

    def _start_heartbeat(self) -> None:
        if self.heartbeat_interval is None or self.heartbeat_interval <= 0:
            return

        previous_stop = getattr(self, "_heartbeat_stop", None)
        if previous_stop is not None:
            previous_stop.set()
        heartbeat_stop = threading.Event()
        self._heartbeat_stop = heartbeat_stop
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(self._sock, heartbeat_stop),
            name="ferricstore-protocol-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread = heartbeat_thread
        try:
            heartbeat_thread.start()
        except BaseException:
            heartbeat_stop.set()
            if heartbeat_thread.ident is None:
                self._heartbeat_thread = None
                self._heartbeat_stop = None
            raise

    def _heartbeat_loop(
        self,
        sock: socket.socket | ssl.SSLSocket | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        if sock is None:
            sock = self._sock
        if stop_event is None:
            stop_event = threading.Event()
        interval = float(self.heartbeat_interval or 0)
        timeout = self.heartbeat_timeout
        while interval > 0 and self._sock is sock and sock is not None:
            if stop_event.wait(interval):
                return
            if self._sock is not sock:
                return
            if getattr(self, "_heartbeat_pause_count", 0) > 0:
                continue
            if time.monotonic() - self._last_activity < interval:
                continue

            request_id: int | None = None
            try:
                request_id, future = self._submit_request(_OPCODES["PING"], 0, {})
                if timeout is None:
                    future.result()
                else:
                    future.result(timeout=timeout)
            except Exception as exc:
                if request_id is not None:
                    self._discard_pending_request(request_id)
                self._close_transport(
                    FerricStoreError("protocol heartbeat failed", raw=exc),
                    mark_closed=False,
                    expected_sock=sock,
                )
                return

    def _next_request_id(self) -> int:
        self._request_id = (self._request_id + 1) & 0xFFFFFFFFFFFFFFFF
        if self._request_id == 0:
            self._request_id = 1
        return self._request_id

    def _next_lane_id(self, lane_id: int) -> int:
        if lane_id == 0:
            return lane_id
        fixed_lane_id = cast(int | None, getattr(self, "_fixed_lane_id", None))
        if fixed_lane_id is not None:
            return fixed_lane_id
        if self.lanes == 1:
            return lane_id
        self._lane_cursor = (self._lane_cursor % self.lanes) + 1
        return self._lane_cursor

    def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> dict[str, Any] | None:
        sock = self._pending_socket(request_id)
        trace_enabled = bool(extra_flags & _FLAG_TRACE)
        encode_started_ns = time.perf_counter_ns() if trace_enabled else 0
        body, compressed = self._encode_pending_request_body(request_id, payload)
        flags = extra_flags
        if compressed:
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        self._set_pending_request_size(request_id, len(header) + len(body))
        encode_done_ns = time.perf_counter_ns() if trace_enabled else 0
        write_started_ns = encode_done_ns
        write_timeout = _timeout_with_deadline(
            self.timeout,
            _sync_adapter_deadline(self),
        )
        try:
            _send_frames(sock, frame_parts(header, body), timeout=write_timeout)
        except BaseException as write_error:
            try:
                self._close_transport(
                    FerricStoreError("protocol write failed", raw=write_error),
                    mark_closed=False,
                    expected_sock=sock,
                )
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(
                    write_error,
                    write_error.__traceback__,
                    cleanup_error,
                )
            raise
        self._last_activity = time.monotonic()
        if not trace_enabled:
            return None
        write_done_ns = time.perf_counter_ns()
        return {
            "encode_us": (encode_done_ns - encode_started_ns) // 1000,
            "socket_write_us": (write_done_ns - write_started_ns) // 1000,
        }

    def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse:
        trace_enabled = bool(flags & _FLAG_TRACE)
        request_started_ns = time.perf_counter_ns() if trace_enabled else 0
        effective_timeout = self.timeout if timeout is _USE_ADAPTER_TIMEOUT else timeout
        effective_timeout = cast(float | None, effective_timeout)
        deadline = None if effective_timeout is None else time.monotonic() + effective_timeout
        parent_deadline = _sync_adapter_deadline(self)
        if parent_deadline is not None:
            deadline = parent_deadline if deadline is None else min(deadline, parent_deadline)
        request_id: int | None = None

        try:
            request_id, future = self._submit_request(
                opcode,
                lane_id,
                payload,
                flags,
                exact_lane=exact_lane,
                expected_collection_items=expected_collection_items,
                _deadline=deadline,
                _expire_at_adapter_timeout=False,
            )
            wait_started_ns = time.perf_counter_ns() if trace_enabled else 0
            if deadline is None:
                response = future.result()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise FutureTimeoutError
                response = future.result(timeout=remaining)
            if trace_enabled and response.trace is not None:
                wait_done_ns = time.perf_counter_ns()
                client_trace = response.trace.setdefault("client", {})
                client_trace["future_wait_us"] = (wait_done_ns - wait_started_ns) // 1000
                client_trace["request_total_us"] = (wait_done_ns - request_started_ns) // 1000
            return response
        except (FutureTimeoutError, TimeoutError) as exc:
            if request_id is not None:
                self._discard_pending_request(request_id)
                self._notify_idle_if_needed()
            raise FerricStoreError("protocol request timed out") from exc

    def _request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse:
        return self._request(
            opcode,
            lane_id,
            payload,
            flags,
            timeout=timeout,
            exact_lane=True,
            expected_collection_items=expected_collection_items,
        )

    def _submit_request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
        _deadline: float | None = None,
        _expire_at_adapter_timeout: bool = True,
    ) -> tuple[int, Future[ProtocolResponse]]:
        trace_enabled = bool(flags & _FLAG_TRACE)
        submit_started_ns = time.perf_counter_ns() if trace_enabled else 0
        deadline_context = getattr(self, "_deadline_context", None)
        if deadline_context is None:
            deadline_context = threading.local()
            self._deadline_context = deadline_context
        had_deadline = hasattr(deadline_context, "deadline")
        previous_deadline = getattr(deadline_context, "deadline", None)
        deadline_context.deadline = _deadline
        lock_acquired = False
        try:
            self._ensure_connected()
            if _deadline is None:
                self._lock.acquire()
                lock_acquired = True
            else:
                remaining = _deadline - time.monotonic()
                if remaining <= 0 or not self._lock.acquire(timeout=remaining):
                    raise FutureTimeoutError
                lock_acquired = True
            lock_acquired_ns = time.perf_counter_ns() if trace_enabled else 0
            request_id = self._next_request_id()
            if not exact_lane:
                lane_id = self._next_lane_id(lane_id)
            future: Future[ProtocolResponse] = Future()
            if expected_collection_items is None:
                expected_collection_items = _expected_payload_collection_items(opcode, payload)
            client_trace: dict[str, Any] | None = None
            if trace_enabled:
                client_trace = {
                    "request_lock_wait_us": (lock_acquired_ns - submit_started_ns) // 1000
                }
            response_timeout: float | None = getattr(self, "timeout", None)
            self._register_pending_request(
                request_id,
                future,
                expected_collection_items=expected_collection_items,
                client_trace=client_trace,
                expires_at=(
                    None
                    if (
                        not _expire_at_adapter_timeout
                        or _deadline is not None
                        or response_timeout is None
                    )
                    else time.monotonic() + max(0.0, response_timeout)
                ),
                response_identity=ResponseIdentity(
                    lane_id=lane_id,
                    opcode=opcode,
                    request_id=request_id,
                ),
            )
            try:
                trace = self._send(opcode, lane_id, request_id, payload, flags)
                if trace_enabled and client_trace is not None:
                    submit_done_ns = time.perf_counter_ns()
                    if trace is not None:
                        client_trace.update(trace)
                    client_trace["submit_locked_us"] = (submit_done_ns - lock_acquired_ns) // 1000
                    client_trace["submit_total_us"] = (submit_done_ns - submit_started_ns) // 1000
            except Exception:
                self._discard_pending_request(request_id, expected_future=future)
                self._notify_idle_if_needed()
                raise
            return request_id, future
        finally:
            if lock_acquired:
                self._lock.release()
            if had_deadline:
                deadline_context.deadline = previous_deadline
            else:
                del deadline_context.deadline

    def _submit_request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        expected_collection_items: int | None = None,
    ) -> tuple[int, Future[ProtocolResponse]]:
        return self._submit_request(
            opcode,
            lane_id,
            payload,
            flags,
            exact_lane=True,
            expected_collection_items=expected_collection_items,
        )

    def _reader_loop(self, sock: socket.socket | ssl.SSLSocket | None = None) -> None:
        if sock is None:
            sock = self._sock
        try:
            while self._sock is sock and sock is not None:
                response = self._recv_response(sock)
                self._last_activity = time.monotonic()
                if response.request_id == 0:
                    self._enqueue_event(response.value)
                    continue
                future, client_trace = self._take_pending_response(response.request_id, sock)
                if future is not None:
                    response = self._attach_client_trace(
                        response,
                        client_trace,
                    )
                    try_set_future_result(future, response)
                self._notify_idle_if_needed()
        except Exception as exc:
            self._close_transport(exc, mark_closed=False, expected_sock=sock)

    def _fail_pending(
        self,
        exc: BaseException,
        *,
        transport_generation: int | None = None,
    ) -> None:
        pending: list[Future[ProtocolResponse]] = []
        with self._pending_state_lock():
            bindings = getattr(self, "_pending_transport_bindings", None)
            for request_id, future in list(self._pending.items()):
                binding = None if bindings is None else bindings.get(request_id)
                if (
                    transport_generation is not None
                    and binding is not None
                    and binding[0] != transport_generation
                ):
                    continue
                pending.append(future)
                self._pending.pop(request_id, None)
                self._pending_traces.pop(request_id, None)
                _response_item_count_map(self).pop(request_id, None)
                _response_identity_map(self).pop(request_id, None)
                if bindings is not None:
                    bindings.pop(request_id, None)
                self._release_pending_lifecycle_locked(request_id)
        for future in pending:
            try_set_future_exception(future, exc)
        self._notify_idle_if_needed()

    def _recv_matching(self, request_id: int) -> ProtocolResponse:
        while True:
            response = self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                self._enqueue_event(response.value)
                continue
            raise FerricStoreError(
                "protocol response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    def _recv_response(
        self,
        sock: socket.socket | ssl.SSLSocket | None = None,
    ) -> ProtocolResponse:
        read_started_ns = time.perf_counter_ns()
        header = self._recv_exact(_HEADER.size, sock)
        magic, version, flags, lane_id, opcode, request_id, body_len = _HEADER.unpack(header)
        if magic != _MAGIC or version != _RESPONSE_VERSION:
            raise FerricStoreError("invalid protocol response frame header")
        _validate_pending_response_identity(
            self,
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
        )

        self._check_response_size(body_len)
        body = self._recv_exact(body_len, sock)
        chunks = ResponseBodyAccumulator(body)
        final_flags = flags
        budget: ResponseFrameBudget | None = None
        if final_flags & _FLAG_MORE_CHUNKS:
            budget = ResponseFrameBudget(
                max_body_bytes=self.max_response_bytes,
                max_chunks=self.max_response_chunks,
            )
            budget.add_chunk(body_len)
        while final_flags & _FLAG_MORE_CHUNKS:
            next_header = self._recv_exact(_HEADER.size, sock)
            (
                next_magic,
                next_version,
                next_flags,
                next_lane_id,
                next_opcode,
                next_request_id,
                next_body_len,
            ) = _HEADER.unpack(next_header)
            if next_magic != _MAGIC or next_version != _RESPONSE_VERSION:
                raise FerricStoreError("invalid protocol chunk continuation")
            validate_response_identity(
                ResponseIdentity(lane_id, opcode, request_id),
                lane_id=next_lane_id,
                opcode=next_opcode,
                request_id=next_request_id,
                message="invalid protocol chunk continuation",
            )
            if budget is not None:
                budget.add_chunk(next_body_len)
            chunks.append(self._recv_exact(next_body_len, sock))
            final_flags = next_flags

        body = chunks.finish()
        read_done_ns = time.perf_counter_ns()
        return _decode_protocol_response(
            self,
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
            flags=final_flags,
            body=body,
            read_started_ns=read_started_ns,
            read_done_ns=read_done_ns,
        )

    def _recv_exact(self, size: int, sock: socket.socket | ssl.SSLSocket | None = None) -> bytes:
        if sock is None:
            sock = self._require_socket()
        if size == 0:
            return b""
        chunks: ResponseBodyAccumulator | None = None
        received = 0
        while received < size:
            try:
                chunk = sock.recv(size - received)
            except TimeoutError:
                if self._sock is not sock:
                    raise FerricStoreError("protocol connection closed") from None
                continue
            if not chunk:
                raise FerricStoreError("protocol connection closed")
            if chunks is None:
                chunks = ResponseBodyAccumulator(chunk)
            else:
                chunks.append(chunk)
            received += len(chunk)
        assert chunks is not None
        return chunks.finish()

    def _check_response_size(self, size: int) -> None:
        limit = self.max_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_response_bytes")

    def _check_decompressed_response_size(self, size: int) -> None:
        limit = self.max_decompressed_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_decompressed_response_bytes")

    def _require_socket(self) -> socket.socket | ssl.SSLSocket:
        if self._sock is None:
            raise FerricStoreError("protocol connection is closed")
        return self._sock

    def _ensure_connected(self) -> None:
        if self._sock is not None:
            return
        if self._closed:
            raise FerricStoreError("protocol connection is closed")
        if self._connecting:
            raise FerricStoreError("protocol connection is closed")
        deadline = _sync_adapter_deadline(self)
        connect_lock_acquired = False
        try:
            if deadline is None:
                self._connect_lock.acquire()
                connect_lock_acquired = True
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._connect_lock.acquire(timeout=remaining):
                    raise FutureTimeoutError
                connect_lock_acquired = True
            if self._sock is not None:
                return
            if self._closed:
                raise FerricStoreError("protocol connection is closed")
            if self._connecting:
                raise FerricStoreError("protocol connection is closed")
            self._connect()
            if self._closed:
                error = FerricStoreError("protocol connection is closed")
                self._close_transport(error, mark_closed=True)
                raise error
        finally:
            if connect_lock_acquired:
                self._connect_lock.release()

    def _response_value(self, response: ProtocolResponse) -> Any:
        return _response_value(response)

    def _attach_client_trace(
        self, response: ProtocolResponse, client_trace: dict[str, Any] | None
    ) -> ProtocolResponse:
        if not client_trace:
            return response
        trace = dict(response.trace or {})
        client = dict(trace.get("client") or {})
        client.update(client_trace)
        trace["client"] = client
        return replace(response, trace=trace)

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)


__all__ = [
    "ProtocolAdapter",
    "ProtocolPipeline",
]
