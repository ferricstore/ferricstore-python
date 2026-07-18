from __future__ import annotations

import contextlib
import socket
import ssl
import threading
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future
from functools import cached_property, partial
from typing import Any, TypeVar, cast

from ferricstore.config_validation import (
    validate_host,
    validate_optional_thread_wait_seconds,
    validate_port,
)
from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import (
    RetryableResourceSet,
    close_resources_sync,
)
from ferricstore.protocol_common import (
    _notify_event_listeners,
    _validated_route_lane,
)
from ferricstore.protocol_config import ProtocolRuntimeConfig
from ferricstore.protocol_constants import (
    _DEFAULT_MAX_DECODED_COLLECTION_ITEMS,
    _DEFAULT_MAX_DECOMPRESSED_RESPONSE_BYTES,
    _DEFAULT_MAX_EVENT_QUEUE_SIZE,
    _DEFAULT_MAX_RESPONSE_BYTES,
    _DEFAULT_MAX_RESPONSE_CHUNKS,
    _DEFAULT_PROTOCOL_LANES,
    ProtocolResponse,
)
from ferricstore.protocol_framing import ResponseFrameAssembler, ResponseIdentity
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_BATCH_ITEMS,
    DEFAULT_MAX_INFLIGHT_REQUESTS,
    DEFAULT_MAX_PENDING_REQUEST_BYTES,
    PendingRequestBudget,
    SyncDeadlineScheduler,
)
from ferricstore.protocol_negotiation import reset_hello_negotiation
from ferricstore.protocol_sync_pending import SyncPendingRequestRegistry
from ferricstore.protocol_tls import ProtocolTLSContextMixin

_StateAdapter = TypeVar("_StateAdapter", bound="_SyncProtocolStateMixin")


class _SyncProtocolStateMixin(ProtocolTLSContextMixin):
    """Connection lifecycle, event, and pending-request state for the sync adapter."""

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
        max_batch_items: int | None = DEFAULT_MAX_BATCH_ITEMS,
        _fixed_lane_id: int | None = None,
        _is_session_adapter: bool = False,
    ) -> None:
        runtime_config = ProtocolRuntimeConfig.build(
            timeout=timeout,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
            lanes=lanes,
            max_response_bytes=max_response_bytes,
            max_response_chunks=max_response_chunks,
            max_decompressed_response_bytes=max_decompressed_response_bytes,
            max_event_queue_size=max_event_queue_size,
            max_decoded_collection_items=max_decoded_collection_items,
            max_inflight_requests=max_inflight_requests,
            max_pending_request_bytes=max_pending_request_bytes,
            max_batch_items=max_batch_items,
            tls=tls,
        )
        self.host = validate_host(host)
        self.port = validate_port(port)
        self.username = username or "default"
        self.password = password
        self.tls = runtime_config.tls
        self.timeout = runtime_config.timeout
        self.client_name = client_name
        self.compression = compression
        self.lanes = runtime_config.lanes
        self.ssl_context = ssl_context
        self._default_ssl_context: ssl.SSLContext | None = None
        self.heartbeat_interval = runtime_config.heartbeat_interval
        self.heartbeat_timeout = runtime_config.heartbeat_timeout
        self._configured_max_response_bytes = runtime_config.max_response_bytes
        self.max_response_bytes = self._configured_max_response_bytes
        self.max_response_chunks = runtime_config.max_response_chunks
        self._configured_max_decompressed_response_bytes = (
            runtime_config.max_decompressed_response_bytes
        )
        self.max_decompressed_response_bytes = self._configured_max_decompressed_response_bytes
        self.max_event_queue_size = runtime_config.max_event_queue_size
        self.max_decoded_collection_items = runtime_config.max_decoded_collection_items
        self.max_inflight_requests = runtime_config.max_inflight_requests
        self.max_pending_request_bytes = runtime_config.max_pending_request_bytes
        self.max_batch_items = runtime_config.max_batch_items
        self._fixed_lane_id = _validated_route_lane(_fixed_lane_id)
        self.requires_explicit_session = not _is_session_adapter
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
        self._response_frame_assembler = ResponseFrameAssembler(
            max_body_bytes=self.max_response_bytes,
            max_chunks=self.max_response_chunks,
        )
        self._compact_response_codecs: dict[int, str] = {}
        self._auth_required = False
        self._authenticated = False
        self._negotiated_capabilities: Any = None
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
        self._connecting_thread_id: int | None = None
        self._connection_ready = False
        self._retired_sockets = RetryableResourceSet(())
        self._transport_close_lock = threading.Lock()
        self._ensure_connected()

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
        timeout = validate_optional_thread_wait_seconds(
            timeout,
            name="wait_event timeout",
        )
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

    def acquire_session(self: _StateAdapter) -> _StateAdapter:
        """Open a dedicated connection for connection-affine commands."""
        return self._new_session_adapter(fixed_lane_id=None)

    def acquire_session_on_lane(self: _StateAdapter, lane_id: int) -> _StateAdapter:
        """Open a dedicated connection pinned to one topology route lane."""
        return self._new_session_adapter(fixed_lane_id=lane_id)

    def _new_session_adapter(self: _StateAdapter, *, fixed_lane_id: int | None) -> _StateAdapter:
        return type(self)(
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
            max_response_bytes=self._configured_max_response_bytes,
            max_response_chunks=self.max_response_chunks,
            max_decompressed_response_bytes=self._configured_max_decompressed_response_bytes,
            max_event_queue_size=self.max_event_queue_size,
            max_decoded_collection_items=self.max_decoded_collection_items,
            max_inflight_requests=self.max_inflight_requests,
            max_pending_request_bytes=self.max_pending_request_bytes,
            max_batch_items=self.max_batch_items,
            _fixed_lane_id=fixed_lane_id,
            _is_session_adapter=True,
        )

    def _pending_state_lock(self) -> threading.Lock:
        return self._pending_registry.state_lock()

    @cached_property
    def _pending_registry(self) -> SyncPendingRequestRegistry:
        return SyncPendingRequestRegistry(self)

    def _pending_request_budget(self) -> PendingRequestBudget:
        return self._pending_registry.request_budget()

    def _pending_deadline_scheduler(self) -> SyncDeadlineScheduler:
        return self._pending_registry.deadline_scheduler()

    def _release_pending_lifecycle_locked(self, request_id: int) -> None:
        self._pending_registry.release_locked(request_id)

    def _set_pending_request_size(self, request_id: int, size: int) -> None:
        self._pending_registry.set_size(request_id, size)

    def _pending_request_body_limit(self, request_id: int) -> int | None:
        return self._pending_registry.body_limit(request_id)

    def _encode_pending_request_body(
        self,
        request_id: int,
        payload: dict[str, Any] | bytes,
        *,
        compression: str | None = None,
    ) -> tuple[bytes, bool]:
        return self._pending_registry.encode_body(
            request_id,
            payload,
            compression=compression,
        )

    def _expire_pending_request(self, request_id: int) -> None:
        self._pending_registry.expire(request_id)

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
        return self._pending_registry.register(
            request_id,
            future,
            expected_collection_items=expected_collection_items,
            client_trace=client_trace,
            binding=binding,
            expires_at=expires_at,
            response_identity=response_identity,
        )

    def _discard_pending_request(
        self,
        request_id: int,
        *,
        expected_future: Future[ProtocolResponse] | None = None,
    ) -> tuple[Future[ProtocolResponse] | None, dict[str, Any] | None]:
        return self._pending_registry.discard(
            request_id,
            expected_future=expected_future,
        )

    def _take_pending_response(
        self,
        request_id: int,
        sock: socket.socket | ssl.SSLSocket | None,
    ) -> tuple[Future[ProtocolResponse] | None, dict[str, Any] | None]:
        return self._pending_registry.take_response(request_id, sock)

    def _pending_socket(self, request_id: int) -> socket.socket | ssl.SSLSocket:
        return self._pending_registry.pending_socket(request_id)

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
            self._connection_ready = False
        reset_hello_negotiation(self)
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

    # Implemented by ProtocolAdapter; declared here so this mixin remains
    # independently type-checkable without importing the facade back.
    def _connect(self) -> None:
        raise NotImplementedError

    def _ensure_connected(self) -> None:
        raise NotImplementedError

    def _fail_pending(
        self,
        exc: BaseException,
        *,
        transport_generation: int | None = None,
    ) -> None:
        raise NotImplementedError

    def _require_socket(self) -> socket.socket | ssl.SSLSocket:
        raise NotImplementedError


__all__ = ["_SyncProtocolStateMixin"]
