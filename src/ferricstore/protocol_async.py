from __future__ import annotations

import asyncio
import contextlib
import inspect
import ssl
import time
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from typing import Any, cast
from urllib.parse import unquote, urlparse

from ferricstore.batch_core import (
    require_batch_items,
    run_async_fanout,
)
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    AsyncCloseCoordinator,
    RetryableResourceSet,
    close_resources_async,
    consume_async_future_exception,
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
    _encode_request_body,
    _flow_wake_payload,
    _normalize_protocol_url_kwargs,
    _notify_event_listeners,
    _protocol_collection_limit,
    _protocol_lane_count,
    _request_body_byte_limit,
    _response_identity_map,
    _response_item_count_map,
    _validate_pending_response_identity,
    _validated_route_lane,
)
from ferricstore.protocol_constants import (
    _ASYNC_ADAPTER_FANOUT_LIMIT,
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
    _OP_PIPELINE,
    _OP_STARTUP,
    _OP_SUBSCRIBE_EVENTS,
    _OPCODES,
    _REQUEST_VERSION,
    _RESPONSE_VERSION,
    _SUPPORTED_SCHEMES,
    _TLS_SCHEMES,
    _USE_ADAPTER_TIMEOUT,
    ProtocolResponse,
)
from ferricstore.protocol_framing import (
    ResponseBodyAccumulator,
    ResponseFrameBudget,
    ResponseIdentity,
    validate_response_identity,
    validated_nonnegative_int,
    validated_optional_nonnegative_int,
    validated_response_chunk_limit,
)
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_INFLIGHT_REQUESTS,
    DEFAULT_MAX_PENDING_REQUEST_BYTES,
    PendingRequestBudget,
    PendingRequestCapacityError,
    validated_pending_limit,
)
from ferricstore.protocol_pipeline_codec import (
    _blocks_forever,
    _expected_command_collection_items,
    _expected_payload_collection_items,
    _pipeline_frame_supported,
)
from ferricstore.protocol_pipelines import AsyncProtocolPipeline
from ferricstore.protocol_responses import (
    _batch_item_value,
    _decode_protocol_response,
    _flow_many_group_values,
    _pipeline_pair_list,
    _response_value,
)


class AsyncProtocolAdapter:
    """FerricStore protocol TCP adapter for the async SDK."""

    client: AsyncProtocolAdapter
    supports_concurrent_fanout = True
    _request_ensures_connection = True

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6388,
        *,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        timeout: float | None = 30.0,
        client_name: str | None = "ferricstore-python-async",
        compression: str = "none",
        lanes: int = _DEFAULT_PROTOCOL_LANES,
        write_drain_bytes: int = 1_048_576,
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
        self.write_drain_bytes = validated_nonnegative_int(
            write_drain_bytes,
            name="write_drain_bytes",
        )
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
        self._connect_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._request_id = 0
        self._lane_cursor = 0
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[ProtocolResponse]] = {}
        self._pending_budget = PendingRequestBudget(
            max_requests=self.max_inflight_requests,
            max_bytes=self.max_pending_request_bytes,
        )
        self._pending_traces: dict[int, dict[str, Any]] = {}
        self._pending_response_item_counts: dict[int, int] = {}
        self._pending_response_identities: dict[int, ResponseIdentity] = {}
        self._events: deque[Any] = deque()
        self._events_cv = asyncio.Condition()
        self._event_error: BaseException | None = None
        self._event_listeners: list[Callable[[], None]] = []
        self._idle_listeners: list[Callable[[], None]] = []
        self._flow_wake_subscriptions: list[dict[str, Any]] = []
        self._queued_write_bytes = 0
        self._last_activity = time.monotonic()
        self._closed = False
        self._heartbeat_pause_count = 0
        self._close_coordinator = AsyncCloseCoordinator()
        self._retired_writers = RetryableResourceSet(())
        self._transport_close_lock = asyncio.Lock()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncProtocolAdapter:
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
        return list(self._events)

    @property
    def backpressure_scope(self) -> tuple[str, bool, str, int, str]:
        return ("protocol", self.tls, self.host.lower(), self.port, self.username)

    @property
    def pending_request_count(self) -> int:
        return self._pending_request_budget().count

    @property
    def pending_request_bytes(self) -> int:
        return self._pending_request_budget().total_bytes

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

    def _reserve_pending_request(self, request_id: int) -> None:
        try:
            self._pending_request_budget().reserve(request_id)
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc

    def _set_pending_request_size(self, request_id: int, size: int) -> None:
        self._pending_request_budget().set_size(request_id, size)

    def _release_pending_request(self, request_id: int) -> None:
        self._pending_request_budget().release(request_id)

    def add_event_listener(self, listener: Callable[[], None]) -> None:
        if listener not in self._event_listeners:
            self._event_listeners.append(listener)

    def remove_event_listener(self, listener: Callable[[], None]) -> None:
        with contextlib.suppress(ValueError):
            self._event_listeners.remove(listener)

    def add_idle_listener(self, listener: Callable[[], None]) -> None:
        if listener not in self._idle_listeners:
            self._idle_listeners.append(listener)

    def remove_idle_listener(self, listener: Callable[[], None]) -> None:
        with contextlib.suppress(ValueError):
            self._idle_listeners.remove(listener)

    async def _enqueue_event(self, value: Any) -> None:
        limit = self.max_event_queue_size
        error: FerricStoreError | None = None
        async with self._events_cv:
            if limit is not None and len(self._events) >= limit:
                error = FerricStoreError("protocol event queue exceeds max_event_queue_size")
                self._event_error = error
            else:
                self._events.append(value)
            self._events_cv.notify_all()
        _notify_event_listeners(list(self._event_listeners))
        if error is not None:
            raise error

    async def wait_event(self, timeout: float | None = None) -> Any | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        async with self._events_cv:
            while not self._events and self._event_error is None:
                if timeout == 0.0:
                    return None
                try:
                    if deadline is None:
                        await self._events_cv.wait()
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return None
                        await asyncio.wait_for(self._events_cv.wait(), remaining)
                except asyncio.TimeoutError:
                    return None
            if self._events:
                return self._events.popleft()
            if self._event_error is not None:
                raise self._event_error
            return None

    def pause_heartbeat(self) -> None:
        self._heartbeat_pause_count += 1

    def resume_heartbeat(self) -> None:
        self._heartbeat_pause_count = max(self._heartbeat_pause_count - 1, 0)

    async def close(self) -> None:
        coordinator = getattr(self, "_close_coordinator", None)
        if coordinator is None:
            coordinator = AsyncCloseCoordinator()
            self._close_coordinator = coordinator
        await coordinator.run(self._close_once)

    async def _close_once(self) -> None:
        await self._close_transport(mark_closed=True)

    async def invalidate(self) -> None:
        """Drop connection-local state while keeping this adapter reusable."""
        await self._close_transport(
            FerricStoreError("protocol connection was invalidated"),
            mark_closed=False,
        )

    async def acquire_session(self) -> AsyncProtocolAdapter:
        """Create a dedicated lazy connection for connection-affine commands."""
        return self._new_session_adapter(fixed_lane_id=None)

    async def acquire_session_on_lane(self, lane_id: int) -> AsyncProtocolAdapter:
        """Create a dedicated lazy connection pinned to one topology route lane."""
        return self._new_session_adapter(fixed_lane_id=lane_id)

    def _new_session_adapter(self, *, fixed_lane_id: int | None) -> AsyncProtocolAdapter:
        return AsyncProtocolAdapter(
            self.host,
            self.port,
            username=self.username,
            password=self.password,
            tls=self.tls,
            timeout=self.timeout,
            client_name=self.client_name,
            compression=self.compression,
            lanes=self.lanes,
            write_drain_bytes=self.write_drain_bytes,
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

    async def _close_transport(
        self,
        exc: BaseException | None = None,
        *,
        mark_closed: bool = False,
        expected_reader: asyncio.StreamReader | None = None,
        expected_writer: asyncio.StreamWriter | None = None,
    ) -> None:
        if mark_closed:
            self._closed = True
        if expected_reader is not None and self._reader is not expected_reader:
            return
        if expected_writer is not None and self._writer is not expected_writer:
            return
        writer = self._writer
        reader_task = self._reader_task
        heartbeat_task = self._heartbeat_task
        self._reader = None
        self._writer = None
        self._reader_task = None
        self._heartbeat_task = None
        self._queued_write_bytes = 0
        retired_writers = getattr(self, "_retired_writers", None)
        if retired_writers is None:
            retired_writers = RetryableResourceSet(())
            self._retired_writers = retired_writers
        if writer is not None:
            retired_writers.add(writer)
        current_task = asyncio.current_task()
        if heartbeat_task is not None and heartbeat_task is not current_task:
            heartbeat_task.cancel()
        if reader_task is not None and reader_task is not current_task:
            reader_task.cancel()
        close_error = exc or FerricStoreError("protocol connection is closed")
        self._fail_pending(close_error)

        cleanup_task = asyncio.create_task(
            self._finish_transport_lifecycle(
                reader_task,
                heartbeat_task,
                current_task,
                close_error,
                notify_error=exc is not None or mark_closed,
            )
        )
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            cleanup_task.add_done_callback(consume_async_future_exception)
            raise

    async def _finish_transport_lifecycle(
        self,
        reader_task: asyncio.Task[None] | None,
        heartbeat_task: asyncio.Task[None] | None,
        caller_task: asyncio.Task[Any] | None,
        close_error: BaseException,
        *,
        notify_error: bool,
    ) -> None:
        lifecycle: list[Callable[[], Any]] = []
        if notify_error:
            lifecycle.append(partial(self._publish_event_error, close_error))
        lifecycle.append(
            partial(
                self._finish_transport_close,
                reader_task,
                heartbeat_task,
                caller_task,
            )
        )
        await close_resources_async(lifecycle, max_concurrency=1)

    async def _finish_transport_close(
        self,
        reader_task: asyncio.Task[None] | None,
        heartbeat_task: asyncio.Task[None] | None,
        caller_task: asyncio.Task[Any] | None,
    ) -> None:
        retired_writers = getattr(self, "_retired_writers", None)
        if retired_writers is None:
            retired_writers = RetryableResourceSet(())
            self._retired_writers = retired_writers

        async def close_writer(resource: Any) -> None:
            if not retired_writers.contains(resource):
                return
            resource.close()
            wait_closed = getattr(resource, "wait_closed", None)
            if callable(wait_closed):
                result = wait_closed()
                if inspect.isawaitable(result):
                    await result
            retired_writers.complete(resource)

        async def join_task(task: asyncio.Task[Any]) -> None:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        cleanup: list[Callable[[], Any]] = [
            partial(close_writer, resource) for resource in retired_writers.snapshot()
        ]
        if reader_task is not None and reader_task is not caller_task:
            cleanup.append(partial(join_task, reader_task))
        if heartbeat_task is not None and heartbeat_task is not caller_task:
            cleanup.append(partial(join_task, heartbeat_task))
        transport_close_lock = getattr(self, "_transport_close_lock", None)
        if transport_close_lock is None:
            transport_close_lock = asyncio.Lock()
            self._transport_close_lock = transport_close_lock
        try:
            async with transport_close_lock:
                await close_resources_async(cleanup)
        finally:
            self._notify_idle_if_needed()

    def _notify_idle_if_needed(self) -> None:
        connect_lock = getattr(self, "_connect_lock", None)
        if getattr(self, "_pending", None) or (connect_lock is not None and connect_lock.locked()):
            return
        idle_listeners = getattr(self, "_idle_listeners", ())
        if idle_listeners:
            _notify_event_listeners(list(idle_listeners))

    def pipeline(self, transaction: bool = False) -> AsyncProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return AsyncProtocolPipeline(self)

    async def execute_command(self, *args: Any) -> Any:
        command = build_protocol_command(*args)
        expected_collection_items = _expected_command_collection_items(args)
        if _blocks_forever(args):
            response = await self._request(
                command.opcode,
                command.lane_id,
                command.payload,
                command.flags,
                timeout=None,
                expected_collection_items=expected_collection_items,
            )
        else:
            response = await self._request(
                command.opcode,
                command.lane_id,
                command.payload,
                command.flags,
                expected_collection_items=expected_collection_items,
            )
        return self._response_value(response)

    async def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        """Execute a routed command on the exact topology lane."""
        command = build_protocol_command(*args)
        expected_collection_items = _expected_command_collection_items(args)
        if _blocks_forever(args):
            response = await self._request_on_lane(
                command.opcode,
                lane_id,
                command.payload,
                command.flags,
                timeout=None,
                expected_collection_items=expected_collection_items,
            )
        else:
            response = await self._request_on_lane(
                command.opcode,
                lane_id,
                command.payload,
                command.flags,
                expected_collection_items=expected_collection_items,
            )
        return self._response_value(response)

    async def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        command = build_protocol_command(*args)
        response = await self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            expected_collection_items=_expected_command_collection_items(args),
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    async def execute_command_with_trace_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> dict[str, Any]:
        command = build_protocol_command(*args)
        response = await self._request_on_lane(
            command.opcode,
            lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            expected_collection_items=_expected_command_collection_items(args),
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    async def subscribe_flow_wake(
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
        response = await self._request(
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
        self._flow_wake_subscriptions[:] = [payload]

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=None,
            concurrent_flow_many=True,
        )

    async def execute_batch_ordered(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=None,
            concurrent_flow_many=False,
        )

    async def execute_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=lane_id,
            concurrent_flow_many=True,
        )

    async def execute_batch_ordered_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=lane_id,
            concurrent_flow_many=False,
        )

    async def _execute_batch(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
        concurrent_flow_many: bool,
    ) -> list[Any]:
        if not commands:
            return []

        lane_id = 1 if routed_lane is None else routed_lane
        request = self._request if routed_lane is None else self._request_on_lane

        flow_many_payloads = _compact_flow_many_payloads_from_raw(commands)
        if flow_many_payloads is not None:

            async def request_group(group: tuple[int, bytes, int]) -> list[Any]:
                opcode, payload, count = group
                response = await request(
                    opcode,
                    lane_id,
                    payload,
                    _FLAG_CUSTOM_PAYLOAD,
                )
                group_values = self._response_value(response)
                return _flow_many_group_values(group_values, count)

            groups = await run_async_fanout(
                flow_many_payloads,
                request_group,
                concurrent=concurrent_flow_many,
                max_concurrency=_ASYNC_ADAPTER_FANOUT_LIMIT,
            )
            return [value for group in groups for value in group]

        compact_payload = _compact_pipeline_payload_from_raw(commands, values_only=True)
        if compact_payload is not None:
            response = await request(
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
                return [await self.execute_command(*command) for command in commands]
            values = []
            for raw_command, command in zip(commands, protocol_commands, strict=True):
                command_lane = command.lane_id if routed_lane is None else routed_lane
                if _blocks_forever(raw_command):
                    response = await request(
                        command.opcode,
                        command_lane,
                        command.payload,
                        command.flags,
                        timeout=None,
                    )
                else:
                    response = await request(
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
        response = await request(
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

    async def _ensure_connected(self) -> None:
        try:
            await self._ensure_connected_without_idle_notification()
        finally:
            self._notify_idle_if_needed()

    async def _ensure_connected_without_idle_notification(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return
        if getattr(self, "_closed", False):
            raise FerricStoreError("protocol connection is closed")

        connect = self._ensure_connected_without_timeout()
        if self.timeout is None:
            await connect
            return
        try:
            await asyncio.wait_for(connect, self.timeout)
        except asyncio.TimeoutError as exc:
            error = FerricStoreError("protocol connection timed out")
            await self._publish_event_error(error)
            raise error from exc

    async def _publish_event_error(self, error: BaseException) -> None:
        async with self._events_cv:
            self._event_error = error
            self._events_cv.notify_all()
        _notify_event_listeners(list(self._event_listeners))

    async def _ensure_connected_without_timeout(self) -> None:
        async with self._connect_lock:
            if self._writer is not None and not self._writer.is_closing():
                return
            if self._closed:
                raise FerricStoreError("protocol connection is closed")
            if self._writer is not None:
                await self._close_transport(mark_closed=False)

            reader: asyncio.StreamReader | None = None
            writer: asyncio.StreamWriter | None = None
            try:
                context = (self.ssl_context or ssl.create_default_context()) if self.tls else None
                reader, writer = await asyncio.open_connection(
                    self.host,
                    self.port,
                    ssl=context,
                    server_hostname=self.host if self.tls else None,
                )
                if self._closed:
                    writer.close()
                    with contextlib.suppress(Exception):
                        await writer.wait_closed()
                    raise FerricStoreError("protocol connection is closed")
                self._reader, self._writer = reader, writer
                self._reader_task = asyncio.create_task(self._reader_loop(reader, writer))

                startup: dict[str, Any] = {
                    "compression": self.compression,
                    "compact_flow_responses": True,
                }
                if self.client_name is not None:
                    startup["client_name"] = self.client_name
                    startup["driver_name"] = self.client_name

                self._response_value(await self._request(_OP_STARTUP, 0, startup))

                if self.password is not None:
                    self._response_value(
                        await self._request(
                            _OP_AUTH,
                            0,
                            {"username": self.username, "password": self.password},
                        )
                    )
                for payload in list(self._flow_wake_subscriptions):
                    self._response_value(await self._request(_OP_SUBSCRIBE_EVENTS, 0, payload))
                if self._closed:
                    raise FerricStoreError("protocol connection is closed")
                async with self._events_cv:
                    self._event_error = None
                self._start_heartbeat()
            except BaseException as exc:
                published_error = (
                    FerricStoreError("protocol connection attempt cancelled", raw=exc)
                    if isinstance(exc, asyncio.CancelledError)
                    else exc
                )
                await self._close_transport(
                    published_error,
                    mark_closed=False,
                    expected_reader=reader,
                    expected_writer=writer,
                )
                raise

    def _start_heartbeat(self) -> None:
        if self.heartbeat_interval is None or self.heartbeat_interval <= 0:
            return
        old_task = self._heartbeat_task
        if old_task is not None and old_task is not asyncio.current_task() and not old_task.done():
            old_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(self._writer))

    async def _heartbeat_loop(self, writer: asyncio.StreamWriter | None = None) -> None:
        if writer is None:
            writer = self._writer
        interval = float(self.heartbeat_interval or 0)
        timeout = self.heartbeat_timeout
        try:
            while interval > 0 and self._writer is writer and writer is not None:
                await asyncio.sleep(interval)
                if self._writer is not writer:
                    return
                if getattr(self, "_heartbeat_pause_count", 0) > 0:
                    continue
                if time.monotonic() - self._last_activity < interval:
                    continue
                try:
                    request = self._request(_OPCODES["PING"], 0, {})
                    if timeout is None:
                        await request
                    else:
                        await asyncio.wait_for(request, timeout=timeout)
                except Exception as exc:
                    await self._close_transport(
                        FerricStoreError("protocol heartbeat failed", raw=exc),
                        mark_closed=False,
                        expected_writer=writer,
                    )
                    return
        except asyncio.CancelledError:
            raise

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

    async def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> dict[str, Any] | None:
        writer = self._require_writer()
        trace_enabled = bool(extra_flags & _FLAG_TRACE)
        encode_started_ns = time.perf_counter_ns() if trace_enabled else 0
        pending_budget = self._pending_request_budget()
        body, compressed = _encode_request_body(
            payload,
            compression=self.compression,
            max_body_bytes=_request_body_byte_limit(pending_budget, request_id),
            pending_limit=getattr(
                self,
                "max_pending_request_bytes",
                pending_budget.max_bytes,
            ),
        )
        flags = extra_flags
        if compressed:
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        self._set_pending_request_size(request_id, len(header) + len(body))
        writer.write(header)
        if body:
            writer.write(body)
        self._queued_write_bytes += len(header) + len(body)
        self._last_activity = time.monotonic()
        encode_done_ns = time.perf_counter_ns() if trace_enabled else 0
        write_started_ns = encode_done_ns
        if self.write_drain_bytes == 0 or self._queued_write_bytes >= self.write_drain_bytes:
            self._queued_write_bytes = 0
            await writer.drain()
        if not trace_enabled:
            return None
        write_done_ns = time.perf_counter_ns()
        return {
            "encode_us": (encode_done_ns - encode_started_ns) // 1000,
            "socket_write_us": (write_done_ns - write_started_ns) // 1000,
        }

    async def _request(
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
        request = self._request_without_timeout(
            opcode,
            lane_id,
            payload,
            flags,
            exact_lane=exact_lane,
            expected_collection_items=expected_collection_items,
        )
        effective_timeout = self.timeout if timeout is _USE_ADAPTER_TIMEOUT else timeout
        effective_timeout = cast(float | None, effective_timeout)
        if effective_timeout is None:
            return await request
        try:
            return await asyncio.wait_for(request, effective_timeout)
        except asyncio.TimeoutError as exc:
            raise FerricStoreError("protocol request timed out") from exc

    async def _request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse:
        return await self._request(
            opcode,
            lane_id,
            payload,
            flags,
            timeout=timeout,
            exact_lane=True,
            expected_collection_items=expected_collection_items,
        )

    async def _request_without_timeout(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse:
        if hasattr(self, "_connect_lock"):
            await self._ensure_connected()
        loop = asyncio.get_running_loop()
        request_id: int | None = None
        future: asyncio.Future[ProtocolResponse] | None = None
        try:
            async with self._write_lock:
                request_id = self._next_request_id()
                if not exact_lane:
                    lane_id = self._next_lane_id(lane_id)
                future = loop.create_future()
                self._reserve_pending_request(request_id)
                self._pending[request_id] = future
                _response_identity_map(self)[request_id] = ResponseIdentity(
                    lane_id=lane_id,
                    opcode=opcode,
                    request_id=request_id,
                )
                if expected_collection_items is None:
                    expected_collection_items = _expected_payload_collection_items(opcode, payload)
                if expected_collection_items is not None:
                    _response_item_count_map(self)[request_id] = expected_collection_items
                trace_enabled = bool(flags & _FLAG_TRACE)
                if trace_enabled:
                    self._pending_traces[request_id] = {}
                writer = getattr(self, "_writer", None)
                try:
                    trace = await self._send(opcode, lane_id, request_id, payload, flags)
                except PendingRequestCapacityError as capacity_error:
                    raise FerricStoreError(str(capacity_error)) from capacity_error
                except BaseException as write_error:
                    cleanup_error: BaseException | None = None
                    if writer is not None:
                        try:
                            await self._close_transport(
                                FerricStoreError("protocol write failed", raw=write_error),
                                mark_closed=False,
                                expected_writer=writer,
                            )
                        except BaseException as exc:
                            cleanup_error = exc
                    if future.done() and not future.cancelled():
                        future.exception()
                    if cleanup_error is not None:
                        raise_primary_with_cleanup(
                            write_error,
                            write_error.__traceback__,
                            cleanup_error,
                        )
                    raise
                if trace_enabled and trace is not None:
                    pending_trace = self._pending_traces.get(request_id)
                    if pending_trace is not None:
                        pending_trace.update(trace)
            return await future
        finally:
            if request_id is not None:
                self._pending.pop(request_id, None)
                self._pending_traces.pop(request_id, None)
                _response_item_count_map(self).pop(request_id, None)
                _response_identity_map(self).pop(request_id, None)
                self._release_pending_request(request_id)
                self._notify_idle_if_needed()

    async def _reader_loop(
        self,
        reader: asyncio.StreamReader | None = None,
        writer: asyncio.StreamWriter | None = None,
    ) -> None:
        if reader is None:
            reader = self._reader
        if writer is None:
            writer = self._writer
        try:
            while self._reader is reader and reader is not None:
                response = await self._recv_response(reader)
                self._last_activity = time.monotonic()
                if response.request_id == 0:
                    await self._enqueue_event(response.value)
                    continue
                future = self._pending.pop(response.request_id, None)
                client_trace = self._pending_traces.pop(response.request_id, None)
                _response_identity_map(self).pop(response.request_id, None)
                self._release_pending_request(response.request_id)
                if future is not None:
                    response = self._attach_client_trace(
                        response,
                        client_trace,
                    )
                    try_set_future_result(future, response)
                self._notify_idle_if_needed()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._close_transport(
                exc,
                mark_closed=False,
                expected_reader=reader,
                expected_writer=writer,
            )

    def _fail_pending(self, exc: BaseException) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        self._pending_traces.clear()
        _response_item_count_map(self).clear()
        _response_identity_map(self).clear()
        self._pending_request_budget().clear()
        for future in pending:
            try_set_future_exception(future, exc)
        self._notify_idle_if_needed()

    async def _recv_matching(self, request_id: int) -> ProtocolResponse:
        while True:
            response = await self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                await self._enqueue_event(response.value)
                continue
            raise FerricStoreError(
                "protocol response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    async def _recv_response(self, reader: asyncio.StreamReader | None = None) -> ProtocolResponse:
        read_started_ns = time.perf_counter_ns()
        header = await self._recv_exact(_HEADER.size, reader)
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
        body = await self._recv_exact(body_len, reader)
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
            next_header = await self._recv_exact(_HEADER.size, reader)
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
            chunks.append(await self._recv_exact(next_body_len, reader))
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

    async def _recv_exact(self, size: int, reader: asyncio.StreamReader | None = None) -> bytes:
        if reader is None:
            reader = self._require_reader()
        try:
            return await reader.readexactly(size)
        except asyncio.IncompleteReadError as exc:
            raise FerricStoreError("protocol connection closed") from exc

    def _check_response_size(self, size: int) -> None:
        limit = self.max_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_response_bytes")

    def _check_decompressed_response_size(self, size: int) -> None:
        limit = self.max_decompressed_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_decompressed_response_bytes")

    def _require_reader(self) -> asyncio.StreamReader:
        if self._reader is None:
            raise FerricStoreError("protocol connection is closed")
        return self._reader

    def _require_writer(self) -> asyncio.StreamWriter:
        if self._writer is None:
            raise FerricStoreError("protocol connection is closed")
        return self._writer

    def _response_value(self, response: ProtocolResponse) -> Any:
        return _response_value(response)

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)

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


__all__ = [
    "AsyncProtocolAdapter",
    "AsyncProtocolPipeline",
]
