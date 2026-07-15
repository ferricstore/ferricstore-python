from __future__ import annotations

import asyncio
import contextlib
import inspect
import ssl
import time
from collections import deque
from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING, Any, TypeVar, cast

from ferricstore.config_validation import (
    validate_host,
    validate_nonnegative_int,
    validate_optional_thread_wait_seconds,
    validate_port,
)
from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import (
    AsyncCloseCoordinator,
    RetryableResourceSet,
    close_resources_async,
    consume_async_future_exception,
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
from ferricstore.protocol_framing import ResponseIdentity
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_BATCH_ITEMS,
    DEFAULT_MAX_INFLIGHT_REQUESTS,
    DEFAULT_MAX_PENDING_REQUEST_BYTES,
    PendingRequestBudget,
    PendingRequestCapacityError,
)

_StateAdapter = TypeVar("_StateAdapter", bound="_AsyncProtocolStateMixin")


class _AsyncProtocolStateMixin:
    """Connection lifecycle, event, and pending-request state for the async adapter."""

    if TYPE_CHECKING:

        async def _publish_event_error(self, error: BaseException) -> None: ...

        def _fail_pending(self, exc: BaseException) -> None: ...

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
        self.write_drain_bytes = validate_nonnegative_int(
            write_drain_bytes,
            name="write_drain_bytes",
        )
        self.ssl_context = ssl_context
        self.heartbeat_interval = runtime_config.heartbeat_interval
        self.heartbeat_timeout = runtime_config.heartbeat_timeout
        self.max_response_bytes = runtime_config.max_response_bytes
        self.max_response_chunks = runtime_config.max_response_chunks
        self.max_decompressed_response_bytes = runtime_config.max_decompressed_response_bytes
        self.max_event_queue_size = runtime_config.max_event_queue_size
        self.max_decoded_collection_items = runtime_config.max_decoded_collection_items
        self.max_inflight_requests = runtime_config.max_inflight_requests
        self.max_pending_request_bytes = runtime_config.max_pending_request_bytes
        self.max_batch_items = runtime_config.max_batch_items
        self._fixed_lane_id = _validated_route_lane(_fixed_lane_id)
        self.requires_explicit_session = not _is_session_adapter
        self.client = self
        self._connect_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._request_id = 0
        self._lane_cursor = 0
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connection_ready = False
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
        timeout = validate_optional_thread_wait_seconds(
            timeout,
            name="wait_event timeout",
        )
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

    async def acquire_session(self: _StateAdapter) -> _StateAdapter:
        """Create a dedicated lazy connection for connection-affine commands."""
        return self._new_session_adapter(fixed_lane_id=None)

    async def acquire_session_on_lane(
        self: _StateAdapter,
        lane_id: int,
    ) -> _StateAdapter:
        """Create a dedicated lazy connection pinned to one topology route lane."""
        return self._new_session_adapter(fixed_lane_id=lane_id)

    def _new_session_adapter(
        self: _StateAdapter,
        *,
        fixed_lane_id: int | None,
    ) -> _StateAdapter:
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
            max_batch_items=self.max_batch_items,
            _fixed_lane_id=fixed_lane_id,
            _is_session_adapter=True,
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
            self._connection_ready = False
        if expected_reader is not None and self._reader is not expected_reader:
            return
        if expected_writer is not None and self._writer is not expected_writer:
            return
        writer = self._writer
        reader_task = self._reader_task
        heartbeat_task = self._heartbeat_task
        self._reader = None
        self._writer = None
        self._connection_ready = False
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


__all__ = ["_AsyncProtocolStateMixin"]
