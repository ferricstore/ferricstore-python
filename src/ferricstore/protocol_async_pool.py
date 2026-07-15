from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections.abc import Callable, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from ferricstore.async_ownership import rollback_async_resources
from ferricstore.batch_core import (
    run_async_fanout,
)
from ferricstore.config_validation import validate_optional_thread_wait_seconds
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    AsyncCloseCoordinator,
    RetryableResourceSet,
    await_cancellation_safe,
    close_resources_async,
    consume_async_future_exception,
    register_event_listener_transactionally,
)
from ferricstore.protocol_async import (
    AsyncProtocolAdapter,
    AsyncProtocolPipeline,
)
from ferricstore.protocol_common import (
    _async_adapter_outer_fanout_limit,
    _close_adapter_async,
    _notify_event_listeners,
    _pool_topology_options,
    _protocol_connection_count,
)
from ferricstore.protocol_constants import (
    _ASYNC_ADAPTER_FANOUT_LIMIT,
)
from ferricstore.protocol_planning import PreparedCommand

if TYPE_CHECKING:
    from ferricstore.protocol_async_topology import AsyncTopologyProtocolAdapterPool


class AsyncProtocolAdapterPool:
    """Small async protocol socket pool; each socket still multiplexes request lanes."""

    client: AsyncProtocolAdapterPool
    requires_explicit_session = True
    supports_concurrent_fanout = True

    def __init__(self, adapters: list[AsyncProtocolAdapter]) -> None:
        if not adapters:
            raise ValueError("AsyncProtocolAdapterPool requires at least one adapter")
        self.adapters = adapters
        self.client = self
        self._condition = asyncio.Condition()
        self._cursor = 0
        self._leased: set[int] = set()
        self._active = [0] * len(adapters)
        self._closed = False
        self._session_waiters = 0
        self._broadcasting = False
        self._event_ready = asyncio.Event()
        self._event_listener = self._event_ready.set
        self._idle_listeners: list[Callable[[], None]] = []
        self._event_poll_fallback = False
        self._close_coordinator = AsyncCloseCoordinator()
        self._close_adapters = RetryableResourceSet(adapters)
        self._event_poll_fallback = register_event_listener_transactionally(
            adapters,
            self._event_listener,
        )

    @classmethod
    def from_url(
        cls, url: str, **kwargs: Any
    ) -> AsyncProtocolAdapterPool | AsyncProtocolAdapter | AsyncTopologyProtocolAdapterPool:
        seeds, ha_routing = _pool_topology_options(kwargs)
        if seeds is not None or ha_routing:
            urls = [url]
            if seeds is not None:
                urls.extend(list(seeds))
            return cls.from_urls(urls, **kwargs)

        max_connections = _protocol_connection_count(kwargs.pop("max_connections", 1))
        if max_connections <= 1:
            return AsyncProtocolAdapter.from_url(url, **kwargs)
        adapters: list[AsyncProtocolAdapter] = []
        try:
            adapters.extend(
                AsyncProtocolAdapter.from_url(url, **kwargs) for _ in range(max_connections)
            )
            return cls(adapters)
        except BaseException:
            rollback_async_resources(adapters)
            raise

    @classmethod
    def from_urls(cls, urls: Sequence[str], **kwargs: Any) -> AsyncTopologyProtocolAdapterPool:
        from ferricstore.protocol_async_topology import AsyncTopologyProtocolAdapterPool

        return AsyncTopologyProtocolAdapterPool(list(urls), **kwargs)

    @property
    def events(self) -> list[Any]:
        events: list[Any] = []
        for index, adapter in enumerate(self.adapters):
            if index in self._leased:
                continue
            events.extend(adapter.events)
        return events

    @property
    def backpressure_scope(self) -> Any:
        scopes = tuple(
            getattr(adapter, "backpressure_scope", id(adapter)) for adapter in self.adapters
        )
        return ("async-protocol-pool", scopes)

    def add_event_listener(self, listener: Callable[[], None]) -> None:
        for adapter in self.adapters:
            add_listener = getattr(adapter, "add_event_listener", None)
            if callable(add_listener):
                add_listener(listener)

    def remove_event_listener(self, listener: Callable[[], None]) -> None:
        for adapter in self.adapters:
            remove_listener = getattr(adapter, "remove_event_listener", None)
            if callable(remove_listener):
                remove_listener(listener)

    def add_idle_listener(self, listener: Callable[[], None]) -> None:
        if listener not in self._idle_listeners:
            self._idle_listeners.append(listener)

    def remove_idle_listener(self, listener: Callable[[], None]) -> None:
        with contextlib.suppress(ValueError):
            self._idle_listeners.remove(listener)

    def _idle_listeners_if_idle(self) -> list[Callable[[], None]]:
        if any(self._active) or self._leased or self._broadcasting:
            return []
        return list(self._idle_listeners)

    async def wait_event(self, timeout: float | None = None) -> Any | None:
        timeout = validate_optional_thread_wait_seconds(
            timeout,
            name="wait_event timeout",
        )
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._closed:
                raise FerricStoreError("protocol pool is closed")
            event = await self._take_available_event()
            if event is not None:
                return event
            if timeout == 0.0:
                return None
            self._event_ready.clear()
            if self._closed:
                raise FerricStoreError("protocol pool is closed")
            event = await self._take_available_event()
            if event is not None:
                return event
            wait_for: float | None = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                wait_for = remaining
            if self._event_poll_fallback:
                wait_for = 0.05 if wait_for is None else min(wait_for, 0.05)
            try:
                if wait_for is None:
                    await self._event_ready.wait()
                else:
                    await asyncio.wait_for(self._event_ready.wait(), wait_for)
            except asyncio.TimeoutError:
                if deadline is not None and time.monotonic() >= deadline:
                    return None

    async def _take_available_event(self) -> Any | None:
        checked: set[int] = set()
        while True:
            reserved = await self._reserve_event_adapter(checked)
            if reserved is None:
                return None
            index, adapter = reserved
            checked.add(index)
            try:
                event = await adapter.wait_event(timeout=0.0)
            finally:
                await self._release_adapter(index)
            if event is not None:
                return event

    async def _reserve_event_adapter(
        self,
        excluded: set[int],
    ) -> tuple[int, AsyncProtocolAdapter] | None:
        async with self._condition:
            if self._closed or self._session_waiters:
                return None
            for index, adapter in enumerate(self.adapters):
                if index in excluded or index in self._leased:
                    continue
                self._active[index] += 1
                return index, adapter
        return None

    async def close(self) -> None:
        await self._close_coordinator.run(self._close_once)

    async def _close_once(self) -> None:
        async with self._condition:
            if not self._closed:
                self._closed = True
                self._condition.notify_all()
        self._event_ready.set()
        pending = self._close_adapters.snapshot()

        async def close_adapter(adapter: Any) -> None:
            await _close_adapter_async(adapter, self._event_listener)
            self._close_adapters.complete(adapter)

        await close_resources_async(
            [partial(close_adapter, adapter) for adapter in pending],
            max_concurrency=_async_adapter_outer_fanout_limit(pending),
        )

    async def _ensure_connected(self) -> None:
        if self._closed:
            raise FerricStoreError("protocol pool is closed")

        async def connect(adapter: AsyncProtocolAdapter) -> None:
            await adapter._ensure_connected()

        await run_async_fanout(self.adapters, connect, concurrent=True)

    def pipeline(self, transaction: bool = False) -> AsyncProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return AsyncProtocolPipeline(self)

    async def execute_command(self, *args: Any) -> Any:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_command(*args)
        finally:
            await self._release_adapter(index)

    async def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_command_on_lane(args, lane_id)
        finally:
            await self._release_adapter(index)

    async def execute_prepared_command_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> Any:
        index, adapter = await self._acquire_adapter()
        try:
            execute_prepared = getattr(adapter, "execute_prepared_command_on_lane", None)
            if callable(execute_prepared):
                return await execute_prepared(prepared, lane_id)
            return await adapter.execute_command_on_lane(prepared.args, lane_id)
        finally:
            await self._release_adapter(index)

    async def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_command_with_trace(*args)
        finally:
            await self._release_adapter(index)

    async def execute_command_with_trace_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> dict[str, Any]:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_command_with_trace_on_lane(args, lane_id)
        finally:
            await self._release_adapter(index)

    async def execute_prepared_command_with_trace_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> dict[str, Any]:
        index, adapter = await self._acquire_adapter()
        try:
            execute_prepared = getattr(
                adapter,
                "execute_prepared_command_with_trace_on_lane",
                None,
            )
            if callable(execute_prepared):
                return cast(dict[str, Any], await execute_prepared(prepared, lane_id))
            return await adapter.execute_command_with_trace_on_lane(prepared.args, lane_id)
        finally:
            await self._release_adapter(index)

    async def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> Any:
        async with self._condition:
            while (self._leased or self._broadcasting) and not self._closed:
                await self._condition.wait()
            if self._closed:
                raise FerricStoreError("protocol pool is closed")
            self._broadcasting = True
            adapters = list(self.adapters)
        try:

            async def subscribe(adapter: AsyncProtocolAdapter) -> Any:
                return await adapter.subscribe_flow_wake(*args, **kwargs)

            replies = await run_async_fanout(
                adapters,
                subscribe,
                concurrent=True,
                max_concurrency=_ASYNC_ADAPTER_FANOUT_LIMIT,
            )
            return replies[0] if len(replies) == 1 else replies
        finally:
            listeners: list[Callable[[], None]]
            async with self._condition:
                self._broadcasting = False
                listeners = self._idle_listeners_if_idle()
                self._condition.notify_all()
            _notify_event_listeners(listeners)

    def register_flow_wake_subscription(self, *args: Any, **kwargs: Any) -> None:
        """Persist the reconnect filter on every socket in this endpoint pool."""
        for adapter in self.adapters:
            adapter.register_flow_wake_subscription(*args, **kwargs)

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_batch(commands)
        finally:
            await self._release_adapter(index)

    async def execute_batch_ordered(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_batch_ordered(commands)
        finally:
            await self._release_adapter(index)

    async def execute_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_batch_on_lane(commands, lane_id)
        finally:
            await self._release_adapter(index)

    async def execute_batch_ordered_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.execute_batch_ordered_on_lane(commands, lane_id)
        finally:
            await self._release_adapter(index)

    async def execute_prepared_batch_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
        *,
        ordered: bool = False,
    ) -> list[Any]:
        index, adapter = await self._acquire_adapter()
        try:
            execute_prepared = getattr(adapter, "execute_prepared_batch_on_lane", None)
            if callable(execute_prepared):
                return cast(
                    list[Any],
                    await execute_prepared(
                        prepared_commands,
                        lane_id,
                        ordered=ordered,
                    ),
                )
            raw_commands = [prepared.args for prepared in prepared_commands]
            if ordered:
                return await adapter.execute_batch_ordered_on_lane(raw_commands, lane_id)
            return await adapter.execute_batch_on_lane(raw_commands, lane_id)
        finally:
            await self._release_adapter(index)

    async def acquire_session(self) -> _AsyncProtocolAdapterSession:
        async with self._condition:
            if self._closed:
                raise FerricStoreError("protocol pool is closed")
            self._session_waiters += 1
            try:
                while True:
                    if self._closed:
                        raise FerricStoreError("protocol pool is closed")
                    if not self._broadcasting:
                        for offset in range(len(self.adapters)):
                            index = (self._cursor + offset) % len(self.adapters)
                            if index not in self._leased and self._active[index] == 0:
                                self._leased.add(index)
                                self._cursor = index + 1
                                return _AsyncProtocolAdapterSession(
                                    self, index, self.adapters[index]
                                )
                    await self._condition.wait()
            finally:
                self._session_waiters -= 1
                if self._session_waiters == 0:
                    self._event_ready.set()
                self._condition.notify_all()

    async def acquire_dedicated_session(self) -> AsyncProtocolAdapter:
        """Create an independent connection without retaining a pooled adapter lease."""
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.acquire_session()
        finally:
            await self._release_adapter(index)

    async def acquire_session_on_lane(self, lane_id: int) -> AsyncProtocolAdapter:
        index, adapter = await self._acquire_adapter()
        try:
            return await adapter.acquire_session_on_lane(lane_id)
        finally:
            await self._release_adapter(index)

    async def acquire_session_for_key(self, _key: str | bytes) -> _AsyncProtocolAdapterSession:
        return await self.acquire_session()

    async def _acquire_adapter(self) -> tuple[int, AsyncProtocolAdapter]:
        async with self._condition:
            while True:
                if self._closed:
                    raise FerricStoreError("protocol pool is closed")
                if self._session_waiters == 0:
                    for offset in range(len(self.adapters)):
                        index = (self._cursor + offset) % len(self.adapters)
                        if index not in self._leased:
                            self._active[index] += 1
                            self._cursor = index + 1
                            return index, self.adapters[index]
                await self._condition.wait()

    async def _release_adapter(self, index: int) -> None:
        async def release() -> None:
            listeners: list[Callable[[], None]]
            async with self._condition:
                self._active[index] -= 1
                listeners = self._idle_listeners_if_idle()
                self._condition.notify_all()
            _notify_event_listeners(listeners)

        await await_cancellation_safe(release())

    async def _release_session(self, index: int) -> None:
        async def release() -> None:
            listeners: list[Callable[[], None]]
            async with self._condition:
                self._leased.discard(index)
                self._event_ready.set()
                listeners = self._idle_listeners_if_idle()
                self._condition.notify_all()
            _notify_event_listeners(listeners)

        await await_cancellation_safe(release())


class _AsyncProtocolAdapterSession:
    requires_explicit_session = False

    def __init__(
        self,
        pool: AsyncProtocolAdapterPool,
        index: int,
        adapter: AsyncProtocolAdapter,
    ) -> None:
        self._pool = pool
        self._index = index
        self._adapter = adapter
        self._closed = False
        self._invalid = False
        self._close_task: asyncio.Task[None] | None = None
        self.client = self

    def invalidate(self) -> None:
        self._invalid = True

    async def close(self) -> None:
        close_task = self._close_task
        if close_task is None:
            self._closed = True
            close_task = asyncio.create_task(self._finish_close())
            self._close_task = close_task
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError:
            close_task.add_done_callback(consume_async_future_exception)
            raise

    async def _finish_close(self) -> None:
        try:
            if self._invalid:
                invalidate = getattr(self._adapter, "invalidate", None)
                if callable(invalidate):
                    result = invalidate()
                    if inspect.isawaitable(result):
                        await result
                else:
                    await self._adapter.close()
        finally:
            await self._pool._release_session(self._index)

    def __getattr__(self, name: str) -> Any:
        if self._closed:
            raise FerricStoreError("protocol session is closed")
        return getattr(self._adapter, name)


__all__ = [
    "AsyncProtocolAdapterPool",
    "_AsyncProtocolAdapterSession",
]
