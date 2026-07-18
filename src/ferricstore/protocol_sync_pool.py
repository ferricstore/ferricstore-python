from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from ferricstore.config_validation import validate_optional_thread_wait_seconds
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    RetryableResourceSet,
    SyncCloseCoordinator,
    close_resources_sync,
    register_event_listener_transactionally,
)
from ferricstore.protocol_common import (
    _close_adapter_sync,
    _notify_event_listeners,
    _pool_topology_options,
    _protocol_connection_count,
)
from ferricstore.protocol_planning import PreparedCommand
from ferricstore.protocol_sync import (
    ProtocolAdapter,
    ProtocolPipeline,
)

if TYPE_CHECKING:
    from ferricstore.protocol_sync_topology import TopologyProtocolAdapterPool


class ProtocolAdapterPool:
    """Small protocol socket pool; each socket still multiplexes request lanes."""

    client: ProtocolAdapterPool
    requires_explicit_session = True
    supports_concurrent_fanout = True

    def __init__(self, adapters: list[ProtocolAdapter]) -> None:
        if not adapters:
            raise ValueError("ProtocolAdapterPool requires at least one adapter")
        self.adapters = adapters
        self.client = self
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._cursor = 0
        self._leased: set[int] = set()
        self._active = [0] * len(adapters)
        self._closed = False
        self._session_waiters = 0
        self._broadcasting = False
        self._event_ready = threading.Event()
        self._event_listener = self._event_ready.set
        self._idle_listeners: list[Callable[[], None]] = []
        self._event_poll_fallback = False
        self._close_coordinator = SyncCloseCoordinator()
        self._close_adapters = RetryableResourceSet(adapters)
        self._event_poll_fallback = register_event_listener_transactionally(
            adapters,
            self._event_listener,
        )

    @classmethod
    def from_url(
        cls, url: str, **kwargs: Any
    ) -> ProtocolAdapterPool | ProtocolAdapter | TopologyProtocolAdapterPool:
        seeds, ha_routing = _pool_topology_options(kwargs)
        if seeds is not None or ha_routing:
            urls = [url]
            if seeds is not None:
                urls.extend(list(seeds))
            return cls.from_urls(urls, **kwargs)

        max_connections = _protocol_connection_count(kwargs.pop("max_connections", 1))
        if max_connections <= 1:
            return ProtocolAdapter.from_url(url, **kwargs)
        adapters: list[ProtocolAdapter] = []
        try:
            adapters.extend(ProtocolAdapter.from_url(url, **kwargs) for _ in range(max_connections))
            return cls(adapters)
        except BaseException:
            for adapter in reversed(adapters):
                with contextlib.suppress(BaseException):
                    adapter.close()
            raise

    @classmethod
    def from_urls(cls, urls: Sequence[str], **kwargs: Any) -> TopologyProtocolAdapterPool:
        from ferricstore.protocol_sync_topology import TopologyProtocolAdapterPool

        return TopologyProtocolAdapterPool(list(urls), **kwargs)

    @property
    def events(self) -> list[Any]:
        events: list[Any] = []
        for adapter in self._available_adapters():
            events.extend(adapter.events)
        return events

    @property
    def backpressure_scope(self) -> Any:
        scopes = tuple(
            getattr(adapter, "backpressure_scope", id(adapter)) for adapter in self.adapters
        )
        return ("protocol-pool", scopes)

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
        with self._condition:
            if listener not in self._idle_listeners:
                self._idle_listeners.append(listener)

    def remove_idle_listener(self, listener: Callable[[], None]) -> None:
        with self._condition, contextlib.suppress(ValueError):
            self._idle_listeners.remove(listener)

    def _idle_listeners_if_idle_locked(self) -> list[Callable[[], None]]:
        if any(self._active) or self._leased or self._broadcasting:
            return []
        return list(self._idle_listeners)

    def wait_event(self, timeout: float | None = None) -> Any | None:
        timeout = validate_optional_thread_wait_seconds(
            timeout,
            name="wait_event timeout",
        )
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._condition:
                if self._closed:
                    raise FerricStoreError("protocol pool is closed")
            event = self._take_available_event()
            if event is not None:
                return event
            if timeout == 0.0:
                return None
            self._event_ready.clear()
            with self._condition:
                if self._closed:
                    raise FerricStoreError("protocol pool is closed")
            event = self._take_available_event()
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
            if not self._event_ready.wait(wait_for) and deadline is not None:
                return None

    def _take_available_event(self) -> Any | None:
        checked: set[int] = set()
        while True:
            reserved = self._reserve_event_adapter(checked)
            if reserved is None:
                return None
            index, adapter = reserved
            checked.add(index)
            try:
                event = adapter.wait_event(timeout=0.0)
            finally:
                self._release_adapter(index)
            if event is not None:
                return event

    def _reserve_event_adapter(
        self,
        excluded: set[int],
    ) -> tuple[int, ProtocolAdapter] | None:
        with self._condition:
            if self._closed or self._session_waiters:
                return None
            for index, adapter in enumerate(self.adapters):
                if index in excluded or index in self._leased:
                    continue
                self._active[index] += 1
                return index, adapter
        return None

    def close(self) -> None:
        self._close_coordinator.run(self._close_once)

    def _close_once(self) -> None:
        with self._condition:
            if not self._closed:
                self._closed = True
                self._condition.notify_all()
        self._event_ready.set()
        pending = self._close_adapters.snapshot()

        def close_adapter(adapter: Any) -> None:
            _close_adapter_sync(adapter, self._event_listener)
            self._close_adapters.complete(adapter)

        close_resources_sync([partial(close_adapter, adapter) for adapter in pending])

    def pipeline(self, transaction: bool = False) -> ProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return ProtocolPipeline(self)

    def execute_command(self, *args: Any) -> Any:
        index, adapter = self._acquire_adapter()
        try:
            return adapter.execute_command(*args)
        finally:
            self._release_adapter(index)

    def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        index, adapter = self._acquire_adapter()
        try:
            return adapter.execute_command_on_lane(args, lane_id)
        finally:
            self._release_adapter(index)

    def execute_prepared_command_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> Any:
        index, adapter = self._acquire_adapter()
        try:
            execute_prepared = getattr(adapter, "execute_prepared_command_on_lane", None)
            if callable(execute_prepared):
                return execute_prepared(prepared, lane_id)
            return adapter.execute_command_on_lane(prepared.args, lane_id)
        finally:
            self._release_adapter(index)

    def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        index, adapter = self._acquire_adapter()
        try:
            return adapter.execute_command_with_trace(*args)
        finally:
            self._release_adapter(index)

    def execute_command_with_trace_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> dict[str, Any]:
        index, adapter = self._acquire_adapter()
        try:
            return adapter.execute_command_with_trace_on_lane(args, lane_id)
        finally:
            self._release_adapter(index)

    def execute_prepared_command_with_trace_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> dict[str, Any]:
        index, adapter = self._acquire_adapter()
        try:
            execute_prepared = getattr(
                adapter,
                "execute_prepared_command_with_trace_on_lane",
                None,
            )
            if callable(execute_prepared):
                return cast(dict[str, Any], execute_prepared(prepared, lane_id))
            return adapter.execute_command_with_trace_on_lane(prepared.args, lane_id)
        finally:
            self._release_adapter(index)

    def submit_command(self, *args: Any) -> Future[Any]:
        return self._submit_tracked(lambda adapter: adapter.submit_command(*args))

    def submit_command_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> Future[Any]:
        return self._submit_tracked(lambda adapter: adapter.submit_command_on_lane(args, lane_id))

    def submit_prepared_command_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> Future[Any]:
        return self._submit_tracked(
            lambda adapter: (
                adapter.submit_prepared_command_on_lane(prepared, lane_id)
                if callable(getattr(adapter, "submit_prepared_command_on_lane", None))
                else adapter.submit_command_on_lane(prepared.args, lane_id)
            )
        )

    def submit_commands(self, commands: list[tuple[Any, ...]]) -> list[Future[Any]]:
        index, adapter = self._acquire_adapter()
        try:
            futures = adapter.submit_commands(commands)
        except BaseException:
            self._release_adapter(index)
            raise
        self._track_futures(index, self._wire_futures(futures))
        return futures

    def submit_commands_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Future[Any]]:
        index, adapter = self._acquire_adapter()
        try:
            futures = adapter.submit_commands_on_lane(commands, lane_id)
        except BaseException:
            self._release_adapter(index)
            raise
        self._track_futures(index, self._wire_futures(futures))
        return futures

    def submit_prepared_commands_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
    ) -> list[Future[Any]]:
        index, adapter = self._acquire_adapter()
        try:
            submit_prepared = getattr(adapter, "submit_prepared_commands_on_lane", None)
            futures = (
                submit_prepared(prepared_commands, lane_id)
                if callable(submit_prepared)
                else adapter.submit_commands_on_lane(
                    [prepared.args for prepared in prepared_commands],
                    lane_id,
                )
            )
        except BaseException:
            self._release_adapter(index)
            raise
        self._track_futures(index, self._wire_futures(futures))
        return futures

    def submit_batch(self, commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
        return cast(
            Future[list[Any]],
            self._submit_tracked(lambda adapter: adapter.submit_batch(commands)),
        )

    def submit_prepared_batch_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
    ) -> Future[list[Any]]:
        return cast(
            Future[list[Any]],
            self._submit_tracked(
                lambda adapter: (
                    adapter.submit_prepared_batch_on_lane(prepared_commands, lane_id)
                    if callable(getattr(adapter, "submit_prepared_batch_on_lane", None))
                    else adapter.submit_batch_on_lane(
                        [prepared.args for prepared in prepared_commands],
                        lane_id,
                    )
                )
            ),
        )

    def submit_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> Future[list[Any]]:
        return cast(
            Future[list[Any]],
            self._submit_tracked(lambda adapter: adapter.submit_batch_on_lane(commands, lane_id)),
        )

    def submit_mget(self, keys: Sequence[Any]) -> Future[Any]:
        return self._submit_tracked(lambda adapter: adapter.submit_mget(keys))

    def submit_mset_same_value(self, keys: Sequence[Any], value: Any) -> Future[Any]:
        return self._submit_tracked(lambda adapter: adapter.submit_mset_same_value(keys, value))

    def submit_mset_payload(self, payload: bytes) -> Future[Any]:
        return self._submit_tracked(lambda adapter: adapter.submit_mset_payload(payload))

    def submit_mset_payload_on_lane(self, payload: bytes, lane_id: int) -> Future[Any]:
        return self._submit_tracked(
            lambda adapter: adapter.submit_mset_payload_on_lane(payload, lane_id)
        )

    def _submit_validated_mset_payload_on_lane(
        self,
        payload: bytes,
        lane_id: int,
    ) -> Future[Any]:
        return self._submit_tracked(
            lambda adapter: adapter._submit_validated_mset_payload_on_lane(payload, lane_id)
        )

    def submit_pipeline_payload(self, payload: bytes, count: int) -> Future[list[Any]]:
        return cast(
            Future[list[Any]],
            self._submit_tracked(lambda adapter: adapter.submit_pipeline_payload(payload, count)),
        )

    def submit_pipeline_payload_on_lane(
        self,
        payload: bytes,
        count: int,
        lane_id: int,
    ) -> Future[list[Any]]:
        return cast(
            Future[list[Any]],
            self._submit_tracked(
                lambda adapter: adapter.submit_pipeline_payload_on_lane(payload, count, lane_id)
            ),
        )

    def submit_flow_many_payload(
        self, command: str, payload: bytes, count: int
    ) -> Future[list[Any]]:
        return cast(
            Future[list[Any]],
            self._submit_tracked(
                lambda adapter: adapter.submit_flow_many_payload(command, payload, count)
            ),
        )

    def submit_flow_many_payload_on_lane(
        self,
        command: str,
        payload: bytes,
        count: int,
        lane_id: int,
    ) -> Future[list[Any]]:
        return cast(
            Future[list[Any]],
            self._submit_tracked(
                lambda adapter: adapter.submit_flow_many_payload_on_lane(
                    command,
                    payload,
                    count,
                    lane_id,
                )
            ),
        )

    def submit_flow_value_mget_payload(self, payload: bytes) -> Future[Any]:
        return self._submit_tracked(lambda adapter: adapter.submit_flow_value_mget_payload(payload))

    def submit_flow_value_mget_payload_on_lane(
        self,
        payload: bytes,
        lane_id: int,
    ) -> Future[Any]:
        return self._submit_tracked(
            lambda adapter: adapter.submit_flow_value_mget_payload_on_lane(payload, lane_id)
        )

    def _submit_tracked(
        self,
        submit: Callable[[ProtocolAdapter], Future[Any]],
    ) -> Future[Any]:
        index, adapter = self._acquire_adapter()
        try:
            future = submit(adapter)
        except BaseException:
            self._release_adapter(index)
            raise
        self._track_futures(index, self._wire_futures((future,)))
        return future

    def _track_futures(self, index: int, futures: Sequence[Future[Any]]) -> None:
        if not futures:
            self._release_adapter(index)
            return
        remaining = len(futures)
        lock = threading.Lock()

        def done(_future: Future[Any]) -> None:
            nonlocal remaining
            with lock:
                remaining -= 1
                release = remaining == 0
            if release:
                self._release_adapter(index)

        for future in futures:
            future.add_done_callback(done)

    @staticmethod
    def _wire_futures(futures: Sequence[Future[Any]]) -> list[Future[Any]]:
        sources: list[Future[Any]] = []
        seen: set[int] = set()
        for future in futures:
            raw_sources = getattr(future, "_ferricstore_sources", (future,))
            for source in raw_sources:
                if not isinstance(source, Future) or id(source) in seen:
                    continue
                seen.add(id(source))
                sources.append(source)
        return sources

    def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> Any:
        with self._condition:
            while (self._leased or self._broadcasting) and not self._closed:
                self._condition.wait()
            if self._closed:
                raise FerricStoreError("protocol pool is closed")
            self._broadcasting = True
            adapters = list(self.adapters)
        try:
            replies = [adapter.subscribe_flow_wake(*args, **kwargs) for adapter in adapters]
            return replies[0] if len(replies) == 1 else replies
        finally:
            listeners: list[Callable[[], None]]
            with self._condition:
                self._broadcasting = False
                listeners = self._idle_listeners_if_idle_locked()
                self._condition.notify_all()
            _notify_event_listeners(listeners)

    def register_flow_wake_subscription(self, *args: Any, **kwargs: Any) -> None:
        """Persist the reconnect filter on every socket in this endpoint pool."""
        for adapter in self.adapters:
            adapter.register_flow_wake_subscription(*args, **kwargs)

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        index, adapter = self._acquire_adapter()
        try:
            return adapter.execute_batch(commands)
        finally:
            self._release_adapter(index)

    def execute_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        index, adapter = self._acquire_adapter()
        try:
            return adapter.execute_batch_on_lane(commands, lane_id)
        finally:
            self._release_adapter(index)

    def execute_prepared_batch_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
    ) -> list[Any]:
        index, adapter = self._acquire_adapter()
        try:
            execute_prepared = getattr(adapter, "execute_prepared_batch_on_lane", None)
            if callable(execute_prepared):
                return cast(list[Any], execute_prepared(prepared_commands, lane_id))
            return adapter.execute_batch_on_lane(
                [prepared.args for prepared in prepared_commands],
                lane_id,
            )
        finally:
            self._release_adapter(index)

    def acquire_session(self) -> _ProtocolAdapterSession:
        with self._condition:
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
                                return _ProtocolAdapterSession(self, index, self.adapters[index])
                    self._condition.wait()
            finally:
                self._session_waiters -= 1
                if self._session_waiters == 0:
                    self._event_ready.set()
                self._condition.notify_all()

    def acquire_dedicated_session(self) -> ProtocolAdapter:
        """Create an independent connection without retaining a pooled adapter lease."""
        index, adapter = self._acquire_adapter()
        try:
            return adapter.acquire_session()
        finally:
            self._release_adapter(index)

    def acquire_session_on_lane(self, lane_id: int) -> ProtocolAdapter:
        index, adapter = self._acquire_adapter()
        try:
            return adapter.acquire_session_on_lane(lane_id)
        finally:
            self._release_adapter(index)

    def acquire_session_for_key(self, _key: str | bytes) -> _ProtocolAdapterSession:
        return self.acquire_session()

    def _acquire_adapter(self) -> tuple[int, ProtocolAdapter]:
        with self._condition:
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
                self._condition.wait()

    def _release_adapter(self, index: int) -> None:
        listeners: list[Callable[[], None]]
        with self._condition:
            self._active[index] -= 1
            listeners = self._idle_listeners_if_idle_locked()
            self._condition.notify_all()
        _notify_event_listeners(listeners)

    def _release_session(self, index: int) -> None:
        listeners: list[Callable[[], None]]
        with self._condition:
            self._leased.discard(index)
            self._event_ready.set()
            listeners = self._idle_listeners_if_idle_locked()
            self._condition.notify_all()
        _notify_event_listeners(listeners)

    def _available_adapters(self) -> list[ProtocolAdapter]:
        with self._condition:
            return [
                adapter for index, adapter in enumerate(self.adapters) if index not in self._leased
            ]


class _ProtocolAdapterSession:
    requires_explicit_session = False

    def __init__(
        self,
        pool: ProtocolAdapterPool,
        index: int,
        adapter: ProtocolAdapter,
    ) -> None:
        self._pool = pool
        self._index = index
        self._adapter = adapter
        self._closed = False
        self._invalid = False
        self.client = self

    def invalidate(self) -> None:
        self._invalid = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._invalid:
                invalidate = getattr(self._adapter, "invalidate", None)
                if callable(invalidate):
                    invalidate()
                else:
                    self._adapter.close()
        finally:
            self._pool._release_session(self._index)

    def __getattr__(self, name: str) -> Any:
        if self._closed:
            raise FerricStoreError("protocol session is closed")
        return getattr(self._adapter, name)


__all__ = [
    "ProtocolAdapterPool",
    "_ProtocolAdapterSession",
]
