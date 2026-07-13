from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from typing import Any, Generic, TypeVar

_Key = TypeVar("_Key")
_Result = TypeVar("_Result")


class EndpointAdapterLifecycle(Generic[_Key]):
    """Own active and draining endpoint adapters until cleanup succeeds elsewhere."""

    def __init__(self, *, is_idle: Callable[[Any], bool]) -> None:
        self.active: dict[_Key, Any] = {}
        self.retired: dict[_Key, tuple[Any, int]] = {}
        self.generation = 0
        self._is_idle = is_idle
        self._idle_callbacks: dict[_Key, Callable[[], None]] = {}

    def get(self, key: _Key) -> Any | None:
        adapter = self.active.get(key)
        if adapter is not None:
            return adapter
        retired = self.retired.pop(key, None)
        if retired is None:
            return None
        adapter = retired[0]
        self._remove_idle_listener(key, adapter)
        self.active[key] = adapter
        return adapter

    def put(self, key: _Key, adapter: Any) -> None:
        self.active[key] = adapter

    def install(
        self,
        live_keys: set[_Key],
        on_idle: Callable[[_Key, Any], None],
    ) -> list[Any]:
        self.generation += 1
        for key in live_keys:
            self.get(key)

        ready: list[Any] = []
        for key in tuple(self.active):
            if key in live_keys:
                continue
            adapter = self.active.pop(key)
            if self._is_idle(adapter):
                ready.append(adapter)
                continue
            self.retired[key] = (adapter, self.generation)
            self._add_idle_listener(key, adapter, on_idle)

        # Production adapters notify on idle. The scan is a conservative fallback
        # for custom adapters that expose activity state but no listener API.
        for key, (adapter, _generation) in tuple(self.retired.items()):
            if self._is_idle(adapter):
                claimed = self.claim_idle(key, adapter)
                if claimed is not None:
                    ready.append(claimed)
        return ready

    def claim_idle(self, key: _Key, adapter: Any) -> Any | None:
        retired = self.retired.get(key)
        if retired is None or retired[0] is not adapter or not self._is_idle(adapter):
            return None
        self.retired.pop(key, None)
        self._remove_idle_listener(key, adapter)
        return adapter

    def drain(self) -> list[Any]:
        adapters = [*self.active.values(), *(item[0] for item in self.retired.values())]
        for key, (adapter, _generation) in tuple(self.retired.items()):
            self._remove_idle_listener(key, adapter)
        self.active.clear()
        self.retired.clear()
        unique: dict[int, Any] = {}
        for adapter in adapters:
            unique.setdefault(id(adapter), adapter)
        return list(unique.values())

    def _add_idle_listener(
        self,
        key: _Key,
        adapter: Any,
        on_idle: Callable[[_Key, Any], None],
    ) -> None:
        add_listener = getattr(adapter, "add_idle_listener", None)
        if not callable(add_listener):
            return

        def idle() -> None:
            on_idle(key, adapter)

        self._idle_callbacks[key] = idle
        try:
            add_listener(idle)
        except Exception:
            self._idle_callbacks.pop(key, None)

    def _remove_idle_listener(self, key: _Key, adapter: Any) -> None:
        callback = self._idle_callbacks.pop(key, None)
        remove_listener = getattr(adapter, "remove_idle_listener", None)
        if callback is not None and callable(remove_listener):
            with contextlib.suppress(Exception):
                remove_listener(callback)


class SyncSingleFlight(Generic[_Result]):
    """Share one in-flight blocking operation among concurrent callers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._operation: Future[_Result] | None = None

    def run(self, operation: Callable[[], _Result]) -> _Result:
        with self._lock:
            future = self._operation
            if future is None:
                future = Future()
                self._operation = future
                creator = True
            else:
                creator = False
        if not creator:
            return future.result()

        try:
            result = operation()
        except BaseException as exc:
            with self._lock:
                future.set_exception(exc)
                if self._operation is future:
                    self._operation = None
            raise
        else:
            with self._lock:
                future.set_result(result)
                if self._operation is future:
                    self._operation = None
            return result


class AsyncSingleFlight(Generic[_Result]):
    """Cancellation-safe async singleflight for topology discovery."""

    def __init__(self) -> None:
        self._task: asyncio.Future[_Result] | None = None

    async def run(self, operation: Callable[[], Awaitable[_Result]]) -> _Result:
        task = self._task
        if task is None:
            task = asyncio.ensure_future(operation())
            self._task = task
            task.add_done_callback(_consume_task_exception)
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._task is task:
                self._task = None


def _consume_task_exception(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()
