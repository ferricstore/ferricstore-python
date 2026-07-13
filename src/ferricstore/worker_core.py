from __future__ import annotations

import asyncio
import inspect
import threading
import time
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, TypeVar

from ferricstore.errors import FerricStoreError, classify_server_error, map_exception

_MANY_OK_STATUSES = {"ok", "success"}
_MANY_ERROR_STATUSES = {"busy", "error", "err", "failed", "failure"}
_AsyncResult = TypeVar("_AsyncResult")
_SyncResult = TypeVar("_SyncResult")


class CloseTimeoutError(TimeoutError):
    """A shutdown deadline expired before cleanup completed."""


def _status_text(value: Any) -> str | None:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").lower()
    if isinstance(value, str):
        return value.lower()
    return None


def _error_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Mapping):
        for key in ("message", b"message", "error", b"error", "reason", b"reason"):
            if key in value:
                return _error_text(value[key])
    return str(value)


def many_item_error(item: Any) -> BaseException | None:
    """Return the error represented by one independent many-command item."""
    if isinstance(item, BaseException):
        return map_exception(item) if isinstance(item, Exception) else item

    status: str | None = None
    value: Any = None
    if isinstance(item, (list, tuple)) and len(item) == 2:
        status = _status_text(item[0])
        value = item[1]
    elif isinstance(item, Mapping):
        raw_status = item.get("status", item.get(b"status"))
        status = _status_text(raw_status)
        if "value" in item:
            value = item["value"]
        elif b"value" in item:
            value = item[b"value"]
        else:
            value = item

    if status in _MANY_OK_STATUSES or status is None:
        return None
    if status in _MANY_ERROR_STATUSES:
        message = _error_text(value)
        if status == "busy" and "busy" not in message.lower():
            message = f"busy: {message}"
        return classify_server_error(message, raw=item)
    return FerricStoreError(f"unknown Flow many item status {status!r}", raw=item)


def expand_many_result(response: Any, expected_count: int, *, operation: str) -> list[Any]:
    """Normalize a many response and enforce exact item cardinality."""
    if expected_count < 0:
        raise ValueError("expected_count must be non-negative")
    if isinstance(response, list):
        if len(response) != expected_count:
            raise FerricStoreError(
                f"{operation} returned {len(response)} items; expected {expected_count} items",
                raw=response,
            )
        return response
    return [response] * expected_count


def validate_many_result(response: Any, expected_count: int, *, operation: str) -> list[Any]:
    """Raise for cardinality mismatches or any independent item failure."""
    items = expand_many_result(response, expected_count, operation=operation)
    for item in items:
        error = many_item_error(item)
        if error is not None:
            raise error
    return items


@dataclass(slots=True)
class WorkerIdleScheduler:
    """Transport-neutral idle backoff shared by sync and async workers."""

    minimum_s: float
    maximum_s: float
    current_s: float = 0.0

    def __post_init__(self) -> None:
        self.minimum_s = max(self.minimum_s, 0.0)
        self.maximum_s = max(self.maximum_s, self.minimum_s)
        self.current_s = self.minimum_s

    def after_batch(self, claimed: int) -> float:
        if claimed > 0:
            self.current_s = self.minimum_s
            return 0.0
        delay = self.current_s
        self.current_s = min(
            self.maximum_s,
            max(self.current_s * 2, self.minimum_s),
        )
        return delay


class WorkerInvocationTracker:
    """Track public sync worker calls so close can enforce one deadline."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._active = 0
        self._closing = False

    @property
    def closing(self) -> bool:
        with self._condition:
            return self._closing

    def begin(self, message: str) -> None:
        with self._condition:
            if self._closing:
                raise RuntimeError(message)
            self._active += 1

    def end(self) -> None:
        with self._condition:
            self._active -= 1
            self._condition.notify_all()

    def begin_close(self) -> None:
        with self._condition:
            self._closing = True
            self._condition.notify_all()

    def wait_for_idle(self, deadline: CloseDeadline, message: str) -> None:
        with self._condition:
            while self._active:
                self._condition.wait(deadline.remaining(message))


class SyncWorkerRunGate:
    """Serialize sync worker run/start/stop/close state transitions."""

    def __init__(self, *, closing_message: str) -> None:
        self._lock = threading.RLock()
        self._closing = False
        self._closing_message = closing_message

    @property
    def closing(self) -> bool:
        with self._lock:
            return self._closing

    def run_while_open(self, transition: Callable[[], _SyncResult]) -> _SyncResult:
        with self._lock:
            if self._closing:
                raise RuntimeError(self._closing_message)
            return transition()

    def synchronized(self, transition: Callable[[], _SyncResult]) -> _SyncResult:
        with self._lock:
            return transition()

    def begin_close(self, transition: Callable[[], _SyncResult]) -> _SyncResult:
        with self._lock:
            self._closing = True
            return transition()


class AsyncWorkerInvocationTracker:
    """Track public async worker calls across terminal, retryable shutdown."""

    def __init__(self) -> None:
        self._active = 0
        self._closing = False
        self._idle = asyncio.Event()
        self._idle.set()

    @property
    def closing(self) -> bool:
        return self._closing

    def begin(self, message: str) -> None:
        if self._closing:
            raise RuntimeError(message)
        self._active += 1
        if self._active == 1:
            self._idle.clear()

    def end(self) -> None:
        if self._active <= 0:
            raise RuntimeError("async worker invocation tracker underflow")
        self._active -= 1
        if self._active == 0:
            self._idle.set()

    async def run_while_open(
        self,
        operation: Callable[[], Awaitable[_AsyncResult]],
        *,
        closed_message: str,
    ) -> _AsyncResult:
        self.begin(closed_message)
        try:
            return await operation()
        finally:
            self.end()

    def begin_close(self) -> None:
        self._closing = True

    async def wait_for_idle(self, deadline: CloseDeadline, message: str) -> None:
        while self._active:
            timeout = deadline.remaining(message)
            try:
                if timeout is None:
                    await self._idle.wait()
                else:
                    await asyncio.wait_for(self._idle.wait(), timeout)
            except asyncio.TimeoutError:
                raise CloseTimeoutError(message) from None


def can_fuse_complete_claim(
    *,
    enabled: bool,
    has_jobs: bool,
    has_mixed_results: bool,
    has_failures: bool,
    claims_values: bool,
    supported: bool,
) -> bool:
    """Shared correctness gate for the complete-and-claim fast path."""
    return (
        enabled
        and has_jobs
        and not has_mixed_results
        and not has_failures
        and not claims_values
        and supported
    )


@dataclass(frozen=True, slots=True)
class CloseDeadline:
    """One absolute deadline shared by every stage of worker shutdown."""

    expires_at: float | None

    @classmethod
    def start(cls, timeout: float | None) -> CloseDeadline:
        if timeout is not None and timeout < 0:
            raise ValueError("close timeout must be non-negative")
        return cls(None if timeout is None else time.monotonic() + timeout)

    def remaining(self, message: str) -> float | None:
        if self.expires_at is None:
            return None
        remaining = self.expires_at - time.monotonic()
        if remaining <= 0:
            raise CloseTimeoutError(message)
        return remaining

    def check(self, message: str) -> None:
        self.remaining(message)

    def join_thread(self, thread: threading.Thread | None, message: str) -> None:
        if thread is None or not thread.is_alive():
            return
        thread.join(self.remaining(message))
        if thread.is_alive():
            raise CloseTimeoutError(message)

    def future_result(self, future: Future[Any], message: str) -> Any:
        if future.done():
            return future.result()
        try:
            return future.result(timeout=self.remaining(message))
        except TimeoutError:
            if future.done():
                return future.result()
            raise CloseTimeoutError(message) from None

    async def wait_tasks(
        self,
        tasks: list[asyncio.Task[Any]],
        message: str,
    ) -> None:
        pending = [task for task in tasks if not task.done()]
        if not pending:
            return
        _done, remaining = await asyncio.wait(
            pending,
            timeout=self.remaining(message),
        )
        if remaining:
            raise CloseTimeoutError(message)

    async def wait_awaitable(self, awaitable: Any, message: str) -> Any:
        try:
            timeout = self.remaining(message)
        except BaseException:
            _discard_unstarted_awaitable(awaitable)
            raise
        task = asyncio.ensure_future(awaitable)
        if task.done():
            return task.result()
        try:
            _done, pending = await asyncio.wait([task], timeout=timeout)
        except BaseException:
            _observe_task_completion(task)
            raise
        if pending:
            _observe_task_completion(task)
            raise CloseTimeoutError(message)
        return task.result()

    async def wait_task(self, task: asyncio.Future[Any], message: str) -> Any:
        """Wait within the deadline without taking ownership of the task."""
        if task.done():
            return task.result()
        _done, pending = await asyncio.wait([task], timeout=self.remaining(message))
        if pending:
            raise CloseTimeoutError(message)
        return task.result()


class WorkerTerminalState:
    """Thread-safe storage for a background worker's terminal exception."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._error: BaseException | None = None

    def reset(self) -> None:
        with self._lock:
            self._error = None

    def capture(self, error: BaseException) -> None:
        with self._lock:
            if self._error is None:
                self._error = error

    def error(self) -> BaseException | None:
        with self._lock:
            return self._error

    def raise_if_failed(self) -> None:
        error = self.error()
        if error is not None:
            raise error


def task_terminal_error(task: asyncio.Task[Any]) -> BaseException | None:
    if task.cancelled():
        return None
    return task.exception()


def _consume_task_exception(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _observe_task_completion(task: asyncio.Future[Any]) -> None:
    if task.done():
        _consume_task_exception(task)
    else:
        task.add_done_callback(_consume_task_exception)


def _discard_unstarted_awaitable(awaitable: Any) -> None:
    if isinstance(awaitable, asyncio.Future):
        _observe_task_completion(awaitable)
    elif inspect.iscoroutine(awaitable):
        awaitable.close()
