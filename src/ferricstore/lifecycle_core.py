from __future__ import annotations

import contextlib
import inspect
import threading
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Future
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
from types import TracebackType
from typing import TYPE_CHECKING, Any, NoReturn, TypeVar, cast

from ferricstore.config_validation import validate_positive_int

if TYPE_CHECKING:
    import asyncio

_DEFAULT_ASYNC_CLEANUP_CONCURRENCY = 16
_T = TypeVar("_T")


class DeferredCallbackFuture(Future[Any]):
    """Allow a producer to publish related Future states before callbacks run."""

    def __init__(self) -> None:
        super().__init__()
        self._callback_gate = threading.Lock()
        self._callbacks_deferred = False
        self._callback_invocation_pending = False
        self._callbacks_added_while_deferred: list[Callable[[Future[Any]], Any]] = []

    def defer_callbacks(self) -> None:
        with self._callback_gate:
            self._callbacks_deferred = True

    def release_callbacks(self) -> None:
        with self._callback_gate:
            self._callbacks_deferred = False
            invoke_pending = self._callback_invocation_pending
            self._callback_invocation_pending = False
            added = self._callbacks_added_while_deferred
            self._callbacks_added_while_deferred = []
        if invoke_pending:
            cast(Any, super())._invoke_callbacks()
        for callback in added:
            super().add_done_callback(callback)

    def add_done_callback(self, fn: Callable[[Future[Any]], Any]) -> None:
        with self._callback_gate:
            if self._callbacks_deferred:
                self._callbacks_added_while_deferred.append(fn)
                return
        super().add_done_callback(fn)

    def _invoke_callbacks(self) -> None:
        with self._callback_gate:
            if self._callbacks_deferred:
                self._callback_invocation_pending = True
                return
        cast(Any, super())._invoke_callbacks()


class RetryableResourceSet:
    """Retain only resources whose cleanup has not completed successfully."""

    def __init__(self, resources: Sequence[Any]) -> None:
        self._lock = threading.Lock()
        self._resources = {id(resource): resource for resource in resources}

    def snapshot(self) -> tuple[Any, ...]:
        with self._lock:
            return tuple(self._resources.values())

    def add(self, resource: Any) -> None:
        with self._lock:
            self._resources[id(resource)] = resource

    def contains(self, resource: Any) -> bool:
        with self._lock:
            return self._resources.get(id(resource)) is resource

    def complete(self, resource: Any) -> None:
        with self._lock:
            self._resources.pop(id(resource), None)


def register_event_listener_transactionally(
    resources: Sequence[Any],
    listener: Callable[[], None],
) -> bool:
    """Attach one listener everywhere and undo partial registration on failure.

    Returns whether at least one resource lacks push-listener support and
    therefore needs polling fallback.
    """

    registered: list[Any] = []
    poll_fallback = False
    try:
        for resource in resources:
            add_listener = getattr(resource, "add_event_listener", None)
            if callable(add_listener):
                add_listener(listener)
                registered.append(resource)
            else:
                poll_fallback = True
    except BaseException:
        for resource in reversed(registered):
            remove_listener = getattr(resource, "remove_event_listener", None)
            if callable(remove_listener):
                with contextlib.suppress(BaseException):
                    remove_listener(listener)
        raise
    return poll_fallback


class AsyncCloseTaskRegistry:
    """Retain independently owned async close operations across deadline retries."""

    def __init__(self) -> None:
        self._tasks: dict[int, tuple[Any, asyncio.Future[Any]]] = {}

    async def run(
        self,
        resource: Any,
        close: Callable[[], Any],
        wait: Callable[[asyncio.Future[Any]], Awaitable[Any]],
    ) -> Any:
        import asyncio

        identity = id(resource)
        state = self._tasks.get(identity)
        if state is not None:
            existing = state[1]
            if existing.done() and (existing.cancelled() or existing.exception() is not None):
                self._tasks.pop(identity, None)
                state = None
        task: asyncio.Future[Any]
        if state is None or state[0] is not resource:
            result = close()
            if not inspect.isawaitable(result):
                return result
            task = asyncio.ensure_future(result)
            self._tasks[identity] = (resource, task)
            task.add_done_callback(consume_async_future_exception)
        else:
            task = state[1]

        try:
            return await wait(task)
        except BaseException:
            if task.done() and (task.cancelled() or task.exception() is not None):
                current = self._tasks.get(identity)
                if current is not None and current[1] is task:
                    self._tasks.pop(identity, None)
            raise


class SyncCloseTaskRegistry:
    """Retain daemon-backed blocking close operations across deadline retries."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: dict[int, tuple[Any, Future[Any]]] = {}

    def run(
        self,
        resource: Any,
        close: Callable[[], Any],
        wait: Callable[[Future[Any]], Any],
    ) -> Any:
        identity = id(resource)
        with self._lock:
            state = self._tasks.get(identity)
            if state is not None:
                existing = state[1]
                if existing.done() and (existing.cancelled() or existing.exception() is not None):
                    self._tasks.pop(identity, None)
                    state = None
            if state is None or state[0] is not resource:
                task: Future[Any] = Future()
                self._tasks[identity] = (resource, task)
                thread = threading.Thread(
                    target=self._run_close,
                    args=(task, close),
                    name="ferricstore-close",
                    daemon=True,
                )
                try:
                    thread.start()
                except BaseException:
                    # Publishing an operation and starting its owner thread are one
                    # transaction.  An unstarted Future must never be retained as
                    # though useful work were still running.
                    if thread.ident is None:
                        current = self._tasks.get(identity)
                        if current is not None and current[1] is task:
                            self._tasks.pop(identity, None)
                    raise
            else:
                task = state[1]

        try:
            return wait(task)
        except BaseException:
            if task.done() and (task.cancelled() or task.exception() is not None):
                with self._lock:
                    current = self._tasks.get(identity)
                    if current is not None and current[1] is task:
                        self._tasks.pop(identity, None)
            raise

    @staticmethod
    def _run_close(task: Future[Any], close: Callable[[], Any]) -> None:
        try:
            result = close()
        except BaseException as exc:
            try_set_future_exception(task, exc)
        else:
            try_set_future_result(task, result)


class SyncCloseCoordinator:
    """Serialize close calls, retain success, and allow retry after failure."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._running = False
        self._complete = False
        self._started = False
        self._open_operations = 0

    @property
    def started(self) -> bool:
        with self._condition:
            return self._started

    def run(self, close: Callable[[], Any]) -> None:
        with self._condition:
            while self._running:
                self._condition.wait()
            if self._complete:
                return
            self._started = True
            self._running = True
            while self._open_operations:
                self._condition.wait()

        try:
            close()
        except BaseException:
            with self._condition:
                self._running = False
                self._condition.notify_all()
            raise
        else:
            with self._condition:
                self._complete = True
                self._running = False
                self._condition.notify_all()

    def run_while_open(
        self,
        operation: Callable[[], _T],
        *,
        closed_message: str,
    ) -> _T:
        """Run lifecycle-sensitive work before close takes its resource snapshot."""
        with self._condition:
            if self._started:
                raise RuntimeError(closed_message)
            self._open_operations += 1

        try:
            return operation()
        finally:
            with self._condition:
                self._open_operations -= 1
                self._condition.notify_all()


class AsyncCloseCoordinator:
    """Share cancellation-safe close work and allow retry after operation failure."""

    def __init__(self) -> None:
        self._task: asyncio.Future[Any] | None = None
        self._complete = False
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    @property
    def current_task(self) -> asyncio.Future[Any] | None:
        return self._task

    def task(self, close: Callable[[], Awaitable[Any]]) -> asyncio.Future[Any]:
        """Return the shared cleanup task without coupling it to a caller wait."""
        import asyncio

        task = self._task
        if task is not None and task.done() and (task.cancelled() or task.exception() is not None):
            self._task = None
            task = None
        if task is None:
            if self._complete:
                task = asyncio.get_running_loop().create_future()
                task.set_result(None)
                self._task = task
                return task
            self._started = True
            task = asyncio.ensure_future(close())
            self._task = task
            task.add_done_callback(self._close_finished)
        return task

    async def run(self, close: Callable[[], Awaitable[Any]]) -> None:
        if self._complete:
            return
        task = self.task(close)

        try:
            await await_cancellation_safe(task)
        except BaseException:
            if (
                task.done()
                and (task.cancelled() or task.exception() is not None)
                and self._task is task
            ):
                self._task = None
            raise
        else:
            self._complete = True

    def _close_finished(self, task: asyncio.Future[Any]) -> None:
        if task.cancelled() or task.exception() is not None:
            if self._task is task:
                self._task = None
            return
        self._complete = True


def consume_async_future_exception(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


def try_set_future_result(future: Any, result: Any) -> bool:
    """Complete a future without losing a concurrent cancellation/completion race."""
    import asyncio

    try:
        future.set_result(result)
    except (asyncio.InvalidStateError, ConcurrentInvalidStateError):
        return False
    return True


def try_set_future_exception(future: Any, error: BaseException) -> bool:
    """Fail a future without allowing completion races to abort resource cleanup."""
    import asyncio

    try:
        future.set_exception(error)
    except (asyncio.InvalidStateError, ConcurrentInvalidStateError):
        return False
    return True


async def await_cancellation_safe(awaitable: Awaitable[_T]) -> _T:
    """Keep independently owned cleanup running if its caller is cancelled."""
    import asyncio

    task = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        task.add_done_callback(consume_async_future_exception)
        raise


def raise_primary_with_cleanup(
    primary: BaseException,
    traceback: TracebackType | None,
    cleanup: BaseException | None,
) -> NoReturn:
    """Re-raise the primary failure while retaining cleanup failure context."""
    if cleanup is not None:
        raise primary.with_traceback(traceback) from cleanup
    raise primary.with_traceback(traceback)


def chain_cleanup_errors(
    errors: Sequence[BaseException | None],
) -> BaseException | None:
    """Retain every cleanup failure in deterministic attempt order."""
    return _chain_cleanup_errors([error for error in errors if error is not None])


def close_resources_sync(resources: Sequence[Callable[[], Any]]) -> None:
    """Attempt every cleanup in order and preserve the first failure."""
    errors: list[tuple[BaseException, TracebackType | None]] = []
    for close in resources:
        try:
            close()
        except BaseException as exc:
            errors.append((exc, exc.__traceback__))
    _raise_cleanup_errors(errors)


async def close_resources_async(
    resources: Sequence[Callable[[], Any]],
    *,
    max_concurrency: int = _DEFAULT_ASYNC_CLEANUP_CONCURRENCY,
) -> None:
    """Cancellation-safe, bounded async cleanup that attempts every resource."""
    import asyncio

    max_concurrency = validate_positive_int(max_concurrency, name="max_concurrency")
    if not resources:
        return

    async def close_all() -> None:
        errors: dict[int, tuple[BaseException, TracebackType | None]] = {}
        next_index = 0

        async def worker() -> None:
            nonlocal next_index
            while next_index < len(resources):
                index = next_index
                next_index += 1
                try:
                    result = resources[index]()
                    if inspect.isawaitable(result):
                        await result
                except BaseException as exc:
                    errors[index] = (exc, exc.__traceback__)

        worker_count = min(max_concurrency, len(resources))
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        await asyncio.gather(*workers)
        _raise_cleanup_errors([errors[index] for index in sorted(errors)])

    await await_cancellation_safe(close_all())


def _raise_cleanup_errors(
    errors: Sequence[tuple[BaseException, TracebackType | None]],
) -> None:
    if not errors:
        return
    primary, traceback = errors[0]
    cleanup = _chain_cleanup_errors([error for error, _traceback in errors[1:]])
    raise_primary_with_cleanup(primary, traceback, cleanup)


def _chain_cleanup_errors(errors: Sequence[BaseException]) -> BaseException | None:
    if not errors:
        return None
    chained = errors[-1]
    for error in reversed(errors[:-1]):
        error.__cause__ = chained
        error.__suppress_context__ = True
        chained = error
    return chained
