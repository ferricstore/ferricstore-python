from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import TypeVar

from ferricstore.backpressure import BackpressureController
from ferricstore.errors import FerricStoreError, OverloadedError

_T = TypeVar("_T")
_RETRY_MANAGED: ContextVar[bool] = ContextVar("ferricstore_flow_query_retry", default=False)
_MIN_UNBOUNDED_RETRY_DELAY_SECONDS = 0.001


def execute_flow_query_read_with_retry(
    operation: Callable[[], _T],
    backpressure: BackpressureController,
    *,
    deadline_ms: int | None = None,
) -> _T:
    """Execute a read, replaying only explicit server-declared safe outcomes."""

    attempt = 0
    started = time.monotonic()
    while True:
        elapsed_s = time.monotonic() - started
        if not backpressure.before_request(
            elapsed_s=elapsed_s,
            remaining_s=_remaining_deadline_seconds(deadline_ms),
        ):
            raise OverloadedError(
                "client backpressure wait exceeds the query deadline or max_elapsed_ms"
            )
        try:
            token = _RETRY_MANAGED.set(True)
            try:
                result = operation()
            finally:
                _RETRY_MANAGED.reset(token)
        except FerricStoreError as exc:
            elapsed_s = time.monotonic() - started
            if not _schedule_sync_retry(
                exc,
                backpressure,
                attempt=attempt,
                elapsed_s=elapsed_s,
                deadline_ms=deadline_ms,
            ):
                raise
            if _unbounded_zero_delay_retry(exc, backpressure):
                time.sleep(_MIN_UNBOUNDED_RETRY_DELAY_SECONDS)
            attempt += 1
        else:
            backpressure.record_success()
            return result


async def execute_flow_query_read_with_retry_async(
    operation: Callable[[], Awaitable[_T]],
    backpressure: BackpressureController,
    *,
    deadline_ms: int | None = None,
) -> _T:
    """Async counterpart that never blocks the event loop while backing off."""

    attempt = 0
    started = time.monotonic()
    while True:
        elapsed_s = time.monotonic() - started
        if not await backpressure.before_request_async(
            elapsed_s=elapsed_s,
            remaining_s=_remaining_deadline_seconds(deadline_ms),
        ):
            raise OverloadedError(
                "client backpressure wait exceeds the query deadline or max_elapsed_ms"
            )
        try:
            token = _RETRY_MANAGED.set(True)
            try:
                result = await operation()
            finally:
                _RETRY_MANAGED.reset(token)
        except FerricStoreError as exc:
            elapsed_s = time.monotonic() - started
            if not await _schedule_async_retry(
                exc,
                backpressure,
                attempt=attempt,
                elapsed_s=elapsed_s,
                deadline_ms=deadline_ms,
            ):
                raise
            if _unbounded_zero_delay_retry(exc, backpressure):
                import asyncio

                await asyncio.sleep(_MIN_UNBOUNDED_RETRY_DELAY_SECONDS)
            attempt += 1
        else:
            backpressure.record_success()
            return result


def _schedule_sync_retry(
    exc: FerricStoreError,
    backpressure: BackpressureController,
    *,
    attempt: int,
    elapsed_s: float,
    deadline_ms: int | None,
) -> bool:
    if not _server_declares_safe_retry(exc) or not backpressure.can_retry(
        attempt, elapsed_s=elapsed_s
    ):
        return False
    remaining_s = _remaining_deadline_seconds(deadline_ms)
    if isinstance(exc, OverloadedError):
        return backpressure.record_overload(
            attempt,
            exc.retry_after_ms,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
        )
    return backpressure.record_retry(
        exc.retry_after_ms,
        elapsed_s=elapsed_s,
        remaining_s=remaining_s,
    )


async def _schedule_async_retry(
    exc: FerricStoreError,
    backpressure: BackpressureController,
    *,
    attempt: int,
    elapsed_s: float,
    deadline_ms: int | None,
) -> bool:
    if not _server_declares_safe_retry(exc) or not backpressure.can_retry(
        attempt, elapsed_s=elapsed_s
    ):
        return False
    remaining_s = _remaining_deadline_seconds(deadline_ms)
    if isinstance(exc, OverloadedError):
        return await backpressure.record_overload_async(
            attempt,
            exc.retry_after_ms,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
        )
    return await backpressure.record_retry_async(
        exc.retry_after_ms,
        elapsed_s=elapsed_s,
        remaining_s=remaining_s,
    )


def _server_declares_safe_retry(exc: FerricStoreError) -> bool:
    return exc.retryable is True and exc.safe_to_retry is True


def _unbounded_zero_delay_retry(
    exc: FerricStoreError,
    backpressure: BackpressureController,
) -> bool:
    """Yield a fully unbounded loop when neither server nor policy provides delay."""

    policy = backpressure.policy
    if policy.max_retries is not None or policy.max_elapsed_ms is not None:
        return False
    if exc.retry_after_ms is not None and exc.retry_after_ms > 0:
        return False
    return not isinstance(exc, OverloadedError) or (
        policy.base_delay_ms <= 0 or policy.max_delay_ms <= 0
    )


def _remaining_deadline_seconds(deadline_ms: int | None) -> float | None:
    if deadline_ms in (None, 0):
        return None
    return (deadline_ms - time.time() * 1_000.0) / 1_000.0


def flow_query_retry_is_managed() -> bool:
    """Return whether the high-level read retry loop currently owns replay."""

    return _RETRY_MANAGED.get()


__all__ = [
    "execute_flow_query_read_with_retry",
    "execute_flow_query_read_with_retry_async",
    "flow_query_retry_is_managed",
]
