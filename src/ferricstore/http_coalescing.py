from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Sequence
from time import monotonic
from typing import Any, Protocol, cast

from ferricstore.errors import HttpError
from ferricstore.http_transport import _HttpDeadline

_UNSET = object()


class HttpCoalescingAdapter(Protocol):
    def _encode_command(self, command: Sequence[Any], index: int) -> Any: ...

    def _request_body_size(self, encoded: list[Any]) -> int: ...

    def _request_batch_with_deadline(
        self,
        commands: Sequence[Sequence[Any]],
        deadline: _HttpDeadline,
    ) -> tuple[list[Any], bool]: ...


class _CoalescedCall:
    def __init__(
        self,
        command: tuple[Any, ...],
        encoded: Any,
        deadline: _HttpDeadline,
        cancelled: threading.Event | None,
    ) -> None:
        self.command = command
        self.encoded = encoded
        self.deadline = deadline
        self.cancelled = cancelled
        self.event = threading.Event()
        self.result: Any = _UNSET
        self.error: BaseException | None = None


class _CoalescedBatch:
    def __init__(self) -> None:
        self.calls: list[_CoalescedCall] = []

    def can_accept(
        self,
        call: _CoalescedCall,
        *,
        adapter: HttpCoalescingAdapter,
        max_items: int,
        max_bytes: int,
    ) -> bool:
        if len(self.calls) >= max_items:
            return False
        if not self.calls:
            return True
        encoded = [existing.encoded for existing in self.calls] + [call.encoded]
        return adapter._request_body_size(encoded) <= max_bytes


class CommandCoalescer:
    """Briefly join independent calls without sharing their command outcomes."""

    def __init__(
        self,
        adapter: HttpCoalescingAdapter,
        *,
        window_ms: float,
        max_items: int,
        max_bytes: int,
        command_result: Callable[..., Any],
    ) -> None:
        self._adapter = adapter
        self._window_seconds = window_ms / 1_000
        self._max_items = max_items
        self._max_bytes = max_bytes
        self._command_result = command_result
        self._condition = threading.Condition()
        self._pending: _CoalescedBatch | None = None

    def execute(
        self,
        command: tuple[Any, ...],
        deadline: _HttpDeadline,
        cancelled: threading.Event | None = None,
    ) -> Any:
        call = _CoalescedCall(
            command,
            self._adapter._encode_command(command, 0),
            deadline,
            cancelled,
        )

        with self._condition:
            batch = self._pending
            leader = batch is None or not batch.can_accept(
                call,
                adapter=self._adapter,
                max_items=self._max_items,
                max_bytes=self._max_bytes,
            )
            if leader:
                batch = _CoalescedBatch()
                self._pending = batch
            if batch is None:
                raise RuntimeError("coalescing batch was not initialized")
            batch.calls.append(call)
            self._condition.notify_all()

        if leader:
            self._dispatch(batch)

        try:
            remaining = deadline.remaining()
        except TimeoutError as exc:
            raise _coalesced_timeout_error() from exc
        if not call.event.wait(remaining):
            raise _coalesced_timeout_error()
        if call.error is not None:
            raise call.error
        if call.result is _UNSET:
            raise RuntimeError("coalesced command completed without a result")
        return call.result

    def _dispatch(self, batch: _CoalescedBatch) -> None:
        expires_at = monotonic() + self._window_seconds
        with self._condition:
            while self._pending is batch and len(batch.calls) < self._max_items:
                self._remove_inactive_calls(batch)
                if not batch.calls:
                    break
                remaining = expires_at - monotonic()
                deadline_remaining = _minimum_remaining(call.deadline for call in batch.calls)
                if deadline_remaining is not None:
                    remaining = min(remaining, deadline_remaining)
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            if self._pending is batch:
                self._pending = None
            self._remove_inactive_calls(batch)
            calls = list(batch.calls)

        if not calls:
            return
        deadline = _earliest_deadline(call.deadline for call in calls)
        try:
            results, binary = self._adapter._request_batch_with_deadline(
                [call.command for call in calls],
                deadline,
            )
        except BaseException as exc:
            for call in calls:
                call.error = exc
                call.event.set()
            return

        for call, result in zip(calls, results, strict=True):
            try:
                call.result = self._command_result(result, binary=binary)
            except BaseException as exc:
                call.error = exc
            finally:
                call.event.set()

    @staticmethod
    def _remove_inactive_calls(batch: _CoalescedBatch) -> None:
        active: list[_CoalescedCall] = []
        for call in batch.calls:
            if call.cancelled is not None and call.cancelled.is_set():
                call.error = _coalesced_cancelled_error()
                call.event.set()
                continue
            try:
                call.deadline.remaining()
            except TimeoutError:
                call.error = _coalesced_timeout_error()
                call.event.set()
                continue
            active.append(call)
        batch.calls = active


def _coalesced_timeout_error() -> HttpError:
    return HttpError(
        "FerricStore HTTP coalesced request deadline exceeded",
        error_code="transport_timeout",
        retryable=True,
        safe_to_retry=False,
    )


def _coalesced_cancelled_error() -> HttpError:
    return HttpError(
        "FerricStore HTTP coalesced request was cancelled before dispatch",
        error_code="transport_cancelled",
        retryable=True,
        safe_to_retry=True,
    )


def _minimum_remaining(deadlines: Iterable[_HttpDeadline]) -> float | None:
    minimum: float | None = None
    for deadline in deadlines:
        try:
            remaining = deadline.remaining()
        except TimeoutError:
            return 0
        if remaining is not None and (minimum is None or remaining < minimum):
            minimum = remaining
    return minimum


def _earliest_deadline(deadlines: Iterable[_HttpDeadline]) -> _HttpDeadline:
    values = list(deadlines)
    finite = [deadline for deadline in values if deadline.expires_at is not None]
    if not finite:
        return values[0]
    return min(finite, key=lambda deadline: cast(float, deadline.expires_at))


__all__ = ["CommandCoalescer"]
