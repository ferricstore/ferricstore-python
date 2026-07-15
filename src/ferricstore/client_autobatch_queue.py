from __future__ import annotations

from concurrent.futures import Future
from typing import Any, Protocol


class QueuedBatchOp(Protocol):
    future: Future[Any]
    queued: bool
    cancelled_while_queued: bool


class AutobatchQueueHost(Protocol):
    max_batch: int
    _cancelled_pending: int
    _condition: Any
    _pending: Any

    def _process_ops(self, ops: list[Any]) -> None: ...


def take_pending_locked(host: AutobatchQueueHost) -> list[Any]:
    count = min(host.max_batch, len(host._pending))
    ops = [host._pending.popleft() for _ in range(count)]
    cancelled_pending = host.__dict__.get("_cancelled_pending", 0)
    for op in ops:
        op.queued = False
        if op.cancelled_while_queued:
            cancelled_pending -= 1
    host._cancelled_pending = max(cancelled_pending, 0)
    host._condition.notify_all()
    return ops


def pending_future_done(
    host: AutobatchQueueHost,
    op: QueuedBatchOp,
    future: Future[Any],
) -> None:
    if not future.cancelled():
        return
    with host._condition:
        if op.queued and not op.cancelled_while_queued:
            op.cancelled_while_queued = True
            host._cancelled_pending = host.__dict__.get("_cancelled_pending", 0) + 1
            host._condition.notify_all()


def drain_reentrant_until(host: AutobatchQueueHost, target: Future[Any]) -> None:
    while not target.done():
        with host._condition:
            ops = take_pending_locked(host)
        if not ops:
            raise RuntimeError("autobatch reentrant operation was not queued")
        host._process_ops(ops)


__all__ = [
    "AutobatchQueueHost",
    "QueuedBatchOp",
    "drain_reentrant_until",
    "pending_future_done",
    "take_pending_locked",
]
