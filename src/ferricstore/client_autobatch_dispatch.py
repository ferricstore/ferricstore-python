from __future__ import annotations

import threading
from collections import deque
from typing import Any, Protocol


class AutobatchDispatcherHost(Protocol):
    _condition: threading.Condition
    _pending: deque[Any]
    _closed: bool
    _terminal_error: BaseException | None

    def _take_batch(self) -> list[Any]: ...

    def _process_ops(self, ops: list[Any]) -> None: ...

    def _fail_group(self, group: list[Any], exc: BaseException) -> None: ...


def run_autobatch_dispatcher(host: AutobatchDispatcherHost) -> None:
    """Dispatch batches and atomically publish any terminal worker failure."""
    try:
        while True:
            ops = host._take_batch()
            if not ops:
                return
            host._process_ops(ops)
    except BaseException as exc:
        with host._condition:
            host._closed = True
            host._terminal_error = exc
            pending = list(host._pending)
            host._pending.clear()
            host._condition.notify_all()
        host._fail_group(pending, exc)
