from __future__ import annotations

import threading
from collections import deque
from typing import Any, Protocol

from ferricstore.lifecycle_core import (
    try_set_future_exception,
    try_set_future_result,
)


class AutobatchDispatcherHost(Protocol):
    _condition: threading.Condition
    _pending: deque[Any]
    _closed: bool
    _terminal_error: BaseException | None

    def _take_batch(self) -> list[Any]: ...

    def _process_ops(self, ops: list[Any]) -> None: ...

    def _flush_group(self, group: list[Any]) -> None: ...

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


def flush_autobatch_ops(host: AutobatchDispatcherHost, ops: list[Any]) -> None:
    """Group adjacent compatible operations and dispatch them in wire order."""
    groups: list[list[Any]] = []
    for op in ops:
        if not groups or groups[-1][0].key != op.key:
            groups.append([op])
        else:
            groups[-1].append(op)
    for group in groups:
        if group[0].kind == "flush":
            for op in group:
                try_set_future_result(op.future, None)
            continue
        if group[0].kind == "direct":
            for op in group:
                try:
                    try_set_future_result(op.future, op.args["call"]())
                except BaseException as exc:
                    try_set_future_exception(op.future, exc)
            continue
        host._flush_group(group)
