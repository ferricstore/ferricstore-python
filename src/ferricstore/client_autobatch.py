from __future__ import annotations

import atexit
import builtins
import contextlib
import threading
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any

from ferricstore.client_autobatch_dispatch import (
    flush_autobatch_ops,
    run_autobatch_dispatcher,
)
from ferricstore.client_autobatch_queue import (
    drain_reentrant_until,
    pending_future_done,
    take_pending_locked,
)
from ferricstore.client_helpers import (
    _FLOW_MANY_BATCH_LIMIT,
    _batch_key_value,
    _batch_named_key,
    _flow_return,
)
from ferricstore.config_validation import (
    normalize_optional_max_active_ms,
    validate_positive_int,
    validate_thread_wait_milliseconds,
    validate_thread_wait_seconds,
)
from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import (
    DeferredCallbackFuture,
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.types import (
    ClaimedFlow,
    CreateItem,
    FencedItem,
    FlowRecord,
)
from ferricstore.worker_core import many_item_error

if TYPE_CHECKING:
    from ferricstore.client_core import FlowClient


@dataclass
class _BatchOp:
    kind: str
    key: tuple[Any, ...]
    args: dict[str, Any]
    future: Future[Any]
    queued: bool = False
    cancelled_while_queued: bool = False


class AutobatchFlowClient:
    """Thread-safe auto-batching wrapper for hot Flow write commands."""

    def __init__(
        self,
        client: FlowClient,
        *,
        max_batch: int = 100,
        max_delay_ms: float = 1.0,
        max_pending: int = 10_000,
    ) -> None:
        max_batch = validate_positive_int(max_batch, name="max_batch")
        max_pending = validate_positive_int(max_pending, name="max_pending")
        delay_ms = validate_thread_wait_milliseconds(max_delay_ms, name="max_delay_ms")
        delay_s = delay_ms / 1000.0
        self.client = client
        self.max_batch = min(_FLOW_MANY_BATCH_LIMIT, max(1, max_batch))
        self.max_delay_s = delay_s
        self.max_pending = max_pending
        self._condition = threading.Condition()
        self._pending: deque[_BatchOp] = deque()
        self._cancelled_pending = 0
        self._closed = False
        self._terminal_error: BaseException | None = None
        self._worker = threading.Thread(
            target=self._run, name="ferricstore-flow-autobatch", daemon=True
        )
        self._worker.start()
        atexit.register(self._close_at_exit)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def close(self, timeout: float | None = 5.0) -> None:
        if timeout is not None:
            timeout = validate_thread_wait_seconds(timeout, name="close timeout")
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        if threading.current_thread() is self._worker:
            with contextlib.suppress(Exception):
                atexit.unregister(self._close_at_exit)
            return
        if self._worker.is_alive():
            self._worker.join(timeout=timeout)
        if self._worker.is_alive():
            raise TimeoutError("AutobatchFlowClient close timed out")
        with contextlib.suppress(Exception):
            atexit.unregister(self._close_at_exit)

    def _close_at_exit(self) -> None:
        with contextlib.suppress(TimeoutError):
            self.close(timeout=1.0)

    def flush(self) -> None:
        marker: Future[Any] = DeferredCallbackFuture()
        self._enqueue(_BatchOp("flush", ("flush", id(marker)), {}, marker))
        marker.result()

    def create_async(
        self,
        id: str,
        *,
        type: str,
        state: str = "queued",
        payload: Any = None,
        partition_key: str | None = None,
        parent_flow_id: str | None = None,
        root_flow_id: str | None = None,
        correlation_id: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
        max_active_ms: int | float | str | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> Future[FlowRecord | bytes]:
        max_active_ms = normalize_optional_max_active_ms(max_active_ms)
        future: Future[FlowRecord | bytes] = DeferredCallbackFuture()
        if (
            return_record
            or parent_flow_id is not None
            or root_flow_id is not None
            or correlation_id is not None
            or state_meta is not None
        ):
            self._enqueue(
                _BatchOp(
                    "direct",
                    ("direct", builtins.id(future)),
                    {
                        "call": lambda: self.client.create(
                            id,
                            type=type,
                            state=state,
                            payload=payload,
                            partition_key=partition_key,
                            parent_flow_id=parent_flow_id,
                            root_flow_id=root_flow_id,
                            correlation_id=correlation_id,
                            run_at_ms=run_at_ms,
                            now_ms=now_ms,
                            priority=priority,
                            idempotent=idempotent,
                            max_active_ms=max_active_ms,
                            state_meta=state_meta,
                            values=values,
                            value_refs=value_refs,
                            return_record=return_record,
                        )
                    },
                    future,
                )
            )
            return future

        auto_partition = partition_key is None
        batch_partition_key = partition_key
        batch_key = (
            (
                "create-auto",
                type,
                state,
                run_at_ms,
                now_ms,
                priority,
                idempotent,
                max_active_ms,
            )
            if auto_partition
            else (
                "create",
                type,
                state,
                run_at_ms,
                now_ms,
                priority,
                idempotent,
                max_active_ms,
            )
        )
        self._enqueue(
            _BatchOp(
                "create",
                batch_key,
                {
                    "id": id,
                    "type": type,
                    "state": state,
                    "payload": payload,
                    "partition_key": batch_partition_key,
                    "run_at_ms": run_at_ms,
                    "now_ms": now_ms,
                    "priority": priority,
                    "idempotent": idempotent,
                    "max_active_ms": max_active_ms,
                    "values": values,
                    "value_refs": value_refs,
                },
                future,
            )
        )
        return future

    def create(
        self,
        id: str,
        *,
        type: str,
        state: str = "queued",
        payload: Any = None,
        partition_key: str | None = None,
        parent_flow_id: str | None = None,
        root_flow_id: str | None = None,
        correlation_id: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
        max_active_ms: int | float | str | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        return self.create_async(
            id,
            type=type,
            state=state,
            payload=payload,
            partition_key=partition_key,
            parent_flow_id=parent_flow_id,
            root_flow_id=root_flow_id,
            correlation_id=correlation_id,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
            priority=priority,
            idempotent=idempotent,
            max_active_ms=max_active_ms,
            state_meta=state_meta,
            values=values,
            value_refs=value_refs,
            return_record=return_record,
        ).result()

    def complete_async(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        result: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> Future[FlowRecord | bytes]:
        future: Future[FlowRecord | bytes] = DeferredCallbackFuture()
        if return_record or partition_key is None or state_meta is not None:
            self._enqueue(
                _BatchOp(
                    "direct",
                    ("direct", builtins.id(future)),
                    {
                        "call": lambda: self.client.complete(
                            id,
                            lease_token=lease_token,
                            fencing_token=fencing_token,
                            partition_key=partition_key,
                            result=result,
                            payload=payload,
                            values=values,
                            value_refs=value_refs,
                            drop_values=drop_values,
                            override_values=override_values,
                            state_meta=state_meta,
                            ttl_ms=ttl_ms,
                            now_ms=now_ms,
                            return_record=return_record,
                        )
                    },
                    future,
                )
            )
            return future

        self._enqueue(
            _BatchOp(
                "complete",
                (
                    "complete",
                    _batch_key_value(result),
                    _batch_key_value(payload),
                    ttl_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "result": result,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "ttl_ms": ttl_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return future

    def complete(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        result: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        return self.complete_async(
            id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            partition_key=partition_key,
            result=result,
            payload=payload,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
            state_meta=state_meta,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            return_record=return_record,
        ).result()

    def transition(
        self,
        id: str,
        *,
        from_state: str,
        to_state: str,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        if return_record or partition_key is None or state_meta is not None:
            return self.client.transition(
                id,
                from_state=from_state,
                to_state=to_state,
                lease_token=lease_token,
                fencing_token=fencing_token,
                partition_key=partition_key,
                payload=payload,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                state_meta=state_meta,
                run_at_ms=run_at_ms,
                now_ms=now_ms,
                priority=priority,
                return_record=return_record,
            )

        future: Future[Any] = DeferredCallbackFuture()
        self._enqueue(
            _BatchOp(
                "transition",
                (
                    "transition",
                    from_state,
                    to_state,
                    _batch_key_value(payload),
                    run_at_ms,
                    now_ms,
                    priority,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "from_state": from_state,
                    "to_state": to_state,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "run_at_ms": run_at_ms,
                    "now_ms": now_ms,
                    "priority": priority,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def retry(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        if return_record or partition_key is None or state_meta is not None:
            return self.client.retry(
                id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                partition_key=partition_key,
                error=error,
                payload=payload,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                state_meta=state_meta,
                run_at_ms=run_at_ms,
                now_ms=now_ms,
                return_record=return_record,
            )

        future: Future[Any] = DeferredCallbackFuture()
        self._enqueue(
            _BatchOp(
                "retry",
                (
                    "retry",
                    _batch_key_value(error),
                    _batch_key_value(payload),
                    run_at_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "error": error,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "run_at_ms": run_at_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def fail(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        if return_record or partition_key is None or state_meta is not None:
            return self.client.fail(
                id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                partition_key=partition_key,
                error=error,
                payload=payload,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                state_meta=state_meta,
                ttl_ms=ttl_ms,
                now_ms=now_ms,
                return_record=return_record,
            )

        future: Future[Any] = DeferredCallbackFuture()
        self._enqueue(
            _BatchOp(
                "fail",
                (
                    "fail",
                    _batch_key_value(error),
                    _batch_key_value(payload),
                    ttl_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "error": error,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "ttl_ms": ttl_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def cancel(
        self,
        id: str,
        *,
        fencing_token: int,
        lease_token: bytes | None = None,
        partition_key: str | None = None,
        reason: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        if return_record or partition_key is None or state_meta is not None:
            return self.client.cancel(
                id,
                fencing_token=fencing_token,
                lease_token=lease_token,
                partition_key=partition_key,
                reason=reason,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                state_meta=state_meta,
                ttl_ms=ttl_ms,
                now_ms=now_ms,
                return_record=return_record,
            )

        future: Future[Any] = DeferredCallbackFuture()
        self._enqueue(
            _BatchOp(
                "cancel",
                (
                    "cancel",
                    _batch_key_value(reason),
                    ttl_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "reason": reason,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "ttl_ms": ttl_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def _enqueue(self, op: _BatchOp) -> None:
        reentrant = threading.current_thread() is self.__dict__.get("_worker")
        while True:
            immediate: builtins.list[_BatchOp] = []
            with self._condition:
                while len(self._pending) >= self.max_pending and not self._closed:
                    if reentrant:
                        immediate = take_pending_locked(self)
                        break
                    self._condition.wait()
                if self._closed:
                    if self._terminal_error is not None:
                        raise RuntimeError("autobatch dispatcher failed") from self._terminal_error
                    raise RuntimeError("autobatch client is closed")
                if not immediate:
                    op.queued = True
                    self._pending.append(op)
                    op.future.add_done_callback(partial(pending_future_done, self, op))
                    self._condition.notify()
                    break
            self._process_ops(immediate)

        if reentrant:
            drain_reentrant_until(self, op.future)

    def _run(self) -> None:
        run_autobatch_dispatcher(self)

    def _process_ops(self, ops: builtins.list[_BatchOp]) -> None:
        active = [op for op in ops if op.future.set_running_or_notify_cancel()]
        if not active:
            return
        try:
            self._flush_ops(active)
        except BaseException as exc:
            self._fail_group(active, exc)

    def _take_batch(self) -> builtins.list[_BatchOp]:
        with self._condition:
            while not self._pending and not self._closed:
                self._condition.wait()
            if not self._pending and self._closed:
                return []

            deadline = time.monotonic() + self.max_delay_s
            while (
                len(self._pending) < self.max_batch
                and not self._closed
                and not self.__dict__.get("_cancelled_pending", 0)
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)

            return take_pending_locked(self)

    def _flush_ops(self, ops: builtins.list[_BatchOp]) -> None:
        flush_autobatch_ops(self, ops)

    def _flush_group(self, group: builtins.list[_BatchOp]) -> None:
        kind = group[0].kind
        try:
            if kind == "create":
                partition_keys = {op.args["partition_key"] for op in group}
                partition_key = next(iter(partition_keys)) if len(partition_keys) == 1 else None
                response = self.client.create_many(
                    partition_key,
                    [
                        CreateItem(
                            op.args["id"],
                            op.args["payload"],
                            partition_key=(
                                None if partition_key is not None else op.args["partition_key"]
                            ),
                            values=op.args.get("values"),
                            value_refs=op.args.get("value_refs"),
                        )
                        for op in group
                    ],
                    type=group[0].args["type"],
                    state=group[0].args["state"],
                    run_at_ms=group[0].args["run_at_ms"],
                    now_ms=group[0].args["now_ms"],
                    priority=group[0].args["priority"],
                    idempotent=group[0].args["idempotent"],
                    max_active_ms=group[0].args["max_active_ms"],
                    independent=True,
                )
            elif kind == "complete":
                response = self.client.complete_many(
                    None,
                    [self._claimed_item(op) for op in group],
                    result=group[0].args["result"],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    ttl_ms=group[0].args["ttl_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            elif kind == "transition":
                response = self.client.transition_many(
                    None,
                    from_state=group[0].args["from_state"],
                    to_state=group[0].args["to_state"],
                    items=[self._fenced_item(op, include_lease=True) for op in group],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    run_at_ms=group[0].args["run_at_ms"],
                    now_ms=group[0].args["now_ms"],
                    priority=group[0].args["priority"],
                    independent=True,
                )
            elif kind == "retry":
                response = self.client.retry_many(
                    None,
                    [self._claimed_item(op) for op in group],
                    error=group[0].args["error"],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    run_at_ms=group[0].args["run_at_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            elif kind == "fail":
                response = self.client.fail_many(
                    None,
                    [self._claimed_item(op) for op in group],
                    error=group[0].args["error"],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    ttl_ms=group[0].args["ttl_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            elif kind == "cancel":
                response = self.client.cancel_many(
                    None,
                    [self._fenced_item(op, include_lease=False) for op in group],
                    reason=group[0].args["reason"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    ttl_ms=group[0].args["ttl_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            else:
                raise RuntimeError(f"unknown batch op {kind!r}")
        except BaseException as exc:
            self._fail_group(group, exc)
            return

        self._complete_group(group, response)

    def _complete_group(self, group: builtins.list[_BatchOp], response: Any) -> None:
        deferred = self._defer_group_callbacks(group)
        try:
            if isinstance(response, list):
                if len(response) != len(group):
                    error = FerricStoreError(
                        "autobatch response cardinality does not match request group",
                        raw=response,
                    )
                    for op in group:
                        try_set_future_exception(op.future, error)
                    return
                for op, item in zip(group, response, strict=True):
                    item_error = many_item_error(item)
                    if item_error is None:
                        try_set_future_result(op.future, item)
                    else:
                        try_set_future_exception(op.future, item_error)
                return
            for op in group:
                try_set_future_result(op.future, response)
        finally:
            self._release_group_callbacks(deferred)

    def _fail_group(self, group: builtins.list[_BatchOp], exc: BaseException) -> None:
        deferred = self._defer_group_callbacks(group)
        try:
            for op in group:
                try_set_future_exception(op.future, exc)
        finally:
            self._release_group_callbacks(deferred)

    @staticmethod
    def _defer_group_callbacks(
        group: builtins.list[_BatchOp],
    ) -> builtins.list[DeferredCallbackFuture]:
        deferred = [op.future for op in group if isinstance(op.future, DeferredCallbackFuture)]
        for future in deferred:
            future.defer_callbacks()
        return deferred

    @staticmethod
    def _release_group_callbacks(futures: builtins.list[DeferredCallbackFuture]) -> None:
        for future in futures:
            future.release_callbacks()

    def _claimed_item(self, op: _BatchOp) -> ClaimedFlow:
        return ClaimedFlow(
            op.args["id"],
            op.args["lease_token"],
            op.args["fencing_token"],
            partition_key=op.args["partition_key"],
        )

    def _fenced_item(self, op: _BatchOp, *, include_lease: bool) -> FencedItem:
        return FencedItem(
            op.args["id"],
            op.args["fencing_token"],
            op.args["lease_token"] if include_lease else None,
            partition_key=op.args["partition_key"],
        )
