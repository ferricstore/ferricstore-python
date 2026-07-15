from __future__ import annotations

import builtins
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, cast

from ferricstore.batch_core import (
    CreateManyPlan,
)
from ferricstore.client_helpers import (
    _append,
    _append_attributes,
    _append_bool,
    _append_encoded,
    _append_named_values,
    _append_priority,
    _append_state_meta,
    _expand_many_response,
    _flow_return,
    _now_ms,
    _run_steps_many_args,
    _run_steps_many_items,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.errors import map_exception
from ferricstore.lifecycle_core import (
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.producer_commands import _create_many_args
from ferricstore.types import (
    CreateItem,
    FlowRecord,
)


class _ClientProducerMixin(_ClientMixinBase):
    if TYPE_CHECKING:
        _execute_producer_write: Callable[..., Any]
        _record_or_get: Callable[..., FlowRecord]
        _records_or_response: Callable[[Any], builtins.list[FlowRecord] | Any]

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
        retention_ttl_ms: int | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        now_ms = now_ms if now_ms is not None else _now_ms()
        args: builtins.list[Any] = ["FLOW.CREATE", id, "TYPE", type, "STATE", state, "NOW", now_ms]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "PARENT_FLOW_ID", parent_flow_id)
        _append(args, "ROOT_FLOW_ID", root_flow_id)
        _append(args, "CORRELATION_ID", correlation_id)
        _append(args, "RUN_AT", run_at_ms if run_at_ms is not None else now_ms)
        _append_priority(args, priority)
        _append_bool(args, "IDEMPOTENT", idempotent)
        _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
        _append_attributes(args, attributes=attributes)
        _append_state_meta(args, state_meta)
        _append_named_values(args, self.codec, values=values, value_refs=value_refs)
        response = self._execute_producer_write(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def enqueue(
        self,
        id: str,
        *,
        type: str,
        payload: Any = None,
        state: str = "queued",
        partition_key: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = 0,
        idempotent: bool | None = None,
        retention_ttl_ms: int | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        """Create a queued flow using the optimized ack-only path by default."""
        return self.create(
            id,
            type=type,
            state=state,
            payload=payload,
            partition_key=partition_key,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
            priority=priority,
            idempotent=idempotent,
            retention_ttl_ms=retention_ttl_ms,
            attributes=attributes,
            state_meta=state_meta,
            values=values,
            value_refs=value_refs,
            return_record=return_record,
        )

    def start_and_claim(
        self,
        id: str,
        *,
        type: str,
        initial_state: str,
        worker: str,
        lease_ms: int = 30_000,
        payload: Any = None,
        partition_key: str | None = None,
        parent_flow_id: str | None = None,
        root_flow_id: str | None = None,
        correlation_id: str | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        retention_ttl_ms: int | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
    ) -> FlowRecord:
        now_ms = now_ms if now_ms is not None else _now_ms()
        args: builtins.list[Any] = [
            "FLOW.START_AND_CLAIM",
            id,
            "TYPE",
            type,
            "INITIAL_STATE",
            initial_state,
            "WORKER",
            worker,
            "LEASE_MS",
            lease_ms,
            "NOW",
            now_ms,
        ]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "PARENT_FLOW_ID", parent_flow_id)
        _append(args, "ROOT_FLOW_ID", root_flow_id)
        _append(args, "CORRELATION_ID", correlation_id)
        _append_priority(args, priority)
        _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
        _append_attributes(args, attributes=attributes)
        _append_state_meta(args, state_meta)
        _append_named_values(args, self.codec, values=values, value_refs=value_refs)
        response = self.executor.execute_command(*args)
        return self._record_or_get(response, id, partition_key)

    def run_steps_many(
        self,
        items: builtins.list[str | dict[str, Any] | CreateItem],
        *,
        type: str,
        states: Sequence[str] | None = None,
        steps: int | None = None,
        worker: str,
        lease_ms: int = 30_000,
        now_ms: int | None = None,
        payload: Any = None,
        result: Any = None,
        partition_key: str | None = None,
        retention_ttl_ms: int | None = None,
    ) -> bytes:
        """Run deterministic workflow step chains in one durable Flow command.

        This is the low-latency workflow-continuation path: FerricStore writes the
        created/step/completed state and history for each item in one Ra command.
        Use it only when the step chain is deterministic and does not need worker
        code between individual states.
        """
        if not items:
            return b"OK"
        args = _run_steps_many_args(
            self.codec,
            items,
            type=type,
            states=states,
            steps=steps,
            worker=worker,
            lease_ms=lease_ms,
            now_ms=now_ms,
            payload=payload,
            result=result,
            partition_key=partition_key,
            retention_ttl_ms=retention_ttl_ms,
        )
        return cast(bytes, _flow_return(self.executor.execute_command(*args)))

    @staticmethod
    def _run_steps_many_items(
        items: builtins.list[str | dict[str, Any] | CreateItem],
        partition_key: str | None,
    ) -> builtins.list[dict[str, str]]:
        return _run_steps_many_items(items, partition_key)

    def enqueue_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str,
        state: str = "queued",
        partition_key: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = 0,
        idempotent: bool | None = None,
        independent: bool | None = True,
        return_ok_on_success: bool = False,
        retention_ttl_ms: int | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
    ) -> builtins.list[Any] | Any:
        """Create many queued flows, grouping no-partition items by auto bucket."""
        if not items:
            return []

        if partition_key is not None:
            return self.create_many(
                partition_key,
                items,
                type=type,
                state=state,
                run_at_ms=run_at_ms,
                now_ms=now_ms,
                priority=priority,
                idempotent=idempotent,
                independent=independent,
                return_ok_on_success=return_ok_on_success,
                retention_ttl_ms=retention_ttl_ms,
                values=values,
                value_refs=value_refs,
                attributes=attributes,
                state_meta=state_meta,
            )

        plan = CreateManyPlan.build(items, now_ms=now_ms, clock=_now_ms)
        commands = [
            _create_many_args(
                self.codec,
                group.partition_key,
                group.items,
                type=type,
                state=state,
                run_at_ms=run_at_ms,
                now_ms=plan.now_ms,
                priority=priority,
                idempotent=idempotent,
                independent=independent,
                return_ok_on_success=return_ok_on_success,
                retention_ttl_ms=retention_ttl_ms,
                values=values,
                value_refs=value_refs,
                attributes=attributes,
                state_meta=state_meta,
            )
            for group in plan.groups
        ]

        def execute_group(command: builtins.list[Any]) -> Any:
            return self._execute_producer_write(*command)

        raw_executor = getattr(self.executor, "_executor", self.executor)
        responses = self._fanout_executor.run(
            commands,
            execute_group,
            concurrent=getattr(raw_executor, "supports_concurrent_fanout", False) is True,
        )

        decoded_responses = [self._records_or_response(response) for response in responses]
        return plan.merge(decoded_responses, _expand_many_response)

    def create_many(
        self,
        partition_key: str | None,
        items: builtins.list[CreateItem],
        *,
        type: str,
        state: str = "queued",
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
        independent: bool | None = None,
        return_ok_on_success: bool = False,
        retention_ttl_ms: int | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args = _create_many_args(
            self.codec,
            partition_key,
            items,
            type=type,
            state=state,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
            priority=priority,
            idempotent=idempotent,
            independent=independent,
            return_ok_on_success=return_ok_on_success,
            retention_ttl_ms=retention_ttl_ms,
            values=values,
            value_refs=value_refs,
            attributes=attributes,
            state_meta=state_meta,
        )
        return self._records_or_response(self._execute_producer_write(*args))

    def submit_create_many(
        self,
        partition_key: str | None,
        items: builtins.list[CreateItem],
        *,
        type: str,
        state: str = "queued",
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
        independent: bool | None = None,
        return_ok_on_success: bool = False,
        retention_ttl_ms: int | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
    ) -> Future[Any]:
        future: Future[Any] = Future()
        if not items:
            future.set_result([])
            return future

        submit_command = getattr(self.executor, "submit_command", None)
        if not callable(submit_command):
            future.set_exception(RuntimeError("submit_create_many requires async executor support"))
            return future

        args = _create_many_args(
            self.codec,
            partition_key,
            items,
            type=type,
            state=state,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
            priority=priority,
            idempotent=idempotent,
            independent=independent,
            return_ok_on_success=return_ok_on_success,
            retention_ttl_ms=retention_ttl_ms,
            values=values,
            value_refs=value_refs,
            attributes=attributes,
            state_meta=state_meta,
        )
        source = submit_command(*args)
        future.set_running_or_notify_cancel()

        def complete(source_future: Future[Any]) -> None:
            if future.cancelled():
                return
            try:
                value = self._records_or_response(source_future.result())
            except Exception as exc:
                mapped = map_exception(exc)
                try_set_future_exception(future, mapped if mapped is not exc else exc)
            else:
                try_set_future_result(future, value)

        source.add_done_callback(complete)
        return future
