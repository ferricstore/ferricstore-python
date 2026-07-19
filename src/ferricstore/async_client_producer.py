from __future__ import annotations

import builtins
from collections.abc import Sequence
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.batch_core import (
    CreateManyGroup,
    CreateManyPlan,
    require_batch_items,
    run_async_fanout,
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
)
from ferricstore.config_validation import normalize_optional_max_active_ms
from ferricstore.model_core import _normalize_ref_meta
from ferricstore.producer_commands import _create_many_args
from ferricstore.types import CreateItem, FlowRecord


class _AsyncClientProducerMixin(_AsyncClientMixinBase):
    async def create(
        self,
        id: str,
        *,
        type: str,
        state: str = "queued",
        payload: Any = None,
        partition_key: str | bytes | None = None,
        parent_flow_id: str | None = None,
        root_flow_id: str | None = None,
        correlation_id: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
        retention_ttl_ms: int | None = None,
        max_active_ms: int | float | str | None = None,
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
        _append(args, "MAX_ACTIVE_MS", normalize_optional_max_active_ms(max_active_ms))
        _append_attributes(args, attributes=attributes)
        _append_state_meta(args, state_meta)
        _append_named_values(args, self.codec, values=values, value_refs=value_refs)
        response = await self._execute_producer_write(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def enqueue(
        self,
        id: str,
        *,
        type: str,
        payload: Any = None,
        state: str = "queued",
        partition_key: str | bytes | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = 0,
        idempotent: bool | None = None,
        retention_ttl_ms: int | None = None,
        max_active_ms: int | float | str | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        return await self.create(
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
            max_active_ms=max_active_ms,
            attributes=attributes,
            state_meta=state_meta,
            values=values,
            value_refs=value_refs,
            return_record=return_record,
        )

    async def start_and_claim(
        self,
        id: str,
        *,
        type: str,
        initial_state: str,
        worker: str,
        lease_ms: int = 30_000,
        payload: Any = None,
        partition_key: str | bytes | None = None,
        parent_flow_id: str | None = None,
        root_flow_id: str | None = None,
        correlation_id: str | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        retention_ttl_ms: int | None = None,
        max_active_ms: int | float | str | None = None,
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
        _append(args, "MAX_ACTIVE_MS", normalize_optional_max_active_ms(max_active_ms))
        _append_attributes(args, attributes=attributes)
        _append_state_meta(args, state_meta)
        _append_named_values(args, self.codec, values=values, value_refs=value_refs)
        response = await self.executor.execute_command(*args)
        return await self._record_or_get(response, id, partition_key)

    async def run_steps_many(
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
        partition_key: str | bytes | None = None,
        retention_ttl_ms: int | None = None,
    ) -> bytes:
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
        return cast(bytes, _flow_return(await self.executor.execute_command(*args)))

    async def enqueue_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str,
        state: str = "queued",
        partition_key: str | bytes | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = 0,
        idempotent: bool | None = None,
        independent: bool | None = True,
        return_ok_on_success: bool = False,
        retention_ttl_ms: int | None = None,
        max_active_ms: int | float | str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
    ) -> builtins.list[Any] | Any:
        if not items:
            return []

        if partition_key is not None:
            return await self.create_many(
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
                max_active_ms=max_active_ms,
                values=values,
                value_refs=value_refs,
                attributes=attributes,
                state_meta=state_meta,
            )

        plan = CreateManyPlan.build(items, now_ms=now_ms, clock=_now_ms)

        async def create_group(group: CreateManyGroup) -> Any:
            return await self.create_many(
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
                max_active_ms=max_active_ms,
                values=values,
                value_refs=value_refs,
                attributes=attributes,
                state_meta=state_meta,
            )

        raw_executor = getattr(self.executor, "_executor", self.executor)
        responses = await run_async_fanout(
            plan.groups,
            create_group,
            concurrent=getattr(raw_executor, "supports_concurrent_fanout", False) is True,
        )

        return plan.merge(responses, _expand_many_response)

    async def create_many(
        self,
        partition_key: str | bytes | None,
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
        max_active_ms: int | float | str | None = None,
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
            max_active_ms=max_active_ms,
            values=values,
            value_refs=value_refs,
            attributes=attributes,
            state_meta=state_meta,
        )
        return self._records_or_response(await self._execute_producer_write(*args))

    async def value_put(
        self,
        value: Any,
        *,
        partition_key: str | bytes | None = None,
        owner_flow_id: str | None = None,
        name: str | None = None,
        override: bool | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> Any:
        args: builtins.list[Any] = [
            "FLOW.VALUE.PUT",
            self.codec.encode(value),
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "OWNER_FLOW_ID", owner_flow_id)
        _append(args, "NAME", name)
        _append_bool(args, "OVERRIDE", override)
        _append(args, "TTL", ttl_ms)
        return await self.executor.execute_command(*args)

    async def value_mget(
        self, refs: builtins.list[str], *, max_bytes: int | None = None
    ) -> builtins.list[Any]:
        if not refs:
            return []
        args: builtins.list[Any] = ["FLOW.VALUE.MGET", *refs]
        _append(args, "MAX_BYTES", max_bytes)
        response = await self.executor.execute_command(*args)
        items = require_batch_items(response, len(refs), operation="FLOW.VALUE.MGET")
        return [
            _normalize_ref_meta(value)
            if isinstance(value, dict)
            else value
            if value is None
            else self.codec.decode(value)
            for value in items
        ]

    async def signal(
        self,
        id: str,
        *,
        signal: str,
        partition_key: str | bytes | None = None,
        idempotency_key: str | None = None,
        if_state: str | builtins.list[str] | tuple[str, ...] | None = None,
        transition_to: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
    ) -> Any:
        args: builtins.list[Any] = ["FLOW.SIGNAL", id, "SIGNAL", signal]
        _append(args, "PARTITION", partition_key)
        _append(args, "IDEMPOTENCY", idempotency_key)
        if isinstance(if_state, (list, tuple)):
            for state in if_state:
                _append(args, "IF_STATE", state)
        else:
            _append(args, "IF_STATE", if_state)
        _append(args, "TRANSITION_TO", transition_to)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_priority(args, priority)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        return await self.executor.execute_command(*args)

    async def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)
