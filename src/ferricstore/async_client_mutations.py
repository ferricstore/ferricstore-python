from __future__ import annotations

import builtins
import inspect
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.batch_core import (
    require_batch_items,
)
from ferricstore.client_claim_options import _claim_due_command_args
from ferricstore.client_helpers import (
    _append,
    _append_attributes,
    _append_bool,
    _append_named_values,
    _append_priority,
    _append_state_meta,
    _complete_command_args,
    _complete_jobs_command_args,
    _complete_many_args,
    _fail_command_args,
    _flow_return,
    _job_mutation_command_args,
    _now_ms,
    _retry_command_args,
    _step_continue_args,
    _transition_command_args,
)
from ferricstore.errors import FerricStoreError, map_exception
from ferricstore.mutation_core import JobMutation, MutationKind
from ferricstore.types import (
    ClaimedFlow,
    FencedItem,
    FlowRecord,
)
from ferricstore.worker_core import validate_many_result


class _AsyncClientMutationsMixin(_AsyncClientMixinBase):
    async def transition(
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
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args = _transition_command_args(
            self.codec,
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
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
            state_meta=state_meta,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
            priority=priority,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def step_continue(
        self,
        id: str,
        *,
        lease_token: bytes,
        from_state: str,
        to_state: str,
        fencing_token: int,
        lease_ms: int = 30_000,
        partition_key: str | None = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        now_ms: int | None = None,
        worker: str | None = None,
        return_job: bool = False,
    ) -> FlowRecord | ClaimedFlow:
        args = _step_continue_args(
            self.codec,
            id,
            lease_token=lease_token,
            from_state=from_state,
            to_state=to_state,
            fencing_token=fencing_token,
            lease_ms=lease_ms,
            partition_key=partition_key,
            payload=payload,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
            state_meta=state_meta,
            now_ms=now_ms,
            worker=worker,
            return_job=return_job,
        )
        response = await self.executor.execute_command(*args)
        if return_job:
            return ClaimedFlow.from_resp(response)
        return await self._record_or_get(response, id, partition_key)

    async def complete_many(
        self,
        partition_key: str | None,
        items: builtins.list[ClaimedFlow],
        *,
        result: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
        return_ok_on_success: bool = False,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args = _complete_many_args(
            self.codec,
            partition_key,
            items,
            result=result,
            payload=payload,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
            state_meta=state_meta,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
            return_ok_on_success=return_ok_on_success,
        )
        return self._records_or_response(await self.executor.execute_command(*args))

    async def complete_jobs(
        self,
        jobs: builtins.list[ClaimedFlow],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
        return_ok_on_success: bool = False,
    ) -> builtins.list[FlowRecord] | Any:
        if not jobs:
            return []

        first_partition = jobs[0].partition_key
        partition_key = (
            first_partition
            if first_partition is not None
            and all(job.partition_key == first_partition for job in jobs)
            else None
        )
        return await self.complete_many(
            partition_key,
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
            return_ok_on_success=return_ok_on_success,
        )

    async def complete_flows_and_claim_flows(
        self,
        jobs: builtins.list[ClaimedFlow],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
        type: str,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        lease_ms: int = 30_000,
        limit: int = 100,
        priority: int | None = 0,
        claim_now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> builtins.list[ClaimedFlow]:
        """Complete claimed flows and claim more in one async transport round trip."""
        if not jobs:
            return cast(
                builtins.list[ClaimedFlow],
                await self.claim_flows(
                    type,
                    state=state,
                    states=states,
                    worker=worker,
                    partition_key=partition_key,
                    partition_keys=partition_keys,
                    lease_ms=lease_ms,
                    limit=limit,
                    priority=priority,
                    now_ms=claim_now_ms,
                    block_ms=block_ms,
                    reclaim_expired=reclaim_expired,
                    reclaim_ratio=reclaim_ratio,
                    include_state=include_state,
                    include_attributes=include_attributes,
                ),
            )

        complete_args = _complete_jobs_command_args(
            self.codec,
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
        )
        claim_args = _claim_due_command_args(
            type,
            state=state,
            states=states,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            lease_ms=lease_ms,
            limit=limit,
            priority=priority,
            now_ms=claim_now_ms,
            block_ms=block_ms,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            include_record=False,
            include_state=include_state,
            include_attributes=include_attributes,
        )
        responses = await self._execute_command_batch([tuple(complete_args), tuple(claim_args)])
        if len(responses) != 2:
            raise FerricStoreError(
                "complete-and-claim batch returned invalid response cardinality",
                raw=responses,
            )
        validate_many_result(
            responses[0],
            len(jobs),
            operation="FLOW.COMPLETE_MANY",
        )
        return ClaimedFlow.from_compact_rows(responses[1])

    async def complete_jobs_and_claim_jobs(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> builtins.list[ClaimedFlow]:
        return await self.complete_flows_and_claim_flows(*args, **kwargs)

    async def _execute_command_batch(
        self,
        commands: builtins.list[tuple[Any, ...]],
    ) -> builtins.list[Any]:
        execute_batch = getattr(self.executor, "execute_batch", None)
        if callable(execute_batch):
            try:
                result = execute_batch(commands)
                if inspect.isawaitable(result):
                    result = await result
                return require_batch_items(
                    result,
                    len(commands),
                    operation="executor batch",
                )
            except Exception as exc:
                mapped = map_exception(exc)
                if mapped is exc:
                    raise
                raise mapped from exc
        return [await self.command(*command) for command in commands]

    async def complete(
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
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args = _complete_command_args(
            self.codec,
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
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
            state_meta=state_meta,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def complete_job_results(
        self,
        items: builtins.list[tuple[ClaimedFlow, Any]],
        *,
        now_ms: int | None = None,
    ) -> builtins.list[Any]:
        """Pipeline distinct completion results in one transport write/round trip."""
        return await self.complete_job_mutations(
            [(job, {"result": result}) for job, result in items],
            now_ms=now_ms,
        )

    async def complete_job_mutations(
        self,
        items: builtins.list[tuple[ClaimedFlow, Mapping[str, Any]]],
        *,
        now_ms: int | None = None,
    ) -> builtins.list[Any]:
        """Pipeline per-job completion fields without losing item-level errors."""
        return await self.apply_job_mutations(
            [JobMutation(MutationKind.COMPLETE, job, options) for job, options in items],
            now_ms=now_ms,
        )

    async def apply_job_mutations(
        self,
        mutations: Sequence[JobMutation],
        *,
        now_ms: int | None = None,
    ) -> builtins.list[Any]:
        """Apply heterogeneous fenced mutations in one routed transport batch."""
        if not mutations:
            return []
        batch_now_ms = now_ms if now_ms is not None else _now_ms()
        commands = [
            tuple(_job_mutation_command_args(self.codec, mutation, now_ms=batch_now_ms))
            for mutation in mutations
        ]
        responses = await self._execute_command_batch(commands)
        return validate_many_result(
            responses,
            len(mutations),
            operation="Flow mutation batch",
        )

    async def transition_many(
        self,
        partition_key: str | None,
        *,
        from_state: str,
        to_state: str,
        items: builtins.list[FencedItem],
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        mixed = partition_key is None
        wire_partition = "MIXED" if mixed else partition_key
        args: builtins.list[Any] = ["FLOW.TRANSITION_MANY", wire_partition, from_state, to_state]
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "RUN_AT", run_at_ms)
        _append_priority(args, priority)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_fenced_items(
            args,
            partition_key,
            items,
            "FLOW.TRANSITION_MANY",
            include_lease=True,
        )
        return self._records_or_response(await self.executor.execute_command(*args))

    async def retry(
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
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args = _retry_command_args(
            self.codec,
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
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
            state_meta=state_meta,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def retry_many(
        self,
        partition_key: str | None,
        items: builtins.list[ClaimedFlow],
        *,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args: builtins.list[Any] = [
            "FLOW.RETRY_MANY",
            "MIXED" if partition_key is None else partition_key,
        ]
        _append(args, "ERROR", self.codec.encode(error) if error is not None else None)
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_claimed_items(args, partition_key, items, "FLOW.RETRY_MANY")
        return self._records_or_response(await self.executor.execute_command(*args))

    async def fail(
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
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args = _fail_command_args(
            self.codec,
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
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
            state_meta=state_meta,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def fail_many(
        self,
        partition_key: str | None,
        items: builtins.list[ClaimedFlow],
        *,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args: builtins.list[Any] = [
            "FLOW.FAIL_MANY",
            "MIXED" if partition_key is None else partition_key,
        ]
        _append(args, "ERROR", self.codec.encode(error) if error is not None else None)
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_claimed_items(args, partition_key, items, "FLOW.FAIL_MANY")
        return self._records_or_response(await self.executor.execute_command(*args))

    async def cancel(
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
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args: builtins.list[Any] = [
            "FLOW.CANCEL",
            id,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "PARTITION", partition_key)
        _append(args, "REASON", self.codec.encode(reason) if reason is not None else None)
        _append(args, "TTL", ttl_ms)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def cancel_many(
        self,
        partition_key: str | None,
        items: builtins.list[FencedItem],
        *,
        reason: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args: builtins.list[Any] = [
            "FLOW.CANCEL_MANY",
            "MIXED" if partition_key is None else partition_key,
        ]
        _append(args, "REASON", self.codec.encode(reason) if reason is not None else None)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_fenced_items(args, partition_key, items, "FLOW.CANCEL_MANY")
        return self._records_or_response(await self.executor.execute_command(*args))

    async def rewind(
        self,
        id: str,
        *,
        to_event: str,
        partition_key: str | None = None,
        expect_state: str | None = None,
        run_at_ms: int | None = None,
        reason_ref: str | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args: builtins.list[Any] = [
            "FLOW.REWIND",
            id,
            "TO_EVENT",
            to_event,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "EXPECT_STATE", expect_state)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "REASON_REF", reason_ref)
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)
