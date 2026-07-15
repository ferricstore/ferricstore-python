from __future__ import annotations

import builtins
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ferricstore.client_helpers import (
    _append,
    _append_attributes,
    _append_bool,
    _append_named_values,
    _append_priority,
    _append_state_meta,
    _complete_command_args,
    _fail_command_args,
    _flow_return,
    _job_mutation_command_args,
    _now_ms,
    _retry_command_args,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.mutation_core import JobMutation, MutationKind
from ferricstore.types import (
    ClaimedFlow,
    FencedItem,
    FlowRecord,
)
from ferricstore.worker_core import validate_many_result


class _ClientMutationsMixin(_ClientMixinBase):
    if TYPE_CHECKING:
        _append_claimed_items: Callable[..., builtins.list[Any]]
        _append_fenced_items: Callable[..., builtins.list[Any]]
        _execute_command_batch: Callable[[builtins.list[tuple[Any, ...]]], builtins.list[Any]]
        _record_or_get: Callable[..., FlowRecord]
        _records_or_response: Callable[[Any], builtins.list[FlowRecord] | Any]

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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def complete_job_results(
        self,
        items: builtins.list[tuple[ClaimedFlow, Any]],
        *,
        now_ms: int | None = None,
    ) -> builtins.list[Any]:
        """Pipeline distinct completion results in one transport write/round trip."""
        return self.complete_job_mutations(
            [(job, {"result": result}) for job, result in items],
            now_ms=now_ms,
        )

    def complete_job_mutations(
        self,
        items: builtins.list[tuple[ClaimedFlow, Mapping[str, Any]]],
        *,
        now_ms: int | None = None,
    ) -> builtins.list[Any]:
        """Pipeline per-job completion fields without losing item-level errors."""
        return self.apply_job_mutations(
            [JobMutation(MutationKind.COMPLETE, job, options) for job, options in items],
            now_ms=now_ms,
        )

    def apply_job_mutations(
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
        responses = self._execute_command_batch(commands)
        return validate_many_result(
            responses,
            len(mutations),
            operation="Flow mutation batch",
        )

    def transition_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def retry_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def fail_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def cancel_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

    def rewind(
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)
