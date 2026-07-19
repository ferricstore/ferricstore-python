from __future__ import annotations

import builtins
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, cast

from ferricstore.batch_core import (
    require_batch_items,
)
from ferricstore.client_claim_options import (
    _claim_due_command_args,
    _claim_return_compat_args,
    _claim_return_mode_unsupported,
    _reclaim_command_args,
    _resolve_include_record,
)
from ferricstore.client_helpers import (
    _append,
    _complete_jobs_command_args,
    _complete_many_args,
    _flow_return,
    _now_ms,
    _step_continue_args,
    _transition_command_args,
)
from ferricstore.client_sessions import CommandPipeline
from ferricstore.client_state import _ClientMixinBase
from ferricstore.errors import FerricStoreError, map_exception
from ferricstore.lifecycle_core import (
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.types import (
    ClaimedFlow,
    FlowRecord,
)
from ferricstore.worker_core import validate_many_result


class _ClientClaimsMixin(_ClientMixinBase):
    if TYPE_CHECKING:
        _record_or_get: Callable[..., FlowRecord]
        _records: Callable[[Any], builtins.list[FlowRecord]]
        _records_or_response: Callable[[Any], builtins.list[FlowRecord] | Any]
        pipeline: Callable[[], CommandPipeline]

    def claim_due(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        priority: int | None = None,
        now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_record: bool | None = None,
        job_only: bool | None = None,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        include_record = _resolve_include_record(include_record, job_only)
        args = self._claim_due_args(
            type,
            state=state,
            states=states,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            lease_ms=lease_ms,
            limit=limit,
            priority=priority,
            now_ms=now_ms,
            block_ms=block_ms,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            include_record=include_record,
            payload=payload,
            payload_max_bytes=payload_max_bytes,
            values=values,
            value_max_bytes=value_max_bytes,
            include_state=include_state,
            include_attributes=include_attributes,
        )
        return self._decode_claim_due_response(self._execute_claim_command(args), include_record)

    def claim_due_future(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        priority: int | None = None,
        now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_record: bool | None = None,
        job_only: bool | None = None,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> Future[builtins.list[FlowRecord] | builtins.list[ClaimedFlow]]:
        include_record = _resolve_include_record(include_record, job_only)
        submit_command = getattr(self.executor, "submit_command", None)
        if not callable(submit_command):
            raise RuntimeError("claim_due_future requires protocol executor submit support")
        args = self._claim_due_args(
            type,
            state=state,
            states=states,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            lease_ms=lease_ms,
            limit=limit,
            priority=priority,
            now_ms=now_ms,
            block_ms=block_ms,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            include_record=include_record,
            payload=payload,
            payload_max_bytes=payload_max_bytes,
            values=values,
            value_max_bytes=value_max_bytes,
            include_state=include_state,
            include_attributes=include_attributes,
        )
        source = submit_command(*args)
        future: Future[builtins.list[FlowRecord] | builtins.list[ClaimedFlow]] = Future()
        # The wire request can already grant leases, so cancellation after this
        # point must not discard its eventual ownership-bearing response.
        future.set_running_or_notify_cancel()
        compat_args = _claim_return_compat_args(args)

        def complete(done: Future[Any], *, allow_compat: bool = True) -> None:
            if future.cancelled():
                return
            try:
                value = self._decode_claim_due_response(done.result(), include_record)
            except Exception as exc:
                mapped = map_exception(exc)
                error = mapped if mapped is not exc else exc
                if (
                    allow_compat
                    and not future.cancelled()
                    and compat_args is not None
                    and isinstance(error, FerricStoreError)
                    and _claim_return_mode_unsupported(error)
                ):
                    try:
                        compat_source = submit_command(*compat_args)
                    except Exception as submit_exc:
                        try_set_future_exception(future, submit_exc)
                    else:
                        compat_source.add_done_callback(
                            lambda compat_done: complete(compat_done, allow_compat=False)
                        )
                    return
                try_set_future_exception(future, error)
            else:
                try_set_future_result(future, value)

        source.add_done_callback(complete)
        return future

    def _claim_due_args(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        priority: int | None = None,
        now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_record: bool = True,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> builtins.list[Any]:
        return _claim_due_command_args(
            type,
            state=state,
            states=states,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            lease_ms=lease_ms,
            limit=limit,
            priority=priority,
            now_ms=now_ms,
            block_ms=block_ms,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            include_record=include_record,
            payload=payload,
            payload_max_bytes=payload_max_bytes,
            values=values,
            value_max_bytes=value_max_bytes,
            include_state=include_state,
            include_attributes=include_attributes,
        )

    def _execute_claim_command(self, args: builtins.list[Any]) -> Any:
        try:
            return self.executor.execute_command(*args)
        except FerricStoreError as exc:
            compat_args = _claim_return_compat_args(args)
            if compat_args is not None and _claim_return_mode_unsupported(exc):
                return self.executor.execute_command(*compat_args)
            raise

    def _decode_claim_due_response(
        self,
        response: Any,
        include_record: bool,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        if include_record:
            return self._records(response)
        return ClaimedFlow.from_compact_rows(response)

    def claim_flows(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        lease_ms: int = 30_000,
        limit: int = 100,
        priority: int | None = 0,
        now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> builtins.list[ClaimedFlow]:
        """Claim due flows with the optimized claimed-flow response shape."""
        return cast(
            builtins.list[ClaimedFlow],
            self.claim_due(
                type,
                state=state,
                states=states,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=lease_ms,
                limit=limit,
                priority=priority,
                now_ms=now_ms,
                block_ms=block_ms,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                include_record=False,
                include_state=include_state,
                include_attributes=include_attributes,
            ),
        )

    def claim_jobs(self, *args: Any, **kwargs: Any) -> builtins.list[ClaimedFlow]:
        """Compatibility alias for claim_flows()."""
        return self.claim_flows(*args, **kwargs)

    def claim_flows_future(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        lease_ms: int = 30_000,
        limit: int = 100,
        priority: int | None = 0,
        now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> Future[builtins.list[ClaimedFlow]]:
        return cast(
            Future[builtins.list[ClaimedFlow]],
            self.claim_due_future(
                type,
                state=state,
                states=states,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=lease_ms,
                limit=limit,
                priority=priority,
                now_ms=now_ms,
                block_ms=block_ms,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                include_record=False,
                include_state=include_state,
                include_attributes=include_attributes,
            ),
        )

    def claim_jobs_future(self, *args: Any, **kwargs: Any) -> Future[builtins.list[ClaimedFlow]]:
        """Compatibility alias for claim_flows_future()."""
        return self.claim_flows_future(*args, **kwargs)

    def reclaim(
        self,
        type: str,
        *,
        state: str | None = None,
        worker: str,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        priority: int | None = None,
        now_ms: int | None = None,
        include_record: bool | None = None,
        job_only: bool | None = None,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        include_attributes: bool = True,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        include_record = _resolve_include_record(include_record, job_only)
        if state not in (None, "running"):
            raise ValueError("FLOW.RECLAIM only supports running state")

        args = _reclaim_command_args(
            type,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            lease_ms=lease_ms,
            limit=limit,
            priority=priority,
            now_ms=now_ms,
            include_record=include_record,
            payload=payload,
            payload_max_bytes=payload_max_bytes,
            values=values,
            value_max_bytes=value_max_bytes,
            include_attributes=include_attributes,
        )
        response = self._execute_claim_command(args)
        return self._decode_claim_due_response(response, include_record)

    def extend_lease(
        self,
        id: str,
        lease_token: bytes,
        *,
        fencing_token: int,
        lease_ms: int,
        partition_key: str | bytes | None = None,
        now_ms: int | None = None,
    ) -> FlowRecord:
        args: builtins.list[Any] = [
            "FLOW.EXTEND_LEASE",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "LEASE_MS",
            lease_ms,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        response = self.executor.execute_command(*args)
        return self._record_or_get(response, id, partition_key)

    def transition(
        self,
        id: str,
        *,
        from_state: str,
        to_state: str,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | bytes | None = None,
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def step_continue(
        self,
        id: str,
        *,
        lease_token: bytes,
        from_state: str,
        to_state: str,
        fencing_token: int,
        lease_ms: int = 30_000,
        partition_key: str | bytes | None = None,
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
        response = self.executor.execute_command(*args)
        if return_job:
            return ClaimedFlow.from_resp(response)
        return self._record_or_get(response, id, partition_key)

    def complete_many(
        self,
        partition_key: str | bytes | None,
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
        return self._records_or_response(self.executor.execute_command(*args))

    def complete_jobs(
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
        """Complete claimed jobs, choosing single-partition or mixed batch wire format."""
        if not jobs:
            return []

        first_partition = jobs[0].partition_key
        partition_key = (
            first_partition
            if first_partition is not None
            and all(job.partition_key == first_partition for job in jobs)
            else None
        )
        return self.complete_many(
            partition_key,
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
            return_ok_on_success=return_ok_on_success,
        )

    def _complete_jobs_args(
        self,
        jobs: builtins.list[ClaimedFlow],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
    ) -> builtins.list[Any]:
        return _complete_jobs_command_args(
            self.codec,
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
        )

    def complete_flows_and_claim_flows(
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
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
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
        """Complete claimed flows and claim more flows in one transport round trip."""
        if not jobs:
            return self.claim_flows(
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
            )

        complete_args = self._complete_jobs_args(
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
        )
        claim_args = self._claim_due_args(
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
        complete_response, claim_response = self._execute_command_batch(
            [tuple(complete_args), tuple(claim_args)]
        )
        validate_many_result(
            complete_response,
            len(jobs),
            operation="FLOW.COMPLETE_MANY",
        )
        return cast(
            builtins.list[ClaimedFlow],
            self._decode_claim_due_response(claim_response, False),
        )

    def complete_jobs_and_claim_jobs(self, *args: Any, **kwargs: Any) -> builtins.list[ClaimedFlow]:
        """Compatibility alias for complete_flows_and_claim_flows()."""
        return self.complete_flows_and_claim_flows(*args, **kwargs)

    def submit_complete_flows_and_claim_flows(
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
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        lease_ms: int = 30_000,
        limit: int = 100,
        priority: int | None = 0,
        claim_now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> tuple[Future[int], Future[builtins.list[ClaimedFlow]]] | None:
        submit_commands = getattr(self.executor, "submit_commands", None)
        if not callable(submit_commands) or not jobs:
            return None

        complete_args = self._complete_jobs_args(
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
        )
        claim_args = self._claim_due_args(
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
        source_complete, source_claim = submit_commands([tuple(complete_args), tuple(claim_args)])
        complete_future: Future[int] = Future()
        claim_future: Future[builtins.list[ClaimedFlow]] = Future()
        complete_future.set_running_or_notify_cancel()
        claim_future.set_running_or_notify_cancel()

        def complete_done(source: Future[Any]) -> None:
            if complete_future.cancelled():
                return
            try:
                validate_many_result(
                    source.result(),
                    len(jobs),
                    operation="FLOW.COMPLETE_MANY",
                )
                value = len(jobs)
            except Exception as exc:
                mapped = map_exception(exc)
                try_set_future_exception(
                    complete_future,
                    mapped if mapped is not exc else exc,
                )
            else:
                try_set_future_result(complete_future, value)

        def claim_done(source: Future[Any]) -> None:
            if claim_future.cancelled():
                return
            try:
                value = cast(
                    builtins.list[ClaimedFlow],
                    self._decode_claim_due_response(source.result(), False),
                )
            except Exception as exc:
                mapped = map_exception(exc)
                try_set_future_exception(claim_future, mapped if mapped is not exc else exc)
            else:
                try_set_future_result(claim_future, value)

        source_complete.add_done_callback(complete_done)
        source_claim.add_done_callback(claim_done)
        return complete_future, claim_future

    def submit_complete_jobs_and_claim_jobs(
        self, *args: Any, **kwargs: Any
    ) -> tuple[Future[int], Future[builtins.list[ClaimedFlow]]] | None:
        """Compatibility alias for submit_complete_flows_and_claim_flows()."""
        return self.submit_complete_flows_and_claim_flows(*args, **kwargs)

    def _execute_command_batch(
        self, commands: builtins.list[tuple[Any, ...]]
    ) -> builtins.list[Any]:
        execute_batch = getattr(self.executor, "execute_batch", None)
        if callable(execute_batch):
            try:
                return require_batch_items(
                    execute_batch(commands),
                    len(commands),
                    operation="executor batch",
                )
            except Exception as exc:
                mapped = map_exception(exc)
                if mapped is exc:
                    raise
                raise mapped from exc

        with self.pipeline() as pipe:
            for command in commands:
                pipe.command(*command)
            return pipe.execute()
