from __future__ import annotations

import builtins
from collections.abc import Sequence
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_claim_options import (
    _claim_due_command_args,
    _claim_return_compat_args,
    _claim_return_mode_unsupported,
    _reclaim_command_args,
    _resolve_include_record,
)
from ferricstore.client_helpers import (
    _append,
    _now_ms,
)
from ferricstore.errors import FerricStoreError
from ferricstore.types import (
    ClaimedFlow,
    FlowRecord,
)


class _AsyncClientClaimsMixin(_AsyncClientMixinBase):
    async def claim_due(
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
        args = _claim_due_command_args(
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
        response = await self._execute_claim_command(args)
        if include_record:
            return self._records(response)
        return ClaimedFlow.from_compact_rows(response)

    async def _execute_claim_command(self, args: builtins.list[Any]) -> Any:
        try:
            return await self.executor.execute_command(*args)
        except FerricStoreError as exc:
            compat_args = _claim_return_compat_args(args)
            if compat_args is not None and _claim_return_mode_unsupported(exc):
                return await self.executor.execute_command(*compat_args)
            raise

    async def claim_flows(
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
        return cast(
            builtins.list[ClaimedFlow],
            await self.claim_due(
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

    async def claim_jobs(self, *args: Any, **kwargs: Any) -> builtins.list[ClaimedFlow]:
        """Compatibility alias for claim_flows()."""
        return await self.claim_flows(*args, **kwargs)

    async def reclaim(
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
        response = await self._execute_claim_command(args)
        if include_record:
            return self._records(response)
        return ClaimedFlow.from_compact_rows(response)

    async def extend_lease(
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
        response = await self.executor.execute_command(*args)
        return await self._record_or_get(response, id, partition_key)
