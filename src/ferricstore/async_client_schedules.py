from __future__ import annotations

import builtins
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_helpers import (
    _append,
    _append_bool,
    _append_extra_options,
    _normalize_admin_response,
    _ok_response,
)
from ferricstore.governance_validation import (
    validate_schedule_create,
    validate_schedule_fire_due,
    validate_schedule_list,
    validate_schedule_operation,
)
from ferricstore.types import ScheduleResult


class _AsyncClientSchedulesMixin(_AsyncClientMixinBase):
    async def schedule_create(
        self,
        id: str,
        *,
        target: dict[str, Any],
        kind: str | None = None,
        at_ms: int | None = None,
        delay_ms: int | None = None,
        start_at_ms: int | None = None,
        every_ms: int | None = None,
        cron: str | None = None,
        timezone: str | None = None,
        overlap_policy: str | None = None,
        overlap_retry_ms: int | None = None,
        max_fires: int | None = None,
        end_at_ms: int | None = None,
        overwrite: bool | None = None,
        now_ms: int | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ScheduleResult:
        validate_schedule_create(
            id,
            target=target,
            kind=kind,
            at_ms=at_ms,
            delay_ms=delay_ms,
            start_at_ms=start_at_ms,
            every_ms=every_ms,
            cron=cron,
            timezone=timezone,
            overlap_policy=overlap_policy,
            overlap_retry_ms=overlap_retry_ms,
            max_fires=max_fires,
            end_at_ms=end_at_ms,
            overwrite=overwrite,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.SCHEDULE.CREATE", id]
        _append(args, "KIND", kind)
        _append(args, "AT_MS", at_ms)
        _append(args, "DELAY_MS", delay_ms)
        _append(args, "START_AT_MS", start_at_ms)
        _append(args, "EVERY_MS", every_ms)
        _append(args, "CRON", cron)
        _append(args, "TIMEZONE", timezone)
        _append(args, "TARGET", target)
        _append(args, "OVERLAP_POLICY", overlap_policy)
        _append(args, "OVERLAP_RETRY_MS", overlap_retry_ms)
        _append(args, "MAX_FIRES", max_fires)
        _append(args, "END_AT_MS", end_at_ms)
        _append_bool(args, "OVERWRITE", overwrite)
        _append(args, "NOW", now_ms)
        _append_extra_options(args, extra_options)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_get(self, id: str, *, now_ms: int | None = None) -> ScheduleResult | None:
        validate_schedule_operation(id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.GET", id]
        _append(args, "NOW", now_ms)
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return ScheduleResult.from_resp(response) if response is not None else None

    async def schedule_fire(
        self,
        id: str,
        *,
        fire_at_ms: int | None = None,
        now_ms: int | None = None,
    ) -> ScheduleResult:
        validate_schedule_operation(id, fire_at_ms=fire_at_ms, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.FIRE", id]
        _append(args, "FIRE_AT_MS", fire_at_ms)
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_pause(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        validate_schedule_operation(id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.PAUSE", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_resume(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        validate_schedule_operation(id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.RESUME", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_delete(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        validate_schedule_operation(id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.DELETE", id]
        _append(args, "NOW", now_ms)
        response = _normalize_admin_response(await self.executor.execute_command(*args))
        if _ok_response(response):
            return ScheduleResult(id=id, status="deleted", raw={"id": id, "status": "deleted"})
        return ScheduleResult.from_resp(cast(dict[str, Any], response))

    async def schedule_fire_due(
        self,
        *,
        now_ms: int | None = None,
        worker: str | None = None,
        lease_ms: int | None = None,
        block_ms: int | None = None,
        limit: int | None = None,
    ) -> ScheduleResult:
        validate_schedule_fire_due(
            now_ms=now_ms,
            worker=worker,
            lease_ms=lease_ms,
            block_ms=block_ms,
            limit=limit,
        )
        args: builtins.list[Any] = ["FLOW.SCHEDULE.FIRE_DUE"]
        _append(args, "NOW", now_ms)
        _append(args, "WORKER", worker)
        _append(args, "LEASE_MS", lease_ms)
        _append(args, "BLOCK", block_ms)
        _append(args, "LIMIT", limit)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_list(
        self,
        *,
        kind: str | None = None,
        state: str | None = None,
        timezone: str | None = None,
        target_type: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        count: int | None = None,
        rev: bool | None = None,
    ) -> builtins.list[ScheduleResult]:
        validate_schedule_list(
            kind=kind,
            state=state,
            timezone=timezone,
            target_type=target_type,
            from_ms=from_ms,
            to_ms=to_ms,
            count=count,
            rev=rev,
        )
        args: builtins.list[Any] = ["FLOW.SCHEDULE.LIST"]
        _append(args, "KIND", kind)
        _append(args, "STATE", state)
        _append(args, "TIMEZONE", timezone)
        _append(args, "TARGET_TYPE", target_type)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append(args, "COUNT", count)
        _append_bool(args, "REV", rev)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return [ScheduleResult.from_resp(item) for item in response]
