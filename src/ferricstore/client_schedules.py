from __future__ import annotations

import builtins
from typing import Any, cast

from ferricstore.client_helpers import (
    _append,
    _append_bool,
    _append_extra_options,
    _normalize_admin_response,
    _ok_response,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.governance_validation import (
    validate_schedule_create,
    validate_schedule_fire_due,
    validate_schedule_list,
    validate_schedule_operation,
)
from ferricstore.types import ScheduleFireDueResult, ScheduleFireResult, ScheduleRecord


class _ClientSchedulesMixin(_ClientMixinBase):
    def schedule_create(
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
        catchup_policy: str | None = None,
        overlap_policy: str | None = None,
        overlap_retry_ms: int | None = None,
        max_fires: int | None = None,
        end_at_ms: int | None = None,
        overwrite: bool | None = None,
        now_ms: int | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ScheduleRecord:
        """Create or replace a durable Flow schedule."""

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
            catchup_policy=catchup_policy,
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
        _append(args, "CATCHUP_POLICY", catchup_policy)
        _append(args, "OVERLAP_POLICY", overlap_policy)
        _append(args, "OVERLAP_RETRY_MS", overlap_retry_ms)
        _append(args, "MAX_FIRES", max_fires)
        _append(args, "END_AT_MS", end_at_ms)
        _append_bool(args, "OVERWRITE", overwrite)
        _append(args, "NOW", now_ms)
        _append_extra_options(args, extra_options)
        return ScheduleRecord.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_get(self, id: str) -> ScheduleRecord | None:
        validate_schedule_operation(id, now_ms=None)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.GET", id]
        response = cast(
            dict[str, Any] | None, _normalize_admin_response(self.executor.execute_command(*args))
        )
        return ScheduleRecord.from_resp(response) if response is not None else None

    def schedule_fire(
        self,
        id: str,
        *,
        fire_at_ms: int | None = None,
        now_ms: int | None = None,
    ) -> ScheduleFireResult:
        validate_schedule_operation(id, fire_at_ms=fire_at_ms, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.FIRE", id]
        _append(args, "FIRE_AT_MS", fire_at_ms)
        _append(args, "NOW", now_ms)
        return ScheduleFireResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_pause(self, id: str, *, now_ms: int | None = None) -> ScheduleRecord:
        validate_schedule_operation(id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.PAUSE", id]
        _append(args, "NOW", now_ms)
        return ScheduleRecord.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_resume(self, id: str, *, now_ms: int | None = None) -> ScheduleRecord:
        validate_schedule_operation(id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.RESUME", id]
        _append(args, "NOW", now_ms)
        return ScheduleRecord.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_delete(self, id: str, *, now_ms: int | None = None) -> None:
        validate_schedule_operation(id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.SCHEDULE.DELETE", id]
        _append(args, "NOW", now_ms)
        response = _normalize_admin_response(self.executor.execute_command(*args))
        if _ok_response(response):
            return None
        raise TypeError("FLOW.SCHEDULE.DELETE response must be OK")

    def schedule_fire_due(
        self,
        *,
        now_ms: int | None = None,
        worker: str | None = None,
        lease_ms: int | None = None,
        block_ms: int | None = None,
        limit: int | None = None,
    ) -> ScheduleFireDueResult:
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
        return ScheduleFireDueResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_list(
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
    ) -> builtins.list[ScheduleRecord]:
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
            _normalize_admin_response(self.executor.execute_command(*args)),
        )
        return [ScheduleRecord.from_resp(item) for item in response]
