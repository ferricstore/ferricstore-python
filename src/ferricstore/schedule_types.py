from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from ferricstore.model_core import _MappingResult, _raw_map


@dataclass(frozen=True, slots=True)
class ScheduleRecord(_MappingResult):
    """Canonical durable schedule state returned by the server."""

    id: str = ""
    flow_id: str = ""
    kind: str = ""
    state: str = ""
    target: dict[str, Any] | None = None
    timezone: str | None = None
    cron: str | None = None
    catchup_policy: str | None = None
    coalesced_count: int = 0
    last_catchup_at_ms: int | None = None
    last_coalesced_count: int = 0
    overlap_policy: str | None = None
    next_run_at_ms: int | None = None
    last_fire_at_ms: int | None = None
    fire_count: int = 0
    max_fires: int | None = None
    end_at_ms: int | None = None
    attempts: int = 0
    last_target_id: str | None = None
    last_overlap_at_ms: int | None = None
    last_overlap_target_id: str | None = None
    last_overlap_reason: str | None = None
    last_skipped_at_ms: int | None = None
    skipped_count: int = 0
    overlap_queued_due_at_ms: int | None = None
    end_reason: str | None = None
    last_planning_error: str | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> ScheduleRecord:
        from ferricstore.schedule_response_record import parse_schedule_record

        return cast(ScheduleRecord, parse_schedule_record(cls, _raw_map(value)))


@dataclass(frozen=True, slots=True)
class ScheduleFireResult(_MappingResult):
    """Outcome of one explicit schedule fire."""

    fired: int = 0
    skipped: int = 0
    target_id: str | None = None
    reason: str | None = None
    schedule: ScheduleRecord | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> ScheduleFireResult:
        from ferricstore.schedule_response_outcomes import parse_schedule_fire

        return cast(
            ScheduleFireResult,
            parse_schedule_fire(cls, ScheduleRecord, _raw_map(value)),
        )


@dataclass(frozen=True, slots=True)
class ScheduleFireDueResult(_MappingResult):
    """Aggregate outcome of one bounded due-schedule batch."""

    claimed: int = 0
    fired: int = 0
    skipped: int = 0
    coalesced: int = 0
    errors: list[tuple[str, str]] | None = None
    claim_error: str | None = None
    last_target_id: str | None = None
    last_skip_reason: str | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> ScheduleFireDueResult:
        from ferricstore.schedule_response_outcomes import parse_schedule_fire_due

        return cast(ScheduleFireDueResult, parse_schedule_fire_due(cls, _raw_map(value)))


for result_type in (ScheduleRecord, ScheduleFireResult, ScheduleFireDueResult):
    result_type.__module__ = "ferricstore.types"


__all__ = ["ScheduleFireDueResult", "ScheduleFireResult", "ScheduleRecord"]
