from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ferricstore.model_core import (
    _int,
    _MappingResult,
    _optional_int,
    _optional_str,
    _raw_map,
    _str,
)


@dataclass(frozen=True, slots=True)
class ScheduleResult(_MappingResult):
    """Typed response for Flow scheduler commands."""

    id: str = ""
    flow_id: str = ""
    kind: str = ""
    status: str = ""
    target: dict[str, Any] | None = None
    timezone: str | None = None
    cron: str | None = None
    overlap_policy: str | None = None
    next_fire_at_ms: int | None = None
    last_fire_at_ms: int | None = None
    fires: int = 0
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
    fired: int = 0
    claimed: int = 0
    skipped: int = 0
    target_id: str | None = None
    reason: str | None = None
    errors: list[Any] | None = None
    schedule: ScheduleResult | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> ScheduleResult:
        raw = _raw_map(value)
        nested_schedule = raw.get("schedule")
        view = _raw_map(nested_schedule) if isinstance(nested_schedule, dict) else raw
        target = view.get("target")
        state = view.get("state", view.get("status"))
        next_run_at_ms = view.get("next_run_at_ms", view.get("next_fire_at_ms"))
        fire_count = view.get("fire_count", view.get("fires"))
        return cls(
            id=_str(view.get("id")),
            flow_id=_str(view.get("flow_id")),
            kind=_str(view.get("kind")),
            status=_str(state),
            target=target if isinstance(target, dict) else None,
            timezone=_optional_str(view.get("timezone")),
            cron=_optional_str(view.get("cron")),
            overlap_policy=_optional_str(view.get("overlap_policy")),
            next_fire_at_ms=_optional_int(next_run_at_ms),
            last_fire_at_ms=_optional_int(view.get("last_fire_at_ms")),
            fires=_int(fire_count),
            max_fires=_optional_int(view.get("max_fires")),
            end_at_ms=_optional_int(view.get("end_at_ms")),
            attempts=_int(view.get("attempts")),
            last_target_id=_optional_str(view.get("last_target_id")),
            last_overlap_at_ms=_optional_int(view.get("last_overlap_at_ms")),
            last_overlap_target_id=_optional_str(view.get("last_overlap_target_id")),
            last_overlap_reason=_optional_str(view.get("last_overlap_reason")),
            last_skipped_at_ms=_optional_int(view.get("last_skipped_at_ms")),
            skipped_count=_int(view.get("skipped_count")),
            overlap_queued_due_at_ms=_optional_int(view.get("overlap_queued_due_at_ms")),
            end_reason=_optional_str(view.get("end_reason")),
            fired=_int(raw.get("fired")),
            claimed=_int(raw.get("claimed")),
            skipped=_int(raw.get("skipped")),
            target_id=_optional_str(raw.get("target_id")),
            reason=_optional_str(raw.get("reason")),
            errors=raw.get("errors") if isinstance(raw.get("errors"), list) else None,
            schedule=cls.from_resp(nested_schedule) if isinstance(nested_schedule, dict) else None,
            raw=raw,
        )

    @property
    def state(self) -> str:
        """KV-native alias for ``status``."""

        return self.status

    @property
    def next_run_at_ms(self) -> int | None:
        """KV-native alias for ``next_fire_at_ms``."""

        return self.next_fire_at_ms

    @property
    def fire_count(self) -> int:
        """KV-native alias for ``fires``."""

        return self.fires


# Preserve the original public pickle path across the module extraction.
ScheduleResult.__module__ = "ferricstore.types"


__all__ = ["ScheduleResult"]
