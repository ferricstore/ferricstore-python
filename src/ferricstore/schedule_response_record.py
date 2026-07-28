from __future__ import annotations

from typing import Any

from ferricstore.schedule_response_values import (
    OVERLAP_POLICIES,
    SCHEDULE_KINDS,
    SCHEDULE_STATES,
    optional_exact_integer,
    optional_text,
    required_exact_integer,
    required_nullable_exact_integer,
    required_nullable_text,
    required_text,
    validate_catchup_state,
)


def parse_schedule_record(result_type: type[Any], view: dict[str, Any]) -> Any:
    id = required_text(view, "id")
    kind = required_text(view, "kind")
    if kind not in SCHEDULE_KINDS:
        raise ValueError(f"schedule response kind is invalid: {kind!r}")
    state = required_text(view, "state")
    if state not in SCHEDULE_STATES:
        raise ValueError(f"schedule response state is invalid: {state!r}")

    target = view.get("target")
    if not isinstance(target, dict):
        raise TypeError("schedule response target must be a mapping")
    required_text(target, "type", context="schedule response target")

    if "catchup_policy" not in view:
        raise ValueError("schedule response is missing catchup_policy")
    catchup_policy = optional_text(view, "catchup_policy")
    if (kind == "interval" and catchup_policy != "fire_once") or (
        kind != "interval" and catchup_policy is not None
    ):
        raise ValueError("schedule response catchup_policy is invalid for kind")

    overlap_policy = required_text(view, "overlap_policy")
    if overlap_policy not in OVERLAP_POLICIES:
        raise ValueError("schedule response overlap_policy is invalid")

    created_at_ms = required_exact_integer(view, "created_at_ms")
    every_ms = required_nullable_exact_integer(view, "every_ms")
    cron = required_nullable_text(view, "cron")
    timezone = required_nullable_text(view, "timezone")
    overlap_retry_ms = required_nullable_exact_integer(view, "overlap_retry_ms")
    _validate_recurrence(kind, every_ms, cron, timezone, overlap_policy, overlap_retry_ms)

    next_run_at_ms = optional_exact_integer(view, "next_run_at_ms")
    fire_count = required_exact_integer(view, "fire_count")
    coalesced_count = required_exact_integer(view, "coalesced_count")
    last_coalesced_count = required_exact_integer(view, "last_coalesced_count")
    last_catchup_at_ms = optional_exact_integer(view, "last_catchup_at_ms")
    validate_catchup_state(
        kind,
        coalesced_count=coalesced_count,
        last_coalesced_count=last_coalesced_count,
        last_catchup_at_ms=last_catchup_at_ms,
    )
    return result_type(
        id=id,
        flow_id=optional_text(view, "flow_id") or "",
        kind=kind,
        state=state,
        target=target,
        created_at_ms=created_at_ms,
        every_ms=every_ms,
        timezone=timezone,
        cron=cron,
        catchup_policy=catchup_policy,
        coalesced_count=coalesced_count,
        last_catchup_at_ms=last_catchup_at_ms,
        last_coalesced_count=last_coalesced_count,
        overlap_policy=overlap_policy,
        overlap_retry_ms=overlap_retry_ms,
        next_run_at_ms=next_run_at_ms,
        last_fire_at_ms=optional_exact_integer(view, "last_fire_at_ms"),
        fire_count=fire_count,
        max_fires=optional_exact_integer(view, "max_fires"),
        end_at_ms=optional_exact_integer(view, "end_at_ms"),
        attempts=required_exact_integer(view, "attempts"),
        last_target_id=optional_text(view, "last_target_id"),
        last_overlap_at_ms=optional_exact_integer(view, "last_overlap_at_ms"),
        last_overlap_target_id=optional_text(view, "last_overlap_target_id"),
        last_overlap_reason=optional_text(view, "last_overlap_reason"),
        last_skipped_at_ms=optional_exact_integer(view, "last_skipped_at_ms"),
        skipped_count=required_exact_integer(view, "skipped_count"),
        overlap_queued_due_at_ms=optional_exact_integer(view, "overlap_queued_due_at_ms"),
        end_reason=optional_text(view, "end_reason"),
        last_planning_error=optional_text(view, "last_planning_error"),
        raw=view,
    )


def _validate_recurrence(
    kind: str,
    every_ms: int | None,
    cron: str | None,
    timezone: str | None,
    overlap_policy: str,
    overlap_retry_ms: int | None,
) -> None:
    if kind == "interval":
        if every_ms is None or every_ms <= 0:
            raise ValueError("schedule response interval every_ms must be positive")
    elif every_ms is not None:
        raise ValueError("schedule response every_ms is only valid for interval schedules")

    if kind == "cron":
        if cron is None:
            raise ValueError("schedule response cron schedule is missing cron")
        if timezone is None:
            raise ValueError("schedule response cron schedule is missing timezone")
    elif cron is not None:
        raise ValueError("schedule response cron is only valid for cron schedules")
    elif timezone is not None:
        raise ValueError("schedule response timezone is only valid for cron schedules")

    if kind not in {"interval", "cron"} and overlap_policy != "allow":
        raise ValueError("schedule response overlap_policy is only valid for recurring schedules")

    if overlap_retry_ms is not None:
        if overlap_retry_ms <= 0:
            raise ValueError("schedule response overlap_retry_ms must be positive")
        if overlap_policy != "queue_after_previous":
            raise ValueError("schedule response overlap_retry_ms requires queue_after_previous")
