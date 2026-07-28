from __future__ import annotations

from typing import Any

from ferricstore.schedule_response_values import (
    OVERLAP_POLICIES,
    SCHEDULE_KINDS,
    SCHEDULE_STATES,
    optional_exact_integer,
    optional_text,
    required_exact_integer,
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
        timezone=optional_text(view, "timezone"),
        cron=optional_text(view, "cron"),
        catchup_policy=catchup_policy,
        coalesced_count=coalesced_count,
        last_catchup_at_ms=last_catchup_at_ms,
        last_coalesced_count=last_coalesced_count,
        overlap_policy=overlap_policy,
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
