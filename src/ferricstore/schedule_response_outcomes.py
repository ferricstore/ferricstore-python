from __future__ import annotations

from typing import Any

from ferricstore.model_core import _raw_map
from ferricstore.schedule_response_record import parse_schedule_record
from ferricstore.schedule_response_values import (
    optional_exact_integer,
    optional_text,
    required_exact_integer,
    schedule_errors,
)


def parse_schedule_fire(result_type: type[Any], record_type: type[Any], raw: dict[str, Any]) -> Any:
    nested = raw.get("schedule")
    if not isinstance(nested, dict):
        raise TypeError("schedule fire response schedule must be a mapping")
    schedule = parse_schedule_record(record_type, _raw_map(nested))
    fired = required_exact_integer(raw, "fired")
    skipped = optional_exact_integer(raw, "skipped") or 0
    if fired + skipped != 1 or fired > 1 or skipped > 1:
        raise ValueError("schedule fire response outcomes must equal one")
    target_id = optional_text(raw, "target_id")
    reason = optional_text(raw, "reason")
    if fired == 1 and target_id is None:
        raise ValueError("schedule fire response is missing target_id")
    if skipped == 1 and reason is None:
        raise ValueError("schedule fire response is missing reason")
    if fired == 0 and target_id is not None:
        raise ValueError("schedule fire response target_id requires a fired outcome")
    if skipped == 0 and reason is not None:
        raise ValueError("schedule fire response reason requires a skipped outcome")
    return result_type(
        fired=fired,
        skipped=skipped,
        target_id=target_id,
        reason=reason,
        schedule=schedule,
        raw=raw,
    )


def parse_schedule_fire_due(result_type: type[Any], raw: dict[str, Any]) -> Any:
    claimed = required_exact_integer(raw, "claimed")
    fired = required_exact_integer(raw, "fired")
    skipped = required_exact_integer(raw, "skipped")
    coalesced = required_exact_integer(raw, "coalesced")
    errors = schedule_errors(raw)
    if fired + skipped + len(errors) != claimed:
        raise ValueError("schedule fire_due response outcomes do not equal claimed")
    claim_error = optional_text(raw, "claim_error")
    last_target_id = optional_text(raw, "last_target_id")
    last_skip_reason = optional_text(raw, "last_skip_reason")
    if fired > 0 and last_target_id is None:
        raise ValueError("schedule fire_due response is missing last_target_id")
    if fired == 0 and last_target_id is not None:
        raise ValueError("schedule fire_due response last_target_id requires a fired outcome")
    if skipped > 0 and last_skip_reason is None:
        raise ValueError("schedule fire_due response is missing last_skip_reason")
    if skipped == 0 and last_skip_reason is not None:
        raise ValueError("schedule fire_due response last_skip_reason requires a skipped outcome")
    if coalesced > 0 and fired + skipped == 0:
        raise ValueError(
            "schedule fire_due response coalesced count requires a fired or skipped outcome"
        )
    return result_type(
        claimed=claimed,
        fired=fired,
        skipped=skipped,
        coalesced=coalesced,
        errors=errors,
        claim_error=claim_error,
        last_target_id=last_target_id,
        last_skip_reason=last_skip_reason,
        raw=raw,
    )
