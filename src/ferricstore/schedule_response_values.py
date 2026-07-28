from __future__ import annotations

from typing import Any

from ferricstore.model_core import _optional_str

MAX_EXACT_INTEGER = 9_007_199_254_740_991
SCHEDULE_KINDS = frozenset({"one_shot", "delay", "interval", "cron"})
SCHEDULE_STATES = frozenset({"active", "paused", "running", "completed", "failed", "cancelled"})
OVERLAP_POLICIES = frozenset({"allow", "skip", "queue_after_previous", "fail_schedule"})


def required_text(value: dict[str, Any], field: str, *, context: str = "schedule response") -> str:
    text = optional_text(value, field)
    if text is None:
        raise ValueError(f"{context} {field} is missing")
    return text


def optional_text(value: dict[str, Any], field: str) -> str | None:
    raw = value.get(field)
    if raw is None:
        return None
    if raw == "" or raw == b"":
        raise ValueError(f"schedule response {field} must be non-empty text")
    if not isinstance(raw, (str, bytes)):
        raise TypeError(f"schedule response {field} must be text")
    return _optional_str(raw)


def required_nullable_text(value: dict[str, Any], field: str) -> str | None:
    if field not in value:
        raise ValueError(f"schedule response is missing {field}")
    return optional_text(value, field)


def required_exact_integer(value: dict[str, Any], field: str) -> int:
    if field not in value or value[field] is None:
        raise ValueError(f"schedule response is missing {field}")
    parsed = value[field]
    if type(parsed) is not int or parsed < 0 or parsed > MAX_EXACT_INTEGER:
        raise ValueError(f"schedule response {field} must be an integer in 0..{MAX_EXACT_INTEGER}")
    return parsed


def optional_exact_integer(value: dict[str, Any], field: str) -> int | None:
    if field not in value or value[field] is None:
        return None
    return required_exact_integer(value, field)


def required_nullable_exact_integer(value: dict[str, Any], field: str) -> int | None:
    if field not in value:
        raise ValueError(f"schedule response is missing {field}")
    return optional_exact_integer(value, field)


def schedule_errors(value: dict[str, Any]) -> list[tuple[str, str]]:
    errors = value.get("errors")
    if not isinstance(errors, list):
        raise TypeError("schedule fire_due response errors must be a list")
    parsed: list[tuple[str, str]] = []
    for item in errors:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError("schedule fire_due response errors must contain id/reason pairs")
        if any(not isinstance(part, str) or not part for part in item):
            raise ValueError("schedule fire_due response errors must contain non-empty text")
        parsed.append((item[0], item[1]))
    return parsed


def validate_catchup_state(
    kind: str,
    *,
    coalesced_count: int,
    last_coalesced_count: int,
    last_catchup_at_ms: int | None,
) -> None:
    if kind != "interval":
        if coalesced_count != 0:
            raise ValueError("schedule response non-interval coalesced_count must be zero")
        if last_coalesced_count != 0:
            raise ValueError("schedule response non-interval last_coalesced_count must be zero")
        if last_catchup_at_ms is not None:
            raise ValueError("schedule response non-interval last_catchup_at_ms must be null")
        return
    if last_coalesced_count > coalesced_count:
        raise ValueError("schedule response last_coalesced_count exceeds coalesced_count")
    if last_coalesced_count > 0 and last_catchup_at_ms is None:
        raise ValueError("schedule response is missing last_catchup_at_ms after catch-up")
    if last_coalesced_count == 0 and last_catchup_at_ms is not None:
        raise ValueError("schedule response last_catchup_at_ms requires a catch-up")
