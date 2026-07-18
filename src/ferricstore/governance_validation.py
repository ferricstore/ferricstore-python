from __future__ import annotations

from collections.abc import Mapping

from ferricstore.config_validation import (
    validate_bounded_nonnegative_int,
    validate_nonnegative_int,
    validate_optional_bool,
    validate_optional_nonnegative_int,
    validate_optional_positive_int,
    validate_positive_int,
    validate_string_sequence,
)

_MAX_LIMIT_MUTATION_AMOUNT = 1_000
_MAX_RESERVATION_ID_BYTES = 256
_MAX_EXACT_INTEGER = 9_007_199_254_740_991
_APPROVAL_STATUSES = frozenset({"pending", "approved", "rejected"})
_SCHEDULE_KINDS = frozenset({"one_shot", "delay", "interval", "cron"})
_RECURRING_SCHEDULE_KINDS = frozenset({"interval", "cron"})
_SCHEDULE_OVERLAP_POLICIES = frozenset({"allow", "skip", "queue_after_previous", "fail_schedule"})


def validate_nonempty_string(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def validate_optional_nonempty_string(value: object | None, *, name: str) -> str | None:
    if value is None:
        return None
    return validate_nonempty_string(value, name=name)


def _validate_schedule_kind(value: object | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in _SCHEDULE_KINDS:
        raise ValueError("kind must be one_shot, delay, interval, or cron")
    return value


def _inferred_schedule_kind(
    kind: str | None,
    *,
    delay_ms: object | None,
    every_ms: object | None,
    cron: object | None,
) -> str:
    if kind is not None:
        return kind
    if cron is not None:
        return "cron"
    if every_ms is not None:
        return "interval"
    if delay_ms is not None:
        return "delay"
    return "one_shot"


def _known_first_schedule_run(
    kind: str,
    *,
    at_ms: object | None,
    delay_ms: object | None,
    start_at_ms: object | None,
    now_ms: object | None,
) -> int | None:
    """Return a lower bound only when the client has enough deterministic inputs."""

    if kind == "delay":
        if isinstance(now_ms, int) and isinstance(delay_ms, int):
            return now_ms + delay_ms
        return None
    if kind == "cron":
        candidate = start_at_ms if start_at_ms is not None else at_ms
    else:
        candidate = at_ms if at_ms is not None else start_at_ms
    if candidate is None:
        candidate = now_ms
    return candidate if isinstance(candidate, int) else None


def _validate_schedule_target(value: object, *, recurring: bool) -> None:
    if not isinstance(value, Mapping):
        raise ValueError("target must be a mapping with a non-empty type")
    validate_nonempty_string(value.get("type"), name="target type")
    for key in (
        "state",
        "id",
        "id_prefix",
        "partition_key",
        "correlation_id",
        "parent_flow_id",
        "root_flow_id",
    ):
        if key in value:
            validate_optional_nonempty_string(value[key], name=f"target {key}")
    if "run_at_ms" in value:
        validate_optional_nonnegative_int(value["run_at_ms"], name="target run_at_ms")
    if "priority" in value:
        validate_bounded_nonnegative_int(
            value["priority"],
            name="target priority",
            maximum=2,
        )
    if recurring and value.get("id") is not None:
        raise ValueError("target id is not supported for recurring schedules; use id_prefix")


def validate_schedule_create(
    id: object,
    *,
    target: object,
    kind: object | None,
    at_ms: object | None,
    delay_ms: object | None,
    start_at_ms: object | None,
    every_ms: object | None,
    cron: object | None,
    timezone: object | None,
    overlap_policy: object | None,
    overlap_retry_ms: object | None,
    max_fires: object | None,
    end_at_ms: object | None,
    overwrite: object | None,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(id, name="id")
    validated_kind = _validate_schedule_kind(kind)
    effective_kind = _inferred_schedule_kind(
        validated_kind,
        delay_ms=delay_ms,
        every_ms=every_ms,
        cron=cron,
    )
    recurring = effective_kind in _RECURRING_SCHEDULE_KINDS
    _validate_schedule_target(target, recurring=recurring)
    validate_optional_nonnegative_int(at_ms, name="at_ms")
    validate_optional_nonnegative_int(delay_ms, name="delay_ms")
    validate_optional_nonnegative_int(start_at_ms, name="start_at_ms")
    validate_optional_positive_int(every_ms, name="every_ms")
    validate_optional_positive_int(overlap_retry_ms, name="overlap_retry_ms")
    validate_optional_nonnegative_int(end_at_ms, name="end_at_ms")
    validate_optional_bool(overwrite, name="overwrite")
    validate_optional_nonnegative_int(now_ms, name="now_ms")

    if effective_kind == "delay" and delay_ms is None:
        raise ValueError("delay_ms is required for delay schedules")
    if effective_kind == "interval" and every_ms is None:
        raise ValueError("every_ms is required for interval schedules")
    if effective_kind == "cron":
        validate_nonempty_string(cron, name="cron")
        validate_optional_nonempty_string(timezone, name="timezone")
    elif timezone is not None:
        raise ValueError("timezone is only supported for cron schedules")

    if recurring:
        if overlap_policy is not None and overlap_policy not in _SCHEDULE_OVERLAP_POLICIES:
            raise ValueError(
                "overlap_policy must be allow, skip, queue_after_previous, or fail_schedule"
            )
        validate_optional_positive_int(max_fires, name="max_fires")
        first_run = _known_first_schedule_run(
            effective_kind,
            at_ms=at_ms,
            delay_ms=delay_ms,
            start_at_ms=start_at_ms,
            now_ms=now_ms,
        )
        if isinstance(end_at_ms, int) and first_run is not None and end_at_ms < first_run:
            raise ValueError("end_at_ms must be at or after first run")
    else:
        if overlap_policy is not None:
            raise ValueError("overlap_policy is only supported for recurring schedules")
        if max_fires is not None:
            raise ValueError("max_fires is only supported for recurring schedules")
        if end_at_ms is not None:
            raise ValueError("end_at_ms is only supported for recurring schedules")


def validate_schedule_operation(
    id: object,
    *,
    now_ms: object | None,
    fire_at_ms: object | None = None,
) -> None:
    validate_nonempty_string(id, name="id")
    validate_optional_nonnegative_int(now_ms, name="now_ms")
    validate_optional_nonnegative_int(fire_at_ms, name="fire_at_ms")


def validate_schedule_fire_due(
    *,
    now_ms: object | None,
    worker: object | None,
    lease_ms: object | None,
    block_ms: object | None,
    limit: object | None,
) -> None:
    validate_optional_nonnegative_int(now_ms, name="now_ms")
    validate_optional_nonempty_string(worker, name="worker")
    validate_optional_positive_int(lease_ms, name="lease_ms")
    validate_optional_nonnegative_int(block_ms, name="block_ms")
    validate_optional_positive_int(limit, name="limit")


def validate_schedule_list(
    *,
    kind: object | None,
    state: object | None,
    timezone: object | None,
    target_type: object | None,
    from_ms: object | None,
    to_ms: object | None,
    count: object | None,
    rev: object | None,
) -> None:
    _validate_schedule_kind(kind)
    validate_optional_nonempty_string(state, name="state")
    validate_optional_nonempty_string(timezone, name="timezone")
    validate_optional_nonempty_string(target_type, name="target_type")
    validate_optional_nonnegative_int(from_ms, name="from_ms")
    validate_optional_nonnegative_int(to_ms, name="to_ms")
    validate_optional_positive_int(count, name="count")
    validate_optional_bool(rev, name="rev")


def _validate_optional_scope_filters(
    scope: object | None,
    partition_key: object | None,
) -> None:
    # This follows the KV contract: an exact scope takes precedence over the
    # partition-derived aliases, so an unused partition value is not inspected.
    if scope is not None:
        validate_nonempty_string(scope, name="scope")
    elif partition_key is not None:
        validate_nonempty_string(partition_key, name="partition_key")


def _validate_optional_exact_nonnegative_int(value: object | None, *, name: str) -> None:
    validated = validate_optional_nonnegative_int(value, name=name)
    if validated is not None and validated > _MAX_EXACT_INTEGER:
        raise ValueError(f"{name} cannot exceed {_MAX_EXACT_INTEGER}")


def _validate_exact_positive_int(value: object, *, name: str) -> int:
    validated = validate_positive_int(value, name=name)
    if validated > _MAX_EXACT_INTEGER:
        raise ValueError(f"{name} cannot exceed {_MAX_EXACT_INTEGER}")
    return validated


def validate_effect_reserve(
    id: object,
    effect_key: object,
    effect_type: object,
    *,
    lease_token: object,
    fencing_token: object,
    operation_digest: object,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(id, name="id")
    validate_nonempty_string(effect_key, name="effect_key")
    validate_nonempty_string(effect_type, name="effect_type")
    _validate_effect_lease(lease_token, fencing_token)
    validate_nonempty_string(operation_digest, name="operation_digest")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_effect_status(
    id: object,
    effect_key: object,
    *,
    lease_token: object,
    fencing_token: object,
    latency_ms: object | None,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(id, name="id")
    validate_nonempty_string(effect_key, name="effect_key")
    _validate_effect_lease(lease_token, fencing_token)
    validate_optional_nonnegative_int(latency_ms, name="latency_ms")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_effect_get(id: object, effect_key: object) -> None:
    validate_nonempty_string(id, name="id")
    validate_nonempty_string(effect_key, name="effect_key")


def _validate_effect_lease(lease_token: object, fencing_token: object) -> None:
    if not isinstance(lease_token, bytes) or not lease_token:
        raise ValueError("lease_token must be non-empty bytes")
    validate_nonnegative_int(fencing_token, name="fencing_token")


def validate_ledger_options(
    id: object,
    *,
    limit: object | None,
    from_ms: object | None,
    to_ms: object | None,
    rev: object | None,
) -> None:
    validate_nonempty_string(id, name="id")
    validate_optional_positive_int(limit, name="limit")
    validate_optional_nonnegative_int(from_ms, name="from_ms")
    validate_optional_nonnegative_int(to_ms, name="to_ms")
    validate_optional_bool(rev, name="rev")


def validate_retention_cleanup(*, limit: object | None, now_ms: object | None) -> None:
    validate_optional_positive_int(limit, name="limit")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_approval_request(
    *,
    id: object,
    flow_id: object,
    scope: object,
    policy_hash: object | None,
    policy_version: object | None,
    timeout_ms: object | None,
    expires_at_ms: object | None,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(id, name="id")
    validate_nonempty_string(flow_id, name="flow_id")
    validate_nonempty_string(scope, name="scope")
    validate_optional_nonempty_string(policy_hash, name="policy_hash")
    if policy_version is not None:
        if isinstance(policy_version, str):
            validate_nonempty_string(policy_version, name="policy_version")
        else:
            validate_nonnegative_int(policy_version, name="policy_version")
    validate_optional_positive_int(timeout_ms, name="timeout_ms")
    if timeout_ms is None:
        validate_optional_nonnegative_int(expires_at_ms, name="expires_at_ms")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_approval_decision(
    *,
    id: object,
    approver: object,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(id, name="id")
    validate_nonempty_string(approver, name="approver")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_approval_list(
    *,
    status: object | None,
    scope: object | None,
    partition_key: object | None,
    flow_id: object | None,
    limit: object | None,
) -> None:
    if status is not None and status not in _APPROVAL_STATUSES:
        raise ValueError("status must be pending, approved, or rejected")
    _validate_optional_scope_filters(scope, partition_key)
    validate_optional_nonempty_string(flow_id, name="flow_id")
    validate_optional_positive_int(limit, name="limit")


def validate_circuit_operation(
    scope: object,
    *,
    open_ms: object | None = None,
    failure_threshold: object | None = None,
    now_ms: object | None = None,
) -> None:
    validate_nonempty_string(scope, name="scope")
    validate_optional_positive_int(open_ms, name="open_ms")
    validate_optional_positive_int(failure_threshold, name="failure_threshold")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_circuit_open(
    scope: object,
    *,
    open_ms: object | None,
    failure_threshold: object | None,
    window_ms: object | None,
    min_calls: object | None,
    failure_rate_pct: object | None,
    latency_threshold_ms: object | None,
    error_classes: object | None,
    half_open_max_probes: object | None,
    half_open_success_threshold: object | None,
    now_ms: object | None,
) -> tuple[str, ...] | None:
    validate_circuit_operation(
        scope,
        open_ms=open_ms,
        failure_threshold=failure_threshold,
        now_ms=now_ms,
    )
    validate_optional_positive_int(window_ms, name="window_ms")
    validated_min_calls = validate_optional_positive_int(min_calls, name="min_calls")
    validated_failure_rate = validate_optional_positive_int(
        failure_rate_pct,
        name="failure_rate_pct",
    )
    if validated_failure_rate is not None and validated_failure_rate > 100:
        raise ValueError("failure_rate_pct must be between 1 and 100")
    validate_optional_positive_int(latency_threshold_ms, name="latency_threshold_ms")
    validate_optional_positive_int(half_open_max_probes, name="half_open_max_probes")
    validate_optional_positive_int(
        half_open_success_threshold,
        name="half_open_success_threshold",
    )

    threshold = 5 if failure_threshold is None else failure_threshold
    if validated_min_calls is not None and validated_min_calls > 64:
        raise ValueError("min_calls cannot exceed 64")
    if isinstance(threshold, int) and threshold > 64:
        if validated_failure_rate is None:
            raise ValueError("failure_threshold cannot exceed 64 without failure_rate_pct")
        if validated_min_calls is None:
            raise ValueError("min_calls is required when failure_threshold exceeds 64")

    if error_classes is None:
        return None
    values = validate_string_sequence(error_classes, name="error_classes")
    return tuple(dict.fromkeys(values))


def validate_budget_reserve(
    scope: object,
    amount: object,
    *,
    limit: object | None,
    window_ms: object | None,
    reservation_id: object | None,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(scope, name="scope")
    validate_positive_int(amount, name="amount")
    validate_optional_positive_int(limit, name="limit")
    validate_optional_positive_int(window_ms, name="window_ms")
    validate_optional_nonempty_string(reservation_id, name="reservation_id")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_budget_settlement(
    scope: object,
    reservation_id: object,
    *,
    actual_amount: object | None = None,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(scope, name="scope")
    validate_nonempty_string(reservation_id, name="reservation_id")
    if actual_amount is not None:
        validate_nonnegative_int(actual_amount, name="actual_amount")
    validate_optional_nonnegative_int(now_ms, name="now_ms")


def validate_budget_list(
    *,
    scope: object | None,
    partition_key: object | None,
    limit: object | None,
) -> None:
    _validate_optional_scope_filters(scope, partition_key)
    validate_optional_positive_int(limit, name="limit")


def validate_workflow_budget_options(
    scope: object,
    amount: object,
    *,
    limit: object | None,
    window_ms: object | None,
    usage_key: object,
    attribute_prefix: object,
) -> None:
    validate_budget_reserve(
        scope,
        amount,
        limit=limit,
        window_ms=window_ms,
        reservation_id=None,
        now_ms=None,
    )
    validate_nonempty_string(usage_key, name="usage_key")
    validate_nonempty_string(attribute_prefix, name="attribute_prefix")


def _validate_limit_common(
    scope: object,
    *,
    shard_id: object,
    amount: object,
    now_ms: object | None,
) -> None:
    validate_nonempty_string(scope, name="scope")
    validate_nonnegative_int(shard_id, name="shard_id")
    validated_amount = validate_positive_int(amount, name="amount")
    if validated_amount > _MAX_LIMIT_MUTATION_AMOUNT:
        raise ValueError(f"amount cannot exceed {_MAX_LIMIT_MUTATION_AMOUNT}")
    _validate_optional_exact_nonnegative_int(now_ms, name="now_ms")


def validate_limit_lease(
    scope: object,
    *,
    shard_id: object,
    amount: object,
    ttl_ms: object,
    limit: object | None,
    now_ms: object | None,
) -> None:
    _validate_limit_common(scope, shard_id=shard_id, amount=amount, now_ms=now_ms)
    validated_ttl_ms = _validate_exact_positive_int(ttl_ms, name="ttl_ms")
    _validate_optional_exact_nonnegative_int(limit, name="limit")
    if (
        isinstance(now_ms, int)
        and not isinstance(now_ms, bool)
        and now_ms > _MAX_EXACT_INTEGER - validated_ttl_ms
    ):
        raise ValueError("now_ms plus ttl_ms exceeds the supported integer range")


def validate_limit_spend(
    scope: object,
    *,
    shard_id: object,
    amount: object,
    now_ms: object | None,
) -> None:
    _validate_limit_common(scope, shard_id=shard_id, amount=amount, now_ms=now_ms)


def validate_limit_reservation_ids(value: object) -> tuple[str, ...]:
    """Validate the exact identifiers required to safely release spent credits."""

    reservation_ids = validate_string_sequence(
        value,
        name="reservation_ids",
        allow_empty=False,
    )
    if len(reservation_ids) > _MAX_LIMIT_MUTATION_AMOUNT:
        raise ValueError(
            f"reservation_ids cannot contain more than {_MAX_LIMIT_MUTATION_AMOUNT} items"
        )
    if len(set(reservation_ids)) != len(reservation_ids):
        raise ValueError("reservation_ids must contain unique values")
    if any(len(item.encode("utf-8")) > _MAX_RESERVATION_ID_BYTES for item in reservation_ids):
        raise ValueError(f"reservation_ids values cannot exceed {_MAX_RESERVATION_ID_BYTES} bytes")
    return reservation_ids


def validate_limit_release(
    scope: object,
    *,
    shard_id: object,
    reservation_ids: object,
    now_ms: object | None,
) -> tuple[str, ...]:
    validate_nonempty_string(scope, name="scope")
    validate_nonnegative_int(shard_id, name="shard_id")
    validated_ids = validate_limit_reservation_ids(reservation_ids)
    _validate_optional_exact_nonnegative_int(now_ms, name="now_ms")
    return validated_ids


def validate_limit_get(scope: object, *, now_ms: object | None) -> None:
    validate_nonempty_string(scope, name="scope")
    _validate_optional_exact_nonnegative_int(now_ms, name="now_ms")


def validate_limit_list(
    *,
    scope: object | None,
    partition_key: object | None,
    limit: object | None,
    now_ms: object | None,
) -> None:
    _validate_optional_scope_filters(scope, partition_key)
    validate_optional_positive_int(limit, name="limit")
    _validate_optional_exact_nonnegative_int(now_ms, name="now_ms")


__all__ = [
    "validate_approval_decision",
    "validate_approval_list",
    "validate_approval_request",
    "validate_budget_list",
    "validate_budget_reserve",
    "validate_budget_settlement",
    "validate_circuit_open",
    "validate_circuit_operation",
    "validate_effect_get",
    "validate_effect_reserve",
    "validate_effect_status",
    "validate_ledger_options",
    "validate_limit_get",
    "validate_limit_lease",
    "validate_limit_list",
    "validate_limit_release",
    "validate_limit_reservation_ids",
    "validate_limit_spend",
    "validate_nonempty_string",
    "validate_optional_nonempty_string",
    "validate_retention_cleanup",
    "validate_schedule_create",
    "validate_schedule_fire_due",
    "validate_schedule_list",
    "validate_schedule_operation",
    "validate_workflow_budget_options",
]
