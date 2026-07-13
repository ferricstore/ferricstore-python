from __future__ import annotations

from collections.abc import Sequence
from typing import Any

FLOW_OPTION_FIELD_NAMES = {
    "AFTER_MS": "after_ms",
    "ACTUAL_AMOUNT": "actual_amount",
    "AMOUNT": "amount",
    "APPROVER": "approver",
    "ASSIGNEES": "assignees",
    "AT_MS": "at_ms",
    "ATTRIBUTE": "attribute",
    "ATTRIBUTES": "attributes",
    "ATTRIBUTES_DELETE": "attributes_delete",
    "ATTRIBUTES_MERGE": "attributes_merge",
    "BACKOFF": "backoff",
    "BASE_MS": "base_ms",
    "BLOCK": "block_ms",
    "CONSISTENT_PROJECTION": "consistent_projection",
    "CORRELATION_ID": "correlation_id",
    "COUNT": "count",
    "CRON": "cron",
    "DELAY_MS": "delay_ms",
    "ERROR": "error",
    "ERROR_CLASS": "error_class",
    "ERROR_CLASSES": "error_classes",
    "ERROR_REF": "error_ref",
    "EFFECT_KEY": "effect_key",
    "EFFECT_TYPE": "effect_type",
    "END_AT_MS": "end_at_ms",
    "EVENT": "event",
    "EVERY_MS": "every_ms",
    "EXHAUSTED_TO": "exhausted_to",
    "EXPIRES_AT_MS": "expires_at_ms",
    "EXPECT_STATE": "expect_state",
    "EXTERNAL_ID": "external_id",
    "FENCING": "fencing_token",
    "FENCING_TOKEN": "fencing_token",
    "FAILURE": "failure",
    "FAILURE_RATE_PCT": "failure_rate_pct",
    "FROM_EVENT": "from_event",
    "FROM_MS": "from_ms",
    "FROM_STATE": "from_state",
    "FROM_VERSION": "from_version",
    "FULL": "full",
    "FAILURE_THRESHOLD": "failure_threshold",
    "FLOW_ID": "flow_id",
    "FIRE_AT_MS": "fire_at_ms",
    "GROUP": "group_id",
    "GROUP_ID": "group_id",
    "GOVERNANCE_SCOPE": "governance_scope",
    "HALF_OPEN_MAX_PROBES": "half_open_max_probes",
    "HALF_OPEN_SUCCESS_THRESHOLD": "half_open_success_threshold",
    "HISTORY_HOT_MAX_EVENTS": "history_hot_max_events",
    "HISTORY_MAX_EVENTS": "history_max_events",
    "IDEMPOTENT": "idempotent",
    "IDEMPOTENCY": "idempotency_key",
    "IDEMPOTENCY_KEY": "idempotency_key",
    "INCLUDE_COLD": "include_cold",
    "INDEPENDENT": "independent",
    "INDEXED_STATE_META": "indexed_state_meta",
    "IF_STATE": "if_state",
    "INITIAL_STATE": "initial_state",
    "ITEMS": "items",
    "JITTER_PCT": "jitter_pct",
    "KIND": "kind",
    "LATENCY_MS": "latency_ms",
    "LATENCY_THRESHOLD_MS": "latency_threshold_ms",
    "LEASE_MS": "lease_ms",
    "LEASE_TOKEN": "lease_token",
    "LIMIT": "limit",
    "LOCAL_CACHE": "local_cache",
    "MAX_FIRES": "max_fires",
    "MAX_ATTEMPTS": "max_attempts",
    "MAX_BYTES": "max_bytes",
    "MAX_MS": "max_ms",
    "MAX_RETRIES": "max_retries",
    "MIN_CALLS": "min_calls",
    "NAME": "name",
    "NOW": "now_ms",
    "OLDER_THAN": "older_than_ms",
    "ON_CHILD_FAILED": "on_child_failed",
    "ON_PARENT_CLOSED": "on_parent_closed",
    "OPEN_MS": "open_ms",
    "OPERATION_DIGEST": "operation_digest",
    "OVERRIDE": "override",
    "OVERLAP_POLICY": "overlap_policy",
    "OVERLAP_RETRY_MS": "overlap_retry_ms",
    "OVERWRITE": "overwrite",
    "OWNER_FLOW_ID": "owner_flow_id",
    "PARENT_FLOW_ID": "parent_id",
    "PARENT_ID": "parent_id",
    "PARTITION": "partition_key",
    "PAYLOAD": "payload",
    "PAYLOAD_MAX_BYTES": "payload_max_bytes",
    "PRIORITY": "priority",
    "POLICY_HASH": "policy_hash",
    "POLICY_VERSION": "policy_version",
    "REASON": "reason",
    "REASON_REF": "reason_ref",
    "REQUESTED_BY": "requested_by",
    "RESERVATION_ID": "reservation_id",
    "RECLAIM_EXPIRED": "reclaim_expired",
    "RECLAIM_RATIO": "reclaim_ratio",
    "RESULT": "result",
    "RESULT_REF": "result_ref",
    "RETENTION_TTL_MS": "retention_ttl_ms",
    "REV": "rev",
    "ROOT_FLOW_ID": "root_id",
    "ROOT_ID": "root_id",
    "RUN_AT": "run_at_ms",
    "RUN_AT_MS": "run_at_ms",
    "SIGNAL": "signal",
    "SHARD_ID": "shard_id",
    "SCOPE": "scope",
    "STATE": "state",
    "STATE_META": "state_meta",
    "STATUS": "status",
    "STATES": "states",
    "START_AT_MS": "start_at_ms",
    "STEPS": "steps",
    "SUCCESS": "success",
    "TERMINAL_ONLY": "terminal_only",
    "TERMINAL_LOCAL_ONLY": "terminal_local_only",
    "TO_EVENT": "to_event",
    "TO_MS": "to_ms",
    "TO_STATE": "to_state",
    "TO_VERSION": "to_version",
    "TARGET": "target",
    "TARGET_TYPE": "target_type",
    "TRANSITION_TO": "transition_to",
    "TTL": "ttl_ms",
    "TTL_MS": "ttl_ms",
    "TIMEOUT_MS": "timeout_ms",
    "TIMEZONE": "timezone",
    "TYPE": "type",
    "USAGE": "usage",
    "VALUE_MAX_BYTES": "value_max_bytes",
    "VALUES": "values",
    "WAIT": "wait",
    "WAIT_STATE": "wait_state",
    "WINDOW_MS": "window_ms",
    "WORKER": "worker",
}

FLOW_THREE_VALUE_OPTIONS = frozenset(
    {
        "ATTRIBUTE",
        "ATTRIBUTE_MERGE",
        "STATE_META",
        "VALUE",
        "VALUE_REF",
    }
)

FLOW_OPTION_TOKENS = frozenset(FLOW_OPTION_FIELD_NAMES) | frozenset(
    {
        "ATTRIBUTE_DELETE",
        "ATTRIBUTE_MERGE",
        "DROP_VALUE",
        "ITEMS_EXT",
        "NOPAYLOAD",
        "OVERRIDE_VALUE",
        "PARTITIONS",
        "RETURN",
        "VALUE",
        "VALUE_REF",
    }
)


def flow_payload_is_flag(args: Sequence[Any], index: int) -> bool:
    """Resolve PAYLOAD's flag/value ambiguity from the shared Flow grammar."""
    return index + 1 >= len(args) or (
        isinstance(args[index + 1], str) and _token(args[index + 1]) in FLOW_OPTION_TOKENS
    )


def flow_option_width(args: Sequence[Any], index: int) -> int | None:
    """Return the option width, or ``None`` for an invalid variable-width option."""
    token = _token(args[index])
    if token == "NOPAYLOAD" or (token == "PAYLOAD" and flow_payload_is_flag(args, index)):
        return 1
    if token == "PARTITIONS":
        count = _non_negative_int(args[index + 1] if index + 1 < len(args) else None)
        return None if count is None else 2 + count
    return 3 if token in FLOW_THREE_VALUE_OPTIONS else 2


def _token(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").upper()
    return str(value).upper()


def _non_negative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None
