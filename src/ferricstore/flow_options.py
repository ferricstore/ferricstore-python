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
    "MAX_ACTIVE_MS": "max_active_ms",
    "MAX_BYTES": "max_bytes",
    "MAXBYTES": "payload_max_bytes",
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
    "PAYLOAD_REF": "payload_ref",
    "PRIORITY": "priority",
    "POLICY_HASH": "policy_hash",
    "POLICY_VERSION": "policy_version",
    "REASON": "reason",
    "REASON_REF": "reason_ref",
    "REQUESTED_BY": "requested_by",
    "RESERVATION_ID": "reservation_id",
    "RESERVATION_IDS": "reservation_ids",
    "RECLAIM_EXPIRED": "reclaim_expired",
    "RECLAIM_RATIO": "reclaim_ratio",
    "RESULT": "result",
    "RESULT_REF": "result_ref",
    "RETENTION_TTL_MS": "retention_ttl_ms",
    "RETENTION_TTL": "retention_ttl_ms",
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
_MAX_FLOW_OPTION_TOKEN_BYTES = max(len(token) for token in FLOW_OPTION_TOKENS)
_FLOW_OPTION_BYTES = {token.encode("ascii"): token for token in FLOW_OPTION_TOKENS}


class FlowOptionPlan:
    """One linear grammar analysis reused by routing and command construction."""

    __slots__ = ("_valid", "args", "tokens")

    def __init__(self, args: Sequence[Any]) -> None:
        self.args = args
        self.tokens = tuple(_token(value) for value in args)
        size = len(args)
        valid = bytearray(size + 1)
        valid[size] = 1

        for index in range(size - 1, -1, -1):
            token = self.tokens[index]
            if token in {"ITEMS", "ITEMS_EXT"}:
                valid[index] = 1
            elif token not in FLOW_OPTION_TOKENS:
                continue
            elif token == "NOPAYLOAD":
                valid[index] = self._suffix(valid, index + 1)
            elif token == "PAYLOAD":
                if index + 1 >= size:
                    valid[index] = 1
                    continue
                next_value = args[index + 1]
                next_is_option = self.tokens[index + 1] in FLOW_OPTION_TOKENS
                if isinstance(next_value, str) and next_is_option:
                    valid[index] = self._suffix(valid, index + 1)
                elif next_is_option:
                    valid[index] = self._suffix(valid, index + 1) or self._suffix(valid, index + 2)
                else:
                    valid[index] = self._suffix(valid, index + 2)
            elif token == "PARTITIONS":
                count = _non_negative_int(args[index + 1] if index + 1 < size else None)
                if count is not None:
                    valid[index] = self._suffix(valid, index + 2 + count)
            else:
                width = 3 if token in FLOW_THREE_VALUE_OPTIONS else 2
                valid[index] = self._suffix(valid, index + width)
        self._valid = valid

    @staticmethod
    def _suffix(valid: bytearray, index: int) -> int:
        return valid[index] if 0 <= index < len(valid) else 0

    def suffix_is_valid(self, start: int) -> bool:
        return bool(self._suffix(self._valid, start))

    def payload_is_flag(self, index: int) -> bool:
        if index + 1 >= len(self.args):
            return True
        next_value = self.args[index + 1]
        if self.tokens[index + 1] not in FLOW_OPTION_TOKENS:
            return False
        if isinstance(next_value, str):
            # Text tokens are public grammar. Bytes remain eligible as opaque
            # payloads even when they spell an option keyword.
            return True
        return self.suffix_is_valid(index + 1) and not self.suffix_is_valid(index + 2)

    def option_width(self, index: int) -> int | None:
        token = self.tokens[index]
        if token not in FLOW_OPTION_TOKENS or token in {"ITEMS", "ITEMS_EXT"}:
            return None
        if token == "NOPAYLOAD" or (token == "PAYLOAD" and self.payload_is_flag(index)):
            return 1
        if token == "PARTITIONS":
            count = _non_negative_int(self.args[index + 1] if index + 1 < len(self.args) else None)
            return None if count is None else 2 + count
        return 3 if token in FLOW_THREE_VALUE_OPTIONS else 2


def flow_payload_is_flag(
    args: Sequence[Any],
    index: int,
    *,
    plan: FlowOptionPlan | None = None,
) -> bool:
    """Resolve PAYLOAD's flag/value ambiguity from the shared Flow grammar."""
    return (plan or FlowOptionPlan(args)).payload_is_flag(index)


def flow_option_width(
    args: Sequence[Any],
    index: int,
    *,
    plan: FlowOptionPlan | None = None,
) -> int | None:
    """Return the option width, or ``None`` for an invalid variable-width option."""
    return (plan or FlowOptionPlan(args)).option_width(index)


def _valid_option_suffix(args: Sequence[Any], start: int) -> bool:
    return FlowOptionPlan(args).suffix_is_valid(start)


def _token(value: Any) -> str | None:
    if isinstance(value, bytes):
        if len(value) > _MAX_FLOW_OPTION_TOKEN_BYTES:
            return None
        return _FLOW_OPTION_BYTES.get(bytes(value).upper())
    if not isinstance(value, str) or len(value) > _MAX_FLOW_OPTION_TOKEN_BYTES:
        return None
    normalized = value.upper()
    return normalized if normalized in FLOW_OPTION_TOKENS else None


def _non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None
