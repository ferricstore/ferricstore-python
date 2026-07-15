from __future__ import annotations

import builtins
from typing import Any

from ferricstore.client_helpers import (
    _append,
    _append_bool,
    _append_payload_read,
    _append_priority,
    _append_value_return,
    _now_ms,
)
from ferricstore.config_validation import (
    validate_bool,
    validate_bounded_nonnegative_int,
    validate_optional_bool,
    validate_optional_nonnegative_int,
    validate_positive_int,
    validate_string_sequence,
)
from ferricstore.errors import FerricStoreError


def _claim_due_command_args(
    type: str,
    *,
    state: str | None = None,
    states: builtins.list[str] | None = None,
    worker: str,
    partition_key: str | None = None,
    partition_keys: builtins.list[str] | None = None,
    lease_ms: int = 30_000,
    limit: int = 1,
    priority: int | None = None,
    now_ms: int | None = None,
    block_ms: int | None = None,
    reclaim_expired: bool | None = None,
    reclaim_ratio: int | None = None,
    include_record: bool = True,
    payload: bool | None = None,
    payload_max_bytes: int | None = None,
    values: builtins.list[str] | None = None,
    value_max_bytes: int | None = None,
    include_state: bool = False,
    include_attributes: bool = True,
) -> builtins.list[Any]:
    lease_ms = validate_positive_int(lease_ms, name="lease_ms")
    limit = validate_positive_int(limit, name="limit")
    now_ms = validate_optional_nonnegative_int(now_ms, name="now_ms")
    block_ms = validate_optional_nonnegative_int(block_ms, name="block_ms")
    include_record = validate_bool(include_record, name="include_record")
    include_state = validate_bool(include_state, name="include_state")
    include_attributes = validate_bool(include_attributes, name="include_attributes")
    reclaim_expired = validate_optional_bool(reclaim_expired, name="reclaim_expired")
    if reclaim_ratio is not None:
        reclaim_ratio = validate_bounded_nonnegative_int(
            reclaim_ratio,
            name="reclaim_ratio",
            maximum=100,
        )
    resolved_states = (
        validate_string_sequence(states, name="states", allow_empty=False)
        if states is not None
        else None
    )
    resolved_partition_keys = (
        validate_string_sequence(partition_keys, name="partition_keys", allow_empty=False)
        if partition_keys is not None
        else None
    )

    args: builtins.list[Any] = ["FLOW.CLAIM_DUE", type]
    if state is not None and states is not None:
        raise ValueError("state and states are mutually exclusive")
    if resolved_states is not None:
        for item in resolved_states:
            _append(args, "STATE", item)
    else:
        _append(args, "STATE", state)
    args.extend(["WORKER", worker, "LEASE_MS", lease_ms, "LIMIT", limit])
    _append(args, "NOW", now_ms)
    if partition_key is not None and partition_keys is not None:
        raise ValueError("partition_key and partition_keys are mutually exclusive")
    _append(args, "PARTITION", partition_key)
    if resolved_partition_keys is not None:
        args.extend(["PARTITIONS", len(resolved_partition_keys), *resolved_partition_keys])
    _append_priority(args, priority)
    if not include_record:
        if include_state and include_attributes:
            return_mode = "JOBS_COMPACT_STATE_ATTRS"
        elif include_state:
            return_mode = "JOBS_COMPACT_STATE"
        elif include_attributes:
            return_mode = "JOBS_COMPACT_ATTRS"
        else:
            return_mode = "JOBS_COMPACT"
        _append(args, "RETURN", return_mode)
    _append(args, "BLOCK", block_ms)
    _append_payload_read(args, payload, payload_max_bytes)
    _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
    _append_bool(args, "RECLAIM_EXPIRED", reclaim_expired)
    _append(args, "RECLAIM_RATIO", reclaim_ratio)
    return args


def _reclaim_command_args(
    type: str,
    *,
    worker: str,
    partition_key: str | None,
    partition_keys: builtins.list[str] | None,
    lease_ms: int,
    limit: int,
    priority: int | None,
    now_ms: int | None,
    include_record: bool,
    payload: bool | None,
    payload_max_bytes: int | None,
    values: builtins.list[str] | None,
    value_max_bytes: int | None,
    include_attributes: bool,
) -> builtins.list[Any]:
    lease_ms = validate_positive_int(lease_ms, name="lease_ms")
    limit = validate_positive_int(limit, name="limit")
    now_ms = validate_optional_nonnegative_int(now_ms, name="now_ms")
    include_record = validate_bool(include_record, name="include_record")
    include_attributes = validate_bool(include_attributes, name="include_attributes")
    resolved_partition_keys = (
        validate_string_sequence(partition_keys, name="partition_keys", allow_empty=False)
        if partition_keys is not None
        else None
    )
    if partition_key is not None and partition_keys is not None:
        raise ValueError("partition_key and partition_keys are mutually exclusive")

    args: builtins.list[Any] = [
        "FLOW.RECLAIM",
        type,
        "WORKER",
        worker,
        "LEASE_MS",
        lease_ms,
        "LIMIT",
        limit,
        "NOW",
        now_ms if now_ms is not None else _now_ms(),
    ]
    _append(args, "PARTITION", partition_key)
    if resolved_partition_keys is not None:
        args.extend(["PARTITIONS", len(resolved_partition_keys), *resolved_partition_keys])
    _append_priority(args, priority)
    if not include_record:
        _append(args, "RETURN", "JOBS_COMPACT_ATTRS" if include_attributes else "JOBS_COMPACT")
    _append_payload_read(args, payload, payload_max_bytes)
    _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
    return args


def _resolve_include_record(include_record: bool | None, job_only: bool | None) -> bool:
    include_record = validate_optional_bool(include_record, name="include_record")
    job_only = validate_optional_bool(job_only, name="job_only")
    if job_only is None:
        return True if include_record is None else include_record
    legacy_include_record = not job_only
    if include_record is not None and include_record != legacy_include_record:
        raise ValueError("include_record and job_only cannot disagree")
    return legacy_include_record


def _claim_return_mode_unsupported(exc: FerricStoreError) -> bool:
    message = f"{exc.message} {exc.raw or ''}".lower()
    return "flow claim return must be records, jobs, or jobs_compact" in message


def _claim_return_compat_args(args: builtins.list[Any]) -> builtins.list[Any] | None:
    try:
        return_index = args.index("RETURN")
    except ValueError:
        return None

    rich_return_modes = {
        "JOBS_COMPACT_ATTRS",
        "JOBS_COMPACT_STATE",
        "JOBS_COMPACT_STATE_ATTRS",
    }
    if return_index + 1 >= len(args) or args[return_index + 1] not in rich_return_modes:
        return None

    compat_args = list(args)
    compat_args[return_index + 1] = "JOBS_COMPACT"
    return compat_args


__all__ = []
