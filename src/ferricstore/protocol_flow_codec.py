from __future__ import annotations

import struct
from typing import Any, cast

from ferricstore.protocol_constants import (
    _COMPACT_FLOW_CANCEL_MANY_OK_REQUEST,
    _COMPACT_FLOW_CANCEL_MANY_REQUEST,
    _COMPACT_FLOW_CLAIM_DUE_REQUEST,
    _COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
    _COMPACT_FLOW_COMPLETE_MANY_REQUEST,
    _COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST,
    _COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST,
    _COMPACT_FLOW_CREATE_MANY_REQUEST,
    _COMPACT_FLOW_LIST_REQUEST,
    _COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST,
    _COMPACT_FLOW_TRANSITION_MANY_REQUEST,
    _COMPACT_FLOW_VALUE_MGET_REQUEST,
    _COMPACT_I64,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_U32,
    _I64_MIN,
    _NULL_U32,
)


def _compact_flow_create_many_payload(payload: dict[str, Any]) -> bytes | None:
    allowed = {
        "type",
        "state",
        "partition_key",
        "now_ms",
        "run_at_ms",
        "independent",
        "items",
        "return",
    }
    if not set(payload).issubset(allowed):
        return None
    items = payload.get("items")
    if not isinstance(items, list):
        return None
    type_value = _maybe_bytes(payload.get("type"))
    state = _maybe_bytes(payload.get("state"))
    now_ms = payload.get("now_ms")
    run_at_ms = payload.get("run_at_ms")
    if (
        type_value is None
        or state is None
        or not isinstance(now_ms, int)
        or not isinstance(run_at_ms, int)
    ):
        return None
    return_mode = _compact_create_many_return_mode(payload.get("return"))
    if return_mode is None:
        return None
    partition_value = _optional_bytes(payload.get("partition_key"))
    if partition_value is False:
        return None
    partition_key = cast(bytes | None, partition_value)
    mixed = partition_key is None and all(
        isinstance(item, list) and len(item) == 3 for item in items
    )

    parts = [
        bytes(
            [
                _COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST
                if mixed
                else (
                    _COMPACT_FLOW_CREATE_MANY_REQUEST
                    if partition_key is None
                    else _COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST
                )
            ]
        ),
        _compact_binary(type_value),
        _compact_binary(state),
    ]
    if partition_key is not None:
        parts.append(_compact_optional_binary(partition_key))
    parts.append(
        struct.pack(
            ">qqBBI",
            now_ms,
            run_at_ms,
            _compact_bool_marker(payload.get("independent")),
            return_mode,
            len(items),
        )
    )
    for item in items:
        if not isinstance(item, list) or len(item) != (3 if mixed else 2):
            return None
        item_id = _maybe_bytes(item[0])
        item_payload = _maybe_bytes(item[2] if mixed else item[1])
        if item_id is None or item_payload is None:
            return None
        parts.append(_compact_binary(item_id))
        if mixed:
            item_partition = _maybe_bytes(item[1])
            if item_partition is None:
                return None
            parts.append(_compact_binary(item_partition))
        parts.append(_compact_binary(item_payload))
    return b"".join(parts)


def _compact_flow_claim_due_payload(payload: dict[str, Any]) -> bytes | None:
    allowed = {
        "type",
        "state",
        "worker",
        "lease_ms",
        "limit",
        "partition_key",
        "partition_keys",
        "return",
        "block_ms",
        "reclaim_expired",
        "reclaim_ratio",
        "priority",
    }
    if not set(payload).issubset(allowed):
        return None
    if "partition_key" in payload and "partition_keys" in payload:
        return None
    type_value = _maybe_bytes(payload.get("type"))
    state = _optional_bytes(payload.get("state"))
    worker = _maybe_bytes(payload.get("worker"))
    lease_ms = payload.get("lease_ms")
    limit = payload.get("limit")
    block_ms = payload.get("block_ms", -1)
    reclaim_ratio = payload.get("reclaim_ratio", 0)
    priority = payload.get("priority", _I64_MIN)
    if (
        type_value is None
        or state is False
        or worker is None
        or not isinstance(lease_ms, int)
        or not isinstance(limit, int)
        or not isinstance(block_ms, int)
        or not isinstance(reclaim_ratio, int)
        or not isinstance(priority, int)
    ):
        return None
    return_mode = _compact_return_mode(payload.get("return"))
    if return_mode is None:
        return None

    partition_mode, partition_body = _compact_partition_request(payload)
    if partition_mode is None:
        return None

    return b"".join(
        [
            bytes([_COMPACT_FLOW_CLAIM_DUE_REQUEST]),
            _compact_binary(type_value),
            _compact_optional_binary(cast(bytes | None, state)),
            _compact_binary(worker),
            struct.pack(
                ">qqqBqqBB",
                lease_ms,
                limit,
                block_ms,
                1 if payload.get("reclaim_expired") else 0,
                reclaim_ratio,
                priority,
                return_mode,
                partition_mode,
            ),
            partition_body,
        ]
    )


def _compact_flow_complete_many_payload(payload: dict[str, Any]) -> bytes | None:
    return _compact_flow_claimed_many_payload(
        payload,
        request_kind=_COMPACT_FLOW_COMPLETE_MANY_REQUEST,
        ok_request_kind=_COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
        extra_allowed={"terminal_local_only"},
    )


def _compact_flow_claimed_many_payload(
    payload: dict[str, Any],
    *,
    request_kind: int,
    ok_request_kind: int,
    extra_allowed: set[str],
) -> bytes | None:
    allowed = {"partition_key", "now_ms", "independent", "items", "return"} | extra_allowed
    if not set(payload).issubset(allowed):
        return None
    now_ms = payload.get("now_ms")
    items = payload.get("items")
    if not isinstance(now_ms, int) or not isinstance(items, list):
        return None
    return_mode = payload.get("return")
    if return_mode is None:
        request_kind = _COMPACT_FLOW_COMPLETE_MANY_REQUEST
    else:
        if isinstance(return_mode, bytes):
            return_mode_text = return_mode.decode("utf-8", errors="replace")
        else:
            return_mode_text = str(return_mode)
        if return_mode_text.upper() != "OK_ON_SUCCESS":
            return None
        request_kind = ok_request_kind
    partition_value = _optional_bytes(payload.get("partition_key"))
    if partition_value is False:
        return None
    partition_key = cast(bytes | None, partition_value)

    pack_u32 = _COMPACT_U32.pack
    pack_i64 = _COMPACT_I64.pack
    body = bytearray()
    body.append(request_kind)
    if partition_key is None:
        body.extend(pack_u32(_NULL_U32))
    else:
        body.extend(pack_u32(len(partition_key)))
        body.extend(partition_key)

    if "run_at_ms" in extra_allowed:
        run_at_ms = payload.get("run_at_ms")
        if not isinstance(run_at_ms, int):
            return None
        body.extend(
            struct.pack(
                ">qqBI",
                now_ms,
                run_at_ms,
                _compact_bool_marker(payload.get("independent")),
                len(items),
            )
        )
    else:
        body.extend(
            struct.pack(
                ">qBI",
                now_ms,
                _compact_terminal_independent_marker(payload),
                len(items),
            )
        )

    for item in items:
        if not isinstance(item, list) or len(item) not in {3, 4}:
            return None
        item_id = _maybe_bytes(item[0])
        item_partition = None
        if len(item) == 4:
            item_partition = _maybe_bytes(item[1])
            if item_partition is None:
                return None
            lease_token = _maybe_bytes(item[2])
            fencing_token = item[3]
        else:
            lease_token = _maybe_bytes(item[1])
            fencing_token = item[2]
        if item_id is None or lease_token is None or not isinstance(fencing_token, int):
            return None
        body.extend(pack_u32(len(item_id)))
        body.extend(item_id)
        if item_partition is None:
            body.extend(pack_u32(_NULL_U32))
        else:
            body.extend(pack_u32(len(item_partition)))
            body.extend(item_partition)
        body.extend(pack_u32(len(lease_token)))
        body.extend(lease_token)
        body.extend(pack_i64(fencing_token))
    return bytes(body)


def _compact_flow_cancel_many_payload(payload: dict[str, Any]) -> bytes | None:
    allowed = {"partition_key", "now_ms", "independent", "items", "return"}
    if not set(payload).issubset(allowed):
        return None
    now_ms = payload.get("now_ms")
    items = payload.get("items")
    if not isinstance(now_ms, int) or not isinstance(items, list):
        return None
    return_mode = payload.get("return")
    if return_mode is None:
        request_kind = _COMPACT_FLOW_CANCEL_MANY_REQUEST
    else:
        return_mode_text = (
            return_mode.decode("utf-8", errors="replace")
            if isinstance(return_mode, bytes)
            else str(return_mode)
        )
        if return_mode_text.upper() != "OK_ON_SUCCESS":
            return None
        request_kind = _COMPACT_FLOW_CANCEL_MANY_OK_REQUEST
    partition_key = _optional_bytes(payload.get("partition_key"))
    if partition_key is False:
        return None

    parts = [
        bytes([request_kind]),
        _compact_optional_binary(cast(bytes | None, partition_key)),
        struct.pack(">qBI", now_ms, _compact_bool_marker(payload.get("independent")), len(items)),
    ]
    for item in items:
        if not isinstance(item, dict):
            return None
        item_id = _maybe_bytes(item.get("id"))
        item_partition = _optional_bytes(item.get("partition_key"))
        fencing_token = item.get("fencing_token")
        if item_id is None or item_partition is False or not isinstance(fencing_token, int):
            return None
        parts.append(_compact_binary(item_id))
        parts.append(_compact_optional_binary(cast(bytes | None, item_partition)))
        parts.append(struct.pack(">q", fencing_token))
    return b"".join(parts)


def _compact_flow_transition_many_payload(payload: dict[str, Any]) -> bytes | None:
    allowed = {
        "partition_key",
        "from_state",
        "to_state",
        "now_ms",
        "run_at_ms",
        "independent",
        "items",
        "return",
    }
    if not set(payload).issubset(allowed):
        return None
    from_state = _maybe_bytes(payload.get("from_state"))
    to_state = _maybe_bytes(payload.get("to_state"))
    now_ms = payload.get("now_ms")
    run_at_ms = payload.get("run_at_ms")
    items = payload.get("items")
    if (
        from_state is None
        or to_state is None
        or not isinstance(now_ms, int)
        or not isinstance(run_at_ms, int)
        or not isinstance(items, list)
    ):
        return None
    return_mode = payload.get("return")
    if return_mode is None:
        request_kind = _COMPACT_FLOW_TRANSITION_MANY_REQUEST
    else:
        return_mode_text = (
            return_mode.decode("utf-8", errors="replace")
            if isinstance(return_mode, bytes)
            else str(return_mode)
        )
        if return_mode_text.upper() != "OK_ON_SUCCESS":
            return None
        request_kind = _COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST
    partition_key = _optional_bytes(payload.get("partition_key"))
    if partition_key is False:
        return None

    parts = [
        bytes([request_kind]),
        _compact_binary(from_state),
        _compact_binary(to_state),
        _compact_optional_binary(cast(bytes | None, partition_key)),
        struct.pack(
            ">qqBI",
            now_ms,
            run_at_ms,
            _compact_bool_marker(payload.get("independent")),
            len(items),
        ),
    ]
    for item in items:
        if not isinstance(item, dict):
            return None
        item_id = _maybe_bytes(item.get("id"))
        item_partition = _optional_bytes(item.get("partition_key"))
        lease_token = _optional_bytes(item.get("lease_token"))
        fencing_token = item.get("fencing_token")
        if (
            item_id is None
            or item_partition is False
            or lease_token is False
            or not isinstance(fencing_token, int)
        ):
            return None
        parts.append(_compact_binary(item_id))
        parts.append(_compact_optional_binary(cast(bytes | None, item_partition)))
        parts.append(struct.pack(">q", fencing_token))
        parts.append(_compact_optional_binary(cast(bytes | None, lease_token)))
    return b"".join(parts)


def _compact_flow_value_mget_payload(payload: dict[str, Any]) -> bytes | None:
    if not set(payload).issubset({"refs", "max_bytes"}):
        return None
    refs = payload.get("refs")
    if not isinstance(refs, list):
        return None
    max_bytes_value = payload.get("max_bytes", _I64_MIN)
    if not isinstance(max_bytes_value, int):
        return None

    parts = [
        bytes([_COMPACT_FLOW_VALUE_MGET_REQUEST]),
        struct.pack(">qI", max_bytes_value, len(refs)),
    ]
    for ref in refs:
        encoded = _maybe_bytes(ref)
        if encoded is None:
            return None
        parts.append(_compact_binary(encoded))
    return b"".join(parts)


def _compact_flow_list_payload(payload: dict[str, Any]) -> bytes | None:
    if not set(payload).issubset({"type", "state", "count", "return"}):
        return None

    flow_type = _maybe_bytes(payload.get("type"))
    state = _optional_bytes(payload.get("state"))
    count = _raw_int(payload.get("count"))
    if flow_type is None or state is False or count is None:
        return None

    return_value = payload.get("return")
    if return_value is None:
        return_mode = 0
    elif _maybe_bytes(return_value) == b"META" or _maybe_bytes(return_value) == b"meta":
        return_mode = 1
    else:
        return None

    return (
        bytes([_COMPACT_FLOW_LIST_REQUEST])
        + _compact_binary(flow_type)
        + _compact_optional_binary(cast(bytes | None, state))
        + struct.pack(">qB", count, return_mode)
    )


def _raw_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ok_on_success_return_mode(value: Any) -> bool:
    normalized = _maybe_bytes(value)
    return normalized is not None and normalized.upper() == b"OK_ON_SUCCESS"


def _compact_flow_value_put_payload(mode: int, items: list[dict[str, Any]]) -> bytes | None:
    if not items:
        return None

    parts = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | mode, len(items))]
    if mode in {7, 15}:
        for item in items:
            parts.append(_compact_binary(cast(bytes, item["value"])))
            parts.append(struct.pack(">q", cast(int, item["now_ms"])))
        return b"".join(parts)

    if mode in {8, 14}:
        for item in items:
            parts.append(_compact_binary(cast(bytes, item["value"])))
            parts.append(_compact_binary(cast(bytes, item["owner_flow_id"])))
            parts.append(_compact_binary(cast(bytes, item["name"])))
            parts.append(_compact_optional_binary(cast(bytes | None, item["partition_key"])))
            parts.append(struct.pack(">q", cast(int, item["now_ms"])))
        return b"".join(parts)

    return None


def _compact_partition_request(payload: dict[str, Any]) -> tuple[int | None, bytes]:
    if "partition_key" in payload:
        value = _maybe_bytes(payload.get("partition_key"))
        if value is None:
            return None, b""
        return 1, _compact_binary(value)
    if "partition_keys" in payload:
        values = payload.get("partition_keys")
        if not isinstance(values, list):
            return None, b""
        parts = [struct.pack(">I", len(values))]
        for value in values:
            encoded = _maybe_bytes(value)
            if encoded is None:
                return None, b""
            parts.append(_compact_binary(encoded))
        return 2, b"".join(parts)
    return 0, b""


def _maybe_bytes(value: Any) -> bytes | None:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    return None


def _optional_bytes(value: Any) -> bytes | None | bool:
    if value is None:
        return None
    encoded = _maybe_bytes(value)
    return encoded if encoded is not None else False


def _compact_binary(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def _compact_optional_binary(value: bytes | None) -> bytes:
    if value is None:
        return struct.pack(">I", _NULL_U32)
    return _compact_binary(value)


def _compact_bool_marker(value: Any) -> int:
    if value is None:
        return 0
    return 2 if bool(value) else 1


def _compact_terminal_independent_marker(payload: dict[str, Any]) -> int:
    if payload.get("terminal_local_only"):
        return 3 if payload.get("independent") is True else 4
    return _compact_bool_marker(payload.get("independent"))


def _compact_return_mode(value: Any) -> int | None:
    if value is None:
        return 0
    if value in {"jobs_compact", "JOBS_COMPACT"}:
        return 1
    if value in {"jobs_compact_state", "JOBS_COMPACT_STATE"}:
        return 2
    if value in {
        "jobs_compact_attrs",
        "JOBS_COMPACT_ATTRS",
        "jobs_compact_attributes",
        "JOBS_COMPACT_ATTRIBUTES",
    }:
        return 3
    if value in {
        "jobs_compact_state_attrs",
        "JOBS_COMPACT_STATE_ATTRS",
        "jobs_compact_with_state_attrs",
        "JOBS_COMPACT_WITH_STATE_ATTRS",
        "jobs_compact_state_attributes",
        "JOBS_COMPACT_STATE_ATTRIBUTES",
        "jobs_compact_with_state_attributes",
        "JOBS_COMPACT_WITH_STATE_ATTRIBUTES",
    }:
        return 4
    return None


def _compact_create_many_return_mode(value: Any) -> int | None:
    if value is None:
        return 0
    normalized = _maybe_bytes(value)
    if normalized is None:
        return None
    if normalized.upper() == b"OK_ON_SUCCESS":
        return 1
    return None


__all__ = [
    "_compact_binary",
    "_compact_bool_marker",
    "_compact_create_many_return_mode",
    "_compact_flow_cancel_many_payload",
    "_compact_flow_claim_due_payload",
    "_compact_flow_claimed_many_payload",
    "_compact_flow_complete_many_payload",
    "_compact_flow_create_many_payload",
    "_compact_flow_list_payload",
    "_compact_flow_transition_many_payload",
    "_compact_flow_value_mget_payload",
    "_compact_flow_value_put_payload",
    "_compact_optional_binary",
    "_compact_partition_request",
    "_compact_return_mode",
    "_compact_terminal_independent_marker",
    "_maybe_bytes",
    "_ok_on_success_return_mode",
    "_optional_bytes",
    "_raw_int",
]
