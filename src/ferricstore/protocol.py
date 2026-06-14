from __future__ import annotations

import asyncio
import contextlib
import socket
import ssl
import struct
import threading
import time
import zlib
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, replace
from typing import Any, Sequence, cast
from urllib.parse import unquote, urlparse

from ferricstore.errors import FerricStoreError, InvalidCommandError, OverloadedError

_MAGIC = b"FSNP"
_REQUEST_VERSION = 0x01
_RESPONSE_VERSION = 0x81
_HEADER = struct.Struct(">4sBBIHQI")
_STATUS = struct.Struct(">H")

_FLAG_TRACE = 0x01
_FLAG_COMPRESSED = 0x08
_FLAG_CUSTOM_PAYLOAD = 0x02
_FLAG_MORE_CHUNKS = 0x20

_OP_PIPELINE = 0x000E
_OP_STARTUP = 0x000C
_OP_AUTH = 0x0002
_OP_SUBSCRIBE_EVENTS = 0x0011
_OP_UNSUBSCRIBE_EVENTS = 0x0012
_OP_GET = 0x0101
_OP_SET = 0x0102
_OP_MGET = 0x0104
_OP_MSET = 0x0105
_OP_FLOW_GET = 0x0202
_OP_FLOW_CLAIM_DUE = 0x0203
_OP_FLOW_VALUE_MGET = 0x020C
_OP_FLOW_CREATE_MANY = 0x020F
_OP_FLOW_COMPLETE_MANY = 0x0210
_OP_FLOW_RETRY_MANY = 0x0212
_OP_FLOW_FAIL_MANY = 0x0213
_OP_FLOW_CANCEL_MANY = 0x0214
_OP_FLOW_RUN_STEPS_MANY = 0x0224

_STATUS_OK = 0
_STATUS_BUSY = 4

_COMPACT_FLOW_CLAIM_JOBS = 0x80
_COMPACT_OK_LIST = 0x81
_COMPACT_KV_GET = 0x82
_COMPACT_KV_MGET = 0x83
_COMPACT_FLOW_RECORD = 0x84
_COMPACT_FLOW_RECORD_LIST = 0x85
_COMPACT_BINARY_LIST_LIST = 0x86
_COMPACT_BINARY_MAP_LIST = 0x87
_COMPACT_INTEGER_LIST = 0x88
_COMPACT_KV_MGET_FIXED = 0x89
_COMPACT_FLOW_CREATE_MANY_REQUEST = 0x90
_COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST = 0x96
_COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST = 0x9E
_COMPACT_FLOW_CLAIM_DUE_REQUEST = 0x91
_COMPACT_FLOW_COMPLETE_MANY_REQUEST = 0x92
_COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST = 0x93
_COMPACT_PIPELINE_REQUEST = 0x94
_COMPACT_PIPELINE_RESPONSE = 0x95
_COMPACT_FLOW_RETRY_MANY_REQUEST = 0x97
_COMPACT_FLOW_RETRY_MANY_OK_REQUEST = 0x98
_COMPACT_FLOW_CANCEL_MANY_REQUEST = 0x99
_COMPACT_FLOW_CANCEL_MANY_OK_REQUEST = 0x9A
_COMPACT_FLOW_TRANSITION_MANY_REQUEST = 0x9B
_COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST = 0x9C
_COMPACT_FLOW_VALUE_MGET_REQUEST = 0x9D
_COMPACT_FLOW_LIST_REQUEST = 0x9F
_COMPACT_PIPELINE_HEADER = struct.Struct(">BBI")
_COMPACT_U32 = struct.Struct(">I")
_COMPACT_I64 = struct.Struct(">q")
_COMPACT_F64 = struct.Struct(">d")
_NULL_U32 = 0xFFFFFFFF
_I64_MIN = -(1 << 63)
_COMPACT_HGET_PIPELINE_MODE = 18
_COMPACT_SISMEMBER_PIPELINE_MODE = 19
_COMPACT_LRANGE_PIPELINE_MODE = 20
_COMPACT_ZRANGE_PIPELINE_MODE = 21
_COMPACT_HSET_PIPELINE_MODE = 22
_COMPACT_LPUSH_PIPELINE_MODE = 23
_COMPACT_RPUSH_PIPELINE_MODE = 24
_COMPACT_SADD_PIPELINE_MODE = 25
_COMPACT_ZADD_PIPELINE_MODE = 26
_COMPACT_SMEMBERS_PIPELINE_MODE = 27
_COMPACT_HMGET_PIPELINE_MODE = 28
_COMPACT_ZSCORE_PIPELINE_MODE = 29
_COMPACT_HGETALL_PIPELINE_MODE = 30

_FLOW_RECORD_LIST_OPCODES = {
    0x020E,  # FLOW.LIST
    0x0217,  # FLOW.TERMINALS
    0x0218,  # FLOW.FAILURES
    0x0219,  # FLOW.BY_PARENT
    0x021A,  # FLOW.BY_ROOT
    0x021B,  # FLOW.BY_CORRELATION
    0x021D,  # FLOW.STUCK
}

_FLOW_RECORD_FIELD_KEYS = (
    b"",
    b"id",
    b"type",
    b"state",
    b"version",
    b"priority",
    b"partition_key",
    b"payload_ref",
    b"result_ref",
    b"error_ref",
    b"payload",
    b"result",
    b"error",
    b"created_at_ms",
    b"updated_at_ms",
    b"next_run_at_ms",
    b"lease_deadline_ms",
    b"lease_owner",
    b"lease_token",
    b"fencing_token",
    b"attempts",
    b"history_max_events",
    b"history_hot_max_events",
    b"child_groups",
    b"parent_flow_id",
    b"parent_partition_key",
    b"root_flow_id",
    b"correlation_id",
    b"terminal_retention_until_ms",
    b"ttl_ms",
    b"retention_ttl_ms",
    b"run_state",
    b"value_refs",
    b"values",
    b"payload_omitted",
    b"payload_size",
    b"result_omitted",
    b"result_size",
    b"error_omitted",
    b"error_size",
    b"max_attempts",
)
_FLOW_RECORD_FIELD_KEYS_LEN = len(_FLOW_RECORD_FIELD_KEYS)

_PLAIN_SCHEMES = {"ferric"}
_TLS_SCHEMES = {"ferrics"}
_SUPPORTED_SCHEMES = _PLAIN_SCHEMES | _TLS_SCHEMES

_OPCODES = {
    "PING": 0x0003,
    "CLIENT.SETNAME": 0x0004,
    "CLIENT.INFO": 0x0005,
    "ROUTE": 0x0006,
    "SHARDS": 0x0007,
    "BACKPRESSURE": 0x0008,
    "QUIT": 0x0009,
    "OPTIONS": 0x000B,
    "PIPELINE": _OP_PIPELINE,
    "SUBSCRIBE_EVENTS": _OP_SUBSCRIBE_EVENTS,
    "UNSUBSCRIBE_EVENTS": _OP_UNSUBSCRIBE_EVENTS,
    "GET": _OP_GET,
    "SET": _OP_SET,
    "DEL": 0x0103,
    "MGET": _OP_MGET,
    "MSET": _OP_MSET,
    "CAS": 0x0106,
    "LOCK": 0x0107,
    "UNLOCK": 0x0108,
    "EXTEND": 0x0109,
    "RATELIMIT.ADD": 0x010A,
    "FETCH_OR_COMPUTE": 0x010B,
    "FETCH_OR_COMPUTE_RESULT": 0x010C,
    "FETCH_OR_COMPUTE_ERROR": 0x010D,
    "HSET": 0x0110,
    "HGET": 0x0111,
    "HMGET": 0x0112,
    "HGETALL": 0x0113,
    "LPUSH": 0x0120,
    "RPUSH": 0x0121,
    "LPOP": 0x0122,
    "RPOP": 0x0123,
    "LRANGE": 0x0124,
    "SADD": 0x0130,
    "SREM": 0x0131,
    "SMEMBERS": 0x0132,
    "SISMEMBER": 0x0133,
    "ZADD": 0x0140,
    "ZREM": 0x0141,
    "ZRANGE": 0x0142,
    "ZSCORE": 0x0143,
    "CLUSTER.HEALTH": 0x0301,
    "CLUSTER.STATS": 0x0302,
    "CLUSTER.KEYSLOT": 0x0303,
    "CLUSTER.SLOTS": 0x0304,
    "CLUSTER.STATUS": 0x0305,
    "CLUSTER.JOIN": 0x0306,
    "CLUSTER.LEAVE": 0x0307,
    "CLUSTER.FAILOVER": 0x0308,
    "CLUSTER.PROMOTE": 0x0309,
    "CLUSTER.DEMOTE": 0x030A,
    "CLUSTER.ROLE": 0x030B,
    "FERRICSTORE.KEY_INFO": 0x030C,
    "FERRICSTORE.CONFIG": 0x030D,
    "FERRICSTORE.HOTNESS": 0x030E,
    "FERRICSTORE.METRICS": 0x030F,
    "FERRICSTORE.BLOBGC": 0x0310,
    "FLOW.CREATE": 0x0201,
    "FLOW.GET": 0x0202,
    "FLOW.CLAIM_DUE": 0x0203,
    "FLOW.COMPLETE": 0x0204,
    "FLOW.TRANSITION": 0x0205,
    "FLOW.RETRY": 0x0206,
    "FLOW.FAIL": 0x0207,
    "FLOW.CANCEL": 0x0208,
    "FLOW.EXTEND_LEASE": 0x0209,
    "FLOW.HISTORY": 0x020A,
    "FLOW.VALUE.PUT": 0x020B,
    "FLOW.VALUE.MGET": 0x020C,
    "FLOW.SIGNAL": 0x020D,
    "FLOW.LIST": 0x020E,
    "FLOW.CREATE_MANY": 0x020F,
    "FLOW.COMPLETE_MANY": 0x0210,
    "FLOW.TRANSITION_MANY": 0x0211,
    "FLOW.RETRY_MANY": 0x0212,
    "FLOW.FAIL_MANY": 0x0213,
    "FLOW.CANCEL_MANY": 0x0214,
    "FLOW.RECLAIM": 0x0215,
    "FLOW.REWIND": 0x0216,
    "FLOW.TERMINALS": 0x0217,
    "FLOW.FAILURES": 0x0218,
    "FLOW.BY_PARENT": 0x0219,
    "FLOW.BY_ROOT": 0x021A,
    "FLOW.BY_CORRELATION": 0x021B,
    "FLOW.INFO": 0x021C,
    "FLOW.STUCK": 0x021D,
    "FLOW.POLICY.SET": 0x021E,
    "FLOW.POLICY.GET": 0x021F,
    "FLOW.SPAWN_CHILDREN": 0x0220,
    "FLOW.RETENTION_CLEANUP": 0x0221,
    "FLOW.STEP_CONTINUE": 0x0222,
    "FLOW.START_AND_CLAIM": 0x0223,
    "FLOW.RUN_STEPS_MANY": _OP_FLOW_RUN_STEPS_MANY,
}

_CONTROL_OPCODES = set(range(0x0001, 0x0013))

_FIELD_NAMES = {
    "AFTER_MS": "after_ms",
    "BACKOFF": "backoff",
    "BASE_MS": "base_ms",
    "BLOCK": "block_ms",
    "CONSISTENT_PROJECTION": "consistent_projection",
    "CORRELATION_ID": "correlation_id",
    "COUNT": "count",
    "DELAY_MS": "delay_ms",
    "ERROR": "error",
    "ERROR_REF": "error_ref",
    "EVENT": "event",
    "EXHAUSTED_TO": "exhausted_to",
    "EXPECT_STATE": "expect_state",
    "FENCING": "fencing_token",
    "FENCING_TOKEN": "fencing_token",
    "FAILURE": "failure",
    "FROM_EVENT": "from_event",
    "FROM_MS": "from_ms",
    "FROM_STATE": "from_state",
    "FROM_VERSION": "from_version",
    "FULL": "full",
    "GROUP": "group_id",
    "GROUP_ID": "group_id",
    "HISTORY_HOT_MAX_EVENTS": "history_hot_max_events",
    "HISTORY_MAX_EVENTS": "history_max_events",
    "IDEMPOTENT": "idempotent",
    "IDEMPOTENCY": "idempotency_key",
    "IDEMPOTENCY_KEY": "idempotency_key",
    "INCLUDE_COLD": "include_cold",
    "INDEPENDENT": "independent",
    "IF_STATE": "if_state",
    "INITIAL_STATE": "initial_state",
    "ITEMS": "items",
    "JITTER_PCT": "jitter_pct",
    "LEASE_MS": "lease_ms",
    "LEASE_TOKEN": "lease_token",
    "LIMIT": "limit",
    "LOCAL_CACHE": "local_cache",
    "MAX_ATTEMPTS": "max_attempts",
    "MAX_BYTES": "max_bytes",
    "MAX_MS": "max_ms",
    "MAX_RETRIES": "max_retries",
    "NAME": "name",
    "NOW": "now_ms",
    "OLDER_THAN": "older_than_ms",
    "ON_CHILD_FAILED": "on_child_failed",
    "ON_PARENT_CLOSED": "on_parent_closed",
    "OVERRIDE": "override",
    "OWNER_FLOW_ID": "owner_flow_id",
    "PARENT_FLOW_ID": "parent_id",
    "PARENT_ID": "parent_id",
    "PARTITION": "partition_key",
    "PAYLOAD": "payload",
    "PAYLOAD_MAX_BYTES": "payload_max_bytes",
    "PRIORITY": "priority",
    "REASON": "reason",
    "REASON_REF": "reason_ref",
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
    "STATE": "state",
    "STATES": "states",
    "STEPS": "steps",
    "SUCCESS": "success",
    "TERMINAL_ONLY": "terminal_only",
    "TERMINAL_LOCAL_ONLY": "terminal_local_only",
    "TO_EVENT": "to_event",
    "TO_MS": "to_ms",
    "TO_STATE": "to_state",
    "TO_VERSION": "to_version",
    "TRANSITION_TO": "transition_to",
    "TTL": "ttl_ms",
    "TTL_MS": "ttl_ms",
    "TYPE": "type",
    "VALUE_MAX_BYTES": "value_max_bytes",
    "VALUES": "values",
    "WAIT": "wait",
    "WAIT_STATE": "wait_state",
    "WORKER": "worker",
}

_BOOL_FIELDS = {
    "consistent_projection",
    "full",
    "idempotent",
    "include_cold",
    "independent",
    "local_cache",
    "override",
    "reclaim_expired",
    "rev",
    "terminal_only",
    "terminal_local_only",
    "values",
}


@dataclass(frozen=True, slots=True)
class ProtocolCommand:
    opcode: int
    payload: dict[str, Any] | bytes
    lane_id: int = 1
    flags: int = 0


@dataclass(frozen=True, slots=True)
class ProtocolResponse:
    lane_id: int
    opcode: int
    request_id: int
    flags: int
    status: int
    value: Any
    trace: dict[str, Any] | None = None


def _pipeline_frame_supported(commands: list[ProtocolCommand]) -> bool:
    return all(
        command.opcode not in _CONTROL_OPCODES
        and command.flags == 0
        and isinstance(command.payload, dict)
        for command in commands
    )


def _compact_pipeline_payload(
    commands: list[ProtocolCommand], *, values_only: bool = False
) -> bytes | None:
    if not commands:
        return None

    opcode = commands[0].opcode
    if opcode == _OPCODES["FLOW.GET"]:
        return _compact_flow_get_pipeline_payload(commands, values_only=values_only)

    if opcode == _OPCODES["SET"]:
        mode = 1
    elif opcode == _OPCODES["GET"]:
        mode = 2
    elif opcode == _OPCODES["HGET"]:
        mode = _COMPACT_HGET_PIPELINE_MODE
    elif opcode == _OPCODES["HMGET"]:
        mode = _COMPACT_HMGET_PIPELINE_MODE
    elif opcode == _OPCODES["HGETALL"]:
        mode = _COMPACT_HGETALL_PIPELINE_MODE
    elif opcode == _OPCODES["SMEMBERS"]:
        mode = _COMPACT_SMEMBERS_PIPELINE_MODE
    elif opcode == _OPCODES["SISMEMBER"]:
        mode = _COMPACT_SISMEMBER_PIPELINE_MODE
    elif opcode == _OPCODES["LRANGE"]:
        mode = _COMPACT_LRANGE_PIPELINE_MODE
    elif opcode == _OPCODES["ZRANGE"]:
        mode = _COMPACT_ZRANGE_PIPELINE_MODE
    elif opcode == _OPCODES["ZSCORE"]:
        mode = _COMPACT_ZSCORE_PIPELINE_MODE
    elif opcode == _OPCODES["HSET"]:
        mode = _COMPACT_HSET_PIPELINE_MODE
    elif opcode == _OPCODES["LPUSH"]:
        mode = _COMPACT_LPUSH_PIPELINE_MODE
    elif opcode == _OPCODES["RPUSH"]:
        mode = _COMPACT_RPUSH_PIPELINE_MODE
    elif opcode == _OPCODES["SADD"]:
        mode = _COMPACT_SADD_PIPELINE_MODE
    elif opcode == _OPCODES["ZADD"]:
        mode = _COMPACT_ZADD_PIPELINE_MODE
    else:
        return None
    if values_only:
        mode |= 0x80

    parts: list[bytes] = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, mode, len(commands))]

    for command in commands:
        if command.opcode != opcode or command.flags != 0 or not isinstance(command.payload, dict):
            return None
        key = _maybe_bytes(command.payload.get("key"))
        if key is None:
            return None
        if opcode == _OPCODES["SET"]:
            if set(command.payload) != {"key", "value"}:
                return None
            value = _maybe_bytes(command.payload.get("value"))
            if value is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(value))
        elif opcode == _OPCODES["GET"]:
            if set(command.payload) != {"key"}:
                return None
            parts.append(_compact_binary(key))
        elif opcode == _OPCODES["SMEMBERS"]:
            if set(command.payload) != {"key"}:
                return None
            parts.append(_compact_binary(key))
        elif opcode == _OPCODES["HGET"]:
            if set(command.payload) != {"key", "field"}:
                return None
            field = _maybe_bytes(command.payload.get("field"))
            if field is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(field))
        elif opcode == _OPCODES["HMGET"]:
            if set(command.payload) != {"key", "fields"}:
                return None
            fields = command.payload.get("fields")
            if not isinstance(fields, list) or not fields:
                return None
            encoded_fields = [_maybe_bytes(field) for field in fields]
            if any(field is None for field in encoded_fields):
                return None
            parts.append(_compact_binary(key))
            parts.append(_COMPACT_U32.pack(len(encoded_fields)))
            parts.extend(_compact_binary(field) for field in encoded_fields if field is not None)
        elif opcode == _OPCODES["HGETALL"]:
            if set(command.payload) != {"key"}:
                return None
            parts.append(_compact_binary(key))
        elif opcode == _OPCODES["SISMEMBER"]:
            if set(command.payload) != {"key", "member"}:
                return None
            member = _maybe_bytes(command.payload.get("member"))
            if member is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(member))
        elif opcode == _OPCODES["ZSCORE"]:
            if set(command.payload) != {"key", "member"}:
                return None
            member = _maybe_bytes(command.payload.get("member"))
            if member is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(member))
        elif opcode == _OPCODES["LRANGE"]:
            if set(command.payload) != {"key", "start", "stop"}:
                return None
            start = command.payload.get("start")
            stop = command.payload.get("stop")
            if not isinstance(start, int) or not isinstance(stop, int):
                return None
            parts.append(_compact_binary(key))
            parts.append(_COMPACT_I64.pack(start))
            parts.append(_COMPACT_I64.pack(stop))
        elif opcode == _OPCODES["ZRANGE"]:
            payload_keys = set(command.payload)
            if not payload_keys.issubset({"key", "start", "stop", "withscores"}):
                return None
            start = command.payload.get("start")
            stop = command.payload.get("stop")
            with_scores = bool(command.payload.get("withscores", False))
            if not isinstance(start, int) or not isinstance(stop, int):
                return None
            parts.append(_compact_binary(key))
            parts.append(_COMPACT_I64.pack(start))
            parts.append(_COMPACT_I64.pack(stop))
            parts.append(b"\x01" if with_scores else b"\x00")
        elif opcode == _OPCODES["HSET"]:
            if set(command.payload) != {"key", "fields"}:
                return None
            fields = command.payload.get("fields")
            if not isinstance(fields, dict) or len(fields) != 1:
                return None
            field_arg, value_arg = next(iter(fields.items()))
            field = _maybe_bytes(field_arg)
            value = _maybe_bytes(value_arg)
            if field is None or value is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(field))
            parts.append(_compact_binary(value))
        elif opcode in {_OPCODES["LPUSH"], _OPCODES["RPUSH"]}:
            if set(command.payload) != {"key", "values"}:
                return None
            values = command.payload.get("values")
            if not isinstance(values, list) or len(values) != 1:
                return None
            value = _maybe_bytes(values[0])
            if value is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(value))
        elif opcode == _OPCODES["SADD"]:
            if set(command.payload) != {"key", "members"}:
                return None
            members = command.payload.get("members")
            if not isinstance(members, list) or len(members) != 1:
                return None
            member = _maybe_bytes(members[0])
            if member is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(member))
        elif opcode == _OPCODES["ZADD"]:
            if set(command.payload) != {"key", "items"}:
                return None
            items = command.payload.get("items")
            if not isinstance(items, list) or len(items) != 1:
                return None
            item = items[0]
            if not isinstance(item, list) or len(item) != 2:
                return None
            score_arg, member_arg = item
            member = _maybe_bytes(member_arg)
            if member is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_COMPACT_F64.pack(float(score_arg)))
            parts.append(_compact_binary(member))
        else:
            return None

    return b"".join(parts)


def _compact_flow_get_pipeline_payload(
    commands: list[ProtocolCommand], *, values_only: bool = False
) -> bytes | None:
    items: list[tuple[bytes, bytes | None]] = []
    has_partition = False
    return_mode: str | None = None

    for command in commands:
        if (
            command.opcode != _OPCODES["FLOW.GET"]
            or command.flags != 0
            or not isinstance(command.payload, dict)
        ):
            return None

        if not set(command.payload).issubset({"id", "partition_key", "return"}):
            return None

        item_return = command.payload.get("return")
        if item_return is not None:
            normalized_return = _text(item_return).lower()
            if normalized_return != "meta":
                return None
            if return_mode is None:
                return_mode = normalized_return
            elif return_mode != normalized_return:
                return None

        flow_id = _maybe_bytes(command.payload.get("id"))
        if flow_id is None:
            return None

        partition_key = None
        if "partition_key" in command.payload:
            partition_key = _optional_bytes(command.payload.get("partition_key"))
            if partition_key is None and command.payload.get("partition_key") is not None:
                return None
            has_partition = has_partition or partition_key is not None

        items.append((flow_id, partition_key))

    mode = 17 if return_mode == "meta" else 16 if has_partition else 9
    if values_only:
        mode |= 0x80

    parts: list[bytes] = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, mode, len(items))]
    for flow_id, partition_key in items:
        parts.append(_compact_binary(flow_id))
        if has_partition or return_mode == "meta":
            parts.append(_compact_optional_binary(partition_key))

    return b"".join(parts)


def _compact_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]], *, values_only: bool = False
) -> bytes | None:
    if not commands:
        return None

    try:
        name = _command_name(commands[0][0])
    except Exception:
        return None

    if name == "FLOW.HISTORY":
        return _compact_flow_history_pipeline_payload_from_raw(commands, values_only=values_only)
    if name == "FLOW.SIGNAL":
        return _compact_flow_signal_pipeline_payload_from_raw(commands, values_only=values_only)
    if name == "FLOW.GET":
        return _compact_flow_get_pipeline_payload_from_raw(commands, values_only=values_only)

    if name == "SET":
        mode = 1
    elif name == "GET":
        mode = 2
    elif name == "HGET":
        mode = _COMPACT_HGET_PIPELINE_MODE
    elif name == "HMGET":
        mode = _COMPACT_HMGET_PIPELINE_MODE
    elif name == "HGETALL":
        mode = _COMPACT_HGETALL_PIPELINE_MODE
    elif name == "SMEMBERS":
        mode = _COMPACT_SMEMBERS_PIPELINE_MODE
    elif name == "SISMEMBER":
        mode = _COMPACT_SISMEMBER_PIPELINE_MODE
    elif name == "LRANGE":
        mode = _COMPACT_LRANGE_PIPELINE_MODE
    elif name == "ZRANGE":
        mode = _COMPACT_ZRANGE_PIPELINE_MODE
    elif name == "ZSCORE":
        mode = _COMPACT_ZSCORE_PIPELINE_MODE
    elif name == "HSET":
        mode = _COMPACT_HSET_PIPELINE_MODE
    elif name == "LPUSH":
        mode = _COMPACT_LPUSH_PIPELINE_MODE
    elif name == "RPUSH":
        mode = _COMPACT_RPUSH_PIPELINE_MODE
    elif name == "SADD":
        mode = _COMPACT_SADD_PIPELINE_MODE
    elif name == "ZADD":
        mode = _COMPACT_ZADD_PIPELINE_MODE
    else:
        return _compact_mixed_pipeline_payload_from_raw(commands)
    if values_only:
        mode |= 0x80

    if name == "SET":
        payload = _compact_pipeline_set_payload_from_raw(commands, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name == "GET":
        payload = _compact_pipeline_keys_payload_from_raw(commands, name, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name == "SMEMBERS":
        payload = _compact_pipeline_keys_payload_from_raw(commands, name, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name == "HMGET":
        payload = _compact_pipeline_hmget_payload_from_raw(commands, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name == "HGETALL":
        payload = _compact_pipeline_keys_payload_from_raw(commands, name, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name in {"HGET", "SISMEMBER", "ZSCORE"}:
        payload = _compact_pipeline_two_binary_payload_from_raw(commands, name, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name in {"LRANGE", "ZRANGE"}:
        payload = _compact_pipeline_range_payload_from_raw(commands, name, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name == "HSET":
        payload = _compact_pipeline_hset_payload_from_raw(commands, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name in {"LPUSH", "RPUSH", "SADD"}:
        payload = _compact_pipeline_two_binary_payload_from_raw(commands, name, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    if name == "ZADD":
        payload = _compact_pipeline_zadd_payload_from_raw(commands, mode)
        return (
            payload if payload is not None else _compact_mixed_pipeline_payload_from_raw(commands)
        )

    parts: list[bytes] = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, mode, len(commands))]

    for command in commands:
        if not command:
            return None
        try:
            command_name = _command_name(command[0])
        except Exception:
            return None
        if command_name != name:
            return _compact_mixed_pipeline_payload_from_raw(commands)

        if name == "SET":
            if len(command) != 3:
                return None
            key = _maybe_bytes(command[1])
            value = _maybe_bytes(command[2])
            if key is None or value is None:
                return None
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(value))
        else:
            if len(command) != 2:
                return None
            key = _maybe_bytes(command[1])
            if key is None:
                return None
            parts.append(_compact_binary(key))

    return b"".join(parts)


def _compact_flow_get_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]], *, values_only: bool = False
) -> bytes | None:
    items: list[tuple[bytes, bytes | None]] = []
    has_partition = False
    return_mode: str | None = None

    for command in commands:
        if len(command) < 2:
            return None

        try:
            command_name = _command_name(command[0])
        except Exception:
            return None
        if command_name != "FLOW.GET":
            return None

        flow_id = _maybe_bytes(command[1])
        if flow_id is None:
            return None

        partition_key = None
        option_index = 2
        while option_index < len(command):
            if option_index + 1 >= len(command):
                return None

            option = _command_token(command[option_index])
            value = command[option_index + 1]

            if option in {"PARTITION", "PARTITION_KEY"}:
                partition_key = _optional_bytes(value)
                if partition_key is None and value is not None:
                    return None
                has_partition = has_partition or partition_key is not None
            elif option == "RETURN":
                normalized_return = _text(value).lower()
                if normalized_return != "meta":
                    return None
                if return_mode is None:
                    return_mode = normalized_return
                elif return_mode != normalized_return:
                    return None
            else:
                return None

            option_index += 2

        items.append((flow_id, partition_key))

    mode = 17 if return_mode == "meta" else 16 if has_partition else 9
    if values_only:
        mode |= 0x80

    parts: list[bytes] = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, mode, len(items))]
    for flow_id, partition_key in items:
        parts.append(_compact_binary(flow_id))
        if has_partition or return_mode == "meta":
            parts.append(_compact_optional_binary(partition_key))

    return b"".join(parts)


def _compact_flow_history_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]], *, values_only: bool
) -> bytes | None:
    history_count: int | None = None
    include_cold: bool | None = None
    consistent_projection: bool | None = None
    items: list[tuple[bytes, bytes | None]] = []

    for command in commands:
        if len(command) < 2:
            return None

        try:
            command_name = _command_name(command[0])
        except Exception:
            return None
        if command_name != "FLOW.HISTORY":
            return None

        flow_id = _maybe_bytes(command[1])
        if flow_id is None:
            return None

        partition_key = None
        item_count = 100
        item_include_cold = False
        item_consistent_projection = True
        option_index = 2
        while option_index < len(command):
            if option_index + 1 >= len(command):
                return None

            option = _command_token(command[option_index])
            value = command[option_index + 1]

            if option == "COUNT":
                try:
                    item_count = int(value)
                except (TypeError, ValueError):
                    return None
            elif option in {"PARTITION", "PARTITION_KEY"}:
                partition_key = _optional_bytes(value)
                if partition_key is False:
                    return None
            elif option == "INCLUDE_COLD":
                item_include_cold = _coerce_bool(value)
            elif option == "CONSISTENT_PROJECTION":
                item_consistent_projection = _coerce_bool(value)
            else:
                return None

            option_index += 2

        if history_count is None:
            history_count = item_count
            include_cold = item_include_cold
            consistent_projection = item_consistent_projection
        elif (
            history_count != item_count
            or include_cold != item_include_cold
            or consistent_projection != item_consistent_projection
        ):
            return None

        items.append((flow_id, cast(bytes | None, partition_key)))

    if history_count is None or include_cold is None or consistent_projection is None:
        return None

    mode = 10 | (0x80 if values_only else 0)
    parts = [
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(items)),
        struct.pack(
            ">qBB",
            history_count,
            _compact_bool_marker(include_cold),
            _compact_bool_marker(consistent_projection),
        ),
    ]

    for flow_id, partition_key in items:
        parts.append(_compact_binary(flow_id))
        parts.append(_compact_optional_binary(partition_key))

    return b"".join(parts)


def _compact_flow_signal_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]], *, values_only: bool
) -> bytes | None:
    signal: bytes | None = None
    if_state: bytes | None = None
    transition_to: bytes | None = None
    items: list[tuple[bytes, bytes | None, int]] = []

    for command in commands:
        protocol_command = build_protocol_command(*command)
        if (
            protocol_command.opcode != _OPCODES["FLOW.SIGNAL"]
            or protocol_command.flags != 0
            or not isinstance(protocol_command.payload, dict)
        ):
            return None

        payload = protocol_command.payload
        if not set(payload).issubset(
            {"id", "signal", "partition_key", "if_state", "transition_to", "now_ms"}
        ):
            return None

        flow_id = _maybe_bytes(payload.get("id"))
        item_signal = _maybe_bytes(payload.get("signal"))
        item_if_state = _maybe_bytes(payload.get("if_state"))
        item_transition_to = _maybe_bytes(payload.get("transition_to"))
        partition_key = _optional_bytes(payload.get("partition_key"))
        now_ms = payload.get("now_ms")
        if (
            flow_id is None
            or item_signal is None
            or item_if_state is None
            or item_transition_to is None
            or partition_key is False
            or not isinstance(now_ms, int)
        ):
            return None

        if signal is None:
            signal = item_signal
            if_state = item_if_state
            transition_to = item_transition_to
        elif (
            signal != item_signal
            or if_state != item_if_state
            or transition_to != item_transition_to
        ):
            return None

        items.append((flow_id, cast(bytes | None, partition_key), now_ms))

    if signal is None or if_state is None or transition_to is None:
        return None

    mode = 11 | (0x80 if values_only else 0)
    parts = [
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(items)),
        _compact_binary(signal),
        _compact_binary(if_state),
        _compact_binary(transition_to),
    ]

    for flow_id, partition_key, now_ms in items:
        parts.append(_compact_binary(flow_id))
        parts.append(_compact_optional_binary(partition_key))
        parts.append(struct.pack(">q", now_ms))

    return b"".join(parts)


def _compact_mixed_pipeline_payload_from_raw(commands: list[tuple[Any, ...]]) -> bytes | None:
    parts: list[bytes] = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 5, len(commands))]
    read_keys: set[bytes] = set()
    written_keys: set[bytes] = set()

    for command in commands:
        if not command:
            return None
        try:
            name = _command_name(command[0])
        except Exception:
            return None

        if name == "GET":
            if len(command) != 2:
                return None
            key = _maybe_bytes(command[1])
            if key is None or key in written_keys:
                return None
            read_keys.add(key)
            parts.append(b"\x02")
            parts.append(_compact_binary(key))
        elif name == "SET":
            if len(command) != 3:
                return None
            key = _maybe_bytes(command[1])
            value = _maybe_bytes(command[2])
            if key is None or value is None or key in read_keys or key in written_keys:
                return None
            written_keys.add(key)
            parts.append(b"\x01")
            parts.append(_compact_binary(key))
            parts.append(_compact_binary(value))
        else:
            return None

    return b"".join(parts)


def _compact_kv_set_pairs_payload(args: tuple[Any, ...], mode: int = 1) -> bytes | None:
    if len(args) == 0 or len(args) % 2 != 0:
        return None
    parts = [_COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(args) // 2)]
    pack_u32 = _COMPACT_U32.pack
    append = parts.append
    for idx in range(0, len(args), 2):
        key_arg = args[idx]
        value_arg = args[idx + 1]
        if isinstance(key_arg, bytes):
            key = key_arg
        elif isinstance(key_arg, str):
            key = key_arg.encode()
        else:
            return None
        if isinstance(value_arg, bytes):
            value = value_arg
        elif isinstance(value_arg, str):
            value = value_arg.encode()
        else:
            return None
        append(pack_u32(len(key)))
        append(key)
        append(pack_u32(len(value)))
        append(value)
    return b"".join(parts)


def _compact_kv_keys_payload(args: Sequence[Any], mode: int) -> bytes | None:
    if not args:
        return None
    parts = [_COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(args))]
    pack_u32 = _COMPACT_U32.pack
    append = parts.append
    for arg in args:
        if isinstance(arg, bytes):
            key = arg
        elif isinstance(arg, str):
            key = arg.encode()
        else:
            return None
        append(pack_u32(len(key)))
        append(key)
    return b"".join(parts)


def _compact_kv_set_keys_value_payload(
    keys: Sequence[Any], value_arg: Any, mode: int = 1
) -> bytes | None:
    if not keys:
        return None
    if isinstance(value_arg, bytes):
        value = value_arg
    elif isinstance(value_arg, str):
        value = value_arg.encode()
    else:
        return None

    payload = bytearray(_COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(keys)))
    pack_u32 = _COMPACT_U32.pack
    extend = payload.extend
    value_len = pack_u32(len(value))
    for key_arg in keys:
        if isinstance(key_arg, bytes):
            key = key_arg
        elif isinstance(key_arg, str):
            key = key_arg.encode()
        else:
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(value_len)
        extend(value)
    return bytes(payload)


def _compact_pipeline_set_payload_from_raw(
    commands: list[tuple[Any, ...]], mode: int
) -> bytes | None:
    payload = bytearray(
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(commands))
    )
    pack_u32 = _COMPACT_U32.pack
    extend = payload.extend
    for command in commands:
        if len(command) != 3:
            return None
        raw_name = command[0]
        if raw_name != "SET" and _command_name(raw_name) != "SET":
            return None
        key_arg = command[1]
        if isinstance(key_arg, bytes):
            key = key_arg
        elif isinstance(key_arg, str):
            key = key_arg.encode()
        else:
            return None
        value_arg = command[2]
        if isinstance(value_arg, bytes):
            value = value_arg
        elif isinstance(value_arg, str):
            value = value_arg.encode()
        else:
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(pack_u32(len(value)))
        extend(value)
    return bytes(payload)


def _compact_pipeline_keys_payload_from_raw(
    commands: list[tuple[Any, ...]], name: str, mode: int
) -> bytes | None:
    payload = bytearray(
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(commands))
    )
    pack_u32 = _COMPACT_U32.pack
    extend = payload.extend
    for command in commands:
        if len(command) != 2:
            return None
        raw_name = command[0]
        if raw_name != name and _command_name(raw_name) != name:
            return None
        key_arg = command[1]
        if isinstance(key_arg, bytes):
            key = key_arg
        elif isinstance(key_arg, str):
            key = key_arg.encode()
        else:
            return None
        extend(pack_u32(len(key)))
        extend(key)
    return bytes(payload)


def _compact_pipeline_two_binary_payload_from_raw(
    commands: list[tuple[Any, ...]], name: str, mode: int
) -> bytes | None:
    payload = bytearray(
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(commands))
    )
    pack_u32 = _COMPACT_U32.pack
    extend = payload.extend
    for command in commands:
        if len(command) != 3:
            return None
        raw_name = command[0]
        if raw_name != name and _command_name(raw_name) != name:
            return None
        key = _maybe_bytes(command[1])
        item = _maybe_bytes(command[2])
        if key is None or item is None:
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(pack_u32(len(item)))
        extend(item)
    return bytes(payload)


def _compact_pipeline_range_payload_from_raw(
    commands: list[tuple[Any, ...]], name: str, mode: int
) -> bytes | None:
    payload = bytearray(
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(commands))
    )
    pack_u32 = _COMPACT_U32.pack
    pack_i64 = _COMPACT_I64.pack
    extend = payload.extend
    for command in commands:
        if not command:
            return None
        try:
            command_name = _command_name(command[0])
        except Exception:
            return None
        if command_name != name:
            return None
        if name == "LRANGE":
            if len(command) != 4:
                return None
            with_scores = False
        else:
            if len(command) not in {4, 5}:
                return None
            with_scores = False
            if len(command) == 5:
                option = _command_name(command[4])
                if option != "WITHSCORES":
                    return None
                with_scores = True
        key = _maybe_bytes(command[1])
        if key is None:
            return None
        try:
            start = int(command[2])
            stop = int(command[3])
        except (TypeError, ValueError):
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(pack_i64(start))
        extend(pack_i64(stop))
        if name == "ZRANGE":
            extend(b"\x01" if with_scores else b"\x00")
    return bytes(payload)


def _compact_pipeline_hset_payload_from_raw(
    commands: list[tuple[Any, ...]], mode: int
) -> bytes | None:
    payload = bytearray(
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(commands))
    )
    pack_u32 = _COMPACT_U32.pack
    extend = payload.extend
    for command in commands:
        if len(command) != 4:
            return None
        raw_name = command[0]
        if raw_name != "HSET" and _command_name(raw_name) != "HSET":
            return None
        key = _maybe_bytes(command[1])
        field = _maybe_bytes(command[2])
        value = _maybe_bytes(command[3])
        if key is None or field is None or value is None:
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(pack_u32(len(field)))
        extend(field)
        extend(pack_u32(len(value)))
        extend(value)
    return bytes(payload)


def _compact_pipeline_hmget_payload_from_raw(
    commands: list[tuple[Any, ...]], mode: int
) -> bytes | None:
    payload = bytearray(
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(commands))
    )
    pack_u32 = _COMPACT_U32.pack
    extend = payload.extend
    for command in commands:
        if len(command) < 3:
            return None
        raw_name = command[0]
        if raw_name != "HMGET" and _command_name(raw_name) != "HMGET":
            return None
        key = _maybe_bytes(command[1])
        if key is None:
            return None
        fields = [_maybe_bytes(field) for field in command[2:]]
        if any(field is None for field in fields):
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(pack_u32(len(fields)))
        for field in fields:
            if field is None:
                return None
            extend(pack_u32(len(field)))
            extend(field)
    return bytes(payload)


def _compact_pipeline_zadd_payload_from_raw(
    commands: list[tuple[Any, ...]], mode: int
) -> bytes | None:
    payload = bytearray(
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(commands))
    )
    pack_u32 = _COMPACT_U32.pack
    pack_f64 = _COMPACT_F64.pack
    extend = payload.extend
    for command in commands:
        if len(command) != 4:
            return None
        raw_name = command[0]
        if raw_name != "ZADD" and _command_name(raw_name) != "ZADD":
            return None
        key = _maybe_bytes(command[1])
        member = _maybe_bytes(command[3])
        if key is None or member is None:
            return None
        try:
            score = float(command[2])
        except (TypeError, ValueError):
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(pack_f64(score))
        extend(pack_u32(len(member)))
        extend(member)
    return bytes(payload)


class ProtocolAdapter:
    """FerricStore protocol TCP adapter for the sync SDK.

    The adapter accepts the same `execute_command(*args)` shape as the Redis
    adapter. It encodes supported Redis/FerricFlow commands into protocol
    typed frames so high-level Queue/Workflow code can switch transport by URL.
    """

    client: ProtocolAdapter

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6388,
        *,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        timeout: float | None = 30.0,
        client_name: str | None = "ferricstore-python",
        compression: str = "none",
        lanes: int = 16,
        ssl_context: ssl.SSLContext | None = None,
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float | None = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username or "default"
        self.password = password
        self.tls = tls
        self.timeout = timeout
        self.client_name = client_name
        self.compression = compression
        self.lanes = max(1, lanes)
        self.ssl_context = ssl_context
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.client = self
        self._lock = threading.Lock()
        self._request_id = 0
        self._lane_cursor = 0
        self._sock: socket.socket | ssl.SSLSocket | None = None
        self._reader_thread: threading.Thread | None = None
        self._pending: dict[int, Future[ProtocolResponse]] = {}
        self._pending_traces: dict[int, dict[str, Any]] = {}
        self._events: list[Any] = []
        self._events_cv = threading.Condition()
        self._heartbeat_thread: threading.Thread | None = None
        self._last_activity = time.monotonic()
        self._connect()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> ProtocolAdapter:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        tls = scheme in _TLS_SCHEMES
        if scheme not in _SUPPORTED_SCHEMES:
            raise ValueError(f"unsupported FerricStore URL scheme: {parsed.scheme}")

        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (6389 if tls else 6388)
        username = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None
        _normalize_protocol_url_kwargs(kwargs)
        kwargs.setdefault("username", username)
        kwargs.setdefault("password", password)
        kwargs.setdefault("tls", tls)
        return cls(host, port, **kwargs)

    @property
    def events(self) -> list[Any]:
        with self._events_cv:
            return list(self._events)

    def wait_event(self, timeout: float | None = None) -> Any | None:
        with self._events_cv:
            if not self._events:
                self._events_cv.wait(timeout)
            if not self._events:
                return None
            return self._events.pop(0)

    def close(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        self._fail_pending(FerricStoreError("protocol connection is closed"))

    def pipeline(self, transaction: bool = False) -> ProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support Redis transactions")
        return ProtocolPipeline(self)

    def execute_command(self, *args: Any) -> Any:
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            response = self._request(opcode, 1, payload, flags)
            return self._response_value(response)

        command = build_protocol_command(*args)
        response = self._request(command.opcode, command.lane_id, command.payload, command.flags)
        return self._response_value(response)

    def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        command = build_protocol_command(*args)
        response = self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    def submit_command(self, *args: Any) -> Future[Any]:
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            _request_id, response_future = self._submit_request(opcode, 1, payload, flags)
            return self._value_future(response_future)

        command = build_protocol_command(*args)
        _request_id, response_future = self._submit_request(
            command.opcode, command.lane_id, command.payload, command.flags
        )
        return self._value_future(response_future)

    def submit_mget(self, keys: Sequence[Any]) -> Future[Any]:
        payload = _compact_kv_keys_payload(keys, 2)
        if payload is None:
            raise InvalidCommandError("MGET requires one or more string/binary keys")
        return self.submit_mget_payload(payload)

    def submit_mget_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MGET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request(
            _OP_MGET, 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_flow_value_mget_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "FLOW.VALUE.MGET payload must be a non-empty compact binary payload"
            )
        _request_id, response_future = self._submit_request(
            _OP_FLOW_VALUE_MGET, 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_mset_same_value(self, keys: Sequence[Any], value: Any) -> Future[Any]:
        payload = _compact_kv_set_keys_value_payload(keys, value)
        if payload is None:
            raise InvalidCommandError(
                "MSET requires one or more string/binary keys and a string/binary value"
            )
        return self.submit_mset_payload(payload)

    def submit_mset_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MSET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request(
            _OPCODES["MSET"], 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_pipeline_payload(self, payload: bytes, count: int) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("PIPELINE payload must be a non-empty compact binary payload")
        if count < 0:
            raise InvalidCommandError("PIPELINE payload count must be non-negative")

        future: Future[list[Any]] = Future()
        response_future = self._submit_pipeline_payload(payload, count)
        self._complete_batch_future(response_future, count, future)
        return future

    def submit_flow_many_payload(self, command: str, payload: bytes, count: int) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("Flow many payload must be a non-empty compact binary payload")
        if count < 0:
            raise InvalidCommandError("Flow many payload count must be non-negative")

        name = _command_name(command)
        if name not in {
            "FLOW.CREATE_MANY",
            "FLOW.COMPLETE_MANY",
            "FLOW.TRANSITION_MANY",
            "FLOW.RETRY_MANY",
            "FLOW.FAIL_MANY",
            "FLOW.CANCEL_MANY",
        }:
            raise InvalidCommandError(f"{name} does not support direct Flow many payload submit")

        future: Future[list[Any]] = Future()
        self._submit_flow_many_batch([(_OPCODES[name], payload, count)], count, future)
        return future

    def _fast_bulk_kv_request(self, args: tuple[Any, ...]) -> tuple[int, bytes, int] | None:
        if not args:
            return None

        try:
            name = _command_name(args[0])
        except Exception:
            return None

        command_args = args[1:]
        if name == "MGET":
            payload = _compact_kv_keys_payload(command_args, 2)
            return (
                (_OPCODES["MGET"], payload, _FLAG_CUSTOM_PAYLOAD) if payload is not None else None
            )
        if name == "MSET":
            payload = _compact_kv_set_pairs_payload(command_args)
            return (
                (_OPCODES["MSET"], payload, _FLAG_CUSTOM_PAYLOAD) if payload is not None else None
            )
        return None

    def submit_commands(self, commands: list[tuple[Any, ...]]) -> list[Future[Any]]:
        if not commands:
            return []

        protocol_commands = [build_protocol_command(*command) for command in commands]
        if _pipeline_frame_supported(protocol_commands):
            return self._submit_pipeline(protocol_commands)

        pending: list[tuple[int, Future[ProtocolResponse]]] = []
        frames: list[bytes] = []

        with self._lock:
            try:
                for command in protocol_commands:
                    request_id = self._next_request_id()
                    lane_id = self._next_lane_id(command.lane_id)
                    response_future: Future[ProtocolResponse] = Future()
                    self._pending[request_id] = response_future
                    pending.append((request_id, response_future))

                    body = (
                        command.payload
                        if isinstance(command.payload, bytes)
                        else encode_value(command.payload)
                    )
                    flags = command.flags
                    if self.compression == "zlib" and body:
                        body = zlib.compress(body)
                        flags |= _FLAG_COMPRESSED
                    frames.append(
                        _HEADER.pack(
                            _MAGIC,
                            _REQUEST_VERSION,
                            flags,
                            lane_id,
                            command.opcode,
                            request_id,
                            len(body),
                        )
                        + body
                    )

                self._require_socket().sendall(b"".join(frames))
                self._last_activity = time.monotonic()
            except BaseException as exc:
                for request_id, response_future in pending:
                    self._pending.pop(request_id, None)
                    self._pending_traces.pop(request_id, None)
                    if not response_future.cancelled():
                        response_future.set_exception(exc)
                raise

        return [self._value_future(response_future) for _request_id, response_future in pending]

    def submit_batch(self, commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
        future: Future[list[Any]] = Future()
        if not commands:
            future.set_result([])
            return future

        compact_payload = _compact_pipeline_payload_from_raw(commands, values_only=True)
        if compact_payload is not None:
            response_future = self._submit_pipeline_payload(compact_payload, len(commands))
            self._complete_batch_future(response_future, len(commands), future)
            return future

        flow_many_payloads = _compact_flow_many_payloads_from_raw(commands)
        if flow_many_payloads is not None:
            self._submit_flow_many_batch(flow_many_payloads, len(commands), future)
            return future

        protocol_commands = [build_protocol_command(*command) for command in commands]
        if not _pipeline_frame_supported(protocol_commands):
            item_futures = self.submit_commands(commands)
            lock = threading.Lock()
            results: list[Any] = [None] * len(item_futures)
            remaining = len(item_futures)

            def complete_items(index: int, item_future: Future[Any]) -> None:
                nonlocal remaining
                try:
                    value = item_future.result()
                except Exception as exc:
                    if not future.done():
                        future.set_exception(exc)
                    return

                with lock:
                    if future.done():
                        return
                    results[index] = value
                    remaining -= 1
                    if remaining == 0:
                        future.set_result(results)

            for index, item in enumerate(item_futures):
                item.add_done_callback(lambda source, index=index: complete_items(index, source))
            return future

        response_future = self._submit_pipeline_request(protocol_commands, values_only=True)
        self._complete_batch_future(response_future, len(protocol_commands), future)
        return future

    def _submit_flow_many_batch(
        self,
        payloads: list[tuple[int, bytes, int]],
        expected_count: int,
        future: Future[list[Any]],
    ) -> None:
        pending: list[tuple[Future[ProtocolResponse], int]] = []
        try:
            for opcode, payload, count in payloads:
                _request_id, response_future = self._submit_request(
                    opcode, 1, payload, _FLAG_CUSTOM_PAYLOAD
                )
                pending.append((response_future, count))
        except Exception as exc:
            future.set_exception(exc)
            return

        results: list[list[Any] | None] = [None] * len(pending)
        remaining = len(pending)
        lock = threading.Lock()

        def complete(index: int, response_future: Future[ProtocolResponse], count: int) -> None:
            nonlocal remaining
            try:
                value = self._response_value(response_future.result())
                if _ok_scalar(value):
                    group_values = [value] * count
                elif isinstance(value, list) and len(value) == count:
                    if _pipeline_pair_list(value):
                        group_values = [self._batch_item_value(item) for item in value]
                    else:
                        group_values = value
                else:
                    raise FerricStoreError("protocol Flow many returned invalid result", raw=value)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)
                return

            with lock:
                if future.done():
                    return
                results[index] = group_values
                remaining -= 1
                if remaining == 0:
                    merged = [item for group in results if group is not None for item in group]
                    if len(merged) != expected_count:
                        future.set_exception(
                            FerricStoreError(
                                "protocol Flow many returned invalid result", raw=merged
                            )
                        )
                    else:
                        future.set_result(merged)

        for index, (response_future, count) in enumerate(pending):
            response_future.add_done_callback(
                lambda source, index=index, count=count: complete(index, source, count)
            )

    def _complete_batch_future(
        self,
        response_future: Future[ProtocolResponse],
        expected_count: int,
        future: Future[list[Any]],
        *,
        allow_scalar_ok: bool = False,
    ) -> None:

        def complete(source_future: Future[ProtocolResponse]) -> None:
            try:
                value = self._response_value(source_future.result())
                if allow_scalar_ok and _ok_scalar(value):
                    future.set_result([value] * expected_count)
                    return
                if not isinstance(value, list) or len(value) != expected_count:
                    raise FerricStoreError("protocol PIPELINE returned invalid result", raw=value)
                if _pipeline_pair_list(value):
                    future.set_result([self._batch_item_value(item) for item in value])
                else:
                    future.set_result(value)
            except Exception as exc:
                future.set_exception(exc)

        response_future.add_done_callback(complete)

    def _submit_pipeline_payload(
        self, payload: bytes, _expected_count: int
    ) -> Future[ProtocolResponse]:
        _request_id, response_future = self._submit_request(
            _OP_PIPELINE,
            1,
            payload,
            _FLAG_CUSTOM_PAYLOAD,
        )
        return response_future

    def _submit_pipeline(self, commands: list[ProtocolCommand]) -> list[Future[Any]]:
        response_future = self._submit_pipeline_request(commands)
        futures: list[Future[Any]] = [Future() for _ in commands]

        def complete(source_future: Future[ProtocolResponse]) -> None:
            try:
                value = self._response_value(source_future.result())
                if not isinstance(value, list) or len(value) != len(futures):
                    raise FerricStoreError("protocol PIPELINE returned invalid result", raw=value)

                for future, item in zip(futures, value, strict=True):
                    if not future.cancelled():
                        future.set_result(self._batch_item_value(item))
            except Exception as exc:
                for future in futures:
                    if not future.cancelled():
                        future.set_exception(exc)

        response_future.add_done_callback(complete)
        return futures

    def _submit_pipeline_request(
        self, commands: list[ProtocolCommand], *, values_only: bool = False
    ) -> Future[ProtocolResponse]:
        compact_payload = _compact_pipeline_payload(commands, values_only=values_only)
        flags = _FLAG_CUSTOM_PAYLOAD if compact_payload is not None else 0
        payload: dict[str, Any] | bytes

        if compact_payload is not None:
            payload = compact_payload
        else:
            pipeline_commands = [
                {
                    "opcode": command.opcode,
                    "lane_id": command.lane_id,
                    "request_id": idx + 1,
                    "body": command.payload,
                }
                for idx, command in enumerate(commands)
            ]
            payload = {"atomicity": "none", "commands": pipeline_commands, "return": "compact"}

        _request_id, response_future = self._submit_request(
            _OP_PIPELINE,
            1,
            payload,
            flags,
        )
        return response_future

    def _value_future(self, response_future: Future[ProtocolResponse]) -> Future[Any]:
        value_future: Future[Any] = Future()

        def complete(source: Future[ProtocolResponse]) -> None:
            if value_future.cancelled():
                return
            try:
                value_future.set_result(self._response_value(source.result()))
            except Exception as exc:
                if not value_future.cancelled():
                    value_future.set_exception(exc)

        response_future.add_done_callback(complete)
        return value_future

    def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | None = None,
        partition_keys: list[str] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")

        flow_wake: dict[str, Any] = {"type": type}
        if states is not None:
            if not states:
                raise ValueError("states must be non-empty")
            flow_wake["states"] = list(states)
        elif state is not None:
            flow_wake["state"] = state
        if partition_keys is not None:
            if not partition_keys:
                raise ValueError("partition_keys must be non-empty")
            flow_wake["partition_keys"] = list(partition_keys)
        elif partition_key is not None:
            flow_wake["partition_key"] = partition_key
        if priority is not None:
            flow_wake["priority"] = priority
        if limit is not None:
            flow_wake["limit"] = limit

        response = self._request(
            _OP_SUBSCRIBE_EVENTS,
            0,
            {"events": ["FLOW_WAKE"], "flow_wake": flow_wake},
        )
        return self._response_value(response)

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        if not commands:
            return []

        flow_many_payloads = _compact_flow_many_payloads_from_raw(commands)
        if flow_many_payloads is not None:
            values: list[Any] = []
            for opcode, payload, count in flow_many_payloads:
                response = self._request(opcode, 1, payload, _FLAG_CUSTOM_PAYLOAD)
                group_values = self._response_value(response)
                if _ok_scalar(group_values):
                    values.extend([group_values] * count)
                    continue
                if not isinstance(group_values, list) or len(group_values) != count:
                    raise FerricStoreError(
                        "protocol Flow many returned non-list response", raw=group_values
                    )
                values.extend(group_values)
            return values

        compact_payload = _compact_pipeline_payload_from_raw(commands, values_only=True)
        if compact_payload is not None:
            response = self._request(_OP_PIPELINE, 1, compact_payload, _FLAG_CUSTOM_PAYLOAD)
            values = self._response_value(response)
            if not isinstance(values, list) or len(values) != len(commands):
                raise FerricStoreError("protocol PIPELINE returned non-list response", raw=values)
            if _pipeline_pair_list(values):
                return [self._batch_item_value(item) for item in values]
            return values

        protocol_commands = [build_protocol_command(*command) for command in commands]
        if any(
            command.opcode in _CONTROL_OPCODES or command.flags for command in protocol_commands
        ):
            return [self.execute_command(*command) for command in commands]

        batch_commands = [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id,
                "request_id": idx + 1,
                "body": command.payload,
            }
            for idx, command in enumerate(protocol_commands)
        ]
        response = self._request(
            _OP_PIPELINE,
            1,
            {"atomicity": "none", "commands": batch_commands, "return": "compact"},
        )

        values = self._response_value(response)
        if not isinstance(values, list):
            raise FerricStoreError("protocol PIPELINE returned non-list response", raw=values)

        return [self._batch_item_value(item) for item in values]

    def _connect(self) -> None:
        last_error: BaseException | None = None

        for attempt in range(3):
            try:
                self._connect_once()
                break
            except (ConnectionError, OSError, TimeoutError, FerricStoreError) as exc:
                if not self._startup_retryable(exc):
                    raise
                last_error = exc
                self.close()
                if attempt == 2:
                    raise
                time.sleep(0.02 * (attempt + 1))
        else:
            if last_error is not None:
                raise last_error

        if self.password is not None:
            self._response_value(
                self._request(
                    _OP_AUTH,
                    0,
                    {"username": self.username, "password": self.password},
                )
            )
        self._start_heartbeat()

    @staticmethod
    def _startup_retryable(exc: BaseException) -> bool:
        if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
            return True
        if isinstance(exc, FerricStoreError):
            message = str(exc).lower()
            return "timed out" in message or "closed" in message or "reset" in message
        return False

    def _connect_once(self) -> None:
        raw_sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        raw_sock.settimeout(None)
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self.tls:
            context = self.ssl_context or ssl.create_default_context()
            self._sock = context.wrap_socket(raw_sock, server_hostname=self.host)
            self._sock.settimeout(None)
        else:
            self._sock = raw_sock
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

        startup: dict[str, Any] = {
            "compression": self.compression,
            "compact_flow_responses": True,
        }
        if self.client_name is not None:
            startup["client_name"] = self.client_name
            startup["driver_name"] = self.client_name
        self._response_value(self._request(_OP_STARTUP, 0, startup))

    def _start_heartbeat(self) -> None:
        if self.heartbeat_interval is None or self.heartbeat_interval <= 0:
            return

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="ferricstore-protocol-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        interval = float(self.heartbeat_interval or 0)
        timeout = self.heartbeat_timeout
        while interval > 0 and self._sock is not None:
            time.sleep(interval)
            if self._sock is None:
                return
            if time.monotonic() - self._last_activity < interval:
                continue

            request_id: int | None = None
            try:
                request_id, future = self._submit_request(_OPCODES["PING"], 0, {})
                if timeout is None:
                    future.result()
                else:
                    future.result(timeout=timeout)
            except Exception as exc:
                if request_id is not None:
                    self._pending.pop(request_id, None)
                    self._pending_traces.pop(request_id, None)
                self._fail_pending(FerricStoreError("protocol heartbeat failed", raw=exc))
                self.close()
                return

    def _next_request_id(self) -> int:
        self._request_id = (self._request_id + 1) & 0xFFFFFFFFFFFFFFFF
        if self._request_id == 0:
            self._request_id = 1
        return self._request_id

    def _next_lane_id(self, lane_id: int) -> int:
        if lane_id == 0 or self.lanes == 1:
            return lane_id
        self._lane_cursor = (self._lane_cursor % self.lanes) + 1
        return self._lane_cursor

    def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> dict[str, Any] | None:
        sock = self._require_socket()
        trace_enabled = bool(extra_flags & _FLAG_TRACE)
        encode_started_ns = time.perf_counter_ns() if trace_enabled else 0
        body = payload if isinstance(payload, bytes) else encode_value(payload)
        flags = extra_flags
        if self.compression == "zlib" and body:
            body = zlib.compress(body)
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        encode_done_ns = time.perf_counter_ns() if trace_enabled else 0
        write_started_ns = encode_done_ns
        _send_frame(sock, header, body)
        self._last_activity = time.monotonic()
        if not trace_enabled:
            return None
        write_done_ns = time.perf_counter_ns()
        return {
            "encode_us": (encode_done_ns - encode_started_ns) // 1000,
            "socket_write_us": (write_done_ns - write_started_ns) // 1000,
        }

    def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
    ) -> ProtocolResponse:
        request_id, future = self._submit_request(opcode, lane_id, payload, flags)

        try:
            if self.timeout is None:
                return future.result()
            return future.result(timeout=self.timeout)
        except FutureTimeoutError as exc:
            self._pending.pop(request_id, None)
            self._pending_traces.pop(request_id, None)
            raise FerricStoreError("protocol request timed out") from exc

    def _submit_request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
    ) -> tuple[int, Future[ProtocolResponse]]:
        with self._lock:
            request_id = self._next_request_id()
            lane_id = self._next_lane_id(lane_id)
            future: Future[ProtocolResponse] = Future()
            self._pending[request_id] = future
            trace_enabled = bool(flags & _FLAG_TRACE)
            if trace_enabled:
                self._pending_traces[request_id] = {}
            try:
                trace = self._send(opcode, lane_id, request_id, payload, flags)
                if trace_enabled and trace is not None:
                    self._pending_traces[request_id].update(trace)
            except Exception:
                self._pending.pop(request_id, None)
                self._pending_traces.pop(request_id, None)
                raise
            return request_id, future

    def _reader_loop(self) -> None:
        try:
            while self._sock is not None:
                response = self._recv_response()
                self._last_activity = time.monotonic()
                if response.request_id == 0:
                    with self._events_cv:
                        self._events.append(response.value)
                        self._events_cv.notify_all()
                    continue
                future = self._pending.pop(response.request_id, None)
                if future is not None:
                    response = self._attach_client_trace(
                        response,
                        self._pending_traces.pop(response.request_id, None),
                    )
                    future.set_result(response)
                else:
                    self._pending_traces.pop(response.request_id, None)
        except Exception as exc:
            if self._sock is not None:
                self._fail_pending(exc)

    def _fail_pending(self, exc: BaseException) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        self._pending_traces.clear()
        for future in pending:
            if not future.done():
                future.set_exception(exc)

    def _recv_matching(self, request_id: int) -> ProtocolResponse:
        while True:
            response = self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                with self._events_cv:
                    self._events.append(response.value)
                    self._events_cv.notify_all()
                continue
            raise FerricStoreError(
                "protocol response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    def _recv_response(self) -> ProtocolResponse:
        read_started_ns = time.perf_counter_ns()
        header = self._recv_exact(_HEADER.size)
        magic, version, flags, lane_id, opcode, request_id, body_len = _HEADER.unpack(header)
        if magic != _MAGIC or version != _RESPONSE_VERSION:
            raise FerricStoreError("invalid protocol response frame header")

        body = self._recv_exact(body_len)
        chunks = [body]
        final_flags = flags
        while final_flags & _FLAG_MORE_CHUNKS:
            next_header = self._recv_exact(_HEADER.size)
            (
                next_magic,
                next_version,
                next_flags,
                next_lane_id,
                next_opcode,
                next_request_id,
                next_body_len,
            ) = _HEADER.unpack(next_header)
            if (
                next_magic != _MAGIC
                or next_version != _RESPONSE_VERSION
                or next_lane_id != lane_id
                or next_opcode != opcode
                or next_request_id != request_id
            ):
                raise FerricStoreError("invalid protocol chunk continuation")
            chunks.append(self._recv_exact(next_body_len))
            final_flags = next_flags

        body = chunks[0] if len(chunks) == 1 else b"".join(chunks)
        read_done_ns = time.perf_counter_ns()
        decode_started_ns = read_done_ns
        if final_flags & _FLAG_COMPRESSED:
            body = zlib.decompress(body)

        if len(body) < _STATUS.size:
            raise FerricStoreError("protocol response body is too short")

        status = _STATUS.unpack_from(body, 0)[0]
        value = (
            _try_fast_response_value_at(opcode, body, _STATUS.size)
            if status == _STATUS_OK
            else None
        )
        fast_decoded = value is not None or _is_custom_compact_nil(opcode, body, _STATUS.size)
        if not fast_decoded:
            value_body = body[_STATUS.size :]
            value, rest = decode_value(value_body)
        else:
            rest = b""
        if rest:
            raise FerricStoreError("protocol response value has trailing bytes")
        decode_done_ns = time.perf_counter_ns()

        trace = None
        if final_flags & _FLAG_TRACE:
            value, server_trace = _extract_traced_value(value)
            trace = {
                "client": {
                    "response_read_us": (read_done_ns - read_started_ns) // 1000,
                    "decode_us": (decode_done_ns - decode_started_ns) // 1000,
                },
                "server": server_trace,
            }

        return ProtocolResponse(
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
            flags=final_flags,
            status=status,
            value=value,
            trace=trace,
        )

    def _recv_exact(self, size: int) -> bytes:
        sock = self._require_socket()
        if size == 0:
            return b""
        chunk = sock.recv(size)
        if len(chunk) == size:
            return chunk
        if not chunk:
            raise FerricStoreError("protocol connection closed")
        chunks = [chunk]
        received = len(chunk)
        while received < size:
            chunk = sock.recv(size - received)
            if not chunk:
                raise FerricStoreError("protocol connection closed")
            chunks.append(chunk)
            received += len(chunk)
        return b"".join(chunks)

    def _require_socket(self) -> socket.socket | ssl.SSLSocket:
        if self._sock is None:
            raise FerricStoreError("protocol connection is closed")
        return self._sock

    def _response_value(self, response: ProtocolResponse) -> Any:
        return _response_value(response)

    def _attach_client_trace(
        self, response: ProtocolResponse, client_trace: dict[str, Any] | None
    ) -> ProtocolResponse:
        if not client_trace:
            return response
        trace = dict(response.trace or {})
        client = dict(trace.get("client") or {})
        client.update(client_trace)
        trace["client"] = client
        return replace(response, trace=trace)

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)


class ProtocolAdapterPool:
    """Small protocol socket pool; each socket still multiplexes request lanes."""

    client: ProtocolAdapterPool

    def __init__(self, adapters: list[ProtocolAdapter]) -> None:
        if not adapters:
            raise ValueError("ProtocolAdapterPool requires at least one adapter")
        self.adapters = adapters
        self.client = self
        self._lock = threading.Lock()
        self._cursor = 0

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> ProtocolAdapterPool | ProtocolAdapter:
        max_connections = int(kwargs.pop("max_connections", 1) or 1)
        if max_connections <= 1:
            return ProtocolAdapter.from_url(url, **kwargs)
        return cls([ProtocolAdapter.from_url(url, **kwargs) for _ in range(max_connections)])

    @property
    def events(self) -> list[Any]:
        events: list[Any] = []
        for adapter in self.adapters:
            events.extend(adapter.events)
        return events

    def wait_event(self, timeout: float | None = None) -> Any | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            for adapter in self.adapters:
                event = adapter.wait_event(timeout=0.0)
                if event is not None:
                    return event
            if timeout == 0.0:
                return None
            wait_for = 0.05
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                wait_for = min(wait_for, remaining)
            time.sleep(wait_for)

    def close(self) -> None:
        for adapter in self.adapters:
            adapter.close()

    def pipeline(self, transaction: bool = False) -> ProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support Redis transactions")
        return ProtocolPipeline(self)

    def execute_command(self, *args: Any) -> Any:
        return self._next_adapter().execute_command(*args)

    def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        return self._next_adapter().execute_command_with_trace(*args)

    def submit_command(self, *args: Any) -> Future[Any]:
        return self._next_adapter().submit_command(*args)

    def submit_commands(self, commands: list[tuple[Any, ...]]) -> list[Future[Any]]:
        return self._next_adapter().submit_commands(commands)

    def submit_batch(self, commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
        return self._next_adapter().submit_batch(commands)

    def submit_mget(self, keys: Sequence[Any]) -> Future[Any]:
        return self._next_adapter().submit_mget(keys)

    def submit_mset_same_value(self, keys: Sequence[Any], value: Any) -> Future[Any]:
        return self._next_adapter().submit_mset_same_value(keys, value)

    def submit_mset_payload(self, payload: bytes) -> Future[Any]:
        return self._next_adapter().submit_mset_payload(payload)

    def submit_pipeline_payload(self, payload: bytes, count: int) -> Future[list[Any]]:
        return self._next_adapter().submit_pipeline_payload(payload, count)

    def submit_flow_many_payload(self, command: str, payload: bytes, count: int) -> Future[list[Any]]:
        return self._next_adapter().submit_flow_many_payload(command, payload, count)

    def submit_flow_value_mget_payload(self, payload: bytes) -> Future[Any]:
        return self._next_adapter().submit_flow_value_mget_payload(payload)

    def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> Any:
        replies = [adapter.subscribe_flow_wake(*args, **kwargs) for adapter in self.adapters]
        return replies[0] if len(replies) == 1 else replies

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return self._next_adapter().execute_batch(commands)

    def _next_adapter(self) -> ProtocolAdapter:
        with self._lock:
            adapter = self.adapters[self._cursor % len(self.adapters)]
            self._cursor += 1
            return adapter


class ProtocolPipeline:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.commands: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> ProtocolPipeline:
        self.commands.append(args)
        return self

    def execute(self) -> list[Any]:
        return cast(list[Any], self.adapter.execute_batch(self.commands))


class AsyncProtocolAdapter:
    """FerricStore protocol TCP adapter for the async SDK."""

    client: AsyncProtocolAdapter

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6388,
        *,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        timeout: float | None = 30.0,
        client_name: str | None = "ferricstore-python-async",
        compression: str = "none",
        lanes: int = 16,
        write_drain_bytes: int = 1_048_576,
        ssl_context: ssl.SSLContext | None = None,
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float | None = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username or "default"
        self.password = password
        self.tls = tls
        self.timeout = timeout
        self.client_name = client_name
        self.compression = compression
        self.lanes = max(1, lanes)
        self.write_drain_bytes = max(0, write_drain_bytes)
        self.ssl_context = ssl_context
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.client = self
        self._connect_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._request_id = 0
        self._lane_cursor = 0
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[ProtocolResponse]] = {}
        self._pending_traces: dict[int, dict[str, Any]] = {}
        self._events: list[Any] = []
        self._queued_write_bytes = 0
        self._last_activity = time.monotonic()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncProtocolAdapter:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        tls = scheme in _TLS_SCHEMES
        if scheme not in _SUPPORTED_SCHEMES:
            raise ValueError(f"unsupported FerricStore URL scheme: {parsed.scheme}")

        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (6389 if tls else 6388)
        username = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None
        _normalize_protocol_url_kwargs(kwargs)
        kwargs.setdefault("username", username)
        kwargs.setdefault("password", password)
        kwargs.setdefault("tls", tls)
        return cls(host, port, **kwargs)

    @property
    def events(self) -> list[Any]:
        return list(self._events)

    async def close(self) -> None:
        writer = self._writer
        reader_task = self._reader_task
        heartbeat_task = self._heartbeat_task
        self._reader = None
        self._writer = None
        self._reader_task = None
        self._heartbeat_task = None
        if heartbeat_task is not None and heartbeat_task is not asyncio.current_task():
            heartbeat_task.cancel()
        if reader_task is not None:
            reader_task.cancel()
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        if reader_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await reader_task
        if heartbeat_task is not None and heartbeat_task is not asyncio.current_task():
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
        if reader_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await reader_task
        self._fail_pending(FerricStoreError("protocol connection is closed"))

    def pipeline(self, transaction: bool = False) -> AsyncProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support Redis transactions")
        return AsyncProtocolPipeline(self)

    async def execute_command(self, *args: Any) -> Any:
        command = build_protocol_command(*args)
        await self._ensure_connected()
        response = await self._request(
            command.opcode, command.lane_id, command.payload, command.flags
        )
        return self._response_value(response)

    async def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        command = build_protocol_command(*args)
        await self._ensure_connected()
        response = await self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        if not commands:
            return []

        protocol_commands = [build_protocol_command(*command) for command in commands]
        if any(
            command.opcode in _CONTROL_OPCODES or command.flags for command in protocol_commands
        ):
            return [await self.execute_command(*command) for command in commands]

        batch_commands = [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id,
                "request_id": idx + 1,
                "body": command.payload,
            }
            for idx, command in enumerate(protocol_commands)
        ]
        await self._ensure_connected()
        response = await self._request(
            _OP_PIPELINE,
            1,
            {"atomicity": "none", "commands": batch_commands, "return": "compact"},
        )

        values = self._response_value(response)
        if not isinstance(values, list):
            raise FerricStoreError("protocol PIPELINE returned non-list response", raw=values)

        return [self._batch_item_value(item) for item in values]

    async def _ensure_connected(self) -> None:
        if self._writer is not None:
            return

        async with self._connect_lock:
            if self._writer is not None:
                return

            context = (self.ssl_context or ssl.create_default_context()) if self.tls else None
            connect = asyncio.open_connection(
                self.host,
                self.port,
                ssl=context,
                server_hostname=self.host if self.tls else None,
            )
            if self.timeout is None:
                self._reader, self._writer = await connect
            else:
                self._reader, self._writer = await asyncio.wait_for(connect, self.timeout)
            self._reader_task = asyncio.create_task(self._reader_loop())

            startup: dict[str, Any] = {
                "compression": self.compression,
                "compact_flow_responses": True,
            }
            if self.client_name is not None:
                startup["client_name"] = self.client_name
                startup["driver_name"] = self.client_name

            self._response_value(await self._request(_OP_STARTUP, 0, startup))

            if self.password is not None:
                self._response_value(
                    await self._request(
                        _OP_AUTH,
                        0,
                        {"username": self.username, "password": self.password},
                    )
                )
            self._start_heartbeat()

    def _start_heartbeat(self) -> None:
        if self.heartbeat_interval is None or self.heartbeat_interval <= 0:
            return
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        interval = float(self.heartbeat_interval or 0)
        timeout = self.heartbeat_timeout
        try:
            while interval > 0 and self._writer is not None:
                await asyncio.sleep(interval)
                if self._writer is None:
                    return
                if time.monotonic() - self._last_activity < interval:
                    continue
                try:
                    request = self._request(_OPCODES["PING"], 0, {})
                    if timeout is None:
                        await request
                    else:
                        await asyncio.wait_for(request, timeout=timeout)
                except Exception as exc:
                    self._fail_pending(FerricStoreError("protocol heartbeat failed", raw=exc))
                    await self._close_after_heartbeat_failure()
                    return
        except asyncio.CancelledError:
            raise

    async def _close_after_heartbeat_failure(self) -> None:
        writer = self._writer
        reader_task = self._reader_task
        self._reader = None
        self._writer = None
        self._reader_task = None
        if reader_task is not None:
            reader_task.cancel()
        if writer is not None:
            writer.close()
            await writer.wait_closed()

    def _next_request_id(self) -> int:
        self._request_id = (self._request_id + 1) & 0xFFFFFFFFFFFFFFFF
        if self._request_id == 0:
            self._request_id = 1
        return self._request_id

    def _next_lane_id(self, lane_id: int) -> int:
        if lane_id == 0 or self.lanes == 1:
            return lane_id
        self._lane_cursor = (self._lane_cursor % self.lanes) + 1
        return self._lane_cursor

    async def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> dict[str, Any] | None:
        writer = self._require_writer()
        trace_enabled = bool(extra_flags & _FLAG_TRACE)
        encode_started_ns = time.perf_counter_ns() if trace_enabled else 0
        body = payload if isinstance(payload, bytes) else encode_value(payload)
        flags = extra_flags
        if self.compression == "zlib" and body:
            body = zlib.compress(body)
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        writer.writelines((header, body))
        self._queued_write_bytes += len(header) + len(body)
        self._last_activity = time.monotonic()
        encode_done_ns = time.perf_counter_ns() if trace_enabled else 0
        write_started_ns = encode_done_ns
        if self.write_drain_bytes == 0 or self._queued_write_bytes >= self.write_drain_bytes:
            self._queued_write_bytes = 0
            await writer.drain()
        if not trace_enabled:
            return None
        write_done_ns = time.perf_counter_ns()
        return {
            "encode_us": (encode_done_ns - encode_started_ns) // 1000,
            "socket_write_us": (write_done_ns - write_started_ns) // 1000,
        }

    async def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
    ) -> ProtocolResponse:
        loop = asyncio.get_running_loop()
        async with self._write_lock:
            request_id = self._next_request_id()
            lane_id = self._next_lane_id(lane_id)
            future: asyncio.Future[ProtocolResponse] = loop.create_future()
            self._pending[request_id] = future
            trace_enabled = bool(flags & _FLAG_TRACE)
            if trace_enabled:
                self._pending_traces[request_id] = {}
            try:
                trace = await self._send(opcode, lane_id, request_id, payload, flags)
                if trace_enabled and trace is not None:
                    self._pending_traces[request_id].update(trace)
            except Exception:
                self._pending.pop(request_id, None)
                self._pending_traces.pop(request_id, None)
                raise
        return await future

    async def _reader_loop(self) -> None:
        try:
            while self._reader is not None:
                response = await self._recv_response()
                self._last_activity = time.monotonic()
                if response.request_id == 0:
                    self._events.append(response.value)
                    continue
                future = self._pending.pop(response.request_id, None)
                if future is not None and not future.done():
                    response = self._attach_client_trace(
                        response,
                        self._pending_traces.pop(response.request_id, None),
                    )
                    future.set_result(response)
                else:
                    self._pending_traces.pop(response.request_id, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._reader = None
            self._writer = None
            self._fail_pending(exc)

    def _fail_pending(self, exc: BaseException) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        self._pending_traces.clear()
        for future in pending:
            if not future.done():
                future.set_exception(exc)

    async def _recv_matching(self, request_id: int) -> ProtocolResponse:
        while True:
            response = await self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                self._events.append(response.value)
                continue
            raise FerricStoreError(
                "protocol response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    async def _recv_response(self) -> ProtocolResponse:
        read_started_ns = time.perf_counter_ns()
        header = await self._recv_exact(_HEADER.size)
        magic, version, flags, lane_id, opcode, request_id, body_len = _HEADER.unpack(header)
        if magic != _MAGIC or version != _RESPONSE_VERSION:
            raise FerricStoreError("invalid protocol response frame header")

        body = await self._recv_exact(body_len)
        chunks = [body]
        final_flags = flags
        while final_flags & _FLAG_MORE_CHUNKS:
            next_header = await self._recv_exact(_HEADER.size)
            (
                next_magic,
                next_version,
                next_flags,
                next_lane_id,
                next_opcode,
                next_request_id,
                next_body_len,
            ) = _HEADER.unpack(next_header)
            if (
                next_magic != _MAGIC
                or next_version != _RESPONSE_VERSION
                or next_lane_id != lane_id
                or next_opcode != opcode
                or next_request_id != request_id
            ):
                raise FerricStoreError("invalid protocol chunk continuation")
            chunks.append(await self._recv_exact(next_body_len))
            final_flags = next_flags

        body = chunks[0] if len(chunks) == 1 else b"".join(chunks)
        read_done_ns = time.perf_counter_ns()
        decode_started_ns = read_done_ns
        if final_flags & _FLAG_COMPRESSED:
            body = zlib.decompress(body)

        if len(body) < _STATUS.size:
            raise FerricStoreError("protocol response body is too short")

        status = _STATUS.unpack_from(body, 0)[0]
        value = (
            _try_fast_response_value_at(opcode, body, _STATUS.size)
            if status == _STATUS_OK
            else None
        )
        fast_decoded = value is not None or _is_custom_compact_nil(opcode, body, _STATUS.size)
        if not fast_decoded:
            value_body = body[_STATUS.size :]
            value, rest = decode_value(value_body)
        else:
            rest = b""
        if rest:
            raise FerricStoreError("protocol response value has trailing bytes")
        decode_done_ns = time.perf_counter_ns()

        trace = None
        if final_flags & _FLAG_TRACE:
            value, server_trace = _extract_traced_value(value)
            trace = {
                "client": {
                    "response_read_us": (read_done_ns - read_started_ns) // 1000,
                    "decode_us": (decode_done_ns - decode_started_ns) // 1000,
                },
                "server": server_trace,
            }

        return ProtocolResponse(
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
            flags=final_flags,
            status=status,
            value=value,
            trace=trace,
        )

    async def _recv_exact(self, size: int) -> bytes:
        reader = self._require_reader()
        try:
            return await reader.readexactly(size)
        except asyncio.IncompleteReadError as exc:
            raise FerricStoreError("protocol connection closed") from exc

    def _require_reader(self) -> asyncio.StreamReader:
        if self._reader is None:
            raise FerricStoreError("protocol connection is closed")
        return self._reader

    def _require_writer(self) -> asyncio.StreamWriter:
        if self._writer is None:
            raise FerricStoreError("protocol connection is closed")
        return self._writer

    def _response_value(self, response: ProtocolResponse) -> Any:
        return _response_value(response)

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)

    def _attach_client_trace(
        self, response: ProtocolResponse, client_trace: dict[str, Any] | None
    ) -> ProtocolResponse:
        if not client_trace:
            return response
        trace = dict(response.trace or {})
        client = dict(trace.get("client") or {})
        client.update(client_trace)
        trace["client"] = client
        return replace(response, trace=trace)


class AsyncProtocolAdapterPool:
    """Small async protocol socket pool; each socket still multiplexes request lanes."""

    client: AsyncProtocolAdapterPool

    def __init__(self, adapters: list[AsyncProtocolAdapter]) -> None:
        if not adapters:
            raise ValueError("AsyncProtocolAdapterPool requires at least one adapter")
        self.adapters = adapters
        self.client = self
        self._lock = asyncio.Lock()
        self._cursor = 0

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncProtocolAdapterPool | AsyncProtocolAdapter:
        max_connections = int(kwargs.pop("max_connections", 1) or 1)
        if max_connections <= 1:
            return AsyncProtocolAdapter.from_url(url, **kwargs)
        return cls([AsyncProtocolAdapter.from_url(url, **kwargs) for _ in range(max_connections)])

    @property
    def events(self) -> list[Any]:
        events: list[Any] = []
        for adapter in self.adapters:
            events.extend(adapter.events)
        return events

    async def close(self) -> None:
        await asyncio.gather(*(adapter.close() for adapter in self.adapters))

    def pipeline(self, transaction: bool = False) -> AsyncProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support Redis transactions")
        return AsyncProtocolPipeline(self)

    async def execute_command(self, *args: Any) -> Any:
        adapter = await self._next_adapter()
        return await adapter.execute_command(*args)

    async def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        adapter = await self._next_adapter()
        return await adapter.execute_command_with_trace(*args)

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        adapter = await self._next_adapter()
        return await adapter.execute_batch(commands)

    async def _next_adapter(self) -> AsyncProtocolAdapter:
        async with self._lock:
            adapter = self.adapters[self._cursor % len(self.adapters)]
            self._cursor += 1
            return adapter


class AsyncProtocolPipeline:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.commands: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> AsyncProtocolPipeline:
        self.commands.append(args)
        return self

    async def execute(self) -> list[Any]:
        return cast(list[Any], await self.adapter.execute_batch(self.commands))


def build_protocol_command(*args: Any) -> ProtocolCommand:
    if not args:
        raise InvalidCommandError("protocol command requires command name")

    name = _command_name(args[0])
    if name not in _OPCODES:
        raise InvalidCommandError(f"FerricStore protocol transport does not support command {name}")

    if name in {
        "GET",
        "SET",
        "DEL",
        "MGET",
        "MSET",
        "PING",
        "CLIENT.SETNAME",
        "CLIENT.INFO",
        "CAS",
        "LOCK",
        "UNLOCK",
        "EXTEND",
        "RATELIMIT.ADD",
        "FETCH_OR_COMPUTE",
        "FETCH_OR_COMPUTE_RESULT",
        "FETCH_OR_COMPUTE_ERROR",
        "HSET",
        "HGET",
        "HMGET",
        "HGETALL",
        "LPUSH",
        "RPUSH",
        "LPOP",
        "RPOP",
        "LRANGE",
        "SADD",
        "SREM",
        "SMEMBERS",
        "SISMEMBER",
        "ZADD",
        "ZREM",
        "ZRANGE",
        "ZSCORE",
        "CLUSTER.HEALTH",
        "CLUSTER.STATS",
        "CLUSTER.KEYSLOT",
        "CLUSTER.SLOTS",
        "CLUSTER.STATUS",
        "CLUSTER.JOIN",
        "CLUSTER.LEAVE",
        "CLUSTER.FAILOVER",
        "CLUSTER.PROMOTE",
        "CLUSTER.DEMOTE",
        "CLUSTER.ROLE",
        "FERRICSTORE.KEY_INFO",
        "FERRICSTORE.CONFIG",
        "FERRICSTORE.HOTNESS",
        "FERRICSTORE.METRICS",
        "FERRICSTORE.BLOBGC",
    }:
        return _build_basic_protocol_command(name, args[1:])

    if name.startswith("FLOW."):
        return _build_flow_protocol_command(name, args[1:])

    return ProtocolCommand(_OPCODES[name], _option_map(args[1:]), _lane_for_opcode(_OPCODES[name]))


def encode_frame(opcode: int, lane_id: int, request_id: int, value: Any, flags: int = 0) -> bytes:
    body = encode_value(value)
    return (
        _HEADER.pack(_MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)) + body
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
    partition_key = _optional_bytes(payload.get("partition_key"))
    if partition_key is False:
        return None
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
    partition_key = _optional_bytes(payload.get("partition_key"))
    if partition_key is False:
        return None

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


def _compact_flow_many_payloads_from_raw(
    commands: list[tuple[Any, ...]],
) -> list[tuple[int, bytes, int]] | None:
    if not commands:
        return None
    try:
        names = [_command_name(command[0]) for command in commands if command]
    except Exception:
        return None
    if len(names) != len(commands):
        return None
    if all(name == "FLOW.CREATE" for name in names):
        return _compact_flow_create_many_payloads_from_raw(commands)
    if all(name == "FLOW.COMPLETE" for name in names):
        return _compact_flow_complete_many_payloads_from_raw(commands)
    if all(name == "FLOW.STEP_CONTINUE" for name in names):
        return _compact_flow_step_continue_payloads_from_raw(commands)
    if all(name == "FLOW.START_AND_CLAIM" for name in names):
        return _compact_flow_start_and_claim_payloads_from_raw(commands)
    if all(name == "FLOW.VALUE.PUT" for name in names):
        return _compact_flow_value_put_payloads_from_raw(commands)
    return None


def _compact_flow_create_many_payloads_from_raw(
    commands: list[tuple[Any, ...]],
) -> list[tuple[int, bytes, int]] | None:
    groups: list[tuple[tuple[bytes, bytes, int, int], list[list[Any]]]] = []
    current_key: tuple[bytes, bytes, int, int] | None = None
    current_items: list[list[Any]] = []

    for command in commands:
        protocol_command = build_protocol_command(*command)
        if (
            protocol_command.opcode != _OPCODES["FLOW.CREATE"]
            or protocol_command.flags != 0
            or not isinstance(protocol_command.payload, dict)
        ):
            return None
        payload = protocol_command.payload
        if not set(payload).issubset({"id", "type", "state", "now_ms", "run_at_ms", "payload"}):
            return None
        flow_id = _maybe_bytes(payload.get("id"))
        flow_type = _maybe_bytes(payload.get("type"))
        state = _maybe_bytes(payload.get("state"))
        item_payload = _maybe_bytes(payload.get("payload"))
        now_ms = payload.get("now_ms")
        run_at_ms = payload.get("run_at_ms")
        if (
            flow_id is None
            or flow_type is None
            or state is None
            or item_payload is None
            or not isinstance(now_ms, int)
            or not isinstance(run_at_ms, int)
        ):
            return None

        key = (flow_type, state, now_ms, run_at_ms)
        if current_key is None:
            current_key = key
        elif key != current_key:
            groups.append((current_key, current_items))
            current_key = key
            current_items = []
        current_items.append([flow_id, item_payload])

    if current_key is not None:
        groups.append((current_key, current_items))

    payloads: list[tuple[int, bytes, int]] = []
    for (flow_type, state, now_ms, run_at_ms), items in groups:
        compact = _compact_flow_create_many_payload(
            {
                "type": flow_type,
                "state": state,
                "now_ms": now_ms,
                "run_at_ms": run_at_ms,
                "independent": True,
                "return": "OK_ON_SUCCESS",
                "items": items,
            }
        )
        if compact is None:
            return None
        payloads.append((_OP_FLOW_CREATE_MANY, compact, len(items)))
    return payloads


def _compact_flow_complete_many_payloads_from_raw(
    commands: list[tuple[Any, ...]],
) -> list[tuple[int, bytes, int]] | None:
    groups: list[tuple[int, bool, list[list[Any]]]] = []
    current_now: int | None = None
    current_partitioned: bool | None = None
    current_items: list[list[Any]] = []

    for command in commands:
        protocol_command = build_protocol_command(*command)
        if (
            protocol_command.opcode != _OPCODES["FLOW.COMPLETE"]
            or protocol_command.flags != 0
            or not isinstance(protocol_command.payload, dict)
        ):
            return None
        payload = protocol_command.payload
        if not set(payload).issubset(
            {"id", "lease_token", "fencing_token", "partition_key", "now_ms"}
        ):
            return None
        flow_id = _maybe_bytes(payload.get("id"))
        lease_token = _maybe_bytes(payload.get("lease_token"))
        partition_key = _optional_bytes(payload.get("partition_key"))
        fencing_token = payload.get("fencing_token")
        now_ms = payload.get("now_ms")
        if (
            flow_id is None
            or lease_token is None
            or partition_key is False
            or not isinstance(fencing_token, int)
            or not isinstance(now_ms, int)
        ):
            return None

        partitioned = partition_key is not None
        if current_now is None:
            current_now = now_ms
            current_partitioned = partitioned
        elif now_ms != current_now or partitioned != current_partitioned:
            groups.append((current_now, bool(current_partitioned), current_items))
            current_now = now_ms
            current_partitioned = partitioned
            current_items = []

        if partitioned:
            current_items.append([flow_id, cast(bytes, partition_key), lease_token, fencing_token])
        else:
            current_items.append([flow_id, lease_token, fencing_token])

    if current_now is not None:
        groups.append((current_now, bool(current_partitioned), current_items))

    payloads: list[tuple[int, bytes, int]] = []
    for now_ms, _partitioned, items in groups:
        compact = _compact_flow_complete_many_payload(
            {
                "now_ms": now_ms,
                "independent": True,
                "return": "OK_ON_SUCCESS",
                "items": items,
            }
        )
        if compact is None:
            return None
        payloads.append((_OP_FLOW_COMPLETE_MANY, compact, len(items)))
    return payloads


def _compact_flow_step_continue_payloads_from_raw(
    commands: list[tuple[Any, ...]],
) -> list[tuple[int, bytes, int]] | None:
    groups: list[
        tuple[tuple[bytes, bytes, int, bytes | None], list[tuple[bytes, bytes | None, bytes, int, int]]]
    ] = []
    current_key: tuple[bytes, bytes, int, bytes | None] | None = None
    current_items: list[tuple[bytes, bytes | None, bytes, int, int]] = []

    for command in commands:
        parsed = _parse_compact_flow_step_continue_raw(command)
        if parsed is None:
            return None

        (
            flow_id,
            lease_token,
            from_state,
            to_state,
            partition_key,
            fencing_token,
            lease_ms,
            now_ms,
            return_mode,
        ) = parsed

        key = (from_state, to_state, lease_ms, return_mode)
        if current_key is None:
            current_key = key
        elif key != current_key:
            groups.append((current_key, current_items))
            current_key = key
            current_items = []

        current_items.append(
            (flow_id, cast(bytes | None, partition_key), lease_token, fencing_token, now_ms)
        )

    if current_key is not None:
        groups.append((current_key, current_items))

    payloads: list[tuple[int, bytes, int]] = []
    for (from_state, to_state, lease_ms, return_mode), items in groups:
        mode = 33 if return_mode == b"JOBS_COMPACT" else 6
        parts = [
            struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | mode, len(items)),
            _compact_binary(from_state),
            _compact_binary(to_state),
            struct.pack(">q", lease_ms),
        ]

        for flow_id, partition_key, lease_token, fencing_token, now_ms in items:
            parts.append(_compact_binary(flow_id))
            parts.append(_compact_optional_binary(partition_key))
            parts.append(_compact_binary(lease_token))
            parts.append(struct.pack(">qq", fencing_token, now_ms))

        payloads.append((_OP_PIPELINE, b"".join(parts), len(items)))

    return payloads


def _parse_compact_flow_step_continue_raw(
    command: tuple[Any, ...],
) -> tuple[bytes, bytes, bytes, bytes, bytes | None, int, int, int, bytes | None] | None:
    if len(command) < 11 or len(command) % 2 != 1:
        return None
    if command[0] != "FLOW.STEP_CONTINUE" and _command_name(command[0]) != "FLOW.STEP_CONTINUE":
        return None

    flow_id = _maybe_bytes(command[1])
    lease_token = _maybe_bytes(command[2])
    from_state = _maybe_bytes(command[3])
    to_state = _maybe_bytes(command[4])
    partition_key: bytes | None | bool = None
    fencing_token: int | None = None
    lease_ms: int | None = None
    now_ms: int | None = None
    return_mode: bytes | None = None

    for idx in range(5, len(command), 2):
        token = command[idx]
        value = command[idx + 1]
        name = token if isinstance(token, str) and token.isupper() else _command_name(token)
        if name == "FENCING":
            fencing_token = _raw_int(value)
        elif name == "LEASE_MS":
            lease_ms = _raw_int(value)
        elif name == "NOW":
            now_ms = _raw_int(value)
        elif name == "PARTITION":
            partition_key = _optional_bytes(value)
        elif name == "RETURN":
            raw_return = _maybe_bytes(value)
            if raw_return is None or raw_return.upper() not in {b"JOBS_COMPACT", b"JOB_COMPACT"}:
                return None
            return_mode = b"JOBS_COMPACT"
        else:
            return None

    if (
        flow_id is None
        or lease_token is None
        or from_state is None
        or to_state is None
        or partition_key is False
        or fencing_token is None
        or lease_ms is None
        or now_ms is None
    ):
        return None

    return (
        flow_id,
        lease_token,
        from_state,
        to_state,
        cast(bytes | None, partition_key),
        fencing_token,
        lease_ms,
        now_ms,
        return_mode,
    )


def _compact_flow_start_and_claim_payloads_from_raw(
    commands: list[tuple[Any, ...]],
) -> list[tuple[int, bytes, int]] | None:
    groups: list[
        tuple[
            tuple[bytes, bytes, bytes, int, bool],
            list[tuple[bytes, bytes | None, bytes | None, int]],
        ]
    ] = []
    current_key: tuple[bytes, bytes, bytes, int, bool] | None = None
    current_items: list[tuple[bytes, bytes | None, bytes | None, int]] = []

    for command in commands:
        parsed = _parse_compact_flow_start_and_claim_raw(command)
        if parsed is None:
            return None

        (
            flow_id,
            flow_type,
            initial_state,
            worker,
            partition_key,
            item_payload,
            lease_ms,
            now_ms,
            jobs_compact,
        ) = parsed

        if (
            flow_id is None
            or flow_type is None
            or initial_state is None
            or worker is None
            or partition_key is False
            or item_payload is False
            or not isinstance(lease_ms, int)
            or not isinstance(now_ms, int)
        ):
            return None

        key = (flow_type, initial_state, worker, lease_ms, jobs_compact)
        if current_key is None:
            current_key = key
        elif key != current_key:
            groups.append((current_key, current_items))
            current_key = key
            current_items = []

        current_items.append(
            (flow_id, cast(bytes | None, partition_key), cast(bytes | None, item_payload), now_ms)
        )

    if current_key is not None:
        groups.append((current_key, current_items))

    payloads: list[tuple[int, bytes, int]] = []
    for (flow_type, initial_state, worker, lease_ms, jobs_compact), items in groups:
        mode = 13 if jobs_compact else 12
        parts = [
            _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | mode, len(items)),
            _compact_binary(flow_type),
            _compact_binary(initial_state),
            _compact_binary(worker),
            struct.pack(">q", lease_ms),
        ]

        for flow_id, partition_key, item_payload, now_ms in items:
            parts.append(_compact_binary(flow_id))
            parts.append(_compact_optional_binary(partition_key))
            parts.append(_compact_optional_binary(item_payload))
            parts.append(struct.pack(">q", now_ms))

        payloads.append((_OP_PIPELINE, b"".join(parts), len(items)))

    return payloads


def _parse_compact_flow_start_and_claim_raw(
    command: tuple[Any, ...],
) -> tuple[bytes, bytes, bytes, bytes, bytes | None, bytes | None, int, int, bool] | None:
    if len(command) < 12 or len(command) % 2 != 0:
        return None
    if command[0] != "FLOW.START_AND_CLAIM" and _command_name(command[0]) != "FLOW.START_AND_CLAIM":
        return None

    flow_id = _maybe_bytes(command[1])
    flow_type = initial_state = worker = None
    partition_key: bytes | None | bool = None
    item_payload: bytes | None | bool = None
    lease_ms: int | None = None
    now_ms: int | None = None
    jobs_compact = False

    for idx in range(2, len(command), 2):
        token = command[idx]
        value = command[idx + 1]
        name = token if isinstance(token, str) and token.isupper() else _command_name(token)
        if name == "TYPE":
            flow_type = _maybe_bytes(value)
        elif name == "INITIAL_STATE":
            initial_state = _maybe_bytes(value)
        elif name == "WORKER":
            worker = _maybe_bytes(value)
        elif name == "LEASE_MS":
            lease_ms = _raw_int(value)
        elif name == "NOW":
            now_ms = _raw_int(value)
        elif name == "PARTITION":
            partition_key = _optional_bytes(value)
        elif name == "PAYLOAD":
            item_payload = _optional_bytes(value)
        elif name == "RETURN":
            if value not in {"JOBS_COMPACT", "jobs_compact"}:
                return None
            jobs_compact = True
        else:
            return None

    if (
        flow_id is None
        or flow_type is None
        or initial_state is None
        or worker is None
        or partition_key is False
        or item_payload is False
        or lease_ms is None
        or now_ms is None
    ):
        return None
    return (
        flow_id,
        flow_type,
        initial_state,
        worker,
        partition_key,
        item_payload,
        lease_ms,
        now_ms,
        jobs_compact,
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


def _compact_flow_value_put_payloads_from_raw(
    commands: list[tuple[Any, ...]],
) -> list[tuple[int, bytes, int]] | None:
    payloads: list[tuple[int, bytes, int]] = []
    current_mode: int | None = None
    current_items: list[dict[str, Any]] = []

    def flush() -> bool:
        nonlocal current_mode, current_items
        if current_mode is None:
            return True
        compact = _compact_flow_value_put_payload(current_mode, current_items)
        if compact is None:
            return False
        payloads.append((_OP_PIPELINE, compact, len(current_items)))
        current_mode = None
        current_items = []
        return True

    for command in commands:
        parsed = _parse_compact_flow_value_put_raw(command)
        if parsed is None:
            return None
        mode, item = parsed

        if current_mode is None:
            current_mode = mode
        elif current_mode != mode:
            if not flush():
                return None
            current_mode = mode
        current_items.append(item)

    if not flush():
        return None
    return payloads


def _parse_compact_flow_value_put_raw(
    command: tuple[Any, ...],
) -> tuple[int, dict[str, Any]] | None:
    if len(command) < 4 or len(command) % 2 != 0:
        return None
    if command[0] != "FLOW.VALUE.PUT" and _command_name(command[0]) != "FLOW.VALUE.PUT":
        return None

    value = _maybe_bytes(command[1])
    owner_flow_id: bytes | None = None
    name_value: bytes | None = None
    partition_key: bytes | None | bool = None
    now_ms: int | None = None
    return_mode: Any = None

    for idx in range(2, len(command), 2):
        token = command[idx]
        option_value = command[idx + 1]
        option_name = token if isinstance(token, str) and token.isupper() else _command_name(token)
        if option_name == "NOW":
            now_ms = _raw_int(option_value)
        elif option_name == "OWNER_FLOW_ID":
            owner_flow_id = _maybe_bytes(option_value)
        elif option_name == "NAME":
            name_value = _maybe_bytes(option_value)
        elif option_name == "PARTITION":
            partition_key = _optional_bytes(option_value)
        elif option_name == "RETURN":
            return_mode = option_value
        else:
            return None

    if value is None or now_ms is None or partition_key is False:
        return None

    if owner_flow_id is None and name_value is None and partition_key is None:
        if return_mode is None:
            mode = 7
        elif _ok_on_success_return_mode(return_mode):
            mode = 15
        else:
            return None
        return mode, {"value": value, "now_ms": now_ms}

    if owner_flow_id is None or name_value is None:
        return None

    if return_mode is None:
        mode = 8
    elif _ok_on_success_return_mode(return_mode):
        mode = 14
    else:
        return None

    return (
        mode,
        {
            "value": value,
            "owner_flow_id": owner_flow_id,
            "name": name_value,
            "partition_key": cast(bytes | None, partition_key),
            "now_ms": now_ms,
        },
    )


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


def _send_frame(sock: socket.socket | ssl.SSLSocket, header: bytes, body: bytes) -> None:
    sock.sendall(header if not body else header + body)


def encode_value(value: Any) -> bytes:
    if value is None:
        return b"\x00"
    if value is True:
        return b"\x01"
    if value is False:
        return b"\x02"
    if isinstance(value, int):
        return b"\x03" + struct.pack(">q", value)
    if isinstance(value, str):
        return _encode_binary(value.encode())
    if isinstance(value, bytes):
        return _encode_binary(value)
    if isinstance(value, bytearray):
        return _encode_binary(bytes(value))
    if isinstance(value, (list, tuple)):
        body = b"".join(encode_value(item) for item in value)
        return b"\x05" + struct.pack(">I", len(value)) + body
    if isinstance(value, dict):
        entries = []
        for key, item in value.items():
            encoded_key = _key_bytes(key)
            entries.append(struct.pack(">I", len(encoded_key)) + encoded_key + encode_value(item))
        return b"\x06" + struct.pack(">I", len(value)) + b"".join(entries)
    if isinstance(value, float):
        return b"\x07" + struct.pack(">d", value)
    return _encode_binary(str(value).encode())


def decode_value(data: bytes) -> tuple[Any, bytes]:
    if not data:
        raise FerricStoreError("protocol value is empty")
    tag = data[0]
    rest = data[1:]
    if tag == 0:
        return None, rest
    if tag == 1:
        return True, rest
    if tag == 2:
        return False, rest
    if tag == 3:
        _require_len(rest, 8)
        return struct.unpack(">q", rest[:8])[0], rest[8:]
    if tag == 4:
        return _decode_binary(rest)
    if tag == 5:
        _require_len(rest, 4)
        count = struct.unpack(">I", rest[:4])[0]
        items = []
        next_data = rest[4:]
        for _ in range(count):
            item, next_data = decode_value(next_data)
            items.append(item)
        return items, next_data
    if tag == 6:
        _require_len(rest, 4)
        count = struct.unpack(">I", rest[:4])[0]
        result: dict[bytes, Any] = {}
        next_data = rest[4:]
        for _ in range(count):
            key, after_key = _decode_binary(next_data)
            value, next_data = decode_value(after_key)
            result[key] = value
        return result, next_data
    if tag == 7:
        _require_len(rest, 8)
        return struct.unpack(">d", rest[:8])[0], rest[8:]
    raise FerricStoreError("protocol value has unknown tag")


def _try_fast_response_value(opcode: int, data: bytes) -> Any | None:
    return _try_fast_response_value_at(opcode, data, 0)


def _try_fast_response_value_at(opcode: int, data: bytes, offset: int) -> Any | None:
    if opcode == _OP_PIPELINE:
        if len(data) <= offset:
            return None
        if data[offset] == _COMPACT_PIPELINE_RESPONSE:
            return _try_decode_custom_pipeline_response(data, offset)
        if data[offset] == _COMPACT_KV_MGET:
            return _try_decode_custom_kv_mget(data, offset)
        if data[offset] == _COMPACT_KV_MGET_FIXED:
            return _try_decode_custom_kv_mget_fixed(data, offset)
        if data[offset] == _COMPACT_FLOW_RECORD_LIST:
            return _try_decode_custom_flow_record_list(data, offset)
        if data[offset] == _COMPACT_FLOW_CLAIM_JOBS:
            return _try_decode_custom_claim_jobs(data, offset)
        if data[offset] == _COMPACT_BINARY_LIST_LIST:
            return _try_decode_custom_binary_list_list(data, offset)
        if data[offset] == _COMPACT_BINARY_MAP_LIST:
            return _try_decode_custom_binary_map_list(data, offset)
        if data[offset] == _COMPACT_INTEGER_LIST:
            return _try_decode_custom_integer_list(data, offset)
        if data[offset] == _COMPACT_OK_LIST:
            return _try_decode_custom_ok_list(data, offset)
        return None
    if opcode == _OP_GET:
        if len(data) > offset and data[offset] == _COMPACT_KV_GET:
            return _try_decode_custom_kv_get(data, offset)
        return None
    if opcode in {_OP_SET, _OP_MSET}:
        if len(data) > offset and data[offset] == _COMPACT_OK_LIST:
            ok_values = _try_decode_custom_ok_list(data, offset)
            if ok_values is not None and len(ok_values) == 1:
                return b"OK"
        return None
    if opcode == _OP_MGET:
        if len(data) > offset and data[offset] == _COMPACT_KV_MGET:
            return _try_decode_custom_kv_mget(data, offset)
        if len(data) > offset and data[offset] == _COMPACT_KV_MGET_FIXED:
            return _try_decode_custom_kv_mget_fixed(data, offset)
        return None
    if opcode == _OP_FLOW_VALUE_MGET:
        if len(data) > offset and data[offset] == _COMPACT_KV_MGET:
            return _try_decode_custom_kv_mget(data, offset)
        if len(data) > offset and data[offset] == _COMPACT_KV_MGET_FIXED:
            return _try_decode_custom_kv_mget_fixed(data, offset)
        return None
    if opcode == _OP_FLOW_GET:
        if len(data) > offset and data[offset] == _COMPACT_FLOW_RECORD:
            return _try_decode_custom_flow_record(data, offset)
        return None
    if opcode in _FLOW_RECORD_LIST_OPCODES:
        if len(data) > offset and data[offset] == _COMPACT_FLOW_RECORD_LIST:
            return _try_decode_custom_flow_record_list(data, offset)
        return None
    if opcode == _OP_FLOW_CLAIM_DUE:
        if len(data) > offset and data[offset] == _COMPACT_FLOW_CLAIM_JOBS:
            return _try_decode_custom_claim_jobs(data, offset)
        return _try_decode_claim_jobs_compact(data, offset)
    if opcode in {
        _OP_FLOW_CREATE_MANY,
        _OP_FLOW_COMPLETE_MANY,
        _OP_FLOW_RETRY_MANY,
        _OP_FLOW_FAIL_MANY,
        _OP_FLOW_CANCEL_MANY,
    }:
        if len(data) > offset and data[offset] == _COMPACT_OK_LIST:
            return _try_decode_custom_ok_list(data, offset)
        return _try_decode_binary_list(data, offset)
    return None


def _try_decode_custom_pipeline_response(data: bytes, offset: int = 0) -> list[list[Any]] | None:
    try:
        offset += 1
        count = struct.unpack_from(">I", data, offset)[0]
        offset += 4
        values: list[list[Any]] = []

        for _ in range(count):
            status_code = data[offset]
            offset += 1

            if status_code == 0:
                present = data[offset]
                offset += 1
                if present == 0:
                    values.append(["ok", None])
                elif present == 1:
                    value, offset = _read_compact_binary(data, offset)
                    values.append(["ok", value])
                elif present == 2:
                    value, offset = _read_custom_flow_record(data, offset)
                    values.append(["ok", value])
                elif present == 3:
                    value, offset = _read_custom_flow_record_list(data, offset)
                    values.append(["ok", value])
                elif present == 4:
                    value, offset = _read_custom_claim_job(data, offset)
                    values.append(["ok", value])
                elif present == 5:
                    value, offset = _read_custom_flow_value_ref(data, offset)
                    values.append(["ok", value])
                elif present == 6:
                    value, offset = _read_custom_binary_list(data, offset)
                    values.append(["ok", value])
                elif present == 7:
                    value, offset = _read_custom_binary_map(data, offset)
                    values.append(["ok", value])
                else:
                    return None
            elif status_code in (1, 2):
                reason, offset = _read_compact_binary(data, offset)
                values.append(["busy" if status_code == 1 else "error", reason])
            else:
                return None

        return values if offset == len(data) else None
    except (IndexError, struct.error, ValueError):
        return None


def _is_custom_compact_nil(opcode: int, data: bytes, offset: int) -> bool:
    return (
        opcode == _OP_GET
        and len(data) == offset + 2
        and data[offset] == _COMPACT_KV_GET
        and data[offset + 1] == 0
    )


def _try_decode_custom_kv_get(data: bytes, offset: int = 0) -> bytes | None:
    try:
        offset += 1
        present = data[offset]
        offset += 1
        if present == 0:
            return None
        if present != 1:
            return None
        value, offset = _read_compact_binary(data, offset)
        if offset != len(data):
            return None
        return value
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_kv_mget(data: bytes, offset: int = 0) -> list[bytes | None] | None:
    offset += 1
    data_len = len(data)
    if offset + 4 > data_len:
        return None

    unpack_u32 = _COMPACT_U32.unpack_from
    count = unpack_u32(data, offset)[0]
    offset += 4
    values: list[bytes | None] = []
    append_value = values.append

    for _ in range(count):
        if offset >= data_len:
            return None

        present = data[offset]
        offset += 1

        if present == 1:
            if offset + 4 > data_len:
                return None

            size = unpack_u32(data, offset)[0]
            offset += 4
            end = offset + size
            if size == _NULL_U32 or end > data_len:
                return None

            append_value(data[offset:end])
            offset = end
        elif present == 0:
            append_value(None)
        else:
            return None

    if offset != data_len:
        return None
    return values


def _try_decode_custom_kv_mget_fixed(
    data: bytes, offset: int = 0
) -> list[bytes | None] | None:
    offset += 1
    data_len = len(data)
    if offset + 8 > data_len:
        return None

    unpack_u32 = _COMPACT_U32.unpack_from
    count = unpack_u32(data, offset)[0]
    offset += 4
    size = unpack_u32(data, offset)[0]
    offset += 4

    payload_len = data_len - offset
    if size == _NULL_U32 or payload_len != count * size:
        return None

    if count == 0:
        return [] if offset == data_len else None
    if size == 0:
        return [b""] * count

    end = offset + payload_len
    return [data[item_offset : item_offset + size] for item_offset in range(offset, end, size)]


def _try_decode_custom_binary_list_list(data: bytes, offset: int = 0) -> list[list[bytes]] | None:
    try:
        offset += 1
        data_len = len(data)
        if offset + 4 > data_len:
            return None
        unpack_u32 = _COMPACT_U32.unpack_from
        count = unpack_u32(data, offset)[0]
        offset += 4
        values: list[list[bytes] | None] = [None] * count

        for outer_index in range(count):
            if offset + 4 > data_len:
                return None
            inner_count = unpack_u32(data, offset)[0]
            offset += 4

            if inner_count == 0:
                values[outer_index] = []
                continue

            if inner_count == 1:
                if offset + 4 > data_len:
                    return None
                size = unpack_u32(data, offset)[0]
                offset += 4
                if size == _NULL_U32 or offset + size > data_len:
                    return None
                values[outer_index] = [data[offset : offset + size]]
                offset += size
                continue

            inner_values = [b""] * inner_count
            for inner_index in range(inner_count):
                if offset + 4 > data_len:
                    return None
                size = unpack_u32(data, offset)[0]
                offset += 4
                if size == _NULL_U32 or offset + size > data_len:
                    return None
                inner_values[inner_index] = data[offset : offset + size]
                offset += size
            values[outer_index] = inner_values

        if offset != data_len:
            return None
        return cast(list[list[bytes]], values)
    except (IndexError, struct.error):
        return None


def _try_decode_custom_binary_map_list(
    data: bytes, offset: int = 0
) -> list[dict[bytes, bytes]] | None:
    try:
        offset += 1
        data_len = len(data)
        if offset + 4 > data_len:
            return None
        unpack_u32 = _COMPACT_U32.unpack_from
        count = unpack_u32(data, offset)[0]
        offset += 4
        values: list[dict[bytes, bytes] | None] = [None] * count

        for outer_index in range(count):
            if offset + 4 > data_len:
                return None
            item_count = unpack_u32(data, offset)[0]
            offset += 4

            if item_count == 0:
                values[outer_index] = {}
                continue

            items: dict[bytes, bytes] = {}
            for _ in range(item_count):
                if offset + 4 > data_len:
                    return None
                key_size = unpack_u32(data, offset)[0]
                offset += 4
                if key_size == _NULL_U32 or offset + key_size > data_len:
                    return None
                key = data[offset : offset + key_size]
                offset += key_size

                if offset + 4 > data_len:
                    return None
                value_size = unpack_u32(data, offset)[0]
                offset += 4
                if value_size == _NULL_U32 or offset + value_size > data_len:
                    return None
                items[key] = data[offset : offset + value_size]
                offset += value_size

            values[outer_index] = items

        if offset != data_len:
            return None
        return cast(list[dict[bytes, bytes]], values)
    except (IndexError, struct.error):
        return None


def _read_custom_binary_list(data: bytes, offset: int) -> tuple[list[bytes], int]:
    count = _read_u32(data, offset)
    offset += 4
    values: list[bytes] = []
    for _ in range(count):
        value, offset = _read_compact_binary(data, offset)
        values.append(value)
    return values, offset


def _read_custom_binary_map(data: bytes, offset: int) -> tuple[dict[bytes, bytes], int]:
    count = _read_u32(data, offset)
    offset += 4
    values: dict[bytes, bytes] = {}
    for _ in range(count):
        key, offset = _read_compact_binary(data, offset)
        value, offset = _read_compact_binary(data, offset)
        values[key] = value
    return values, offset


def _try_decode_custom_claim_jobs(data: bytes, offset: int = 0) -> list[list[Any]] | None:
    try:
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        items: list[list[Any] | None] = [None] * count
        for index in range(count):
            id_value, offset = _read_compact_binary(data, offset)
            partition, offset = _read_compact_optional_binary(data, offset)
            lease, offset = _read_compact_binary(data, offset)
            _require_available(data, offset, 8)
            fencing = struct.unpack_from(">q", data, offset)[0]
            offset += 8
            items[index] = [id_value, partition, lease, fencing]
        if offset != len(data):
            return None
        return cast(list[list[Any]], items)
    except (IndexError, struct.error, FerricStoreError):
        return None


def _read_custom_claim_job(data: bytes, offset: int) -> tuple[list[Any], int]:
    id_value, offset = _read_compact_binary(data, offset)
    partition, offset = _read_compact_optional_binary(data, offset)
    lease, offset = _read_compact_binary(data, offset)
    _require_available(data, offset, 8)
    fencing = struct.unpack_from(">q", data, offset)[0]
    offset += 8
    return [id_value, partition, lease, fencing], offset


def _read_custom_flow_value_ref(data: bytes, offset: int) -> tuple[dict[bytes, Any], int]:
    ref, offset = _read_compact_binary(data, offset)
    partition_key, offset = _read_compact_optional_binary(data, offset)
    owner_flow_id, offset = _read_compact_optional_binary(data, offset)
    value: dict[bytes, Any] = {b"ref": ref}
    if partition_key is not None:
        value[b"partition_key"] = partition_key
    if owner_flow_id is not None:
        value[b"owner_flow_id"] = owner_flow_id
    return value, offset


def _try_decode_custom_ok_list(data: bytes, offset: int = 0) -> list[bytes] | None:
    try:
        if len(data) - offset != 5:
            return None
        return [b"OK"] * _read_u32(data, offset + 1)
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_integer_list(data: bytes, offset: int = 0) -> list[int] | None:
    try:
        if data[offset] != _COMPACT_INTEGER_LIST:
            return None
        offset += 1
        data_len = len(data)
        if offset + 4 > data_len:
            return None
        count = _COMPACT_U32.unpack_from(data, offset)[0]
        offset += 4
        expected_len = offset + count * 8
        if expected_len != data_len:
            return None
        unpack_i64 = _COMPACT_I64.unpack_from
        values = [0] * count
        for index in range(count):
            values[index] = int(unpack_i64(data, offset)[0])
            offset += 8
        return values
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_flow_record(data: bytes, offset: int = 0) -> dict[bytes, Any] | None:
    try:
        value, offset = _read_custom_flow_record(data, offset)
        return value if offset == len(data) else None
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_flow_record_list(
    data: bytes, offset: int = 0
) -> list[dict[bytes, Any]] | None:
    try:
        value, offset = _read_custom_flow_record_list(data, offset)
        return value if offset == len(data) else None
    except (IndexError, struct.error, FerricStoreError):
        return None


def _read_custom_flow_record(data: bytes, offset: int) -> tuple[dict[bytes, Any], int]:
    if data[offset] != _COMPACT_FLOW_RECORD:
        raise FerricStoreError("protocol compact value expected Flow record")
    offset += 1
    data_len = len(data)
    if offset + 4 > data_len:
        raise FerricStoreError("protocol compact Flow record is truncated")
    unpack_u32 = _COMPACT_U32.unpack_from
    count = unpack_u32(data, offset)[0]
    offset += 4
    record: dict[bytes, Any] = {}
    field_keys = _FLOW_RECORD_FIELD_KEYS
    field_keys_len = _FLOW_RECORD_FIELD_KEYS_LEN
    decode_value = _decode_flow_record_value_at
    for _ in range(count):
        if offset >= data_len:
            raise FerricStoreError("protocol compact Flow record is truncated")
        key_id = data[offset]
        offset += 1
        if key_id == 0:
            if offset + 4 > data_len:
                raise FerricStoreError("protocol compact Flow record key is truncated")
            size = unpack_u32(data, offset)[0]
            offset += 4
            if size == _NULL_U32 or offset + size > data_len:
                raise FerricStoreError("protocol compact Flow record key is invalid")
            key = data[offset : offset + size]
            offset += size
        elif key_id < field_keys_len:
            key = field_keys[key_id]
        else:
            raise FerricStoreError("protocol compact Flow record key is unknown")
        value, offset = decode_value(data, offset, data_len)
        record[key] = value
    return record, offset


def _decode_flow_record_value_at(data: bytes, offset: int, data_len: int) -> tuple[Any, int]:
    if offset >= data_len:
        raise FerricStoreError("protocol value is truncated")
    tag = data[offset]
    offset += 1
    if tag == 0:
        return None, offset
    if tag == 1:
        return True, offset
    if tag == 2:
        return False, offset
    if tag == 3:
        if offset + 8 > data_len:
            raise FerricStoreError("protocol integer is truncated")
        return _COMPACT_I64.unpack_from(data, offset)[0], offset + 8
    if tag == 4:
        if offset + 4 > data_len:
            raise FerricStoreError("protocol binary is truncated")
        size = _COMPACT_U32.unpack_from(data, offset)[0]
        offset += 4
        if size == _NULL_U32 or offset + size > data_len:
            raise FerricStoreError("protocol compact value expected binary")
        return data[offset : offset + size], offset + size
    if tag == 5:
        if offset + 4 > data_len:
            raise FerricStoreError("protocol list is truncated")
        count = _COMPACT_U32.unpack_from(data, offset)[0]
        if count == 0:
            return [], offset + 4
        return _decode_value_at(data, offset - 1)
    if tag == 6:
        if offset + 4 > data_len:
            raise FerricStoreError("protocol map is truncated")
        count = _COMPACT_U32.unpack_from(data, offset)[0]
        if count == 0:
            return {}, offset + 4
        return _decode_value_at(data, offset - 1)
    if tag == 7:
        if offset + 8 > data_len:
            raise FerricStoreError("protocol float is truncated")
        return _COMPACT_F64.unpack_from(data, offset)[0], offset + 8
    return _decode_value_at(data, offset - 1)


def _read_custom_flow_record_list(data: bytes, offset: int) -> tuple[list[dict[bytes, Any]], int]:
    if data[offset] != _COMPACT_FLOW_RECORD_LIST:
        raise FerricStoreError("protocol compact value expected Flow record list")
    offset += 1
    data_len = len(data)
    if offset + 4 > data_len:
        raise FerricStoreError("protocol compact Flow record list is truncated")
    count = _COMPACT_U32.unpack_from(data, offset)[0]
    offset += 4
    records: list[dict[bytes, Any] | None] = [None] * count
    for index in range(count):
        record, offset = _read_custom_flow_record(data, offset)
        records[index] = record
    return cast(list[dict[bytes, Any]], records), offset


def _decode_value_at(data: bytes, offset: int) -> tuple[Any, int]:
    _require_available(data, offset, 1)
    tag = data[offset]
    offset += 1
    if tag == 0:
        return None, offset
    if tag == 1:
        return True, offset
    if tag == 2:
        return False, offset
    if tag == 3:
        _require_available(data, offset, 8)
        return struct.unpack_from(">q", data, offset)[0], offset + 8
    if tag == 4:
        return _read_compact_binary(data, offset)
    if tag == 5:
        count = _read_u32(data, offset)
        offset += 4
        values = []
        for _ in range(count):
            value, offset = _decode_value_at(data, offset)
            values.append(value)
        return values, offset
    if tag == 6:
        count = _read_u32(data, offset)
        offset += 4
        values: dict[bytes, Any] = {}
        for _ in range(count):
            key, offset = _read_compact_binary(data, offset)
            value, offset = _decode_value_at(data, offset)
            values[key] = value
        return values, offset
    if tag == 7:
        _require_available(data, offset, 8)
        return struct.unpack_from(">d", data, offset)[0], offset + 8
    raise FerricStoreError("protocol value has unknown tag")


def _try_decode_claim_jobs_compact(data: bytes, offset: int = 0) -> list[list[Any]] | None:
    try:
        if data[offset] != 5:
            return None
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        items: list[list[Any]] = []
        for _ in range(count):
            if data[offset] != 5:
                return None
            offset += 1
            width = _read_u32(data, offset)
            offset += 4
            if width != 4:
                return None
            id_value, offset = _read_tagged_binary(data, offset)
            partition, offset = _read_tagged_binary(data, offset)
            lease, offset = _read_tagged_binary(data, offset)
            fencing, offset = _read_tagged_i64(data, offset)
            items.append([id_value, partition, lease, fencing])
        if offset != len(data):
            return None
        return items
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_binary_list(data: bytes, offset: int = 0) -> list[bytes] | None:
    try:
        if data[offset] != 5:
            return None
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        items: list[bytes] = []
        for _ in range(count):
            item, offset = _read_tagged_binary(data, offset)
            items.append(item)
        if offset != len(data):
            return None
        return items
    except (IndexError, struct.error, FerricStoreError):
        return None


def _read_u32(data: bytes, offset: int) -> int:
    _require_available(data, offset, 4)
    value: int = _COMPACT_U32.unpack_from(data, offset)[0]
    return value


def _read_tagged_binary(data: bytes, offset: int) -> tuple[bytes, int]:
    if data[offset] != 4:
        raise FerricStoreError("protocol fast path expected binary")
    offset += 1
    size = _read_u32(data, offset)
    offset += 4
    _require_available(data, offset, size)
    return data[offset : offset + size], offset + size


def _read_tagged_i64(data: bytes, offset: int) -> tuple[int, int]:
    if data[offset] != 3:
        raise FerricStoreError("protocol fast path expected integer")
    offset += 1
    _require_available(data, offset, 8)
    return struct.unpack_from(">q", data, offset)[0], offset + 8


def _read_compact_binary(data: bytes, offset: int) -> tuple[bytes, int]:
    size = _read_u32(data, offset)
    offset += 4
    if size == _NULL_U32:
        raise FerricStoreError("protocol compact value expected binary")
    _require_available(data, offset, size)
    return data[offset : offset + size], offset + size


def _read_compact_optional_binary(data: bytes, offset: int) -> tuple[bytes | None, int]:
    size = _read_u32(data, offset)
    offset += 4
    if size == _NULL_U32:
        return None, offset
    _require_available(data, offset, size)
    return data[offset : offset + size], offset + size


def _require_available(data: bytes, offset: int, size: int) -> None:
    if len(data) - offset < size:
        raise FerricStoreError("protocol value is truncated")


def _build_basic_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    opcode = _OPCODES[name]
    if name == "PING":
        payload = {"message": args[0]} if args else {}
        return ProtocolCommand(opcode, payload, 0)
    if name == "CLIENT.SETNAME":
        return ProtocolCommand(opcode, {"name": _require_arg(args, 0, name)}, 0)
    if name == "CLIENT.INFO":
        return ProtocolCommand(opcode, {}, 0)
    if name == "GET":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)})
    if name == "SET":
        payload = {"key": _require_arg(args, 0, name), "value": _require_arg(args, 1, name)}
        payload.update(_kv_set_options(args[2:]))
        return ProtocolCommand(opcode, payload)
    if name == "DEL":
        return ProtocolCommand(opcode, {"keys": list(args)})
    if name == "MGET":
        compact = _compact_kv_keys_payload(args, 2)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, {"keys": list(args)})
    if name == "MSET":
        if len(args) % 2 != 0:
            raise InvalidCommandError("MSET requires key/value pairs")
        compact = _compact_kv_set_pairs_payload(args)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        pairs = [[args[idx], args[idx + 1]] for idx in range(0, len(args), 2)]
        return ProtocolCommand(opcode, {"pairs": pairs})
    if name == "CAS":
        payload = {
            "key": _require_arg(args, 0, name),
            "expected": _require_arg(args, 1, name),
            "value": _require_arg(args, 2, name),
        }
        idx = 3
        while idx < len(args):
            token = _command_token(args[idx])
            if token == "EX":
                payload["ttl"] = int(_require_arg(args, idx + 1, "EX")) * 1000
                idx += 2
            elif token == "PX":
                payload["ttl"] = int(_require_arg(args, idx + 1, "PX"))
                idx += 2
            else:
                raise InvalidCommandError(f"protocol CAS does not support option {token}")
        return ProtocolCommand(opcode, payload)
    if name in {"LOCK", "EXTEND"}:
        return ProtocolCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "owner": _require_arg(args, 1, name),
                "ttl_ms": _require_arg(args, 2, name),
            },
        )
    if name == "UNLOCK":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "owner": _require_arg(args, 1, name)},
        )
    if name == "RATELIMIT.ADD":
        payload = {
            "key": _require_arg(args, 0, name),
            "window_ms": _require_arg(args, 1, name),
            "max": _require_arg(args, 2, name),
        }
        if len(args) > 3:
            payload["count"] = _require_arg(args, 3, name)
        return ProtocolCommand(opcode, payload)
    if name == "FETCH_OR_COMPUTE":
        payload = {"key": _require_arg(args, 0, name), "ttl_ms": _require_arg(args, 1, name)}
        if len(args) > 2:
            payload["hint"] = _require_arg(args, 2, name)
        return ProtocolCommand(opcode, payload)
    if name == "FETCH_OR_COMPUTE_RESULT":
        return ProtocolCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "value": _require_arg(args, 1, name),
                "ttl_ms": _require_arg(args, 2, name),
            },
        )
    if name == "FETCH_OR_COMPUTE_ERROR":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "message": _require_arg(args, 1, name)},
        )
    if name == "HSET":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "fields": _field_value_map(name, args[1:])},
        )
    if name == "HGET":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "field": _require_arg(args, 1, name)},
        )
    if name == "HMGET":
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "fields": list(args[1:])},
        )
    if name == "HGETALL":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)})
    if name in {"LPUSH", "RPUSH"}:
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "values": list(args[1:])},
        )
    if name in {"LPOP", "RPOP"}:
        payload = {"key": _require_arg(args, 0, name)}
        if len(args) > 1:
            payload["count"] = _int_arg(args[1], name)
        return ProtocolCommand(opcode, payload)
    if name == "LRANGE":
        return ProtocolCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "start": _int_arg(_require_arg(args, 1, name), name),
                "stop": _int_arg(_require_arg(args, 2, name), name),
            },
        )
    if name in {"SADD", "SREM"}:
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "members": list(args[1:])},
        )
    if name == "SMEMBERS":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)})
    if name == "SISMEMBER":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "member": _require_arg(args, 1, name)},
        )
    if name == "ZADD":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "items": _zadd_items(args[1:])},
        )
    if name == "ZREM":
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "members": list(args[1:])},
        )
    if name == "ZRANGE":
        return ProtocolCommand(opcode, _zrange_payload(args))
    if name == "ZSCORE":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "member": _require_arg(args, 1, name)},
        )
    if name in {"CLUSTER.KEYSLOT", "FERRICSTORE.KEY_INFO"}:
        key = _require_arg(args, 0, name)
        return ProtocolCommand(opcode, {"key": key, "args": [key]})
    if name in {
        "CLUSTER.HEALTH",
        "CLUSTER.STATS",
        "CLUSTER.SLOTS",
        "CLUSTER.STATUS",
        "CLUSTER.ROLE",
        "CLUSTER.LEAVE",
        "FERRICSTORE.HOTNESS",
        "FERRICSTORE.METRICS",
        "FERRICSTORE.BLOBGC",
    }:
        return ProtocolCommand(opcode, {"args": list(args)})
    if name in {
        "CLUSTER.JOIN",
        "CLUSTER.FAILOVER",
        "CLUSTER.PROMOTE",
        "CLUSTER.DEMOTE",
        "FERRICSTORE.CONFIG",
    }:
        return ProtocolCommand(opcode, {"args": list(args)})
    raise InvalidCommandError(f"FerricStore protocol transport does not support command {name}")


def _build_flow_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    opcode = _OPCODES[name]
    if name == "FLOW.CREATE":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CREATE_MANY":
        payload = _flow_create_many_payload(args)
        compact = _compact_flow_create_many_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CLAIM_DUE":
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        _collapse_states(payload)
        compact = _compact_flow_claim_due_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name in {"FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL", "FLOW.EXTEND_LEASE"}:
        payload = {"id": _require_arg(args, 0, name), "lease_token": _require_arg(args, 1, name)}
        payload.update(_option_map(args[2:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.TRANSITION":
        payload = {
            "id": _require_arg(args, 0, name),
            "from_state": _require_arg(args, 1, name),
            "to_state": _require_arg(args, 2, name),
        }
        payload.update(_option_map(args[3:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.STEP_CONTINUE":
        payload = {
            "id": _require_arg(args, 0, name),
            "lease_token": _require_arg(args, 1, name),
            "from_state": _require_arg(args, 2, name),
            "to_state": _require_arg(args, 3, name),
        }
        payload.update(_option_map(args[4:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.START_AND_CLAIM":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.RUN_STEPS_MANY":
        payload = _option_map(args)
        if "items" not in payload:
            raise InvalidCommandError("FLOW.RUN_STEPS_MANY requires ITEMS")
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CANCEL":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in {"FLOW.COMPLETE_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY"}:
        payload = _flow_claimed_many_payload(name, args)
        if name in {"FLOW.COMPLETE_MANY", "FLOW.FAIL_MANY"}:
            compact = _compact_flow_claimed_many_payload(
                payload,
                request_kind=_COMPACT_FLOW_COMPLETE_MANY_REQUEST,
                ok_request_kind=_COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
                extra_allowed={"terminal_local_only"} if name == "FLOW.COMPLETE_MANY" else set(),
            )
            if compact is not None:
                return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        elif name == "FLOW.RETRY_MANY":
            compact = _compact_flow_claimed_many_payload(
                payload,
                request_kind=_COMPACT_FLOW_RETRY_MANY_REQUEST,
                ok_request_kind=_COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
                extra_allowed={"run_at_ms"},
            )
            if compact is not None:
                return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.TRANSITION_MANY":
        payload = {
            "from_state": _require_arg(args, 1, name),
            "to_state": _require_arg(args, 2, name),
        }
        payload.update(_flow_fenced_many_payload(name, args[0:1] + args[3:], include_lease=True))
        compact = _compact_flow_transition_many_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CANCEL_MANY":
        payload = _flow_fenced_many_payload(name, args, include_lease=False)
        compact = _compact_flow_cancel_many_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name in {
        "FLOW.GET",
        "FLOW.HISTORY",
        "FLOW.REWIND",
        "FLOW.BY_PARENT",
        "FLOW.BY_ROOT",
        "FLOW.BY_CORRELATION",
        "FLOW.SIGNAL",
    }:
        key = {
            "FLOW.BY_PARENT": "parent_id",
            "FLOW.BY_ROOT": "root_id",
            "FLOW.BY_CORRELATION": "correlation_id",
        }.get(name, "id")
        payload = {key: _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in {"FLOW.LIST", "FLOW.TERMINALS", "FLOW.FAILURES", "FLOW.INFO", "FLOW.STUCK"}:
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        if name == "FLOW.LIST":
            compact = _compact_flow_list_payload(payload)
            if compact is not None:
                return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.VALUE.PUT":
        payload = {"value": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.VALUE.MGET":
        refs, options = _split_refs_and_options(args)
        payload = {"refs": refs}
        payload.update(options)
        compact = _compact_flow_value_mget_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name in {"FLOW.POLICY.SET", "FLOW.POLICY.GET", "FLOW.RECLAIM"}:
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.SPAWN_CHILDREN":
        return ProtocolCommand(opcode, _flow_spawn_children_payload(args))
    if name == "FLOW.RETENTION_CLEANUP":
        return ProtocolCommand(opcode, _option_map(args))
    raise InvalidCommandError(f"FerricStore protocol transport does not support command {name}")


def _flow_create_many_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, "FLOW.CREATE_MANY"))
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if wire_partition not in {"AUTO", "MIXED", "None", "none"}:
        payload["partition_key"] = args[0]

    token = _command_token(args[item_token])
    if token == "ITEMS_EXT":
        payload["items"] = _parse_create_items_ext(
            args[item_token + 2 :], wire_partition == "MIXED"
        )
    else:
        payload["items"] = _parse_create_items(args[item_token + 1 :], wire_partition == "MIXED")
    return payload


def _flow_spawn_children_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    parent_id = _require_arg(args, 0, "FLOW.SPAWN_CHILDREN")
    item_token = _find_item_token(args, 1)
    payload = {"id": parent_id}
    payload.update(_option_map(args[1:item_token]))

    token = _command_token(args[item_token])
    if token == "ITEMS_EXT":
        payload["children"] = _parse_spawn_children_ext(args[item_token + 2 :])
    else:
        mixed = item_token + 1 < len(args) and _command_token(args[item_token + 1]) == "MIXED"
        start = item_token + 2 if mixed else item_token + 1
        payload["children"] = _parse_spawn_children(args[start:], mixed)
    return payload


def _parse_spawn_children(values: tuple[Any, ...], mixed: bool) -> list[dict[str, Any]]:
    width = 4 if mixed else 3
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.SPAWN_CHILDREN ITEMS has wrong child width")
    children = []
    for idx in range(0, len(values), width):
        if mixed:
            children.append(
                {
                    "id": values[idx],
                    "partition_key": values[idx + 1],
                    "type": values[idx + 2],
                    "payload": values[idx + 3],
                }
            )
        else:
            children.append(
                {
                    "id": values[idx],
                    "type": values[idx + 1],
                    "payload": values[idx + 2],
                }
            )
    return children


def _parse_spawn_children_ext(values: tuple[Any, ...]) -> list[dict[str, Any]]:
    children = []
    idx = 0
    while idx < len(values):
        child = {
            "id": values[idx],
            "type": values[idx + 2],
            "payload": values[idx + 3],
        }
        partition = values[idx + 1]
        if partition != "-":
            child["partition_key"] = partition
        idx += 4

        value_count = int(values[idx])
        idx += 1
        child_values = {}
        for _ in range(value_count):
            child_values[_text(values[idx])] = values[idx + 1]
            idx += 2
        if child_values:
            child["values"] = child_values

        ref_count = int(values[idx])
        idx += 1
        child_refs = {}
        for _ in range(ref_count):
            child_refs[_text(values[idx])] = values[idx + 1]
            idx += 2
        if child_refs:
            child["value_refs"] = child_refs

        children.append(child)
    return children


def _flow_claimed_many_payload(name: str, args: tuple[Any, ...]) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, name))
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if wire_partition != "MIXED":
        payload["partition_key"] = args[0]
    payload["items"] = _parse_claimed_items(args[item_token + 1 :], wire_partition == "MIXED")
    return payload


def _flow_fenced_many_payload(
    name: str, args: tuple[Any, ...], *, include_lease: bool
) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, name))
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if wire_partition != "MIXED":
        payload["partition_key"] = args[0]
    payload["items"] = _parse_fenced_items(
        args[item_token + 1 :],
        wire_partition == "MIXED",
        include_lease=include_lease,
    )
    return payload


def _option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "NOPAYLOAD":
            payload["payload"] = False
            idx += 1
            continue
        if token == "PAYLOAD":
            if idx + 1 >= len(args) or (
                isinstance(args[idx + 1], str) and _command_token(args[idx + 1]) in _FIELD_NAMES
            ):
                payload["payload"] = True
                idx += 1
            else:
                payload["payload"] = _require_arg(args, idx + 1, "PAYLOAD")
                idx += 2
            continue
        if token == "PARTITIONS":
            count = int(_require_arg(args, idx + 1, "PARTITIONS"))
            payload["partition_keys"] = list(args[idx + 2 : idx + 2 + count])
            idx += 2 + count
            continue
        if token == "STATE":
            value = _require_arg(args, idx + 1, "STATE")
            if "states" in payload:
                payload["states"].append(value)
            elif "state" in payload:
                payload["states"] = [payload.pop("state"), value]
            else:
                payload["state"] = value
            idx += 2
            continue
        if token == "IF_STATE":
            value = _require_arg(args, idx + 1, "IF_STATE")
            if "if_state" in payload:
                existing = payload["if_state"]
                if isinstance(existing, list):
                    existing.append(value)
                else:
                    payload["if_state"] = [existing, value]
            else:
                payload["if_state"] = value
            idx += 2
            continue
        if token == "RETURN":
            return_value = _text(_require_arg(args, idx + 1, "RETURN"))
            if return_value in {"JOBS_COMPACT", "JOBS_COMPACT_STATE"}:
                payload["return"] = return_value.lower()
            else:
                payload["return"] = return_value.lower()
            idx += 2
            continue
        if token == "VALUE":
            name = _text(_require_arg(args, idx + 1, "VALUE"))
            value = _require_arg(args, idx + 2, "VALUE")
            payload.setdefault("values", {})[name] = value
            idx += 3
            continue
        if token == "VALUE_REF":
            name = _text(_require_arg(args, idx + 1, "VALUE_REF"))
            ref = _require_arg(args, idx + 2, "VALUE_REF")
            payload.setdefault("value_refs", {})[name] = ref
            idx += 3
            continue
        if token in {"DROP_VALUE", "OVERRIDE_VALUE"}:
            list_field = "drop_values" if token == "DROP_VALUE" else "override_values"
            payload.setdefault(list_field, []).append(_text(_require_arg(args, idx + 1, token)))
            idx += 2
            continue

        mapped_field = _FIELD_NAMES.get(token)
        if mapped_field is None:
            raise InvalidCommandError(
                f"FerricStore protocol transport does not support option {token}"
            )
        value = _require_arg(args, idx + 1, token)
        payload[mapped_field] = _coerce_bool(value) if mapped_field in _BOOL_FIELDS else value
        idx += 2
    return payload


def _kv_set_options(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "EX":
            payload["ttl"] = int(_require_arg(args, idx + 1, "EX")) * 1000
            idx += 2
        elif token == "PX":
            payload["ttl"] = int(_require_arg(args, idx + 1, "PX"))
            idx += 2
        elif token in {"NX", "XX", "GET", "KEEPTTL"}:
            payload[token.lower()] = True
            idx += 1
        else:
            raise InvalidCommandError(f"protocol SET does not support option {token}")
    return payload


def _field_value_map(command: str, args: tuple[Any, ...]) -> dict[Any, Any]:
    if not args or len(args) % 2 != 0:
        raise InvalidCommandError(f"{command} requires field/value pairs")
    return {args[idx]: args[idx + 1] for idx in range(0, len(args), 2)}


def _require_values(command: str, args: tuple[Any, ...], start: int) -> None:
    _require_arg(args, 0, command)
    if len(args) <= start:
        raise InvalidCommandError(f"{command} requires at least one value")


def _int_arg(value: Any, command: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidCommandError(f"{command} requires integer arguments") from exc


def _zadd_items(args: tuple[Any, ...]) -> list[list[Any]]:
    if not args or len(args) % 2 != 0:
        raise InvalidCommandError("ZADD requires score/member pairs")
    items: list[list[Any]] = []
    for idx in range(0, len(args), 2):
        try:
            score = float(args[idx])
        except (TypeError, ValueError) as exc:
            raise InvalidCommandError("ZADD score must be numeric") from exc
        items.append([score, args[idx + 1]])
    return items


def _zrange_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    payload = {
        "key": _require_arg(args, 0, "ZRANGE"),
        "start": _int_arg(_require_arg(args, 1, "ZRANGE"), "ZRANGE"),
        "stop": _int_arg(_require_arg(args, 2, "ZRANGE"), "ZRANGE"),
    }
    idx = 3
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "WITHSCORES":
            payload["withscores"] = True
            idx += 1
        else:
            raise InvalidCommandError(f"protocol ZRANGE does not support option {token}")
    return payload


def _parse_create_items(values: tuple[Any, ...], mixed: bool) -> list[list[Any]]:
    width = 3 if mixed else 2
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.CREATE_MANY ITEMS has wrong item width")
    items = []
    for idx in range(0, len(values), width):
        if mixed:
            items.append([values[idx], values[idx + 1], values[idx + 2]])
        else:
            items.append([values[idx], values[idx + 1]])
    return items


def _parse_create_items_ext(values: tuple[Any, ...], mixed: bool) -> list[dict[str, Any]]:
    items = []
    idx = 0
    while idx < len(values):
        item = {"id": values[idx], "payload": values[idx + 2]}
        partition = values[idx + 1]
        if mixed or partition != "-":
            item["partition_key"] = partition
        idx += 3
        value_count = int(values[idx])
        idx += 1
        item_values = {}
        for _ in range(value_count):
            item_values[_text(values[idx])] = values[idx + 1]
            idx += 2
        if item_values:
            item["values"] = item_values
        ref_count = int(values[idx])
        idx += 1
        item_refs = {}
        for _ in range(ref_count):
            item_refs[_text(values[idx])] = values[idx + 1]
            idx += 2
        if item_refs:
            item["value_refs"] = item_refs
        items.append(item)
    return items


def _parse_claimed_items(values: tuple[Any, ...], mixed: bool) -> list[list[Any]]:
    width = 4 if mixed else 3
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.*_MANY ITEMS has wrong claimed item width")
    items = []
    for idx in range(0, len(values), width):
        if mixed:
            items.append([values[idx], values[idx + 1], values[idx + 2], values[idx + 3]])
        else:
            items.append([values[idx], values[idx + 1], values[idx + 2]])
    return items


def _parse_fenced_items(
    values: tuple[Any, ...], mixed: bool, *, include_lease: bool
) -> list[dict[str, Any]]:
    width = (4 if mixed else 3) if include_lease else (3 if mixed else 2)
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.*_MANY ITEMS has wrong fenced item width")
    items = []
    for idx in range(0, len(values), width):
        item = {"id": values[idx], "fencing_token": values[idx + (2 if mixed else 1)]}
        if mixed:
            item["partition_key"] = values[idx + 1]
        if include_lease:
            lease = values[idx + (3 if mixed else 2)]
            if lease != "-":
                item["lease_token"] = lease
        items.append(item)
    return items


def _split_refs_and_options(args: tuple[Any, ...]) -> tuple[list[Any], dict[str, Any]]:
    refs: list[Any] = []
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token in {"MAX_BYTES", "MAXBYTES"}:
            return refs, {"max_bytes": _require_arg(args, idx + 1, token)}
        refs.append(args[idx])
        idx += 1
    return refs, {}


def _find_item_token(args: tuple[Any, ...], start: int) -> int:
    for idx in range(start, len(args)):
        if _command_token(args[idx]) in {"ITEMS", "ITEMS_EXT"}:
            return idx
    raise InvalidCommandError("FLOW many command requires ITEMS or ITEMS_EXT")


def _collapse_states(payload: dict[str, Any]) -> None:
    states = payload.get("states")
    if isinstance(states, list) and len(states) == 1:
        payload["state"] = states[0]
        del payload["states"]


def _lane_for_opcode(opcode: int) -> int:
    return 0 if opcode in _CONTROL_OPCODES else 1


def _command_name(value: Any) -> str:
    return _text(value).upper()


def _command_token(value: Any) -> str:
    return _text(value).upper()


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _require_arg(args: tuple[Any, ...], idx: int, command: str) -> Any:
    if idx >= len(args):
        raise InvalidCommandError(f"{command} is missing argument {idx + 1}")
    return args[idx]


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, bytes):
        value = value.decode()
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return bool(value)


def _encode_binary(value: bytes) -> bytes:
    return b"\x04" + struct.pack(">I", len(value)) + value


def _decode_binary(data: bytes) -> tuple[bytes, bytes]:
    _require_len(data, 4)
    size = struct.unpack(">I", data[:4])[0]
    rest = data[4:]
    _require_len(rest, size)
    return rest[:size], rest[size:]


def _require_len(data: bytes, size: int) -> None:
    if len(data) < size:
        raise FerricStoreError("protocol value is truncated")


def _key_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    return str(value).encode()


def _map_get(mapping: Any, key: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    return mapping.get(key) if key in mapping else mapping.get(key.encode())


def _optional_text(mapping: Any, key: str) -> str | None:
    value = _map_get(mapping, key)
    if value is None:
        return None
    return _text(value)


def _optional_int(mapping: Any, key: str) -> int | None:
    value = _map_get(mapping, key)
    if value is None:
        return None
    return int(value)


def _error_message(value: Any) -> str:
    message = _optional_text(value, "message")
    if message is not None:
        return message
    return _text(value)


def _extract_traced_value(value: Any) -> tuple[Any, dict[str, Any]]:
    if not isinstance(value, dict):
        return value, {}

    unwrapped = _map_get(value, "value")
    trace = _map_get(value, "trace")
    if trace is None:
        return value, {}
    if not isinstance(trace, dict):
        trace = {}
    return unwrapped, _normalize_trace_map(trace)


def _normalize_trace_map(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(key, bytes):
                trace_key = key.decode("utf-8", errors="replace")
            else:
                trace_key = str(key)
            normalized[trace_key] = _normalize_trace_map(item)
        return normalized
    return value


def _response_value(response: ProtocolResponse) -> Any:
    if response.status == _STATUS_OK:
        return response.value

    message = _error_message(response.value)
    if response.status == _STATUS_BUSY:
        raise OverloadedError(
            message,
            raw=response.value,
            retry_after_ms=_optional_int(response.value, "retry_after_ms"),
            reason=_optional_text(response.value, "reason"),
        )
    raise FerricStoreError(message, raw=response.value)


def _batch_item_value(item: Any) -> Any:
    if isinstance(item, list) and len(item) == 2:
        status = _status_text(item[0]) or "error"
        value = item[1]
        raw = item
    elif isinstance(item, tuple) and len(item) == 2:
        status = _status_text(item[0]) or "error"
        value = item[1]
        raw = item
    else:
        if not isinstance(item, dict):
            raise FerricStoreError("protocol PIPELINE item is not a map or status pair", raw=item)

        status = _optional_text(item, "status") or "error"
        value = _map_get(item, "value")
        raw = item

    if status == "ok":
        return value
    message = _error_message(value)
    if status == "busy":
        raise OverloadedError(message, raw=raw)
    raise FerricStoreError(message, raw=raw)


def _pipeline_pair_list(value: list[Any]) -> bool:
    if not value:
        return True
    first = value[0]
    if isinstance(first, (list, tuple)) and len(first) == 2:
        return _status_text(first[0]) is not None
    if isinstance(first, dict):
        return _optional_text(first, "status") is not None
    return False


def _ok_scalar(value: Any) -> bool:
    if isinstance(value, bytes):
        return value.lower() == b"ok"
    if isinstance(value, str):
        return value.lower() == "ok"
    return False


def _status_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode()
    return None


def _normalize_protocol_url_kwargs(kwargs: dict[str, Any]) -> None:
    if "socket_timeout" in kwargs and "timeout" not in kwargs:
        kwargs["timeout"] = kwargs.pop("socket_timeout")
    if "health_check_interval" in kwargs and "heartbeat_interval" not in kwargs:
        kwargs["heartbeat_interval"] = kwargs.pop("health_check_interval")
    for redis_only in (
        "decode_responses",
        "max_connections",
        "protocol",
        "retry_on_timeout",
    ):
        kwargs.pop(redis_only, None)
