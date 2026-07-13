from __future__ import annotations

import struct
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ferricstore.errors import (
    InvalidCommandError,
)
from ferricstore.flow_options import flow_payload_is_flag
from ferricstore.protocol_codec import (
    encode_value,
)
from ferricstore.protocol_common import (
    _coerce_bool,
    _command_name,
    _command_token,
    _lane_for_opcode,
    _map_get,
    _require_arg,
    _text,
    _text_or_none,
)
from ferricstore.protocol_constants import (
    _BOOL_FIELDS,
    _COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
    _COMPACT_FLOW_COMPLETE_MANY_REQUEST,
    _COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
    _COMPACT_FLOW_RETRY_MANY_REQUEST,
    _COMPACT_HGET_PIPELINE_MODE,
    _COMPACT_HGETALL_PIPELINE_MODE,
    _COMPACT_HMGET_PIPELINE_MODE,
    _COMPACT_HSET_PIPELINE_MODE,
    _COMPACT_LPUSH_PIPELINE_MODE,
    _COMPACT_LRANGE_PIPELINE_MODE,
    _COMPACT_PIPELINE_HEADER,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_RPUSH_PIPELINE_MODE,
    _COMPACT_SADD_PIPELINE_MODE,
    _COMPACT_SISMEMBER_PIPELINE_MODE,
    _COMPACT_SMEMBERS_PIPELINE_MODE,
    _COMPACT_ZADD_PIPELINE_MODE,
    _COMPACT_ZRANGE_PIPELINE_MODE,
    _COMPACT_ZSCORE_PIPELINE_MODE,
    _FIELD_NAMES,
    _FLAG_CUSTOM_PAYLOAD,
    _FLOW_POLICY_FIELD_NAMES,
    _HEADER,
    _MAGIC,
    _OP_COMMAND_EXEC,
    _OP_FLOW_COMPLETE_MANY,
    _OP_FLOW_CREATE_MANY,
    _OP_PIPELINE,
    _OPCODES,
    _REQUEST_VERSION,
    ProtocolCommand,
)
from ferricstore.protocol_flow_codec import (
    _compact_binary,
    _compact_flow_cancel_many_payload,
    _compact_flow_claim_due_payload,
    _compact_flow_claimed_many_payload,
    _compact_flow_complete_many_payload,
    _compact_flow_create_many_payload,
    _compact_flow_list_payload,
    _compact_flow_transition_many_payload,
    _compact_flow_value_mget_payload,
    _compact_flow_value_put_payload,
    _compact_optional_binary,
    _maybe_bytes,
    _ok_on_success_return_mode,
    _optional_bytes,
    _raw_int,
)
from ferricstore.protocol_pipeline_codec import (
    _compact_flow_get_pipeline_payload_from_raw,
    _compact_flow_history_pipeline_payload_from_raw,
    _compact_kv_keys_payload,
    _compact_kv_set_pairs_payload,
    _compact_mixed_pipeline_payload_from_raw,
    _compact_pipeline_hmget_payload_from_raw,
    _compact_pipeline_hset_payload_from_raw,
    _compact_pipeline_keys_payload_from_raw,
    _compact_pipeline_range_payload_from_raw,
    _compact_pipeline_set_payload_from_raw,
    _compact_pipeline_two_binary_payload_from_raw,
    _compact_pipeline_zadd_payload_from_raw,
)


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


def build_protocol_command(*args: Any) -> ProtocolCommand:
    if not args:
        raise InvalidCommandError("protocol command requires command name")

    name = _command_name(args[0])
    if name not in _OPCODES:
        return _command_exec_protocol_command(name, args[1:])

    if name == "COMMAND_EXEC":
        raw_name = _command_name(_require_arg(args, 1, name))
        return _command_exec_protocol_command(raw_name, args[2:])

    if name in {
        "HELLO",
        "AUTH",
        "GET",
        "SET",
        "DEL",
        "MGET",
        "MSET",
        "PING",
        "OPTIONS",
        "ROUTE",
        "SHARDS",
        "BACKPRESSURE",
        "QUIT",
        "STARTUP",
        "WINDOW_UPDATE",
        "ROUTE_BATCH",
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
        tuple[
            tuple[bytes, bytes, int, bytes | None],
            list[tuple[bytes, bytes | None, bytes, int, int]],
        ]
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

        current_items.append((flow_id, partition_key, lease_token, fencing_token, now_ms))

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

        current_items.append((flow_id, partition_key, item_payload, now_ms))

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
        cast(bytes | None, partition_key),
        cast(bytes | None, item_payload),
        lease_ms,
        now_ms,
        jobs_compact,
    )


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


def _build_basic_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    opcode = _OPCODES[name]
    if name in {"HELLO", "STARTUP", "WINDOW_UPDATE"}:
        return ProtocolCommand(opcode, _generic_option_map(args), 0)
    if name == "AUTH":
        payload = {
            "username": _require_arg(args, 0, name),
            "password": _require_arg(args, 1, name),
        }
        return ProtocolCommand(opcode, payload, 0)
    if name == "PING":
        payload = {"message": args[0]} if args else {}
        return ProtocolCommand(opcode, payload, 0)
    if name == "OPTIONS":
        return ProtocolCommand(opcode, {}, 0)
    if name == "ROUTE":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)}, 0)
    if name == "ROUTE_BATCH":
        return ProtocolCommand(opcode, {"keys": list(args)}, 0)
    if name == "SHARDS":
        return ProtocolCommand(opcode, {}, 0)
    if name == "BACKPRESSURE":
        return ProtocolCommand(opcode, {}, 0)
    if name == "QUIT":
        return ProtocolCommand(opcode, {}, 0)
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
    return _command_exec_protocol_command(name, args)


def _build_flow_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    opcode = _OPCODES[name]
    if _has_flow_command_only_option(name, args):
        return _command_exec_protocol_command(name, args)
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
    if name in {
        "FLOW.LIST",
        "FLOW.STATS",
        "FLOW.TERMINALS",
        "FLOW.FAILURES",
        "FLOW.INFO",
        "FLOW.STUCK",
    }:
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        if name == "FLOW.LIST":
            compact = _compact_flow_list_payload(payload)
            if compact is not None:
                return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.ATTRIBUTES":
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.ATTRIBUTE_VALUES":
        payload = {
            "type": _require_arg(args, 0, name),
            "attribute": _require_arg(args, 1, name),
        }
        payload.update(_option_map(args[2:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.SEARCH":
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        _normalize_flow_search_state_meta_payload(payload)
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
        if name == "FLOW.POLICY.SET":
            payload.update(_flow_policy_set_option_map(args[1:]))
        else:
            payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in {
        "FLOW.SCHEDULE.CREATE",
        "FLOW.SCHEDULE.GET",
        "FLOW.SCHEDULE.FIRE",
        "FLOW.SCHEDULE.PAUSE",
        "FLOW.SCHEDULE.RESUME",
        "FLOW.SCHEDULE.DELETE",
        "FLOW.EFFECT.RESERVE",
        "FLOW.EFFECT.CONFIRM",
        "FLOW.EFFECT.FAIL",
        "FLOW.EFFECT.COMPENSATE",
        "FLOW.EFFECT.GET",
        "FLOW.GOVERNANCE.LEDGER",
        "FLOW.APPROVAL.REQUEST",
        "FLOW.APPROVAL.APPROVE",
        "FLOW.APPROVAL.REJECT",
        "FLOW.APPROVAL.GET",
    }:
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in {
        "FLOW.CIRCUIT.OPEN",
        "FLOW.CIRCUIT.CLOSE",
        "FLOW.CIRCUIT.GET",
        "FLOW.BUDGET.RESERVE",
        "FLOW.BUDGET.COMMIT",
        "FLOW.BUDGET.RELEASE",
        "FLOW.BUDGET.GET",
        "FLOW.LIMIT.LEASE",
        "FLOW.LIMIT.SPEND",
        "FLOW.LIMIT.RELEASE",
        "FLOW.LIMIT.GET",
    }:
        payload = {"scope": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in {
        "FLOW.SCHEDULE.FIRE_DUE",
        "FLOW.SCHEDULE.LIST",
        "FLOW.APPROVAL.LIST",
        "FLOW.GOVERNANCE.OVERVIEW",
        "FLOW.BUDGET.LIST",
        "FLOW.LIMIT.LIST",
    }:
        return ProtocolCommand(opcode, _option_map(args))
    if name == "FLOW.SPAWN_CHILDREN":
        return ProtocolCommand(opcode, _flow_spawn_children_payload(args))
    if name == "FLOW.RETENTION_CLEANUP":
        return ProtocolCommand(opcode, _option_map(args))
    raise InvalidCommandError(f"FerricStore protocol transport does not support command {name}")


def _has_flow_command_only_option(name: str, args: tuple[Any, ...]) -> bool:
    command_only_options = {"INDEXED_STATE_META"}
    if name != "FLOW.SEARCH":
        command_only_options.add("STATE_META")
    return any(_command_token(arg) in command_only_options for arg in args)


def _normalize_flow_search_state_meta_payload(payload: dict[str, Any]) -> None:
    state_meta = payload.get("state_meta")
    if not isinstance(state_meta, dict) or not state_meta:
        return
    if all(isinstance(value, dict) for value in state_meta.values()):
        return

    state = payload.get("state")
    if state is None:
        raise InvalidCommandError(
            "FLOW.SEARCH STATE_META filters require STATE or nested state metadata"
        )
    payload["state_meta"] = {_text(state): state_meta}


def _flow_policy_set_option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    states: dict[str, dict[str, Any]] = {}
    current_state: Any = None
    current_args: list[Any] = []

    def flush_current() -> None:
        nonlocal current_args
        if current_state is None:
            payload.update(_flow_policy_option_map(tuple(current_args)))
        else:
            states[_text(current_state)] = _flow_policy_option_map(tuple(current_args))
        current_args = []

    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "STATE":
            flush_current()
            current_state = _require_arg(args, idx + 1, "STATE")
            idx += 2
            continue
        current_args.extend([args[idx], _require_arg(args, idx + 1, token)])
        idx += 2

    flush_current()
    if states:
        payload["states"] = states
    return payload


def _flow_policy_option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        mapped_field = _FLOW_POLICY_FIELD_NAMES.get(token) or _FIELD_NAMES.get(token)
        if mapped_field is None:
            raise InvalidCommandError(
                f"FerricStore protocol transport does not support option {token}"
            )
        payload[mapped_field] = _require_arg(args, idx + 1, token)
        idx += 2
    return payload


def _command_exec_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    command_args = list(args)
    payload: dict[str, Any] = {"command": name, "args": command_args}

    if len(command_args) >= 2 and _command_token(command_args[-2]) == "REQUEST_CONTEXT":
        request_context = _normalize_request_context(command_args[-1])
        payload["args"] = command_args[:-2]
        if request_context:
            payload["request_context"] = request_context

    return ProtocolCommand(_OP_COMMAND_EXEC, payload, 1)


def _normalize_request_context(context: Any) -> dict[str, Any] | None:
    if not isinstance(context, Mapping):
        return None

    payload: dict[str, Any] = {}
    subject = _text_or_none(_map_get(context, "subject"))
    tenant = _text_or_none(_map_get(context, "tenant"))
    scopes = _normalize_request_context_scopes(_map_get(context, "scopes"))

    if subject:
        payload["subject"] = subject
    if tenant:
        payload["tenant"] = tenant
    if scopes:
        payload["scopes"] = scopes
    return payload or None


def _normalize_request_context_scopes(scopes: Any) -> list[str]:
    if scopes is None:
        return []
    if isinstance(scopes, (str, bytes)):
        values = _text(scopes).split()
    elif isinstance(scopes, Sequence):
        values = [_text(value) for value in scopes if value is not None and value != ""]
    else:
        return []
    return list(dict.fromkeys(value for value in values if value))


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


def _generic_option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    if len(args) % 2 != 0:
        raise InvalidCommandError("protocol options require name/value pairs")
    return {_command_token(args[idx]).lower(): args[idx + 1] for idx in range(0, len(args), 2)}


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
            if flow_payload_is_flag(args, idx):
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
        if token in {"ATTRIBUTE", "ATTRIBUTE_MERGE"}:
            map_field = "attributes" if token == "ATTRIBUTE" else "attributes_merge"
            name = _text(_require_arg(args, idx + 1, token))
            value = _require_arg(args, idx + 2, token)
            payload.setdefault(map_field, {})[name] = value
            idx += 3
            continue
        if token == "STATE_META":
            name = _text(_require_arg(args, idx + 1, token))
            value = _require_arg(args, idx + 2, token)
            payload.setdefault("state_meta", {})[name] = value
            idx += 3
            continue
        if token == "ATTRIBUTE_DELETE":
            payload.setdefault("attributes_delete", []).append(
                _text(_require_arg(args, idx + 1, token))
            )
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


__all__ = [
    "_build_basic_protocol_command",
    "_build_flow_protocol_command",
    "_collapse_states",
    "_command_exec_protocol_command",
    "_compact_flow_complete_many_payloads_from_raw",
    "_compact_flow_create_many_payloads_from_raw",
    "_compact_flow_many_payloads_from_raw",
    "_compact_flow_signal_pipeline_payload_from_raw",
    "_compact_flow_start_and_claim_payloads_from_raw",
    "_compact_flow_step_continue_payloads_from_raw",
    "_compact_flow_value_put_payloads_from_raw",
    "_compact_pipeline_payload_from_raw",
    "_field_value_map",
    "_find_item_token",
    "_flow_claimed_many_payload",
    "_flow_create_many_payload",
    "_flow_fenced_many_payload",
    "_flow_policy_option_map",
    "_flow_policy_set_option_map",
    "_flow_spawn_children_payload",
    "_generic_option_map",
    "_has_flow_command_only_option",
    "_int_arg",
    "_kv_set_options",
    "_normalize_flow_search_state_meta_payload",
    "_normalize_request_context",
    "_normalize_request_context_scopes",
    "_option_map",
    "_parse_claimed_items",
    "_parse_compact_flow_start_and_claim_raw",
    "_parse_compact_flow_step_continue_raw",
    "_parse_compact_flow_value_put_raw",
    "_parse_create_items",
    "_parse_create_items_ext",
    "_parse_fenced_items",
    "_parse_spawn_children",
    "_parse_spawn_children_ext",
    "_require_values",
    "_split_refs_and_options",
    "_zadd_items",
    "_zrange_payload",
    "build_protocol_command",
    "encode_frame",
]
