from __future__ import annotations

import struct
from collections.abc import Callable, Sequence
from functools import partial
from typing import Any, cast

from ferricstore.protocol_common import (
    _command_name,
    _pending_request_capacity_error,
)
from ferricstore.protocol_compact_budget import (
    _binary_wire_size,
    _bounded_maybe_bytes,
    _bounded_optional_bytes,
    _CompactPayloadBudget,
)
from ferricstore.protocol_constants import (
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
    _COMPACT_SREM_PIPELINE_MODE,
    _COMPACT_ZADD_PIPELINE_MODE,
    _COMPACT_ZRANGE_PIPELINE_MODE,
    _COMPACT_ZREM_PIPELINE_MODE,
    _COMPACT_ZSCORE_PIPELINE_MODE,
    _OP_FLOW_COMPLETE_MANY,
    _OP_FLOW_CREATE_MANY,
    _OP_PIPELINE,
    _OPCODES,
    ProtocolCommand,
)
from ferricstore.protocol_flow_codec import (
    _compact_binary,
    _compact_flow_complete_many_payload,
    _compact_flow_create_many_payload,
    _compact_flow_value_put_payload,
    _compact_i64,
    _compact_optional_binary,
    _maybe_bytes,
    _ok_on_success_return_mode,
    _optional_bytes,
    _raw_int,
)
from ferricstore.protocol_pipeline_codec import (
    _compact_flow_get_pipeline_payload_from_raw,
    _compact_flow_history_pipeline_payload_from_raw,
    _compact_mixed_pipeline_payload_from_raw,
    _compact_pipeline_hmget_payload_from_raw,
    _compact_pipeline_hset_payload_from_raw,
    _compact_pipeline_keys_payload_from_raw,
    _compact_pipeline_range_payload_from_raw,
    _compact_pipeline_set_payload_from_raw,
    _compact_pipeline_two_binary_payload_from_raw,
    _compact_pipeline_zadd_payload_from_raw,
)


def _build_protocol_command(*args: Any) -> ProtocolCommand:
    from ferricstore.protocol_commands import build_protocol_command

    return build_protocol_command(*args)


_RawPipelineEncoder = Callable[..., bytes | None]


_RAW_COMPACT_PIPELINE_SPECS: dict[str, tuple[int, _RawPipelineEncoder]] = {
    "SET": (1, _compact_pipeline_set_payload_from_raw),
    "GET": (2, partial(_compact_pipeline_keys_payload_from_raw, name="GET")),
    "HGET": (
        _COMPACT_HGET_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="HGET"),
    ),
    "HMGET": (_COMPACT_HMGET_PIPELINE_MODE, _compact_pipeline_hmget_payload_from_raw),
    "HGETALL": (
        _COMPACT_HGETALL_PIPELINE_MODE,
        partial(_compact_pipeline_keys_payload_from_raw, name="HGETALL"),
    ),
    "SMEMBERS": (
        _COMPACT_SMEMBERS_PIPELINE_MODE,
        partial(_compact_pipeline_keys_payload_from_raw, name="SMEMBERS"),
    ),
    "SISMEMBER": (
        _COMPACT_SISMEMBER_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="SISMEMBER"),
    ),
    "LRANGE": (
        _COMPACT_LRANGE_PIPELINE_MODE,
        partial(_compact_pipeline_range_payload_from_raw, name="LRANGE"),
    ),
    "ZRANGE": (
        _COMPACT_ZRANGE_PIPELINE_MODE,
        partial(_compact_pipeline_range_payload_from_raw, name="ZRANGE"),
    ),
    "ZSCORE": (
        _COMPACT_ZSCORE_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="ZSCORE"),
    ),
    "HSET": (_COMPACT_HSET_PIPELINE_MODE, _compact_pipeline_hset_payload_from_raw),
    "LPUSH": (
        _COMPACT_LPUSH_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="LPUSH"),
    ),
    "RPUSH": (
        _COMPACT_RPUSH_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="RPUSH"),
    ),
    "SADD": (
        _COMPACT_SADD_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="SADD"),
    ),
    "SREM": (
        _COMPACT_SREM_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="SREM"),
    ),
    "ZADD": (_COMPACT_ZADD_PIPELINE_MODE, _compact_pipeline_zadd_payload_from_raw),
    "ZREM": (
        _COMPACT_ZREM_PIPELINE_MODE,
        partial(_compact_pipeline_two_binary_payload_from_raw, name="ZREM"),
    ),
}


def _raw_standard_pipeline_size(
    commands: Sequence[tuple[Any, ...]],
    name: str,
) -> int | None:
    size = _COMPACT_PIPELINE_HEADER.size

    def add_binary(value: Any) -> bool:
        nonlocal size
        item_size = _binary_wire_size(value)
        if item_size is None:
            return False
        size += item_size
        return True

    for command in commands:
        if not command or _command_name(command[0]) != name:
            return None
        if name == "SET":
            if len(command) != 3 or not add_binary(command[1]) or not add_binary(command[2]):
                return None
        elif name in {"GET", "HGETALL", "SMEMBERS"}:
            if len(command) != 2 or not add_binary(command[1]):
                return None
        elif name == "HMGET":
            if len(command) < 3 or not add_binary(command[1]):
                return None
            size += 4
            if not all(add_binary(field) for field in command[2:]):
                return None
        elif name in {
            "HGET",
            "SISMEMBER",
            "ZSCORE",
            "LPUSH",
            "RPUSH",
            "SADD",
            "SREM",
            "ZREM",
        }:
            if len(command) != 3 or not add_binary(command[1]) or not add_binary(command[2]):
                return None
        elif name in {"LRANGE", "ZRANGE"}:
            expected_lengths = {4} if name == "LRANGE" else {4, 5}
            if len(command) not in expected_lengths or not add_binary(command[1]):
                return None
            size += 16 + (1 if name == "ZRANGE" else 0)
        elif name == "HSET":
            if len(command) != 4 or not all(add_binary(value) for value in command[1:4]):
                return None
        elif name == "ZADD":
            if len(command) != 4 or not add_binary(command[1]) or not add_binary(command[3]):
                return None
            size += 8
        else:
            return None
    return size


def _raw_mixed_pipeline_size(commands: Sequence[tuple[Any, ...]]) -> int | None:
    size = _COMPACT_PIPELINE_HEADER.size
    read_keys: set[str | bytes] = set()
    written_keys: set[str | bytes] = set()
    for command in commands:
        if not command:
            return None
        name = _command_name(command[0])
        if name == "GET" and len(command) == 2 and isinstance(command[1], (str, bytes)):
            key = command[1]
            if key in written_keys:
                return None
            key_size = _binary_wire_size(key)
            if key_size is None:
                return None
            read_keys.add(key)
            size += 1 + key_size
        elif name == "SET" and len(command) == 3 and isinstance(command[1], (str, bytes)):
            key = command[1]
            if key in read_keys or key in written_keys:
                return None
            key_size = _binary_wire_size(key)
            value_size = _binary_wire_size(command[2])
            if key_size is None or value_size is None:
                return None
            written_keys.add(key)
            size += 1 + key_size + value_size
        else:
            return None
    return size


def _compact_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]],
    *,
    values_only: bool = False,
    protocol_commands: Sequence[ProtocolCommand] | None = None,
    max_payload_bytes: int | None = None,
    pending_limit: int | None = None,
) -> bytes | None:
    if not commands:
        return None

    try:
        name = _command_name(commands[0][0])
    except Exception:
        return None

    if name == "FLOW.HISTORY":
        return _compact_flow_history_pipeline_payload_from_raw(
            commands,
            values_only=values_only,
            max_payload_bytes=max_payload_bytes,
            pending_limit=pending_limit,
        )
    if name == "FLOW.SIGNAL":
        return _compact_flow_signal_pipeline_payload_from_raw(
            commands,
            values_only=values_only,
            protocol_commands=protocol_commands,
            max_payload_bytes=max_payload_bytes,
            pending_limit=pending_limit,
        )
    if name == "FLOW.GET":
        return _compact_flow_get_pipeline_payload_from_raw(
            commands,
            values_only=values_only,
            max_payload_bytes=max_payload_bytes,
            pending_limit=pending_limit,
        )

    spec = _RAW_COMPACT_PIPELINE_SPECS.get(name)
    standard_size = _raw_standard_pipeline_size(commands, name) if spec is not None else None
    mixed_size = _raw_mixed_pipeline_size(commands) if standard_size is None else None
    payload_size = standard_size if standard_size is not None else mixed_size
    if max_payload_bytes is not None and payload_size is None:
        return None
    if (
        max_payload_bytes is not None
        and payload_size is not None
        and payload_size > max_payload_bytes
    ):
        raise _pending_request_capacity_error(pending_limit)
    if standard_size is None:
        if mixed_size is None:
            return None
        return _compact_mixed_pipeline_payload_from_raw(commands)
    if spec is None:
        return None
    mode, encoder = spec
    if values_only:
        mode |= 0x80
    try:
        return encoder(commands, mode=mode)
    except (OverflowError, struct.error):
        # Compact encoding is an optimization. Unsupported wire magnitudes must
        # fall through to the generic codec, which reports a normalized error.
        return None


def _compact_flow_signal_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]],
    *,
    values_only: bool,
    protocol_commands: Sequence[ProtocolCommand] | None = None,
    max_payload_bytes: int | None = None,
    pending_limit: int | None = None,
) -> bytes | None:
    signal: bytes | None = None
    if_state: bytes | None = None
    transition_to: bytes | None = None
    items: list[tuple[bytes, bytes | None, int]] = []
    budget = _CompactPayloadBudget(
        max_payload_bytes,
        pending_limit,
        initial_size=_COMPACT_PIPELINE_HEADER.size,
    )

    if protocol_commands is not None and len(protocol_commands) != len(commands):
        return None
    for index, command in enumerate(commands):
        protocol_command = (
            protocol_commands[index]
            if protocol_commands is not None
            else _build_protocol_command(*command)
        )
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

        flow_id = _bounded_maybe_bytes(
            payload.get("id"),
            max_payload_bytes,
            pending_limit,
            budget=budget,
        )
        shared_budget = budget if signal is None else None
        item_signal = _bounded_maybe_bytes(
            payload.get("signal"),
            max_payload_bytes,
            pending_limit,
            budget=shared_budget,
        )
        item_if_state = _bounded_maybe_bytes(
            payload.get("if_state"),
            max_payload_bytes,
            pending_limit,
            budget=shared_budget,
        )
        item_transition_to = _bounded_maybe_bytes(
            payload.get("transition_to"),
            max_payload_bytes,
            pending_limit,
            budget=shared_budget,
        )
        partition_key = _bounded_optional_bytes(
            payload.get("partition_key"),
            max_payload_bytes,
            pending_limit,
            budget=budget,
        )
        now_ms = _compact_i64(payload.get("now_ms"))
        if (
            flow_id is None
            or item_signal is None
            or item_if_state is None
            or item_transition_to is None
            or partition_key is False
            or now_ms is None
        ):
            return None
        if partition_key is None:
            budget.reserve(4)
        budget.reserve(8)

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


def _compact_flow_many_payloads_from_raw(
    commands: list[tuple[Any, ...]],
    *,
    protocol_commands: Sequence[ProtocolCommand] | None = None,
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
        return _compact_flow_create_many_payloads_from_raw(
            commands,
            protocol_commands=protocol_commands,
        )
    if all(name == "FLOW.COMPLETE" for name in names):
        return _compact_flow_complete_many_payloads_from_raw(
            commands,
            protocol_commands=protocol_commands,
        )
    if all(name == "FLOW.STEP_CONTINUE" for name in names):
        return _compact_flow_step_continue_payloads_from_raw(commands)
    if all(name == "FLOW.START_AND_CLAIM" for name in names):
        return _compact_flow_start_and_claim_payloads_from_raw(commands)
    if all(name == "FLOW.VALUE.PUT" for name in names):
        return _compact_flow_value_put_payloads_from_raw(commands)
    return None


def _compact_flow_create_many_payloads_from_raw(
    commands: list[tuple[Any, ...]],
    *,
    protocol_commands: Sequence[ProtocolCommand] | None = None,
) -> list[tuple[int, bytes, int]] | None:
    groups: list[tuple[tuple[bytes, bytes, int, int], list[list[Any]]]] = []
    current_key: tuple[bytes, bytes, int, int] | None = None
    current_items: list[list[Any]] = []

    if protocol_commands is not None and len(protocol_commands) != len(commands):
        return None
    for index, command in enumerate(commands):
        protocol_command = (
            protocol_commands[index]
            if protocol_commands is not None
            else _build_protocol_command(*command)
        )
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
    *,
    protocol_commands: Sequence[ProtocolCommand] | None = None,
) -> list[tuple[int, bytes, int]] | None:
    groups: list[tuple[int, bool, list[list[Any]]]] = []
    current_now: int | None = None
    current_partitioned: bool | None = None
    current_items: list[list[Any]] = []

    if protocol_commands is not None and len(protocol_commands) != len(commands):
        return None
    for index, command in enumerate(commands):
        protocol_command = (
            protocol_commands[index]
            if protocol_commands is not None
            else _build_protocol_command(*command)
        )
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
            fencing_token = _compact_i64(_raw_int(value))
        elif name == "LEASE_MS":
            lease_ms = _compact_i64(_raw_int(value))
        elif name == "NOW":
            now_ms = _compact_i64(_raw_int(value))
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
            lease_ms = _compact_i64(_raw_int(value))
        elif name == "NOW":
            now_ms = _compact_i64(_raw_int(value))
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
            now_ms = _compact_i64(_raw_int(option_value))
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


__all__ = [
    "_compact_flow_complete_many_payloads_from_raw",
    "_compact_flow_create_many_payloads_from_raw",
    "_compact_flow_many_payloads_from_raw",
    "_compact_flow_signal_pipeline_payload_from_raw",
    "_compact_flow_start_and_claim_payloads_from_raw",
    "_compact_flow_step_continue_payloads_from_raw",
    "_compact_flow_value_put_payloads_from_raw",
    "_compact_pipeline_payload_from_raw",
    "_parse_compact_flow_start_and_claim_raw",
    "_parse_compact_flow_step_continue_raw",
    "_parse_compact_flow_value_put_raw",
]
