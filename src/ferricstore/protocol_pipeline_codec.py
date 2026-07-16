from __future__ import annotations

import math
import struct
from collections.abc import Callable, Sequence
from typing import Any, cast

from ferricstore.command_grammar import (
    flow_create_many_item_count,
    parse_stream_read,
    split_flow_value_mget,
)
from ferricstore.protocol_common import (
    _coerce_bool,
    _command_name,
    _command_token,
    _text,
)
from ferricstore.protocol_compact_budget import (
    _binary_wire_size,
    _bounded_maybe_bytes,
    _bounded_optional_bytes,
    _CompactPayloadBudget,
)
from ferricstore.protocol_constants import (
    _COMPACT_F64,
    _COMPACT_FLOW_CANCEL_MANY_OK_REQUEST,
    _COMPACT_FLOW_CANCEL_MANY_REQUEST,
    _COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
    _COMPACT_FLOW_COMPLETE_MANY_REQUEST,
    _COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST,
    _COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST,
    _COMPACT_FLOW_CREATE_MANY_REQUEST,
    _COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
    _COMPACT_FLOW_RETRY_MANY_REQUEST,
    _COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST,
    _COMPACT_FLOW_TRANSITION_MANY_REQUEST,
    _COMPACT_FLOW_VALUE_MGET_REQUEST,
    _COMPACT_HGET_PIPELINE_MODE,
    _COMPACT_HGETALL_PIPELINE_MODE,
    _COMPACT_HMGET_PIPELINE_MODE,
    _COMPACT_HSET_PIPELINE_MODE,
    _COMPACT_I64,
    _COMPACT_LPUSH_PIPELINE_MODE,
    _COMPACT_LRANGE_PIPELINE_MODE,
    _COMPACT_PIPELINE_HEADER,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_RPUSH_PIPELINE_MODE,
    _COMPACT_SADD_PIPELINE_MODE,
    _COMPACT_SISMEMBER_PIPELINE_MODE,
    _COMPACT_SMEMBERS_PIPELINE_MODE,
    _COMPACT_SREM_PIPELINE_MODE,
    _COMPACT_U32,
    _COMPACT_ZADD_PIPELINE_MODE,
    _COMPACT_ZRANGE_PIPELINE_MODE,
    _COMPACT_ZREM_PIPELINE_MODE,
    _COMPACT_ZSCORE_PIPELINE_MODE,
    _CONTROL_OPCODES,
    _NULL_U32,
    _OP_COMMAND_EXEC,
    _OP_FLOW_CANCEL_MANY,
    _OP_FLOW_COMPLETE_MANY,
    _OP_FLOW_CREATE_MANY,
    _OP_FLOW_FAIL_MANY,
    _OP_FLOW_RETRY_MANY,
    _OP_FLOW_TRANSITION_MANY,
    _OP_FLOW_VALUE_MGET,
    _OP_MGET,
    _OP_PIPELINE,
    _OPCODES,
    _STATEFUL_COMMAND_EXEC,
    ProtocolCommand,
)
from ferricstore.protocol_flow_codec import (
    _compact_binary,
    _compact_bool_marker,
    _compact_i64,
    _compact_optional_binary,
    _maybe_bytes,
    _optional_bytes,
    _raw_int,
)
from ferricstore.protocol_pipeline_mutations import (
    _compact_pipeline_hmget_payload_from_raw,
    _compact_pipeline_hset_payload_from_raw,
    _compact_pipeline_range_payload_from_raw,
    _compact_pipeline_zadd_payload_from_raw,
)
from ferricstore.protocol_pipeline_raw import (
    _compact_kv_keys_payload,
    _compact_kv_set_keys_value_payload,
    _compact_kv_set_pairs_payload,
    _compact_pipeline_keys_payload_from_raw,
    _compact_pipeline_set_payload_from_raw,
    _compact_pipeline_two_binary_payload_from_raw,
)


def _pipeline_frame_supported(commands: list[ProtocolCommand]) -> bool:
    return all(
        command.opcode not in _CONTROL_OPCODES
        and command.flags == 0
        and isinstance(command.payload, dict)
        and not _stateful_command_exec(command)
        for command in commands
    )


def _stateful_command_exec(command: ProtocolCommand) -> bool:
    if command.opcode != _OP_COMMAND_EXEC or not isinstance(command.payload, dict):
        return False
    name = command.payload.get("command")
    return isinstance(name, str) and name.upper() in _STATEFUL_COMMAND_EXEC


def _blocks_forever(args: Sequence[Any]) -> bool:
    if not args:
        return False
    name = _command_name(args[0])
    values = args[1:]
    if name == "COMMAND_EXEC" and values:
        name = _command_name(values[0])
        values = values[1:]
    candidate: Any = None
    if name in {"BLPOP", "BRPOP", "BLMOVE"} and values:
        candidate = values[-1]
    elif name == "BLMPOP" and values:
        candidate = values[0]
    elif name in {"XREAD", "XREADGROUP"}:
        parsed = parse_stream_read(values, read_group=name == "XREADGROUP")
        candidate = parsed.block if parsed.valid else None
    try:
        return candidate is not None and float(candidate) == 0.0
    except (TypeError, ValueError):
        return False


def _expected_command_collection_items(args: Sequence[Any]) -> int | None:
    """Return an exact list cardinality only when command grammar makes it certain."""

    if not args:
        return None
    name = _command_name(args[0])
    if name == "MGET":
        return max(len(args) - 1, 0)
    if name == "MSET":
        return 1
    if name == "FLOW.VALUE.MGET":
        refs, _max_bytes = split_flow_value_mget(args[1:])
        return len(refs)
    if name != "FLOW.CREATE_MANY":
        return None
    return flow_create_many_item_count(args[1:])


def _expected_payload_collection_items(
    opcode: int,
    payload: dict[str, Any] | bytes,
) -> int | None:
    if isinstance(payload, dict):
        if opcode == _OP_PIPELINE:
            commands = payload.get("commands")
            return len(commands) if isinstance(commands, list) else None
        if opcode in {
            _OP_FLOW_CREATE_MANY,
            _OP_FLOW_COMPLETE_MANY,
            _OP_FLOW_TRANSITION_MANY,
            _OP_FLOW_RETRY_MANY,
            _OP_FLOW_FAIL_MANY,
            _OP_FLOW_CANCEL_MANY,
        }:
            items = payload.get("items")
            return len(items) if isinstance(items, list) else None
        return None

    if len(payload) >= _COMPACT_PIPELINE_HEADER.size:
        marker, _mode, count = _COMPACT_PIPELINE_HEADER.unpack_from(payload, 0)
        if marker == _COMPACT_PIPELINE_REQUEST and opcode in {_OP_PIPELINE, _OP_MGET}:
            return int(count)
    if (
        opcode == _OP_FLOW_VALUE_MGET
        and len(payload) >= 13
        and payload[0] == _COMPACT_FLOW_VALUE_MGET_REQUEST
    ):
        return int(_COMPACT_U32.unpack_from(payload, 9)[0])

    def skip_binary(offset: int) -> int | None:
        if offset + 4 > len(payload):
            return None
        size = _COMPACT_U32.unpack_from(payload, offset)[0]
        if size == _NULL_U32:
            return offset + 4
        end = offset + 4 + size
        return end if end <= len(payload) else None

    def read_count(offset: int) -> int | None:
        if offset + 4 > len(payload):
            return None
        return int(_COMPACT_U32.unpack_from(payload, offset)[0])

    marker = payload[0] if payload else None
    if opcode == _OP_FLOW_CREATE_MANY and marker in {
        _COMPACT_FLOW_CREATE_MANY_REQUEST,
        _COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST,
        _COMPACT_FLOW_CREATE_MANY_MIXED_REQUEST,
    }:
        offset = skip_binary(1)
        offset = skip_binary(offset) if offset is not None else None
        if offset is not None and marker == _COMPACT_FLOW_CREATE_MANY_PARTITION_REQUEST:
            offset = skip_binary(offset)
        return read_count(offset + 18) if offset is not None else None

    if opcode in {
        _OP_FLOW_COMPLETE_MANY,
        _OP_FLOW_RETRY_MANY,
        _OP_FLOW_FAIL_MANY,
        _OP_FLOW_CANCEL_MANY,
    } and marker in {
        _COMPACT_FLOW_COMPLETE_MANY_REQUEST,
        _COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
        _COMPACT_FLOW_RETRY_MANY_REQUEST,
        _COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
        _COMPACT_FLOW_CANCEL_MANY_REQUEST,
        _COMPACT_FLOW_CANCEL_MANY_OK_REQUEST,
    }:
        offset = skip_binary(1)
        if offset is None:
            return None
        count_offset = offset + (17 if opcode == _OP_FLOW_RETRY_MANY else 9)
        return read_count(count_offset)

    if opcode == _OP_FLOW_TRANSITION_MANY and marker in {
        _COMPACT_FLOW_TRANSITION_MANY_REQUEST,
        _COMPACT_FLOW_TRANSITION_MANY_OK_REQUEST,
    }:
        offset = skip_binary(1)
        offset = skip_binary(offset) if offset is not None else None
        offset = skip_binary(offset) if offset is not None else None
        return read_count(offset + 17) if offset is not None else None
    return None


_PipelineItemEncoder = Callable[[dict[str, Any], bytes], tuple[bytes, ...] | None]


def _compact_key_only_pipeline_item(
    payload: dict[str, Any], key: bytes
) -> tuple[bytes, ...] | None:
    if set(payload) != {"key"}:
        return None
    return (_compact_binary(key),)


def _compact_set_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if set(payload) != {"key", "value"}:
        return None
    value = _maybe_bytes(payload.get("value"))
    if value is None:
        return None
    return _compact_binary(key), _compact_binary(value)


def _compact_hget_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if set(payload) != {"key", "field"}:
        return None
    field = _maybe_bytes(payload.get("field"))
    if field is None:
        return None
    return _compact_binary(key), _compact_binary(field)


def _compact_hmget_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if set(payload) != {"key", "fields"}:
        return None
    fields = payload.get("fields")
    if not isinstance(fields, list) or not fields:
        return None
    encoded_fields = [_maybe_bytes(field) for field in fields]
    if any(field is None for field in encoded_fields):
        return None
    return (
        _compact_binary(key),
        _COMPACT_U32.pack(len(encoded_fields)),
        *(_compact_binary(field) for field in encoded_fields if field is not None),
    )


def _compact_member_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if set(payload) != {"key", "member"}:
        return None
    member = _maybe_bytes(payload.get("member"))
    if member is None:
        return None
    return _compact_binary(key), _compact_binary(member)


def _compact_lrange_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if set(payload) != {"key", "start", "stop"}:
        return None
    start = payload.get("start")
    stop = payload.get("stop")
    start = _compact_i64(start)
    stop = _compact_i64(stop)
    if start is None or stop is None:
        return None
    return _compact_binary(key), _COMPACT_I64.pack(start), _COMPACT_I64.pack(stop)


def _compact_zrange_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if not set(payload).issubset({"key", "start", "stop", "withscores"}):
        return None
    start = payload.get("start")
    stop = payload.get("stop")
    start = _compact_i64(start)
    stop = _compact_i64(stop)
    if start is None or stop is None:
        return None
    return (
        _compact_binary(key),
        _COMPACT_I64.pack(start),
        _COMPACT_I64.pack(stop),
        b"\x01" if bool(payload.get("withscores", False)) else b"\x00",
    )


def _compact_hset_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if set(payload) != {"key", "fields"}:
        return None
    fields = payload.get("fields")
    if not isinstance(fields, dict) or len(fields) != 1:
        return None
    field_arg, value_arg = next(iter(fields.items()))
    field = _maybe_bytes(field_arg)
    value = _maybe_bytes(value_arg)
    if field is None or value is None:
        return None
    return _compact_binary(key), _compact_binary(field), _compact_binary(value)


def _single_binary_list_pipeline_encoder(field_name: str) -> _PipelineItemEncoder:
    def encode(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
        if set(payload) != {"key", field_name}:
            return None
        values = payload.get(field_name)
        if not isinstance(values, list) or len(values) != 1:
            return None
        value = _maybe_bytes(values[0])
        if value is None:
            return None
        return _compact_binary(key), _compact_binary(value)

    return encode


def _compact_zadd_pipeline_item(payload: dict[str, Any], key: bytes) -> tuple[bytes, ...] | None:
    if set(payload) != {"key", "items"}:
        return None
    items = payload.get("items")
    if not isinstance(items, list) or len(items) != 1:
        return None
    item = items[0]
    if not isinstance(item, list) or len(item) != 2:
        return None
    score_arg, member_arg = item
    member = _maybe_bytes(member_arg)
    if member is None or isinstance(score_arg, bool):
        return None
    try:
        score = float(score_arg)
    except (OverflowError, TypeError, ValueError):
        return None
    if not math.isfinite(score):
        return None
    return _compact_binary(key), _COMPACT_F64.pack(score), _compact_binary(member)


_COMPACT_PIPELINE_MODES = {
    _OPCODES["SET"]: 1,
    _OPCODES["GET"]: 2,
    _OPCODES["HGET"]: _COMPACT_HGET_PIPELINE_MODE,
    _OPCODES["HMGET"]: _COMPACT_HMGET_PIPELINE_MODE,
    _OPCODES["HGETALL"]: _COMPACT_HGETALL_PIPELINE_MODE,
    _OPCODES["SMEMBERS"]: _COMPACT_SMEMBERS_PIPELINE_MODE,
    _OPCODES["SISMEMBER"]: _COMPACT_SISMEMBER_PIPELINE_MODE,
    _OPCODES["LRANGE"]: _COMPACT_LRANGE_PIPELINE_MODE,
    _OPCODES["ZRANGE"]: _COMPACT_ZRANGE_PIPELINE_MODE,
    _OPCODES["ZSCORE"]: _COMPACT_ZSCORE_PIPELINE_MODE,
    _OPCODES["HSET"]: _COMPACT_HSET_PIPELINE_MODE,
    _OPCODES["LPUSH"]: _COMPACT_LPUSH_PIPELINE_MODE,
    _OPCODES["RPUSH"]: _COMPACT_RPUSH_PIPELINE_MODE,
    _OPCODES["SADD"]: _COMPACT_SADD_PIPELINE_MODE,
    _OPCODES["SREM"]: _COMPACT_SREM_PIPELINE_MODE,
    _OPCODES["ZADD"]: _COMPACT_ZADD_PIPELINE_MODE,
    _OPCODES["ZREM"]: _COMPACT_ZREM_PIPELINE_MODE,
}

_COMPACT_PIPELINE_ITEM_ENCODERS: dict[int, _PipelineItemEncoder] = {
    _OPCODES["SET"]: _compact_set_pipeline_item,
    _OPCODES["GET"]: _compact_key_only_pipeline_item,
    _OPCODES["HGET"]: _compact_hget_pipeline_item,
    _OPCODES["HMGET"]: _compact_hmget_pipeline_item,
    _OPCODES["HGETALL"]: _compact_key_only_pipeline_item,
    _OPCODES["SMEMBERS"]: _compact_key_only_pipeline_item,
    _OPCODES["SISMEMBER"]: _compact_member_pipeline_item,
    _OPCODES["LRANGE"]: _compact_lrange_pipeline_item,
    _OPCODES["ZRANGE"]: _compact_zrange_pipeline_item,
    _OPCODES["ZSCORE"]: _compact_member_pipeline_item,
    _OPCODES["HSET"]: _compact_hset_pipeline_item,
    _OPCODES["LPUSH"]: _single_binary_list_pipeline_encoder("values"),
    _OPCODES["RPUSH"]: _single_binary_list_pipeline_encoder("values"),
    _OPCODES["SADD"]: _single_binary_list_pipeline_encoder("members"),
    _OPCODES["SREM"]: _single_binary_list_pipeline_encoder("members"),
    _OPCODES["ZADD"]: _compact_zadd_pipeline_item,
    _OPCODES["ZREM"]: _single_binary_list_pipeline_encoder("members"),
}


def _compact_pipeline_payload(
    commands: list[ProtocolCommand], *, values_only: bool = False
) -> bytes | None:
    if not commands:
        return None
    opcode = commands[0].opcode
    if opcode == _OPCODES["FLOW.GET"]:
        return _compact_flow_get_pipeline_payload(commands, values_only=values_only)

    mode = _COMPACT_PIPELINE_MODES.get(opcode)
    encoder = _COMPACT_PIPELINE_ITEM_ENCODERS.get(opcode)
    if mode is None or encoder is None:
        return None
    if values_only:
        mode |= 0x80

    parts = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, mode, len(commands))]
    for command in commands:
        if command.opcode != opcode or command.flags != 0 or not isinstance(command.payload, dict):
            return None
        key = _maybe_bytes(command.payload.get("key"))
        encoded_item = encoder(command.payload, key) if key is not None else None
        if encoded_item is None:
            return None
        parts.extend(encoded_item)
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

        partition_key: bytes | None = None
        if "partition_key" in command.payload:
            partition_value = _optional_bytes(command.payload.get("partition_key"))
            if partition_value is False:
                return None
            partition_key = cast(bytes | None, partition_value)
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


def _compact_flow_get_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]],
    *,
    values_only: bool = False,
    max_payload_bytes: int | None = None,
    pending_limit: int | None = None,
) -> bytes | None:
    items: list[tuple[bytes, bytes | None]] = []
    has_partition = False
    return_mode: str | None = None
    budget = _CompactPayloadBudget(
        max_payload_bytes,
        pending_limit,
        initial_size=_COMPACT_PIPELINE_HEADER.size,
    )

    for command in commands:
        if len(command) < 2:
            return None

        try:
            command_name = _command_name(command[0])
        except Exception:
            return None
        if command_name != "FLOW.GET":
            return None

        flow_id = _bounded_maybe_bytes(
            command[1],
            max_payload_bytes,
            pending_limit,
            budget=budget,
        )
        if flow_id is None:
            return None

        partition_arg: Any = None
        option_index = 2
        while option_index < len(command):
            if option_index + 1 >= len(command):
                return None

            option = _command_token(command[option_index])
            value = command[option_index + 1]

            if option in {"PARTITION", "PARTITION_KEY"}:
                partition_arg = value
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

        partition_value = _bounded_optional_bytes(
            partition_arg,
            max_payload_bytes,
            pending_limit,
            budget=budget,
        )
        if partition_value is False:
            return None
        partition_key = cast(bytes | None, partition_value)
        has_partition = has_partition or partition_key is not None
        items.append((flow_id, partition_key))

    mode = 17 if return_mode == "meta" else 16 if has_partition else 9
    if values_only:
        mode |= 0x80

    if has_partition or return_mode == "meta":
        budget.reserve(4 * sum(partition_key is None for _flow_id, partition_key in items))

    parts: list[bytes] = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, mode, len(items))]
    for flow_id, partition_key in items:
        parts.append(_compact_binary(flow_id))
        if has_partition or return_mode == "meta":
            parts.append(_compact_optional_binary(partition_key))

    return b"".join(parts)


def _compact_flow_history_pipeline_payload_from_raw(
    commands: list[tuple[Any, ...]],
    *,
    values_only: bool,
    max_payload_bytes: int | None = None,
    pending_limit: int | None = None,
) -> bytes | None:
    history_count: int | None = None
    include_cold: bool | None = None
    consistent_projection: bool | None = None
    items: list[tuple[bytes, bytes | None]] = []
    budget = _CompactPayloadBudget(
        max_payload_bytes,
        pending_limit,
        initial_size=_COMPACT_PIPELINE_HEADER.size + 10,
    )

    for command in commands:
        if len(command) < 2:
            return None

        try:
            command_name = _command_name(command[0])
        except Exception:
            return None
        if command_name != "FLOW.HISTORY":
            return None

        flow_id = _bounded_maybe_bytes(
            command[1],
            max_payload_bytes,
            pending_limit,
            budget=budget,
        )
        if flow_id is None:
            return None

        partition_arg: Any = None
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
                compact_count = _compact_i64(_raw_int(value))
                if compact_count is None:
                    return None
                item_count = compact_count
            elif option in {"PARTITION", "PARTITION_KEY"}:
                partition_arg = value
            elif option == "INCLUDE_COLD":
                item_include_cold = _coerce_bool(value)
            elif option == "CONSISTENT_PROJECTION":
                item_consistent_projection = _coerce_bool(value)
            else:
                return None

            option_index += 2

        partition_value = _bounded_optional_bytes(
            partition_arg,
            max_payload_bytes,
            pending_limit,
            budget=budget,
        )
        if partition_value is False:
            return None
        partition_key = cast(bytes | None, partition_value)
        if partition_key is None:
            budget.reserve(4)

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

        items.append((flow_id, partition_key))

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


__all__ = [
    "_CompactPayloadBudget",
    "_binary_wire_size",
    "_blocks_forever",
    "_compact_flow_get_pipeline_payload",
    "_compact_flow_get_pipeline_payload_from_raw",
    "_compact_flow_history_pipeline_payload_from_raw",
    "_compact_kv_keys_payload",
    "_compact_kv_set_keys_value_payload",
    "_compact_kv_set_pairs_payload",
    "_compact_mixed_pipeline_payload_from_raw",
    "_compact_pipeline_hmget_payload_from_raw",
    "_compact_pipeline_hset_payload_from_raw",
    "_compact_pipeline_keys_payload_from_raw",
    "_compact_pipeline_payload",
    "_compact_pipeline_range_payload_from_raw",
    "_compact_pipeline_set_payload_from_raw",
    "_compact_pipeline_two_binary_payload_from_raw",
    "_compact_pipeline_zadd_payload_from_raw",
    "_expected_command_collection_items",
    "_expected_payload_collection_items",
    "_pipeline_frame_supported",
    "_stateful_command_exec",
]
