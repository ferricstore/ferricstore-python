from __future__ import annotations

import struct
from collections.abc import Sequence
from typing import Any, cast

from ferricstore.protocol_common import (
    _coerce_bool,
    _command_name,
    _command_token,
    _text,
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
    _COMPACT_U32,
    _COMPACT_ZADD_PIPELINE_MODE,
    _COMPACT_ZRANGE_PIPELINE_MODE,
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
    _compact_optional_binary,
    _maybe_bytes,
    _optional_bytes,
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
        for index, value in enumerate(values[:-1]):
            if _command_name(value) == "BLOCK":
                candidate = values[index + 1]
                break
    try:
        return candidate is not None and float(candidate) == 0.0
    except (TypeError, ValueError):
        return False


def _expected_command_collection_items(args: Sequence[Any]) -> int | None:
    """Return an exact list cardinality only when command grammar makes it certain."""

    def token_is(value: Any, token: str) -> bool:
        if isinstance(value, str):
            return value.upper() == token
        if isinstance(value, bytes):
            return value.upper() == token.encode()
        return False

    if not args:
        return None
    name = _command_name(args[0])
    if name == "MGET":
        return max(len(args) - 1, 0)
    if name in {"SET", "MSET"}:
        return 1
    if name == "FLOW.VALUE.MGET":
        values = args[1:]
        for index, value in enumerate(values):
            if token_is(value, "MAX_BYTES") or token_is(value, "MAXBYTES"):
                return index
        return len(values)
    if name != "FLOW.CREATE_MANY":
        return None

    for marker in ("ITEMS_EXT", "ITEMS"):
        try:
            marker_index = next(
                index for index, value in enumerate(args) if token_is(value, marker)
            )
        except StopIteration:
            continue
        if marker == "ITEMS_EXT":
            if marker_index + 1 < len(args):
                count = args[marker_index + 1]
                if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
                    return count
            return None
        width = 3 if len(args) > 1 and token_is(args[1], "MIXED") else 2
        item_values = len(args) - marker_index - 1
        return item_values // width if item_values % width == 0 else None
    return None


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
        elif opcode == _OPCODES["GET"] or opcode == _OPCODES["SMEMBERS"]:
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
        elif opcode == _OPCODES["SISMEMBER"] or opcode == _OPCODES["ZSCORE"]:
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
                partition_value = _optional_bytes(value)
                if partition_value is False:
                    return None
                partition_key = cast(bytes | None, partition_value)
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


__all__ = [
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
