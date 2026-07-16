from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ferricstore.protocol_common import _command_name
from ferricstore.protocol_compact_budget import (
    _binary_wire_size,
    _bounded_maybe_bytes,
    _CompactPayloadBudget,
    _pending_request_capacity_error,
    current_compact_encoding_policy,
)
from ferricstore.protocol_constants import (
    _COMPACT_PIPELINE_HEADER,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_U32,
)
from ferricstore.protocol_flow_codec import _maybe_bytes


def _compact_binary_sequence(
    args: Sequence[Any],
    *,
    mode: int,
    item_count: int,
) -> bytes | None:
    policy = current_compact_encoding_policy()
    if not policy.enabled:
        return None
    header = _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, item_count)
    remaining = policy.max_payload_bytes
    pack_u32 = _COMPACT_U32.pack
    parts = [header]
    append = parts.append
    if remaining is None:
        for arg in args:
            if isinstance(arg, bytes):
                value = arg
            elif isinstance(arg, str):
                value = arg.encode()
            else:
                return None
            append(pack_u32(len(value)))
            append(value)
        return b"".join(parts)

    if _COMPACT_PIPELINE_HEADER.size > remaining:
        raise _pending_request_capacity_error(policy.pending_limit)
    remaining -= _COMPACT_PIPELINE_HEADER.size
    prefix_size = _COMPACT_U32.size
    for arg in args:
        if isinstance(arg, bytes):
            value = arg
            wire_size = prefix_size + len(arg)
        elif isinstance(arg, str):
            character_count = len(arg)
            minimum_wire_size = prefix_size + character_count
            if minimum_wire_size > remaining:
                raise _pending_request_capacity_error(policy.pending_limit)
            if prefix_size + 4 * character_count > remaining:
                measured_wire_size = _binary_wire_size(arg)
                if measured_wire_size is None:
                    arg.encode()
                    return None
                wire_size = measured_wire_size
                if wire_size > remaining:
                    raise _pending_request_capacity_error(policy.pending_limit)
                value = arg.encode()
            else:
                value = arg.encode()
                wire_size = prefix_size + len(value)
        else:
            return None
        if wire_size > remaining:
            raise _pending_request_capacity_error(policy.pending_limit)
        remaining -= wire_size
        append(pack_u32(len(value)))
        append(value)
    return b"".join(parts)


def _compact_kv_set_pairs_payload(args: tuple[Any, ...], mode: int = 1) -> bytes | None:
    if len(args) == 0 or len(args) % 2 != 0:
        return None
    return _compact_binary_sequence(args, mode=mode, item_count=len(args) // 2)


def _compact_kv_keys_payload(args: Sequence[Any], mode: int) -> bytes | None:
    if not args:
        return None
    return _compact_binary_sequence(args, mode=mode, item_count=len(args))


def _compact_kv_set_keys_value_payload(
    keys: Sequence[Any], value_arg: Any, mode: int = 1
) -> bytes | None:
    if not keys:
        return None
    policy = current_compact_encoding_policy()
    if not policy.enabled:
        return None
    budget = _CompactPayloadBudget(
        initial_size=_COMPACT_PIPELINE_HEADER.size,
        policy=policy,
    )
    value = _bounded_maybe_bytes(value_arg, budget=budget)
    if value is None:
        return None
    value_wire_size = _COMPACT_U32.size + len(value)
    payload = bytearray(_COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, len(keys)))
    pack_u32 = _COMPACT_U32.pack
    extend = payload.extend
    value_len = pack_u32(len(value))
    for index, key_arg in enumerate(keys):
        key = _bounded_maybe_bytes(key_arg, budget=budget)
        if key is None:
            return None
        if index:
            budget.reserve(value_wire_size)
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


__all__ = [
    "_compact_kv_keys_payload",
    "_compact_kv_set_keys_value_payload",
    "_compact_kv_set_pairs_payload",
    "_compact_pipeline_keys_payload_from_raw",
    "_compact_pipeline_set_payload_from_raw",
    "_compact_pipeline_two_binary_payload_from_raw",
]
