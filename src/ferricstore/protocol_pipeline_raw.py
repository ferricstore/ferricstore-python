from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ferricstore.protocol_common import _command_name
from ferricstore.protocol_constants import (
    _COMPACT_PIPELINE_HEADER,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_U32,
)
from ferricstore.protocol_flow_codec import _maybe_bytes


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


__all__ = [
    "_compact_kv_keys_payload",
    "_compact_kv_set_keys_value_payload",
    "_compact_kv_set_pairs_payload",
    "_compact_pipeline_keys_payload_from_raw",
    "_compact_pipeline_set_payload_from_raw",
    "_compact_pipeline_two_binary_payload_from_raw",
]
