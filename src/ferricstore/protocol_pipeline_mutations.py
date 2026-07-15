from __future__ import annotations

from typing import Any

from ferricstore.protocol_common import _command_name
from ferricstore.protocol_constants import (
    _COMPACT_F64,
    _COMPACT_I64,
    _COMPACT_PIPELINE_HEADER,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_U32,
)
from ferricstore.protocol_flow_codec import _compact_i64, _maybe_bytes


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
            start = _compact_i64(int(command[2]))
            stop = _compact_i64(int(command[3]))
        except (OverflowError, TypeError, ValueError):
            return None
        if start is None or stop is None:
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
        except (OverflowError, TypeError, ValueError):
            return None
        extend(pack_u32(len(key)))
        extend(key)
        extend(pack_f64(score))
        extend(pack_u32(len(member)))
        extend(member)
    return bytes(payload)


__all__ = [
    "_compact_pipeline_hmget_payload_from_raw",
    "_compact_pipeline_hset_payload_from_raw",
    "_compact_pipeline_range_payload_from_raw",
    "_compact_pipeline_zadd_payload_from_raw",
]
