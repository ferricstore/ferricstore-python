from __future__ import annotations

import struct

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_constants import _COMPACT_U32, _NULL_U32


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


__all__ = [
    "_read_compact_binary",
    "_read_compact_optional_binary",
    "_read_tagged_binary",
    "_read_tagged_i64",
    "_read_u32",
    "_require_available",
]
