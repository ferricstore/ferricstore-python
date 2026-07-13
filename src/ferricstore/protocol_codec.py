from __future__ import annotations

import io
import struct
from collections.abc import Callable
from typing import Any, Final

from ferricstore.errors import FerricStoreError

MAX_VALUE_NESTING: Final = 128
_U32 = struct.Struct(">I")
_I64 = struct.Struct(">q")
_F64 = struct.Struct(">d")


class DecodedCollectionLimitError(FerricStoreError):
    """Raised before a decoded collection can exceed the configured item budget."""


class EncodedValueLimitError(FerricStoreError):
    """Raised before an encoded value can exceed its caller-provided byte budget."""


class _ValueWriter:
    __slots__ = ("_max_bytes", "_size", "_write")

    def __init__(
        self,
        write: Callable[[bytes | bytearray], Any],
        *,
        max_bytes: int | None,
    ) -> None:
        self._write = write
        self._max_bytes = max_bytes
        self._size = 0

    def write(self, value: bytes | bytearray) -> None:
        size = len(value)
        if self._max_bytes is not None and size > self._max_bytes - self._size:
            raise EncodedValueLimitError("encoded protocol value exceeds max_bytes")
        self._write(value)
        self._size += size


class DecodeBudget:
    """One cumulative item budget shared by every nested protocol collection."""

    __slots__ = ("remaining",)

    def __init__(self, max_collection_items: int | None) -> None:
        if max_collection_items is not None and max_collection_items < 0:
            raise ValueError("max_collection_items must be non-negative or None")
        self.remaining = max_collection_items

    def consume(self, count: int) -> None:
        if self.remaining is None:
            return
        if count > self.remaining:
            raise DecodedCollectionLimitError(
                "protocol response collection exceeds max_decoded_collection_items"
            )
        self.remaining -= count

    def checkpoint(self) -> int | None:
        return self.remaining

    def restore(self, checkpoint: int | None) -> None:
        self.remaining = checkpoint


def encode_value(value: Any, *, max_bytes: int | None = None) -> bytes:
    """Encode one value into a single growing buffer with an explicit depth bound."""
    if max_bytes is not None and (
        isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 0
    ):
        raise ValueError("max_bytes must be a non-negative integer or None")
    stream = io.BytesIO()
    _write_value(_ValueWriter(stream.write, max_bytes=max_bytes), value, depth=0)
    return stream.getvalue()


def encode_value_into(
    value: Any,
    write: Callable[[bytes | bytearray], Any],
    *,
    max_bytes: int | None = None,
) -> None:
    """Stream one encoded value to a sink, optionally stopping at a byte bound."""
    if max_bytes is not None and (
        isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 0
    ):
        raise ValueError("max_bytes must be a non-negative integer or None")
    _write_value(_ValueWriter(write, max_bytes=max_bytes), value, depth=0)


def decode_value(
    data: bytes,
    *,
    max_collection_items: int | None = None,
) -> tuple[Any, bytes]:
    """Decode one value and return its unconsumed suffix."""
    if not data:
        raise FerricStoreError("protocol value is empty")
    value, offset = decode_value_at(
        data,
        0,
        max_collection_items=max_collection_items,
    )
    return value, data[offset:]


def decode_value_at(
    data: bytes,
    offset: int,
    *,
    max_collection_items: int | None = None,
    budget: DecodeBudget | None = None,
) -> tuple[Any, int]:
    """Decode one value without repeatedly copying the remaining buffer."""
    if budget is not None and max_collection_items is not None:
        raise ValueError("budget and max_collection_items are mutually exclusive")
    active_budget = budget if budget is not None else DecodeBudget(max_collection_items)
    return _decode_value_at(data, offset, depth=0, budget=active_budget)


def _write_value(stream: _ValueWriter, value: Any, *, depth: int) -> None:
    if depth > MAX_VALUE_NESTING:
        raise FerricStoreError("protocol value nesting exceeds maximum depth")
    if value is None:
        stream.write(b"\x00")
        return
    if value is True:
        stream.write(b"\x01")
        return
    if value is False:
        stream.write(b"\x02")
        return
    if isinstance(value, int):
        stream.write(b"\x03")
        stream.write(_I64.pack(value))
        return
    if isinstance(value, str):
        _write_binary(stream, value.encode())
        return
    if isinstance(value, (bytes, bytearray)):
        _write_binary(stream, value)
        return
    if isinstance(value, (list, tuple)):
        stream.write(b"\x05")
        stream.write(_U32.pack(len(value)))
        for item in value:
            _write_value(stream, item, depth=depth + 1)
        return
    if isinstance(value, dict):
        stream.write(b"\x06")
        stream.write(_U32.pack(len(value)))
        for key, item in value.items():
            encoded_key = _key_bytes(key)
            stream.write(_U32.pack(len(encoded_key)))
            stream.write(encoded_key)
            _write_value(stream, item, depth=depth + 1)
        return
    if isinstance(value, float):
        stream.write(b"\x07")
        stream.write(_F64.pack(value))
        return
    _write_binary(stream, str(value).encode())


def _decode_value_at(
    data: bytes,
    offset: int,
    *,
    depth: int,
    budget: DecodeBudget,
) -> tuple[Any, int]:
    if depth > MAX_VALUE_NESTING:
        raise FerricStoreError("protocol value nesting exceeds maximum depth")
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
        _require_available(data, offset, _I64.size)
        return _I64.unpack_from(data, offset)[0], offset + _I64.size
    if tag == 4:
        return _read_binary(data, offset)
    if tag == 5:
        count = _read_u32(data, offset)
        offset += _U32.size
        budget.consume(count)
        list_values = []
        for _ in range(count):
            value, offset = _decode_value_at(
                data,
                offset,
                depth=depth + 1,
                budget=budget,
            )
            list_values.append(value)
        return list_values, offset
    if tag == 6:
        count = _read_u32(data, offset)
        offset += _U32.size
        budget.consume(count)
        map_values: dict[bytes, Any] = {}
        for _ in range(count):
            key, offset = _read_binary(data, offset)
            value, offset = _decode_value_at(
                data,
                offset,
                depth=depth + 1,
                budget=budget,
            )
            map_values[key] = value
        return map_values, offset
    if tag == 7:
        _require_available(data, offset, _F64.size)
        return _F64.unpack_from(data, offset)[0], offset + _F64.size
    raise FerricStoreError("protocol value has unknown tag")


def _write_binary(stream: _ValueWriter, value: bytes | bytearray) -> None:
    stream.write(b"\x04")
    stream.write(_U32.pack(len(value)))
    stream.write(value)


def _read_binary(data: bytes, offset: int) -> tuple[bytes, int]:
    size = _read_u32(data, offset)
    offset += _U32.size
    _require_available(data, offset, size)
    return data[offset : offset + size], offset + size


def _read_u32(data: bytes, offset: int) -> int:
    _require_available(data, offset, _U32.size)
    return int(_U32.unpack_from(data, offset)[0])


def _require_available(data: bytes, offset: int, size: int) -> None:
    if offset < 0 or size < 0 or offset > len(data) - size:
        raise FerricStoreError("protocol value is truncated")


def _key_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    return str(value).encode()
