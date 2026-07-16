from __future__ import annotations

import io
import struct
from collections.abc import Callable
from typing import Any, Final

from ferricstore.config_validation import validate_optional_nonnegative_int
from ferricstore.errors import FerricStoreError

MAX_VALUE_NESTING: Final = 128
_U32 = struct.Struct(">I")
_I64 = struct.Struct(">q")
_F64 = struct.Struct(">d")
_U32_MAX: Final = 2**32 - 1
_I64_MIN: Final = -(2**63)
_I64_MAX: Final = 2**63 - 1


class DecodedCollectionLimitError(FerricStoreError):
    """Raised before a decoded collection can exceed the configured item budget."""


class EncodedValueLimitError(FerricStoreError):
    """Raised before an encoded value can exceed its caller-provided byte budget."""


class DuplicateProtocolMapKeyError(FerricStoreError):
    """Raised when one wire map contains the same canonical key more than once."""


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
        size = memoryview(value).nbytes
        self.ensure_capacity(size)
        self._write(value)
        self._size += size

    def ensure_capacity(self, size: int) -> None:
        if self._max_bytes is not None and size > self._max_bytes - self._size:
            raise EncodedValueLimitError("encoded protocol value exceeds max_bytes")

    @property
    def remaining_capacity(self) -> int | None:
        if self._max_bytes is None:
            return None
        return self._max_bytes - self._size


class DecodeBudget:
    """One cumulative item budget shared by every nested protocol collection."""

    __slots__ = ("remaining",)

    def __init__(self, max_collection_items: int | None) -> None:
        self.remaining = validate_optional_nonnegative_int(
            max_collection_items,
            name="max_collection_items",
        )

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
    max_bytes = validate_optional_nonnegative_int(max_bytes, name="max_bytes")
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
    max_bytes = validate_optional_nonnegative_int(max_bytes, name="max_bytes")
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
        if value < _I64_MIN or value > _I64_MAX:
            raise FerricStoreError("protocol integer must fit in a signed 64-bit value")
        stream.write(b"\x03")
        stream.write(_I64.pack(value))
        return
    if isinstance(value, str):
        remaining = stream.remaining_capacity
        if remaining is not None:
            stream.ensure_capacity(5)
            _require_utf8_within(value, remaining - 5)
        try:
            encoded = str.encode(value)
        except UnicodeEncodeError as exc:
            raise FerricStoreError("protocol strings must contain valid UTF-8") from exc
        _write_binary(stream, encoded)
        return
    if isinstance(value, (bytes, bytearray)):
        _write_binary(stream, value)
        return
    if isinstance(value, list):
        count = list.__len__(value)
        _write_collection_header(stream, b"\x05", count)
        for item in list.__iter__(value):
            _write_value(stream, item, depth=depth + 1)
        return
    if isinstance(value, tuple):
        count = tuple.__len__(value)
        _write_collection_header(stream, b"\x05", count)
        for item in tuple.__iter__(value):
            _write_value(stream, item, depth=depth + 1)
        return
    if isinstance(value, dict):
        count = dict.__len__(value)
        _write_collection_header(stream, b"\x06", count)
        encoded_keys: set[bytes] = set()
        for key, item in dict.items(value):
            encoded_key = _key_bytes(key)
            if encoded_key in encoded_keys:
                raise DuplicateProtocolMapKeyError("duplicate protocol map key after wire encoding")
            encoded_keys.add(encoded_key)
            _write_binary_length(stream, len(encoded_key))
            stream.write(encoded_key)
            _write_value(stream, item, depth=depth + 1)
        return
    if isinstance(value, float):
        stream.write(b"\x07")
        stream.write(_F64.pack(value))
        return
    raise FerricStoreError(f"unsupported protocol value type: {type(value).__name__}")


def _write_collection_header(stream: _ValueWriter, tag: bytes, count: int) -> None:
    if count > _U32_MAX:
        raise FerricStoreError("protocol collection must fit in an unsigned 32-bit count")
    stream.write(tag)
    stream.write(_U32.pack(count))


def _require_utf8_within(value: str, limit: int) -> None:
    """Reject an oversized UTF-8 string without allocating its encoded copy."""
    length = str.__len__(value)
    # Every Unicode code point needs at least one UTF-8 byte.  This cheap bound
    # avoids both ``isascii`` and the exact byte-count scan when character count
    # alone proves that the value cannot fit.
    if length > limit:
        raise EncodedValueLimitError("encoded protocol value exceeds max_bytes")
    if str.isascii(value):
        return

    size = 0
    for index in range(length):
        codepoint = ord(str.__getitem__(value, index))
        if codepoint <= 0x7F:
            size += 1
        elif codepoint <= 0x7FF:
            size += 2
        elif codepoint <= 0xFFFF:
            size += 3
        else:
            size += 4
        if size > limit:
            raise EncodedValueLimitError("encoded protocol value exceeds max_bytes")


def _write_binary_length(stream: _ValueWriter, size: int) -> None:
    if size > _U32_MAX:
        raise FerricStoreError("protocol binary must fit in an unsigned 32-bit length")
    stream.write(_U32.pack(size))


def _write_binary(stream: _ValueWriter, value: bytes | bytearray) -> None:
    size = memoryview(value).nbytes
    stream.write(b"\x04")
    _write_binary_length(stream, size)
    stream.write(value)


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
            if key in map_values:
                raise DuplicateProtocolMapKeyError("duplicate protocol map key while decoding")
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
        try:
            return value.encode()
        except UnicodeEncodeError as exc:
            raise FerricStoreError("protocol map keys must contain valid UTF-8") from exc
    raise FerricStoreError("protocol map keys must be str or bytes")
