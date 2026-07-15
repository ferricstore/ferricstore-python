from __future__ import annotations

import struct
from typing import cast

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_codec import DecodeBudget, DuplicateProtocolMapKeyError
from ferricstore.protocol_constants import _COMPACT_U32, _NULL_U32
from ferricstore.protocol_response_primitives import _read_compact_binary, _read_u32


def _try_decode_custom_kv_get(data: bytes, offset: int = 0) -> bytes | None:
    try:
        offset += 1
        present = data[offset]
        offset += 1
        if present == 0:
            return None
        if present != 1:
            return None
        value, offset = _read_compact_binary(data, offset)
        if offset != len(data):
            return None
        return value
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_kv_mget(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[bytes | None] | None:
    offset += 1
    data_len = len(data)
    if offset + 4 > data_len:
        return None

    unpack_u32 = _COMPACT_U32.unpack_from
    count = unpack_u32(data, offset)[0]
    offset += 4
    budget.consume(count)
    values: list[bytes | None] = []
    append_value = values.append

    for _ in range(count):
        if offset >= data_len:
            return None

        present = data[offset]
        offset += 1

        if present == 1:
            if offset + 4 > data_len:
                return None

            size = unpack_u32(data, offset)[0]
            offset += 4
            end = offset + size
            if size == _NULL_U32 or end > data_len:
                return None

            append_value(data[offset:end])
            offset = end
        elif present == 0:
            append_value(None)
        else:
            return None

    if offset != data_len:
        return None
    return values


def _try_decode_custom_kv_mget_fixed(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[bytes | None] | None:
    offset += 1
    data_len = len(data)
    if offset + 8 > data_len:
        return None

    unpack_u32 = _COMPACT_U32.unpack_from
    count = unpack_u32(data, offset)[0]
    offset += 4
    budget.consume(count)
    size = unpack_u32(data, offset)[0]
    offset += 4

    payload_len = data_len - offset
    if size == _NULL_U32 or payload_len != count * size:
        return None

    if count == 0:
        return [] if offset == data_len else None
    if size == 0:
        return [b""] * count

    end = offset + payload_len
    return [data[item_offset : item_offset + size] for item_offset in range(offset, end, size)]


def _try_decode_custom_binary_list_list(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[list[bytes]] | None:
    try:
        offset += 1
        data_len = len(data)
        if offset + 4 > data_len:
            return None
        unpack_u32 = _COMPACT_U32.unpack_from
        count = unpack_u32(data, offset)[0]
        offset += 4
        budget.consume(count)
        values: list[list[bytes] | None] = [None] * count

        for outer_index in range(count):
            if offset + 4 > data_len:
                return None
            inner_count = unpack_u32(data, offset)[0]
            offset += 4
            budget.consume(inner_count)

            if inner_count == 0:
                values[outer_index] = []
                continue

            if inner_count == 1:
                if offset + 4 > data_len:
                    return None
                size = unpack_u32(data, offset)[0]
                offset += 4
                if size == _NULL_U32 or offset + size > data_len:
                    return None
                values[outer_index] = [data[offset : offset + size]]
                offset += size
                continue

            if inner_count > (data_len - offset) // 4:
                return None
            inner_values = [b""] * inner_count
            for inner_index in range(inner_count):
                if offset + 4 > data_len:
                    return None
                size = unpack_u32(data, offset)[0]
                offset += 4
                if size == _NULL_U32 or offset + size > data_len:
                    return None
                inner_values[inner_index] = data[offset : offset + size]
                offset += size
            values[outer_index] = inner_values

        if offset != data_len:
            return None
        return cast(list[list[bytes]], values)
    except (IndexError, struct.error):
        return None


def _try_decode_custom_binary_map_list(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[dict[bytes, bytes]] | None:
    try:
        offset += 1
        data_len = len(data)
        if offset + 4 > data_len:
            return None
        unpack_u32 = _COMPACT_U32.unpack_from
        count = unpack_u32(data, offset)[0]
        offset += 4
        budget.consume(count)
        values: list[dict[bytes, bytes] | None] = [None] * count

        for outer_index in range(count):
            if offset + 4 > data_len:
                return None
            item_count = unpack_u32(data, offset)[0]
            offset += 4
            budget.consume(item_count)

            if item_count == 0:
                values[outer_index] = {}
                continue

            items: dict[bytes, bytes] = {}
            for _ in range(item_count):
                if offset + 4 > data_len:
                    return None
                key_size = unpack_u32(data, offset)[0]
                offset += 4
                if key_size == _NULL_U32 or offset + key_size > data_len:
                    return None
                key = data[offset : offset + key_size]
                offset += key_size
                if key in items:
                    raise DuplicateProtocolMapKeyError("duplicate protocol map key while decoding")

                if offset + 4 > data_len:
                    return None
                value_size = unpack_u32(data, offset)[0]
                offset += 4
                if value_size == _NULL_U32 or offset + value_size > data_len:
                    return None
                items[key] = data[offset : offset + value_size]
                offset += value_size

            values[outer_index] = items

        if offset != data_len:
            return None
        return cast(list[dict[bytes, bytes]], values)
    except (IndexError, struct.error):
        return None


def _read_custom_binary_list(
    data: bytes,
    offset: int,
    *,
    budget: DecodeBudget,
) -> tuple[list[bytes], int]:
    count = _read_u32(data, offset)
    offset += 4
    budget.consume(count)
    values: list[bytes] = []
    for _ in range(count):
        value, offset = _read_compact_binary(data, offset)
        values.append(value)
    return values, offset


def _read_custom_binary_map(
    data: bytes,
    offset: int,
    *,
    budget: DecodeBudget,
) -> tuple[dict[bytes, bytes], int]:
    count = _read_u32(data, offset)
    offset += 4
    budget.consume(count)
    values: dict[bytes, bytes] = {}
    for _ in range(count):
        key, offset = _read_compact_binary(data, offset)
        if key in values:
            raise DuplicateProtocolMapKeyError("duplicate protocol map key while decoding")
        value, offset = _read_compact_binary(data, offset)
        values[key] = value
    return values, offset


__all__ = [
    "_read_custom_binary_list",
    "_read_custom_binary_map",
    "_try_decode_custom_binary_list_list",
    "_try_decode_custom_binary_map_list",
    "_try_decode_custom_kv_get",
    "_try_decode_custom_kv_mget",
    "_try_decode_custom_kv_mget_fixed",
]
