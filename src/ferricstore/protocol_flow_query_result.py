from __future__ import annotations

import struct
from typing import Any, Final

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_codec import DecodeBudget, DecodedCollectionLimitError, decode_value_at

_TAG: Final = 0xA0
_MAX_RECORDS: Final = 100
_MIN_CURSOR_BYTES: Final = 16
_MAX_CURSOR_BYTES: Final = 4_096
_MAX_INTEGER: Final = 2**63 - 1
_NULL_U32: Final = 0xFFFF_FFFF
_U32 = struct.Struct(">I")
_U64 = struct.Struct(">Q")
_RECORD_FIELDS = (
    b"id",
    b"type",
    b"state",
    b"version",
    b"priority",
    b"partition_key",
    b"created_at_ms",
    b"updated_at_ms",
    b"next_run_at_ms",
    b"lease_deadline_ms",
    b"attempts",
    b"run_state",
    b"max_active_ms",
    b"parent_flow_id",
    b"root_flow_id",
    b"correlation_id",
    b"attributes",
    b"state_meta",
    b"event_id",
    b"fields",
)
_RECORD_FIELD_MASK: Final = (1 << len(_RECORD_FIELDS)) - 1
_QUALITY_FIELDS = (b"exactness", b"freshness", b"coverage", b"pagination")
_QUALITY_VALUES = (
    (b"authoritative", b"projected_exact", b"exact", b"not_applicable"),
    (b"current", b"projection_watermark", b"not_applicable"),
    (b"complete", b"unavailable"),
    (b"none", b"complete", b"authenticated_seek", b"live_seek"),
)
_USAGE_FIELDS = (
    b"range_seeks",
    b"range_pages",
    b"scanned_entries",
    b"scanned_bytes",
    b"hydrated_records",
    b"residual_checks",
    b"duplicate_entries",
    b"result_records",
    b"response_bytes",
    b"memory_high_water_bytes",
    b"wall_time_us",
)


def decode_compact_flow_query_result(
    data: bytes,
    offset: int,
    budget: DecodeBudget,
) -> dict[bytes, Any] | None:
    """Decode one negotiated fixed-schema FQL1 result without intermediate maps."""
    start = offset
    try:
        _require(data, offset, 2 + len(_QUALITY_FIELDS) + len(_USAGE_FIELDS) * _U64.size)
        if data[offset] != _TAG:
            return None
        kind = data[offset + 1]
        offset += 2

        quality: dict[bytes, bytes] = {}
        for field, values in zip(_QUALITY_FIELDS, _QUALITY_VALUES, strict=True):
            code = data[offset]
            offset += 1
            if code >= len(values):
                raise FerricStoreError("invalid compact FLOW.QUERY quality code")
            quality[field] = values[code]

        usage: dict[bytes, int] = {}
        for field in _USAGE_FIELDS:
            value, offset = _read_u64(data, offset)
            if value > _MAX_INTEGER:
                raise FerricStoreError("compact FLOW.QUERY usage exceeds signed 64-bit range")
            usage[field] = value
        if not _valid_usage(usage):
            raise FerricStoreError("compact FLOW.QUERY usage counters are inconsistent")

        if kind == 0:
            page, offset = _read_page(data, offset)
            count, offset = _read_u32(data, offset)
            if count > _MAX_RECORDS:
                raise FerricStoreError("compact FLOW.QUERY page exceeds 100 records")
            if usage[b"result_records"] != count or count > usage[b"scanned_entries"]:
                raise FerricStoreError("compact FLOW.QUERY record usage is inconsistent")
            budget.consume(count)
            records: list[dict[bytes, Any]] = []
            for _ in range(count):
                record, offset = _read_record(data, offset, budget)
                records.append(record)
            result: dict[bytes, Any] = {
                b"version": b"ferric.flow.query.result/v1",
                b"records": records,
                b"page": page,
                b"quality": quality,
                b"usage": usage,
            }
        elif kind == 1:
            if usage[b"result_records"] != 1:
                raise FerricStoreError("compact FLOW.QUERY count usage is inconsistent")
            count, offset = _read_u64(data, offset)
            if count > _MAX_INTEGER:
                raise FerricStoreError("compact FLOW.QUERY count exceeds signed 64-bit range")
            result = {
                b"version": b"ferric.flow.query.result/v1",
                b"result": {b"kind": b"count", b"value": count},
                b"quality": quality,
                b"usage": usage,
            }
        else:
            raise FerricStoreError(f"unsupported compact FLOW.QUERY result kind {kind}")

        if offset != len(data):
            raise FerricStoreError("compact FLOW.QUERY result has trailing bytes")
        if usage[b"response_bytes"] != len(data) - start:
            raise FerricStoreError("compact FLOW.QUERY response_bytes does not match payload")
        return result
    except DecodedCollectionLimitError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _read_page(data: bytes, offset: int) -> tuple[dict[bytes, Any], int]:
    _require(data, offset, 1 + _U32.size)
    has_more = data[offset]
    size = _U32.unpack_from(data, offset + 1)[0]
    offset += 1 + _U32.size
    if has_more == 0 and size == _NULL_U32:
        return {b"has_more": False, b"cursor": None}, offset
    if has_more != 1 or size < _MIN_CURSOR_BYTES or size == _NULL_U32 or size > _MAX_CURSOR_BYTES:
        raise FerricStoreError("invalid compact FLOW.QUERY page cursor")
    _require(data, offset, size)
    cursor = data[offset : offset + size]
    try:
        cursor.decode("utf-8")
    except UnicodeDecodeError as error:
        raise FerricStoreError("invalid compact FLOW.QUERY page cursor") from error
    if not cursor.startswith(b"fqc1_"):
        raise FerricStoreError("invalid compact FLOW.QUERY page cursor")
    return {b"has_more": True, b"cursor": cursor}, offset + size


def _valid_usage(usage: dict[bytes, int]) -> bool:
    scanned = usage[b"scanned_entries"]
    return (
        usage[b"hydrated_records"] <= scanned
        and usage[b"duplicate_entries"] <= scanned
        and usage[b"range_pages"] <= scanned + usage[b"range_seeks"]
        and usage[b"residual_checks"] <= scanned * 12
    )


def _read_record(
    data: bytes,
    offset: int,
    budget: DecodeBudget,
) -> tuple[dict[bytes, Any], int]:
    bitmap, offset = _read_u32(data, offset)
    if bitmap & ~_RECORD_FIELD_MASK:
        raise FerricStoreError("compact FLOW.QUERY record contains reserved fields")
    budget.consume(bitmap.bit_count())
    record: dict[bytes, Any] = {}
    for index, field in enumerate(_RECORD_FIELDS):
        if bitmap & (1 << index):
            value, offset = decode_value_at(data, offset, budget=budget)
            record[field] = value
    return record, offset


def _read_u32(data: bytes, offset: int) -> tuple[int, int]:
    _require(data, offset, _U32.size)
    return int(_U32.unpack_from(data, offset)[0]), offset + _U32.size


def _read_u64(data: bytes, offset: int) -> tuple[int, int]:
    _require(data, offset, _U64.size)
    return int(_U64.unpack_from(data, offset)[0]), offset + _U64.size


def _require(data: bytes, offset: int, size: int) -> None:
    if offset < 0 or size < 0 or offset > len(data) - size:
        raise FerricStoreError("compact FLOW.QUERY result is truncated")


__all__ = ["decode_compact_flow_query_result"]
