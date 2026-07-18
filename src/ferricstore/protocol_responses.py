from __future__ import annotations

import struct
import time
from collections.abc import Callable
from typing import Any, cast

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_codec import (
    DecodeBudget,
    DecodedCollectionLimitError,
    DuplicateProtocolMapKeyError,
)
from ferricstore.protocol_codec import (
    decode_value_at as _decode_value_at,
)
from ferricstore.protocol_common import (
    _map_get,
    _pop_response_item_count,
)
from ferricstore.protocol_constants import (
    _COMPACT_BINARY_LIST_LIST,
    _COMPACT_BINARY_MAP_LIST,
    _COMPACT_COLLECTION_MARKERS,
    _COMPACT_COLLECTION_MIN_ITEM_BYTES,
    _COMPACT_F64,
    _COMPACT_FLOW_CLAIM_JOBS,
    _COMPACT_FLOW_RECORD,
    _COMPACT_FLOW_RECORD_LIST,
    _COMPACT_I64,
    _COMPACT_INTEGER_LIST,
    _COMPACT_KV_GET,
    _COMPACT_KV_MGET,
    _COMPACT_KV_MGET_FIXED,
    _COMPACT_OK_LIST,
    _COMPACT_PIPELINE_RESPONSE,
    _COMPACT_U32,
    _DEFAULT_MAX_DECODED_COLLECTION_ITEMS,
    _DEFAULT_MAX_DECOMPRESSED_RESPONSE_BYTES,
    _FLAG_COMPRESSED,
    _FLAG_TRACE,
    _FLOW_RECORD_FIELD_KEYS,
    _FLOW_RECORD_FIELD_KEYS_LEN,
    _NULL_U32,
    _OP_FLOW_CLAIM_DUE,
    _OP_MSET,
    _OP_PIPELINE,
    _OP_SET,
    _STATUS,
    _STATUS_OK,
    ProtocolResponse,
)
from ferricstore.protocol_framing import (
    decompress_response as _decompress_response,
)
from ferricstore.protocol_response_collections import (
    _read_custom_binary_list,
    _read_custom_binary_map,
    _try_decode_custom_binary_list_list,
    _try_decode_custom_binary_map_list,
    _try_decode_custom_kv_get,
    _try_decode_custom_kv_mget,
    _try_decode_custom_kv_mget_fixed,
)
from ferricstore.protocol_response_contracts import (
    require_compact_collection_count as _require_compact_collection_count,
)
from ferricstore.protocol_response_contracts import validate_response_cardinality
from ferricstore.protocol_response_primitives import (
    _read_compact_binary,
    _read_compact_optional_binary,
    _read_tagged_binary,
    _read_tagged_i64,
    _read_u32,
    _require_available,
)
from ferricstore.protocol_response_values import (
    _batch_item_value,
    _flow_many_group_values,
    _ok_scalar,
    _pipeline_pair_list,
    _response_value,
    _status_text,
)


def _preflight_compact_collection(
    data: bytes,
    offset: int,
    *,
    max_collection_items: int | None,
    expected_collection_items: int | None,
) -> bool:
    if offset >= len(data) or data[offset] not in _COMPACT_COLLECTION_MARKERS:
        return True
    if offset + 5 > len(data):
        return False
    marker = data[offset]
    count = int(_COMPACT_U32.unpack_from(data, offset + 1)[0])
    _require_compact_collection_count(
        count,
        max_collection_items=max_collection_items,
        expected_collection_items=expected_collection_items,
    )
    minimum_item_bytes = _COMPACT_COLLECTION_MIN_ITEM_BYTES.get(marker)
    if minimum_item_bytes is None:
        return True
    remaining = len(data) - (offset + 5)
    return count <= remaining // minimum_item_bytes


def _try_fast_response_value(
    opcode: int,
    data: bytes,
    *,
    max_collection_items: int | None = _DEFAULT_MAX_DECODED_COLLECTION_ITEMS,
    expected_collection_items: int | None = None,
) -> Any | None:
    return _try_fast_response_value_at(
        opcode,
        data,
        0,
        max_collection_items=max_collection_items,
        expected_collection_items=expected_collection_items,
    )


def _try_fast_response_value_at(
    opcode: int,
    data: bytes,
    offset: int,
    *,
    max_collection_items: int | None = _DEFAULT_MAX_DECODED_COLLECTION_ITEMS,
    expected_collection_items: int | None = None,
) -> Any | None:
    """Decode compact data for legacy direct codec tests.

    Production response decoding uses HELLO's codec name for the opcode.  This
    marker-based helper remains for compatibility with the SDK's low-level
    codec tests and does not act as a capability table.
    """
    codec_name = _legacy_compact_codec_name(opcode, data, offset)
    if codec_name is None:
        return None
    return _try_compact_response_value_at(
        codec_name,
        opcode,
        data,
        offset,
        max_collection_items=max_collection_items,
        expected_collection_items=expected_collection_items,
    )


def _try_compact_response_value_at(
    codec_name: str,
    opcode: int,
    data: bytes,
    offset: int,
    *,
    max_collection_items: int | None,
    expected_collection_items: int | None,
) -> Any | None:
    if not _preflight_compact_collection(
        data,
        offset,
        max_collection_items=max_collection_items,
        expected_collection_items=expected_collection_items,
    ):
        return None
    decoder = _COMPACT_RESPONSE_CODEC_DECODERS.get(codec_name)
    if decoder is None:
        return None
    budget = DecodeBudget(max_collection_items)
    if codec_name == "ok_list_v1":
        values = _try_decode_custom_ok_list(data, offset, budget=budget)
        if values is None:
            return None
        if opcode in {_OP_SET, _OP_MSET}:
            return b"OK" if len(values) == 1 else None
        return values
    return decoder(data, offset, budget)


_FastResponseDecoder = Callable[[bytes, int, DecodeBudget], Any | None]
_COMPACT_RESPONSE_CODEC_DECODERS: dict[str, _FastResponseDecoder] = {}
_PIPELINE_MARKER_DECODERS: dict[int, Callable[..., Any | None]] = {}
_MGET_MARKER_DECODERS: dict[int, Callable[..., Any | None]] = {}


def _legacy_compact_codec_name(opcode: int, data: bytes, offset: int) -> str | None:
    if offset >= len(data):
        return None
    marker = data[offset]
    if opcode == _OP_PIPELINE:
        return "pipeline_v1"
    if marker == _COMPACT_KV_GET:
        return "kv_get_v1"
    if marker in {_COMPACT_KV_MGET, _COMPACT_KV_MGET_FIXED}:
        return "kv_mget_v1"
    if marker == _COMPACT_FLOW_RECORD:
        return "flow_record_v1"
    if marker == _COMPACT_FLOW_RECORD_LIST:
        return "flow_record_list_v1"
    if marker == _COMPACT_FLOW_CLAIM_JOBS or opcode == _OP_FLOW_CLAIM_DUE:
        return "flow_claim_jobs_v1"
    if marker == _COMPACT_OK_LIST:
        return "ok_list_v1"
    return None


def _try_fast_pipeline(data: bytes, offset: int, budget: DecodeBudget) -> Any | None:
    if offset >= len(data):
        return None
    decoder = _PIPELINE_MARKER_DECODERS.get(data[offset])
    return None if decoder is None else decoder(data, offset, budget=budget)


def _try_fast_get(data: bytes, offset: int, _budget: DecodeBudget) -> Any | None:
    if offset < len(data) and data[offset] == _COMPACT_KV_GET:
        return _try_decode_custom_kv_get(data, offset)
    return None


def _try_fast_ok(data: bytes, offset: int, budget: DecodeBudget) -> Any | None:
    if offset >= len(data) or data[offset] != _COMPACT_OK_LIST:
        return None
    values = _try_decode_custom_ok_list(data, offset, budget=budget)
    return b"OK" if values is not None and len(values) == 1 else None


def _try_fast_mget(data: bytes, offset: int, budget: DecodeBudget) -> Any | None:
    if offset >= len(data):
        return None
    decoder = _MGET_MARKER_DECODERS.get(data[offset])
    return None if decoder is None else decoder(data, offset, budget=budget)


def _try_fast_flow_record(data: bytes, offset: int, budget: DecodeBudget) -> Any | None:
    if offset < len(data) and data[offset] == _COMPACT_FLOW_RECORD:
        return _try_decode_custom_flow_record(data, offset, budget=budget)
    return None


def _try_fast_flow_record_list(
    data: bytes,
    offset: int,
    budget: DecodeBudget,
) -> Any | None:
    if offset < len(data) and data[offset] == _COMPACT_FLOW_RECORD_LIST:
        return _try_decode_custom_flow_record_list(data, offset, budget=budget)
    return None


def _try_fast_claim(data: bytes, offset: int, budget: DecodeBudget) -> Any | None:
    if offset < len(data) and data[offset] == _COMPACT_FLOW_CLAIM_JOBS:
        return _try_decode_custom_claim_jobs(data, offset, budget=budget)
    return _try_decode_claim_jobs_compact(data, offset, budget=budget)


def _try_fast_many(data: bytes, offset: int, budget: DecodeBudget) -> Any | None:
    if offset < len(data) and data[offset] == _COMPACT_OK_LIST:
        return _try_decode_custom_ok_list(data, offset, budget=budget)
    return _try_decode_binary_list(data, offset, budget=budget)


def _try_decode_custom_pipeline_response(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[list[Any]] | None:
    try:
        offset += 1
        count = struct.unpack_from(">I", data, offset)[0]
        offset += 4
        budget.consume(count)
        values: list[list[Any]] = []

        for _ in range(count):
            budget.consume(2)
            status_code = data[offset]
            offset += 1

            if status_code == 0:
                present = data[offset]
                offset += 1
                if present == 0:
                    values.append(["ok", None])
                elif present == 1:
                    binary_value, offset = _read_compact_binary(data, offset)
                    values.append(["ok", binary_value])
                elif present == 2:
                    record_value, offset = _read_custom_flow_record(data, offset, budget=budget)
                    values.append(["ok", record_value])
                elif present == 3:
                    record_list_value, offset = _read_custom_flow_record_list(
                        data,
                        offset,
                        budget=budget,
                    )
                    values.append(["ok", record_list_value])
                elif present == 4:
                    claim_value, offset = _read_custom_claim_job(data, offset, budget=budget)
                    values.append(["ok", claim_value])
                elif present == 5:
                    ref_value, offset = _read_custom_flow_value_ref(data, offset, budget=budget)
                    values.append(["ok", ref_value])
                elif present == 6:
                    binary_list_value, offset = _read_custom_binary_list(
                        data,
                        offset,
                        budget=budget,
                    )
                    values.append(["ok", binary_list_value])
                elif present == 7:
                    binary_map_value, offset = _read_custom_binary_map(
                        data,
                        offset,
                        budget=budget,
                    )
                    values.append(["ok", binary_map_value])
                else:
                    return None
            elif status_code in (1, 2):
                reason, offset = _read_compact_binary(data, offset)
                values.append(["busy" if status_code == 1 else "error", reason])
            else:
                return None

        return values if offset == len(data) else None
    except (IndexError, struct.error, ValueError):
        return None


def _is_custom_compact_nil(codec_name: str | None, data: bytes, offset: int) -> bool:
    return (
        codec_name == "kv_get_v1"
        and len(data) == offset + 2
        and data[offset] == _COMPACT_KV_GET
        and data[offset + 1] == 0
    )


def _try_decode_custom_claim_jobs(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[list[Any]] | None:
    try:
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        budget.consume(count)
        checkpoint = budget.checkpoint()
        limit_error: DecodedCollectionLimitError | None = None
        for width in (6, 5, 4):
            budget.restore(checkpoint)
            try:
                decoded = _try_decode_custom_claim_jobs_width(
                    data,
                    offset,
                    count,
                    width,
                    budget=budget,
                )
            except DecodedCollectionLimitError as exc:
                if limit_error is None:
                    limit_error = exc
                continue
            if decoded is not None:
                return decoded
        if limit_error is not None:
            raise limit_error
        return None
    except DecodedCollectionLimitError:
        raise
    except DuplicateProtocolMapKeyError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_claim_jobs_width(
    data: bytes,
    offset: int,
    count: int,
    width: int,
    *,
    budget: DecodeBudget,
) -> list[list[Any]] | None:
    try:
        items: list[list[Any] | None] = [None] * count
        for index in range(count):
            budget.consume(width)
            id_value, offset = _read_compact_binary(data, offset)
            partition, offset = _read_compact_optional_binary(data, offset)
            lease, offset = _read_compact_binary(data, offset)
            _require_available(data, offset, 8)
            fencing = struct.unpack_from(">q", data, offset)[0]
            offset += 8
            row: list[Any] = [id_value, partition, lease, fencing]
            if width == 5:
                attrs, offset = _decode_value_at(data, offset, budget=budget)
                if not isinstance(attrs, dict):
                    return None
                row.append(attrs)
            elif width == 6:
                run_state, offset = _read_compact_optional_binary(data, offset)
                attrs, offset = _decode_value_at(data, offset, budget=budget)
                if not isinstance(attrs, dict):
                    return None
                row.extend([run_state, attrs])
            items[index] = row
        if offset != len(data):
            return None
        return cast(list[list[Any]], items)
    except DecodedCollectionLimitError:
        raise
    except DuplicateProtocolMapKeyError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _read_custom_claim_job(
    data: bytes,
    offset: int,
    *,
    budget: DecodeBudget,
) -> tuple[list[Any], int]:
    budget.consume(4)
    id_value, offset = _read_compact_binary(data, offset)
    partition, offset = _read_compact_optional_binary(data, offset)
    lease, offset = _read_compact_binary(data, offset)
    _require_available(data, offset, 8)
    fencing = struct.unpack_from(">q", data, offset)[0]
    offset += 8
    return [id_value, partition, lease, fencing], offset


def _read_custom_flow_value_ref(
    data: bytes,
    offset: int,
    *,
    budget: DecodeBudget,
) -> tuple[dict[bytes, Any], int]:
    ref, offset = _read_compact_binary(data, offset)
    partition_key, offset = _read_compact_optional_binary(data, offset)
    owner_flow_id, offset = _read_compact_optional_binary(data, offset)
    budget.consume(1 + (partition_key is not None) + (owner_flow_id is not None))
    value: dict[bytes, Any] = {b"ref": ref}
    if partition_key is not None:
        value[b"partition_key"] = partition_key
    if owner_flow_id is not None:
        value[b"owner_flow_id"] = owner_flow_id
    return value, offset


def _try_decode_custom_ok_list(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[bytes] | None:
    try:
        if len(data) - offset != 5:
            return None
        count = _read_u32(data, offset + 1)
        budget.consume(count)
        return [b"OK"] * count
    except DecodedCollectionLimitError:
        raise
    except DuplicateProtocolMapKeyError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_integer_list(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[int] | None:
    try:
        if data[offset] != _COMPACT_INTEGER_LIST:
            return None
        offset += 1
        data_len = len(data)
        if offset + 4 > data_len:
            return None
        count = _COMPACT_U32.unpack_from(data, offset)[0]
        offset += 4
        budget.consume(count)
        expected_len = offset + count * 8
        if expected_len != data_len:
            return None
        unpack_i64 = _COMPACT_I64.unpack_from
        values = [0] * count
        for index in range(count):
            values[index] = int(unpack_i64(data, offset)[0])
            offset += 8
        return values
    except DecodedCollectionLimitError:
        raise
    except DuplicateProtocolMapKeyError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_flow_record(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> dict[bytes, Any] | None:
    try:
        value, offset = _read_custom_flow_record(data, offset, budget=budget)
        return value if offset == len(data) else None
    except DecodedCollectionLimitError:
        raise
    except DuplicateProtocolMapKeyError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_flow_record_list(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[dict[bytes, Any]] | None:
    try:
        value, offset = _read_custom_flow_record_list(data, offset, budget=budget)
        return value if offset == len(data) else None
    except DecodedCollectionLimitError:
        raise
    except DuplicateProtocolMapKeyError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _read_custom_flow_record(
    data: bytes,
    offset: int,
    *,
    budget: DecodeBudget,
) -> tuple[dict[bytes, Any], int]:
    if data[offset] != _COMPACT_FLOW_RECORD:
        raise FerricStoreError("protocol compact value expected Flow record")
    offset += 1
    data_len = len(data)
    if offset + 4 > data_len:
        raise FerricStoreError("protocol compact Flow record is truncated")
    unpack_u32 = _COMPACT_U32.unpack_from
    count = unpack_u32(data, offset)[0]
    offset += 4
    budget.consume(count)
    record: dict[bytes, Any] = {}
    field_keys = _FLOW_RECORD_FIELD_KEYS
    field_keys_len = _FLOW_RECORD_FIELD_KEYS_LEN
    decode_record_value = _decode_flow_record_value_at
    for _ in range(count):
        if offset >= data_len:
            raise FerricStoreError("protocol compact Flow record is truncated")
        key_id = data[offset]
        offset += 1
        if key_id == 0:
            if offset + 4 > data_len:
                raise FerricStoreError("protocol compact Flow record key is truncated")
            size = unpack_u32(data, offset)[0]
            offset += 4
            if size == _NULL_U32 or offset + size > data_len:
                raise FerricStoreError("protocol compact Flow record key is invalid")
            key = data[offset : offset + size]
            offset += size
        elif key_id < field_keys_len:
            key = field_keys[key_id]
        else:
            key = None
        value, offset = decode_record_value(data, offset, data_len, budget=budget)
        if key is None:
            continue
        if key in record:
            raise DuplicateProtocolMapKeyError("duplicate protocol map key while decoding")
        record[key] = value
    return record, offset


def _decode_flow_record_value_at(
    data: bytes,
    offset: int,
    data_len: int,
    *,
    budget: DecodeBudget,
) -> tuple[Any, int]:
    if offset >= data_len:
        raise FerricStoreError("protocol value is truncated")
    tag = data[offset]
    offset += 1
    if tag == 0:
        return None, offset
    if tag == 1:
        return True, offset
    if tag == 2:
        return False, offset
    if tag == 3:
        if offset + 8 > data_len:
            raise FerricStoreError("protocol integer is truncated")
        return _COMPACT_I64.unpack_from(data, offset)[0], offset + 8
    if tag == 4:
        if offset + 4 > data_len:
            raise FerricStoreError("protocol binary is truncated")
        size = _COMPACT_U32.unpack_from(data, offset)[0]
        offset += 4
        if size == _NULL_U32 or offset + size > data_len:
            raise FerricStoreError("protocol compact value expected binary")
        return data[offset : offset + size], offset + size
    if tag == 5:
        if offset + 4 > data_len:
            raise FerricStoreError("protocol list is truncated")
        count = _COMPACT_U32.unpack_from(data, offset)[0]
        if count == 0:
            budget.consume(0)
            return [], offset + 4
        return _decode_value_at(data, offset - 1, budget=budget)
    if tag == 6:
        if offset + 4 > data_len:
            raise FerricStoreError("protocol map is truncated")
        count = _COMPACT_U32.unpack_from(data, offset)[0]
        if count == 0:
            budget.consume(0)
            return {}, offset + 4
        return _decode_value_at(data, offset - 1, budget=budget)
    if tag == 7:
        if offset + 8 > data_len:
            raise FerricStoreError("protocol float is truncated")
        return _COMPACT_F64.unpack_from(data, offset)[0], offset + 8
    return _decode_value_at(data, offset - 1, budget=budget)


def _read_custom_flow_record_list(
    data: bytes,
    offset: int,
    *,
    budget: DecodeBudget,
) -> tuple[list[dict[bytes, Any]], int]:
    if data[offset] != _COMPACT_FLOW_RECORD_LIST:
        raise FerricStoreError("protocol compact value expected Flow record list")
    offset += 1
    data_len = len(data)
    if offset + 4 > data_len:
        raise FerricStoreError("protocol compact Flow record list is truncated")
    count = _COMPACT_U32.unpack_from(data, offset)[0]
    offset += 4
    budget.consume(count)
    records: list[dict[bytes, Any] | None] = [None] * count
    for index in range(count):
        record, offset = _read_custom_flow_record(data, offset, budget=budget)
        records[index] = record
    return cast(list[dict[bytes, Any]], records), offset


def _try_decode_claim_jobs_compact(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget | None = None,
) -> list[list[Any]] | None:
    budget = budget if budget is not None else DecodeBudget(None)
    try:
        if data[offset] != 5:
            return None
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        budget.consume(count)
        items: list[list[Any]] = []
        for _ in range(count):
            if data[offset] != 5:
                return None
            offset += 1
            width = _read_u32(data, offset)
            offset += 4
            if width not in {4, 5, 6}:
                return None
            budget.consume(width)
            id_value, offset = _read_tagged_binary(data, offset)
            partition, offset = _read_tagged_binary(data, offset)
            lease, offset = _read_tagged_binary(data, offset)
            fencing, offset = _read_tagged_i64(data, offset)
            row: list[Any] = [id_value, partition, lease, fencing]
            if width == 5:
                attrs, offset = _decode_value_at(data, offset, budget=budget)
                if not isinstance(attrs, dict):
                    return None
                row.append(attrs)
            elif width == 6:
                run_state, offset = _read_compact_optional_binary(data, offset)
                attrs, offset = _decode_value_at(data, offset, budget=budget)
                if not isinstance(attrs, dict):
                    return None
                row.extend([run_state, attrs])
            items.append(row)
        if offset != len(data):
            return None
        return items
    except DecodedCollectionLimitError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_binary_list(
    data: bytes,
    offset: int = 0,
    *,
    budget: DecodeBudget,
) -> list[bytes] | None:
    try:
        if data[offset] != 5:
            return None
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        budget.consume(count)
        items: list[bytes] = []
        for _ in range(count):
            item, offset = _read_tagged_binary(data, offset)
            items.append(item)
        if offset != len(data):
            return None
        return items
    except DecodedCollectionLimitError:
        raise
    except (IndexError, struct.error, FerricStoreError):
        return None


_PIPELINE_MARKER_DECODERS.update(
    {
        _COMPACT_PIPELINE_RESPONSE: _try_decode_custom_pipeline_response,
        _COMPACT_KV_MGET: _try_decode_custom_kv_mget,
        _COMPACT_KV_MGET_FIXED: _try_decode_custom_kv_mget_fixed,
        _COMPACT_FLOW_RECORD_LIST: _try_decode_custom_flow_record_list,
        _COMPACT_FLOW_CLAIM_JOBS: _try_decode_custom_claim_jobs,
        _COMPACT_BINARY_LIST_LIST: _try_decode_custom_binary_list_list,
        _COMPACT_BINARY_MAP_LIST: _try_decode_custom_binary_map_list,
        _COMPACT_INTEGER_LIST: _try_decode_custom_integer_list,
        _COMPACT_OK_LIST: _try_decode_custom_ok_list,
    }
)
_MGET_MARKER_DECODERS.update(
    {
        _COMPACT_KV_MGET: _try_decode_custom_kv_mget,
        _COMPACT_KV_MGET_FIXED: _try_decode_custom_kv_mget_fixed,
    }
)
_COMPACT_RESPONSE_CODEC_DECODERS.update(
    {
        "pipeline_v1": _try_fast_pipeline,
        "kv_get_v1": _try_fast_get,
        "kv_mget_v1": _try_fast_mget,
        "flow_record_v1": _try_fast_flow_record,
        "flow_record_list_v1": _try_fast_flow_record_list,
        "flow_claim_jobs_v1": _try_fast_claim,
        "ok_list_v1": _try_fast_many,
    }
)


def _extract_traced_value(value: Any) -> tuple[Any, dict[str, Any]]:
    if not isinstance(value, dict):
        return value, {}

    unwrapped = _map_get(value, "value")
    trace = _map_get(value, "trace")
    if trace is None:
        return value, {}
    if not isinstance(trace, dict):
        trace = {}
    return unwrapped, _normalize_trace_map(trace)


def _decode_protocol_response(
    adapter: Any,
    *,
    lane_id: int,
    opcode: int,
    request_id: int,
    flags: int,
    body: bytes,
    read_started_ns: int,
    read_done_ns: int,
) -> ProtocolResponse:
    """Decode one assembled logical response for both sync and async transports."""
    decode_started_ns = read_done_ns
    if flags & _FLAG_COMPRESSED:
        body = _decompress_response(
            body,
            getattr(
                adapter,
                "max_decompressed_response_bytes",
                _DEFAULT_MAX_DECOMPRESSED_RESPONSE_BYTES,
            ),
        )

    if len(body) < _STATUS.size:
        raise FerricStoreError("protocol response body is too short")

    status = _STATUS.unpack_from(body, 0)[0]
    collection_limit = getattr(
        adapter,
        "max_decoded_collection_items",
        _DEFAULT_MAX_DECODED_COLLECTION_ITEMS,
    )
    expected_collection_items = _pop_response_item_count(adapter, request_id)
    compact_codec = getattr(adapter, "_compact_response_codecs", {}).get(opcode)
    value = (
        _try_compact_response_value_at(
            compact_codec,
            opcode,
            body,
            _STATUS.size,
            max_collection_items=collection_limit,
            expected_collection_items=expected_collection_items,
        )
        if status == _STATUS_OK and compact_codec is not None
        else None
    )
    fast_decoded = value is not None or _is_custom_compact_nil(
        compact_codec,
        body,
        _STATUS.size,
    )
    if not fast_decoded:
        value, value_end = _decode_value_at(
            body,
            _STATUS.size,
            budget=DecodeBudget(collection_limit),
        )
        if value_end != len(body):
            raise FerricStoreError("protocol response value has trailing bytes")
    decode_done_ns = time.perf_counter_ns()

    trace = None
    if flags & _FLAG_TRACE:
        value, server_trace = _extract_traced_value(value)
        trace = {
            "client": {
                "response_read_us": (read_done_ns - read_started_ns) // 1000,
                "decode_us": (decode_done_ns - decode_started_ns) // 1000,
            },
            "server": server_trace,
        }
    if status == _STATUS_OK:
        validate_response_cardinality(opcode, value, expected_collection_items)

    return ProtocolResponse(
        lane_id=lane_id,
        opcode=opcode,
        request_id=request_id,
        flags=flags,
        status=status,
        value=value,
        trace=trace,
    )


def _normalize_trace_map(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(key, bytes):
                trace_key = key.decode("utf-8", errors="replace")
            else:
                trace_key = str(key)
            normalized[trace_key] = _normalize_trace_map(item)
        return normalized
    return value


__all__ = [
    "_batch_item_value",
    "_decode_flow_record_value_at",
    "_decode_protocol_response",
    "_extract_traced_value",
    "_flow_many_group_values",
    "_is_custom_compact_nil",
    "_normalize_trace_map",
    "_ok_scalar",
    "_pipeline_pair_list",
    "_preflight_compact_collection",
    "_read_compact_binary",
    "_read_compact_optional_binary",
    "_read_custom_binary_list",
    "_read_custom_binary_map",
    "_read_custom_claim_job",
    "_read_custom_flow_record",
    "_read_custom_flow_record_list",
    "_read_custom_flow_value_ref",
    "_read_tagged_binary",
    "_read_tagged_i64",
    "_read_u32",
    "_require_available",
    "_require_compact_collection_count",
    "_response_value",
    "_status_text",
    "_try_decode_binary_list",
    "_try_decode_claim_jobs_compact",
    "_try_decode_custom_binary_list_list",
    "_try_decode_custom_binary_map_list",
    "_try_decode_custom_claim_jobs",
    "_try_decode_custom_claim_jobs_width",
    "_try_decode_custom_flow_record",
    "_try_decode_custom_flow_record_list",
    "_try_decode_custom_integer_list",
    "_try_decode_custom_kv_get",
    "_try_decode_custom_kv_mget",
    "_try_decode_custom_kv_mget_fixed",
    "_try_decode_custom_ok_list",
    "_try_decode_custom_pipeline_response",
    "_try_fast_response_value",
    "_try_fast_response_value_at",
]
