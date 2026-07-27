from __future__ import annotations

import json
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from ferricstore import protocol_flow_query_result as compact_query_result
from ferricstore.errors import FerricStoreError
from ferricstore.protocol_codec import DecodeBudget, DecodedCollectionLimitError, encode_value
from ferricstore.protocol_constants import (
    _FLAG_CUSTOM_PAYLOAD,
    _OPCODES,
    _STATUS,
    _STATUS_OK,
)
from ferricstore.protocol_flow_query_result import decode_compact_flow_query_result
from ferricstore.protocol_responses import _decode_protocol_response

_CODEC = "flow_query_result_v1"
_TAG = 0xA0
_U32 = struct.Struct(">I")
_U64 = struct.Struct(">Q")


def test_shared_server_golden_corpus_decodes_without_schema_drift() -> None:
    corpus = json.loads(
        (Path(__file__).parent / "fixtures" / "flow_query_result_v1.json").read_text()
    )

    assert corpus["tag"] == _TAG
    assert corpus["record_fields"] == [
        field.decode() for field in compact_query_result._RECORD_FIELDS
    ]
    assert corpus["quality_fields"] == [
        field.decode() for field in compact_query_result._QUALITY_FIELDS
    ]
    assert corpus["usage_fields"] == [
        field.decode() for field in compact_query_result._USAGE_FIELDS
    ]

    page_vector, count_vector = corpus["vectors"]
    page = _decode(bytes.fromhex(page_vector["payload_hex"]), _OPCODES["FLOW.QUERY"])
    count = _decode(
        bytes.fromhex(count_vector["payload_hex"]),
        _OPCODES["COMMAND_EXEC"],
    )

    assert page[b"records"] == [
        {b"id": b"run-1", b"state": b"failed", b"fields": {b"invoice_total": 42}}
    ]
    assert page[b"quality"] == {
        key.encode(): value.encode() for key, value in page_vector["quality"].items()
    }
    assert page[b"usage"] == {key.encode(): value for key, value in page_vector["usage"].items()}
    assert count[b"result"] == {
        b"kind": b"count",
        b"value": int(count_vector["count_decimal"]),
    }


def test_compact_flow_query_page_reconstructs_the_existing_contract() -> None:
    payload = _page_payload()
    response = _decode(payload, _OPCODES["FLOW.QUERY"])

    assert response[b"version"] == b"ferric.flow.query.result/v1"
    assert response[b"page"] == {b"has_more": False, b"cursor": None}
    assert response[b"quality"] == {
        b"exactness": b"authoritative",
        b"freshness": b"current",
        b"coverage": b"complete",
        b"pagination": b"authenticated_seek",
    }
    assert response[b"records"] == [
        {b"id": b"run-1", b"state": b"failed", b"fields": {b"invoice_total": 42}}
    ]
    assert response[b"usage"][b"result_records"] == 1
    assert response[b"usage"][b"response_bytes"] == len(payload)


def test_compact_flow_query_page_preserves_an_opaque_cursor() -> None:
    cursor = b"fqc1_" + b"x" * 11

    response = _decode(_page_payload(cursor=cursor), _OPCODES["FLOW.QUERY"])

    assert response[b"page"] == {b"has_more": True, b"cursor": cursor}


def test_compact_flow_query_count_preserves_full_signed_64_bit_range() -> None:
    count = 2**63 - 1
    response = _decode(_count_payload(count), _OPCODES["COMMAND_EXEC"])
    assert response[b"result"] == {b"kind": b"count", b"value": count}


def test_compact_flow_query_decoder_rejects_reserved_bits_truncation_and_trailing_bytes() -> None:
    valid = _page_payload()
    reserved = bytearray(valid)
    reserved[103:107] = _U32.pack(_U32.unpack_from(reserved, 103)[0] | 1 << 20)

    for invalid in (bytes(reserved), valid[:-1], valid + b"\x00"):
        with pytest.raises(FerricStoreError):
            _decode(invalid, _OPCODES["FLOW.QUERY"])


def test_compact_flow_query_decoder_rejects_invalid_usage_and_short_cursors() -> None:
    hydrated_beyond_scan = bytearray(_page_payload())
    hydrated_beyond_scan[38:46] = _U64.pack(2)
    wrong_record_count = bytearray(_page_payload())
    wrong_record_count[62:70] = _U64.pack(2)
    wrong_count_usage = bytearray(_count_payload(42))
    wrong_count_usage[62:70] = _U64.pack(0)

    for invalid in (
        bytes(hydrated_beyond_scan),
        bytes(wrong_record_count),
        bytes(wrong_count_usage),
        _page_payload(cursor=b"fqc1_short"),
        _page_payload(cursor=b"other_cursor_token"),
        _page_payload(cursor=b"fqc1_" + b"\xff" * 11),
    ):
        assert decode_compact_flow_query_result(invalid, 0, DecodeBudget(1_000)) is None


def test_compact_flow_query_tag_requires_negotiated_codec() -> None:
    with pytest.raises(FerricStoreError):
        _decode(_page_payload(), _OPCODES["FLOW.QUERY"], negotiated=False)


def test_compact_flow_query_requires_custom_payload_flag() -> None:
    with pytest.raises(FerricStoreError):
        _decode(_page_payload(), _OPCODES["FLOW.QUERY"], flags=0)


def test_generic_flow_query_result_remains_valid_without_custom_payload_flag() -> None:
    value = {
        b"version": b"ferric.flow.query.result/v1",
        b"records": [],
        b"future_extension": {b"preserved": True},
    }

    assert _decode(encode_value(value), _OPCODES["FLOW.QUERY"], flags=0) == value


@pytest.mark.parametrize(
    ("payload", "codec"),
    [
        (b"\xa1", _CODEC),
        (b"\xa0", "future_flow_query_result_v2"),
    ],
)
def test_custom_flow_query_payload_fails_closed_for_unknown_tag_or_codec(
    payload: bytes,
    codec: str,
) -> None:
    with pytest.raises(FerricStoreError, match="custom protocol response"):
        _decode(payload, _OPCODES["FLOW.QUERY"], codec=codec)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.__setitem__(0, 0xA1),
        lambda payload: payload.__setitem__(2, 0xFF),
        lambda payload: payload.__setitem__(slice(6, 14), _U64.pack(2**63)),
        lambda payload: payload.__setitem__(slice(99, 103), _U32.pack(101)),
        lambda payload: payload.__setitem__(1, 2),
        lambda payload: payload.__setitem__(slice(70, 78), _U64.pack(0)),
        lambda payload: payload.__setitem__(94, 2),
    ],
)
def test_compact_flow_query_decoder_rejects_each_fixed_schema_boundary(
    mutate,
) -> None:
    payload = bytearray(_page_payload())
    mutate(payload)

    assert decode_compact_flow_query_result(bytes(payload), 0, DecodeBudget(1_000)) is None


def test_compact_flow_query_decoder_rejects_count_overflow() -> None:
    payload = bytearray(_count_payload(0))
    payload[94:102] = _U64.pack(2**63)

    assert decode_compact_flow_query_result(bytes(payload), 0, DecodeBudget(1_000)) is None


def test_compact_flow_query_decoder_preserves_collection_limit_errors() -> None:
    with pytest.raises(DecodedCollectionLimitError):
        decode_compact_flow_query_result(_page_payload(), 0, DecodeBudget(0))


def _decode(
    payload: bytes,
    opcode: int,
    *,
    negotiated: bool = True,
    codec: str = _CODEC,
    flags: int = _FLAG_CUSTOM_PAYLOAD,
):
    adapter = SimpleNamespace(
        _compact_response_codecs={opcode: codec} if negotiated else {},
        max_decoded_collection_items=100_000,
        max_decompressed_response_bytes=64 * 1024 * 1024,
        _pending_response_item_counts={},
    )
    body = _STATUS.pack(_STATUS_OK) + payload
    return _decode_protocol_response(
        adapter,
        lane_id=1,
        opcode=opcode,
        request_id=9,
        flags=flags,
        body=body,
        read_started_ns=1,
        read_done_ns=2,
    ).value


def _page_payload(*, cursor: bytes | None = None) -> bytes:
    values = encode_value(b"run-1") + encode_value(b"failed") + encode_value({b"invoice_total": 42})
    page = (
        bytes([0]) + _U32.pack(0xFFFF_FFFF)
        if cursor is None
        else bytes([1]) + _U32.pack(len(cursor)) + cursor
    )
    payload = bytearray(
        bytes([_TAG, 0, 0, 0, 0, 2])
        + _usage(1)
        + page
        + _U32.pack(1)
        + _U32.pack((1 << 0) | (1 << 2) | (1 << 19))
        + values
    )
    payload[70:78] = _U64.pack(len(payload))
    return bytes(payload)


def _count_payload(count: int) -> bytes:
    payload = bytearray(bytes([_TAG, 1, 2, 1, 0, 0]) + _usage(1) + _U64.pack(count))
    payload[70:78] = _U64.pack(len(payload))
    return bytes(payload)


def _usage(result_records: int) -> bytes:
    values = [0] * 11
    values[2] = result_records
    values[4] = result_records
    values[7] = result_records
    return b"".join(_U64.pack(value) for value in values)
