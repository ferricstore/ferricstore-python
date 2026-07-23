from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_codec import decode_value, decode_value_at, encode_value
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_compact_commands import _compact_pipeline_payload_from_raw
from ferricstore.protocol_pipeline_codec import _compact_pipeline_payload
from ferricstore.protocol_responses import _decode_protocol_response

_WIRE_SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**64 - 1),
    st.floats(allow_nan=False, allow_infinity=False),
    st.binary(max_size=32),
    st.text(max_size=32),
)
_WIRE_VALUES = st.recursive(
    _WIRE_SCALARS,
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.lists(children, max_size=5).map(tuple),
        st.dictionaries(st.binary(max_size=16), children, max_size=5),
    ),
    max_leaves=30,
)


def _decoded_form(value: Any) -> Any:
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, (list, tuple)):
        return [_decoded_form(item) for item in value]
    if isinstance(value, dict):
        return {
            key.encode() if isinstance(key, str) else key: _decoded_form(item)
            for key, item in value.items()
        }
    return value


@settings(max_examples=300, deadline=None)
@given(value=_WIRE_VALUES, suffix=st.binary(max_size=24))
def test_protocol_value_round_trip_preserves_value_and_suffix(value: Any, suffix: bytes) -> None:
    decoded, remaining = decode_value(encode_value(value) + suffix, max_collection_items=256)

    assert decoded == _decoded_form(value)
    assert remaining == suffix


@settings(max_examples=1_000, deadline=None)
@given(data=st.binary(max_size=256))
def test_arbitrary_protocol_bytes_fail_only_with_sdk_errors(data: bytes) -> None:
    try:
        value, suffix = decode_value(data, max_collection_items=128)
    except FerricStoreError:
        return

    assert isinstance(suffix, bytes)
    assert encode_value(value)


@settings(max_examples=500, deadline=None)
@given(data=st.binary(max_size=128), offset=st.integers(min_value=-32, max_value=256))
def test_arbitrary_protocol_offsets_never_leak_index_or_struct_errors(
    data: bytes,
    offset: int,
) -> None:
    try:
        _value, end = decode_value_at(data, offset, max_collection_items=64)
    except FerricStoreError:
        return

    assert 0 <= offset < end <= len(data)


@settings(max_examples=1_000, deadline=None)
@given(
    opcode=st.integers(min_value=0, max_value=0xFFFF),
    flags=st.integers(min_value=0, max_value=0xFF),
    body=st.binary(max_size=256),
)
def test_arbitrary_server_response_bodies_fail_only_with_sdk_errors(
    opcode: int,
    flags: int,
    body: bytes,
) -> None:
    adapter = SimpleNamespace(
        max_decoded_collection_items=128,
        max_decompressed_response_bytes=1_024,
        _pending_response_item_counts={},
    )
    try:
        response = _decode_protocol_response(
            adapter,
            lane_id=1,
            opcode=opcode,
            request_id=1,
            flags=flags,
            body=body,
            read_started_ns=0,
            read_done_ns=0,
        )
    except FerricStoreError:
        return

    assert response.opcode == opcode
    assert response.request_id == 1


_BINARY_ARG = st.one_of(st.binary(max_size=24), st.text(max_size=24))
_RANGE_INT = st.one_of(
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.integers(min_value=-(2**63), max_value=2**63 - 1).map(str),
    st.integers(min_value=-(2**63), max_value=2**63 - 1).map(lambda value: str(value).encode()),
)


@st.composite
def _compactable_command(draw: st.DrawFn) -> tuple[Any, ...]:
    name = draw(
        st.sampled_from(
            [
                "SET",
                "GET",
                "HGET",
                "HMGET",
                "HGETALL",
                "SMEMBERS",
                "SISMEMBER",
                "LRANGE",
                "ZRANGE",
                "ZSCORE",
                "HSET",
                "LPUSH",
                "RPUSH",
                "SADD",
                "SREM",
                "ZADD",
                "ZREM",
            ]
        )
    )
    key = draw(_BINARY_ARG)
    if name == "SET":
        return name, key, draw(_BINARY_ARG)
    if name in {"GET", "HGETALL", "SMEMBERS"}:
        return name, key
    if name == "HMGET":
        fields = draw(st.lists(_BINARY_ARG, min_size=1, max_size=4))
        return name, key, *fields
    if name in {"HGET", "SISMEMBER", "ZSCORE", "LPUSH", "RPUSH", "SADD", "SREM", "ZREM"}:
        return name, key, draw(_BINARY_ARG)
    if name in {"LRANGE", "ZRANGE"}:
        command = (name, key, draw(_RANGE_INT), draw(_RANGE_INT))
        if name == "ZRANGE" and draw(st.booleans()):
            return *command, draw(st.sampled_from(["WITHSCORES", "withscores", b"WithScores"]))
        return command
    if name == "HSET":
        return name, key, draw(_BINARY_ARG), draw(_BINARY_ARG)
    score = draw(
        st.one_of(
            st.integers(min_value=-(2**53), max_value=2**53),
            st.floats(allow_nan=False, allow_infinity=False),
        )
    )
    return name, key, score, draw(_BINARY_ARG)


@settings(max_examples=500, deadline=None)
@given(command=_compactable_command(), values_only=st.booleans())
def test_raw_and_built_compact_pipeline_encoders_are_wire_equivalent(
    command: tuple[Any, ...],
    values_only: bool,
) -> None:
    built = build_protocol_command(*command)

    assert _compact_pipeline_payload_from_raw([command], values_only=values_only) == (
        _compact_pipeline_payload([built], values_only=values_only)
    )
