from __future__ import annotations

import math
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_request import (
    _with_flow_query_command_options,
    build_flow_query_args,
    build_flow_query_payload,
)
from ferricstore.flow_query_response import (
    decode_flow_explain_result,
    decode_flow_query_index_status,
    decode_flow_query_result,
)

QUERY = "FROM runs WHERE partition_key = @partition RETURN COUNT"

_PARAMETER_NAMES = st.text(
    alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-",
    min_size=1,
    max_size=20,
)

_FINITE_FLOATS = st.floats(
    allow_nan=False,
    allow_infinity=False,
    width=64,
).filter(math.isfinite)

_PARAMETER_VALUES = st.one_of(
    st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=50),
    st.binary(max_size=50),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    _FINITE_FLOATS,
)


@given(
    params=st.dictionaries(_PARAMETER_NAMES, _PARAMETER_VALUES, max_size=20),
    deadline_ms=st.one_of(st.none(), st.integers(min_value=0, max_value=2**64 - 1)),
)
@settings(max_examples=250, deadline=None)
def test_query_command_payload_round_trips_bounded_parameters_deterministically(
    params: dict[str, Any],
    deadline_ms: int | None,
) -> None:
    args = _with_flow_query_command_options(
        build_flow_query_args(QUERY, params),
        deadline_ms=deadline_ms,
        routing_key=None,
    )
    payload = build_flow_query_payload(args[1:])

    assert payload["version"] == "FQL1"
    assert payload["query"] == QUERY
    if params:
        assert payload["params"] == params
        assert list(payload["params"]) == sorted(params)
    else:
        assert "params" not in payload
    if deadline_ms is None:
        assert "deadline_ms" not in payload
    else:
        assert payload["deadline_ms"] == deadline_ms


_PROTOCOL_SCALAR = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**64), max_value=2**65),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(max_size=40),
    st.binary(max_size=40),
)

_PROTOCOL_VALUE = st.recursive(
    _PROTOCOL_SCALAR,
    lambda children: st.one_of(
        st.lists(children, max_size=8),
        st.dictionaries(
            st.one_of(st.text(max_size=20), st.binary(max_size=20)),
            children,
            max_size=8,
        ),
    ),
    max_leaves=30,
)


@given(value=_PROTOCOL_VALUE)
@settings(max_examples=400, deadline=None)
def test_query_decoders_fail_closed_with_sdk_errors(value: Any) -> None:
    decoders: tuple[Callable[[Any], Any], ...] = (
        decode_flow_query_result,
        decode_flow_explain_result,
        decode_flow_query_index_status,
    )
    for decoder in decoders:
        with suppress(FerricStoreError):
            decoder(value)
