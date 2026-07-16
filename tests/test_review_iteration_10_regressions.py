from __future__ import annotations

import asyncio
import math
from typing import Any, ClassVar

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ferricstore.async_workflow_execution import handle_claimed_batch
from ferricstore.errors import InvalidCommandError
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_compact_commands import _compact_pipeline_payload_from_raw
from ferricstore.protocol_flow_codec import _raw_int
from ferricstore.protocol_flow_payloads import (
    _flow_claimed_many_payload,
    _flow_create_many_payload,
    _flow_fenced_many_payload,
    _parse_create_items_ext,
    _parse_fenced_items,
    _parse_spawn_children_ext,
)


@pytest.mark.parametrize("mixed_token", ["mixed", "MiXeD", b"mixed", b"MiXeD"])
def test_flow_create_many_partition_mode_is_case_insensitive(mixed_token: str | bytes) -> None:
    payload = _flow_create_many_payload((mixed_token, "ITEMS", "job-1", "tenant-a", b"payload"))

    assert payload == {"items": [["job-1", "tenant-a", b"payload"]]}


@pytest.mark.parametrize("mixed_token", ["mixed", "MiXeD", b"mixed", b"MiXeD"])
def test_flow_claimed_many_partition_mode_is_case_insensitive(mixed_token: str | bytes) -> None:
    payload = _flow_claimed_many_payload(
        "FLOW.COMPLETE_MANY",
        (mixed_token, "ITEMS", "job-1", "tenant-a", b"lease", 7),
    )

    assert payload == {"items": [["job-1", "tenant-a", b"lease", 7]]}


@pytest.mark.parametrize("mixed_token", ["mixed", "MiXeD", b"mixed", b"MiXeD"])
def test_flow_fenced_many_partition_mode_is_case_insensitive(mixed_token: str | bytes) -> None:
    payload = _flow_fenced_many_payload(
        "FLOW.CANCEL_MANY",
        (mixed_token, "ITEMS", "job-1", "tenant-a", 7, b"lease"),
        include_lease=True,
    )

    assert payload == {
        "items": [
            {
                "id": "job-1",
                "partition_key": "tenant-a",
                "fencing_token": 7,
                "lease_token": b"lease",
            }
        ]
    }


def test_flow_extended_payload_sentinels_accept_wire_bytes() -> None:
    assert _parse_create_items_ext(
        ("job-1", b"-", b"payload", 0, 0),
        False,
        expected_count=1,
    ) == [{"id": "job-1", "payload": b"payload"}]
    assert _parse_spawn_children_ext(
        ("child-1", b"-", "child", b"payload", 0, 0),
        expected_count=1,
    ) == [{"id": "child-1", "type": "child", "payload": b"payload"}]
    assert _parse_fenced_items(
        ("job-1", 7, b"-"),
        False,
        include_lease=True,
    ) == [{"id": "job-1", "fencing_token": 7}]


def test_async_workflow_empty_claim_batch_is_a_noop() -> None:
    class Host:
        handlers: ClassVar[dict[str, object]] = {"queued": lambda _ctx: None}
        error_modes: ClassVar[dict[str, str]] = {}
        on_error = "retry"
        budget_policies: ClassVar[dict[str, object]] = {}
        concurrency = 4

    assert asyncio.run(handle_claimed_batch(Host(), "queued", [])) == 0  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [1.5, True, False, math.nan, math.inf, -math.inf, None])
def test_compact_flow_integer_parser_never_coerces_non_integer_types(value: object) -> None:
    assert _raw_int(value) is None


@pytest.mark.parametrize("value", [7, "7", b"7", -7, "-7", b"-7"])
def test_compact_flow_integer_parser_matches_text_protocol_integers(value: object) -> None:
    assert _raw_int(value) == int(value)


@pytest.mark.parametrize(
    "command",
    [
        ("LRANGE", "key", 1.5, 2),
        ("LRANGE", "key", True, 2),
        ("ZRANGE", "key", 1.5, 2),
        ("ZRANGE", "key", 1, False),
        ("ZADD", "key", True, "member"),
        ("ZADD", "key", math.nan, "member"),
        ("ZADD", "key", math.inf, "member"),
        ("ZADD", "key", -math.inf, "member"),
    ],
)
def test_raw_compact_pipeline_never_bypasses_direct_command_validation(
    command: tuple[Any, ...],
) -> None:
    with pytest.raises(InvalidCommandError):
        build_protocol_command(*command)

    assert _compact_pipeline_payload_from_raw([command]) is None


@settings(max_examples=200, deadline=None)
@given(
    command=st.one_of(
        st.tuples(
            st.sampled_from(["LRANGE", "ZRANGE"]),
            st.text(max_size=12),
            st.one_of(st.floats(allow_nan=True, allow_infinity=True), st.booleans()),
            st.integers(),
        ),
        st.tuples(
            st.just("ZADD"),
            st.text(max_size=12),
            st.one_of(
                st.floats(allow_nan=True, allow_infinity=True),
                st.booleans(),
                st.none(),
            ),
            st.binary(max_size=12),
        ),
    )
)
def test_raw_compact_pipeline_validation_matches_protocol_builder(
    command: tuple[Any, ...],
) -> None:
    try:
        build_protocol_command(*command)
    except InvalidCommandError:
        assert _compact_pipeline_payload_from_raw([command]) is None
