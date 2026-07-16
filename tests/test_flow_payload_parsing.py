from __future__ import annotations

import pytest

from ferricstore.errors import InvalidCommandError
from ferricstore.protocol_flow_payloads import (
    _collapse_states,
    _flow_create_many_payload,
    _flow_spawn_children_payload,
    _parse_claimed_items,
    _parse_create_items,
    _parse_create_items_ext,
    _parse_fenced_items,
    _parse_spawn_children,
    _parse_spawn_children_ext,
)


def test_create_many_extended_payload_preserves_per_item_values_and_refs() -> None:
    payload = _flow_create_many_payload(
        (
            "AUTO",
            "TYPE",
            "order",
            "ITEMS_EXT",
            2,
            "job-1",
            "-",
            b"payload-1",
            1,
            "amount",
            10,
            1,
            "customer",
            "ref-1",
            "job-2",
            "tenant-b",
            b"payload-2",
            0,
            0,
        )
    )

    assert payload == {
        "type": "order",
        "items": [
            {
                "id": "job-1",
                "payload": b"payload-1",
                "values": {"amount": 10},
                "value_refs": {"customer": "ref-1"},
            },
            {
                "id": "job-2",
                "partition_key": "tenant-b",
                "payload": b"payload-2",
            },
        ],
    }


def test_spawn_children_extended_payload_preserves_per_child_values_and_refs() -> None:
    payload = _flow_spawn_children_payload(
        (
            "parent-1",
            "ITEMS_EXT",
            1,
            "child-1",
            "tenant-a",
            "invoice",
            b"payload",
            1,
            "amount",
            10,
            1,
            "source",
            "ref-1",
        )
    )

    assert payload == {
        "id": "parent-1",
        "children": [
            {
                "id": "child-1",
                "partition_key": "tenant-a",
                "type": "invoice",
                "payload": b"payload",
                "values": {"amount": 10},
                "value_refs": {"source": "ref-1"},
            }
        ],
    }


@pytest.mark.parametrize(
    "call",
    [
        lambda: _parse_create_items_ext(("job", "-", b"payload", 0, 0), False, expected_count=2),
        lambda: _parse_create_items_ext(("job", "-", b"payload", 1), False, expected_count=1),
        lambda: _parse_create_items_ext(
            ("job", "-", b"payload", 0, 0, "trailing"), False, expected_count=1
        ),
        lambda: _parse_spawn_children_ext(("job", "-", "type", b"payload", 0, 0), expected_count=2),
        lambda: _parse_spawn_children_ext(("job", "-", "type", b"payload", 1), expected_count=1),
        lambda: _parse_spawn_children_ext(
            ("job", "-", "type", b"payload", 0, 0, "trailing"), expected_count=1
        ),
    ],
)
def test_extended_flow_item_parsers_reject_truncation_and_count_mismatches(
    call: object,
) -> None:
    with pytest.raises(InvalidCommandError):
        call()  # type: ignore[operator]


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda: _parse_create_items(("id", "partition", b"payload", "extra"), True), "width"),
        (lambda: _parse_spawn_children(("id", "type"), False), "width"),
        (lambda: _parse_claimed_items(("id", b"lease"), False), "width"),
        (lambda: _parse_fenced_items(("id",), False, include_lease=False), "width"),
    ],
)
def test_plain_flow_item_parsers_reject_partial_items(call: object, message: str) -> None:
    with pytest.raises(InvalidCommandError, match=message):
        call()  # type: ignore[operator]


def test_spawn_children_mixed_marker_is_case_insensitive() -> None:
    assert _flow_spawn_children_payload(
        ("parent-1", "ITEMS", b"mixed", "child-1", "tenant-a", "type", b"payload")
    ) == {
        "id": "parent-1",
        "children": [
            {
                "id": "child-1",
                "partition_key": "tenant-a",
                "type": "type",
                "payload": b"payload",
            }
        ],
    }


def test_state_collapse_only_rewrites_singleton_lists() -> None:
    singleton = {"states": ["queued"]}
    multiple = {"states": ["queued", "retry"]}
    non_list = {"states": "queued"}

    _collapse_states(singleton)
    _collapse_states(multiple)
    _collapse_states(non_list)

    assert singleton == {"state": "queued"}
    assert multiple == {"states": ["queued", "retry"]}
    assert non_list == {"states": "queued"}
