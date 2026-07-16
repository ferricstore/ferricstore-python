from __future__ import annotations

from typing import Any

from ferricstore.command_grammar import (
    consume_counted_arguments,
    find_flow_many_item_marker,
    require_nonnegative_count,
    split_flow_value_mget,
)
from ferricstore.errors import (
    InvalidCommandError,
)
from ferricstore.protocol_command_options import (
    _option_map,
)
from ferricstore.protocol_common import (
    _command_token,
    _require_arg,
    _text,
)


def _flow_create_many_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, "FLOW.CREATE_MANY"))
    partition_mode = _command_token(wire_partition)
    mixed = partition_mode == "MIXED"
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if partition_mode not in {"AUTO", "MIXED", "NONE"}:
        payload["partition_key"] = args[0]

    token = _command_token(args[item_token])
    if token == "ITEMS_EXT":
        item_count = require_nonnegative_count(
            _require_arg(args, item_token + 1, "FLOW.CREATE_MANY ITEMS_EXT"),
            label="FLOW.CREATE_MANY ITEMS_EXT",
        )
        payload["items"] = _parse_create_items_ext(
            args[item_token + 2 :],
            mixed,
            expected_count=item_count,
        )
    else:
        payload["items"] = _parse_create_items(args[item_token + 1 :], mixed)
    return payload


def _flow_spawn_children_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    parent_id = _require_arg(args, 0, "FLOW.SPAWN_CHILDREN")
    item_token = _find_item_token(args, 1)
    payload = {"id": parent_id}
    payload.update(_option_map(args[1:item_token]))

    token = _command_token(args[item_token])
    if token == "ITEMS_EXT":
        item_count = require_nonnegative_count(
            _require_arg(args, item_token + 1, "FLOW.SPAWN_CHILDREN ITEMS_EXT"),
            label="FLOW.SPAWN_CHILDREN ITEMS_EXT",
        )
        payload["children"] = _parse_spawn_children_ext(
            args[item_token + 2 :],
            expected_count=item_count,
        )
    else:
        mixed = item_token + 1 < len(args) and _command_token(args[item_token + 1]) == "MIXED"
        start = item_token + 2 if mixed else item_token + 1
        payload["children"] = _parse_spawn_children(args[start:], mixed)
    return payload


def _parse_spawn_children(values: tuple[Any, ...], mixed: bool) -> list[dict[str, Any]]:
    width = 4 if mixed else 3
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.SPAWN_CHILDREN ITEMS has wrong child width")
    children = []
    for idx in range(0, len(values), width):
        if mixed:
            children.append(
                {
                    "id": values[idx],
                    "partition_key": values[idx + 1],
                    "type": values[idx + 2],
                    "payload": values[idx + 3],
                }
            )
        else:
            children.append(
                {
                    "id": values[idx],
                    "type": values[idx + 1],
                    "payload": values[idx + 2],
                }
            )
    return children


def _parse_spawn_children_ext(
    values: tuple[Any, ...],
    *,
    expected_count: int,
) -> list[dict[str, Any]]:
    if expected_count > len(values) // 6:
        raise InvalidCommandError("FLOW.SPAWN_CHILDREN ITEMS_EXT count does not match items")
    children: list[dict[str, Any]] = []
    idx = 0
    for _ in range(expected_count):
        if idx + 4 > len(values):
            raise InvalidCommandError("FLOW.SPAWN_CHILDREN ITEMS_EXT item is truncated")
        child = {
            "id": values[idx],
            "type": values[idx + 2],
            "payload": values[idx + 3],
        }
        partition = values[idx + 1]
        if partition != "-" and partition != b"-":
            child["partition_key"] = partition
        idx += 4

        value_segment = consume_counted_arguments(
            values,
            idx,
            width=2,
            label="FLOW.SPAWN_CHILDREN ITEMS_EXT VALUE",
        )
        idx = value_segment.next_index
        child_values = {
            _text(value_segment.values[offset]): value_segment.values[offset + 1]
            for offset in range(0, len(value_segment.values), 2)
        }
        if child_values:
            child["values"] = child_values

        ref_segment = consume_counted_arguments(
            values,
            idx,
            width=2,
            label="FLOW.SPAWN_CHILDREN ITEMS_EXT VALUE_REF",
        )
        idx = ref_segment.next_index
        child_refs = {
            _text(ref_segment.values[offset]): ref_segment.values[offset + 1]
            for offset in range(0, len(ref_segment.values), 2)
        }
        if child_refs:
            child["value_refs"] = child_refs

        children.append(child)
    if idx != len(values):
        raise InvalidCommandError("FLOW.SPAWN_CHILDREN ITEMS_EXT count does not match items")
    return children


def _flow_claimed_many_payload(name: str, args: tuple[Any, ...]) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, name))
    mixed = _command_token(wire_partition) == "MIXED"
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if not mixed:
        payload["partition_key"] = args[0]
    payload["items"] = _parse_claimed_items(args[item_token + 1 :], mixed)
    return payload


def _flow_fenced_many_payload(
    name: str, args: tuple[Any, ...], *, include_lease: bool
) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, name))
    mixed = _command_token(wire_partition) == "MIXED"
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if not mixed:
        payload["partition_key"] = args[0]
    payload["items"] = _parse_fenced_items(
        args[item_token + 1 :],
        mixed,
        include_lease=include_lease,
    )
    return payload


def _parse_create_items(values: tuple[Any, ...], mixed: bool) -> list[list[Any]]:
    width = 3 if mixed else 2
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.CREATE_MANY ITEMS has wrong item width")
    items = []
    for idx in range(0, len(values), width):
        if mixed:
            items.append([values[idx], values[idx + 1], values[idx + 2]])
        else:
            items.append([values[idx], values[idx + 1]])
    return items


def _parse_create_items_ext(
    values: tuple[Any, ...],
    mixed: bool,
    *,
    expected_count: int,
) -> list[dict[str, Any]]:
    if expected_count > len(values) // 5:
        raise InvalidCommandError("FLOW.CREATE_MANY ITEMS_EXT count does not match items")
    items: list[dict[str, Any]] = []
    idx = 0
    for _ in range(expected_count):
        if idx + 3 > len(values):
            raise InvalidCommandError("FLOW.CREATE_MANY ITEMS_EXT item is truncated")
        item = {"id": values[idx], "payload": values[idx + 2]}
        partition = values[idx + 1]
        if mixed or (partition != "-" and partition != b"-"):
            item["partition_key"] = partition
        idx += 3
        value_segment = consume_counted_arguments(
            values,
            idx,
            width=2,
            label="FLOW.CREATE_MANY ITEMS_EXT VALUE",
        )
        idx = value_segment.next_index
        item_values = {
            _text(value_segment.values[offset]): value_segment.values[offset + 1]
            for offset in range(0, len(value_segment.values), 2)
        }
        if item_values:
            item["values"] = item_values
        ref_segment = consume_counted_arguments(
            values,
            idx,
            width=2,
            label="FLOW.CREATE_MANY ITEMS_EXT VALUE_REF",
        )
        idx = ref_segment.next_index
        item_refs = {
            _text(ref_segment.values[offset]): ref_segment.values[offset + 1]
            for offset in range(0, len(ref_segment.values), 2)
        }
        if item_refs:
            item["value_refs"] = item_refs
        items.append(item)
    if idx != len(values):
        raise InvalidCommandError("FLOW.CREATE_MANY ITEMS_EXT count does not match items")
    return items


def _parse_claimed_items(values: tuple[Any, ...], mixed: bool) -> list[list[Any]]:
    width = 4 if mixed else 3
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.*_MANY ITEMS has wrong claimed item width")
    items = []
    for idx in range(0, len(values), width):
        if mixed:
            items.append([values[idx], values[idx + 1], values[idx + 2], values[idx + 3]])
        else:
            items.append([values[idx], values[idx + 1], values[idx + 2]])
    return items


def _parse_fenced_items(
    values: tuple[Any, ...], mixed: bool, *, include_lease: bool
) -> list[dict[str, Any]]:
    width = (4 if mixed else 3) if include_lease else (3 if mixed else 2)
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.*_MANY ITEMS has wrong fenced item width")
    items = []
    for idx in range(0, len(values), width):
        item = {"id": values[idx], "fencing_token": values[idx + (2 if mixed else 1)]}
        if mixed:
            item["partition_key"] = values[idx + 1]
        if include_lease:
            lease = values[idx + (3 if mixed else 2)]
            if lease != "-" and lease != b"-":
                item["lease_token"] = lease
        items.append(item)
    return items


def _split_refs_and_options(args: tuple[Any, ...]) -> tuple[list[Any], dict[str, Any]]:
    refs, max_bytes = split_flow_value_mget(args)
    return list(refs), {} if max_bytes is None else {"max_bytes": max_bytes}


def _find_item_token(args: tuple[Any, ...], start: int) -> int:
    marker = find_flow_many_item_marker(args, start)
    if marker is not None:
        return marker[0]
    raise InvalidCommandError("FLOW many command requires ITEMS or ITEMS_EXT")


def _collapse_states(payload: dict[str, Any]) -> None:
    states = payload.get("states")
    if isinstance(states, list) and len(states) == 1:
        payload["state"] = states[0]
        del payload["states"]


__all__ = [
    "_collapse_states",
    "_find_item_token",
    "_flow_claimed_many_payload",
    "_flow_create_many_payload",
    "_flow_fenced_many_payload",
    "_flow_spawn_children_payload",
    "_parse_claimed_items",
    "_parse_create_items",
    "_parse_create_items_ext",
    "_parse_fenced_items",
    "_parse_spawn_children",
    "_parse_spawn_children_ext",
    "_split_refs_and_options",
]
