from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ferricstore.errors import InvalidCommandError
from ferricstore.flow_options import FlowOptionPlan


def command_token(value: Any) -> str:
    """Normalize a command grammar token without constraining opaque values."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").upper()
    return str(value).upper()


@dataclass(frozen=True, slots=True)
class CountedArguments:
    values: tuple[Any, ...]
    next_index: int
    count: int


def require_nonnegative_count(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidCommandError(f"{label} count must be a non-negative integer")
    return value


def consume_counted_arguments(
    args: Sequence[Any],
    count_index: int,
    *,
    width: int = 1,
    label: str,
) -> CountedArguments:
    """Consume a bounded counted segment while guaranteeing cursor progress."""
    if count_index >= len(args):
        raise InvalidCommandError(f"{label} requires a count")
    count = require_nonnegative_count(args[count_index], label=label)
    start = count_index + 1
    end = start + count * width
    if end > len(args):
        raise InvalidCommandError(f"{label} count exceeds the remaining arguments")
    return CountedArguments(tuple(args[start:end]), end, count)


def split_flow_value_mget(
    args: Sequence[Any],
) -> tuple[tuple[Any, ...], int | None]:
    """Split refs from the only unambiguous MGET option form.

    Flow value refs are arbitrary strings, including strings equal to option
    keywords. Therefore MAX_BYTES is an option only as a trailing keyword plus
    a typed integer value.
    """
    values = tuple(args)
    if (
        len(values) >= 2
        and command_token(values[-2]) in {"MAX_BYTES", "MAXBYTES"}
        and isinstance(values[-1], int)
        and not isinstance(values[-1], bool)
    ):
        return values[:-2], values[-1]
    return values, None


def find_flow_many_item_marker(
    args: Sequence[Any],
    start: int,
) -> tuple[int, str] | None:
    """Find an ITEMS marker by consuming the option grammar before it."""
    plan = FlowOptionPlan(args)
    index = start
    while index < len(args):
        token = command_token(args[index])
        if token in {"ITEMS", "ITEMS_EXT", "ITEMS_EXT_V2"}:
            return index, token
        width = plan.option_width(index)
        if width is None or index + width > len(args):
            return None
        index += width
    return None


def flow_create_many_item_count(args: Sequence[Any]) -> int | None:
    """Infer exact create-many response cardinality from parsed grammar."""
    values = tuple(args)
    if not values:
        return None
    marker = find_flow_many_item_marker(values, 1)
    if marker is None:
        return None
    marker_index, marker_token = marker
    if marker_token in {"ITEMS_EXT", "ITEMS_EXT_V2"}:
        if marker_index + 1 >= len(values):
            return None
        count = values[marker_index + 1]
        return (
            count if isinstance(count, int) and not isinstance(count, bool) and count >= 0 else None
        )

    width = 3 if command_token(values[0]) == "MIXED" else 2
    item_values = len(values) - marker_index - 1
    return item_values // width if item_values % width == 0 else None


@dataclass(frozen=True, slots=True)
class StreamReadGrammar:
    keys: tuple[Any, ...]
    block: Any | None
    valid: bool


def parse_stream_read(
    args: Sequence[Any],
    *,
    read_group: bool,
) -> StreamReadGrammar:
    """Parse XREAD/XREADGROUP without inspecting stream names or IDs."""
    values = tuple(args)
    index = 0
    if read_group:
        if len(values) < 3 or command_token(values[0]) != "GROUP":
            return StreamReadGrammar((), None, False)
        # Group and consumer are opaque, even when equal to option keywords.
        index = 3

    block: Any | None = None
    while index < len(values):
        token = command_token(values[index])
        if token == "STREAMS":
            remaining = values[index + 1 :]
            if not remaining or len(remaining) % 2 != 0:
                return StreamReadGrammar((), block, False)
            return StreamReadGrammar(remaining[: len(remaining) // 2], block, True)
        if token == "NOACK":
            index += 1
            continue
        if token not in {"BLOCK", "COUNT"} or index + 1 >= len(values):
            return StreamReadGrammar((), block, False)
        if token == "BLOCK":
            block = values[index + 1]
        index += 2
    return StreamReadGrammar((), block, False)


__all__ = [
    "CountedArguments",
    "StreamReadGrammar",
    "command_token",
    "consume_counted_arguments",
    "find_flow_many_item_marker",
    "flow_create_many_item_count",
    "parse_stream_read",
    "require_nonnegative_count",
    "split_flow_value_mget",
]
