from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ferricstore.command_grammar import command_token, parse_stream_read
from ferricstore.errors import InvalidCommandError
from ferricstore.flow_options import FlowOptionPlan

HTTP_UNSUPPORTED_COMMANDS = frozenset(
    {
        "ASKING",
        "AUTH",
        "BACKPRESSURE",
        "CLIENT",
        "CLIENT.INFO",
        "CLIENT.SETNAME",
        "DISCARD",
        "EVENT",
        "EXEC",
        "GOAWAY",
        "HELLO",
        "MONITOR",
        "MULTI",
        "OPTIONS",
        "PIPELINE",
        "PSUBSCRIBE",
        "PSYNC",
        "PUNSUBSCRIBE",
        "QUIT",
        "READONLY",
        "READWRITE",
        "REPLCONF",
        "RESET",
        "ROUTE",
        "ROUTE_BATCH",
        "SANDBOX",
        "SELECT",
        "SHARDS",
        "SSUBSCRIBE",
        "STARTUP",
        "SUBSCRIBE",
        "SUBSCRIBE_EVENTS",
        "SUNSUBSCRIBE",
        "SYNC",
        "UNSUBSCRIBE",
        "UNSUBSCRIBE_EVENTS",
        "UNWATCH",
        "WATCH",
        "WINDOW_UPDATE",
    }
)

_MAX_COMMAND_EXEC_DEPTH = 8


@dataclass(frozen=True, slots=True)
class HttpCommandBudget:
    extension: float = 0
    disable_default: bool = False


def command_values(command: Sequence[Any], index: int) -> list[Any]:
    if isinstance(command, (str, bytes, bytearray)):
        raise TypeError(f"HTTP command {index} must be a sequence of command arguments")
    values = list(command)
    if not values:
        raise ValueError(f"HTTP command {index} cannot be empty")
    return values


def command_name(value: Any, index: int) -> str:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            decoded = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise TypeError(f"HTTP command {index} name must be UTF-8 text") from exc
        if decoded:
            return decoded
    raise TypeError(f"HTTP command {index} name must be text")


def require_http_command(command_name: str) -> None:
    if command_name in HTTP_UNSUPPORTED_COMMANDS:
        raise InvalidCommandError(
            f"{command_name} requires a connection-affine native transport and "
            "is not supported through the HTTP transport"
        )


def unwrapped_command_values(values: Sequence[Any], index: int) -> list[Any]:
    unwrapped = list(values)
    offset = 0
    while command_name(unwrapped[offset], index).upper() == "COMMAND_EXEC":
        offset += 1
        if offset > _MAX_COMMAND_EXEC_DEPTH:
            raise InvalidCommandError("COMMAND_EXEC nesting exceeds the HTTP transport limit")
        if offset >= len(unwrapped):
            raise InvalidCommandError("COMMAND_EXEC requires a command name")
    return unwrapped[offset:]


def effective_command_values(values: Sequence[Any], index: int) -> list[Any]:
    effective = unwrapped_command_values(values, index)
    if len(effective) >= 3 and command_token(effective[-2]) == "REQUEST_CONTEXT":
        return effective[:-2]
    return effective


def is_blocking_command(command: Sequence[Any]) -> bool:
    budget = blocking_command_budget(command)
    return budget.disable_default or budget.extension > 0


def effective_timeout(
    commands: Sequence[Sequence[Any]],
    base_timeout: float | None,
) -> float | None:
    extension = 0.0
    for command in commands:
        budget = blocking_command_budget(command)
        if budget.disable_default:
            return None
        extension += budget.extension
        if not math.isfinite(extension):
            return None
    if base_timeout is None:
        return None
    effective = base_timeout + extension
    return effective if math.isfinite(effective) else None


def blocking_command_budget(command: Sequence[Any]) -> HttpCommandBudget:
    values = effective_command_values(command_values(command, 0), 0)
    name = command_name(values[0], 0).upper()
    args = values[1:]
    candidate: Any = None
    unit = 0.0

    if name in {"BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX"}:
        if args:
            candidate, unit = args[-1], 1.0
    elif name in {"BLMPOP", "BZMPOP"}:
        if args:
            candidate, unit = args[0], 1.0
    elif name in {"XREAD", "XREADGROUP"}:
        parsed = parse_stream_read(args, read_group=name == "XREADGROUP")
        if parsed.valid and parsed.block is not None:
            candidate, unit = parsed.block, 0.001
    elif name in {"WAIT", "WAITAOF"}:
        if args:
            candidate, unit = args[-1], 0.001
    elif name in {"FLOW.CLAIM_DUE", "FLOW.SCHEDULE.FIRE_DUE"}:
        plan = FlowOptionPlan(args)
        option_index = 1 if name == "FLOW.CLAIM_DUE" else 0
        while option_index < len(args):
            width = plan.option_width(option_index)
            if width is None:
                break
            if command_token(args[option_index]) == "BLOCK" and width == 2:
                candidate, unit = args[option_index + 1], 0.001
                break
            option_index += width

    duration = _blocking_duration(candidate, unit)
    if duration is None:
        return HttpCommandBudget()
    if duration == 0:
        if name == "FLOW.CLAIM_DUE":
            return HttpCommandBudget()
        return HttpCommandBudget(disable_default=True)
    if not math.isfinite(duration):
        return HttpCommandBudget(disable_default=True)
    return HttpCommandBudget(extension=duration)


def _blocking_duration(value: Any, unit: float) -> float | None:
    if value is None or unit == 0 or isinstance(value, bool):
        return None
    if isinstance(value, (bytes, bytearray)):
        try:
            value = bytes(value).decode("ascii")
        except UnicodeDecodeError:
            return None
    try:
        number = float(value)
    except OverflowError:
        return math.inf
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or number < 0:
        return None
    return number * unit


__all__ = [
    "HTTP_UNSUPPORTED_COMMANDS",
    "blocking_command_budget",
    "command_name",
    "command_values",
    "effective_command_values",
    "effective_timeout",
    "is_blocking_command",
    "require_http_command",
    "unwrapped_command_values",
]
