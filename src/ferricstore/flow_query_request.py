from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ferricstore.flow_query_limits import (
    FLOW_QUERY_MAX_BYTES,
    FLOW_QUERY_MAX_PARAMETER_NAME_BYTES,
    FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES,
    FLOW_QUERY_MAX_PARAMETERS,
)

FLOW_QUERY_LANGUAGE_VERSION = "FQL1"
FLOW_QUERY_REQUEST_CONTRACT = "ferric.flow.query.request/v1"
_INDEX_ID = re.compile(r"[A-Za-z0-9_.:-]{1,64}\Z", re.ASCII)
_PARAMETER_NAME = re.compile(r"[A-Za-z0-9_.-]+\Z", re.ASCII)
_U64_MAX = 2**64 - 1


@dataclass(frozen=True, slots=True)
class _FlowQueryCommandOptions:
    """Out-of-band native options that cannot collide with named FQL parameters."""

    deadline_ms: int | None = None
    routing_key: str | bytes | None = None


def build_flow_query_args(
    query: str,
    params: Mapping[str, Any] | None = None,
) -> list[Any]:
    validate_flow_query_text(query)
    normalized = normalize_flow_query_params(params)
    args: list[Any] = ["FLOW.QUERY", FLOW_QUERY_LANGUAGE_VERSION, query]
    for name in sorted(normalized):
        args.extend((name, normalized[name]))
    return args


def _with_flow_query_command_options(
    args: Sequence[Any],
    *,
    deadline_ms: int | None,
    routing_key: str | bytes | None,
) -> tuple[Any, ...]:
    if any(isinstance(value, _FlowQueryCommandOptions) for value in args):
        raise ValueError("FLOW.QUERY command options are already present")
    deadline = None if deadline_ms is None else normalize_flow_query_deadline(deadline_ms)
    route = _normalize_flow_query_routing_key(routing_key)
    if deadline is None and route is None:
        return tuple(args)
    return (*args, _FlowQueryCommandOptions(deadline, route))


def build_flow_query_payload(args: Sequence[Any]) -> dict[str, Any]:
    command_args = args
    options: _FlowQueryCommandOptions | None = None
    if args and isinstance(args[-1], _FlowQueryCommandOptions):
        options = args[-1]
        command_args = args[:-1]
    if any(isinstance(value, _FlowQueryCommandOptions) for value in command_args):
        raise ValueError("FLOW.QUERY command options must be last")
    if len(command_args) < 2:
        raise ValueError("FLOW.QUERY requires version and query")
    if (len(command_args) - 2) % 2:
        raise ValueError("FLOW.QUERY parameters must be name/value pairs")
    version = _command_text(command_args[0], "FLOW.QUERY version")
    if version != FLOW_QUERY_LANGUAGE_VERSION:
        raise ValueError(f"FLOW.QUERY requires version {FLOW_QUERY_LANGUAGE_VERSION}")
    query = _command_text(command_args[1], "FLOW.QUERY query")
    validate_flow_query_text(query)
    parameter_count = (len(command_args) - 2) // 2
    if parameter_count > FLOW_QUERY_MAX_PARAMETERS:
        raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_PARAMETERS} parameters")
    params: dict[str, Any] = {}
    for index in range(2, len(command_args), 2):
        name = _command_text(command_args[index], "FLOW.QUERY parameter name")
        validate_flow_query_parameter_name(name)
        if name in params:
            raise ValueError(f"FLOW.QUERY parameter {name!r} is duplicated")
        params[name] = normalize_flow_query_parameter(command_args[index + 1], name=name)
    payload: dict[str, Any] = {"version": version, "query": query}
    if params:
        payload["params"] = params
    if options is not None and options.deadline_ms is not None:
        payload["deadline_ms"] = options.deadline_ms
    return payload


def flow_query_command_routing_key(args: Sequence[Any]) -> str | bytes | None:
    if args and isinstance(args[-1], _FlowQueryCommandOptions):
        return args[-1].routing_key
    return None


def _normalize_flow_query_routing_key(value: str | bytes | None) -> str | bytes | None:
    if value is None:
        return None
    if not isinstance(value, (str, bytes)) or not value:
        raise ValueError("FLOW.QUERY routing key must be non-empty text or bytes")
    return value


def normalize_flow_query_deadline(value: Any) -> int:
    if type(value) is not int:
        raise TypeError("FLOW.QUERY deadline_ms must be an integer")
    if not 0 <= value <= _U64_MAX:
        raise ValueError("FLOW.QUERY deadline_ms must be an unsigned 64-bit integer")
    return value


def normalize_flow_query_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    if params is None:
        return {}
    if not isinstance(params, Mapping):
        raise TypeError("FLOW.QUERY params must be a mapping")
    if len(params) > FLOW_QUERY_MAX_PARAMETERS:
        raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_PARAMETERS} parameters")
    normalized: dict[str, Any] = {}
    for index, (name, value) in enumerate(params.items()):
        if index >= FLOW_QUERY_MAX_PARAMETERS:
            raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_PARAMETERS} parameters")
        if not isinstance(name, str):
            raise TypeError("FLOW.QUERY parameter names must be strings")
        validate_flow_query_parameter_name(name)
        normalized[name] = normalize_flow_query_parameter(value, name=name)
    return normalized


def normalize_flow_query_parameter(value: Any, *, name: str) -> Any:
    if isinstance(value, str):
        if len(value) > FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES:
            raise ValueError(
                f"FLOW.QUERY parameter {name!r} exceeds "
                f"{FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES} bytes"
            )
        try:
            encoded = value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError(f"FLOW.QUERY parameter {name!r} must be valid UTF-8") from exc
        if len(encoded) > FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES:
            raise ValueError(
                f"FLOW.QUERY parameter {name!r} exceeds "
                f"{FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES} bytes"
            )
        return value
    if isinstance(value, bytes):
        if len(value) > FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES:
            raise ValueError(
                f"FLOW.QUERY parameter {name!r} exceeds "
                f"{FLOW_QUERY_MAX_PARAMETER_VALUE_BYTES} bytes"
            )
        return value
    if type(value) is bool:
        return value
    if type(value) is int:
        if -(2**63) <= value <= 2**63 - 1:
            return value
    elif type(value) is float and math.isfinite(value):
        return value
    raise TypeError(
        f"FLOW.QUERY parameter {name!r} must be text, bytes, boolean, a finite float, "
        "or a signed 64-bit integer"
    )


def validate_flow_query_text(query: str) -> None:
    if not isinstance(query, str):
        raise TypeError("FLOW.QUERY query must be text")
    if len(query) > FLOW_QUERY_MAX_BYTES:
        raise ValueError(f"FLOW.QUERY query exceeds {FLOW_QUERY_MAX_BYTES} bytes")
    if not query.strip():
        raise ValueError("FLOW.QUERY query must not be empty")
    try:
        size = len(query.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("FLOW.QUERY query must be valid UTF-8") from exc
    if size > FLOW_QUERY_MAX_BYTES:
        raise ValueError(f"FLOW.QUERY query exceeds {FLOW_QUERY_MAX_BYTES} bytes")


def validate_flow_query_parameter_name(name: str) -> None:
    if len(name) > FLOW_QUERY_MAX_PARAMETER_NAME_BYTES:
        raise ValueError(
            f"FLOW.QUERY parameter names must be 1..{FLOW_QUERY_MAX_PARAMETER_NAME_BYTES} bytes"
        )
    try:
        size = len(name.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("FLOW.QUERY parameter names must be valid UTF-8") from exc
    if size == 0 or size > FLOW_QUERY_MAX_PARAMETER_NAME_BYTES:
        raise ValueError(
            f"FLOW.QUERY parameter names must be 1..{FLOW_QUERY_MAX_PARAMETER_NAME_BYTES} bytes"
        )
    if _PARAMETER_NAME.fullmatch(name) is None:
        raise ValueError(
            "FLOW.QUERY parameter names may contain only ASCII letters, digits, '_', '-', or '.'"
        )


def validate_flow_query_index_id(index_id: str) -> None:
    if not isinstance(index_id, str):
        raise TypeError("query index id must be text")
    if _INDEX_ID.fullmatch(index_id) is None:
        raise ValueError(
            "query index id must be 1..64 ASCII letters, digits, '_', '-', ':', or '.'"
        )


def has_explain_prefix(query: str) -> bool:
    # FQL's grammar recognizes ASCII whitespace only.  Treating every Unicode
    # whitespace code point as syntax would silently change otherwise-invalid
    # raw queries before they reach the server.
    stripped = query.lstrip(" \t\n\r")
    keyword = stripped[:7]
    return keyword.casefold() == "explain" and (len(stripped) == 7 or stripped[7] in " \t\n\r")


def _command_text(value: Any, context: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"{context} must be valid UTF-8") from exc
    raise TypeError(f"{context} must be text")


__all__ = [
    "FLOW_QUERY_LANGUAGE_VERSION",
    "FLOW_QUERY_MAX_BYTES",
    "FLOW_QUERY_MAX_PARAMETERS",
    "FLOW_QUERY_MAX_PARAMETER_NAME_BYTES",
    "FLOW_QUERY_REQUEST_CONTRACT",
    "build_flow_query_args",
    "build_flow_query_payload",
    "flow_query_command_routing_key",
    "has_explain_prefix",
    "normalize_flow_query_deadline",
    "normalize_flow_query_parameter",
    "normalize_flow_query_params",
    "validate_flow_query_index_id",
    "validate_flow_query_parameter_name",
    "validate_flow_query_text",
]
