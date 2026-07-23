from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

FLOW_QUERY_LANGUAGE_VERSION = "FQL1"
FLOW_QUERY_REQUEST_CONTRACT = "ferric.flow.query.request/v1"
FLOW_QUERY_MAX_BYTES = 16 * 1024
FLOW_QUERY_MAX_PARAMETERS = 64
FLOW_QUERY_MAX_PARAMETER_NAME_BYTES = 128

_INDEX_ID = re.compile(r"[A-Za-z0-9_.:-]{1,64}\Z", re.ASCII)


def build_flow_query_args(query: str, params: Mapping[str, Any] | None = None) -> list[Any]:
    validate_flow_query_text(query)
    normalized = normalize_flow_query_params(params)
    args: list[Any] = ["FLOW.QUERY", FLOW_QUERY_LANGUAGE_VERSION, query]
    for name in sorted(normalized):
        args.extend((name, normalized[name]))
    return args


def build_flow_query_payload(args: Sequence[Any]) -> dict[str, Any]:
    if len(args) < 2:
        raise ValueError("FLOW.QUERY requires version and query")
    if (len(args) - 2) % 2:
        raise ValueError("FLOW.QUERY parameters must be name/value pairs")
    version = _command_text(args[0], "FLOW.QUERY version")
    if version != FLOW_QUERY_LANGUAGE_VERSION:
        raise ValueError(f"FLOW.QUERY requires version {FLOW_QUERY_LANGUAGE_VERSION}")
    query = _command_text(args[1], "FLOW.QUERY query")
    validate_flow_query_text(query)
    parameter_count = (len(args) - 2) // 2
    if parameter_count > FLOW_QUERY_MAX_PARAMETERS:
        raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_PARAMETERS} parameters")
    params: dict[str, Any] = {}
    for index in range(2, len(args), 2):
        name = _command_text(args[index], "FLOW.QUERY parameter name")
        validate_flow_query_parameter_name(name)
        if name in params:
            raise ValueError(f"FLOW.QUERY parameter {name!r} is duplicated")
        params[name] = normalize_flow_query_parameter(args[index + 1], name=name)
    payload: dict[str, Any] = {"version": version, "query": query}
    if params:
        payload["params"] = params
    return payload


def normalize_flow_query_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    if params is None:
        return {}
    if not isinstance(params, Mapping):
        raise TypeError("FLOW.QUERY params must be a mapping")
    if len(params) > FLOW_QUERY_MAX_PARAMETERS:
        raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_PARAMETERS} parameters")
    normalized: dict[str, Any] = {}
    for name, value in params.items():
        if not isinstance(name, str):
            raise TypeError("FLOW.QUERY parameter names must be strings")
        validate_flow_query_parameter_name(name)
        normalized[name] = normalize_flow_query_parameter(value, name=name)
    return normalized


def normalize_flow_query_parameter(value: Any, *, name: str) -> Any:
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError(f"FLOW.QUERY parameter {name!r} must be valid UTF-8") from exc
        return value
    if isinstance(value, bytes) or type(value) is bool:
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
    if not query.strip():
        raise ValueError("FLOW.QUERY query must not be empty")
    try:
        size = len(query.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("FLOW.QUERY query must be valid UTF-8") from exc
    if size > FLOW_QUERY_MAX_BYTES:
        raise ValueError(f"FLOW.QUERY query exceeds {FLOW_QUERY_MAX_BYTES} bytes")


def validate_flow_query_parameter_name(name: str) -> None:
    try:
        size = len(name.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("FLOW.QUERY parameter names must be valid UTF-8") from exc
    if size == 0 or size > FLOW_QUERY_MAX_PARAMETER_NAME_BYTES:
        raise ValueError(
            f"FLOW.QUERY parameter names must be 1..{FLOW_QUERY_MAX_PARAMETER_NAME_BYTES} bytes"
        )


def validate_flow_query_index_id(index_id: str) -> None:
    if not isinstance(index_id, str):
        raise TypeError("query index id must be text")
    if _INDEX_ID.fullmatch(index_id) is None:
        raise ValueError(
            "query index id must be 1..64 ASCII letters, digits, '_', '-', ':', or '.'"
        )


def has_explain_prefix(query: str) -> bool:
    stripped = query.lstrip()
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
    "has_explain_prefix",
    "normalize_flow_query_parameter",
    "normalize_flow_query_params",
    "validate_flow_query_index_id",
    "validate_flow_query_parameter_name",
    "validate_flow_query_text",
]
