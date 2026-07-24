from __future__ import annotations

from typing import Any


def normalize_flow_selector_segment(
    value: Any,
    context: str,
    *,
    maximum_bytes: int,
    reject_reserved: bool,
) -> str:
    """Validate an FQL bracket segment without changing its byte identity."""

    if not isinstance(value, str):
        raise TypeError(f"Flow query {context} must be text")
    if len(value) > maximum_bytes:
        raise ValueError(f"Flow query {context} must be 1..{maximum_bytes} bytes")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError(f"Flow query {context} must be valid UTF-8") from exc
    if not 1 <= size <= maximum_bytes:
        raise ValueError(f"Flow query {context} must be 1..{maximum_bytes} bytes")
    if reject_reserved and value.startswith("__"):
        raise ValueError(f"Flow query {context} is reserved")
    return value


def quote_flow_selector_segment(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def flow_metadata_selector(root: str, *segments: str) -> str:
    return root + "".join(f"[{quote_flow_selector_segment(value)}]" for value in segments)


__all__ = [
    "flow_metadata_selector",
    "normalize_flow_selector_segment",
    "quote_flow_selector_segment",
]
