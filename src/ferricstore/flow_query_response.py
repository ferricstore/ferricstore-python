from __future__ import annotations

from typing import Any

from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_index_response import (
    FLOW_QUERY_INDEXES_CONTRACT,
    decode_flow_query_index_status,
)
from ferricstore.flow_query_types import (
    FlowExplainCapabilities,
    FlowExplainResult,
    FlowQueryError,
    FlowQueryErrorPosition,
    FlowQueryPage,
    FlowQueryQuality,
    FlowQueryResult,
    FlowQueryUsage,
)
from ferricstore.protocol_common import _map_get

FLOW_QUERY_RESULT_CONTRACT = "ferric.flow.query.result/v1"
FLOW_EXPLAIN_CONTRACT = "ferric.flow.explain/v1"

_USAGE_FIELDS = (
    "range_seeks",
    "range_pages",
    "scanned_entries",
    "scanned_bytes",
    "hydrated_records",
    "residual_checks",
    "duplicate_entries",
    "result_records",
    "response_bytes",
    "memory_high_water_bytes",
    "wall_time_us",
)

_DIAGNOSTIC_TEXT_BYTES = 1_024
_DIAGNOSTIC_CONTEXT_ENTRIES = 16
_DIAGNOSTIC_CONTEXT_LIST_ITEMS = 32
_DIAGNOSTIC_CONTEXT_KEY_BYTES = 128
_DIAGNOSTIC_CONTEXT_DEPTH = 6
_DIAGNOSTIC_CONTEXT_NODES = 512
_QUALITY_VALUES = {
    "exactness": frozenset(("authoritative", "projected_exact", "exact", "not_applicable")),
    "freshness": frozenset(("current", "projection_watermark", "not_applicable")),
    "coverage": frozenset(("complete", "unavailable")),
    "pagination": frozenset(("none", "complete", "authenticated_seek", "live_seek")),
}


def decode_flow_query_result(value: Any) -> FlowQueryResult:
    mapping = _required_map_value(value, "FLOW.QUERY result")
    _require_contract(mapping, "version", FLOW_QUERY_RESULT_CONTRACT, "FLOW.QUERY result")
    quality = _decode_quality(_map_get(mapping, "quality"))
    usage = _decode_usage(_map_get(mapping, "usage"))
    has_records = _has_key(mapping, "records")
    has_count = _has_key(mapping, "result")
    if has_records == has_count:
        raise _decode_error(
            "FLOW.QUERY result must contain exactly one records or count shape", value
        )

    if has_records:
        raw_records = _map_get(mapping, "records")
        if not isinstance(raw_records, (list, tuple)) or len(raw_records) > 100:
            raise _decode_error("FLOW.QUERY records must be an array of at most 100 maps", value)
        if not all(isinstance(item, dict) for item in raw_records):
            raise _decode_error("FLOW.QUERY records contain a non-map item", value)
        records = tuple(raw_records)
        if usage.result_records != len(records):
            raise _decode_error("FLOW.QUERY usage result_records does not match records", value)
        if usage.result_records > usage.scanned_entries:
            raise _decode_error("FLOW.QUERY usage counters are inconsistent", value)
        page = _decode_page(_map_get(mapping, "page"))
        return FlowQueryResult(
            version=FLOW_QUERY_RESULT_CONTRACT,
            records=records,
            page=page,
            count=None,
            quality=quality,
            usage=usage,
            raw=mapping,
        )

    if _has_key(mapping, "page"):
        raise _decode_error("FLOW.QUERY count result contains an unexpected page", value)
    count_result = _required_map_value(_map_get(mapping, "result"), "FLOW.QUERY count result")
    if _required_text(count_result, "kind", "FLOW.QUERY count result") != "count":
        raise _decode_error("FLOW.QUERY count result kind must be count", value)
    count = _nonnegative_int(_map_get(count_result, "value"), "FLOW.QUERY count value")
    if usage.result_records != 1:
        raise _decode_error("FLOW.QUERY count usage result_records must be 1", value)
    return FlowQueryResult(
        version=FLOW_QUERY_RESULT_CONTRACT,
        records=None,
        page=None,
        count=count,
        quality=quality,
        usage=usage,
        raw=mapping,
    )


def decode_flow_explain_result(value: Any) -> FlowExplainResult:
    mapping = _required_map_value(value, "FLOW.QUERY explain")
    _require_contract(mapping, "version", FLOW_EXPLAIN_CONTRACT, "FLOW.QUERY explain")
    fingerprint = _required_text(mapping, "query_fingerprint", "FLOW.QUERY explain")
    if len(fingerprint) != 64 or any(char not in "0123456789abcdefABCDEF" for char in fingerprint):
        raise _decode_error("FLOW.QUERY explain query_fingerprint is invalid", value)
    status = _required_text(mapping, "status", "FLOW.QUERY explain")
    if status not in {"planned", "rejected", "executed"}:
        raise _decode_error(f"FLOW.QUERY explain status {status!r} is unsupported", value)
    plan = _required_map(mapping, "plan", "FLOW.QUERY explain")
    estimate = _required_map(mapping, "estimate", "FLOW.QUERY explain")
    bounds = _required_map(mapping, "bounds", "FLOW.QUERY explain")
    capabilities = _decode_explain_capabilities(mapping)
    extended_fields = ("stats", "quality", "pressure", "decision", "alternatives")
    present = tuple(_has_key(mapping, field) for field in extended_fields)
    specialized = capabilities is not None and not any(present)
    if specialized:
        if status != "planned":
            raise _decode_error("FLOW.QUERY specialized explain must be planned", value)
        if _has_key(mapping, "actual") or _has_key(mapping, "diagnostic"):
            raise _decode_error("FLOW.QUERY specialized explain has extended status fields", value)
        stats = None
        quality = None
        pressure = None
        decision = None
        alternatives: tuple[dict[Any, Any], ...] = ()
    else:
        if (
            not all(present)
            or not _has_key(mapping, "actual")
            or not _has_key(mapping, "diagnostic")
        ):
            raise _decode_error("FLOW.QUERY explain is missing required v1 fields", value)
        stats = _required_map(mapping, "stats", "FLOW.QUERY explain")
        quality = _decode_quality(_map_get(mapping, "quality"))
        pressure = _required_map(mapping, "pressure", "FLOW.QUERY explain")
        decision = _required_map(mapping, "decision", "FLOW.QUERY explain")
        alternatives = _decode_explain_alternatives(_map_get(mapping, "alternatives"))

    actual_value = _map_get(mapping, "actual")
    if status == "executed":
        if actual_value is None:
            raise _decode_error("FLOW.QUERY executed explain is missing actual usage", value)
        actual = _decode_usage(actual_value)
    else:
        if actual_value is not None:
            raise _decode_error("FLOW.QUERY non-executed explain contains actual usage", value)
        actual = None

    diagnostic_value = _map_get(mapping, "diagnostic")
    if status == "rejected":
        diagnostic = decode_flow_query_error(diagnostic_value, raw=diagnostic_value)
        if diagnostic is None:
            raise _decode_error("FLOW.QUERY rejected explain has an invalid diagnostic", value)
    else:
        if diagnostic_value is not None:
            raise _decode_error("FLOW.QUERY non-rejected explain contains a diagnostic", value)
        diagnostic = None

    return FlowExplainResult(
        version=FLOW_EXPLAIN_CONTRACT,
        query_fingerprint=fingerprint,
        status=status,
        plan=plan,
        estimate=estimate,
        bounds=bounds,
        stats=stats,
        quality=quality,
        pressure=pressure,
        decision=decision,
        alternatives=alternatives,
        capabilities=capabilities,
        actual=actual,
        diagnostic=diagnostic,
        raw=mapping,
    )


def decode_flow_query_error(value: Any, *, raw: Any) -> FlowQueryError | None:
    if not isinstance(value, dict):
        return None
    try:
        code = _required_bounded_text(
            value, "code", "FLOW.QUERY diagnostic", _DIAGNOSTIC_TEXT_BYTES
        )
        message = _required_bounded_text(
            value, "message", "FLOW.QUERY diagnostic", _DIAGNOSTIC_TEXT_BYTES
        )
        detail = _optional_bounded_text(
            value, "detail", "FLOW.QUERY diagnostic", _DIAGNOSTIC_TEXT_BYTES
        )
        hint = _optional_bounded_text(
            value, "hint", "FLOW.QUERY diagnostic", _DIAGNOSTIC_TEXT_BYTES
        )
        retryable = _required_bool(value, "retryable", "FLOW.QUERY diagnostic")
        safe_to_retry = _required_bool(value, "safe_to_retry", "FLOW.QUERY diagnostic")
        retry_after_ms = _nonnegative_int(
            _map_get(value, "retry_after_ms"), "FLOW.QUERY diagnostic retry_after_ms"
        )
        context_value = _map_get(value, "context")
        if context_value is not None and not isinstance(context_value, dict):
            raise _decode_error("FLOW.QUERY diagnostic context must be a map", value)
        if context_value is not None:
            _validate_diagnostic_context(context_value)
        position = _decode_position(_map_get(value, "position"))
    except FerricStoreError:
        return None
    return FlowQueryError(
        code=code,
        message=message,
        detail=detail,
        hint=hint,
        retryable=retryable,
        safe_to_retry=safe_to_retry,
        retry_after_ms=retry_after_ms,
        position=position,
        context=context_value,
        raw=raw,
    )


def _decode_quality(value: Any) -> FlowQueryQuality:
    mapping = _required_map_value(value, "FLOW.QUERY quality")
    return FlowQueryQuality(
        exactness=_decode_quality_value(mapping, "exactness"),
        freshness=_decode_quality_value(mapping, "freshness"),
        coverage=_decode_quality_value(mapping, "coverage"),
        pagination=_decode_quality_value(mapping, "pagination"),
    )


def _decode_quality_value(mapping: dict[Any, Any], field: str) -> str:
    value = _required_bounded_text(mapping, field, "FLOW.QUERY quality", 64)
    if value not in _QUALITY_VALUES[field]:
        raise _decode_error(f"FLOW.QUERY quality {field} is unsupported", mapping)
    return value


def _decode_usage(value: Any) -> FlowQueryUsage:
    mapping = _required_map_value(value, "FLOW.QUERY usage")
    values = {
        field: _nonnegative_int(_map_get(mapping, field), f"FLOW.QUERY usage {field}")
        for field in _USAGE_FIELDS
    }
    usage = FlowQueryUsage(**values)
    if not (
        usage.hydrated_records <= usage.scanned_entries
        and usage.duplicate_entries <= usage.scanned_entries
        and usage.range_pages <= usage.scanned_entries + usage.range_seeks
        and usage.residual_checks <= usage.scanned_entries * 12
    ):
        raise _decode_error("FLOW.QUERY usage counters are inconsistent", value)
    return usage


def _decode_explain_capabilities(mapping: dict[Any, Any]) -> FlowExplainCapabilities | None:
    if not _has_key(mapping, "capabilities"):
        return None
    capabilities = _required_map(mapping, "capabilities", "FLOW.QUERY explain")
    decoded: dict[str, tuple[str, ...]] = {}
    for field in ("requested", "available", "missing"):
        values = _bounded_text_sequence(
            _map_get(capabilities, field),
            f"FLOW.QUERY explain capabilities {field}",
            maximum=64,
        )
        if len(values) != len(set(values)):
            raise _decode_error(
                f"FLOW.QUERY explain capabilities {field} contains duplicates",
                capabilities,
            )
        decoded[field] = values
    return FlowExplainCapabilities(
        requested=decoded["requested"],
        available=decoded["available"],
        missing=decoded["missing"],
        raw=capabilities,
    )


def _decode_explain_alternatives(value: Any) -> tuple[dict[Any, Any], ...]:
    if not isinstance(value, (list, tuple)) or len(value) > 31:
        raise _decode_error(
            "FLOW.QUERY explain alternatives must be an array of at most 31 maps",
            value,
        )
    if not all(isinstance(item, dict) for item in value):
        raise _decode_error("FLOW.QUERY explain alternatives contain a non-map", value)
    return tuple(value)


def _bounded_text_sequence(value: Any, context: str, *, maximum: int) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > maximum:
        raise _decode_error(f"{context} must be a bounded array", value)
    decoded: list[str] = []
    for item in value:
        text = _text(item)
        if text is None or text == "" or len(text.encode()) > 128:
            raise _decode_error(f"{context} contains invalid text", value)
        decoded.append(text)
    return tuple(decoded)


def _decode_page(value: Any) -> FlowQueryPage:
    mapping = _required_map_value(value, "FLOW.QUERY page")
    has_more = _required_bool(mapping, "has_more", "FLOW.QUERY page")
    cursor = _optional_text(mapping, "cursor", "FLOW.QUERY page")
    if cursor is not None:
        cursor_bytes = len(cursor.encode())
        if not cursor.startswith("fqc1_") or not 16 <= cursor_bytes <= 4_096:
            raise _decode_error("FLOW.QUERY page cursor is invalid", value)
    if has_more != (cursor is not None):
        raise _decode_error("FLOW.QUERY page has_more and cursor are inconsistent", value)
    return FlowQueryPage(has_more=has_more, cursor=cursor)


def _decode_position(value: Any) -> FlowQueryErrorPosition | None:
    if value is None:
        return None
    mapping = _required_map_value(value, "FLOW.QUERY diagnostic position")
    return FlowQueryErrorPosition(
        byte=_positive_int(_map_get(mapping, "byte"), "FLOW.QUERY diagnostic position byte"),
        line=_positive_int(_map_get(mapping, "line"), "FLOW.QUERY diagnostic position line"),
        column=_positive_int(_map_get(mapping, "column"), "FLOW.QUERY diagnostic position column"),
    )


def _required_map(mapping: dict[Any, Any], field: str, context: str) -> dict[Any, Any]:
    return _required_map_value(_map_get(mapping, field), f"{context} {field}")


def _required_map_value(value: Any, context: str) -> dict[Any, Any]:
    if not isinstance(value, dict):
        raise _decode_error(f"{context} must be a map", value)
    return value


def _require_contract(mapping: dict[Any, Any], field: str, expected: str, context: str) -> None:
    actual = _required_text(mapping, field, context)
    if actual != expected:
        raise _decode_error(f"{context} has unsupported contract {actual!r}", mapping)


def _required_text(mapping: dict[Any, Any], field: str, context: str) -> str:
    text = _text(_map_get(mapping, field))
    if text is None or text == "":
        raise _decode_error(f"{context} {field} must be non-empty text", mapping)
    return text


def _required_bounded_text(
    mapping: dict[Any, Any], field: str, context: str, maximum_bytes: int
) -> str:
    text = _required_text(mapping, field, context)
    if len(text.encode()) > maximum_bytes:
        raise _decode_error(f"{context} {field} exceeds {maximum_bytes} bytes", mapping)
    return text


def _optional_text(mapping: dict[Any, Any], field: str, context: str) -> str | None:
    value = _map_get(mapping, field)
    if value is None:
        return None
    text = _text(value)
    if text is None:
        raise _decode_error(f"{context} {field} must be text", mapping)
    return text


def _optional_bounded_text(
    mapping: dict[Any, Any], field: str, context: str, maximum_bytes: int
) -> str | None:
    text = _optional_text(mapping, field, context)
    if text is not None and len(text.encode()) > maximum_bytes:
        raise _decode_error(f"{context} {field} exceeds {maximum_bytes} bytes", mapping)
    return text


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        try:
            value.encode()
        except UnicodeEncodeError:
            return None
        return value
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return None
    return None


def _required_bool(mapping: dict[Any, Any], field: str, context: str) -> bool:
    value = _map_get(mapping, field)
    if type(value) is not bool:
        raise _decode_error(f"{context} {field} must be boolean", mapping)
    return value


def _nonnegative_int(value: Any, context: str) -> int:
    if type(value) is not int or value < 0 or value > 2**63 - 1:
        raise _decode_error(f"{context} must be a non-negative signed integer", value)
    return value


def _positive_int(value: Any, context: str) -> int:
    parsed = _nonnegative_int(value, context)
    if parsed == 0:
        raise _decode_error(f"{context} must be positive", value)
    return parsed


def _has_key(mapping: dict[Any, Any], field: str) -> bool:
    return field in mapping or field.encode() in mapping


def _validate_diagnostic_context(value: dict[Any, Any]) -> None:
    if len(value) > _DIAGNOSTIC_CONTEXT_ENTRIES:
        raise _decode_error("FLOW.QUERY diagnostic context contains too many entries", value)
    if _validate_context_value(value, _DIAGNOSTIC_CONTEXT_DEPTH, _DIAGNOSTIC_CONTEXT_NODES) < 0:
        raise _decode_error("FLOW.QUERY diagnostic context is invalid", value)


def _validate_context_value(value: Any, depth: int, remaining: int) -> int:
    if remaining <= 0:
        return -1
    if value is None or type(value) is bool:
        return remaining - 1
    if type(value) is int:
        return remaining - 1 if -(2**63) <= value <= 2**63 - 1 else -1
    if isinstance(value, (str, bytes)):
        text = _text(value)
        return (
            remaining - 1
            if text is not None and len(text.encode()) <= _DIAGNOSTIC_TEXT_BYTES
            else -1
        )
    if isinstance(value, dict):
        if depth <= 0 or len(value) > _DIAGNOSTIC_CONTEXT_ENTRIES:
            return -1
        remaining -= 1
        for raw_key, item in value.items():
            key = _text(raw_key)
            if key is None or key == "" or len(key.encode()) > _DIAGNOSTIC_CONTEXT_KEY_BYTES:
                return -1
            remaining = _validate_context_value(item, depth - 1, remaining)
            if remaining < 0:
                return -1
        return remaining
    if isinstance(value, (list, tuple)):
        if depth <= 0 or len(value) > _DIAGNOSTIC_CONTEXT_LIST_ITEMS:
            return -1
        remaining -= 1
        for item in value:
            remaining = _validate_context_value(item, depth - 1, remaining)
            if remaining < 0:
                return -1
        return remaining
    return -1


def _decode_error(message: str, raw: Any) -> FerricStoreError:
    return FerricStoreError(f"invalid server response: {message}", raw=raw)


__all__ = [
    "FLOW_EXPLAIN_CONTRACT",
    "FLOW_QUERY_INDEXES_CONTRACT",
    "FLOW_QUERY_RESULT_CONTRACT",
    "decode_flow_explain_result",
    "decode_flow_query_error",
    "decode_flow_query_index_status",
    "decode_flow_query_result",
]
