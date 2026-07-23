from __future__ import annotations

from typing import Any

from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_types import (
    FlowExplainResult,
    FlowQueryError,
    FlowQueryErrorPosition,
    FlowQueryIndex,
    FlowQueryIndexRegistry,
    FlowQueryIndexStatus,
    FlowQueryPage,
    FlowQueryQuality,
    FlowQueryResult,
    FlowQueryUsage,
)
from ferricstore.protocol_common import _map_get

FLOW_QUERY_RESULT_CONTRACT = "ferric.flow.query.result/v1"
FLOW_EXPLAIN_CONTRACT = "ferric.flow.explain/v1"
FLOW_QUERY_INDEXES_CONTRACT = "ferric.flow.query.indexes/v1"

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
        actual=actual,
        diagnostic=diagnostic,
        raw=mapping,
    )


def decode_flow_query_error(value: Any, *, raw: Any) -> FlowQueryError | None:
    if not isinstance(value, dict):
        return None
    try:
        code = _required_text(value, "code", "FLOW.QUERY diagnostic")
        message = _required_text(value, "message", "FLOW.QUERY diagnostic")
        detail = _optional_text(value, "detail", "FLOW.QUERY diagnostic")
        hint = _optional_text(value, "hint", "FLOW.QUERY diagnostic")
        retryable = _required_bool(value, "retryable", "FLOW.QUERY diagnostic")
        safe_to_retry = _required_bool(value, "safe_to_retry", "FLOW.QUERY diagnostic")
        retry_after_ms = _nonnegative_int(
            _map_get(value, "retry_after_ms"), "FLOW.QUERY diagnostic retry_after_ms"
        )
        context_value = _map_get(value, "context")
        if context_value is not None and not isinstance(context_value, dict):
            raise _decode_error("FLOW.QUERY diagnostic context must be a map", value)
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


def decode_flow_query_index_status(value: Any) -> FlowQueryIndexStatus:
    mapping = _required_map_value(value, "FLOW.QUERY.INDEXES")
    _require_contract(
        mapping, "contract_version", FLOW_QUERY_INDEXES_CONTRACT, "FLOW.QUERY.INDEXES"
    )
    observed_at_ms = _nonnegative_int(
        _map_get(mapping, "observed_at_ms"), "FLOW.QUERY.INDEXES observed_at_ms"
    )
    statistics_max_age_ms = _nonnegative_int(
        _map_get(mapping, "statistics_max_age_ms"),
        "FLOW.QUERY.INDEXES statistics_max_age_ms",
    )
    raw_registry = _required_map(mapping, "registry", "FLOW.QUERY.INDEXES")
    epoch = _unsigned_int(_map_get(raw_registry, "epoch"), "FLOW.QUERY.INDEXES epoch")
    catalog_version = _positive_unsigned_int(
        _map_get(raw_registry, "catalog_version"), "FLOW.QUERY.INDEXES catalog_version"
    )
    services = _required_map(mapping, "services", "FLOW.QUERY.INDEXES")
    raw_indexes = _map_get(mapping, "indexes")
    if not isinstance(raw_indexes, (list, tuple)) or len(raw_indexes) > 32:
        raise _decode_error("FLOW.QUERY.INDEXES indexes must contain at most 32 entries", value)
    indexes = tuple(_decode_index(entry, position) for position, entry in enumerate(raw_indexes))
    return FlowQueryIndexStatus(
        contract_version=FLOW_QUERY_INDEXES_CONTRACT,
        observed_at_ms=observed_at_ms,
        statistics_max_age_ms=statistics_max_age_ms,
        registry=FlowQueryIndexRegistry(epoch=epoch, catalog_version=catalog_version),
        services=services,
        indexes=indexes,
        raw=mapping,
    )


def _decode_index(value: Any, position: int) -> FlowQueryIndex:
    mapping = _required_map_value(value, f"FLOW.QUERY.INDEXES index {position}")
    return FlowQueryIndex(
        id=_required_text(mapping, "id", "FLOW.QUERY.INDEXES index"),
        version=_positive_unsigned_int(
            _map_get(mapping, "version"), "FLOW.QUERY.INDEXES index version"
        ),
        build_id=_required_text(mapping, "build_id", "FLOW.QUERY.INDEXES index"),
        state=_required_text(mapping, "state", "FLOW.QUERY.INDEXES index"),
        queryable=_required_bool(mapping, "queryable", "FLOW.QUERY.INDEXES index"),
        raw=mapping,
    )


def _decode_quality(value: Any) -> FlowQueryQuality:
    mapping = _required_map_value(value, "FLOW.QUERY quality")
    return FlowQueryQuality(
        exactness=_required_bounded_text(mapping, "exactness", "FLOW.QUERY quality", 64),
        freshness=_required_bounded_text(mapping, "freshness", "FLOW.QUERY quality", 64),
        coverage=_required_bounded_text(mapping, "coverage", "FLOW.QUERY quality", 64),
        pagination=_required_bounded_text(mapping, "pagination", "FLOW.QUERY quality", 64),
    )


def _decode_usage(value: Any) -> FlowQueryUsage:
    mapping = _required_map_value(value, "FLOW.QUERY usage")
    values = {
        field: _nonnegative_int(_map_get(mapping, field), f"FLOW.QUERY usage {field}")
        for field in _USAGE_FIELDS
    }
    return FlowQueryUsage(**values)


def _decode_page(value: Any) -> FlowQueryPage:
    mapping = _required_map_value(value, "FLOW.QUERY page")
    has_more = _required_bool(mapping, "has_more", "FLOW.QUERY page")
    cursor = _optional_text(mapping, "cursor", "FLOW.QUERY page")
    if cursor is not None and (not cursor.startswith("fqc1_") or len(cursor.encode()) > 4096):
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
    value = _map_get(mapping, field)
    text = _text(value)
    if text is None or text == "":
        raise _decode_error(f"{context} {field} must be non-empty text", mapping)
    return text


def _required_bounded_text(
    mapping: dict[Any, Any], field: str, context: str, maximum_bytes: int
) -> str:
    text = _required_text(mapping, field, context)
    if len(text.encode("utf-8")) > maximum_bytes:
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


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            return None
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
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


def _unsigned_int(value: Any, context: str) -> int:
    if type(value) is not int or value < 0 or value > 2**64 - 1:
        raise _decode_error(f"{context} must be an unsigned 64-bit integer", value)
    return value


def _positive_unsigned_int(value: Any, context: str) -> int:
    parsed = _unsigned_int(value, context)
    if parsed == 0:
        raise _decode_error(f"{context} must be positive", value)
    return parsed


def _has_key(mapping: dict[Any, Any], field: str) -> bool:
    return field in mapping or field.encode() in mapping


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
