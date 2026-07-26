from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_index_contract import (
    BUILD_PHASES,
    RETIREMENT_PHASES,
    VALIDATION_PHASES,
    validate_flow_query_index_contract,
)
from ferricstore.flow_query_types import (
    FlowQueryIndex,
    FlowQueryIndexBuild,
    FlowQueryIndexCoverage,
    FlowQueryIndexField,
    FlowQueryIndexFormat,
    FlowQueryIndexRegistry,
    FlowQueryIndexRetirement,
    FlowQueryIndexServices,
    FlowQueryIndexStatistics,
    FlowQueryIndexStatus,
    FlowQueryIndexValidation,
)
from ferricstore.protocol_common import _map_get

FLOW_QUERY_INDEXES_CONTRACT = "ferric.flow.query.indexes/v1"

_INDEX_STATES = frozenset({"building", "validating", "active", "retiring", "failed"})
_INDEX_DIRECTIONS = frozenset({"asc", "desc"})
_INDEX_ENCODINGS = frozenset({"hashed", "ordered"})
_VALIDATION_STATES = frozenset({"pending", "passed", "failed"})
_RETIREMENT_STATES = frozenset({"not_applicable", "pending", "complete"})
_STATISTICS_STATES = frozenset({"fresh", "stale", "future", "mixed", "missing", "unavailable"})
_SERVICE_STATES = frozenset({"ready", "unavailable"})


def decode_flow_query_index_status(
    value: Any, *, expected_id: str | None = None
) -> FlowQueryIndexStatus:
    mapping = _required_map(value, "FLOW.QUERY.INDEXES")
    contract = _required_text(mapping, "contract_version", "FLOW.QUERY.INDEXES", 64)
    if contract != FLOW_QUERY_INDEXES_CONTRACT:
        raise _error(f"FLOW.QUERY.INDEXES has unsupported contract {contract!r}", value)
    registry = _required_field_map(mapping, "registry", "FLOW.QUERY.INDEXES")
    raw_indexes = _map_get(mapping, "indexes")
    if not isinstance(raw_indexes, (list, tuple)) or len(raw_indexes) > 32:
        raise _error("FLOW.QUERY.INDEXES indexes must contain at most 32 entries", value)
    status = FlowQueryIndexStatus(
        contract_version=FLOW_QUERY_INDEXES_CONTRACT,
        observed_at_ms=_unsigned(
            _map_get(mapping, "observed_at_ms"), "FLOW.QUERY.INDEXES observed_at_ms"
        ),
        statistics_max_age_ms=_unsigned(
            _map_get(mapping, "statistics_max_age_ms"),
            "FLOW.QUERY.INDEXES statistics_max_age_ms",
        ),
        registry=FlowQueryIndexRegistry(
            epoch=_unsigned(_map_get(registry, "epoch"), "FLOW.QUERY.INDEXES epoch"),
            catalog_version=_positive_unsigned(
                _map_get(registry, "catalog_version"),
                "FLOW.QUERY.INDEXES catalog_version",
            ),
        ),
        services=_decode_services(_map_get(mapping, "services")),
        indexes=tuple(_decode_index(entry, pos) for pos, entry in enumerate(raw_indexes)),
        raw=mapping,
    )
    validate_flow_query_index_contract(status, expected_id=expected_id)
    return status


def _decode_services(value: Any) -> FlowQueryIndexServices:
    mapping = _required_map(value, "FLOW.QUERY.INDEXES services")
    context = "FLOW.QUERY.INDEXES services"
    return FlowQueryIndexServices(
        registry=_choice(mapping, "registry", context, _SERVICE_STATES),
        lifecycle_worker=_choice(mapping, "lifecycle_worker", context, _SERVICE_STATES),
        statistics_store=_choice(mapping, "statistics_store", context, _SERVICE_STATES),
        statistics_worker=_choice(mapping, "statistics_worker", context, _SERVICE_STATES),
        raw=mapping,
    )


def _decode_index(value: Any, position: int) -> FlowQueryIndex:
    mapping = _required_map(value, f"FLOW.QUERY.INDEXES index {position}")
    context = "FLOW.QUERY.INDEXES index"
    fields = _decode_fields(_map_get(mapping, "fields"))
    workloads = _text_sequence(
        _map_get(mapping, "workloads"), f"{context} workloads", maximum=16, text_bytes=64
    )
    if len(workloads) != len(set(workloads)):
        raise _error(f"{context} workloads contain duplicates", value)
    return FlowQueryIndex(
        id=_required_text(mapping, "id", context, 64),
        version=_positive_unsigned(_map_get(mapping, "version"), f"{context} version"),
        build_id=_required_text(mapping, "build_id", context, 128),
        source=_choice(mapping, "source", context, frozenset({"runs"})),
        state=_choice(mapping, "state", context, _INDEX_STATES),
        queryable=_required_bool(mapping, "queryable", context),
        fields=fields,
        workloads=workloads,
        count_prefixes=_decode_count_prefixes(_map_get(mapping, "count_prefixes"), len(fields)),
        covering_fields=_decode_covering_fields(_map_get(mapping, "covering_fields")),
        format=_decode_format(_map_get(mapping, "format")),
        coverage=_decode_coverage(_map_get(mapping, "coverage")),
        build=_decode_build(_map_get(mapping, "build")),
        validation=_decode_validation(_map_get(mapping, "validation")),
        retirement=_decode_retirement(_map_get(mapping, "retirement")),
        statistics=_decode_statistics(_map_get(mapping, "statistics")),
        raw=mapping,
    )


def _decode_fields(value: Any) -> tuple[FlowQueryIndexField, ...]:
    context = "FLOW.QUERY.INDEXES index fields"
    if not isinstance(value, (list, tuple)) or not 2 <= len(value) <= 8:
        raise _error(f"{context} must contain 2 to 8 entries", value)
    fields: list[FlowQueryIndexField] = []
    for position, entry in enumerate(value):
        mapping = _required_map(entry, f"FLOW.QUERY.INDEXES index field {position}")
        field_context = "FLOW.QUERY.INDEXES index field"
        fields.append(
            FlowQueryIndexField(
                # Quoted state_meta segments can expand through apostrophe escaping.
                name=_required_text(mapping, "name", field_context, 512),
                direction=_choice(mapping, "direction", field_context, _INDEX_DIRECTIONS),
                encoding=_choice(mapping, "encoding", field_context, _INDEX_ENCODINGS),
                raw=mapping,
            )
        )
    names = [field.name for field in fields]
    if len(names) != len(set(names)):
        raise _error(f"{context} contain duplicates", value)
    return tuple(fields)


def _decode_count_prefixes(value: Any, field_count: int) -> tuple[int, ...]:
    context = "FLOW.QUERY.INDEXES index count_prefixes"
    if not isinstance(value, (list, tuple)) or len(value) > field_count:
        raise _error(f"{context} are invalid", value)
    prefixes = tuple(_positive_unsigned(item, f"{context} entry") for item in value)
    if any(item > field_count for item in prefixes) or tuple(sorted(set(prefixes))) != prefixes:
        raise _error(f"{context} are invalid", value)
    return prefixes


def _decode_covering_fields(value: Any) -> tuple[str, ...]:
    context = "FLOW.QUERY.INDEXES index covering_fields"
    fields = _text_sequence(value, context, maximum=32, text_bytes=512)
    if len(fields) != len(set(fields)):
        raise _error(f"{context} contain duplicates", value)
    return fields


def _decode_format(value: Any) -> FlowQueryIndexFormat:
    context = "FLOW.QUERY.INDEXES index format"
    mapping = _required_map(value, context)
    if not _has_key(mapping, "counter"):
        raise _error(f"{context} is missing nullable counter", value)
    counter = _optional_text(mapping, "counter", context, 128)
    if counter == "":
        raise _error(f"{context} counter must be null or non-empty text", value)
    return FlowQueryIndexFormat(
        query_row=_required_text(mapping, "query_row", context, 128),
        key=_required_text(mapping, "key", context, 128),
        entry=_required_text(mapping, "entry", context, 128),
        reverse=_required_text(mapping, "reverse", context, 128),
        counter=counter,
        raw=mapping,
    )


def _decode_coverage(value: Any) -> FlowQueryIndexCoverage:
    context = "FLOW.QUERY.INDEXES index coverage"
    mapping = _required_map(value, context)
    complete = _unsigned(_map_get(mapping, "complete_shards"), f"{context} complete_shards")
    total = _positive_unsigned(_map_get(mapping, "total_shards"), f"{context} total_shards")
    if complete > total:
        raise _error(f"{context} complete_shards exceeds total_shards", value)
    return FlowQueryIndexCoverage(
        complete_shards=complete,
        total_shards=total,
        validation=_choice(mapping, "validation", context, _VALIDATION_STATES),
        raw=mapping,
    )


def _decode_build(value: Any) -> FlowQueryIndexBuild:
    section = "build"
    mapping = _required_map(value, f"FLOW.QUERY.INDEXES index {section}")
    completed, total = _shard_progress(mapping, section)
    return FlowQueryIndexBuild(
        scope=_catalog_scope(mapping, section),
        phase_counts=_phase_counts(_map_get(mapping, "phase_counts"), section),
        current_phases=_phases(_map_get(mapping, "current_phases"), section, BUILD_PHASES),
        completed_shards=completed,
        total_shards=total,
        scanned_records=_counter(mapping, "scanned_records", section),
        written_entries=_counter(mapping, "written_entries", section),
        written_bytes=_counter(mapping, "written_bytes", section),
        raw=mapping,
    )


def _decode_validation(value: Any) -> FlowQueryIndexValidation:
    section = "validation"
    mapping = _required_map(value, f"FLOW.QUERY.INDEXES index {section}")
    if not _has_key(mapping, "failure_reason") or not _has_key(mapping, "validated_at_ms"):
        raise _error(f"FLOW.QUERY.INDEXES index {section} is missing nullable fields", value)
    completed, total = _shard_progress(mapping, section)
    context = f"FLOW.QUERY.INDEXES index {section}"
    return FlowQueryIndexValidation(
        scope=_catalog_scope(mapping, section),
        status=_choice(mapping, "status", context, _VALIDATION_STATES),
        phase_counts=_phase_counts(_map_get(mapping, "phase_counts"), section),
        current_phases=_phases(_map_get(mapping, "current_phases"), section, VALIDATION_PHASES),
        completed_shards=completed,
        total_shards=total,
        checked_records=_counter(mapping, "checked_records", section),
        checked_entries=_counter(mapping, "checked_entries", section),
        mismatches=_counter(mapping, "mismatches", section),
        failure_reason=_optional_text(mapping, "failure_reason", context, 128),
        validated_at_ms=_optional_unsigned(
            _map_get(mapping, "validated_at_ms"), f"{context} validated_at_ms"
        ),
        raw=mapping,
    )


def _decode_retirement(value: Any) -> FlowQueryIndexRetirement:
    section = "retirement"
    context = f"FLOW.QUERY.INDEXES index {section}"
    mapping = _required_map(value, context)
    status = _choice(mapping, "status", context, _RETIREMENT_STATES)
    if status == "not_applicable":
        return FlowQueryIndexRetirement(
            status=status,
            phase_counts=None,
            current_phases=None,
            completed_shards=None,
            total_shards=None,
            deleted_entries=None,
            deleted_bytes=None,
            rewritten_reverse_rows=None,
            raw=mapping,
        )
    completed, total = _shard_progress(mapping, section)
    return FlowQueryIndexRetirement(
        status=status,
        phase_counts=_phase_counts(_map_get(mapping, "phase_counts"), section),
        current_phases=_phases(_map_get(mapping, "current_phases"), section, RETIREMENT_PHASES),
        completed_shards=completed,
        total_shards=total,
        deleted_entries=_counter(mapping, "deleted_entries", section),
        deleted_bytes=_counter(mapping, "deleted_bytes", section),
        rewritten_reverse_rows=_counter(mapping, "rewritten_reverse_rows", section),
        raw=mapping,
    )


def _decode_statistics(value: Any) -> FlowQueryIndexStatistics:
    section = "statistics"
    context = f"FLOW.QUERY.INDEXES index {section}"
    mapping = _required_map(value, context)
    nullable_fields = (
        "oldest_collected_at_ms",
        "newest_collected_at_ms",
        "oldest_age_ms",
        "newest_age_ms",
    )
    if any(not _has_key(mapping, field) for field in nullable_fields):
        raise _error(f"{context} is missing required nullable fields", value)
    samples = _counter(mapping, "samples", section)
    fresh = _counter(mapping, "fresh_samples", section)
    stale = _counter(mapping, "stale_samples", section)
    future = _counter(mapping, "future_samples", section)
    if fresh + stale != samples or future > stale:
        raise _error(f"{context} counters are inconsistent", value)
    return FlowQueryIndexStatistics(
        status=_choice(mapping, "status", context, _STATISTICS_STATES),
        samples=samples,
        fresh_samples=fresh,
        stale_samples=stale,
        future_samples=future,
        oldest_collected_at_ms=_optional_counter(mapping, "oldest_collected_at_ms", section),
        newest_collected_at_ms=_optional_counter(mapping, "newest_collected_at_ms", section),
        oldest_age_ms=_optional_counter(mapping, "oldest_age_ms", section),
        newest_age_ms=_optional_counter(mapping, "newest_age_ms", section),
        raw=mapping,
    )


def _shard_progress(mapping: dict[Any, Any], section: str) -> tuple[int, int]:
    context = f"FLOW.QUERY.INDEXES index {section}"
    completed = _unsigned(_map_get(mapping, "completed_shards"), f"{context} completed_shards")
    total = _positive_unsigned(_map_get(mapping, "total_shards"), f"{context} total_shards")
    if completed > total:
        raise _error(f"{context} completed_shards exceeds total_shards", mapping)
    return completed, total


def _catalog_scope(mapping: dict[Any, Any], section: str) -> str:
    return _choice(
        mapping,
        "scope",
        f"FLOW.QUERY.INDEXES index {section}",
        frozenset({"catalog_build"}),
    )


def _counter(mapping: dict[Any, Any], field: str, section: str) -> int:
    return _unsigned(_map_get(mapping, field), f"FLOW.QUERY.INDEXES index {section} {field}")


def _optional_counter(mapping: dict[Any, Any], field: str, section: str) -> int | None:
    return _optional_unsigned(
        _map_get(mapping, field), f"FLOW.QUERY.INDEXES index {section} {field}"
    )


def _phase_counts(value: Any, section: str) -> dict[str, int]:
    context = f"FLOW.QUERY.INDEXES index {section} phase_counts"
    mapping = _required_map(value, context)
    if len(mapping) > 16:
        raise _error(f"{context} contains too many entries", value)
    counts: dict[str, int] = {}
    for raw_phase, raw_count in mapping.items():
        phase = _decode_bounded_text(raw_phase, 64)
        if phase is None or phase == "" or phase in counts:
            raise _error(f"{context} contains an invalid phase", value)
        counts[phase] = _unsigned(raw_count, f"{context} {phase}")
    return counts


def _phases(value: Any, section: str, allowed: Sequence[str]) -> tuple[str, ...]:
    context = f"FLOW.QUERY.INDEXES index {section} current_phases"
    phases = _text_sequence(value, context, maximum=len(allowed), text_bytes=64)
    if len(phases) != len(set(phases)) or any(phase not in allowed for phase in phases):
        raise _error(f"{context} are invalid", value)
    return phases


def _text_sequence(value: Any, context: str, *, maximum: int, text_bytes: int) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) > maximum:
        raise _error(f"{context} must be a bounded array", value)
    items: list[str] = []
    for item in value:
        text = _decode_bounded_text(item, text_bytes)
        if text is None or text == "":
            raise _error(f"{context} contains invalid text", value)
        items.append(text)
    return tuple(items)


def _required_field_map(mapping: dict[Any, Any], field: str, context: str) -> dict[Any, Any]:
    return _required_map(_map_get(mapping, field), f"{context} {field}")


def _has_key(mapping: dict[Any, Any], field: str) -> bool:
    return field in mapping or field.encode() in mapping


def _required_map(value: Any, context: str) -> dict[Any, Any]:
    if not isinstance(value, dict):
        raise _error(f"{context} must be a map", value)
    return value


def _required_text(mapping: dict[Any, Any], field: str, context: str, maximum_bytes: int) -> str:
    raw = _map_get(mapping, field)
    value = _decode_bounded_text(raw, maximum_bytes)
    if value is None or value == "":
        raise _error(f"{context} {field} must be non-empty text", mapping)
    return value


def _optional_text(
    mapping: dict[Any, Any], field: str, context: str, maximum_bytes: int
) -> str | None:
    raw = _map_get(mapping, field)
    if raw is None:
        return None
    value = _decode_bounded_text(raw, maximum_bytes)
    if value is None:
        raise _error(f"{context} {field} must be bounded text", mapping)
    return value


def _choice(mapping: dict[Any, Any], field: str, context: str, choices: frozenset[str]) -> str:
    value = _required_text(mapping, field, context, 64)
    if value not in choices:
        raise _error(f"{context} {field} has unsupported value {value!r}", mapping)
    return value


def _decode_bounded_text(value: Any, maximum_bytes: int) -> str | None:
    if not isinstance(value, (str, bytes)) or len(value) > maximum_bytes:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return None
    try:
        encoded = value.encode()
    except UnicodeEncodeError:
        return None
    return value if len(encoded) <= maximum_bytes else None


def _required_bool(mapping: dict[Any, Any], field: str, context: str) -> bool:
    value = _map_get(mapping, field)
    if type(value) is not bool:
        raise _error(f"{context} {field} must be boolean", mapping)
    return value


def _unsigned(value: Any, context: str) -> int:
    if type(value) is not int or value < 0 or value > 2**64 - 1:
        raise _error(f"{context} must be an unsigned 64-bit integer", value)
    return value


def _positive_unsigned(value: Any, context: str) -> int:
    parsed = _unsigned(value, context)
    if parsed == 0:
        raise _error(f"{context} must be positive", value)
    return parsed


def _optional_unsigned(value: Any, context: str) -> int | None:
    return None if value is None else _unsigned(value, context)


def _error(message: str, raw: Any) -> FerricStoreError:
    return FerricStoreError(f"invalid server response: {message}", raw=raw)


__all__ = ["FLOW_QUERY_INDEXES_CONTRACT", "decode_flow_query_index_status"]
