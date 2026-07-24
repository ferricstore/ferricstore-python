from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, NoReturn

from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_types import (
    FlowQueryIndex,
    FlowQueryIndexStatus,
)

BUILD_PHASES = ("pending", "snapshot", "backfill", "done")
VALIDATION_PHASES = ("pending", "source", "index", "counter", "cleanup", "done")
RETIREMENT_PHASES = (
    "pending",
    "fence",
    "index",
    "counter",
    "reverse",
    "cleanup",
    "done",
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9_.:-]+\Z")
_UNQUOTED_METADATA = re.compile(r"[A-Za-z0-9_-]+\Z")
_ATTRIBUTE_BRACKET = re.compile(r"attribute\['((?:[^']|'')*)'\]\Z")
_STATE_META_BRACKET = re.compile(r"state_meta\['((?:[^']|'')*)'\]\['((?:[^']|'')*)'\]\Z")
_INTEGER_FIELDS = frozenset(
    {
        "version",
        "priority",
        "created_at_ms",
        "updated_at_ms",
        "next_run_at_ms",
        "lease_deadline_ms",
        "attempts",
        "max_active_ms",
    }
)
_BUILTIN_FIELDS = _INTEGER_FIELDS | frozenset(
    {
        "partition_key",
        "run_id",
        "event_id",
        "type",
        "state",
        "run_state",
        "parent_flow_id",
        "root_flow_id",
        "correlation_id",
    }
)
_RETIREMENT_PROGRESS_FIELDS = frozenset(
    {
        "phase_counts",
        "current_phases",
        "completed_shards",
        "total_shards",
        "deleted_entries",
        "deleted_bytes",
        "rewritten_reverse_rows",
    }
)


def validate_flow_query_index_contract(
    status: FlowQueryIndexStatus,
    *,
    expected_id: str | None = None,
) -> None:
    identities = tuple((index.id, index.version) for index in status.indexes)
    if identities != tuple(sorted(set(identities))):
        _fail("indexes must be uniquely sorted by id and version", status.raw)
    if expected_id is not None and (
        not status.indexes or any(index.id != expected_id for index in status.indexes)
    ):
        _fail("filtered indexes do not match the requested id", status.raw)

    for index in status.indexes:
        _validate_index(index, status)

    statuses = {index.statistics.status for index in status.indexes}
    if status.services.statistics_store == "unavailable":
        if statuses - {"unavailable"}:
            _fail("statistics must be unavailable when the service is unavailable", status.raw)
    elif "unavailable" in statuses:
        _fail("statistics cannot be unavailable while the service is ready", status.raw)


def _validate_index(index: FlowQueryIndex, status: FlowQueryIndexStatus) -> None:
    if _IDENTIFIER.fullmatch(index.id) is None:
        _fail("index id contains invalid characters", index.raw)
    if any(_IDENTIFIER.fullmatch(workload) is None for workload in index.workloads):
        _fail("index workload contains invalid characters", index.raw)
    first = index.fields[0]
    if (first.name, first.direction, first.encoding) != ("partition_key", "asc", "hashed"):
        _fail("index must begin with partition_key asc hashed", index.raw)
    if any(field.encoding == "hashed" and field.direction != "asc" for field in index.fields):
        _fail("hashed index fields must be ascending", index.raw)
    field_kinds = tuple(_field_kind(field.name) for field in index.fields)
    if any(kind is None for kind in field_kinds):
        _fail("index contains an unsupported field selector", index.raw)
    if any(
        field.encoding == "ordered" and kind != "integer"
        for field, kind in zip(index.fields, field_kinds, strict=True)
    ):
        _fail("ordered index fields must be integers", index.raw)
    if field_kinds.count("attribute") > 1:
        _fail("index may contain at most one attribute field", index.raw)
    if any(
        any(field.encoding != "hashed" for field in index.fields[:prefix])
        for prefix in index.count_prefixes
    ):
        _fail("count prefixes may cover only hashed fields", index.raw)

    _validate_progress(index.build, BUILD_PHASES, "build")
    _validate_progress(index.validation, VALIDATION_PHASES, "validation")
    if index.retirement.status == "not_applicable":
        if any(_has_key(index.retirement.raw, field) for field in _RETIREMENT_PROGRESS_FIELDS):
            _fail("not_applicable retirement must not contain progress", index.retirement.raw)
    else:
        _validate_progress(index.retirement, RETIREMENT_PHASES, "retirement")

    totals = {
        index.coverage.total_shards,
        index.build.total_shards,
        index.validation.total_shards,
    }
    if index.retirement.total_shards is not None:
        totals.add(index.retirement.total_shards)
    if len(totals) != 1:
        _fail("index shard totals are inconsistent", index.raw)
    if index.coverage.complete_shards != index.build.completed_shards:
        _fail("coverage and build completion are inconsistent", index.raw)
    if index.coverage.validation != index.validation.status:
        _fail("coverage and validation status are inconsistent", index.raw)

    queryable = (
        index.state == "active"
        and index.coverage.complete_shards == index.coverage.total_shards
        and index.coverage.validation == "passed"
    )
    if index.queryable != queryable:
        _fail("index queryable flag is inconsistent", index.raw)

    _validate_lifecycle(index)
    _validate_validation(index)
    _validate_statistics(index, status)


def _validate_lifecycle(index: FlowQueryIndex) -> None:
    build_complete = index.build.completed_shards == index.build.total_shards
    validation_status = index.validation.status
    retirement_status = index.retirement.status

    if index.state == "building":
        valid = (
            not build_complete
            and validation_status == "pending"
            and retirement_status == "not_applicable"
        )
    elif index.state == "validating":
        # Validation can become passed immediately before the lifecycle worker
        # publishes the activation transition.
        valid = (
            build_complete
            and validation_status in {"pending", "passed"}
            and retirement_status == "not_applicable"
        )
    elif index.state == "active":
        valid = (
            build_complete
            and validation_status == "passed"
            and retirement_status == "not_applicable"
        )
    elif index.state == "retiring":
        valid = (
            build_complete
            and validation_status in {"passed", "failed"}
            and retirement_status in {"pending", "complete"}
        )
    else:
        valid = validation_status in {"passed", "failed"} and retirement_status in {
            "pending",
            "complete",
        }
    if not valid:
        _fail("index lifecycle fields are inconsistent", index.raw)


def _validate_progress(progress: Any, phases: Sequence[str], section: str) -> None:
    counts = progress.phase_counts
    current = progress.current_phases
    completed = progress.completed_shards
    total = progress.total_shards
    if counts is None or current is None or completed is None or total is None:
        _fail(f"{section} progress is incomplete", progress.raw)
    if not counts or any(phase not in phases or count <= 0 for phase, count in counts.items()):
        _fail(f"{section} phase_counts are invalid", progress.raw)
    if sum(counts.values()) != total:
        _fail(f"{section} phase_counts do not match total_shards", progress.raw)
    expected_current = tuple(phase for phase in phases if phase in counts)
    if current != expected_current:
        _fail(f"{section} current_phases are inconsistent", progress.raw)
    if completed != counts.get("done", 0):
        _fail(f"{section} completed_shards is inconsistent", progress.raw)


def _validate_validation(index: FlowQueryIndex) -> None:
    validation = index.validation
    if validation.status == "pending":
        valid = (
            validation.mismatches == 0
            and validation.failure_reason is None
            and validation.validated_at_ms is None
        )
    elif validation.status == "passed":
        valid = (
            validation.mismatches == 0
            and validation.failure_reason is None
            and validation.validated_at_ms is not None
        )
    else:
        valid = (
            validation.mismatches > 0
            and bool(validation.failure_reason)
            and validation.validated_at_ms is not None
        )
    if not valid:
        _fail("validation status fields are inconsistent", validation.raw)


def _validate_statistics(index: FlowQueryIndex, status: FlowQueryIndexStatus) -> None:
    statistics = index.statistics
    times = (
        statistics.oldest_collected_at_ms,
        statistics.newest_collected_at_ms,
        statistics.oldest_age_ms,
        statistics.newest_age_ms,
    )
    if statistics.samples == 0:
        if statistics.status not in {"missing", "unavailable"} or any(
            value is not None for value in times
        ):
            _fail("empty statistics fields are inconsistent", statistics.raw)
        return
    if any(value is None for value in times):
        _fail("sampled statistics require timestamps and ages", statistics.raw)

    oldest, newest, oldest_age, newest_age = times
    if oldest is None or newest is None or oldest_age is None or newest_age is None:
        _fail("sampled statistics require timestamps and ages", statistics.raw)
    if oldest > newest:
        _fail("statistics timestamps are out of order", statistics.raw)
    if oldest_age != max(status.observed_at_ms - oldest, 0) or newest_age != max(
        status.observed_at_ms - newest, 0
    ):
        _fail("statistics ages do not match collection timestamps", statistics.raw)

    if statistics.fresh_samples == statistics.samples:
        expected = {"fresh"}
    elif statistics.fresh_samples == 0:
        expected = {"stale"}
        if statistics.future_samples > 0:
            expected.add("future")
    else:
        expected = {"mixed"}
    if statistics.status not in expected:
        _fail("statistics status does not match sample counters", statistics.raw)


def _has_key(mapping: dict[Any, Any], key: str) -> bool:
    return key in mapping or key.encode() in mapping


def _field_kind(name: str) -> str | None:
    if name in _BUILTIN_FIELDS:
        return "integer" if name in _INTEGER_FIELDS else "keyword"

    parts = name.split(".")
    if len(parts) == 2 and parts[0] == "attribute" and _valid_unquoted(parts[1]):
        return "attribute"
    if (
        len(parts) == 3
        and parts[0] == "state_meta"
        and _valid_unquoted(parts[1])
        and _valid_unquoted(parts[2])
    ):
        return "state_meta"

    attribute = _ATTRIBUTE_BRACKET.fullmatch(name)
    if attribute is not None:
        metadata = attribute.group(1).replace("''", "'")
        if _valid_metadata(metadata, reject_reserved=True) and name == _external_selector(
            "attribute", metadata
        ):
            return "attribute"

    state_meta = _STATE_META_BRACKET.fullmatch(name)
    if state_meta is not None:
        state = state_meta.group(1).replace("''", "'")
        metadata = state_meta.group(2).replace("''", "'")
        if (
            _valid_metadata(state, reject_reserved=False)
            and _valid_metadata(metadata, reject_reserved=True)
            and name == _external_selector("state_meta", state, metadata)
        ):
            return "state_meta"
    return None


def _valid_unquoted(value: str) -> bool:
    return (
        not value.startswith("__")
        and _UNQUOTED_METADATA.fullmatch(value) is not None
        and len(value.encode("ascii")) <= 64
    )


def _valid_metadata(value: str, *, reject_reserved: bool) -> bool:
    if not value or len(value) > 64 or (reject_reserved and value.startswith("__")):
        return False
    try:
        return len(value.encode("utf-8")) <= 64
    except UnicodeEncodeError:
        return False


def _bracket_selector(root: str, *segments: str) -> str:
    return root + "".join("['" + segment.replace("'", "''") + "']" for segment in segments)


def _external_selector(root: str, *segments: str) -> str:
    if all(_valid_unquoted(segment) for segment in segments):
        return ".".join((root, *segments))
    return _bracket_selector(root, *segments)


def _fail(message: str, raw: Any) -> NoReturn:
    raise FerricStoreError(f"invalid server response: FLOW.QUERY.INDEXES {message}", raw=raw)


__all__ = [
    "BUILD_PHASES",
    "RETIREMENT_PHASES",
    "VALIDATION_PHASES",
    "validate_flow_query_index_contract",
]
