from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import replace
from typing import Any

import pytest

import ferricstore.flow_query_builder as query_builder
from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_api import resolve_flow_query_input
from ferricstore.flow_query_dsl import FlowQuery
from ferricstore.flow_query_index_contract import (
    _field_kind,
    validate_flow_query_index_contract,
)
from ferricstore.flow_query_request import (
    _with_flow_query_command_options,
    normalize_flow_query_parameter,
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


class _DuplicateMapping(Mapping[str, Any]):
    """A legal Mapping view whose iterator repeats a key."""

    def __init__(self, value: Any) -> None:
        self.value = value

    def __getitem__(self, key: str) -> Any:
        if key != "same":
            raise KeyError(key)
        return self.value

    def __iter__(self) -> Iterator[str]:
        return iter(("same", "same"))

    def __len__(self) -> int:
        return 2


class _QueryWithoutRoutingHint(FlowQuery):
    def compile(self) -> tuple[str, dict[str, Any]]:
        return "FROM runs WHERE partition_key = @partition RETURN COUNT", {"partition": 1}

    def _routing_hint(self) -> None:
        return None


def _status() -> FlowQueryIndexStatus:
    build = FlowQueryIndexBuild(
        scope="catalog_build",
        phase_counts={"done": 2},
        current_phases=("done",),
        completed_shards=2,
        total_shards=2,
        scanned_records=10,
        written_entries=10,
        written_bytes=900,
        raw={},
    )
    validation = FlowQueryIndexValidation(
        scope="catalog_build",
        status="passed",
        phase_counts={"done": 2},
        current_phases=("done",),
        completed_shards=2,
        total_shards=2,
        checked_records=10,
        checked_entries=10,
        mismatches=0,
        failure_reason=None,
        validated_at_ms=999_000,
        raw={},
    )
    retirement = FlowQueryIndexRetirement(
        status="not_applicable",
        phase_counts=None,
        current_phases=None,
        completed_shards=None,
        total_shards=None,
        deleted_entries=None,
        deleted_bytes=None,
        rewritten_reverse_rows=None,
        raw={"status": "not_applicable"},
    )
    statistics = FlowQueryIndexStatistics(
        status="fresh",
        samples=2,
        fresh_samples=2,
        stale_samples=0,
        future_samples=0,
        oldest_collected_at_ms=998_000,
        newest_collected_at_ms=999_000,
        oldest_age_ms=2_000,
        newest_age_ms=1_000,
        raw={},
    )
    index = FlowQueryIndex(
        id="flow_runs_tenant_updated",
        version=1,
        build_id="build-1",
        source="runs",
        state="active",
        queryable=True,
        fields=(
            FlowQueryIndexField("partition_key", "asc", "hashed", {}),
            FlowQueryIndexField("updated_at_ms", "desc", "ordered", {}),
        ),
        workloads=("tenant_updated",),
        count_prefixes=(1,),
        covering_fields=("partition_key", "run_id", "updated_at_ms", "version"),
        format=FlowQueryIndexFormat(
            query_row="ferric.flow.query.row/v1",
            key="ferric.flow.query.composite.key/v1",
            entry="ferric.flow.query.composite.entry/v2",
            reverse="ferric.flow.query.composite.reverse/v1",
            counter="ferric.flow.query.composite.counter/v1",
            raw={},
        ),
        coverage=FlowQueryIndexCoverage(2, 2, "passed", {}),
        build=build,
        validation=validation,
        retirement=retirement,
        statistics=statistics,
        raw={},
    )
    return FlowQueryIndexStatus(
        contract_version="ferric.flow.query.indexes/v1",
        observed_at_ms=1_000_000,
        statistics_max_age_ms=300_000,
        registry=FlowQueryIndexRegistry(2, 3),
        services=FlowQueryIndexServices("ready", "ready", "ready", "ready", {}),
        indexes=(index,),
        raw={},
    )


def _with_index(status: FlowQueryIndexStatus, **changes: Any) -> FlowQueryIndexStatus:
    return replace(status, indexes=(replace(status.indexes[0], **changes),))


def test_query_input_handles_compiled_queries_without_hints_and_rejects_other_types() -> None:
    assert resolve_flow_query_input(_QueryWithoutRoutingHint("runs"), None) == (
        "FROM runs WHERE partition_key = @partition RETURN COUNT",
        {"partition": 1},
        None,
    )

    with pytest.raises(TypeError, match="text or a FlowQuery"):
        resolve_flow_query_input(1, None)  # type: ignore[arg-type]


def test_builder_rejects_multibyte_field_values_and_duplicate_mapping_entries() -> None:
    builder = query_builder._FlowCollectionQuery("tenant-a", 10, False)
    with pytest.raises(ValueError, match="1024 bytes"):
        builder.equality("state", "state", "é" * 513)

    with pytest.raises(ValueError, match="attribute key is duplicated"):
        builder.metadata("attribute", _DuplicateMapping(1))

    with pytest.raises(ValueError, match="state is duplicated"):
        builder.state_metadata(_DuplicateMapping({"risk": 1}))


def test_request_options_and_multibyte_parameters_cover_defensive_boundaries() -> None:
    args = _with_flow_query_command_options(
        ("FQL1", "RETURN COUNT"),
        deadline_ms=1,
        routing_key=None,
    )
    with pytest.raises(ValueError, match="already present"):
        _with_flow_query_command_options(args, deadline_ms=None, routing_key=None)

    with pytest.raises(ValueError, match="routing key"):
        _with_flow_query_command_options((), deadline_ms=None, routing_key="")

    with pytest.raises(ValueError, match="65535 bytes"):
        normalize_flow_query_parameter("é" * 32_768, name="value")


def test_index_services_mapping_preserves_canonical_and_extension_compatibility() -> None:
    services = FlowQueryIndexServices(
        "ready",
        "ready",
        "ready",
        "unavailable",
        {
            b"byte_extension": 1,
            "text_extension": 2,
            b"duplicate": 3,
            "duplicate": 4,
            b"\xff": "ignored",
            5: "ignored",
        },
    )

    assert services["registry"] == "ready"
    assert services["byte_extension"] == 1
    assert services["text_extension"] == 2
    assert services["duplicate"] == 4
    with pytest.raises(KeyError):
        services["missing"]

    assert list(services) == [
        "registry",
        "lifecycle_worker",
        "statistics_store",
        "statistics_worker",
        "byte_extension",
        "text_extension",
        "duplicate",
    ]
    assert len(services) == 7


def test_index_contract_cross_checks_statistics_service_and_retirement_shape() -> None:
    status = _status()
    empty_statistics = replace(
        status.indexes[0].statistics,
        status="unavailable",
        samples=0,
        fresh_samples=0,
        stale_samples=0,
        future_samples=0,
        oldest_collected_at_ms=None,
        newest_collected_at_ms=None,
        oldest_age_ms=None,
        newest_age_ms=None,
    )
    unavailable = _with_index(status, statistics=empty_statistics)
    with pytest.raises(FerricStoreError, match="service is ready"):
        validate_flow_query_index_contract(unavailable)

    unavailable = replace(
        unavailable,
        services=replace(unavailable.services, statistics_store="unavailable"),
    )
    validate_flow_query_index_contract(unavailable)

    retirement = replace(status.indexes[0].retirement, raw={b"total_shards": None})
    with pytest.raises(FerricStoreError, match="must not contain progress"):
        validate_flow_query_index_contract(_with_index(status, retirement=retirement))


def test_index_contract_rejects_inconsistent_totals_and_incomplete_progress() -> None:
    status = _status()
    validation = replace(
        status.indexes[0].validation,
        phase_counts={"done": 3},
        completed_shards=3,
        total_shards=3,
    )
    with pytest.raises(FerricStoreError, match="shard totals"):
        validate_flow_query_index_contract(_with_index(status, validation=validation))

    build = replace(status.indexes[0].build, phase_counts=None)
    with pytest.raises(FerricStoreError, match="progress is incomplete"):
        validate_flow_query_index_contract(_with_index(status, build=build))


def test_index_contract_accepts_validating_and_pending_lifecycle_snapshots() -> None:
    status = _status()
    validate_flow_query_index_contract(_with_index(status, state="validating", queryable=False))

    build = replace(
        status.indexes[0].build,
        phase_counts={"snapshot": 2},
        current_phases=("snapshot",),
        completed_shards=0,
    )
    validation = replace(
        status.indexes[0].validation,
        status="pending",
        phase_counts={"pending": 2},
        current_phases=("pending",),
        completed_shards=0,
        mismatches=0,
        failure_reason=None,
        validated_at_ms=None,
    )
    pending = _with_index(
        status,
        state="building",
        queryable=False,
        coverage=FlowQueryIndexCoverage(0, 2, "pending", {}),
        build=build,
        validation=validation,
    )
    validate_flow_query_index_contract(pending)


def test_index_contract_accepts_failed_validation_and_retirement_progress() -> None:
    status = _status()
    validation = replace(
        status.indexes[0].validation,
        status="failed",
        mismatches=1,
        failure_reason="counter mismatch",
    )
    retirement = FlowQueryIndexRetirement(
        status="pending",
        phase_counts={"fence": 2},
        current_phases=("fence",),
        completed_shards=0,
        total_shards=2,
        deleted_entries=0,
        deleted_bytes=0,
        rewritten_reverse_rows=0,
        raw={},
    )
    failed = _with_index(
        status,
        state="retiring",
        queryable=False,
        coverage=FlowQueryIndexCoverage(2, 2, "failed", {}),
        validation=validation,
        retirement=retirement,
    )
    validate_flow_query_index_contract(failed)


def test_index_contract_covers_timestamp_order_and_sample_statuses() -> None:
    status = _status()
    out_of_order = replace(
        status.indexes[0].statistics,
        oldest_collected_at_ms=999_000,
        newest_collected_at_ms=998_000,
        oldest_age_ms=1_000,
        newest_age_ms=2_000,
    )
    with pytest.raises(FerricStoreError, match="timestamps are out of order"):
        validate_flow_query_index_contract(_with_index(status, statistics=out_of_order))

    mixed = replace(
        status.indexes[0].statistics,
        status="mixed",
        fresh_samples=1,
        stale_samples=1,
    )
    validate_flow_query_index_contract(_with_index(status, statistics=mixed))

    stale = replace(
        status.indexes[0].statistics,
        status="stale",
        fresh_samples=0,
        stale_samples=2,
    )
    validate_flow_query_index_contract(_with_index(status, statistics=stale))


def test_index_contract_recognizes_state_metadata_and_rejects_invalid_quoted_names() -> None:
    status = _status()
    fields = (
        status.indexes[0].fields[0],
        FlowQueryIndexField("state_meta.running.risk", "asc", "hashed", {}),
    )
    validate_flow_query_index_contract(
        _with_index(
            status,
            fields=fields,
            covering_fields=("partition_key", "run_id", "state_meta.running.risk", "version"),
        )
    )

    assert _field_kind("attribute['']") is None
    assert _field_kind("attribute['\ud800']") is None


def test_index_contract_validates_covering_identity_and_counter_format() -> None:
    status = _status()

    with pytest.raises(FerricStoreError, match="covering fields omit"):
        validate_flow_query_index_contract(
            _with_index(status, covering_fields=("partition_key", "updated_at_ms"))
        )

    with pytest.raises(FerricStoreError, match="unsupported covering field"):
        validate_flow_query_index_contract(
            _with_index(status, covering_fields=("partition_key", "run_id", "version", "payload"))
        )

    without_counter = replace(status.indexes[0].format, counter=None)
    with pytest.raises(FerricStoreError, match="counter format"):
        validate_flow_query_index_contract(_with_index(status, format=without_counter))
