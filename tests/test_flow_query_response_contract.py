from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_response import (
    decode_flow_explain_result,
    decode_flow_query_error,
    decode_flow_query_index_status,
    decode_flow_query_result,
)


class EncodeBomb(str):
    def encode(self, *_args: Any, **_kwargs: Any) -> bytes:
        raise AssertionError("oversized response text must be rejected before encoding")


def _usage(*, result_records: int = 1) -> dict[str, int]:
    return {
        "range_seeks": 1,
        "range_pages": 1,
        "scanned_entries": 1,
        "scanned_bytes": 100,
        "hydrated_records": 1,
        "residual_checks": 0,
        "duplicate_entries": 0,
        "result_records": result_records,
        "response_bytes": 256,
        "memory_high_water_bytes": 1_024,
        "wall_time_us": 10,
    }


def _quality() -> dict[str, str]:
    return {
        "exactness": "projected_exact",
        "freshness": "projection_watermark",
        "coverage": "complete",
        "pagination": "live_seek",
    }


def _records_response() -> dict[str, Any]:
    return {
        "version": "ferric.flow.query.result/v1",
        "records": [{b"id": b"run-1"}],
        "page": {"has_more": False, "cursor": None},
        "quality": _quality(),
        "usage": _usage(),
    }


def _count_response() -> dict[str, Any]:
    return {
        "version": "ferric.flow.query.result/v1",
        "result": {"kind": "count", "value": 7},
        "quality": _quality(),
        "usage": _usage(),
    }


def _diagnostic() -> dict[str, Any]:
    return {
        "code": "unsupported_field",
        "message": "unsupported query field",
        "detail": "Use a supported field.",
        "hint": "See context.supported_fields.",
        "retryable": False,
        "safe_to_retry": False,
        "retry_after_ms": 0,
        "position": {"byte": 18, "line": 1, "column": 19},
        "context": {"supported_fields": ["partition_key", "run_id"]},
    }


def _explain_response(status: str = "planned") -> dict[str, Any]:
    response: dict[str, Any] = {
        "version": "ferric.flow.explain/v1",
        "query_fingerprint": "a" * 64,
        "status": status,
        "plan": {"path": "ordered_range"},
        "estimate": {"scanned_entries": 1},
        "stats": {"source": "fresh"},
        "quality": _quality(),
        "bounds": {"scanned_entries": 50_000},
        "pressure": {"resources": []},
        "decision": {"reason": "only_bounded_candidate"},
        "alternatives": [],
        "actual": None,
        "diagnostic": None,
    }
    if status == "executed":
        response["actual"] = _usage()
    elif status == "rejected":
        response["diagnostic"] = _diagnostic()
    return response


def _index_response() -> dict[str, Any]:
    return {
        "contract_version": "ferric.flow.query.indexes/v1",
        "observed_at_ms": 1_000_000,
        "statistics_max_age_ms": 300_000,
        "registry": {"epoch": 2, "catalog_version": 3},
        "services": {
            "registry": "ready",
            "lifecycle_worker": "ready",
            "statistics_store": "ready",
            "statistics_worker": "unavailable",
        },
        "indexes": [
            {
                "id": "flow_runs_tenant_updated",
                "version": 1,
                "build_id": "build-1",
                "source": "runs",
                "state": "active",
                "queryable": True,
                "fields": [
                    {"name": "partition_key", "direction": "asc", "encoding": "hashed"},
                    {"name": "updated_at_ms", "direction": "desc", "encoding": "ordered"},
                ],
                "workloads": ["tenant_updated"],
                "count_prefixes": [1],
                "covering_fields": [
                    "partition_key",
                    "run_id",
                    "updated_at_ms",
                    "version",
                ],
                "format": {
                    "query_row": "ferric.flow.query.row/v1",
                    "key": "ferric.flow.query.composite.key/v1",
                    "entry": "ferric.flow.query.composite.entry/v2",
                    "reverse": "ferric.flow.query.composite.reverse/v1",
                    "counter": "ferric.flow.query.composite.counter/v1",
                },
                "coverage": {
                    "complete_shards": 2,
                    "total_shards": 2,
                    "validation": "passed",
                },
                "build": {
                    "scope": "catalog_build",
                    "phase_counts": {"done": 2},
                    "current_phases": ["done"],
                    "completed_shards": 2,
                    "total_shards": 2,
                    "scanned_records": 10,
                    "written_entries": 10,
                    "written_bytes": 900,
                },
                "validation": {
                    "scope": "catalog_build",
                    "status": "passed",
                    "phase_counts": {"done": 2},
                    "current_phases": ["done"],
                    "completed_shards": 2,
                    "total_shards": 2,
                    "checked_records": 10,
                    "checked_entries": 10,
                    "mismatches": 0,
                    "failure_reason": None,
                    "validated_at_ms": 999_000,
                },
                "retirement": {"status": "not_applicable"},
                "statistics": {
                    "status": "fresh",
                    "samples": 2,
                    "fresh_samples": 2,
                    "stale_samples": 0,
                    "future_samples": 0,
                    "oldest_collected_at_ms": 998_000,
                    "newest_collected_at_ms": 999_000,
                    "oldest_age_ms": 2_000,
                    "newest_age_ms": 1_000,
                },
            }
        ],
    }


def _bytes(value: Any) -> Any:
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, dict):
        return {_bytes(key): _bytes(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_bytes(item) for item in value]
    return value


def _set_path(mapping: dict[str, Any], path: tuple[Any, ...], value: Any) -> None:
    target: Any = mapping
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value


def test_result_decoder_accepts_byte_keys_and_values() -> None:
    response = _bytes(_records_response())

    result = decode_flow_query_result(response)

    assert result.version == "ferric.flow.query.result/v1"
    assert result.records == ({b"id": b"run-1"},)
    assert result.page is not None and result.page.has_more is False
    assert result.quality.exactness == "projected_exact"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda response: response.pop("records"),
        lambda response: response.__setitem__("result", {"kind": "count", "value": 1}),
        lambda response: response.__setitem__("records", "not-an-array"),
        lambda response: response.__setitem__("records", [{}] * 101),
        lambda response: response.__setitem__("records", ["not-a-map"]),
        lambda response: response["usage"].__setitem__("result_records", 0),
        lambda response: response.__setitem__("page", None),
        lambda response: response["page"].__setitem__("has_more", 1),
        lambda response: response["page"].__setitem__("cursor", 123),
        lambda response: response["page"].update(has_more=True, cursor=None),
        lambda response: response["page"].update(has_more=False, cursor="fqc1_opaque-token"),
        lambda response: response["page"].update(has_more=True, cursor="fqc1_short"),
        lambda response: response["page"].update(has_more=True, cursor="other_token_value"),
        lambda response: response["page"].update(has_more=True, cursor="fqc1_" + "x" * 4092),
    ],
)
def test_records_result_rejects_malformed_shapes(mutate: Any) -> None:
    response = _records_response()
    mutate(response)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_result(response)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("version",), "future/v2"),
        (("quality",), []),
        (("quality", "coverage"), ""),
        (("quality", "pagination"), "x" * 65),
        (("usage",), []),
        (("usage", "range_seeks"), True),
        (("usage", "wall_time_us"), -1),
        (("usage", "response_bytes"), 2**63),
    ],
)
def test_result_rejects_invalid_envelope_fields(path: tuple[Any, ...], value: Any) -> None:
    response = _records_response()
    _set_path(response, path, value)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_result(response)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda usage: usage.update(hydrated_records=2),
        lambda usage: usage.update(duplicate_entries=2),
        lambda usage: usage.update(range_pages=3),
        lambda usage: usage.update(residual_checks=13),
    ],
)
def test_records_result_rejects_internally_inconsistent_usage(mutate: Any) -> None:
    response = _records_response()
    mutate(response["usage"])

    with pytest.raises(FerricStoreError, match=r"usage.*inconsistent"):
        decode_flow_query_result(response)


def test_records_result_accepts_query_row_without_log_hydration() -> None:
    response = _records_response()
    response["usage"].update(hydrated_records=0, residual_checks=1)

    result = decode_flow_query_result(response)

    assert result.usage.result_records == 1
    assert result.usage.hydrated_records == 0
    assert result.usage.residual_checks == 1


@pytest.mark.parametrize(
    "mutate",
    [
        lambda response: response.__setitem__("page", {"has_more": False, "cursor": None}),
        lambda response: response.__setitem__("result", []),
        lambda response: response["result"].__setitem__("kind", "records"),
        lambda response: response["result"].__setitem__("value", True),
        lambda response: response["result"].__setitem__("value", -1),
        lambda response: response["result"].__setitem__("value", 2**63),
        lambda response: response["usage"].__setitem__("result_records", 0),
    ],
)
def test_count_result_rejects_malformed_shapes(mutate: Any) -> None:
    response = _count_response()
    mutate(response)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_result(response)


def test_count_result_decodes_signed_maximum() -> None:
    response = _count_response()
    response["result"]["value"] = 2**63 - 1

    assert decode_flow_query_result(response).count == 2**63 - 1


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("version",), "future/v2"),
        (("query_fingerprint",), "a" * 63),
        (("query_fingerprint",), "g" * 64),
        (("status",), "unknown"),
        (("plan",), []),
        (("estimate",), None),
        (("stats",), []),
        (("quality",), []),
        (("bounds",), "bad"),
        (("pressure",), []),
        (("decision",), []),
        (("alternatives",), {}),
        (("alternatives",), [1]),
        (("alternatives",), [{}] * 32),
    ],
)
def test_explain_rejects_invalid_contract_fields(path: tuple[Any, ...], value: Any) -> None:
    response = _explain_response()
    _set_path(response, path, value)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_explain_result(response)


@pytest.mark.parametrize(
    "response",
    [
        {**_explain_response("executed"), "actual": None},
        {**_explain_response("executed"), "actual": {}},
        {**_explain_response("planned"), "actual": _usage()},
        {**_explain_response("rejected"), "diagnostic": None},
        {**_explain_response("rejected"), "diagnostic": {"code": "bad"}},
        {**_explain_response("planned"), "diagnostic": _diagnostic()},
    ],
)
def test_explain_status_controls_actual_and_diagnostic(response: dict[str, Any]) -> None:
    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_explain_result(response)


@pytest.mark.parametrize(
    "field",
    ["stats", "quality", "pressure", "decision", "alternatives"],
)
def test_extended_explain_requires_every_v1_top_level_shape(field: str) -> None:
    response = _explain_response()
    response.pop(field)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_explain_result(response)


@pytest.mark.parametrize("field", ["actual", "diagnostic"])
def test_extended_explain_requires_nullable_status_shape_keys(field: str) -> None:
    response = _explain_response()
    response.pop(field)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_explain_result(response)


def test_specialized_explain_requires_and_preserves_capabilities() -> None:
    response = {
        "version": "ferric.flow.explain/v1",
        "query_fingerprint": "a" * 64,
        "status": "planned",
        "capabilities": {
            "requested": [],
            "available": ["flow_query_point_v1"],
            "missing": [],
        },
        "plan": {"path": "primary_key", "extension": {"future": True}},
        "estimate": {"scan_records": 1},
        "bounds": {"scan_records": 1},
    }

    result = decode_flow_explain_result(response)

    assert result.capabilities is response["capabilities"]
    assert result.quality is None
    assert result.stats is None
    assert result.alternatives == ()


@pytest.mark.parametrize(
    "capabilities",
    [
        None,
        [],
        {"requested": [], "available": [], "missing": "nope"},
        {"requested": [], "available": [1], "missing": []},
        {"requested": [], "available": ["x"] * 65, "missing": []},
    ],
)
def test_specialized_explain_rejects_missing_or_malformed_capabilities(
    capabilities: Any,
) -> None:
    response = {
        "version": "ferric.flow.explain/v1",
        "query_fingerprint": "a" * 64,
        "status": "planned",
        "capabilities": capabilities,
        "plan": {"path": "primary_key"},
        "estimate": {},
        "bounds": {},
    }

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_explain_result(response)


def test_explain_decodes_executed_and_rejected_byte_payloads() -> None:
    executed = decode_flow_explain_result(_bytes(_explain_response("executed")))
    rejected = decode_flow_explain_result(_bytes(_explain_response("rejected")))

    assert executed.actual is not None and executed.actual.result_records == 1
    assert executed.quality is not None and executed.quality.coverage == "complete"
    assert executed.alternatives == ()
    assert rejected.diagnostic is not None
    assert rejected.diagnostic.code == "unsupported_field"
    assert rejected.diagnostic.position is not None
    assert rejected.diagnostic.position.byte == 18


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("code",), ""),
        (("message",), 1),
        (("detail",), 1),
        (("detail",), "x" * 1_025),
        (("hint",), b"\xff"),
        (("retryable",), 0),
        (("safe_to_retry",), "false"),
        (("retry_after_ms",), -1),
        (("context",), []),
        (("context",), {str(index): index for index in range(17)}),
        (("context",), {"": "bad"}),
        (("context",), {"key": [0] * 33}),
        (("context",), {"key": 2**63}),
        (("context",), {"key": 1.5}),
        (("context",), {"key": {"a": {"b": {"c": {"d": {"e": {"f": 1}}}}}}}),
        (("position",), []),
        (("position", "byte"), 0),
        (("position", "line"), True),
        (("position", "column"), 2**63),
    ],
)
def test_diagnostic_rejects_malformed_bounded_fields(path: tuple[Any, ...], value: Any) -> None:
    response = _diagnostic()
    _set_path(response, path, value)

    assert decode_flow_query_error(response, raw=response) is None


def test_diagnostic_rejects_non_map_and_accepts_absent_optional_fields() -> None:
    assert decode_flow_query_error([], raw=[]) is None

    response = _diagnostic()
    response.pop("detail")
    response.pop("hint")
    response.pop("context")
    response.pop("position")
    error = decode_flow_query_error(_bytes(response), raw=response)

    assert error is not None
    assert error.detail is None
    assert error.hint is None
    assert error.context is None
    assert error.position is None


def test_diagnostic_accepts_every_bounded_context_scalar_and_container() -> None:
    response = _diagnostic()
    response["context"] = {
        "none": None,
        "bool": True,
        "minimum": -(2**63),
        "text": b"value",
        "list": [1, {"nested": "ok"}],
    }

    error = decode_flow_query_error(response, raw=response)

    assert error is not None and error.context == response["context"]


def test_index_status_decodes_the_stable_v1_surface_and_preserves_raw() -> None:
    response = _index_response()
    response["services"]["future_service"] = {"status": "ready"}

    status = decode_flow_query_index_status(response)

    assert status.raw is response
    assert status.services.registry == "ready"
    assert status.services.statistics_worker == "unavailable"
    index = status.indexes[0]
    assert index.source == "runs"
    assert index.fields[1].name == "updated_at_ms"
    assert index.fields[1].direction == "desc"
    assert index.workloads == ("tenant_updated",)
    assert index.count_prefixes == (1,)
    assert index.coverage.complete_shards == 2
    assert index.build.written_bytes == 900
    assert index.validation.validated_at_ms == 999_000
    assert index.retirement.status == "not_applicable"
    assert index.statistics.fresh_samples == 2
    assert index.raw is response["indexes"][0]
    assert status.services["registry"] == "ready"
    assert dict(status.services)["statistics_worker"] == "unavailable"
    assert status.services["future_service"] == {"status": "ready"}
    assert "future_service" in status.services
    assert dict(status.services)["future_service"] == {"status": "ready"}


def test_index_status_accepts_byte_keys_and_text_values() -> None:
    status = decode_flow_query_index_status(_bytes(_index_response()))

    assert status.registry.epoch == 2
    assert status.services.lifecycle_worker == "ready"
    assert status.indexes[0].fields[0].encoding == "hashed"


def test_index_status_accepts_unsigned_64_bit_counters_and_timestamps() -> None:
    maximum = 2**64 - 1
    response = _index_response()
    response["observed_at_ms"] = maximum
    response["statistics_max_age_ms"] = maximum
    index = response["indexes"][0]
    index.update(state="retiring", queryable=False)
    index["coverage"].update(complete_shards=maximum, total_shards=maximum)
    index["build"].update(
        phase_counts={"done": maximum},
        completed_shards=maximum,
        total_shards=maximum,
        scanned_records=maximum,
        written_entries=maximum,
        written_bytes=maximum,
    )
    index["validation"].update(
        phase_counts={"done": maximum},
        completed_shards=maximum,
        total_shards=maximum,
        checked_records=maximum,
        checked_entries=maximum,
        validated_at_ms=maximum,
    )
    index["retirement"] = {
        "status": "complete",
        "phase_counts": {"done": maximum},
        "current_phases": ["done"],
        "completed_shards": maximum,
        "total_shards": maximum,
        "deleted_entries": maximum,
        "deleted_bytes": maximum,
        "rewritten_reverse_rows": maximum,
    }
    index["statistics"].update(
        samples=maximum,
        fresh_samples=maximum,
        stale_samples=0,
        future_samples=0,
        oldest_collected_at_ms=0,
        newest_collected_at_ms=0,
        oldest_age_ms=maximum,
        newest_age_ms=maximum,
    )

    status = decode_flow_query_index_status(response)

    assert status.observed_at_ms == maximum
    assert status.indexes[0].build.scanned_records == maximum
    assert status.indexes[0].validation.validated_at_ms == maximum
    assert status.indexes[0].retirement.deleted_entries == maximum
    assert status.indexes[0].statistics.samples == maximum


@pytest.mark.parametrize(
    "path",
    [
        ("observed_at_ms",),
        ("indexes", 0, "coverage", "total_shards"),
        ("indexes", 0, "build", "phase_counts", "done"),
        ("indexes", 0, "build", "scanned_records"),
        ("indexes", 0, "validation", "validated_at_ms"),
        ("indexes", 0, "statistics", "oldest_collected_at_ms"),
    ],
)
def test_index_status_rejects_counters_above_unsigned_64_bit(
    path: tuple[Any, ...],
) -> None:
    response = _index_response()
    _set_path(response, path, 2**64)

    with pytest.raises(FerricStoreError, match="unsigned 64-bit"):
        decode_flow_query_index_status(response)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("contract_version",), "future/v2"),
        (("observed_at_ms",), -1),
        (("statistics_max_age_ms",), True),
        (("registry",), []),
        (("registry", "epoch"), 2**64),
        (("registry", "catalog_version"), 0),
        (("services",), []),
        (("services", "registry"), 1),
        (("indexes",), {}),
        (("indexes",), [_index_response()["indexes"][0]] * 33),
        (("indexes", 0), []),
        (("indexes", 0, "id"), ""),
        (("indexes", 0, "version"), 0),
        (("indexes", 0, "build_id"), 1),
        (("indexes", 0, "source"), "events"),
        (("indexes", 0, "state"), "unknown"),
        (("indexes", 0, "queryable"), 1),
        (("indexes", 0, "fields"), []),
        (("indexes", 0, "fields", 0), []),
        (("indexes", 0, "fields", 0, "name"), ""),
        (("indexes", 0, "fields", 0, "direction"), "sideways"),
        (("indexes", 0, "fields", 0, "encoding"), "plain"),
        (("indexes", 0, "fields", 1, "name"), "partition_key"),
        (("indexes", 0, "workloads"), "bad"),
        (("indexes", 0, "workloads"), [1]),
        (("indexes", 0, "workloads"), ["same", "same"]),
        (("indexes", 0, "workloads"), ["x"] * 17),
        (("indexes", 0, "count_prefixes"), [0]),
        (("indexes", 0, "count_prefixes"), [1, 2, 3]),
        (("indexes", 0, "count_prefixes"), [2, 1]),
        (("indexes", 0, "coverage", "complete_shards"), 3),
        (("indexes", 0, "coverage", "total_shards"), 0),
        (("indexes", 0, "coverage", "validation"), "unknown"),
        (("indexes", 0, "build", "scope"), "tenant"),
        (("indexes", 0, "build", "phase_counts"), []),
        (("indexes", 0, "build", "phase_counts"), {"done": -1}),
        (("indexes", 0, "build", "phase_counts"), {str(index): 0 for index in range(17)}),
        (("indexes", 0, "build", "phase_counts"), {b"\xff": 1}),
        (("indexes", 0, "build", "current_phases"), "done"),
        (("indexes", 0, "build", "current_phases"), ["future"]),
        (("indexes", 0, "build", "completed_shards"), 3),
        (("indexes", 0, "validation", "status"), "unknown"),
        (("indexes", 0, "validation", "failure_reason"), 1),
        (("indexes", 0, "validation", "validated_at_ms"), -1),
        (("indexes", 0, "retirement", "status"), "unknown"),
        (("indexes", 0, "statistics", "status"), "unknown"),
        (("indexes", 0, "statistics", "samples"), -1),
        (("indexes", 0, "statistics", "fresh_samples"), 1),
        (("indexes", 0, "statistics", "oldest_collected_at_ms"), "never"),
    ],
)
def test_index_status_rejects_malformed_contract_fields(path: tuple[Any, ...], value: Any) -> None:
    response = _index_response()
    _set_path(response, path, value)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_index_status(response)


def test_index_status_decodes_retiring_index_and_missing_statistics() -> None:
    response = _index_response()
    index = response["indexes"][0]
    index.update(state="retiring", queryable=False)
    index["retirement"] = {
        "status": "pending",
        "phase_counts": {"fence": 2},
        "current_phases": ["fence"],
        "completed_shards": 0,
        "total_shards": 2,
        "deleted_entries": 0,
        "deleted_bytes": 0,
        "rewritten_reverse_rows": 0,
    }
    index["statistics"] = {
        "status": "missing",
        "samples": 0,
        "fresh_samples": 0,
        "stale_samples": 0,
        "future_samples": 0,
        "oldest_collected_at_ms": None,
        "newest_collected_at_ms": None,
        "oldest_age_ms": None,
        "newest_age_ms": None,
    }

    decoded = decode_flow_query_index_status(response).indexes[0]

    assert decoded.retirement.current_phases == ("fence",)
    assert decoded.retirement.deleted_entries == 0
    assert decoded.statistics.oldest_age_ms is None


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("indexes", 0, "build", "phase_counts"), {"future": 2}),
        (("indexes", 0, "build", "phase_counts"), {"done": 1}),
        (("indexes", 0, "build", "phase_counts"), {"pending": 0, "done": 2}),
        (("indexes", 0, "build", "current_phases"), []),
        (("indexes", 0, "build", "completed_shards"), 1),
        (("indexes", 0, "validation", "total_shards"), 3),
        (("indexes", 0, "coverage", "complete_shards"), 1),
        (("indexes", 0, "coverage", "validation"), "pending"),
        (("indexes", 0, "queryable"), False),
        (("indexes", 0, "validation", "mismatches"), 1),
        (("indexes", 0, "validation", "failure_reason"), "unexpected"),
        (("indexes", 0, "validation", "validated_at_ms"), None),
        (("indexes", 0, "id"), "not valid!"),
        (("indexes", 0, "workloads"), ["not valid!"]),
        (("indexes", 0, "fields", 0, "name"), "type"),
        (("indexes", 0, "fields", 1, "encoding"), "hashed"),
        (("indexes", 0, "fields", 1, "name"), "state"),
        (("indexes", 0, "fields", 1, "name"), "attribute['unterminated]"),
        (("indexes", 0, "count_prefixes"), [2]),
    ],
)
def test_index_status_rejects_impossible_v1_cross_field_shapes(
    path: tuple[Any, ...], value: Any
) -> None:
    response = _index_response()
    _set_path(response, path, value)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_index_status(response)


@pytest.mark.parametrize(
    "field",
    [
        "oldest_collected_at_ms",
        "newest_collected_at_ms",
        "oldest_age_ms",
        "newest_age_ms",
    ],
)
def test_index_statistics_requires_nullable_v1_fields(field: str) -> None:
    response = _index_response()
    response["indexes"][0]["statistics"].pop(field)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_index_status(response)


@pytest.mark.parametrize("field", ["failure_reason", "validated_at_ms"])
def test_index_validation_requires_nullable_v1_fields(field: str) -> None:
    response = _index_response()
    response["indexes"][0]["validation"].pop(field)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_index_status(response)


def test_index_statistics_rejects_inconsistent_status_timestamps_and_service() -> None:
    invalid: list[dict[str, Any]] = []

    missing_with_sample = _index_response()
    missing_with_sample["indexes"][0]["statistics"].update(
        status="missing",
        samples=0,
        fresh_samples=0,
        stale_samples=0,
        oldest_collected_at_ms=1,
    )
    invalid.append(missing_with_sample)

    absent_timestamp = _index_response()
    absent_timestamp["indexes"][0]["statistics"]["newest_collected_at_ms"] = None
    invalid.append(absent_timestamp)

    wrong_status = _index_response()
    wrong_status["indexes"][0]["statistics"]["status"] = "stale"
    invalid.append(wrong_status)

    wrong_age = _index_response()
    wrong_age["indexes"][0]["statistics"]["oldest_age_ms"] = 1_999
    invalid.append(wrong_age)

    unavailable_service = _index_response()
    unavailable_service["services"]["statistics_store"] = "unavailable"
    invalid.append(unavailable_service)

    for response in invalid:
        with pytest.raises(FerricStoreError, match="invalid server response"):
            decode_flow_query_index_status(response)


def test_index_statistics_accepts_documented_future_status() -> None:
    response = _index_response()
    observed_at_ms = response["observed_at_ms"]
    response["indexes"][0]["statistics"].update(
        status="future",
        samples=1,
        fresh_samples=0,
        stale_samples=1,
        future_samples=1,
        oldest_collected_at_ms=observed_at_ms + 1,
        newest_collected_at_ms=observed_at_ms + 1,
        oldest_age_ms=0,
        newest_age_ms=0,
    )

    assert decode_flow_query_index_status(response).indexes[0].statistics.status == "future"


def test_index_status_rejects_duplicate_or_out_of_order_identities() -> None:
    duplicate = _index_response()
    duplicate["indexes"].append(deepcopy(duplicate["indexes"][0]))

    out_of_order = _index_response()
    first = out_of_order["indexes"][0]
    first["id"] = "z-index"
    second = deepcopy(first)
    second["id"] = "a-index"
    out_of_order["indexes"].append(second)

    for response in (duplicate, out_of_order):
        with pytest.raises(FerricStoreError, match="invalid server response"):
            decode_flow_query_index_status(response)


def test_index_status_rejects_multiple_attribute_dimensions() -> None:
    response = _index_response()
    fields = response["indexes"][0]["fields"]
    fields[1].update(name="attribute.customer", direction="asc", encoding="hashed")
    fields.append({"name": "attribute.region", "direction": "asc", "encoding": "hashed"})

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_index_status(response)


@pytest.mark.parametrize(
    "selector",
    [
        "attribute." + "x" * 65,
        "state_meta." + "s" * 65 + ".risk",
        "state_meta.running." + "x" * 65,
        "attribute['region']",
        "state_meta['running']['risk']",
    ],
)
def test_index_status_rejects_noncanonical_or_oversized_unquoted_selectors(
    selector: str,
) -> None:
    response = _index_response()
    response["indexes"][0]["fields"][1].update(
        name=selector,
        direction="asc",
        encoding="hashed",
    )

    with pytest.raises(FerricStoreError, match="unsupported field selector"):
        decode_flow_query_index_status(response)


@pytest.mark.parametrize(
    "selector",
    [
        "attribute['customer.region']",
        "attribute['customer''s tier']",
        "state_meta['review.v2']['ai.model']",
        "state_meta['review''s']['risk tier']",
    ],
)
def test_index_status_accepts_canonical_quoted_metadata_selectors(selector: str) -> None:
    response = _index_response()
    response["indexes"][0]["fields"][1].update(
        name=selector,
        direction="asc",
        encoding="hashed",
    )
    response["indexes"][0]["covering_fields"][2] = selector

    assert decode_flow_query_index_status(response).indexes[0].fields[1].name == selector


@pytest.mark.parametrize("state", ["building", "retiring", "failed"])
def test_index_status_rejects_impossible_lifecycle_state_combinations(state: str) -> None:
    response = _index_response()
    response["indexes"][0].update(state=state, queryable=False)

    with pytest.raises(FerricStoreError, match="lifecycle"):
        decode_flow_query_index_status(response)


@pytest.mark.parametrize("case", ["required", "sequence", "map_key", "optional"])
def test_index_status_rejects_oversized_text_before_utf8_encoding(case: str) -> None:
    response = _index_response()
    if case == "required":
        response["indexes"][0]["id"] = EncodeBomb("x" * 65)
    elif case == "sequence":
        response["indexes"][0]["workloads"] = [EncodeBomb("x" * 65)]
    elif case == "map_key":
        response["indexes"][0]["build"]["phase_counts"] = {EncodeBomb("x" * 65): 2}
    else:
        response["indexes"][0]["validation"]["failure_reason"] = EncodeBomb("x" * 129)

    with pytest.raises(FerricStoreError, match="invalid server response"):
        decode_flow_query_index_status(response)
