from __future__ import annotations

import asyncio
import math
from copy import deepcopy
from typing import Any

import pytest

from ferricstore import (
    AsyncFlowClient,
    FerricStoreError,
    FlowClient,
    FlowExplainResult,
    FlowQueryError,
    FlowQueryIndexStatus,
    FlowQueryResult,
)
from ferricstore.flow_routing import flow_command_route_keys
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_constants import _OPCODES
from ferricstore.protocol_negotiation import parse_hello_capabilities
from ferricstore.protocol_retry import request_may_mutate

QUERY = (
    "FROM runs WHERE partition_key = @tenant AND type = @type "
    "ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS"
)

USAGE = {
    "range_seeks": 1,
    "range_pages": 1,
    "scanned_entries": 2,
    "scanned_bytes": 100,
    "hydrated_records": 2,
    "residual_checks": 0,
    "duplicate_entries": 0,
    "result_records": 2,
    "response_bytes": 100,
    "memory_high_water_bytes": 1024,
    "wall_time_us": 10,
}

QUALITY = {
    "exactness": "projected_exact",
    "freshness": "projection_watermark",
    "coverage": "complete",
    "pagination": "live_seek",
}


def query_response() -> dict[str, Any]:
    return {
        "version": "ferric.flow.query.result/v1",
        "records": [record("one"), record("two")],
        "page": {"has_more": True, "cursor": "fqc1_next"},
        "quality": deepcopy(QUALITY),
        "usage": deepcopy(USAGE),
    }


def count_response(count: int) -> dict[str, Any]:
    usage = deepcopy(USAGE)
    usage["result_records"] = 1
    return {
        "version": "ferric.flow.query.result/v1",
        "result": {"kind": "count", "value": count},
        "quality": deepcopy(QUALITY),
        "usage": usage,
    }


def explain_response(status: str, *, actual: dict[str, Any] | None = None) -> dict[str, Any]:
    response: dict[str, Any] = {
        "version": "ferric.flow.explain/v1",
        "query_fingerprint": "a" * 64,
        "status": status,
        "plan": {"path": "ordered_range"},
        "estimate": {"scanned_entries": 2},
        "bounds": {"scanned_entries": 50_000},
    }
    if actual is not None:
        response["actual"] = actual
    return response


def diagnostic() -> dict[str, Any]:
    return {
        "code": "unsupported_field",
        "message": "unsupported query field",
        "detail": "Use a supported field.",
        "hint": "See context.supported_fields.",
        "retryable": False,
        "safe_to_retry": False,
        "retry_after_ms": 0,
        "position": {"byte": 18, "line": 1, "column": 19},
        "context": {"supported_fields": ["partition_key", "run_id", "type"]},
    }


def index_response() -> dict[str, Any]:
    return {
        "contract_version": "ferric.flow.query.indexes/v1",
        "observed_at_ms": 100,
        "statistics_max_age_ms": 30_000,
        "registry": {"epoch": 2, "catalog_version": 3},
        "services": {"backfill": {"status": "idle"}},
        "indexes": [
            {
                "id": "flow_runs_tenant_updated",
                "version": 1,
                "build_id": "build-1",
                "state": "active",
                "queryable": True,
            }
        ],
    }


def record(identifier: str) -> dict[bytes, Any]:
    return {
        b"id": identifier.encode(),
        b"type": b"invoice",
        b"state": b"queued",
        b"partition_key": b"tenant-a",
        b"version": 1,
    }


class RecordingExecutor:
    def __init__(self, *responses: Any) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.responses = list(responses)

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class AsyncRecordingExecutor(RecordingExecutor):
    async def execute_command(self, *args: Any) -> Any:
        return super().execute_command(*args)


def hello() -> dict[str, Any]:
    return {
        "protocol": "ferricstore-native",
        "version": 1,
        "auth_required": False,
        "capabilities": {
            "limits": {"max_response_bytes": 4096},
            "response_codecs": {"compact_response_opcodes": {}},
            "schemas": {
                "FLOW.POLICY.SET": {
                    "required": ["type"],
                    "fields": ["type", "replace", "expected_generation", "states"],
                },
                "FLOW.QUERY": {
                    "required": ["version", "query"],
                    "fields": ["version", "query", "params", "deadline_ms"],
                },
            },
            "flow_query": {
                "request_contract": "ferric.flow.query.request/v1",
                "result_contract": "ferric.flow.query.result/v1",
                "explain_contract": "ferric.flow.explain/v1",
                "index_status_contract": "ferric.flow.query.indexes/v1",
                "language_versions": ["FQL1"],
                "capabilities": [
                    "flow_query_v1",
                    "flow_explain_v1",
                    "flow_explain_analyze_v1",
                    "flow_composite_index_v1",
                    "flow_query_index_status_v1",
                ],
                "shapes": [
                    "runs_by_run_id_record",
                    "runs_by_partition_and_run_id_record",
                    "runs_by_partition_predicates_ordered_records",
                    "runs_by_partition_type_state_ordered_records",
                    "runs_by_partition_type_terminals_ordered_records",
                    "runs_by_partition_metadata_ordered_records",
                    "runs_by_partition_type_running_lease_deadline_ordered_records",
                    "runs_by_partition_parent_ordered_records",
                    "runs_by_partition_root_ordered_records",
                    "runs_by_partition_correlation_ordered_records",
                    "runs_by_partition_predicates_count",
                    "events_by_run_id_ordered_records",
                ],
            },
        },
    }


def test_v010_uses_one_opaque_non_mutating_collection_opcode() -> None:
    assert _OPCODES["FLOW.QUERY"] == 0x0231
    for removed in {
        "FLOW.LIST",
        "FLOW.SEARCH",
        "FLOW.TERMINALS",
        "FLOW.FAILURES",
        "FLOW.BY_PARENT",
        "FLOW.BY_ROOT",
        "FLOW.BY_CORRELATION",
        "FLOW.STUCK",
    }:
        assert removed not in _OPCODES

    command = build_protocol_command(
        "FLOW.QUERY", "FQL1", QUERY, "tenant", "tenant-a", "type", "invoice"
    )
    assert command.opcode == 0x0231
    assert command.payload == {
        "version": "FQL1",
        "query": QUERY,
        "params": {"tenant": "tenant-a", "type": "invoice"},
    }
    assert flow_command_route_keys("FLOW.QUERY", ("FQL1", QUERY)) == ()
    assert request_may_mutate(0x0231) is False


def test_sync_query_decodes_page_and_sorts_bounded_parameters() -> None:
    executor = RecordingExecutor(query_response())
    client = FlowClient(executor)

    result = client.query(QUERY, {"type": "invoice", "tenant": "tenant-a"})

    assert isinstance(result, FlowQueryResult)
    assert [item[b"id"] for item in result.records or ()] == [b"one", b"two"]
    assert result.count is None
    assert result.page is not None and result.page.cursor == "fqc1_next"
    assert result.usage.result_records == 2
    assert executor.calls == [
        ("FLOW.QUERY", "FQL1", QUERY, "tenant", "tenant-a", "type", "invoice")
    ]


@pytest.mark.parametrize(
    ("query", "params"),
    [
        ("", {}),
        ("x" * (16 * 1024 + 1), {}),
        (QUERY, {"bad": math.inf}),
        (QUERY, {"bad": 2**63}),
        (QUERY, {str(index): index for index in range(65)}),
    ],
)
def test_query_rejects_unbounded_input_before_io(query: str, params: dict[str, Any]) -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)

    with pytest.raises((TypeError, ValueError, FerricStoreError)):
        client.query(query, params)

    assert executor.calls == []


def test_query_preserves_actionable_diagnostic_and_cause() -> None:
    cause = FerricStoreError("unsupported query field", raw=diagnostic())
    client = FlowClient(RecordingExecutor(cause))

    with pytest.raises(FlowQueryError) as raised:
        client.query("FROM runs WHERE nope = 1 RETURN RECORD")

    error = raised.value
    assert error.code == "unsupported_field"
    assert error.position is not None and error.position.column == 19
    assert error.context == {"supported_fields": ["partition_key", "run_id", "type"]}
    assert error.__cause__ is cause


def test_malformed_diagnostic_fails_closed_as_original_error() -> None:
    cause = FerricStoreError("unsupported query field", raw={"code": "unsupported_field"})
    client = FlowClient(RecordingExecutor(cause))

    with pytest.raises(FerricStoreError) as raised:
        client.query("FROM runs WHERE nope = 1 RETURN RECORD")

    assert raised.value is cause


def test_query_response_rejects_invalid_unicode_text() -> None:
    response = query_response()
    response["quality"]["exactness"] = "\ud800"

    with pytest.raises(FerricStoreError, match="quality exactness"):
        FlowClient(RecordingExecutor(response)).query(QUERY)


def test_query_response_rejects_oversized_quality_text() -> None:
    response = query_response()
    response["quality"]["exactness"] = "x" * 65

    with pytest.raises(FerricStoreError, match="quality exactness"):
        FlowClient(RecordingExecutor(response)).query(QUERY)


def test_explain_analyze_count_and_indexes_have_distinct_strict_contracts() -> None:
    analyze_usage = deepcopy(USAGE)
    executor = RecordingExecutor(
        explain_response("planned"),
        explain_response("executed", actual=analyze_usage),
        count_response(3),
        index_response(),
    )
    client = FlowClient(executor)

    planned = client.explain(QUERY, {"tenant": "tenant-a", "type": "invoice"})
    analyzed = client.explain_analyze(QUERY, {"tenant": "tenant-a", "type": "invoice"})
    counted = client.query(
        "FROM runs WHERE partition_key = @tenant AND type = @type RETURN COUNT",
        {"tenant": "tenant-a", "type": "invoice"},
    )
    indexes = client.query_indexes("flow_runs_tenant_updated")

    assert isinstance(planned, FlowExplainResult) and planned.status == "planned"
    assert analyzed.actual is not None and analyzed.actual.result_records == 2
    assert counted.count == 3 and counted.records is None
    assert isinstance(indexes, FlowQueryIndexStatus)
    assert indexes.registry.catalog_version == 3
    assert indexes.indexes[0].queryable is True
    assert executor.calls[-1] == ("FLOW.QUERY.INDEXES", "flow_runs_tenant_updated")


def test_index_status_accepts_exactly_the_unsigned_64_bit_metadata_domain() -> None:
    maximum = 2**64 - 1
    response = index_response()
    response["registry"] = {"epoch": maximum, "catalog_version": maximum}
    response["indexes"][0]["version"] = maximum

    status = FlowClient(RecordingExecutor(response)).query_indexes()
    assert status.registry.epoch == maximum
    assert status.registry.catalog_version == maximum
    assert status.indexes[0].version == maximum

    response["registry"]["epoch"] = maximum + 1
    with pytest.raises(FerricStoreError, match="unsigned 64-bit"):
        FlowClient(RecordingExecutor(response)).query_indexes()


def test_empty_query_index_id_is_rejected_before_io() -> None:
    executor = RecordingExecutor(index_response())

    with pytest.raises(ValueError, match="query index id"):
        FlowClient(executor).query_indexes("")

    async def run() -> None:
        async_executor = AsyncRecordingExecutor(index_response())
        with pytest.raises(ValueError, match="query index id"):
            await AsyncFlowClient(async_executor).query_indexes("")
        assert async_executor.calls == []

    asyncio.run(run())
    assert executor.calls == []


def test_convenience_queries_compile_fql_and_require_a_partition() -> None:
    executor = RecordingExecutor(*(query_response() for _ in range(7)))
    client = FlowClient(executor)

    records = client.list("invoice", partition_key="tenant-a", state="failed", count=2, rev=True)
    searched = client.search(
        "invoice",
        partition_key="tenant-a",
        state="queued",
        attributes={"customer": "one"},
        count=2,
    )

    assert [item.id for item in records] == ["one", "two"]
    assert [item.id for item in searched] == ["one", "two"]
    assert all(call[0] == "FLOW.QUERY" for call in executor.calls)
    assert "ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS" in executor.calls[0][2]
    assert "attribute['customer'] = @attribute_0" in executor.calls[1][2]

    with pytest.raises(ValueError, match="partition_key"):
        client.list("invoice")

    with pytest.raises(ValueError, match="concrete flow type"):
        client.stuck("any", partition_key="tenant-a", now_ms=1_000)

    with pytest.raises(ValueError, match=r"concrete flow type|attribute predicate"):
        client.list("any", partition_key="tenant-a")

    with pytest.raises(ValueError, match="attribute predicate"):
        client.list("invoice", partition_key="tenant-a", state="any")

    with pytest.raises(ValueError, match="concrete flow type"):
        client.search(
            "any",
            partition_key="tenant-a",
            state_meta={"queued": {"risk": 3}},
        )

    with pytest.raises(ValueError, match="concrete flow type"):
        client.terminals("any", partition_key="tenant-a")

    with pytest.raises(ValueError, match=r"concrete flow type|attribute predicate"):
        client.failures("any", partition_key="tenant-a")

    with pytest.raises(ValueError, match="attribute"):
        client.by_parent(
            "parent-1",
            partition_key="tenant-a",
            attributes={"tenant": "acme"},
        )

    assert len(executor.calls) == 2


def test_query_rejects_unencodable_text_parameters_before_io() -> None:
    executor = RecordingExecutor(query_response())
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="valid UTF-8"):
        client.query(QUERY, {"tenant": "\ud800"})

    assert executor.calls == []


def test_query_conveniences_preserve_server_metadata_normalization() -> None:
    executor = RecordingExecutor(query_response(), query_response())
    client = FlowClient(executor)

    client.search(
        "invoice",
        partition_key="tenant-a",
        attributes={" customer ": "one"},
    )
    client.search(
        "invoice",
        partition_key="tenant-a",
        state_meta={" queued ": {" risk ": 3}},
    )

    assert "attribute['customer'] = @attribute_0" in executor.calls[0][2]
    assert "state_meta['queued']['risk'] = @state_meta_0" in executor.calls[1][2]


@pytest.mark.parametrize(
    "attributes",
    [
        {"tenant": "one", " tenant ": "two"},
        {"__internal": "one"},
        {"x" * 65: "one"},
    ],
)
def test_query_conveniences_reject_invalid_metadata_before_io(
    attributes: dict[str, Any],
) -> None:
    executor = RecordingExecutor(query_response())

    with pytest.raises(ValueError, match=r"metadata|attribute"):
        FlowClient(executor).search(
            "invoice",
            partition_key="tenant-a",
            attributes=attributes,
        )

    assert executor.calls == []


def test_query_conveniences_reject_duplicate_normalized_state_names_before_io() -> None:
    executor = RecordingExecutor(query_response())

    with pytest.raises(ValueError, match="state"):
        FlowClient(executor).search(
            "invoice",
            partition_key="tenant-a",
            state_meta={"queued": {"risk": 1}, " queued ": {"risk": 2}},
        )

    assert executor.calls == []


def test_query_conveniences_reject_empty_non_mapping_state_meta_before_io() -> None:
    executor = RecordingExecutor(query_response())

    with pytest.raises(TypeError, match="state_meta"):
        FlowClient(executor).search(
            "invoice",
            partition_key="tenant-a",
            attributes={"tenant": "acme"},
            state_meta=[],
        )

    async def run() -> None:
        async_executor = AsyncRecordingExecutor(query_response())
        with pytest.raises(TypeError, match="state_meta"):
            await AsyncFlowClient(async_executor).search(
                "invoice",
                partition_key="tenant-a",
                attributes={"tenant": "acme"},
                state_meta=[],
            )
        assert async_executor.calls == []

    asyncio.run(run())
    assert executor.calls == []


def test_async_query_uses_the_same_contract() -> None:
    async def run() -> None:
        executor = AsyncRecordingExecutor(query_response(), explain_response("planned"))
        client = AsyncFlowClient(executor)

        result = await client.query(QUERY, {"tenant": "tenant-a", "type": "invoice"})
        explained = await client.explain(QUERY, {"tenant": "tenant-a", "type": "invoice"})

        assert isinstance(result, FlowQueryResult)
        assert isinstance(explained, FlowExplainResult)
        assert all(call[0] == "FLOW.QUERY" for call in executor.calls)

    asyncio.run(run())


def test_hello_requires_the_complete_query_manifest_and_schema() -> None:
    negotiated = parse_hello_capabilities(hello())
    assert negotiated.flow_query.language_versions == frozenset({"FQL1"})

    missing_shape = hello()
    missing_shape["capabilities"]["flow_query"]["shapes"].pop()
    with pytest.raises(FerricStoreError, match="events_by_run_id_ordered_records"):
        parse_hello_capabilities(missing_shape)

    missing_field = hello()
    missing_field["capabilities"]["schemas"]["FLOW.QUERY"]["fields"].remove("deadline_ms")
    with pytest.raises(FerricStoreError, match="deadline_ms"):
        parse_hello_capabilities(missing_field)

    wrong_index_contract = hello()
    wrong_index_contract["capabilities"]["flow_query"]["index_status_contract"] = "future/v2"
    with pytest.raises(FerricStoreError, match="index_status_contract"):
        parse_hello_capabilities(wrong_index_contract)
