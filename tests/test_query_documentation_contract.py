from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from ferricstore import FlowClient, FlowFields, FlowQuery, flow_param
from ferricstore.protocol_commands import build_protocol_command

_ROOT = Path(__file__).resolve().parents[1]
_DOCUMENTATION = ("README.md", "docs/client.md", "docs/sdk.md")
_QUERY_HELPERS = {
    "list",
    "search",
    "terminals",
    "failures",
    "by_parent",
    "by_root",
    "by_correlation",
    "stuck",
}
_LINEAGE_HELPERS = {"by_parent", "by_root", "by_correlation"}
_UNSUPPORTED_QUERY_OPTIONS = {"include_cold", "consistent_projection"}


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.query_options: list[tuple[int | None, str | bytes | None]] = []

    def execute_command(self, *args: Any) -> dict[str, Any]:
        self.calls.append(args)
        return _empty_query_response()

    def execute_flow_query_command(
        self,
        *args: Any,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> dict[str, Any]:
        self.query_options.append((deadline_ms, routing_key))
        return self.execute_command(*args)


def _empty_query_response() -> dict[str, Any]:
    return {
        "version": "ferric.flow.query.result/v1",
        "records": [],
        "page": {"has_more": False},
        "quality": {
            "exactness": "projected_exact",
            "freshness": "projection_watermark",
            "coverage": "complete",
            "pagination": "live_seek",
        },
        "usage": {
            "range_seeks": 0,
            "range_pages": 0,
            "scanned_entries": 0,
            "scanned_bytes": 0,
            "hydrated_records": 0,
            "residual_checks": 0,
            "duplicate_entries": 0,
            "result_records": 0,
            "response_bytes": 0,
            "memory_high_water_bytes": 0,
            "wall_time_us": 0,
        },
    }


def _documented_query_calls() -> Iterator[tuple[str, str, ast.Call]]:
    for relative_path in _DOCUMENTATION:
        text = (_ROOT / relative_path).read_text(encoding="utf-8")
        for block in re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL):
            for node in ast.walk(ast.parse(block)):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in _QUERY_HELPERS
                ):
                    yield relative_path, node.func.attr, node


def test_documented_query_helpers_are_partition_scoped_and_supported() -> None:
    failures: list[str] = []
    calls = list(_documented_query_calls())

    assert calls, "expected documented Flow query helper calls"
    for path, method, call in calls:
        keywords = {keyword.arg: keyword.value for keyword in call.keywords if keyword.arg}
        partition = keywords.get("partition_key")
        if partition is None or (isinstance(partition, ast.Constant) and partition.value is None):
            failures.append(f"{path}: {method}() must include a concrete partition_key")

        unsupported = sorted(keywords.keys() & _UNSUPPORTED_QUERY_OPTIONS)
        if unsupported:
            failures.append(f"{path}: {method}() documents unsupported {unsupported}")

        if method in _LINEAGE_HELPERS:
            incompatible = sorted(keywords.keys() & {"attributes", "terminal_only"})
            if incompatible:
                failures.append(f"{path}: {method}() documents incompatible options {incompatible}")

    assert not failures, "\n" + "\n".join(failures)


def test_representative_documented_query_helpers_compile_to_partitioned_fql() -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)

    query = (
        "FROM runs WHERE partition_key = @partition AND type = @type AND state = @state "
        "ORDER BY updated_at_ms DESC LIMIT 100 RETURN RECORDS"
    )
    client.query(
        query,
        {"partition": "tenant-a", "type": "order", "state": "queued"},
        deadline_ms=1_800_000_000_000,
    )
    client.list("order", partition_key="tenant-a", state="queued", count=100)
    client.search(
        "order",
        partition_key="tenant-a",
        state="completed",
        attributes={"tenant": "acme"},
        state_meta={"version": 3},
        terminal_only=True,
        count=100,
    )
    client.terminals("order", partition_key="tenant-a", state="completed", rev=True, count=100)
    client.failures("order", partition_key="tenant-a", from_ms=0, to_ms=1_000)
    client.by_parent("parent-flow-id", partition_key="tenant-a", count=100)
    client.by_root("root-flow-id", partition_key="tenant-a", state="failed", count=100)
    client.by_correlation("checkout-123", partition_key="tenant-a", count=100)
    client.stuck(
        "order",
        partition_key="tenant-a",
        older_than_ms=60_000,
        now_ms=120_000,
        count=100,
    )

    assert executor.calls[0][:3] == ("FLOW.QUERY", "FQL1", query)
    assert dict(zip(executor.calls[0][3::2], executor.calls[0][4::2], strict=True)) == {
        "partition": "tenant-a",
        "state": "queued",
        "type": "order",
    }
    assert executor.query_options[0][0] == 1_800_000_000_000
    assert len(executor.calls) == 9
    for call in executor.calls[1:]:
        command, version, compiled_query, *_flat_params = call
        assert (command, version) == ("FLOW.QUERY", "FQL1")
        assert "partition_key = @partition_key" in compiled_query
        payload = build_protocol_command(*call).payload
        assert isinstance(payload, dict)
        assert payload["params"]["partition_key"] == "tenant-a"


def test_documented_composable_query_executes_directly_and_retains_page_bindings() -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)
    query = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq(flow_param("partition")),
            FlowFields.type.eq("order"),
            FlowFields.state.in_("queued", "running"),
        )
        .order_by(FlowFields.updated_at_ms.desc())
        .limit(25)
        .return_records()
        .bind(partition="tenant-a")
    )

    client.query(query)
    client.query(query.cursor("fqc1_abcdefghijk"))

    compiled, params = query.compile()
    assert executor.calls[0][2] == compiled
    first_payload = build_protocol_command(*executor.calls[0]).payload
    assert isinstance(first_payload, dict)
    assert first_payload["params"] == params
    assert " CURSOR " in executor.calls[1][2]
    paged_payload = build_protocol_command(*executor.calls[1]).payload
    assert isinstance(paged_payload, dict)
    assert paged_payload["params"]["partition"] == "tenant-a"


def test_client_parity_table_names_the_public_query_and_telemetry_apis() -> None:
    text = (_ROOT / "docs/client.md").read_text(encoding="utf-8")
    query_row = next(line for line in text.splitlines() if line.startswith("| Query |"))
    management_row = next(
        line for line in text.splitlines() if line.startswith("| Management reads/writes |")
    )

    for method in ("query", "explain", "explain_analyze", "query_indexes"):
        assert f"`{method}`" in query_row
    assert "`telemetry_flow_query`" in management_row
    assert "`flow_query`" not in management_row


def test_adapter_docs_cover_async_query_executor_awaitability() -> None:
    text = (_ROOT / "docs/adapters.md").read_text(encoding="utf-8")

    assert "class MyAsyncExecutor:" in text
    assert "async def execute_flow_query_command(" in text
    assert "must return an awaitable" in text


def test_client_docs_describe_specialized_explain_shape() -> None:
    text = (_ROOT / "docs/client.md").read_text(encoding="utf-8")
    normalized = " ".join(text.split())

    assert "specialized planned explain" in normalized
    assert "requested`, `available`, and `missing" in normalized
