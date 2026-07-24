from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Any

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncProtocolAdapter,
    AsyncProtocolAdapterPool,
    AsyncTopologyProtocolAdapterPool,
    FlowClient,
    FlowFields,
    FlowQuery,
    ProtocolAdapter,
    ProtocolAdapterPool,
    TopologyProtocolAdapterPool,
)
from ferricstore.async_client_sessions import _AsyncErrorMappingExecutor
from ferricstore.client_sessions import _ErrorMappingExecutor
from ferricstore.flow_query_request import build_flow_query_payload
from ferricstore.flow_routing import flow_logical_partition_routing_key


def _query_response() -> dict[str, Any]:
    return {
        "version": "ferric.flow.query.result/v1",
        "records": [],
        "page": {"has_more": False, "cursor": None},
        "quality": {
            "exactness": "authoritative_exact",
            "freshness": "authoritative",
            "coverage": "complete",
            "pagination": "none",
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


def _point_query() -> FlowQuery:
    return (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq("tenant-a"),
            FlowFields.run_id.eq("run-1"),
        )
        .return_record()
    )


class WireOnlyExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        return deepcopy(_query_response())


class AsyncWireOnlyExecutor(WireOnlyExecutor):
    async def execute_command(self, *args: Any) -> Any:
        return super().execute_command(*args)


class QueryOptionsExecutor(WireOnlyExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.options: list[tuple[int | None, str | bytes | None]] = []

    def execute_flow_query_command(
        self,
        *args: Any,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> Any:
        self.options.append((deadline_ms, routing_key))
        return self.execute_command(*args)


class AsyncQueryOptionsExecutor(AsyncWireOnlyExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.options: list[tuple[int | None, str | bytes | None]] = []

    async def execute_flow_query_command(
        self,
        *args: Any,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> Any:
        self.options.append((deadline_ms, routing_key))
        return await self.execute_command(*args)


def test_documented_sync_custom_executor_receives_only_wire_arguments() -> None:
    executor = WireOnlyExecutor()

    FlowClient(executor).query(_point_query())

    assert executor.calls == [
        (
            "FLOW.QUERY",
            "FQL1",
            "FROM runs WHERE partition_key = @_fql_0 AND run_id = @_fql_1 RETURN RECORD",
            "_fql_0",
            "tenant-a",
            "_fql_1",
            "run-1",
        )
    ]


def test_documented_async_custom_executor_receives_only_wire_arguments() -> None:
    async def run() -> None:
        executor = AsyncWireOnlyExecutor()

        await AsyncFlowClient(executor).query(_point_query())

        assert executor.calls[0][-1] == "run-1"
        assert all(isinstance(value, (str, bytes, bool, int, float)) for value in executor.calls[0])

    asyncio.run(run())


def test_deadline_requires_the_explicit_sync_query_executor_capability() -> None:
    executor = WireOnlyExecutor()

    with pytest.raises(TypeError, match="execute_flow_query_command"):
        FlowClient(executor).query(_point_query(), deadline_ms=123)

    assert executor.calls == []


def test_deadline_requires_the_explicit_async_query_executor_capability() -> None:
    async def run() -> None:
        executor = AsyncWireOnlyExecutor()

        with pytest.raises(TypeError, match="execute_flow_query_command"):
            await AsyncFlowClient(executor).query(_point_query(), deadline_ms=123)

        assert executor.calls == []

    asyncio.run(run())


def test_sync_query_options_capability_receives_options_out_of_band() -> None:
    executor = QueryOptionsExecutor()

    FlowClient(executor).query(_point_query(), deadline_ms=123)

    assert executor.options == [
        (123, flow_logical_partition_routing_key("tenant-a")),
    ]
    assert executor.calls[0][-1] == "run-1"


def test_async_query_options_capability_receives_options_out_of_band() -> None:
    async def run() -> None:
        executor = AsyncQueryOptionsExecutor()

        await AsyncFlowClient(executor).query(_point_query(), deadline_ms=123)

        assert executor.options == [
            (123, flow_logical_partition_routing_key("tenant-a")),
        ]
        assert executor.calls[0][-1] == "run-1"

    asyncio.run(run())


@pytest.mark.parametrize(
    "executor_type",
    [ProtocolAdapter, ProtocolAdapterPool, TopologyProtocolAdapterPool],
)
def test_every_sync_native_executor_keeps_query_options_inside_transport(
    executor_type: type[Any],
) -> None:
    calls: list[tuple[Any, ...]] = []
    executor = object.__new__(executor_type)

    def execute_command(*args: Any) -> bytes:
        calls.append(args)
        return b"ok"

    executor.execute_command = execute_command

    result = _ErrorMappingExecutor(executor).execute_flow_query_command(
        "FLOW.QUERY",
        "FQL1",
        "FROM runs WHERE run_id = @id RETURN RECORD",
        "id",
        "run-1",
        deadline_ms=123,
        routing_key="route-key",
    )

    assert result == b"ok"
    assert build_flow_query_payload(calls[0][1:]) == {
        "version": "FQL1",
        "query": "FROM runs WHERE run_id = @id RETURN RECORD",
        "params": {"id": "run-1"},
        "deadline_ms": 123,
    }


@pytest.mark.parametrize(
    "executor_type",
    [AsyncProtocolAdapter, AsyncProtocolAdapterPool, AsyncTopologyProtocolAdapterPool],
)
def test_every_async_native_executor_keeps_query_options_inside_transport(
    executor_type: type[Any],
) -> None:
    async def run() -> None:
        calls: list[tuple[Any, ...]] = []
        executor = object.__new__(executor_type)

        async def execute_command(*args: Any) -> bytes:
            calls.append(args)
            return b"ok"

        executor.execute_command = execute_command

        result = await _AsyncErrorMappingExecutor(executor).execute_flow_query_command(
            "FLOW.QUERY",
            "FQL1",
            "FROM runs WHERE run_id = @id RETURN RECORD",
            "id",
            "run-1",
            deadline_ms=123,
            routing_key="route-key",
        )

        assert result == b"ok"
        assert build_flow_query_payload(calls[0][1:])["deadline_ms"] == 123

    asyncio.run(run())
