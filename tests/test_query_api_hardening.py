from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator, Mapping
from copy import deepcopy
from typing import Any

import pytest

from ferricstore import (
    AsyncFlowClient,
    BackpressurePolicy,
    FerricStoreError,
    FlowClient,
    FlowFields,
    FlowQuery,
    ProtocolAdapter,
    RequestOutcomeUnknownError,
    flow_param,
)
from ferricstore.flow_query_builder import build_flow_lineage_query
from ferricstore.flow_routing import (
    flow_auto_id_routing_key,
    flow_logical_partition_routing_key,
)
from ferricstore.protocol_async import AsyncProtocolAdapter
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_constants import _OP_PIPELINE
from ferricstore.protocol_retry import request_may_mutate

QUERY = (
    "FROM runs WHERE partition_key = @partition ORDER BY updated_at_ms ASC LIMIT 1 RETURN RECORDS"
)


def _usage(result_records: int = 0) -> dict[str, int]:
    return {
        "range_seeks": 0,
        "range_pages": 0,
        "scanned_entries": 0,
        "scanned_bytes": 0,
        "hydrated_records": 0,
        "residual_checks": 0,
        "duplicate_entries": 0,
        "result_records": result_records,
        "response_bytes": 0,
        "memory_high_water_bytes": 0,
        "wall_time_us": 0,
    }


def _query_response() -> dict[str, Any]:
    return {
        "version": "ferric.flow.query.result/v1",
        "records": [],
        "page": {"has_more": False, "cursor": None},
        "quality": {
            "exactness": "projected_exact",
            "freshness": "projection_watermark",
            "coverage": "complete",
            "pagination": "live_seek",
        },
        "usage": _usage(),
    }


def _explain_response() -> dict[str, Any]:
    return {
        "version": "ferric.flow.explain/v1",
        "query_fingerprint": "a" * 64,
        "status": "planned",
        "plan": {},
        "estimate": {},
        "stats": {"source": "fresh"},
        "quality": {
            "exactness": "projected_exact",
            "freshness": "projection_watermark",
            "coverage": "complete",
            "pagination": "live_seek",
        },
        "bounds": {},
        "pressure": {"resources": []},
        "decision": {"reason": "only_bounded_candidate"},
        "alternatives": [],
        "actual": None,
        "diagnostic": None,
    }


class ProtocolRecordingExecutor:
    def __init__(self, *responses: Any) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[Any, ...]] = []
        self.payloads: list[dict[str, Any] | bytes] = []
        self.query_options: list[tuple[int | None, str | bytes | None]] = []

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        self.payloads.append(build_protocol_command(*args).payload)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return deepcopy(response)

    def execute_flow_query_command(
        self,
        *args: Any,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> Any:
        self.query_options.append((deadline_ms, routing_key))
        self.calls.append(args)
        payload = build_protocol_command(*args).payload
        if deadline_ms is not None:
            assert isinstance(payload, dict)
            payload["deadline_ms"] = deadline_ms
        self.payloads.append(payload)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return deepcopy(response)


class AsyncProtocolRecordingExecutor(ProtocolRecordingExecutor):
    async def execute_command(self, *args: Any) -> Any:
        return super().execute_command(*args)

    async def execute_flow_query_command(
        self,
        *args: Any,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> Any:
        return super().execute_flow_query_command(
            *args,
            deadline_ms=deadline_ms,
            routing_key=routing_key,
        )


class PrefixScanBomb(str):
    def lstrip(self, *_args: Any, **_kwargs: Any) -> str:
        raise AssertionError("oversized query must be rejected before prefix scanning")


def _query_retry_policy() -> BackpressurePolicy:
    return BackpressurePolicy(
        max_retries=1,
        max_elapsed_ms=1_000,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )


def test_sync_query_retries_only_server_declared_safe_error_and_honors_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    clock = [0.0]

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("ferricstore.backpressure.time.sleep", sleep)
    error = FerricStoreError(
        "busy",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=125,
    )
    executor = ProtocolRecordingExecutor(error, _query_response())

    result = FlowClient(executor, backpressure=_query_retry_policy()).query(
        QUERY, {"partition": "tenant-a"}
    )

    assert result.records == ()
    assert len(executor.calls) == 2
    assert sleeps == [0.125]


def test_sync_query_does_not_retry_after_deadline_or_unsafe_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("ferricstore.backpressure.time.sleep", sleeps.append)
    errors = [
        FerricStoreError(
            "busy",
            retryable=True,
            safe_to_retry=True,
            retry_after_ms=1,
        ),
        FerricStoreError(
            "unsafe",
            retryable=True,
            safe_to_retry=False,
            retry_after_ms=1,
        ),
    ]

    past_deadline_ms = int(time.time() * 1_000) - 1
    for error, deadline_ms in zip(errors, (past_deadline_ms, None), strict=True):
        executor = ProtocolRecordingExecutor(error, _query_response())
        with pytest.raises(FerricStoreError) as raised:
            FlowClient(executor, backpressure=_query_retry_policy()).query(
                QUERY,
                {"partition": "tenant-a"},
                deadline_ms=deadline_ms,
            )
        assert str(raised.value) == str(error)
        assert raised.value.safe_to_retry is error.safe_to_retry
        assert len(executor.calls) == 1
    assert sleeps == []


def test_async_query_retries_server_declared_safe_error_without_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    clock = [0.0]

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: clock[0])
    monkeypatch.setattr(asyncio, "sleep", sleep)
    error = FerricStoreError(
        "busy",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=75,
    )

    async def run() -> None:
        executor = AsyncProtocolRecordingExecutor(error, _query_response())
        result = await AsyncFlowClient(executor, backpressure=_query_retry_policy()).query(
            QUERY, {"partition": "tenant-a"}
        )
        assert result.records == ()
        assert len(executor.calls) == 2

    asyncio.run(run())
    assert sleeps == [0.075]


def test_non_overload_query_retry_does_not_mutate_shared_pressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("ferricstore.backpressure.time.sleep", sleeps.append)
    error = FerricStoreError(
        "query_projection_changed",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=20,
    )
    executor = ProtocolRecordingExecutor(error, _query_response())
    client = FlowClient(executor, backpressure=_query_retry_policy())

    client.query(QUERY, {"partition": "tenant-a"})

    assert sleeps == [0.02]
    assert client.backpressure._state.consecutive_overloads == 0
    assert client.backpressure._state.blocked_until == 0


def test_sync_non_overload_query_retries_follow_policy_beyond_three_attempts() -> None:
    error = FerricStoreError(
        "query_projection_changed",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )
    executor = ProtocolRecordingExecutor(*([error] * 4), _query_response())
    policy = BackpressurePolicy(
        max_retries=4,
        max_elapsed_ms=None,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )

    result = FlowClient(executor, backpressure=policy).query(QUERY, {"partition": "tenant-a"})

    assert result.records == ()
    assert len(executor.calls) == 5


def test_async_non_overload_query_retries_follow_policy_beyond_three_attempts() -> None:
    error = FerricStoreError(
        "query_projection_changed",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )
    policy = BackpressurePolicy(
        max_retries=4,
        max_elapsed_ms=None,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )

    async def run() -> None:
        executor = AsyncProtocolRecordingExecutor(*([error] * 4), _query_response())
        result = await AsyncFlowClient(executor, backpressure=policy).query(
            QUERY, {"partition": "tenant-a"}
        )

        assert result.records == ()
        assert len(executor.calls) == 5

    asyncio.run(run())


def test_sync_fully_unbounded_zero_delay_query_retries_yield_cpu(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("ferricstore.flow_query_retry.time.sleep", sleeps.append)
    error = FerricStoreError(
        "query_projection_changed",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )
    executor = ProtocolRecordingExecutor(error, error, _query_response())
    policy = BackpressurePolicy(
        max_retries=None,
        max_elapsed_ms=None,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )

    result = FlowClient(executor, backpressure=policy).query(QUERY, {"partition": "tenant-a"})

    assert result.records == ()
    assert sleeps == [0.001, 0.001]


def test_async_fully_unbounded_zero_delay_query_retries_yield_cpu(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    error = FerricStoreError(
        "query_projection_changed",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )
    policy = BackpressurePolicy(
        max_retries=None,
        max_elapsed_ms=None,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )

    async def run() -> None:
        executor = AsyncProtocolRecordingExecutor(error, error, _query_response())
        result = await AsyncFlowClient(executor, backpressure=policy).query(
            QUERY, {"partition": "tenant-a"}
        )
        assert result.records == ()

    asyncio.run(run())
    assert sleeps == [0.001, 0.001]


def test_query_indexes_retries_safe_response_and_enforces_requested_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.test_flow_query_response_contract import _index_response

    sleeps: list[float] = []
    clock = [0.0]

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("ferricstore.backpressure.time.sleep", sleep)
    retryable = FerricStoreError(
        "busy",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=25,
    )
    response = _index_response()
    executor = ProtocolRecordingExecutor(retryable, response)

    with pytest.raises(FerricStoreError, match="requested id"):
        FlowClient(executor, backpressure=_query_retry_policy()).query_indexes("different-index")

    assert len(executor.calls) == 2
    assert sleeps == [0.025]


def test_async_query_indexes_enforces_requested_filter() -> None:
    from tests.test_flow_query_response_contract import _index_response

    async def run() -> None:
        executor = AsyncProtocolRecordingExecutor(_index_response())
        with pytest.raises(FerricStoreError, match="requested id"):
            await AsyncFlowClient(executor).query_indexes("different-index")
        assert len(executor.calls) == 1

    asyncio.run(run())


@pytest.mark.parametrize("deadline_ms", [0, 1_725_000_000_000, 2**64 - 1])
def test_sync_query_and_explain_encode_absolute_deadline(deadline_ms: int) -> None:
    executor = ProtocolRecordingExecutor(_query_response(), _explain_response())
    client = FlowClient(executor)

    client.query(QUERY, {"partition": "tenant-a"}, deadline_ms=deadline_ms)
    client.explain(QUERY, {"partition": "tenant-a"}, deadline_ms=deadline_ms)

    assert executor.payloads[0] == {
        "version": "FQL1",
        "query": QUERY,
        "params": {"partition": "tenant-a"},
        "deadline_ms": deadline_ms,
    }
    assert executor.payloads[1]["query"] == "EXPLAIN " + QUERY
    assert executor.payloads[1]["deadline_ms"] == deadline_ms


def test_async_query_and_analyze_encode_absolute_deadline() -> None:
    async def run() -> None:
        executor = AsyncProtocolRecordingExecutor(_query_response(), _explain_response())
        client = AsyncFlowClient(executor)

        await client.query(QUERY, {"partition": "tenant-a"}, deadline_ms=123)
        await client.explain_analyze(QUERY, {"partition": "tenant-a"}, deadline_ms=456)

        assert executor.payloads[0]["deadline_ms"] == 123
        assert executor.payloads[1]["deadline_ms"] == 456
        assert executor.payloads[1]["query"] == "EXPLAIN ANALYZE " + QUERY

    asyncio.run(run())


def test_sync_client_executes_composable_query_and_retains_bindings_for_pages() -> None:
    executor = ProtocolRecordingExecutor(_query_response(), _query_response())
    client = FlowClient(executor)
    query = (
        FlowQuery.runs()
        .where(
            FlowFields.partition_key.eq(flow_param("partition")),
            FlowFields.type.eq("invoice"),
        )
        .order_by(FlowFields.updated_at_ms.desc())
        .limit(10)
        .return_records()
        .bind(partition="tenant-a")
    )

    client.query(query)
    client.query(query.cursor("fqc1_" + "x" * 16))

    assert executor.payloads[0]["query"].startswith("FROM runs WHERE partition_key = @partition")
    assert executor.payloads[0]["params"] == {
        "partition": "tenant-a",
        "_fql_0": "invoice",
    }
    assert "CURSOR @_fql_cursor" in executor.payloads[1]["query"]
    assert executor.payloads[1]["params"]["partition"] == "tenant-a"


def test_typed_and_convenience_queries_emit_local_only_partition_routing_hints() -> None:
    executor = ProtocolRecordingExecutor(
        _query_response(),
        _query_response(),
        _query_response(),
        _query_response(),
    )
    client = FlowClient(executor)
    partition_query = (
        FlowQuery.runs()
        .where(FlowFields.partition_key.eq("tenant-a"))
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .return_records()
    )
    point_query = FlowQuery.runs().where(FlowFields.run_id.eq("run-1")).return_record()

    client.query(partition_query)
    client.query(point_query)
    client.query(QUERY, {"partition": "tenant-a"})
    client.list("invoice", partition_key="tenant-a")

    routes = [() if route is None else (route,) for _deadline, route in executor.query_options]
    assert routes == [
        (flow_logical_partition_routing_key("tenant-a"),),
        (flow_auto_id_routing_key("run-1"),),
        (),
        (flow_logical_partition_routing_key("tenant-a"),),
    ]
    assert all("routing_key" not in payload for payload in executor.payloads)


def test_query_object_rejects_a_second_parameter_source_before_io() -> None:
    executor = ProtocolRecordingExecutor(_query_response())
    query = FlowQuery.runs().where(FlowFields.run_id.eq("run-1")).return_record()

    with pytest.raises(ValueError, match=r"bind.*FlowQuery|separate.*params"):
        FlowClient(executor).query(query, {"extra": "value"})

    assert executor.calls == []


def test_async_client_executes_and_explains_composable_query() -> None:
    async def run() -> None:
        executor = AsyncProtocolRecordingExecutor(_query_response(), _explain_response())
        client = AsyncFlowClient(executor)
        query = FlowQuery.runs().where(FlowFields.partition_key.eq("tenant-a")).return_count()

        await client.query(query, deadline_ms=123)
        await client.explain(query, deadline_ms=456)

        assert executor.payloads[0]["deadline_ms"] == 123
        assert executor.payloads[1]["query"].startswith("EXPLAIN FROM runs")
        assert executor.payloads[1]["deadline_ms"] == 456
        expected_route = (flow_logical_partition_routing_key("tenant-a"),)
        assert [
            () if route is None else (route,) for _deadline, route in executor.query_options
        ] == [
            expected_route,
            expected_route,
        ]

    asyncio.run(run())


def test_sync_explain_rejects_typed_cursor_before_io() -> None:
    query = (
        FlowQuery.runs()
        .where(FlowFields.partition_key.eq("tenant-a"))
        .order_by(FlowFields.updated_at_ms.asc())
        .limit(10)
        .cursor("fqc1_abcdefghijk")
        .return_records()
    )

    for method_name in ("explain", "explain_analyze"):
        executor = ProtocolRecordingExecutor()
        method = getattr(FlowClient(executor), method_name)
        with pytest.raises(ValueError, match=r"cursor.*EXPLAIN"):
            method(query)
        assert executor.calls == []


def test_async_explain_rejects_typed_cursor_before_io() -> None:
    async def run() -> None:
        query = (
            FlowQuery.runs()
            .where(FlowFields.partition_key.eq("tenant-a"))
            .order_by(FlowFields.updated_at_ms.asc())
            .limit(10)
            .cursor("fqc1_abcdefghijk")
            .return_records()
        )

        for method_name in ("explain", "explain_analyze"):
            executor = AsyncProtocolRecordingExecutor()
            method = getattr(AsyncFlowClient(executor), method_name)
            with pytest.raises(ValueError, match=r"cursor.*EXPLAIN"):
                await method(query)
            assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize("deadline_ms", [-1, True, 1.5, 2**64])
def test_query_rejects_invalid_deadline_before_io(deadline_ms: Any) -> None:
    executor = ProtocolRecordingExecutor(_query_response())

    with pytest.raises((TypeError, ValueError), match="deadline_ms"):
        FlowClient(executor).query(QUERY, {"partition": "tenant-a"}, deadline_ms=deadline_ms)

    assert executor.calls == []


def test_query_rejects_oversized_text_before_prefix_scanning() -> None:
    executor = ProtocolRecordingExecutor()
    query = PrefixScanBomb("x" * 16_385)

    with pytest.raises(ValueError, match="exceeds"):
        FlowClient(executor).query(query)

    assert executor.calls == []


def test_explain_preserves_non_grammar_leading_whitespace() -> None:
    executor = ProtocolRecordingExecutor(_explain_response())
    query = "\u00a0FROM runs WHERE run_id = @id RETURN RECORD"

    FlowClient(executor).explain(query, {"id": "run-1"})

    assert executor.calls[0][2] == "EXPLAIN " + query


def test_query_rejects_explain_prefix_before_io_for_sync_and_async() -> None:
    sync_executor = ProtocolRecordingExecutor(_query_response())
    with pytest.raises(ValueError, match="does not accept EXPLAIN"):
        FlowClient(sync_executor).query("EXPLAIN " + QUERY, {"partition": "tenant-a"})
    assert sync_executor.calls == []

    async def run() -> None:
        async_executor = AsyncProtocolRecordingExecutor(_query_response())
        with pytest.raises(ValueError, match="does not accept EXPLAIN"):
            await AsyncFlowClient(async_executor).query(
                "EXPLAIN ANALYZE " + QUERY,
                {"partition": "tenant-a"},
            )
        assert async_executor.calls == []

    asyncio.run(run())


class OversizedMapping(Mapping[str, Any]):
    """A mapping whose entries must never be materialized after its size proves rejection."""

    def __getitem__(self, key: str) -> Any:
        raise AssertionError("oversized mapping was accessed")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("oversized mapping was iterated")

    def __len__(self) -> int:
        return 1_000_000


def test_metadata_predicate_budget_rejects_before_iteration_or_sorting() -> None:
    executor = ProtocolRecordingExecutor(_query_response())

    with pytest.raises(ValueError, match="12 predicates"):
        FlowClient(executor).search(
            "invoice",
            partition_key="tenant-a",
            attributes=OversizedMapping(),
        )

    assert executor.calls == []


def test_state_metadata_outer_budget_rejects_before_iteration() -> None:
    executor = ProtocolRecordingExecutor(_query_response())

    with pytest.raises(ValueError, match="12 predicates"):
        FlowClient(executor).search(
            "invoice",
            partition_key="tenant-a",
            state_meta=OversizedMapping(),
        )

    assert executor.calls == []


def test_public_lineage_builder_rejects_noncanonical_selector() -> None:
    with pytest.raises(ValueError, match="lineage selector"):
        build_flow_lineage_query(
            "parent_flow_id = @lineage_id OR state",
            "parent-1",
            partition_key="tenant-a",
        )


def test_empty_nested_state_metadata_is_not_treated_as_a_search_predicate() -> None:
    executor = ProtocolRecordingExecutor(_query_response())

    with pytest.raises(ValueError, match=r"state_meta.*predicate|attribute.*predicate"):
        FlowClient(executor).search(
            "invoice",
            partition_key="tenant-a",
            state_meta={"queued": {}},
        )

    assert executor.calls == []


def test_telemetry_flow_query_has_an_unambiguous_name_and_legacy_alias() -> None:
    executor = ProtocolRecordingExecutor([], [])
    client = FlowClient(executor)

    assert client.telemetry_flow_query({"type": "invoice"}, state="queued") == []
    with pytest.deprecated_call(match="telemetry_flow_query"):
        assert client.flow_query({"type": "invoice"}, state="queued") == []

    expected = ("FERRICSTORE.TELEMETRY", "FLOW_QUERY", "TYPE", "invoice", "STATE", "queued")
    assert executor.calls == [expected, expected]


def test_async_telemetry_flow_query_has_an_unambiguous_name_and_legacy_alias() -> None:
    async def run() -> None:
        executor = AsyncProtocolRecordingExecutor([], [])
        client = AsyncFlowClient(executor)

        assert await client.telemetry_flow_query({"type": "invoice"}, state="queued") == []
        with pytest.deprecated_call(match="telemetry_flow_query"):
            assert await client.flow_query({"type": "invoice"}, state="queued") == []

    asyncio.run(run())


def test_generic_query_index_command_is_classified_as_a_safe_read() -> None:
    indexes = build_protocol_command("FLOW.QUERY.INDEXES")
    mutation = build_protocol_command("FERRICSTORE.TELEMETRY", "FLOW_QUERY")

    assert request_may_mutate(indexes.opcode, indexes.payload) is False
    assert request_may_mutate(mutation.opcode, mutation.payload) is True


def _pipeline_payload(*commands: tuple[Any, ...]) -> dict[str, Any]:
    prepared = [build_protocol_command(*command) for command in commands]
    return {
        "atomicity": "none",
        "commands": [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id,
                "request_id": index + 1,
                "body": command.payload,
            }
            for index, command in enumerate(prepared)
        ],
        "return": "compact",
    }


def test_structured_pipeline_is_safe_only_when_every_command_is_read_only() -> None:
    reads = _pipeline_payload(
        ("FLOW.QUERY", "FQL1", QUERY, "partition", "tenant-a"),
        ("FLOW.QUERY.INDEXES",),
    )
    mixed = _pipeline_payload(
        ("FLOW.QUERY", "FQL1", QUERY, "partition", "tenant-a"),
        ("SET", "key", "value"),
    )

    assert request_may_mutate(_OP_PIPELINE, reads) is False
    assert request_may_mutate(_OP_PIPELINE, mixed) is True
    assert request_may_mutate(_OP_PIPELINE, b"opaque compact pipeline") is True
    assert request_may_mutate(_OP_PIPELINE, {"commands": []}) is True
    assert request_may_mutate(_OP_PIPELINE, {"commands": [{"opcode": "bad"}]}) is True


def test_sync_read_only_pipeline_write_failure_is_safe_but_mixed_is_unknown(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailingSocket:
        def sendall(self, _data: bytes) -> None:
            raise OSError("pipeline write failed")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            pass

    def adapter() -> ProtocolAdapter:
        value = ProtocolAdapter(timeout=None, heartbeat_interval=None)
        value._sock = FailingSocket()  # type: ignore[assignment]
        return value

    with pytest.raises(FerricStoreError) as safe:
        adapter().execute_batch(
            [
                ("FLOW.QUERY", "FQL1", QUERY, "partition", "tenant-a"),
                ("FLOW.QUERY.INDEXES",),
            ]
        )
    assert not isinstance(safe.value, RequestOutcomeUnknownError)
    assert safe.value.safe_to_retry is True

    with pytest.raises(RequestOutcomeUnknownError):
        adapter().execute_batch(
            [
                ("FLOW.QUERY", "FQL1", QUERY, "partition", "tenant-a"),
                ("SET", "key", "value"),
            ]
        )


def test_async_read_only_pipeline_write_failure_is_safe_but_mixed_is_unknown() -> None:
    class FailingWriter:
        def __init__(self) -> None:
            self.closed = False

        def is_closing(self) -> bool:
            return self.closed

        def write(self, _part: bytes) -> None:
            pass

        async def drain(self) -> None:
            raise OSError("pipeline drain failed")

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            pass

    def adapter() -> AsyncProtocolAdapter:
        value = AsyncProtocolAdapter(timeout=None, heartbeat_interval=None, write_drain_bytes=0)
        value._writer = FailingWriter()  # type: ignore[assignment]
        value._connection_ready = True
        return value

    async def run() -> None:
        with pytest.raises(FerricStoreError) as safe:
            await adapter().execute_batch(
                [
                    ("FLOW.QUERY", "FQL1", QUERY, "partition", "tenant-a"),
                    ("FLOW.QUERY.INDEXES",),
                ]
            )
        assert not isinstance(safe.value, RequestOutcomeUnknownError)
        assert safe.value.safe_to_retry is True

        with pytest.raises(RequestOutcomeUnknownError):
            await adapter().execute_batch(
                [
                    ("FLOW.QUERY", "FQL1", QUERY, "partition", "tenant-a"),
                    ("SET", "key", "value"),
                ]
            )

    asyncio.run(run())


def test_sync_query_index_write_failure_remains_safe_to_retry(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailingSocket:
        def sendall(self, _data: bytes) -> None:
            raise OSError("query index write failed")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            pass

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    adapter._sock = FailingSocket()  # type: ignore[assignment]

    with pytest.raises(FerricStoreError, match="protocol write failed") as raised:
        adapter.execute_command("FLOW.QUERY.INDEXES")

    assert raised.value.safe_to_retry is True


def test_async_query_index_write_failure_remains_safe_to_retry() -> None:
    class FailingWriter:
        def __init__(self) -> None:
            self.closed = False

        def is_closing(self) -> bool:
            return self.closed

        def write(self, _part: bytes) -> None:
            pass

        async def drain(self) -> None:
            raise OSError("query index drain failed")

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            pass

    async def run() -> None:
        adapter = AsyncProtocolAdapter(timeout=None, heartbeat_interval=None, write_drain_bytes=0)
        adapter._writer = FailingWriter()  # type: ignore[assignment]
        adapter._connection_ready = True

        with pytest.raises(FerricStoreError, match="protocol write failed") as raised:
            await adapter.execute_command("FLOW.QUERY.INDEXES")

        assert raised.value.safe_to_retry is True

    asyncio.run(run())
