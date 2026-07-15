from __future__ import annotations

import asyncio
import struct
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, FlowClient, QueueFlowWorker, Worker, WorkflowWorker
from ferricstore.batch_core import SyncFanoutExecutor, run_async_fanout, run_sync_fanout
from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import close_resources_async
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_compact_commands import _compact_pipeline_payload_from_raw
from ferricstore.protocol_constants import (
    _COMPACT_PIPELINE_REQUEST,
    _OP_FLOW_TRANSITION_MANY,
)
from ferricstore.protocol_pipeline_codec import (
    _compact_pipeline_payload,
    _expected_payload_collection_items,
)
from ferricstore.protocol_response_contracts import validate_response_cardinality
from ferricstore.protocol_responses import _batch_item_value
from ferricstore.protocol_sync import ProtocolAdapter
from ferricstore.types import resolve_worker_connection_counts


class _SyncExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> dict[str, Any]:
        self.calls.append(args)
        return {}

    def close(self) -> None:
        pass


class _AsyncExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def execute_command(self, *args: Any) -> dict[str, Any]:
        self.calls.append(args)
        return {}


@pytest.mark.parametrize("assignees", ["ops", b"ops", ["ops", ""]])
def test_approval_request_rejects_invalid_assignee_sequences(assignees: Any) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="assignees"):
        client.approval_request(
            "approval-1",
            flow_id="flow-1",
            scope="tenant-a",
            assignees=assignees,
        )

    assert executor.calls == []


@pytest.mark.parametrize("assignees", ["ops", b"ops", ["ops", ""]])
def test_async_approval_request_rejects_invalid_assignee_sequences(assignees: Any) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match="assignees"):
            await client.approval_request(
                "approval-1",
                flow_id="flow-1",
                scope="tenant-a",
                assignees=assignees,
            )

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize("invalid", [True, 1.5, "2"])
def test_fanout_concurrency_rejects_non_integer_values(invalid: Any) -> None:
    with pytest.raises(ValueError, match="max_concurrency"):
        SyncFanoutExecutor(max_concurrency=invalid)
    with pytest.raises(ValueError, match="max_concurrency"):
        run_sync_fanout([1, 2], lambda value: value, concurrent=True, max_concurrency=invalid)

    async def run() -> None:
        async def identity(value: int) -> int:
            return value

        with pytest.raises(ValueError, match="max_concurrency"):
            await run_async_fanout(
                [1, 2],
                identity,
                concurrent=True,
                max_concurrency=invalid,
            )

    asyncio.run(run())


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("workers", True),
        ("concurrency", 1.5),
        ("command_connections", True),
        ("claim_connections", 1.5),
    ],
)
def test_worker_connection_counts_reject_non_integer_values(name: str, value: Any) -> None:
    kwargs = {name: value}

    with pytest.raises(ValueError, match=name):
        resolve_worker_connection_counts(**kwargs)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("claim_drain_batches", True),
        ("claim_prefetch", 1.5),
        ("complete_async_depth", True),
        ("claim_scan_block_ms", 1.5),
    ],
)
def test_queue_worker_rejects_non_integer_runtime_knobs(name: str, value: Any) -> None:
    client = FlowClient(_SyncExecutor())

    with pytest.raises(ValueError, match=name):
        QueueFlowWorker(client, type="email", **{name: value})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("batch_size", True),
        ("claim_partition_batch_size", 1.5),
        ("block_ms", True),
        ("apply_async_depth", 1.5),
    ],
)
def test_workflow_worker_rejects_non_integer_runtime_knobs(name: str, value: Any) -> None:
    workflow = type("FakeWorkflow", (), {"type": "email", "_states": {"queued": object()}})()

    with pytest.raises(ValueError, match=name):
        WorkflowWorker(workflow, **{name: value})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("limit", True),
        ("states", "queued"),
        ("partial_retries", 1.5),
        ("partial_retry_delay_s", float("nan")),
    ],
)
def test_legacy_worker_rejects_invalid_runtime_knobs(name: str, value: Any) -> None:
    workflow = type("FakeWorkflow", (), {"_states": {"queued": object()}})()

    with pytest.raises(ValueError, match=name):
        Worker(workflow, worker="worker-1", **{name: value})


@pytest.mark.parametrize("invalid", [True, 1.5, "2"])
def test_async_cleanup_concurrency_rejects_non_integer_values(invalid: Any) -> None:
    async def run() -> None:
        with pytest.raises(ValueError, match="max_concurrency"):
            await close_resources_async([], max_concurrency=invalid)

    asyncio.run(run())


@pytest.mark.parametrize("invalid", [True, 1.5, "2"])
def test_direct_protocol_batch_counts_reject_non_integer_values(invalid: Any) -> None:
    adapter = object.__new__(ProtocolAdapter)

    with pytest.raises(ValueError, match="count"):
        adapter.submit_pipeline_payload(b"payload", invalid)
    with pytest.raises(ValueError, match="count"):
        adapter.submit_flow_many_payload("FLOW.COMPLETE_MANY", b"payload", invalid)


def test_transition_many_response_enforces_request_cardinality() -> None:
    with pytest.raises(FerricStoreError, match="returned 1 items; expected 2"):
        validate_response_cardinality(_OP_FLOW_TRANSITION_MANY, [b"only"], 2)

    validate_response_cardinality(_OP_FLOW_TRANSITION_MANY, b"OK", 2)


def test_generic_transition_many_payload_registers_request_cardinality() -> None:
    payload = {"type": "email", "items": [{"id": "one"}, {"id": "two"}]}

    assert _expected_payload_collection_items(_OP_FLOW_TRANSITION_MANY, payload) == 2


def test_malformed_pipeline_status_is_reported_as_protocol_error() -> None:
    with pytest.raises(FerricStoreError, match="status"):
        _batch_item_value([b"\xff", b"value"])


@pytest.mark.parametrize(("command", "mode"), [("SREM", 31), ("ZREM", 32)])
def test_kv_remove_batches_use_server_compact_pipeline_modes(command: str, mode: int) -> None:
    raw_commands = [(command, "key-1", "member-1"), (command, b"key-2", b"member-2")]
    raw_payload = _compact_pipeline_payload_from_raw(raw_commands, values_only=True)

    assert raw_payload is not None
    assert raw_payload[:6] == struct.pack(
        ">BBI",
        _COMPACT_PIPELINE_REQUEST,
        0x80 | mode,
        2,
    )

    built_payload = _compact_pipeline_payload(
        [build_protocol_command(*item) for item in raw_commands],
        values_only=True,
    )
    assert built_payload == raw_payload
