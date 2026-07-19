from __future__ import annotations

import asyncio
import inspect
import math
import struct
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import ferricstore
from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_queue_api import AsyncQueue, AsyncQueueClient, AsyncQueueFlow
from ferricstore.async_workflow_client import AsyncWorkflowClient
from ferricstore.async_workflow_context import AsyncWorkflowFlowCommands
from ferricstore.async_workflow_runtime import AsyncWorkflow
from ferricstore.backpressure import BackpressurePolicy
from ferricstore.client_autobatch import AutobatchFlowClient
from ferricstore.client_core import FlowClient
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
    OverloadedError,
    RequestOutcomeUnknownError,
)
from ferricstore.flow_routing import flow_command_route_keys
from ferricstore.protocol_codec import DecodeBudget
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_common import (
    RoutingTopology,
    _is_retryable_route_error,
    _server_allows_retry,
)
from ferricstore.protocol_constants import (
    _COMPACT_FLOW_RECORD,
    _COMPACT_KV_MGET,
    _COMPACT_OK_LIST,
    _COMPACT_PIPELINE_REQUEST,
    _FLAG_COMPRESSED,
    _FLAG_MORE_CHUNKS,
    _HEADER,
    _MAGIC,
    _OP_COMMAND_EXEC,
    _OP_MSET,
    _OPCODES,
    _REQUEST_VERSION,
    _RESPONSE_VERSION,
    _STATUS,
    _STATUS_OK,
)
from ferricstore.protocol_framing import ResponseFrameAssembler, ResponseIdentity
from ferricstore.protocol_negotiation import (
    MINIMUM_SERVER_VERSION,
    UNAUTHENTICATED_MAX_FRAME_BYTES,
    apply_hello_negotiation,
    parse_hello_capabilities,
)
from ferricstore.protocol_responses import _decode_protocol_response, _read_custom_flow_record
from ferricstore.protocol_retry import request_outcome_error
from ferricstore.protocol_sync import ProtocolAdapter
from ferricstore.protocol_sync_pool import ProtocolAdapterPool
from ferricstore.protocol_sync_topology import TopologyProtocolAdapterPool
from ferricstore.queue_api import Queue, QueueClient
from ferricstore.topology_core import route_for_keys
from ferricstore.types import ChildSpec, CreateItem, FencedItem, FlowRecord
from ferricstore.workflow_client import WorkflowClient
from ferricstore.workflow_models import WorkflowFlowCommands
from ferricstore.workflow_runtime import FlowWorkflow, Workflow


class RecordingExecutor:
    def __init__(self, responses: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.responses = responses or {}

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        if args[0] in {"FLOW.POLICY.SET", "FLOW.POLICY.GET"}:
            return {b"type": str(args[1]).encode(), b"generation": 1}
        return self.responses.get(str(args[0]), b"OK")


class AsyncRecordingExecutor(RecordingExecutor):
    async def execute_command(self, *args: Any) -> Any:
        return super().execute_command(*args)


class FailThenAckExecutor(RecordingExecutor):
    def __init__(self, error: FerricStoreError) -> None:
        super().__init__()
        self.error = error

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        if len(self.calls) == 1:
            raise self.error
        return b"OK"


def _hello(*, max_response_bytes: int = 4096) -> dict[str, Any]:
    return {
        "protocol": "ferricstore-native",
        "version": 1,
        "auth_required": True,
        "capabilities": {
            "limits": {"max_response_bytes": max_response_bytes},
            "response_codecs": {
                "compact_response_opcodes": {
                    "kv_mget_v1": [0x0104, 0x020C],
                    "flow_record_v1": [0x0202],
                    "unknown_future_codec_v1": [0x7FFE],
                }
            },
            "schemas": {"FLOW.POLICY.SET": {"fields": ["type", "expected_generation", "replace"]}},
        },
    }


def test_v091_declares_minimum_server_without_changing_wire_v1() -> None:
    assert ferricstore.MINIMUM_SERVER_VERSION == "0.9.1"
    assert MINIMUM_SERVER_VERSION == "0.9.1"
    assert _MAGIC == b"FSNP"
    assert _REQUEST_VERSION == 0x01
    assert _RESPONSE_VERSION == 0x81
    assert _OPCODES["MGET"] == 0x0104
    assert _OPCODES["FLOW.VALUE.MGET"] == 0x020C


def test_hello_drives_compact_codecs_and_response_limit() -> None:
    negotiated = parse_hello_capabilities(_hello(max_response_bytes=2048))

    assert negotiated.max_response_bytes == 2048
    assert negotiated.compact_response_codecs == {
        0x0104: "kv_mget_v1",
        0x020C: "kv_mget_v1",
        0x0202: "flow_record_v1",
        0x7FFE: "unknown_future_codec_v1",
    }
    assert negotiated.auth_required is True

    adapter = SimpleNamespace(
        _configured_max_response_bytes=8192,
        _configured_max_decompressed_response_bytes=16384,
    )
    apply_hello_negotiation(adapter, _hello(max_response_bytes=2048))
    assert adapter.max_response_bytes == 2048
    assert adapter.max_decompressed_response_bytes == 2048
    assert adapter._compact_response_codecs == negotiated.compact_response_codecs
    assert adapter._authenticated is False


def test_pre_091_hello_contract_is_rejected() -> None:
    with pytest.raises(FerricStoreError, match=r"0\.9\.1"):
        parse_hello_capabilities({"protocol": "ferricstore-native", "version": 1})


def test_compact_response_decoder_is_enabled_only_by_hello_capability() -> None:
    body = _STATUS.pack(_STATUS_OK) + bytes([_COMPACT_KV_MGET]) + struct.pack(">I", 1)
    body += b"\x01" + struct.pack(">I", 1) + b"v"
    common = {
        "lane_id": 1,
        "opcode": _OPCODES["MGET"],
        "request_id": 9,
        "flags": 0,
        "body": body,
        "read_started_ns": 1,
        "read_done_ns": 2,
    }
    negotiated = SimpleNamespace(
        _compact_response_codecs={_OPCODES["MGET"]: "kv_mget_v1"},
        max_decoded_collection_items=10,
        max_decompressed_response_bytes=1024,
        _pending_response_item_counts={9: 1},
    )
    response = _decode_protocol_response(negotiated, **common)
    assert response.value == [b"v"]

    unnegotiated = SimpleNamespace(
        _compact_response_codecs={},
        max_decoded_collection_items=10,
        max_decompressed_response_bytes=1024,
        _pending_response_item_counts={9: 1},
    )
    with pytest.raises(FerricStoreError):
        _decode_protocol_response(unnegotiated, **common)


def test_compact_mset_single_ack_is_normalized_to_ok_scalar() -> None:
    body = _STATUS.pack(_STATUS_OK) + bytes([_COMPACT_OK_LIST]) + struct.pack(">I", 1)
    adapter = SimpleNamespace(
        _compact_response_codecs={_OP_MSET: "ok_list_v1"},
        max_decoded_collection_items=10,
        max_decompressed_response_bytes=1024,
        _pending_response_item_counts={9: 1},
    )

    response = _decode_protocol_response(
        adapter,
        lane_id=1,
        opcode=_OP_MSET,
        request_id=9,
        flags=0,
        body=body,
        read_started_ns=1,
        read_done_ns=2,
    )

    assert response.value == b"OK"


def test_frame_assembler_supports_interleaved_identity_streams() -> None:
    assembler = ResponseFrameAssembler(max_body_bytes=8, max_chunks=3)
    first = ResponseIdentity(1, _OPCODES["MGET"], 10)
    second = ResponseIdentity(2, _OPCODES["FLOW.VALUE.MGET"], 11)

    assert assembler.add(first, _FLAG_MORE_CHUNKS, b"ab", read_started_ns=1) is None
    assert assembler.add(second, _FLAG_MORE_CHUNKS, b"12", read_started_ns=2) is None
    complete_first = assembler.add(first, _FLAG_COMPRESSED, b"cd", read_started_ns=3)
    complete_second = assembler.add(second, 0, b"34", read_started_ns=4)

    assert complete_first is not None
    assert complete_first.identity == first
    assert complete_first.flags == _FLAG_COMPRESSED
    assert complete_first.body == b"abcd"
    assert complete_first.read_started_ns == 1
    assert complete_second is not None
    assert complete_second.identity == second
    assert complete_second.body == b"1234"
    assert assembler.pending_count == 0


def test_frame_assembler_enforces_aggregate_limit_per_identity() -> None:
    assembler = ResponseFrameAssembler(max_body_bytes=3, max_chunks=3)
    identity = ResponseIdentity(1, _OPCODES["MGET"], 10)
    assert assembler.add(identity, _FLAG_MORE_CHUNKS, b"ab", read_started_ns=1) is None
    with pytest.raises(FerricStoreError, match="max_response_bytes"):
        assembler.add(identity, 0, b"cd", read_started_ns=2)
    assert assembler.pending_count == 0


def test_fetch_or_compute_uses_ownership_token_from_third_response_position() -> None:
    executor = RecordingExecutor(
        {"FETCH_OR_COMPUTE": [b"compute", b"expensive", b"ownership-token"]}
    )
    client = FlowClient(executor)

    result = client.fetch_or_compute("cache:key", ttl_ms=1000)

    assert result.should_compute
    assert result.hint == b"expensive"
    assert result.ownership_token == b"ownership-token"
    assert not hasattr(result, "compute_token")


def test_fetch_completion_apis_require_and_send_ownership_token() -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)

    assert client.fetch_or_compute_result("cache:key", b"ownership-token", b"value", ttl_ms=1000)
    assert executor.calls[-1] == (
        "FETCH_OR_COMPUTE_RESULT",
        "cache:key",
        b"ownership-token",
        b"value",
        1000,
    )
    assert client.fetch_or_compute_error("cache:key", b"ownership-token", "boom")
    assert executor.calls[-1] == (
        "FETCH_OR_COMPUTE_ERROR",
        "cache:key",
        b"ownership-token",
        "boom",
    )
    assert list(inspect.signature(client.fetch_or_compute_result).parameters) == [
        "key",
        "ownership_token",
        "value",
        "ttl_ms",
    ]


def test_published_fetch_completion_examples_use_ownership_token() -> None:
    repository = Path(__file__).resolve().parents[1]
    for relative_path in (
        "docs/client.md",
        "docs/sdk.md",
        "examples/protocol_commands.py",
    ):
        contents = (repository / relative_path).read_text()
        completion_count = contents.count("fetch_or_compute_result(") + contents.count(
            "fetch_or_compute_error("
        )
        assert completion_count, relative_path
        assert contents.count("result.ownership_token") >= completion_count, relative_path


def test_limit_release_requires_exact_server_reservation_ids() -> None:
    sync_parameters = inspect.signature(FlowClient.limit_release).parameters
    async_parameters = inspect.signature(AsyncFlowClient.limit_release).parameters
    assert "reservation_ids" in sync_parameters
    assert "reservation_ids" in async_parameters
    assert "amount" not in sync_parameters
    assert "amount" not in async_parameters

    executor = RecordingExecutor({"FLOW.LIMIT.RELEASE": {"released": 2}})
    client = FlowClient(executor)
    assert client.limit_release(
        "tenant-a",
        shard_id=0,
        reservation_ids=["flr1:1:batch:1", "flr1:1:batch:2"],
    ) == {"released": 2}
    assert executor.calls[-1] == (
        "FLOW.LIMIT.RELEASE",
        "tenant-a",
        "SHARD_ID",
        0,
        "RESERVATION_IDS",
        ["flr1:1:batch:1", "flr1:1:batch:2"],
    )


def test_async_fetch_completion_uses_same_token_contract() -> None:
    async def scenario() -> None:
        executor = AsyncRecordingExecutor(
            {"FETCH_OR_COMPUTE": [b"compute", b"hint", b"ownership-token"]}
        )
        client = AsyncFlowClient(executor)
        result = await client.fetch_or_compute("cache:key", ttl_ms=1000)
        assert result.ownership_token == b"ownership-token"
        await client.fetch_or_compute_error("cache:key", b"ownership-token", "boom")
        assert executor.calls[-1] == (
            "FETCH_OR_COMPUTE_ERROR",
            "cache:key",
            b"ownership-token",
            "boom",
        )

    asyncio.run(scenario())


@pytest.mark.parametrize("ownership_token", [b"", "token", None])
def test_fetch_completion_rejects_invalid_ownership_tokens_before_io(
    ownership_token: object,
) -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="ownership_token"):
        client.fetch_or_compute_result(  # type: ignore[arg-type]
            "cache:key",
            ownership_token,
            b"value",
            ttl_ms=1000,
        )
    with pytest.raises(ValueError, match="ownership_token"):
        client.fetch_or_compute_error(  # type: ignore[arg-type]
            "cache:key",
            ownership_token,
            "boom",
        )
    assert executor.calls == []


def test_async_fetch_completion_rejects_invalid_ownership_token_before_io() -> None:
    async def scenario() -> None:
        executor = AsyncRecordingExecutor()
        client = AsyncFlowClient(executor)
        with pytest.raises(ValueError, match="ownership_token"):
            await client.fetch_or_compute_error("cache:key", b"", "boom")
        assert executor.calls == []

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "value, wire",
    [(30_000, 30_000), (math.inf, "INFINITY"), ("infinity", "INFINITY")],
)
def test_max_active_ms_is_available_on_create_start_many_policy_and_children(
    value: int | float | str,
    wire: int | str,
) -> None:
    executor = RecordingExecutor(
        {
            "FLOW.START_AND_CLAIM": {
                b"id": b"flow",
                b"type": b"jobs",
                b"state": b"running",
                b"partition_key": b"tenant",
            }
        }
    )
    client = FlowClient(executor)

    client.create("flow", type="jobs", max_active_ms=value, now_ms=1)
    assert executor.calls[-1][executor.calls[-1].index("MAX_ACTIVE_MS") + 1] == wire
    client.start_and_claim(
        "flow",
        type="jobs",
        initial_state="running",
        worker="worker",
        max_active_ms=value,
        partition_key="tenant",
        now_ms=1,
    )
    assert executor.calls[-1][executor.calls[-1].index("MAX_ACTIVE_MS") + 1] == wire
    client.create_many(
        "tenant",
        [CreateItem("flow-2")],
        type="jobs",
        max_active_ms=value,
    )
    assert executor.calls[-1][executor.calls[-1].index("MAX_ACTIVE_MS") + 1] == wire
    client.install_policy("jobs", max_active_ms=value)
    assert executor.calls[-1][executor.calls[-1].index("MAX_ACTIVE_MS") + 1] == wire
    client.spawn_children(
        "parent",
        [ChildSpec("child", "jobs", max_active_ms=value)],
        partition_key="tenant",
        fencing_token=1,
        now_ms=1,
    )
    command = build_protocol_command(*executor.calls[-1])
    assert command.payload["children"][0]["max_active_ms"] == wire

    client.create_many(
        "tenant",
        [CreateItem("flow-3", max_active_ms=value)],
        type="jobs",
    )
    command = build_protocol_command(*executor.calls[-1])
    assert command.payload["items"][0]["max_active_ms"] == wire


@pytest.mark.parametrize("value", [True, 0, -1, 31_536_000_001, 1.5, "forever"])
def test_max_active_ms_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ValueError, match="max_active_ms"):
        FlowClient(RecordingExecutor()).create("flow", type="jobs", max_active_ms=value, now_ms=1)


def test_spawn_children_exposes_only_canonical_parent_flow_id() -> None:
    for owner in (
        FlowClient,
        AsyncFlowClient,
        WorkflowFlowCommands,
        AsyncWorkflowFlowCommands,
    ):
        parameters = inspect.signature(owner.spawn_children).parameters
        assert "parent_id" not in parameters
        assert "parent_flow_id" in parameters


def test_autobatch_create_exposes_max_active_ms() -> None:
    assert "max_active_ms" in inspect.signature(AutobatchFlowClient.create).parameters
    assert "max_active_ms" in inspect.signature(AutobatchFlowClient.create_async).parameters


@pytest.mark.parametrize(
    ("owner", "methods"),
    [
        (
            WorkflowFlowCommands,
            (
                "create",
                "enqueue",
                "start_and_claim",
                "create_many",
                "enqueue_many",
                "spawn_children",
            ),
        ),
        (
            AsyncWorkflowFlowCommands,
            (
                "create",
                "enqueue",
                "start_and_claim",
                "create_many",
                "enqueue_many",
                "spawn_children",
            ),
        ),
        (Workflow, ("create", "enqueue", "start_and_claim", "create_many", "spawn_children")),
        (FlowWorkflow, ("start",)),
        (AsyncWorkflow, ("enqueue", "start_flow", "enqueue_many")),
        (Queue, ("enqueue", "enqueue_many")),
        (AsyncQueue, ("enqueue", "enqueue_many")),
        (AsyncQueueFlow, ("enqueue", "enqueue_many")),
    ],
)
def test_high_level_flow_creation_surfaces_expose_max_active_ms(
    owner: type[Any], methods: tuple[str, ...]
) -> None:
    for method in methods:
        assert "max_active_ms" in inspect.signature(getattr(owner, method)).parameters


@pytest.mark.parametrize(
    "owner",
    [
        FlowClient,
        AsyncFlowClient,
        WorkflowFlowCommands,
        AsyncWorkflowFlowCommands,
        WorkflowClient,
        AsyncWorkflowClient,
        Workflow,
        AsyncWorkflow,
        Queue,
        AsyncQueue,
        QueueClient,
        AsyncQueueClient,
    ],
)
def test_all_type_policy_surfaces_expose_max_active_ms(owner: type[Any]) -> None:
    assert "max_active_ms" in inspect.signature(owner.install_policy).parameters


def test_sync_high_level_type_policy_surfaces_forward_max_active_ms() -> None:
    executor = RecordingExecutor()
    flow = FlowClient(executor)

    WorkflowClient(flow).install_policy("workflow-client", max_active_ms="infinity")
    Queue(flow, type="queue").install_policy(max_active_ms=1_000)
    QueueClient(flow).install_policy("queue-client", max_active_ms=2_000)

    assert executor.calls[0][-2:] == ("MAX_ACTIVE_MS", "INFINITY")
    assert executor.calls[1][-2:] == ("MAX_ACTIVE_MS", 1_000)
    assert executor.calls[2][-2:] == ("MAX_ACTIVE_MS", 2_000)


def test_flow_record_decodes_max_active_failure_reason() -> None:
    record = FlowRecord.from_resp(
        {
            b"id": b"flow",
            b"type": b"jobs",
            b"state": b"failed",
            b"partition_key": b"tenant",
            b"max_active_ms": 100,
            b"error": {b"reason": b"max_active_ms"},
        }
    )

    assert record.max_active_ms == 100
    assert record.error == {"reason": "max_active_ms"}
    assert record.failure_reason == "max_active_ms"


def test_compact_flow_record_skips_unknown_numeric_extension() -> None:
    binary = bytearray([_COMPACT_FLOW_RECORD])
    binary.extend(struct.pack(">I", 4))
    for field_id, value in ((1, b"flow"), (2, b"jobs"), (3, b"queued")):
        binary.append(field_id)
        binary.append(4)
        binary.extend(struct.pack(">I", len(value)))
        binary.extend(value)
    binary.append(250)
    binary.append(3)
    binary.extend(struct.pack(">q", 42))

    decoded, end = _read_custom_flow_record(bytes(binary), 0, budget=DecodeBudget(100))
    assert end == len(binary)
    assert decoded == {b"id": b"flow", b"type": b"jobs", b"state": b"queued"}


def test_lineage_payloads_use_only_canonical_names() -> None:
    command = build_protocol_command(
        "FLOW.CREATE",
        "child",
        "TYPE",
        "jobs",
        "PARENT_FLOW_ID",
        "parent",
        "ROOT_FLOW_ID",
        "root",
    )
    assert command.payload["parent_flow_id"] == "parent"
    assert command.payload["root_flow_id"] == "root"
    assert "parent_id" not in command.payload
    assert "root_id" not in command.payload
    parent_query = build_protocol_command("FLOW.BY_PARENT", "parent")
    root_query = build_protocol_command("FLOW.BY_ROOT", "root")
    assert parent_query.opcode == _OP_COMMAND_EXEC
    assert parent_query.payload == {"command": "FLOW.BY_PARENT", "args": ["parent"]}
    assert root_query.opcode == _OP_COMMAND_EXEC
    assert root_query.payload == {"command": "FLOW.BY_ROOT", "args": ["root"]}
    assert "parent_id" not in str(parent_query.payload)
    assert "root_id" not in str(root_query.payload)
    with pytest.raises(InvalidCommandError):
        build_protocol_command("FLOW.CREATE", "child", "TYPE", "jobs", "PARENT_ID", "parent")


def test_topk_reserve_has_no_decay_escape_hatch() -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)
    client.topk_reserve("top", 10, width=20, depth=5)
    assert executor.calls[-1] == ("TOPK.RESERVE", "top", 10, 20, 5)
    with pytest.raises(TypeError):
        client.topk_reserve("top", 10, 20, 5, 0.9)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "args",
    [
        ("FLOW.COMPLETE", "flow", b"lease"),
        ("FLOW.RETRY", "flow", b"lease"),
        ("FLOW.FAIL", "flow", b"lease"),
        ("FLOW.TRANSITION", "flow", "queued", "running", "FENCING", 1),
        ("FLOW.CANCEL", "flow"),
    ],
)
def test_flow_mutation_protocol_schemas_reject_missing_required_guards(
    args: tuple[Any, ...],
) -> None:
    with pytest.raises(InvalidCommandError, match=r"token|fencing"):
        build_protocol_command(*args)


def test_transition_many_requires_each_lease_token() -> None:
    client = FlowClient(RecordingExecutor())
    with pytest.raises(ValueError, match="lease_token"):
        client.transition_many(
            "tenant",
            from_state="queued",
            to_state="running",
            items=[FencedItem("flow", 1)],
            now_ms=1,
        )


def test_flow_signal_native_payload_uses_id_and_signal() -> None:
    command = build_protocol_command("FLOW.SIGNAL", "flow", "SIGNAL", "approved")
    assert command.payload == {"id": "flow", "signal": "approved"}


def test_flow_effect_routing_prefers_partition_then_flow_id() -> None:
    explicit = flow_command_route_keys(
        "FLOW.EFFECT.RESERVE",
        ("flow", "PARTITION", "tenant", "EFFECT_KEY", "email"),
    )
    by_id = flow_command_route_keys(
        "FLOW.EFFECT.RESERVE",
        ("flow", "EFFECT_KEY", "email"),
    )
    assert explicit and explicit != by_id
    assert by_id


def test_topology_does_not_swallow_cross_slot_mset_validation() -> None:
    pool = object.__new__(TopologyProtocolAdapterPool)
    pool._prepare_routed_command = lambda args: SimpleNamespace(
        args=args,
        command=build_protocol_command(*args),
    )
    with pytest.raises(InvalidCommandError, match="same slot"):
        pool._route_data(("MSET", "{a}:one", b"1", "{b}:two", b"2"))
    with pytest.raises(InvalidCommandError, match="same slot"):
        pool._route_data(("MSETNX", "{a}:one", b"1", "{b}:two", b"2"))


def test_clients_reject_cross_slot_mset_before_executor_submission() -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)
    with pytest.raises(InvalidCommandError, match="same slot"):
        client.mset({"{a}:one": b"1", "{b}:two": b"2"}, encode=False)
    with pytest.raises(InvalidCommandError, match="same slot"):
        client.msetnx({"{a}:one": b"1", "{b}:two": b"2"}, encode=False)
    assert executor.calls == []


def test_server_retry_metadata_is_preserved_and_controls_replay() -> None:
    error = OverloadedError(
        "busy",
        retryable=True,
        safe_to_retry=False,
        retry_after_ms=25,
        reason="busy",
    )
    assert error.retryable is True
    assert error.safe_to_retry is False
    assert error.retry_after_ms == 25


def test_producer_replays_only_when_server_marks_retryable_and_safe() -> None:
    policy = BackpressurePolicy(
        max_retries=1,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )
    safe_executor = FailThenAckExecutor(
        OverloadedError(
            "busy",
            retryable=True,
            safe_to_retry=True,
            retry_after_ms=0,
        )
    )
    assert (
        FlowClient(safe_executor, backpressure=policy).enqueue(
            "flow",
            type="jobs",
            now_ms=1,
        )
        == b"OK"
    )
    assert len(safe_executor.calls) == 2

    for retryable, safe_to_retry in ((True, False), (False, True), (None, None)):
        unsafe_executor = FailThenAckExecutor(
            OverloadedError(
                "busy",
                retryable=retryable,
                safe_to_retry=safe_to_retry,
                retry_after_ms=0,
            )
        )
        with pytest.raises(OverloadedError):
            FlowClient(unsafe_executor, backpressure=policy).enqueue(
                "flow",
                type="jobs",
                now_ms=1,
            )
        assert len(unsafe_executor.calls) == 1


def test_explicit_retry_metadata_overrides_route_error_message_guessing() -> None:
    assert not _is_retryable_route_error(
        FerricStoreError("connection closed", retryable=False, safe_to_retry=False)
    )
    assert _is_retryable_route_error(
        FerricStoreError("reroute", retryable=True, safe_to_retry=True)
    )


def test_server_safety_metadata_is_required_before_replay() -> None:
    assert _server_allows_retry(FerricStoreError("reroute", retryable=True, safe_to_retry=True))
    assert not _server_allows_retry(
        FerricStoreError("reroute", retryable=True, safe_to_retry=False)
    )
    assert not _server_allows_retry(FerricStoreError("reroute", retryable=True, safe_to_retry=None))
    assert _server_allows_retry(OSError("disconnected"))


def test_mutation_transport_failure_has_unknown_outcome_but_read_does_not() -> None:
    cause = TimeoutError("socket timed out")
    mutation = request_outcome_error(_OPCODES["SET"], cause)
    read = request_outcome_error(_OPCODES["GET"], cause)

    assert isinstance(mutation, RequestOutcomeUnknownError)
    assert mutation.retryable is False
    assert mutation.safe_to_retry is False
    assert mutation.raw is cause
    assert type(read) is FerricStoreError
    assert read.safe_to_retry is True


def test_fetch_or_compute_transport_failure_has_unknown_ownership_outcome() -> None:
    error = request_outcome_error(
        _OPCODES["FETCH_OR_COMPUTE"],
        TimeoutError("socket timed out"),
    )

    assert isinstance(error, RequestOutcomeUnknownError)
    assert error.retryable is False
    assert error.safe_to_retry is False


def test_auto_flow_operations_never_construct_reserved_partition_keys() -> None:
    executor = RecordingExecutor(
        {
            "FLOW.CREATE_MANY": [b"OK", b"OK"],
            "FLOW.COMPLETE": b"OK",
            "FLOW.GET": {
                b"id": b"flow-a",
                b"type": b"jobs",
                b"state": b"complete",
                b"partition_key": b"server-owned",
            },
        }
    )
    client = FlowClient(executor)

    assert client.enqueue_many(
        [CreateItem("flow-a"), CreateItem("flow-b")],
        type="jobs",
        now_ms=1,
    ) == [b"OK", b"OK"]
    create_calls = [call for call in executor.calls if call[0] == "FLOW.CREATE_MANY"]
    assert len(create_calls) == 1
    assert create_calls[0][1] == "AUTO"
    assert "__flow_auto__" not in repr(create_calls)

    client.complete(
        "flow-a",
        lease_token=b"lease",
        fencing_token=1,
        now_ms=1,
        return_record=True,
    )
    get_call = next(call for call in executor.calls if call[0] == "FLOW.GET")
    assert "PARTITION" not in get_call


def _compact_mset_payload(keys: list[str], value: bytes = b"value") -> bytes:
    parts = [struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 1, len(keys))]
    for key in keys:
        encoded = key.encode()
        parts.extend(
            [
                struct.pack(">I", len(encoded)),
                encoded,
                struct.pack(">I", len(value)),
                value,
            ]
        )
    return b"".join(parts)


def test_optimized_mset_submissions_reject_cross_slot_keys_before_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class NoSendSocket:
        def __init__(self) -> None:
            self.sent: list[bytes] = []

        def sendall(self, data: bytes) -> None:
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388)
    socket = NoSendSocket()
    adapter._sock = socket
    cross_slot_keys = ["{tenant-a}:one", "{tenant-b}:two"]
    payload = _compact_mset_payload(cross_slot_keys)

    with pytest.raises(InvalidCommandError, match="same slot"):
        adapter.submit_mset_same_value(cross_slot_keys, b"value")
    with pytest.raises(InvalidCommandError, match="same slot"):
        adapter.submit_mset_payload(payload)
    with pytest.raises(InvalidCommandError, match="same slot"):
        adapter.submit_mset_payload_on_lane(payload, 1)
    with pytest.raises(InvalidCommandError, match="same slot"):
        ProtocolAdapterPool([adapter]).submit_mset_same_value(cross_slot_keys, b"value")

    assert socket.sent == []


def test_same_value_mset_reuses_key_validation_for_generated_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = object.__new__(ProtocolAdapter)
    submitted: list[bytes] = []
    result: Future[Any] = Future()
    result.set_result(b"OK")

    monkeypatch.setattr(
        "ferricstore.protocol_sync._validate_compact_mset_payload",
        lambda _payload: (_ for _ in ()).throw(
            AssertionError("SDK-generated compact MSET payload must not be parsed again")
        ),
    )
    monkeypatch.setattr(
        adapter,
        "_submit_validated_mset_payload",
        lambda payload: submitted.append(payload) or result,
    )

    assert adapter.submit_mset_same_value(["{tenant}:one", "{tenant}:two"], b"value") is result
    assert submitted == [_compact_mset_payload(["{tenant}:one", "{tenant}:two"])]


def test_topology_mset_payload_routes_from_locally_validated_keys() -> None:
    keys = ["{tenant}:one", "{tenant}:two"]
    payload = _compact_mset_payload(keys)
    result: Future[Any] = Future()
    result.set_result(b"OK")
    routed: dict[str, Any] = {}

    class EndpointAdapter:
        def submit_mset_payload_on_lane(self, value: bytes, lane_id: int) -> Future[Any]:
            routed["submitted"] = (value, lane_id)
            return result

    lease = object()
    pool = SimpleNamespace()

    def leased_target(route_keys: tuple[bytes, ...]) -> tuple[SimpleNamespace, object]:
        routed["keys"] = route_keys
        return SimpleNamespace(adapter=EndpointAdapter(), lane_id=7), lease

    pool._leased_batch_target_for_keys = leased_target
    pool._release_adapter_lease = lambda value: routed.setdefault("released", value)

    assert TopologyProtocolAdapterPool.submit_mset_payload(pool, payload) is result
    assert routed == {
        "keys": tuple(key.encode() for key in keys),
        "submitted": (payload, 7),
        "released": lease,
    }


def test_topology_mset_payload_reuses_validation_for_endpoint_submission() -> None:
    payload = _compact_mset_payload(["{tenant}:one", "{tenant}:two"])
    result: Future[Any] = Future()
    result.set_result(b"OK")
    routed: dict[str, Any] = {}

    class EndpointAdapter:
        def _submit_validated_mset_payload_on_lane(
            self,
            value: bytes,
            lane_id: int,
        ) -> Future[Any]:
            routed["submitted"] = (value, lane_id)
            return result

        def submit_mset_payload_on_lane(self, _value: bytes, _lane_id: int) -> Future[Any]:
            raise AssertionError("topology validation must not parse a large MSET payload twice")

    lease = object()
    pool = SimpleNamespace(
        _leased_batch_target_for_keys=lambda _keys: (
            SimpleNamespace(adapter=EndpointAdapter(), lane_id=3),
            lease,
        ),
        _release_adapter_lease=lambda value: routed.setdefault("released", value),
    )

    assert TopologyProtocolAdapterPool.submit_mset_payload(pool, payload) is result
    assert routed == {"submitted": (payload, 3), "released": lease}


def test_topology_mset_payload_hashes_each_key_once(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = ["{tenant}:one", "{tenant}:two"]
    payload = _compact_mset_payload(keys)
    result: Future[Any] = Future()
    result.set_result(b"OK")
    hashed: list[str | bytes] = []
    original_slot_for_key = RoutingTopology.slot_for_key

    def counted_slot_for_key(key: str | bytes) -> int:
        hashed.append(key)
        return original_slot_for_key(key)

    monkeypatch.setattr(RoutingTopology, "slot_for_key", staticmethod(counted_slot_for_key))

    class EndpointAdapter:
        def _submit_validated_mset_payload_on_lane(
            self,
            _payload: bytes,
            _lane_id: int,
        ) -> Future[Any]:
            return result

    def leased_target(route_keys: tuple[bytes, ...]) -> tuple[SimpleNamespace, object]:
        route_for_keys(
            route_keys,
            slot_for_key=RoutingTopology.slot_for_key,
        ).require_routable_key()
        return SimpleNamespace(adapter=EndpointAdapter(), lane_id=3), object()

    pool = SimpleNamespace(
        _leased_batch_target_for_keys=leased_target,
        _release_adapter_lease=lambda _lease: None,
    )

    assert TopologyProtocolAdapterPool.submit_mset_payload(pool, payload) is result
    assert hashed == [key.encode() for key in keys]


def test_topology_mset_payload_rejects_cross_slot_keys_before_submission() -> None:
    payload = _compact_mset_payload(["{tenant-a}:one", "{tenant-b}:two"])
    submitted: list[bytes] = []

    def leased_target(route_keys: tuple[bytes, ...]) -> tuple[SimpleNamespace, object]:
        route_for_keys(
            route_keys,
            slot_for_key=RoutingTopology.slot_for_key,
        ).require_routable_key()
        submitted.append(payload)
        return SimpleNamespace(), object()

    pool = SimpleNamespace(
        _leased_batch_target_for_keys=leased_target,
        _release_adapter_lease=lambda _lease: None,
    )

    with pytest.raises(InvalidCommandError, match="same slot"):
        TopologyProtocolAdapterPool.submit_mset_payload(pool, payload)
    assert submitted == []


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        struct.pack(">BBI", 0, 1, 1),
        struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0, 1),
        struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 1, 0),
        struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 1, 1),
        struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 1, 1) + struct.pack(">I", 2) + b"k",
        struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 1, 1) + struct.pack(">I", 1) + b"k",
        struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 1, 1)
        + struct.pack(">I", 1)
        + b"k"
        + struct.pack(">I", 2)
        + b"v",
        _compact_mset_payload(["{tenant}:one"]) + b"trailing",
        bytearray(_compact_mset_payload(["{tenant}:one"])),
    ],
    ids=[
        "empty",
        "marker",
        "mode",
        "zero-items",
        "missing-key-length",
        "truncated-key",
        "missing-value-length",
        "truncated-value",
        "trailing-data",
        "non-bytes",
    ],
)
def test_optimized_mset_payload_rejects_malformed_data_before_io(payload: object) -> None:
    adapter = object.__new__(ProtocolAdapter)

    with pytest.raises(InvalidCommandError, match="valid compact key/value payload"):
        adapter.submit_mset_payload(payload)  # type: ignore[arg-type]


def test_published_examples_do_not_mirror_private_flow_partitioning() -> None:
    examples = Path(__file__).resolve().parents[1] / "examples"
    private_tokens = (
        "__flow_auto__",
        "AUTO_PARTITION_BUCKETS",
        "auto_partition_index_for_flow_id",
        "auto_partition_server_shard_for_index",
        "fa:",
    )
    offenders = {
        path.name: [token for token in private_tokens if token in source]
        for path in examples.glob("*.py")
        if any(token in (source := path.read_text(encoding="utf-8")) for token in private_tokens)
    }

    assert offenders == {}


@pytest.mark.parametrize(
    "method, kwargs",
    [
        ("complete", {"lease_token": b"", "fencing_token": 1}),
        ("retry", {"lease_token": b"", "fencing_token": 1}),
        ("fail", {"lease_token": b"", "fencing_token": 1}),
        (
            "transition",
            {
                "from_state": "running",
                "to_state": "complete",
                "lease_token": b"",
                "fencing_token": 1,
            },
        ),
        ("cancel", {"fencing_token": -1}),
    ],
)
def test_high_level_flow_mutations_validate_required_guards(
    method: str,
    kwargs: dict[str, Any],
) -> None:
    executor = RecordingExecutor()
    with pytest.raises(ValueError, match=r"lease_token|fencing_token"):
        getattr(FlowClient(executor), method)("flow", now_ms=1, **kwargs)
    assert executor.calls == []


def test_unauthenticated_frame_limit_is_explicit_security_contract() -> None:
    assert UNAUTHENTICATED_MAX_FRAME_BYTES == 64 * 1024
    adapter = SimpleNamespace(_auth_required=True, _authenticated=False)
    from ferricstore.protocol_negotiation import validate_unauthenticated_request_size

    validate_unauthenticated_request_size(adapter, 64 * 1024)
    with pytest.raises(FerricStoreError, match="authenticate"):
        validate_unauthenticated_request_size(adapter, 64 * 1024 + 1)


def test_wire_header_layout_remains_v1() -> None:
    header = _HEADER.pack(_MAGIC, _REQUEST_VERSION, 0, 1, _OPCODES["MGET"], 7, 0)
    assert header[:5] == b"FSNP\x01"
