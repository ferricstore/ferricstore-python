from __future__ import annotations

import asyncio
import zlib
from types import SimpleNamespace

import pytest

from ferricstore import FlowClient
from ferricstore.async_workflow_context import AsyncWorkflowEffect
from ferricstore.batch_core import batch_fingerprint, queued_batch_fingerprint
from ferricstore.errors import InvalidCommandError
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_compact_commands import _compact_pipeline_payload_from_raw
from ferricstore.protocol_framing import decompress_response
from ferricstore.protocol_pipeline_codec import _compact_pipeline_payload
from ferricstore.types import ApprovalResult, CircuitBreakerStatus, EffectResult
from ferricstore.workflow_models import WorkflowEffect, WorkflowFlowCommands


def test_native_limit_release_preserves_kv_reservation_ids() -> None:
    command = build_protocol_command(
        "FLOW.LIMIT.RELEASE",
        "tenant-a",
        "SHARD_ID",
        2,
        "RESERVATION_IDS",
        ["reservation-1", "reservation-2"],
    )

    assert command.payload == {
        "scope": "tenant-a",
        "shard_id": 2,
        "reservation_ids": ["reservation-1", "reservation-2"],
    }


def test_governance_results_preserve_numeric_kv_policy_versions() -> None:
    effect = EffectResult.from_resp({b"policy_version": 7})
    approval = ApprovalResult.from_resp({b"policy_version": 8})

    assert effect.policy_version == 7
    assert approval.policy_version == 8


def test_circuit_result_decodes_the_complete_kv_public_view() -> None:
    circuit = CircuitBreakerStatus.from_resp(
        {
            b"scope": b"payments",
            b"status": b"half_open",
            b"half_open_started_at_ms": 1_500,
            b"event_count": 9,
        }
    )

    assert circuit.half_open_started_at_ms == 1_500
    assert circuit.event_count == 9


def test_schedule_rejects_a_target_priority_that_kv_cannot_create() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def execute_command(self, *args: object) -> dict[str, object]:
            self.calls.append(args)
            return {}

    executor = Executor()
    client = FlowClient(executor)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="target priority cannot exceed 2"):
        client.schedule_create(
            "invalid-priority",
            target={"type": "task", "priority": 3},
        )

    assert executor.calls == []


def test_batch_fingerprints_preserve_unordered_value_multiplicity() -> None:
    one_nan = frozenset({float("nan")})
    two_nans = frozenset({float("nan"), float("nan")})

    assert len(one_nan) == 1
    assert len(two_nans) == 2
    assert batch_fingerprint(one_nan) != batch_fingerprint(two_nans)
    assert queued_batch_fingerprint(one_nan) != queued_batch_fingerprint(two_nans)


def test_flow_query_integer_overflow_is_rejected_before_encoding() -> None:
    with pytest.raises(InvalidCommandError, match="signed 64-bit"):
        build_protocol_command(
            "FLOW.QUERY",
            "FQL1",
            "FROM runs WHERE partition_key = @partition RETURN COUNT",
            "partition",
            2**63,
        )


def test_compact_range_integer_overflow_falls_back_to_generic_encoding() -> None:
    commands = [("LRANGE", "jobs", 2**63, -1)]
    protocol_commands = [build_protocol_command(*commands[0])]

    assert _compact_pipeline_payload_from_raw(commands, values_only=True) is None
    assert _compact_pipeline_payload(protocol_commands, values_only=True) is None


def test_decompression_accepts_a_limit_larger_than_the_c_size_type() -> None:
    payload = b"small response"

    assert decompress_response(zlib.compress(payload), 10**100) == payload


def test_sync_workflow_commands_preserve_explicit_falsy_overrides() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

        def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
            def record(*args: object, **kwargs: object) -> object:
                self.calls.append((name, args, kwargs))
                return object()

            return record

    client = Recorder()
    workflow = SimpleNamespace(client=client, type="current-type", initial_state="queued")
    context = SimpleNamespace(
        workflow=workflow,
        id="current-id",
        state="current-state",
        partition_key="current-partition",
        lease_token=b"current-lease",
        fencing_token=7,
        root_flow_id="current-root",
        correlation_id="current-correlation",
    )
    flow = WorkflowFlowCommands(context)  # type: ignore[arg-type]

    flow.create("new", type="", state="")
    flow.start_and_claim("new", type="", initial_state="", worker="worker")
    flow.get("")
    flow.history("")
    flow.signal("")
    flow.extend_lease("", b"")
    flow.transition("next", id="", from_state="", lease_token=b"")
    flow.step_continue("next", id="", from_state="", lease_token=b"")
    flow.complete("", lease_token=b"")
    flow.retry("", lease_token=b"")
    flow.fail("", lease_token=b"")
    flow.cancel("")
    flow.rewind("")
    flow.by_parent("")
    flow.by_root("")
    flow.by_correlation("")
    flow.spawn_children([], parent_flow_id="")

    calls = {name: (args, kwargs) for name, args, kwargs in client.calls}
    assert calls["create"][1]["type"] == ""
    assert calls["create"][1]["state"] == ""
    assert calls["start_and_claim"][1]["type"] == ""
    assert calls["start_and_claim"][1]["initial_state"] == ""
    for name in ("get", "history", "signal", "cancel", "rewind", "by_parent", "by_root"):
        assert calls[name][0][0] == ""
    assert calls["by_correlation"][0][0] == ""
    assert calls["spawn_children"][0][0] == ""
    assert calls["extend_lease"][0][:2] == ("", b"")
    for name in ("transition", "step_continue"):
        assert calls[name][0][0] == ""
        assert calls[name][1]["from_state"] == ""
        assert calls[name][1]["lease_token"] == b""
    for name in ("complete", "retry", "fail"):
        assert calls[name][0][0] == ""
        assert calls[name][1]["lease_token"] == b""


def test_workflow_effect_does_not_replace_an_explicit_invalid_digest() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def execute_command(self, *args: object) -> dict[str, object]:
            self.calls.append(args)
            return {}

    executor = Executor()
    context = SimpleNamespace(
        client=FlowClient(executor),  # type: ignore[arg-type]
        id="flow",
        partition_key="tenant",
        lease_token=b"lease",
        fencing_token=1,
    )
    effect = WorkflowEffect(
        context,  # type: ignore[arg-type]
        "charge",
        "payment",
        operation_digest="",
    )

    with pytest.raises(ValueError, match="operation_digest"):
        effect.reserve()

    assert executor.calls == []


def test_effect_auto_external_id_ignores_non_utf8_binary_results() -> None:
    class SyncClient:
        def __init__(self) -> None:
            self.external_ids: list[str | None] = []

        def effect_reserve(self, *_args: object, **_kwargs: object) -> EffectResult:
            return EffectResult(status="reserved")

        def effect_confirm(self, *_args: object, **kwargs: object) -> EffectResult:
            self.external_ids.append(kwargs.get("external_id"))  # type: ignore[arg-type]
            return EffectResult(status="confirmed")

    class AsyncClient:
        def __init__(self) -> None:
            self.external_ids: list[str | None] = []

        async def effect_reserve(self, *_args: object, **_kwargs: object) -> EffectResult:
            return EffectResult(status="reserved")

        async def effect_confirm(self, *_args: object, **kwargs: object) -> EffectResult:
            self.external_ids.append(kwargs.get("external_id"))  # type: ignore[arg-type]
            return EffectResult(status="confirmed")

    sync_client = SyncClient()
    sync_context = SimpleNamespace(
        client=sync_client,
        id="flow",
        partition_key=None,
        lease_token=b"lease",
        fencing_token=1,
    )
    sync_effect = WorkflowEffect(sync_context, "effect", "call")  # type: ignore[arg-type]

    async_client = AsyncClient()
    async_context = SimpleNamespace(
        client=async_client,
        id="flow",
        partition_key=None,
        lease_token=b"lease",
        fencing_token=1,
    )
    async_effect = AsyncWorkflowEffect(  # type: ignore[arg-type]
        async_context,
        "effect",
        "call",
    )

    assert sync_effect.call(lambda: b"\xff") == b"\xff"
    assert asyncio.run(async_effect.call(lambda: b"\xff")) == b"\xff"
    assert sync_client.external_ids == [None]
    assert async_client.external_ids == [None]
