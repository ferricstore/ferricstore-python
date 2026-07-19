from __future__ import annotations

import asyncio
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import pytest

import ferricstore
from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_workflow_client import AsyncWorkflowClient
from ferricstore.async_workflow_context import AsyncWorkflowFlowCommands
from ferricstore.client_core import FlowClient
from ferricstore.config_validation import validate_partition_key_sequence
from ferricstore.errors import (
    RequestOutcomeUnknownError,
    StalePolicyGenerationError,
    classify_server_error,
)
from ferricstore.policy_types import MAX_POLICY_GENERATION, PolicySnapshot
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_common import _flow_wake_payload
from ferricstore.protocol_constants import _MAGIC, _OPCODES, _REQUEST_VERSION, _RESPONSE_VERSION
from ferricstore.protocol_negotiation import parse_hello_capabilities
from ferricstore.protocol_retry import request_outcome_error
from ferricstore.types import ClaimedFlow, EffectResult, FlowRecord
from ferricstore.workflow_client import WorkflowClient
from ferricstore.workflow_core import workflow_partition_key
from ferricstore.workflow_models import WorkflowFlowCommands


def _policy_response(*, generation: int = 8) -> dict[bytes, Any]:
    return {
        b"type": b"order",
        b"generation": generation,
        b"version": b"2026-07",
        b"max_active_ms": 30_000,
        b"retry": {b"max_retries": 3},
        b"retention": {b"ttl_ms": 86_400_000},
        b"indexed_attributes": [b"tenant"],
        b"indexed_state_meta": b"phase",
        b"governance": {b"enabled": True},
        b"states": {
            b"queued": {b"mode": b"fifo", b"retry": {b"max_retries": 5}},
            b"ready": {b"mode": b"parallel"},
        },
    }


def _hello(*, policy_fields: list[str] | None = None) -> dict[str, Any]:
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
                    "fields": policy_fields
                    if policy_fields is not None
                    else ["type", "replace", "expected_generation", "states"],
                }
            },
        },
    }


class RecordingExecutor:
    def __init__(self, response: Any | None = None) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.response = _policy_response() if response is None else response

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        return self.response


class AsyncRecordingExecutor(RecordingExecutor):
    async def execute_command(self, *args: Any) -> Any:
        return super().execute_command(*args)


def test_v091_declares_minimum_server_without_changing_native_protocol_v1() -> None:
    assert ferricstore.__version__ == "0.6.2"
    assert ferricstore.MINIMUM_SERVER_VERSION == "0.9.1"
    assert _MAGIC == b"FSNP"
    assert _REQUEST_VERSION == 0x01
    assert _RESPONSE_VERSION == 0x81
    assert _OPCODES["FLOW.POLICY.SET"] == 0x021E


def test_partition_key_has_one_public_sdk_type() -> None:
    assert ferricstore.PartitionKey == str | bytes


def test_hello_requires_and_records_policy_cas_fields() -> None:
    negotiated = parse_hello_capabilities(_hello())

    assert negotiated.flow_policy_set_fields >= {"replace", "expected_generation"}


@pytest.mark.parametrize(
    "policy_fields",
    [
        ["type", "replace", "states"],
        ["type", "expected_generation", "states"],
    ],
)
def test_hello_rejects_server_without_policy_cas_fields(policy_fields: list[str]) -> None:
    with pytest.raises(ferricstore.FerricStoreError, match=r"0\.9\.1.*FLOW\.POLICY\.SET"):
        parse_hello_capabilities(_hello(policy_fields=policy_fields))


def test_direct_policy_update_defaults_to_explicit_deep_patch_and_returns_snapshot() -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)

    snapshot = client.install_policy("order")

    assert isinstance(snapshot, PolicySnapshot)
    assert snapshot.type == "order"
    assert snapshot.generation == 8
    assert snapshot.states["queued"]["mode"] == "fifo"
    assert snapshot.indexed_attributes == ("tenant",)
    assert snapshot["generation"] == 8
    assert executor.calls == [("FLOW.POLICY.SET", "order", "REPLACE", "false")]


def test_policy_update_sends_replace_and_expected_generation() -> None:
    executor = RecordingExecutor(_policy_response(generation=9))
    client = FlowClient(executor)

    snapshot = client.install_policy("order", replace=True, expected_generation=8)

    assert snapshot.generation == 9
    assert executor.calls == [
        (
            "FLOW.POLICY.SET",
            "order",
            "EXPECTED_GENERATION",
            8,
            "REPLACE",
            "true",
        )
    ]


def test_policy_get_returns_typed_state_snapshot() -> None:
    executor = RecordingExecutor(
        {
            b"type": b"order",
            b"generation": 11,
            b"state": b"queued",
            b"mode": b"fifo",
            b"retry": {b"max_retries": 2},
        }
    )
    client = FlowClient(executor)

    snapshot = client.policy_get("order", state="queued")

    assert isinstance(snapshot, PolicySnapshot)
    assert snapshot.generation == 11
    assert snapshot.state == "queued"
    assert snapshot.mode == ferricstore.FlowStateMode.FIFO
    assert snapshot.retry == {"max_retries": 2}


def test_policy_snapshot_preserves_full_mapping_compatibility() -> None:
    snapshot = PolicySnapshot.from_resp(_policy_response())

    assert isinstance(snapshot, Mapping)
    assert dict(snapshot) == snapshot.raw
    assert list(snapshot) == list(snapshot.raw or {})
    assert len(snapshot) == len(snapshot.raw or {})
    assert "generation" in snapshot


@pytest.mark.parametrize("generation", [-1, MAX_POLICY_GENERATION + 1, 1.5, True])
def test_policy_generation_rejects_values_outside_safe_integer_range(generation: Any) -> None:
    executor = RecordingExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="expected_generation"):
        client.install_policy("order", expected_generation=generation)

    assert executor.calls == []


@pytest.mark.parametrize("generation", [0, MAX_POLICY_GENERATION])
def test_policy_generation_accepts_safe_integer_boundaries(generation: int) -> None:
    executor = RecordingExecutor()

    FlowClient(executor).install_policy("order", expected_generation=generation)

    assert ("EXPECTED_GENERATION", generation) == executor.calls[0][2:4]


def test_replace_requires_a_real_boolean() -> None:
    executor = RecordingExecutor()

    with pytest.raises(ValueError, match="replace must be a boolean"):
        FlowClient(executor).install_policy("order", replace=1)  # type: ignore[arg-type]

    assert executor.calls == []


def test_native_policy_payload_uses_typed_replace_and_generation_fields() -> None:
    command = build_protocol_command(
        "FLOW.POLICY.SET",
        "order",
        "EXPECTED_GENERATION",
        7,
        "REPLACE",
        "true",
    )

    assert command.payload == {
        "type": "order",
        "expected_generation": 7,
        "replace": True,
    }


def test_workflow_policy_install_defaults_to_full_replacement_and_allows_patch_override() -> None:
    executor = RecordingExecutor()
    client = WorkflowClient(FlowClient(executor))

    client.install_policy("order")
    client.install_policy("order", replace=False, expected_generation=8)

    assert executor.calls[0] == ("FLOW.POLICY.SET", "order", "REPLACE", "true")
    assert executor.calls[1] == (
        "FLOW.POLICY.SET",
        "order",
        "EXPECTED_GENERATION",
        8,
        "REPLACE",
        "false",
    )


def test_workflow_context_policy_install_defaults_to_full_replacement() -> None:
    executor = RecordingExecutor()
    context = SimpleNamespace(
        workflow=SimpleNamespace(type="order", client=FlowClient(executor)),
    )
    commands = WorkflowFlowCommands(context)  # type: ignore[arg-type]

    commands.install_policy()
    commands.install_policy(replace=False, expected_generation=8)

    assert executor.calls[0] == ("FLOW.POLICY.SET", "order", "REPLACE", "true")
    assert executor.calls[1] == (
        "FLOW.POLICY.SET",
        "order",
        "EXPECTED_GENERATION",
        8,
        "REPLACE",
        "false",
    )


def test_async_policy_api_matches_sync_contract() -> None:
    async def run() -> None:
        executor = AsyncRecordingExecutor()
        direct = AsyncFlowClient(executor)
        workflow = AsyncWorkflowClient(direct)

        direct_snapshot = await direct.install_policy(
            "order",
            replace=False,
            expected_generation=7,
        )
        workflow_snapshot = await workflow.install_policy("order", expected_generation=8)

        assert isinstance(direct_snapshot, PolicySnapshot)
        assert isinstance(workflow_snapshot, PolicySnapshot)
        assert executor.calls[0] == (
            "FLOW.POLICY.SET",
            "order",
            "EXPECTED_GENERATION",
            7,
            "REPLACE",
            "false",
        )
        assert executor.calls[1] == (
            "FLOW.POLICY.SET",
            "order",
            "EXPECTED_GENERATION",
            8,
            "REPLACE",
            "true",
        )

    asyncio.run(run())


def test_async_workflow_context_policy_install_defaults_to_full_replacement() -> None:
    async def run() -> None:
        executor = AsyncRecordingExecutor()
        context = SimpleNamespace(
            client=AsyncFlowClient(executor),
            workflow=SimpleNamespace(type="order"),
        )
        commands = AsyncWorkflowFlowCommands(context)  # type: ignore[arg-type]

        await commands.install_policy()
        await commands.install_policy(replace=False, expected_generation=8)

        assert executor.calls[0] == ("FLOW.POLICY.SET", "order", "REPLACE", "true")
        assert executor.calls[1] == (
            "FLOW.POLICY.SET",
            "order",
            "EXPECTED_GENERATION",
            8,
            "REPLACE",
            "false",
        )

    asyncio.run(run())


def test_policy_clients_pass_raw_responses_to_the_single_normalization_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses: list[Any] = []
    original_from_resp = PolicySnapshot.from_resp.__func__

    def recording_from_resp(cls: type[PolicySnapshot], value: Any) -> PolicySnapshot:
        responses.append(value)
        return original_from_resp(cls, value)

    monkeypatch.setattr(PolicySnapshot, "from_resp", classmethod(recording_from_resp))
    raw_response = _policy_response()

    sync_executor = RecordingExecutor(raw_response)
    FlowClient(sync_executor).install_policy("order")
    FlowClient(sync_executor).policy_get("order")

    async def run() -> None:
        async_executor = AsyncRecordingExecutor(raw_response)
        await AsyncFlowClient(async_executor).install_policy("order")
        await AsyncFlowClient(async_executor).policy_get("order")

    asyncio.run(run())

    assert responses == [raw_response] * 4
    assert all(response is raw_response for response in responses)


def test_stale_policy_generation_is_dedicated_and_never_retryable() -> None:
    error = classify_server_error(
        "ERR stale flow policy generation",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=1,
    )

    assert isinstance(error, StalePolicyGenerationError)
    assert error.retryable is False
    assert error.safe_to_retry is False
    assert error.retry_after_ms is None


def test_stale_cas_is_submitted_once() -> None:
    class StaleExecutor(RecordingExecutor):
        def execute_command(self, *args: Any) -> Any:
            self.calls.append(args)
            raise classify_server_error(
                "ERR stale flow policy generation",
                retryable=True,
                safe_to_retry=True,
            )

    executor = StaleExecutor()

    with pytest.raises(StalePolicyGenerationError):
        FlowClient(executor).install_policy("order", expected_generation=7)

    assert len(executor.calls) == 1


def test_post_send_policy_failure_has_unknown_outcome_and_is_not_replayable() -> None:
    error = request_outcome_error(
        _OPCODES["FLOW.POLICY.SET"],
        ConnectionResetError("peer closed"),
    )

    assert isinstance(error, RequestOutcomeUnknownError)
    assert error.retryable is False
    assert error.safe_to_retry is False


@pytest.mark.parametrize(
    ("attrs", "partition_by", "expected"),
    [
        ({"tenant": "a:b", "order": "c"}, ("tenant", "order"), "fpk:3:a:b1:c"),
        ({"tenant": "a", "order": "b:c"}, ("tenant", "order"), "fpk:1:a3:b:c"),
        ({"tenant": "é", "order": "東京"}, ("tenant", "order"), "fpk:2:é6:東京"),
        ({"tenant": b"\xff:\x00"}, ("tenant",), b"fpk:3:\xff:\x00"),
    ],
)
def test_workflow_partition_key_uses_collision_free_binary_safe_encoding(
    attrs: dict[str, Any],
    partition_by: tuple[str, ...],
    expected: str | bytes,
) -> None:
    assert workflow_partition_key(attrs, partition_by) == expected


def test_partition_encoding_distinguishes_old_colon_join_collision() -> None:
    left = workflow_partition_key({"first": "a:b", "second": "c"}, ("first", "second"))
    right = workflow_partition_key({"first": "a", "second": "b:c"}, ("first", "second"))

    assert left != right


def test_binary_derived_partition_survives_flow_record_decoding() -> None:
    partition = b"fpk:2:\x00\xff"

    record = FlowRecord.from_resp(
        {
            b"id": b"flow-1",
            b"type": b"order",
            b"state": b"queued",
            b"partition_key": partition,
        }
    )
    claimed = ClaimedFlow.from_compact_rows([[b"flow-1", partition, b"lease", 1]])[0]
    effect = EffectResult.from_resp(
        {
            b"flow_id": b"flow-1",
            b"partition_key": partition,
            b"effect_key": b"send-email",
            b"status": b"reserved",
        }
    )

    assert record.partition_key == partition
    assert claimed.partition_key == partition
    assert effect.partition_key == partition


def test_binary_derived_partitions_survive_multi_partition_claim_and_wake_paths() -> None:
    partitions = [b"fpk:2:\x00\xff", b"fpk:3:a:b"]
    executor = RecordingExecutor([])

    jobs = FlowClient(executor).claim_flows(
        "order",
        worker="worker-1",
        partition_keys=partitions,
        priority=None,
    )
    wake = _flow_wake_payload(
        "order",
        partition_keys=partitions,
    )

    assert jobs == []
    partition_start = executor.calls[0].index("PARTITIONS") + 2
    assert executor.calls[0][partition_start : partition_start + len(partitions)] == tuple(
        partitions
    )
    assert wake["flow_wake"]["partition_keys"] == partitions


def test_partition_key_sequence_normalizes_mutable_binary_views() -> None:
    assert validate_partition_key_sequence(["text", bytearray(b"bytes"), memoryview(b"view")]) == (
        "text",
        b"bytes",
        b"view",
    )


@pytest.mark.parametrize("partition_keys", [[], [""], [b""], [object()]])
def test_partition_key_sequence_rejects_invalid_entries(
    partition_keys: list[Any],
) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        validate_partition_key_sequence(partition_keys, allow_empty=False)
