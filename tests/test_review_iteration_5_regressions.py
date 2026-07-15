from __future__ import annotations

import asyncio
import threading

import pytest

from ferricstore import FlowClient
from ferricstore.client_helpers import _normalize_admin_response
from ferricstore.errors import FerricStoreError
from ferricstore.protocol_codec import decode_value, encode_value
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.topology_lifecycle import AsyncSingleFlight
from ferricstore.types import ApprovalResult, EffectResult, ScheduleResult
from ferricstore.workflow_models import WorkflowEffect


def test_native_flow_create_uses_released_kv_lineage_field_names() -> None:
    command = build_protocol_command(
        "FLOW.CREATE",
        "child",
        "TYPE",
        "task",
        "PARENT_FLOW_ID",
        "parent",
        "ROOT_FLOW_ID",
        "root",
    )

    assert command.payload["parent_id"] == "parent"
    assert command.payload["root_id"] == "root"
    assert "parent_flow_id" not in command.payload
    assert "root_flow_id" not in command.payload


def test_schedule_result_decodes_the_kv_view_shape() -> None:
    result = ScheduleResult.from_resp(
        {
            b"id": b"daily",
            b"flow_id": b"__ferricstore_schedule__:daily",
            b"state": b"active",
            b"kind": b"interval",
            b"next_run_at_ms": 2_000,
            b"fire_count": 3,
            b"skipped_count": 1,
            b"last_target_id": b"daily:1000:3",
        }
    )

    assert result.id == "daily"
    assert result.flow_id == "__ferricstore_schedule__:daily"
    assert result.status == "active"
    assert result.state == "active"
    assert result.next_fire_at_ms == 2_000
    assert result.next_run_at_ms == 2_000
    assert result.fires == 3
    assert result.fire_count == 3
    assert result.skipped_count == 1
    assert result.last_target_id == "daily:1000:3"


def test_schedule_result_decodes_manual_fire_envelope() -> None:
    result = ScheduleResult.from_resp(
        {
            b"fired": 1,
            b"target_id": b"daily:1000:1",
            b"schedule": {
                b"id": b"daily",
                b"state": b"active",
                b"kind": b"interval",
                b"next_run_at_ms": 2_000,
                b"fire_count": 1,
            },
        }
    )

    assert result.fired == 1
    assert result.target_id == "daily:1000:1"
    assert result.id == "daily"
    assert result.status == "active"
    assert result.schedule is not None
    assert result.schedule.fire_count == 1


def test_governance_results_decode_the_kv_field_names() -> None:
    approval = ApprovalResult.from_resp(
        {
            b"id": b"approval-1",
            b"status": b"approved",
            b"decided_by": b"operator",
            b"decision_reason": b"verified",
        }
    )
    effect = EffectResult.from_resp(
        {
            b"flow_id": b"flow-1",
            b"effect_key": b"email",
            b"status": b"confirmed",
            b"created_at_ms": 100,
            b"updated_at_ms": 125,
        }
    )

    assert approval.approver == "operator"
    assert approval.decided_by == "operator"
    assert approval.decision_reason == "verified"
    assert effect.created_at_ms == 100
    assert effect.updated_at_ms == 125
    assert effect.reserved_at_ms == 100
    assert effect.confirmed_at_ms == 125


def test_admin_response_normalization_preserves_opaque_binary_values() -> None:
    assert _normalize_admin_response({b"usage": {b"blob": b"\xff"}}) == {"usage": {"blob": b"\xff"}}


def test_schedule_end_must_not_precede_the_first_run_before_io() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def execute_command(self, *args: object) -> dict[str, object]:
            self.calls.append(args)
            return {}

    executor = Executor()
    client = FlowClient(executor)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="end_at_ms must be at or after first run"):
        client.schedule_create(
            "too-late",
            target={"type": "task", "id_prefix": "task"},
            kind="interval",
            start_at_ms=200,
            every_ms=10,
            end_at_ms=199,
        )

    assert executor.calls == []


def test_autobatch_cancellation_releases_a_waiting_queue_slot_promptly() -> None:
    class Executor:
        def execute_command(self, *_args: object) -> bytes:
            return b"OK"

    client = FlowClient(Executor()).autobatch(  # type: ignore[arg-type]
        max_batch=2,
        max_delay_ms=10_000,
        max_pending=1,
    )
    first = client.create_async("first", type="task", partition_key="tenant")
    assert first.cancel()

    submitted = threading.Event()
    submission_error: list[BaseException] = []
    second_holder: list[object] = []

    def submit_second() -> None:
        try:
            second_holder.append(client.create_async("second", type="task", partition_key="tenant"))
        except BaseException as exc:
            submission_error.append(exc)
        finally:
            submitted.set()

    producer = threading.Thread(target=submit_second)
    producer.start()
    try:
        assert submitted.wait(0.5), "cancelled work retained the only queue slot"
        assert submission_error == []
        assert len(second_holder) == 1
        second_holder[0].cancel()  # type: ignore[union-attr]
    finally:
        client.close(timeout=1)
        producer.join(1)


def test_protocol_encoder_uses_the_real_builtin_list_size() -> None:
    class MisreportedList(list[bytes]):
        def __len__(self) -> int:
            return 0

    encoded = encode_value(MisreportedList([b"value"]))
    decoded, remaining = decode_value(encoded)

    assert decoded == [b"value"]
    assert remaining == b""


def test_protocol_encoder_byte_limit_cannot_be_bypassed_by_bytearray_subclass() -> None:
    class MisreportedBytearray(bytearray):
        def __len__(self) -> int:
            return 0

    with pytest.raises(FerricStoreError, match="exceeds max_bytes"):
        encode_value(MisreportedBytearray(b"payload"), max_bytes=5)


def test_async_singleflight_does_not_retain_completed_work_after_waiter_cancellation() -> None:
    async def run() -> None:
        singleflight = AsyncSingleFlight[object]()
        started = asyncio.Event()
        release = asyncio.Event()

        async def operation() -> object:
            started.set()
            await release.wait()
            return object()

        caller = asyncio.create_task(singleflight.run(operation))
        await started.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller

        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert singleflight._task is None

    asyncio.run(run())


def test_sync_workflow_effect_settles_reservation_on_base_exception() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def effect_reserve(self, *_args: object, **_kwargs: object) -> EffectResult:
            self.calls.append("reserve")
            return EffectResult(flow_id="flow", effect_key="effect", status="reserved")

        def effect_fail(self, *_args: object, **_kwargs: object) -> EffectResult:
            self.calls.append("fail")
            return EffectResult(flow_id="flow", effect_key="effect", status="failed")

    class Context:
        id = "flow"
        partition_key = "tenant"
        lease_token = b"lease"
        fencing_token = 1

        def __init__(self) -> None:
            self.client = Client()

    context = Context()
    effect = WorkflowEffect(context, "effect", "external.call")  # type: ignore[arg-type]

    def interrupted() -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        effect.call(interrupted)

    assert context.client.calls == ["reserve", "fail"]
