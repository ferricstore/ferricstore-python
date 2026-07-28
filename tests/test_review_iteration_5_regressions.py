from __future__ import annotations

import asyncio
import inspect
import threading

import pytest

from ferricstore import AsyncFlowClient, FlowClient
from ferricstore.client_helpers import _normalize_admin_response
from ferricstore.errors import FerricStoreError
from ferricstore.protocol_codec import decode_value, encode_value
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.topology_lifecycle import AsyncSingleFlight
from ferricstore.types import (
    ApprovalResult,
    EffectResult,
    ScheduleFireDueResult,
    ScheduleFireResult,
    ScheduleRecord,
)
from ferricstore.workflow_models import WorkflowEffect


def _canonical_schedule_response(**overrides: object) -> dict[str, object]:
    response: dict[str, object] = {
        "attempts": 0,
        "catchup_policy": "fire_once",
        "coalesced_count": 0,
        "created_at_ms": 50,
        "cron": None,
        "every_ms": 1_000,
        "fire_count": 0,
        "id": "daily",
        "kind": "interval",
        "last_coalesced_count": 0,
        "overlap_policy": "allow",
        "overlap_retry_ms": None,
        "skipped_count": 0,
        "state": "active",
        "target": {"id_prefix": "daily", "type": "task"},
        "timezone": None,
    }
    response.update(overrides)
    return response


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

    assert command.payload["parent_flow_id"] == "parent"
    assert command.payload["root_flow_id"] == "root"
    assert "parent_id" not in command.payload
    assert "root_id" not in command.payload


def test_schedule_record_decodes_the_kv_view_shape() -> None:
    result = ScheduleRecord.from_resp(
        {
            b"id": b"daily",
            b"flow_id": b"__ferricstore_schedule__:daily",
            b"state": b"active",
            b"kind": b"interval",
            b"attempts": 0,
            b"catchup_policy": b"fire_once",
            b"coalesced_count": 12,
            b"created_at_ms": 50,
            b"cron": None,
            b"every_ms": 1_000,
            b"last_catchup_at_ms": 1_950,
            b"last_coalesced_count": 4,
            b"overlap_policy": b"allow",
            b"overlap_retry_ms": None,
            b"next_run_at_ms": 2_000,
            b"fire_count": 3,
            b"skipped_count": 1,
            b"last_target_id": b"daily:1000:3",
            b"target": {b"id_prefix": b"daily", b"type": b"task"},
            b"timezone": None,
        }
    )

    assert result.id == "daily"
    assert result.flow_id == "__ferricstore_schedule__:daily"
    assert result.state == "active"
    assert result.catchup_policy == "fire_once"
    assert result.coalesced_count == 12
    assert result.last_catchup_at_ms == 1_950
    assert result.last_coalesced_count == 4
    assert result.next_run_at_ms == 2_000
    assert result.fire_count == 3
    assert result.skipped_count == 1
    assert result.last_target_id == "daily:1000:3"


def test_schedule_record_accepts_transient_running_state() -> None:
    result = ScheduleRecord.from_resp(_canonical_schedule_response(state="running"))

    assert result.state == "running"


def test_schedule_get_matches_the_native_read_only_contract() -> None:
    assert "now_ms" not in inspect.signature(FlowClient.schedule_get).parameters
    assert "now_ms" not in inspect.signature(AsyncFlowClient.schedule_get).parameters


def test_schedule_catchup_policy_builds_sync_and_async_commands() -> None:
    response = _canonical_schedule_response()

    class SyncExecutor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def execute_command(self, *args: object) -> dict[str, object]:
            self.calls.append(args)
            return response

    sync_executor = SyncExecutor()
    sync_result = FlowClient(sync_executor).schedule_create(  # type: ignore[arg-type]
        "daily",
        target={"type": "task", "id_prefix": "daily"},
        kind="interval",
        every_ms=1_000,
        catchup_policy="fire_once",
    )

    assert sync_result.catchup_policy == "fire_once"
    assert sync_executor.calls == [
        (
            "FLOW.SCHEDULE.CREATE",
            "daily",
            "KIND",
            "interval",
            "EVERY_MS",
            1_000,
            "TARGET",
            {"type": "task", "id_prefix": "daily"},
            "CATCHUP_POLICY",
            "fire_once",
        )
    ]

    class AsyncExecutor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        async def execute_command(self, *args: object) -> dict[str, object]:
            self.calls.append(args)
            return response

    async def create_async() -> tuple[ScheduleRecord, AsyncExecutor]:
        executor = AsyncExecutor()
        result = await AsyncFlowClient(executor).schedule_create(  # type: ignore[arg-type]
            "daily",
            target={"type": "task", "id_prefix": "daily"},
            kind="interval",
            every_ms=1_000,
            catchup_policy="fire_once",
        )
        return result, executor

    async_result, async_executor = asyncio.run(create_async())
    assert async_result.catchup_policy == "fire_once"
    assert async_executor.calls == sync_executor.calls


def test_schedule_fire_result_decodes_manual_fire_envelope() -> None:
    result = ScheduleFireResult.from_resp(
        {
            b"fired": 1,
            b"target_id": b"daily:1000:1",
            b"schedule": {
                b"attempts": 0,
                b"catchup_policy": b"fire_once",
                b"coalesced_count": 0,
                b"created_at_ms": 50,
                b"cron": None,
                b"every_ms": 1_000,
                b"id": b"daily",
                b"state": b"active",
                b"kind": b"interval",
                b"last_coalesced_count": 0,
                b"next_run_at_ms": 2_000,
                b"fire_count": 1,
                b"overlap_policy": b"allow",
                b"overlap_retry_ms": None,
                b"skipped_count": 0,
                b"target": {b"id_prefix": b"daily", b"type": b"task"},
                b"timezone": None,
            },
        }
    )

    assert result.fired == 1
    assert result.target_id == "daily:1000:1"
    assert result.schedule is not None
    assert result.schedule.fire_count == 1


@pytest.mark.parametrize(
    "response, message",
    [
        (_canonical_schedule_response(kind="weekly"), "kind"),
        (_canonical_schedule_response(catchup_policy=None), "catchup_policy"),
        (
            _canonical_schedule_response(kind="cron", catchup_policy="fire_once"),
            "catchup_policy",
        ),
        (_canonical_schedule_response(coalesced_count="1"), "coalesced_count"),
        (_canonical_schedule_response(last_coalesced_count=-1), "last_coalesced_count"),
        (
            _canonical_schedule_response(
                kind="one_shot", every_ms=None, catchup_policy=None, coalesced_count=1
            ),
            "coalesced_count",
        ),
        (
            _canonical_schedule_response(
                coalesced_count=3,
                last_coalesced_count=4,
                last_catchup_at_ms=100,
            ),
            "last_coalesced_count",
        ),
        (
            _canonical_schedule_response(
                coalesced_count=1,
                last_coalesced_count=1,
            ),
            "last_catchup_at_ms",
        ),
        (_canonical_schedule_response(target={}), "target type"),
        (_canonical_schedule_response(flow_id=7), "flow_id"),
        (_canonical_schedule_response(last_target_id=""), "last_target_id"),
        (
            _canonical_schedule_response(kind="one_shot", catchup_policy=""),
            "catchup_policy",
        ),
    ],
)
def test_schedule_record_rejects_malformed_canonical_records(
    response: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        ScheduleRecord.from_resp(response)


@pytest.mark.parametrize(
    "field",
    ["created_at_ms", "every_ms", "cron", "timezone", "overlap_retry_ms"],
)
def test_schedule_record_requires_complete_recurrence_shape(field: str) -> None:
    response = _canonical_schedule_response()
    del response[field]

    with pytest.raises(ValueError, match=field):
        ScheduleRecord.from_resp(response)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"every_ms": 0}, "every_ms"),
        ({"cron": "* * * * *"}, "cron"),
        ({"timezone": "Etc/UTC"}, "timezone"),
        (
            {
                "kind": "cron",
                "every_ms": None,
                "cron": "* * * * *",
                "timezone": None,
                "catchup_policy": None,
            },
            "timezone",
        ),
        ({"overlap_retry_ms": 5}, "overlap_retry_ms"),
        (
            {
                "kind": "one_shot",
                "every_ms": None,
                "catchup_policy": None,
                "overlap_policy": "skip",
            },
            "overlap_policy",
        ),
    ],
)
def test_schedule_record_validates_recurrence_shape(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        ScheduleRecord.from_resp(_canonical_schedule_response(**overrides))


@pytest.mark.parametrize(
    "response, message",
    [
        ({"claimed": 0, "errors": [], "fired": 0, "skipped": 0}, "coalesced"),
        (
            {"claimed": 1, "coalesced": 0, "errors": [], "fired": 0, "skipped": 0},
            "outcomes",
        ),
        (
            {
                "claimed": 1,
                "coalesced": 0,
                "errors": [["schedule"]],
                "fired": 0,
                "skipped": 0,
            },
            "errors",
        ),
        (
            {"claimed": 1, "coalesced": 0, "errors": [], "fired": 1, "skipped": 0},
            "last_target_id",
        ),
        (
            {"claimed": 1, "coalesced": 0, "errors": [], "fired": 0, "skipped": 1},
            "last_skip_reason",
        ),
        (
            {
                "claimed": 0,
                "coalesced": 0,
                "errors": [],
                "fired": 0,
                "last_target_id": "stale",
                "skipped": 0,
            },
            "last_target_id",
        ),
        (
            {
                "claimed": 0,
                "coalesced": 0,
                "errors": [],
                "fired": 0,
                "last_skip_reason": "stale",
                "skipped": 0,
            },
            "last_skip_reason",
        ),
        (
            {
                "claimed": 1,
                "coalesced": 1,
                "errors": [["schedule", "failed"]],
                "fired": 0,
                "skipped": 0,
            },
            "coalesced",
        ),
    ],
)
def test_schedule_fire_due_result_rejects_malformed_envelopes(
    response: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        ScheduleFireDueResult.from_resp(response)


def test_schedule_fire_due_result_decodes_summary_fields() -> None:
    result = ScheduleFireDueResult.from_resp(
        {
            "claimed": 1,
            "coalesced": 25,
            "errors": [],
            "fired": 0,
            "last_skip_reason": "target active",
            "skipped": 1,
        }
    )

    assert result.coalesced == 25
    assert result.last_skip_reason == "target active"


@pytest.mark.parametrize(
    "response, message",
    [
        (
            {
                "fired": 0,
                "reason": "overlap",
                "schedule": _canonical_schedule_response(),
                "skipped": 1,
                "target_id": "stale",
            },
            "target_id",
        ),
        (
            {
                "fired": 1,
                "reason": "stale",
                "schedule": _canonical_schedule_response(),
                "target_id": "daily:1:1",
            },
            "reason",
        ),
    ],
)
def test_schedule_fire_result_rejects_contradictory_fields(
    response: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        ScheduleFireResult.from_resp(response)


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
