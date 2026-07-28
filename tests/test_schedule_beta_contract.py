from __future__ import annotations

from typing import Any

import pytest

from ferricstore import FlowClient, ScheduleFireDueResult, ScheduleFireResult, ScheduleRecord


def schedule_record(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
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
    record.update(overrides)
    return record


def test_schedule_responses_use_distinct_canonical_types() -> None:
    record = ScheduleRecord.from_resp(
        schedule_record(
            state="failed",
            end_reason="planning_failed",
            last_planning_error="ERR invalid recurrence",
        )
    )
    fire = ScheduleFireResult.from_resp(
        {
            "fired": 1,
            "target_id": "daily:1000:1",
            "schedule": schedule_record(fire_count=1),
        }
    )
    due = ScheduleFireDueResult.from_resp(
        {
            "claimed": 1,
            "coalesced": 0,
            "errors": [],
            "fired": 1,
            "last_target_id": "daily:1000:1",
            "skipped": 0,
        }
    )

    assert record.last_planning_error == "ERR invalid recurrence"
    assert record.created_at_ms == 50
    assert record.every_ms == 1_000
    assert record.cron is None
    assert record.overlap_retry_ms is None
    assert isinstance(fire.schedule, ScheduleRecord)
    assert fire.target_id == "daily:1000:1"
    assert due.claimed == 1


def test_schedule_fire_due_errors_are_typed_id_reason_pairs() -> None:
    due = ScheduleFireDueResult.from_resp(
        {
            "claimed": 1,
            "coalesced": 0,
            "errors": [["daily", "target failed"]],
            "fired": 0,
            "skipped": 0,
        }
    )

    assert due.errors == [("daily", "target failed")]


def test_schedule_fire_due_keeps_later_claim_failure_separate_from_outcomes() -> None:
    due = ScheduleFireDueResult.from_resp(
        {
            "claimed": 1,
            "coalesced": 0,
            "errors": [],
            "fired": 1,
            "claim_error": "ERR claim unavailable",
            "last_target_id": "daily:1000:1",
            "skipped": 0,
        }
    )

    assert due.claim_error == "ERR claim unavailable"


def test_schedule_client_returns_canonical_types_and_delete_has_no_synthetic_record() -> None:
    class Executor:
        def __init__(self) -> None:
            self.responses: list[Any] = [
                schedule_record(),
                {"fired": 1, "target_id": "daily:1000:1", "schedule": schedule_record()},
                {
                    "claimed": 0,
                    "coalesced": 0,
                    "errors": [],
                    "fired": 0,
                    "skipped": 0,
                },
                "OK",
            ]

        def execute_command(self, *_args: object) -> Any:
            return self.responses.pop(0)

    client = FlowClient(Executor())  # type: ignore[arg-type]

    assert isinstance(
        client.schedule_create(
            "daily",
            target={"id_prefix": "daily", "type": "task"},
            kind="interval",
            every_ms=1_000,
        ),
        ScheduleRecord,
    )
    assert isinstance(client.schedule_fire("daily"), ScheduleFireResult)
    assert isinstance(client.schedule_fire_due(), ScheduleFireDueResult)
    assert client.schedule_delete("daily") is None


@pytest.mark.parametrize(
    "target, message",
    [
        ({"type": "__ferricstore_schedule"}, "target type"),
        (
            {"id": "__ferricstore_schedule__:forged", "type": "task"},
            "target id",
        ),
        (
            {"id_prefix": "__ferricstore_schedule__:forged", "type": "task"},
            "target id_prefix",
        ),
    ],
)
def test_schedule_client_rejects_reserved_internal_targets_before_transport(
    target: dict[str, object], message: str
) -> None:
    class Executor:
        def execute_command(self, *_args: object) -> Any:
            raise AssertionError("invalid schedule request reached transport")

    client = FlowClient(Executor())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=message):
        client.schedule_create("daily", target=target)
