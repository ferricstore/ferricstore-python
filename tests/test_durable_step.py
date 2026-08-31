from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncWorkflow,
    FerricStoreError,
    FlowClient,
    FlowWorkflow,
    HttpError,
    JsonCodec,
    RequestOutcomeUnknownError,
    StaleLeaseError,
    transition,
)
from ferricstore.types import ClaimedFlow, FlowRecord


def _step_value_name(name: str) -> str:
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()
    return f"__ferricstore_step__:sha256:{digest}"


def _record(
    *,
    id: bytes = b"flow-1",
    lease: bytes = b"lease-1",
    fencing: int = 7,
    run_state: bytes = b"charge",
    value_refs: Any = None,
) -> dict[bytes, Any]:
    return {
        b"id": id,
        b"type": b"order",
        b"state": b"running",
        b"run_state": run_state,
        b"partition_key": b"tenant-1",
        b"lease_token": lease,
        b"fencing_token": fencing,
        b"version": fencing,
        b"value_refs": {} if value_refs is None else value_refs,
    }


def _job(*, run_state: str = "charge") -> ClaimedFlow:
    return ClaimedFlow(
        "flow-1",
        b"lease-1",
        7,
        partition_key="tenant-1",
        type="order",
        run_state=run_state,
    )


class SequenceExecutor:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class AsyncSequenceExecutor(SequenceExecutor):
    async def execute_command(self, *args: Any) -> Any:
        return super().execute_command(*args)


def test_advance_infers_claim_fields_and_returns_refreshed_claim() -> None:
    executor = SequenceExecutor([[b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"]])
    client = FlowClient(executor, codec=JsonCodec())

    refreshed = client.advance(_job(), to_state="warn", lease_ms=45_000, now_ms=101)

    assert refreshed == ClaimedFlow(
        "flow-1",
        b"lease-2",
        8,
        partition_key="tenant-1",
        type="order",
        run_state="warn",
    )
    assert executor.calls == [
        (
            "FLOW.STEP_CONTINUE",
            "flow-1",
            b"lease-1",
            "charge",
            "warn",
            "FENCING",
            7,
            "LEASE_MS",
            45_000,
            "NOW",
            101,
            "PARTITION",
            "tenant-1",
            "RETURN",
            "JOBS_COMPACT",
        )
    ]


def test_advance_accepts_a_claimed_full_flow_record_from_workflow_workers() -> None:
    executor = SequenceExecutor([[b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"]])
    client = FlowClient(executor, codec=JsonCodec())
    record = FlowRecord.from_resp(_record())

    refreshed = client.advance(record, to_state="warn", now_ms=101)

    assert refreshed.id == "flow-1"
    assert refreshed.type == "order"
    assert refreshed.run_state == "warn"
    assert refreshed.lease_token == b"lease-2"
    assert refreshed.fencing_token == 8


@pytest.mark.parametrize(
    "job",
    [
        ClaimedFlow(
            "flow-1",
            b"lease-1",
            0,
            partition_key="tenant-1",
            type="order",
            run_state="charge",
        ),
        FlowRecord.from_resp({**_record(), b"state": b"scheduled"}),
    ],
)
def test_durable_operations_reject_non_active_claims_before_io(job: Any) -> None:
    executor = SequenceExecutor([])
    client = FlowClient(executor)

    with pytest.raises((TypeError, ValueError), match=r"fencing_token|running"):
        client.advance(job, to_state="warn")

    assert executor.calls == []


def test_step_validates_claim_then_atomically_stores_result_and_advances() -> None:
    executor = SequenceExecutor(
        [
            _record(),
            [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())
    executions = 0

    def charge() -> dict[str, Any]:
        nonlocal executions
        executions += 1
        return {"charge_id": "ch_123"}

    refreshed, result = client.step(
        _job(),
        name="charge-customer:v1",
        run=charge,
        to_state="warn",
        lease_ms=45_000,
        now_ms=101,
    )

    assert executions == 1
    assert result == {"charge_id": "ch_123"}
    assert refreshed.lease_token == b"lease-2"
    assert refreshed.fencing_token == 8
    assert refreshed.run_state == "warn"
    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
    ]
    commit = executor.calls[1]
    value_index = commit.index("VALUE")
    assert commit[value_index + 1] == _step_value_name("charge-customer:v1")
    assert commit[value_index + 2] == b'{"charge_id":"ch_123"}'
    return_index = commit.index("RETURN")
    assert commit[return_index : return_index + 2] == ("RETURN", "JOBS_COMPACT")


def test_step_replays_committed_result_without_running_closure_or_advancing() -> None:
    value_name = _step_value_name("charge-customer:v1")
    executor = SequenceExecutor(
        [
            _record(
                lease=b"lease-9",
                fencing=12,
                run_state=b"warn",
                value_refs={value_name.encode(): {b"ref": b"value-ref-1"}},
            ),
            [b'{"charge_id":"ch_123"}'],
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())
    current = ClaimedFlow(
        "flow-1",
        b"lease-9",
        12,
        partition_key="tenant-1",
        type="order",
        run_state="warn",
    )

    def must_not_run() -> Any:
        raise AssertionError("committed closure ran again")

    refreshed, result = client.step(
        current,
        name="charge-customer:v1",
        run=must_not_run,
        to_state="warn",
        now_ms=200,
    )

    assert result == {"charge_id": "ch_123"}
    assert refreshed.lease_token == b"lease-9"
    assert refreshed.fencing_token == 12
    assert refreshed.run_state == "warn"
    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.VALUE.MGET",
    ]


def test_durable_helpers_reject_incomplete_claims_before_network_or_closure() -> None:
    executor = SequenceExecutor([])
    client = FlowClient(executor)
    incomplete = ClaimedFlow("flow-1", b"lease", 1)
    ran = False

    def closure() -> bytes:
        nonlocal ran
        ran = True
        return b"result"

    with pytest.raises(ValueError, match="run_state"):
        client.advance(incomplete, to_state="next")
    with pytest.raises(ValueError, match="name"):
        client.step(_job(), name="", run=closure, to_state="next")

    assert ran is False
    assert executor.calls == []


def test_step_rejects_a_stale_claim_before_running_the_closure() -> None:
    executor = SequenceExecutor([StaleLeaseError("stale flow lease")])
    client = FlowClient(executor)
    ran = False

    def closure() -> bytes:
        nonlocal ran
        ran = True
        return b"result"

    with pytest.raises(StaleLeaseError):
        client.step(
            _job(),
            name="charge-customer:v1",
            run=closure,
            to_state="warn",
        )

    assert ran is False
    assert [call[0] for call in executor.calls] == ["FLOW.EXTEND_LEASE"]


def test_step_fails_closed_if_preflight_does_not_match_the_claim() -> None:
    executor = SequenceExecutor([_record(lease=b"different-lease")])
    client = FlowClient(executor)
    ran = False

    def closure() -> bytes:
        nonlocal ran
        ran = True
        return b"result"

    with pytest.raises(FerricStoreError, match="different workflow claim"):
        client.step(
            _job(),
            name="charge-customer:v1",
            run=closure,
            to_state="warn",
        )

    assert ran is False
    assert [call[0] for call in executor.calls] == ["FLOW.EXTEND_LEASE"]


def test_step_fails_closed_if_committed_journal_reference_is_malformed() -> None:
    value_name = _step_value_name("charge-customer:v1")
    executor = SequenceExecutor([_record(value_refs={value_name.encode(): {b"version": 1}})])
    client = FlowClient(executor)
    ran = False

    def closure() -> bytes:
        nonlocal ran
        ran = True
        return b"result"

    with pytest.raises(FerricStoreError, match="invalid result reference"):
        client.step(
            _job(),
            name="charge-customer:v1",
            run=closure,
            to_state="warn",
        )

    assert ran is False
    assert [call[0] for call in executor.calls] == ["FLOW.EXTEND_LEASE"]


def test_step_fails_closed_if_committed_result_did_not_reach_target_state() -> None:
    value_name = _step_value_name("charge-customer:v1")
    executor = SequenceExecutor(
        [_record(value_refs={value_name.encode(): {b"ref": b"value-ref-1"}})]
    )
    client = FlowClient(executor, codec=JsonCodec())

    with pytest.raises(FerricStoreError, match="target state"):
        client.step(
            _job(),
            name="charge-customer:v1",
            run=lambda: pytest.fail("inconsistent committed closure ran again"),
            to_state="warn",
        )

    assert [call[0] for call in executor.calls] == ["FLOW.EXTEND_LEASE"]


@pytest.mark.parametrize("malformed_refs", [[], b"invalid", "invalid", 7])
def test_step_fails_closed_if_value_refs_container_is_malformed(malformed_refs: Any) -> None:
    executor = SequenceExecutor([_record(value_refs=malformed_refs)])
    client = FlowClient(executor)
    ran = False

    def closure() -> bytes:
        nonlocal ran
        ran = True
        return b"result"

    with pytest.raises(FerricStoreError, match="value_refs"):
        client.step(
            _job(),
            name="charge-customer:v1",
            run=closure,
            to_state="warn",
        )

    assert ran is False
    assert [call[0] for call in executor.calls] == ["FLOW.EXTEND_LEASE"]


@pytest.mark.parametrize(
    "stored",
    [None, {b"ref": b"value-ref-1", b"omitted": True, b"size": 123}],
)
def test_step_rejects_missing_or_omitted_committed_result(stored: Any) -> None:
    value_name = _step_value_name("charge-customer:v1")
    executor = SequenceExecutor(
        [
            _record(
                run_state=b"warn",
                value_refs={value_name.encode(): {b"ref": b"value-ref-1"}},
            ),
            [stored],
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())

    with pytest.raises(FerricStoreError, match="missing or omitted"):
        client.step(
            _job(run_state="warn"),
            name="charge-customer:v1",
            run=lambda: pytest.fail("missing committed closure ran again"),
            to_state="warn",
        )

    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.VALUE.MGET",
    ]


def test_step_distinguishes_encoded_json_null_from_a_missing_blob() -> None:
    value_name = _step_value_name("charge-customer:v1")
    executor = SequenceExecutor(
        [
            _record(value_refs={value_name.encode(): {b"ref": b"value-ref-1"}}),
            [b"null"],
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())

    _refreshed, result = client.step(
        _job(),
        name="charge-customer:v1",
        run=lambda: pytest.fail("encoded null closure ran again"),
        to_state="charge",
    )

    assert result is None


def test_new_step_returns_the_codec_normalized_stored_representation() -> None:
    executor = SequenceExecutor(
        [
            _record(),
            [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
        ]
    )
    client = FlowClient(executor)

    _refreshed, result = client.step(
        _job(),
        name="charge-customer:v1",
        run=lambda: "charged",
        to_state="warn",
    )

    assert result == b"charged"
    commit = executor.calls[1]
    value_index = commit.index("VALUE")
    assert commit[value_index + 2] == b"charged"


@pytest.mark.parametrize(
    "response",
    [
        {
            b"id": b"flow-1",
            b"partition_key": b"tenant-1",
            b"lease_token": b"lease-2",
            b"fencing_token": 8,
            b"run_state": b"warn",
        },
        {
            b"id": b"flow-1",
            b"partition_key": b"tenant-1",
            b"lease_token": b"lease-2",
            b"fencing_token": 8,
            b"state": b"running",
        },
    ],
)
def test_advance_does_not_infer_missing_state_fields_for_full_maps(response: Any) -> None:
    client = FlowClient(SequenceExecutor([response]))

    with pytest.raises(FerricStoreError, match="unexpected workflow state"):
        client.advance(_job(), to_state="warn")


def test_advance_fails_closed_if_server_does_not_refresh_the_claim() -> None:
    executor = SequenceExecutor([[b"flow-1", b"tenant-1", b"lease-1", 7, b"warn"]])
    client = FlowClient(executor)

    with pytest.raises(FerricStoreError, match="did not refresh"):
        client.advance(_job(), to_state="warn")


def test_sync_workflow_context_step_applies_continuation_with_refreshed_claim() -> None:
    executor = SequenceExecutor(
        [
            _record(),
            [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
            b"OK",
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())
    workflow = FlowWorkflow(client, type="order", initial_state="charge")
    observed: list[Any] = []

    @workflow.state("charge", exception_policy="raise")
    def handler(ctx: Any) -> Any:
        observed.append(
            ctx.step(
                name="charge-customer:v1",
                run=lambda: {"charge_id": "ch_context"},
                to_state="warn",
            )
        )
        return "completion-result"

    count = workflow.handle_claimed_batch_count(
        "charge",
        [FlowRecord.from_resp(_record())],
    )

    assert count == 1
    assert observed == [{"charge_id": "ch_context"}]
    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
        "FLOW.COMPLETE",
    ]
    completed = executor.calls[2]
    assert completed[2] == b"lease-2"
    assert completed[completed.index("FENCING") + 1] == 8


def test_sync_workflow_context_advance_applies_continuation_with_refreshed_claim() -> None:
    executor = SequenceExecutor([[b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"], b"OK"])
    client = FlowClient(executor)
    workflow = FlowWorkflow(client, type="order", initial_state="charge")
    observed: list[ClaimedFlow] = []

    @workflow.state("charge", exception_policy="raise")
    def handler(ctx: Any) -> Any:
        observed.append(ctx.advance(to_state="warn"))
        return "completion-result"

    count = workflow.handle_claimed_batch_count(
        "charge",
        [FlowRecord.from_resp(_record())],
    )

    assert count == 1
    assert observed[0].run_state == "warn"
    assert [call[0] for call in executor.calls] == [
        "FLOW.STEP_CONTINUE",
        "FLOW.COMPLETE",
    ]
    assert executor.calls[1][2] == b"lease-2"
    assert executor.calls[1][executor.calls[1].index("FENCING") + 1] == 8


def test_sync_context_step_applies_waiting_outcome_with_refreshed_claim() -> None:
    executor = SequenceExecutor(
        [
            _record(),
            [b"flow-1", b"tenant-1", b"lease-2", 8, b"prepared"],
            b"OK",
        ]
    )
    workflow = FlowWorkflow(
        FlowClient(executor, codec=JsonCodec()),
        type="order",
        initial_state="charge",
    )

    @workflow.state("charge", exception_policy="raise")
    def handler(ctx: Any) -> Any:
        ctx.step(
            name="prepare-warning:v1",
            run=lambda: {"prepared": True},
            to_state="prepared",
        )
        return transition("waiting", run_at_ms=500)

    assert workflow.handle_claimed_batch_count("charge", [FlowRecord.from_resp(_record())]) == 1
    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
        "FLOW.TRANSITION",
    ]
    released = executor.calls[2]
    assert released[released.index("LEASE_TOKEN") + 1] == b"lease-2"
    assert released[released.index("FENCING") + 1] == 8
    assert released[2:4] == ("running", "waiting")
    assert released[released.index("RUN_AT") + 1] == 500


def test_sync_workflow_batch_uses_each_refreshed_claim_for_its_continuation() -> None:
    executor = SequenceExecutor(
        [
            _record(),
            [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
            _record(id=b"flow-2", lease=b"lease-3", fencing=9),
            [b"flow-2", b"tenant-1", b"lease-4", 10, b"warn"],
            b"OK",
            b"OK",
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())
    workflow = FlowWorkflow(client, type="order", initial_state="charge")

    @workflow.state("charge", exception_policy="raise")
    def handler(ctx: Any) -> Any:
        return ctx.step(
            name="charge-customer:v1",
            run=lambda: {"flow_id": ctx.id},
            to_state="warn",
        )

    count = workflow.handle_claimed_batch_count(
        "charge",
        [
            FlowRecord.from_resp(_record()),
            FlowRecord.from_resp(_record(id=b"flow-2", lease=b"lease-3", fencing=9)),
        ],
    )

    assert count == 2
    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
        "FLOW.COMPLETE",
        "FLOW.COMPLETE",
    ]
    assert executor.calls[4][2] == b"lease-2"
    assert executor.calls[5][2] == b"lease-4"


def test_sync_workflow_surfaces_handler_errors_after_a_step_without_stale_write() -> None:
    executor = SequenceExecutor(
        [
            _record(),
            [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())
    workflow = FlowWorkflow(client, type="order", initial_state="charge")

    @workflow.state("charge", exception_policy="retry")
    def handler(ctx: Any) -> Any:
        ctx.step(
            name="charge-customer:v1",
            run=lambda: {"charge_id": "ch_context"},
            to_state="warn",
        )
        raise RuntimeError("handler failed after durable commit")

    with pytest.raises(RuntimeError, match="failed after durable commit"):
        workflow.handle_claimed_batch_count(
            "charge",
            [FlowRecord.from_resp(_record())],
        )

    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
    ]


def test_sync_context_step_transport_uncertainty_surfaces_without_a_stale_retry() -> None:
    executor = SequenceExecutor([_record(), ConnectionError("commit response lost"), b"OK"])
    client = FlowClient(executor, codec=JsonCodec())
    workflow = FlowWorkflow(client, type="order", initial_state="charge")

    @workflow.state("charge", exception_policy="retry")
    def handler(ctx: Any) -> Any:
        return ctx.step(
            name="charge-customer:v1",
            run=lambda: {"charge_id": "ch_uncertain"},
            to_state="warn",
        )

    with pytest.raises(ConnectionError, match="commit response lost"):
        workflow.handle_claimed_batch_count("charge", [FlowRecord.from_resp(_record())])

    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
    ]


@pytest.mark.parametrize(
    "error",
    [
        HttpError(
            "connection reset after send",
            error_code="transport_error",
            retryable=True,
            safe_to_retry=False,
        ),
        HttpError(
            "malformed successful response",
            status_code=200,
            error_code="invalid_response",
            retryable=False,
            safe_to_retry=False,
        ),
        HttpError(
            "upstream response failed",
            status_code=500,
            error_code="internal_error",
            retryable=True,
            safe_to_retry=False,
        ),
    ],
)
def test_advance_treats_ambiguous_http_failures_as_outcome_unknown(error: HttpError) -> None:
    client = FlowClient(SequenceExecutor([error]))

    with pytest.raises(RequestOutcomeUnknownError) as raised:
        client.advance(_job(), to_state="warn")

    assert raised.value.raw is error


@pytest.mark.parametrize(
    "error",
    [
        HttpError(
            "unauthorized",
            status_code=401,
            error_code="unauthorized",
            retryable=False,
            safe_to_retry=False,
        ),
        HttpError(
            "request too large",
            error_code="request_too_large",
            retryable=False,
            safe_to_retry=False,
        ),
        HttpError(
            "capacity timeout before send",
            error_code="transport_timeout",
            retryable=True,
            safe_to_retry=True,
        ),
    ],
)
def test_advance_preserves_definite_http_rejections(error: HttpError) -> None:
    client = FlowClient(SequenceExecutor([error]))

    with pytest.raises(HttpError) as raised:
        client.advance(_job(), to_state="warn")

    assert raised.value is error


def test_sync_context_step_http_uncertainty_prevents_a_stale_retry() -> None:
    error = HttpError(
        "connection reset after send",
        error_code="transport_error",
        retryable=True,
        safe_to_retry=False,
    )
    executor = SequenceExecutor([_record(), error, b"OK"])
    workflow = FlowWorkflow(
        FlowClient(executor, codec=JsonCodec()),
        type="order",
        initial_state="charge",
    )

    @workflow.state("charge", exception_policy="retry")
    def handler(ctx: Any) -> Any:
        return ctx.step(
            name="charge-customer:v1",
            run=lambda: {"charge_id": "ch_uncertain"},
            to_state="warn",
        )

    with pytest.raises(HttpError) as raised:
        workflow.handle_claimed_batch_count("charge", [FlowRecord.from_resp(_record())])

    assert raised.value is error
    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
    ]


def test_sync_mixed_batch_applies_unrelated_outcome_before_post_commit_error() -> None:
    executor = SequenceExecutor(
        [
            _record(),
            [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
            b"OK",
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())
    workflow = FlowWorkflow(client, type="order", initial_state="charge")

    @workflow.state("charge", exception_policy="retry")
    def handler(ctx: Any) -> Any:
        if ctx.id == "flow-1":
            ctx.step(
                name="charge-customer:v1",
                run=lambda: {"charge_id": "ch_committed"},
                to_state="warn",
            )
            raise RuntimeError("post-commit handler failure")
        return "unrelated-result"

    jobs = [
        FlowRecord.from_resp(_record()),
        FlowRecord.from_resp(_record(id=b"flow-2", lease=b"lease-3", fencing=9)),
    ]
    with pytest.raises(RuntimeError, match="post-commit handler failure"):
        workflow.handle_claimed_batch_count("charge", jobs)

    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
        "FLOW.COMPLETE",
    ]


def test_sync_mixed_batch_applies_unrelated_outcome_before_uncertain_error() -> None:
    executor = SequenceExecutor([_record(), ConnectionError("mixed commit response lost"), b"OK"])
    workflow = FlowWorkflow(
        FlowClient(executor, codec=JsonCodec()),
        type="order",
        initial_state="charge",
    )

    @workflow.state("charge", exception_policy="retry")
    def handler(ctx: Any) -> Any:
        if ctx.id == "flow-1":
            return ctx.step(
                name="charge-customer:v1",
                run=lambda: {"charge_id": "ch_uncertain"},
                to_state="warn",
            )
        return "unrelated-result"

    jobs = [
        FlowRecord.from_resp(_record()),
        FlowRecord.from_resp(_record(id=b"flow-2", lease=b"lease-3", fencing=9)),
    ]
    with pytest.raises(ConnectionError, match="mixed commit response lost"):
        workflow.handle_claimed_batch_count("charge", jobs)

    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.STEP_CONTINUE",
        "FLOW.COMPLETE",
    ]


def test_workflow_context_replay_does_not_suppress_the_handlers_next_outcome() -> None:
    value_name = _step_value_name("charge-customer:v1")
    executor = SequenceExecutor(
        [
            _record(
                lease=b"lease-9",
                fencing=12,
                run_state=b"warn",
                value_refs={value_name.encode(): {b"ref": b"value-ref-1"}},
            ),
            [b'{"charge_id":"ch_replayed"}'],
            b"OK",
        ]
    )
    client = FlowClient(executor, codec=JsonCodec())
    workflow = FlowWorkflow(client, type="order", initial_state="warn")
    observed: list[Any] = []

    @workflow.state("warn", exception_policy="raise")
    def handler(ctx: Any) -> Any:
        observed.append(
            ctx.step(
                name="charge-customer:v1",
                run=lambda: pytest.fail("committed closure ran again"),
                to_state="warn",
            )
        )
        return "workflow-result"

    count = workflow.handle_claimed_batch_count(
        "warn",
        [
            FlowRecord.from_resp(
                _record(
                    lease=b"lease-9",
                    fencing=12,
                    run_state=b"warn",
                    value_refs={value_name.encode(): {b"ref": b"value-ref-1"}},
                )
            )
        ],
    )

    assert count == 1
    assert observed == [{"charge_id": "ch_replayed"}]
    assert [call[0] for call in executor.calls] == [
        "FLOW.EXTEND_LEASE",
        "FLOW.VALUE.MGET",
        "FLOW.COMPLETE_MANY",
    ]


def test_step_continue_remains_available_with_a_deprecation_warning() -> None:
    executor = SequenceExecutor([[b"flow-1", b"tenant-1", b"lease-2", 8]])
    client = FlowClient(executor)

    with pytest.deprecated_call(match="advance"):
        client.step_continue(
            "flow-1",
            lease_token=b"lease-1",
            from_state="charge",
            to_state="warn",
            fencing_token=7,
            return_job=True,
            now_ms=101,
        )


def test_async_advance_and_step_match_sync_semantics() -> None:
    async def scenario() -> None:
        advance_executor = AsyncSequenceExecutor([[b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"]])
        advance_client = AsyncFlowClient(advance_executor, codec=JsonCodec())
        advanced = await advance_client.advance(
            _job(), to_state="warn", lease_ms=45_000, now_ms=101
        )
        assert advanced.run_state == "warn"
        assert advanced.lease_token == b"lease-2"

        step_executor = AsyncSequenceExecutor(
            [
                _record(),
                [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
            ]
        )
        step_client = AsyncFlowClient(step_executor, codec=JsonCodec())
        executions = 0

        async def charge() -> dict[str, str]:
            nonlocal executions
            await asyncio.sleep(0)
            executions += 1
            return {"charge_id": "ch_async"}

        refreshed, result = await step_client.step(
            _job(),
            name="charge-customer:v1",
            run=charge,
            to_state="warn",
            lease_ms=45_000,
            now_ms=101,
        )
        assert executions == 1
        assert result == {"charge_id": "ch_async"}
        assert refreshed.run_state == "warn"
        assert [call[0] for call in step_executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.STEP_CONTINUE",
        ]

        replay_executor = AsyncSequenceExecutor(
            [
                _record(
                    lease=b"lease-9",
                    fencing=12,
                    run_state=b"warn",
                    value_refs={
                        _step_value_name("charge-customer:v1").encode(): {b"ref": b"value-ref-1"}
                    },
                ),
                [b'{"charge_id":"ch_async"}'],
            ]
        )
        replay_client = AsyncFlowClient(replay_executor, codec=JsonCodec())
        current = ClaimedFlow(
            "flow-1",
            b"lease-9",
            12,
            partition_key="tenant-1",
            run_state="warn",
        )

        async def must_not_run() -> Any:
            raise AssertionError("committed closure ran again")

        replayed, replayed_result = await replay_client.step(
            current,
            name="charge-customer:v1",
            run=must_not_run,
            to_state="warn",
            now_ms=200,
        )
        assert replayed.run_state == "warn"
        assert replayed_result == {"charge_id": "ch_async"}
        assert [call[0] for call in replay_executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.VALUE.MGET",
        ]

        stale_executor = AsyncSequenceExecutor([StaleLeaseError("stale flow lease")])
        stale_client = AsyncFlowClient(stale_executor)
        ran = False

        async def stale_closure() -> bytes:
            nonlocal ran
            ran = True
            return b"result"

        with pytest.raises(StaleLeaseError):
            await stale_client.step(
                _job(),
                name="charge-customer:v1",
                run=stale_closure,
                to_state="warn",
            )
        assert ran is False
        assert [call[0] for call in stale_executor.calls] == ["FLOW.EXTEND_LEASE"]

    asyncio.run(scenario())


def test_async_workflow_context_step_applies_continuation_with_refreshed_claim() -> None:
    async def scenario() -> None:
        executor = AsyncSequenceExecutor(
            [
                _record(),
                [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
                [b"OK"],
            ]
        )
        client = AsyncFlowClient(executor, codec=JsonCodec())
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["charge"],
            initial_state="charge",
            partition_key="tenant-1",
            batch_size=1,
        )
        observed: list[Any] = []

        @workflow.state("charge", exception_policy="raise")
        async def handler(ctx: Any) -> Any:
            observed.append(
                await ctx.step(
                    name="charge-customer:v1",
                    run=lambda: {"charge_id": "ch_async_context"},
                    to_state="warn",
                )
            )
            return "completion-result"

        count = await workflow._handle_claimed_batch(
            "charge",
            [
                ClaimedFlow(
                    "flow-1",
                    b"lease-1",
                    7,
                    partition_key="tenant-1",
                    type="order",
                    run_state="charge",
                )
            ],
        )

        assert count == 1
        assert observed == [{"charge_id": "ch_async_context"}]
        assert [call[0] for call in executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.STEP_CONTINUE",
            "FLOW.COMPLETE_MANY",
        ]
        completed = executor.calls[2]
        items_index = completed.index("ITEMS")
        assert completed[items_index + 1 :] == ("flow-1", b"lease-2", 8)

    asyncio.run(scenario())


def test_async_context_step_applies_waiting_outcome_with_refreshed_claim() -> None:
    async def scenario() -> None:
        executor = AsyncSequenceExecutor(
            [
                _record(),
                [b"flow-1", b"tenant-1", b"lease-2", 8, b"prepared"],
                [b"OK"],
            ]
        )
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor, codec=JsonCodec()),
            type="order",
            states=["charge"],
            initial_state="charge",
            partition_key="tenant-1",
            batch_size=1,
        )

        @workflow.state("charge", exception_policy="raise")
        async def handler(ctx: Any) -> Any:
            await ctx.step(
                name="prepare-warning:v1",
                run=lambda: {"prepared": True},
                to_state="prepared",
            )
            return transition("waiting", run_at_ms=500)

        assert await workflow._handle_claimed_batch("charge", [_job()]) == 1
        assert [call[0] for call in executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.STEP_CONTINUE",
            "FLOW.TRANSITION_MANY",
        ]
        released = executor.calls[2]
        items_index = released.index("ITEMS")
        assert released[items_index + 1 :] == ("flow-1", 8, b"lease-2")
        assert released[released.index("RUN_AT") + 1] == 500

    asyncio.run(scenario())


def test_async_workflow_surfaces_handler_errors_after_step_without_stale_write() -> None:
    async def scenario() -> None:
        executor = AsyncSequenceExecutor(
            [
                _record(),
                [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
            ]
        )
        client = AsyncFlowClient(executor, codec=JsonCodec())
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["charge"],
            initial_state="charge",
            partition_key="tenant-1",
            batch_size=1,
        )

        @workflow.state("charge", exception_policy="retry")
        async def handler(ctx: Any) -> Any:
            await ctx.step(
                name="charge-customer:v1",
                run=lambda: {"charge_id": "ch_async_context"},
                to_state="warn",
            )
            raise RuntimeError("async handler failed after durable commit")

        with pytest.raises(RuntimeError, match="failed after durable commit"):
            await workflow._handle_claimed_batch(
                "charge",
                [
                    ClaimedFlow(
                        "flow-1",
                        b"lease-1",
                        7,
                        partition_key="tenant-1",
                        type="order",
                        run_state="charge",
                    )
                ],
            )

        assert [call[0] for call in executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.STEP_CONTINUE",
        ]

    asyncio.run(scenario())


def test_async_context_advance_transport_uncertainty_surfaces_without_stale_retry() -> None:
    async def scenario() -> None:
        executor = AsyncSequenceExecutor([ConnectionError("advance response lost"), [b"OK"]])
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["charge"],
            initial_state="charge",
            partition_key="tenant-1",
            batch_size=1,
        )

        @workflow.state("charge", exception_policy="retry")
        async def handler(ctx: Any) -> Any:
            return await ctx.advance(to_state="warn")

        with pytest.raises(ConnectionError, match="advance response lost"):
            await workflow._handle_claimed_batch("charge", [_job()])

        assert [call[0] for call in executor.calls] == ["FLOW.STEP_CONTINUE"]

    asyncio.run(scenario())


def test_async_context_advance_http_uncertainty_prevents_a_stale_retry() -> None:
    async def scenario() -> None:
        error = HttpError(
            "malformed successful response",
            status_code=200,
            error_code="invalid_response",
            retryable=False,
            safe_to_retry=False,
        )
        executor = AsyncSequenceExecutor([error, [b"OK"]])
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["charge"],
            initial_state="charge",
            partition_key="tenant-1",
            batch_size=1,
        )

        @workflow.state("charge", exception_policy="retry")
        async def handler(ctx: Any) -> Any:
            return await ctx.advance(to_state="warn")

        with pytest.raises(HttpError) as raised:
            await workflow._handle_claimed_batch("charge", [_job()])

        assert raised.value is error
        assert [call[0] for call in executor.calls] == ["FLOW.STEP_CONTINUE"]

    asyncio.run(scenario())


def test_async_mixed_batch_applies_unrelated_outcome_before_post_commit_error() -> None:
    async def scenario() -> None:
        executor = AsyncSequenceExecutor(
            [
                _record(),
                [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"],
                [b"OK"],
            ]
        )
        client = AsyncFlowClient(executor, codec=JsonCodec())
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["charge"],
            initial_state="charge",
            partition_key="tenant-1",
            batch_size=2,
            concurrency=1,
        )

        @workflow.state("charge", exception_policy="retry")
        async def handler(ctx: Any) -> Any:
            if ctx.id == "flow-1":
                await ctx.step(
                    name="charge-customer:v1",
                    run=lambda: {"charge_id": "ch_committed"},
                    to_state="warn",
                )
                raise RuntimeError("async post-commit handler failure")
            return "unrelated-result"

        second = ClaimedFlow(
            "flow-2",
            b"lease-3",
            9,
            partition_key="tenant-1",
            type="order",
            run_state="charge",
        )
        with pytest.raises(RuntimeError, match="async post-commit handler failure"):
            await workflow._handle_claimed_batch("charge", [_job(), second])

        assert [call[0] for call in executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.STEP_CONTINUE",
            "FLOW.COMPLETE_MANY",
        ]

    asyncio.run(scenario())


def test_async_mixed_batch_applies_unrelated_outcome_before_uncertain_error() -> None:
    async def scenario() -> None:
        executor = AsyncSequenceExecutor(
            [_record(), ConnectionError("async mixed response lost"), [b"OK"]]
        )
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor, codec=JsonCodec()),
            type="order",
            states=["charge"],
            initial_state="charge",
            partition_key="tenant-1",
            batch_size=2,
            concurrency=1,
        )

        @workflow.state("charge", exception_policy="retry")
        async def handler(ctx: Any) -> Any:
            if ctx.id == "flow-1":
                return await ctx.step(
                    name="charge-customer:v1",
                    run=lambda: {"charge_id": "ch_uncertain"},
                    to_state="warn",
                )
            return "unrelated-result"

        second = ClaimedFlow(
            "flow-2",
            b"lease-3",
            9,
            partition_key="tenant-1",
            type="order",
            run_state="charge",
        )
        with pytest.raises(ConnectionError, match="async mixed response lost"):
            await workflow._handle_claimed_batch("charge", [_job(), second])

        assert [call[0] for call in executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.STEP_CONTINUE",
            "FLOW.COMPLETE_MANY",
        ]

    asyncio.run(scenario())


def test_async_durable_result_and_journal_validation_matches_sync() -> None:
    async def scenario() -> None:
        malformed_executor = AsyncSequenceExecutor([_record(value_refs=[])])
        malformed_client = AsyncFlowClient(malformed_executor)
        with pytest.raises(FerricStoreError, match="value_refs"):
            await malformed_client.step(
                _job(),
                name="charge-customer:v1",
                run=lambda: pytest.fail("malformed async closure ran"),
                to_state="warn",
            )

        value_name = _step_value_name("charge-customer:v1")
        missing_executor = AsyncSequenceExecutor(
            [
                _record(
                    run_state=b"warn",
                    value_refs={value_name.encode(): {b"ref": b"value-ref-1"}},
                ),
                [None],
            ]
        )
        missing_client = AsyncFlowClient(missing_executor, codec=JsonCodec())
        with pytest.raises(FerricStoreError, match="missing or omitted"):
            await missing_client.step(
                _job(run_state="warn"),
                name="charge-customer:v1",
                run=lambda: pytest.fail("missing async closure ran"),
                to_state="warn",
            )

        inconsistent_client = AsyncFlowClient(
            AsyncSequenceExecutor(
                [_record(value_refs={value_name.encode(): {b"ref": b"value-ref-1"}})]
            ),
            codec=JsonCodec(),
        )
        with pytest.raises(FerricStoreError, match="target state"):
            await inconsistent_client.step(
                _job(),
                name="charge-customer:v1",
                run=lambda: pytest.fail("inconsistent async closure ran"),
                to_state="warn",
            )

        raw_executor = AsyncSequenceExecutor(
            [_record(), [b"flow-1", b"tenant-1", b"lease-2", 8, b"warn"]]
        )
        _refreshed, normalized = await AsyncFlowClient(raw_executor).step(
            _job(),
            name="charge-customer:v1",
            run=lambda: "charged",
            to_state="warn",
        )
        assert normalized == b"charged"

    asyncio.run(scenario())


def test_async_workflow_context_replay_allows_the_handlers_next_outcome() -> None:
    async def scenario() -> None:
        value_name = _step_value_name("charge-customer:v1")
        executor = AsyncSequenceExecutor(
            [
                _record(
                    lease=b"lease-9",
                    fencing=12,
                    run_state=b"warn",
                    value_refs={value_name.encode(): {b"ref": b"value-ref-1"}},
                ),
                [b'{"charge_id":"ch_async_replayed"}'],
                b"OK",
            ]
        )
        client = AsyncFlowClient(executor, codec=JsonCodec())
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["warn"],
            initial_state="warn",
            partition_key="tenant-1",
            batch_size=1,
        )
        observed: list[Any] = []

        @workflow.state("warn", exception_policy="raise")
        async def handler(ctx: Any) -> Any:
            observed.append(
                await ctx.step(
                    name="charge-customer:v1",
                    run=lambda: pytest.fail("committed closure ran again"),
                    to_state="warn",
                )
            )
            return "workflow-result"

        count = await workflow._handle_claimed_batch(
            "warn",
            [
                ClaimedFlow(
                    "flow-1",
                    b"lease-9",
                    12,
                    partition_key="tenant-1",
                    type="order",
                    run_state="warn",
                )
            ],
        )

        assert count == 1
        assert observed == [{"charge_id": "ch_async_replayed"}]
        assert [call[0] for call in executor.calls] == [
            "FLOW.EXTEND_LEASE",
            "FLOW.VALUE.MGET",
            "FLOW.COMPLETE_MANY",
        ]

    asyncio.run(scenario())
