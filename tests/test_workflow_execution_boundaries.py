from __future__ import annotations

from typing import Any

import pytest

from ferricstore.errors import FerricStoreError
from ferricstore.types import ClaimedFlow
from ferricstore.worker_core import validate_many_result
from ferricstore.workflow_execution import apply_uniform_batch
from ferricstore.workflow_models import FLOW_MANY_BATCH_LIMIT
from ferricstore.workflow_types import (
    Complete,
    Fail,
    Retry,
    Transition,
    complete,
    retry,
    transition,
)


def _jobs(count: int, *, mixed_states: bool = False) -> list[ClaimedFlow]:
    return [
        ClaimedFlow(
            f"job-{index}",
            f"lease-{index}".encode(),
            index,
            state="other" if mixed_states and index % 2 else "running",
            partition_key="tenant-a",
        )
        for index in range(count)
    ]


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[ClaimedFlow]]] = []
        self.partial = False

    def _response(self, name: str, jobs: list[ClaimedFlow]) -> list[bytes]:
        self.calls.append((name, list(jobs)))
        count = max(len(jobs) - 1, 0) if self.partial else len(jobs)
        return [job.id.encode() for job in jobs[:count]]

    def transition_many(
        self, _partition: str | None, *, items: list[ClaimedFlow], **_kw: Any
    ) -> list[bytes]:
        return self._response("transition_many", items)

    def complete_many(
        self, _partition: str | None, jobs: list[ClaimedFlow], **_kw: Any
    ) -> list[bytes]:
        return self._response("complete_many", jobs)

    def retry_many(
        self, _partition: str | None, jobs: list[ClaimedFlow], **_kw: Any
    ) -> list[bytes]:
        return self._response("retry_many", jobs)

    def fail_many(self, _partition: str | None, jobs: list[ClaimedFlow], **_kw: Any) -> list[bytes]:
        return self._response("fail_many", jobs)


class _Host:
    def __init__(self) -> None:
        self.client = _Client()
        self.applied: list[str] = []

    def _apply_uniform_batch(
        self,
        jobs: list[ClaimedFlow],
        state_name: str,
        outcome: object,
        *,
        materialize: bool = True,
    ) -> list[bytes] | int:
        return apply_uniform_batch(
            self,  # type: ignore[arg-type]
            jobs,
            state_name,
            outcome,  # type: ignore[arg-type]
            materialize=materialize,
        )

    @staticmethod
    def _uniform_partition_key(_jobs: list[ClaimedFlow]) -> str:
        return "tenant-a"

    @staticmethod
    def _uniform_current_state(jobs: list[ClaimedFlow]) -> str | None:
        state = jobs[0].state
        return state if all(job.state == state for job in jobs) else None

    @staticmethod
    def _validate_transition_policy(_outcome: Transition) -> None:
        return None

    @staticmethod
    def _batch_response_list(response: object, expected: int, *, operation: str) -> list[bytes]:
        return validate_many_result(response, expected, operation=operation)

    def apply(self, job: ClaimedFlow, _outcome: object, *, state_name: str) -> bytes:
        self.applied.append(job.id)
        return job.id.encode()


def test_materialized_uniform_workflow_batch_chunks_without_reordering() -> None:
    host = _Host()
    jobs = _jobs(FLOW_MANY_BATCH_LIMIT + 7)

    result = apply_uniform_batch(host, jobs, "queued", complete(result=b"ok"))  # type: ignore[arg-type]

    assert result == [job.id.encode() for job in jobs]
    assert [len(items) for _name, items in host.client.calls] == [FLOW_MANY_BATCH_LIMIT, 7]


def test_count_only_uniform_workflow_batch_chunks_and_sums_results() -> None:
    host = _Host()
    jobs = _jobs(FLOW_MANY_BATCH_LIMIT + 7)

    result = apply_uniform_batch(
        host,  # type: ignore[arg-type]
        jobs,
        "queued",
        retry(error="later"),
        materialize=False,
    )

    assert result == len(jobs)
    assert [len(items) for _name, items in host.client.calls] == [FLOW_MANY_BATCH_LIMIT, 7]


def test_mixed_current_states_fall_back_to_individual_transitions() -> None:
    host = _Host()
    jobs = _jobs(4, mixed_states=True)

    result = apply_uniform_batch(host, jobs, "queued", transition("done"))  # type: ignore[arg-type]

    assert result == [job.id.encode() for job in jobs]
    assert host.applied == [job.id for job in jobs]
    assert host.client.calls == []


@pytest.mark.parametrize(
    ("outcome", "method"),
    [
        (Transition("done"), "transition_many"),
        (Complete(result=b"ok"), "complete_many"),
        (Retry(error="later"), "retry_many"),
        (Fail(error="bad"), "fail_many"),
    ],
)
def test_count_only_workflow_batches_still_validate_response_cardinality(
    outcome: Transition | Complete | Retry | Fail,
    method: str,
) -> None:
    host = _Host()
    host.client.partial = True
    jobs = _jobs(2)

    with pytest.raises(FerricStoreError, match="returned 1 items; expected 2"):
        apply_uniform_batch(
            host,  # type: ignore[arg-type]
            jobs,
            "queued",
            outcome,
            materialize=False,
        )

    assert host.client.calls[0][0] == method


def test_scalar_handler_result_is_normalized_to_completion() -> None:
    host = _Host()
    jobs = _jobs(2)

    result = apply_uniform_batch(host, jobs, "queued", b"result")  # type: ignore[arg-type]

    assert result == [job.id.encode() for job in jobs]
    assert host.client.calls[0][0] == "complete_many"
