from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from ferricstore.client_core import FlowClient
from ferricstore.mutation_core import MutationBatchPlan, MutationKind
from ferricstore.types import ClaimedFlow, FlowRecord
from ferricstore.worker_core import CloseDeadline, CloseTimeoutError, validate_many_result
from ferricstore.worker_models import QueueFlowWorkerResult

FlowJob = ClaimedFlow | FlowRecord


@dataclass
class _HandledBatch:
    jobs: list[FlowJob]
    first_result: Any = None
    mixed_results: list[tuple[FlowJob, Any]] | None = None
    failures: list[tuple[FlowJob, Exception]] | None = None


class SyncWorkerCompletionMixin:
    """Own completion draining and terminal mutation policy for a sync worker."""

    if TYPE_CHECKING:
        client: FlowClient
        complete_independent: bool
        on_error: str
        _completion_clients: list[FlowClient]
        _completion_client_index: int
        _pending_completions: deque[Future[QueueFlowWorkerResult]]

    def _finish_batch(
        self,
        handled: _HandledBatch,
        client: FlowClient,
    ) -> QueueFlowWorkerResult:
        completed = self._complete_successes(handled, client)
        retried, failed = self._handle_failures(handled.failures or [], client)
        return QueueFlowWorkerResult(completed=completed, retried=retried, failed=failed)

    def _next_completion_client(self) -> FlowClient:
        client = self._completion_clients[
            self._completion_client_index % len(self._completion_clients)
        ]
        self._completion_client_index += 1
        return client

    def _drain_pending_completions(
        self,
        *,
        block: bool,
        limit: int | None = None,
        deadline: CloseDeadline | None = None,
    ) -> QueueFlowWorkerResult:
        if not self._pending_completions:
            return QueueFlowWorkerResult()

        claimed = completed = retried = failed = claim_calls = 0
        if block:
            remaining = (
                len(self._pending_completions)
                if limit is None
                else min(limit, len(self._pending_completions))
            )
            for _ in range(remaining):
                future = self._pending_completions[0]
                try:
                    value = (
                        deadline.future_result(future, "queue worker close timed out")
                        if deadline is not None
                        else future.result()
                    )
                except CloseTimeoutError:
                    raise
                except BaseException:
                    self._pending_completions.popleft()
                    raise
                self._pending_completions.popleft()
                claimed += value.claimed
                completed += value.completed
                retried += value.retried
                failed += value.failed
                claim_calls += value.claim_calls
        else:
            retained: deque[Future[QueueFlowWorkerResult]] = deque()
            count = 0
            while self._pending_completions:
                future = self._pending_completions.popleft()
                if future.done() and (limit is None or count < limit):
                    try:
                        value = future.result()
                    except BaseException:
                        retained.extend(self._pending_completions)
                        self._pending_completions = retained
                        raise
                    claimed += value.claimed
                    completed += value.completed
                    retried += value.retried
                    failed += value.failed
                    claim_calls += value.claim_calls
                    count += 1
                else:
                    retained.append(future)
            self._pending_completions = retained
        return QueueFlowWorkerResult(
            claimed=claimed,
            completed=completed,
            retried=retried,
            failed=failed,
            claim_calls=claim_calls,
        )

    @staticmethod
    def _merge_results(
        left: QueueFlowWorkerResult, right: QueueFlowWorkerResult
    ) -> QueueFlowWorkerResult:
        return QueueFlowWorkerResult(
            claimed=left.claimed + right.claimed,
            completed=left.completed + right.completed,
            retried=left.retried + right.retried,
            failed=left.failed + right.failed,
            claim_calls=left.claim_calls + right.claim_calls,
        )

    def _complete_successes(
        self,
        handled: _HandledBatch,
        client: FlowClient,
    ) -> int:
        if not handled.jobs:
            return 0

        if handled.mixed_results is None:
            response = client.complete_jobs(
                cast(list[ClaimedFlow], handled.jobs),
                result=handled.first_result,
                independent=self.complete_independent,
            )
            validate_many_result(
                response,
                len(handled.jobs),
                operation="FLOW.COMPLETE_MANY",
            )
            return len(handled.jobs)

        complete_job_results = getattr(client, "complete_job_results", None)
        if callable(complete_job_results):
            response = complete_job_results(
                cast(list[tuple[ClaimedFlow, Any]], handled.mixed_results)
            )
            validate_many_result(
                response,
                len(handled.mixed_results),
                operation="FLOW.COMPLETE batch",
            )
        else:
            for job, result in handled.mixed_results:
                client.complete(
                    job.id,
                    lease_token=job.lease_token,
                    fencing_token=job.fencing_token,
                    partition_key=job.partition_key,
                    result=result,
                    return_record=False,
                )
        return len(handled.jobs)

    def _handle_failures(
        self,
        failures: list[tuple[FlowJob, Exception]],
        client: FlowClient,
    ) -> tuple[int, int]:
        if not failures:
            return 0, 0

        jobs = [job for job, _exc in failures]
        if self.on_error == "raise":
            raise failures[0][1]

        grouped: dict[str, list[FlowJob]] = {}
        for job, exc in failures:
            grouped.setdefault(str(exc), []).append(job)

        mutation_kind = MutationKind.FAIL if self.on_error == "fail" else MutationKind.RETRY
        apply_job_mutations = getattr(client, "apply_job_mutations", None)
        if len(grouped) > 1 and callable(apply_job_mutations):
            plan = MutationBatchPlan.failures(failures, kind=mutation_kind)
            response = apply_job_mutations(plan.mutations)
            validate_many_result(
                response,
                len(plan),
                operation="Flow failure mutation batch",
            )
            return (0, len(jobs)) if self.on_error == "fail" else (len(jobs), 0)

        if self.on_error == "fail":
            for message, group_jobs in grouped.items():
                response = client.fail_many(
                    None,
                    cast(list[ClaimedFlow], group_jobs),
                    error=message,
                    independent=self.complete_independent,
                )
                validate_many_result(
                    response,
                    len(group_jobs),
                    operation="FLOW.FAIL_MANY",
                )
            return 0, len(jobs)

        for message, group_jobs in grouped.items():
            response = client.retry_many(
                None,
                cast(list[ClaimedFlow], group_jobs),
                error=message,
                independent=self.complete_independent,
            )
            validate_many_result(
                response,
                len(group_jobs),
                operation="FLOW.RETRY_MANY",
            )
        return len(jobs), 0


__all__ = ["FlowJob", "SyncWorkerCompletionMixin", "_HandledBatch"]
