from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from ferricstore.mutation_core import MutationBatchPlan, MutationKind
from ferricstore.types import ClaimedFlow, FlowRecord
from ferricstore.worker_core import validate_many_result
from ferricstore.worker_models import QueueFlowWorkerResult

if TYPE_CHECKING:
    from ferricstore.async_client_core import AsyncFlowClient

AsyncFlowJob = ClaimedFlow | FlowRecord


@dataclass
class _AsyncHandledBatch:
    jobs: list[AsyncFlowJob]
    first_result: Any = None
    mixed_results: list[tuple[AsyncFlowJob, Any]] | None = None
    failures: list[tuple[AsyncFlowJob, Exception]] | None = None


class AsyncWorkerCompletionMixin:
    """Own async completion and terminal error mutation policy."""

    if TYPE_CHECKING:
        client: AsyncFlowClient
        complete_independent: bool
        on_error: str

    async def _finish_batch(self, handled: _AsyncHandledBatch) -> QueueFlowWorkerResult:
        completed = await self._complete_successes(handled)
        retried, failed = await self._handle_failures(handled.failures or [])
        return QueueFlowWorkerResult(completed=completed, retried=retried, failed=failed)

    async def _complete_successes(self, handled: _AsyncHandledBatch) -> int:
        if not handled.jobs:
            return 0

        if handled.mixed_results is None:
            response = await self.client.complete_jobs(
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

        complete_job_results = getattr(self.client, "complete_job_results", None)
        if callable(complete_job_results):
            response = await complete_job_results(
                cast(list[tuple[ClaimedFlow, Any]], handled.mixed_results)
            )
            validate_many_result(
                response,
                len(handled.mixed_results),
                operation="FLOW.COMPLETE batch",
            )
        else:
            for job, result in handled.mixed_results:
                await self.client.complete(
                    job.id,
                    lease_token=job.lease_token,
                    fencing_token=job.fencing_token,
                    partition_key=job.partition_key,
                    result=result,
                    return_record=False,
                )
        return len(handled.jobs)

    async def _handle_failures(
        self,
        failures: list[tuple[AsyncFlowJob, Exception]],
    ) -> tuple[int, int]:
        if not failures:
            return 0, 0

        if self.on_error == "raise":
            raise failures[0][1]

        groups: dict[str, list[AsyncFlowJob]] = {}
        for job, exc in failures:
            message = str(exc)
            groups.setdefault(message, []).append(job)

        mutation_kind = MutationKind.FAIL if self.on_error == "fail" else MutationKind.RETRY
        apply_job_mutations = getattr(self.client, "apply_job_mutations", None)
        if len(groups) > 1 and callable(apply_job_mutations):
            plan = MutationBatchPlan.failures(failures, kind=mutation_kind)
            response = apply_job_mutations(plan.mutations)
            if inspect.isawaitable(response):
                response = await response
            validate_many_result(
                response,
                len(plan),
                operation="Flow failure mutation batch",
            )
            return (0, len(failures)) if self.on_error == "fail" else (len(failures), 0)

        if self.on_error == "fail":
            failed = 0
            for message, jobs in groups.items():
                response = await self.client.fail_many(
                    None,
                    cast(list[ClaimedFlow], jobs),
                    error=message,
                    independent=self.complete_independent,
                )
                validate_many_result(
                    response,
                    len(jobs),
                    operation="FLOW.FAIL_MANY",
                )
                failed += len(jobs)
            return 0, failed

        retried_jobs: list[AsyncFlowJob] = []
        for message, jobs in groups.items():
            response = await self.client.retry_many(
                None,
                cast(list[ClaimedFlow], jobs),
                error=message,
                independent=self.complete_independent,
            )
            validate_many_result(
                response,
                len(jobs),
                operation="FLOW.RETRY_MANY",
            )
            retried_jobs.extend(jobs)
        return len(retried_jobs), 0


__all__ = [
    "AsyncFlowJob",
    "AsyncWorkerCompletionMixin",
    "_AsyncHandledBatch",
]
