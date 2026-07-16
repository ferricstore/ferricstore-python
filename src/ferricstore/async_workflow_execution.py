from __future__ import annotations

import inspect
from collections.abc import Sequence
from typing import Any, Protocol, cast

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_queue_runtime import AsyncErrorMode, AsyncFlowJob, AsyncWorkflowHandler
from ferricstore.async_workflow_context import AsyncWorkflowContext
from ferricstore.batch_core import BatchValueMatcher, run_async_fanout
from ferricstore.lifecycle_core import raise_primary_with_cleanup
from ferricstore.mutation_core import JobMutation, MutationBatchPlan
from ferricstore.types import BudgetPolicy, ClaimedFlow, FencedItem, FlowStateMode
from ferricstore.worker_core import validate_many_result
from ferricstore.workflow_mutations import complete_mutation_options
from ferricstore.workflow_types import Complete, Fail, Retry, Transition, fail, retry


class AsyncWorkflowExecutionHost(Protocol):
    """Async workflow state required by the extracted batch algorithms."""

    client: AsyncFlowClient
    handlers: dict[str, AsyncWorkflowHandler]
    error_modes: dict[str, AsyncErrorMode]
    on_error: AsyncErrorMode
    budget_policies: dict[str, BudgetPolicy]
    concurrency: int
    state_modes: dict[str, str]

    def _merge_governance_attributes(
        self,
        value: Any,
        attributes: dict[str, Any],
    ) -> Transition | Complete | Retry | Fail: ...

    def _job_mutation(
        self,
        job: AsyncFlowJob,
        outcome: Transition | Complete | Retry | Fail,
    ) -> JobMutation: ...

    async def _apply_uniform(
        self,
        state_name: str,
        jobs: list[ClaimedFlow],
        outcome: Transition | Complete | Retry | Fail,
    ) -> None: ...

    def _uniform_partition_key(self, jobs: list[ClaimedFlow]) -> str | None: ...


async def handle_claimed_batch(
    self: AsyncWorkflowExecutionHost, state_name: str, jobs: Sequence[AsyncFlowJob]
) -> int:
    if not jobs:
        return 0

    handler = self.handlers.get(state_name)
    if handler is None:
        raise ValueError(f"no handler for workflow state: {state_name!r}")

    on_error = self.error_modes.get(state_name, self.on_error)

    async def run_one(job: AsyncFlowJob) -> Transition | Complete | Retry | Fail:
        ctx = AsyncWorkflowContext(cast(Any, self), job, state_name)
        budget = None
        try:
            budget = ctx._state_budget(self.budget_policies.get(state_name))
            if budget is not None:
                await budget.__aenter__()
            value = handler(ctx)
            if inspect.isawaitable(value):
                value = await value
            if budget is not None:
                await budget.commit()
        except BaseException as exc:
            cleanup_error: BaseException | None = None
            if budget is not None and budget.is_open:
                try:
                    await budget.release()
                except BaseException as cleanup:
                    cleanup_error = cleanup
            if cleanup_error is not None:
                try:
                    raise_primary_with_cleanup(exc, exc.__traceback__, cleanup_error)
                except BaseException as preserved:
                    exc = preserved
            if not isinstance(exc, Exception):
                raise exc
            if on_error == "raise":
                raise exc
            value = fail(error=str(exc)) if on_error == "fail" else retry(error=str(exc))
            return self._merge_governance_attributes(value, ctx._governance_attributes)
        return self._merge_governance_attributes(value, ctx._governance_attributes)

    outcomes = await run_async_fanout(
        jobs,
        run_one,
        concurrent=True,
        max_concurrency=self.concurrency,
        stop_on_error=on_error == "raise",
    )

    first = outcomes[0]
    first_matcher = BatchValueMatcher(first)
    if all(first_matcher.matches(outcome) for outcome in outcomes):
        await self._apply_uniform(state_name, cast(list[ClaimedFlow], jobs), first)
        return len(jobs)

    apply_job_mutations = getattr(self.client, "apply_job_mutations", None)
    if callable(apply_job_mutations):
        plan = MutationBatchPlan.build(
            self._job_mutation(job, outcome) for job, outcome in zip(jobs, outcomes, strict=True)
        )
        response = apply_job_mutations(plan.mutations)
        if inspect.isawaitable(response):
            response = await response
        validate_many_result(
            response,
            len(plan),
            operation="Flow workflow mutation batch",
        )
        return len(plan)

    complete_job_mutations = getattr(self.client, "complete_job_mutations", None)
    if callable(complete_job_mutations) and all(
        isinstance(outcome, Complete) for outcome in outcomes
    ):
        response = complete_job_mutations(
            [
                (
                    cast(ClaimedFlow, job),
                    complete_mutation_options(cast(Complete, outcome)),
                )
                for job, outcome in zip(jobs, outcomes, strict=True)
            ]
        )
        if inspect.isawaitable(response):
            response = await response
        validate_many_result(
            response,
            len(jobs),
            operation="FLOW.COMPLETE batch",
        )
        return len(jobs)

    for job, outcome in zip(jobs, outcomes, strict=True):
        await self._apply_uniform(state_name, [cast(ClaimedFlow, job)], outcome)
    return len(jobs)


async def apply_uniform(
    self: AsyncWorkflowExecutionHost,
    state_name: str,
    jobs: list[ClaimedFlow],
    outcome: Transition | Complete | Retry | Fail,
) -> None:
    partition_key = self._uniform_partition_key(jobs)
    if isinstance(outcome, Transition):
        if (
            self.state_modes.get(outcome.to_state) == FlowStateMode.FIFO.value
            and outcome.priority is not None
        ):
            raise ValueError("priority is not supported for fifo state")
        items = [
            FencedItem(
                id=job.id,
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=job.partition_key,
            )
            for job in jobs
        ]
        response = await self.client.transition_many(
            partition_key,
            from_state="running",
            to_state=outcome.to_state,
            items=items,
            payload=outcome.payload,
            priority=outcome.priority,
            values=outcome.values,
            value_refs=outcome.value_refs,
            drop_values=outcome.drop_values,
            override_values=outcome.override_values,
            attributes_merge=outcome.attributes_merge,
            state_meta=outcome.state_meta,
            run_at_ms=outcome.run_at_ms,
            independent=True,
        )
        validate_many_result(
            response,
            len(jobs),
            operation="FLOW.TRANSITION_MANY",
        )
        return
    if isinstance(outcome, Complete):
        response = await self.client.complete_many(
            partition_key,
            jobs,
            result=outcome.result,
            payload=outcome.payload,
            ttl_ms=outcome.ttl_ms,
            values=outcome.values,
            value_refs=outcome.value_refs,
            drop_values=outcome.drop_values,
            override_values=outcome.override_values,
            attributes_merge=outcome.attributes_merge,
            state_meta=outcome.state_meta,
            independent=True,
        )
        validate_many_result(
            response,
            len(jobs),
            operation="FLOW.COMPLETE_MANY",
        )
        return
    if isinstance(outcome, Retry):
        response = await self.client.retry_many(
            partition_key,
            jobs,
            error=outcome.error,
            payload=outcome.payload,
            values=outcome.values,
            value_refs=outcome.value_refs,
            drop_values=outcome.drop_values,
            override_values=outcome.override_values,
            attributes_merge=outcome.attributes_merge,
            state_meta=outcome.state_meta,
            run_at_ms=outcome.run_at_ms,
            independent=True,
        )
        validate_many_result(
            response,
            len(jobs),
            operation="FLOW.RETRY_MANY",
        )
        return
    response = await self.client.fail_many(
        partition_key,
        jobs,
        error=outcome.error,
        payload=outcome.payload,
        ttl_ms=outcome.ttl_ms,
        values=outcome.values,
        value_refs=outcome.value_refs,
        drop_values=outcome.drop_values,
        override_values=outcome.override_values,
        attributes_merge=outcome.attributes_merge,
        state_meta=outcome.state_meta,
        independent=True,
    )
    validate_many_result(
        response,
        len(jobs),
        operation="FLOW.FAIL_MANY",
    )


__all__ = [
    "AsyncWorkflowExecutionHost",
    "apply_uniform",
    "handle_claimed_batch",
]
