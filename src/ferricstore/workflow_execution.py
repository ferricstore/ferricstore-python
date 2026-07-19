from __future__ import annotations

import builtins
from collections.abc import Sequence
from typing import Any, Protocol, cast

from ferricstore.batch_core import BatchValueMatcher
from ferricstore.client_core import FlowClient
from ferricstore.errors import FerricStoreError
from ferricstore.mutation_core import JobMutation, MutationBatchPlan
from ferricstore.types import ClaimedFlow, FencedItem, FlowRecord
from ferricstore.worker_core import validate_many_result
from ferricstore.workflow_models import (
    FLOW_MANY_BATCH_LIMIT,
    WorkflowContext,
)
from ferricstore.workflow_mutations import complete_mutation_options
from ferricstore.workflow_types import (
    Complete,
    Fail,
    Handler,
    Outcome,
    Retry,
    StateConfig,
    Transition,
    complete,
)


class WorkflowExecutionHost(Protocol):
    """Workflow state required by the extracted batch execution algorithms."""

    client: FlowClient
    _states: dict[str, StateConfig]

    def context(
        self,
        job: FlowRecord | ClaimedFlow,
        state_name: str,
    ) -> WorkflowContext: ...

    def _handler_for(self, state_name: str) -> Handler: ...

    def _run_handler_with_context(
        self,
        handler: Handler,
        ctx: WorkflowContext,
        state_name: str,
        job: FlowRecord | ClaimedFlow,
    ) -> Outcome: ...

    def _exception_outcome(
        self,
        job: FlowRecord | ClaimedFlow,
        exc: Exception,
        *,
        state_name: str,
    ) -> Outcome: ...

    def _apply_uniform_batch(
        self,
        jobs: Sequence[FlowRecord | ClaimedFlow],
        state_name: str,
        outcome: Outcome,
        *,
        materialize: bool = True,
    ) -> builtins.list[FlowRecord | bytes] | int: ...

    def _job_mutation(
        self,
        job: FlowRecord | ClaimedFlow,
        outcome: Outcome,
    ) -> JobMutation: ...

    def apply(
        self,
        job: FlowRecord | ClaimedFlow,
        outcome: Outcome,
        *,
        state_name: str | None = None,
    ) -> FlowRecord | bytes: ...

    def _uniform_partition_key(
        self,
        jobs: Sequence[FlowRecord | ClaimedFlow],
    ) -> str | bytes | None: ...

    def _uniform_current_state(
        self,
        jobs: Sequence[FlowRecord | ClaimedFlow],
    ) -> str | None: ...

    def _validate_transition_policy(self, outcome: Transition) -> None: ...

    def _batch_response_list(
        self,
        response: Any,
        expected: int,
        *,
        operation: str,
    ) -> builtins.list[FlowRecord | bytes]: ...


def handle_known_state_batch(
    self: WorkflowExecutionHost,
    state_name: str,
    jobs: Sequence[FlowRecord | ClaimedFlow],
    *,
    materialize: bool = True,
) -> builtins.list[FlowRecord | bytes] | int:
    if not jobs:
        return [] if materialize else 0

    handler = self._handler_for(state_name)
    mixed_outcomes: builtins.list[Outcome] | None = None
    first_matcher: BatchValueMatcher | None = None

    for idx, job in enumerate(jobs):
        ctx = self.context(job, state_name)
        try:
            outcome = self._run_handler_with_context(handler, ctx, state_name, job)
        except Exception as exc:
            outcome = self._exception_outcome(job, exc, state_name=state_name)

        if idx == 0:
            first_outcome = outcome
            first_matcher = BatchValueMatcher(outcome)
            continue

        if mixed_outcomes is None:
            if first_matcher is not None and first_matcher.matches(outcome):
                continue
            mixed_outcomes = [first_outcome for _ in range(idx)]

        mixed_outcomes.append(outcome)

    if mixed_outcomes is None:
        return self._apply_uniform_batch(
            jobs,
            state_name,
            first_outcome,
            materialize=materialize,
        )

    normalized_outcomes = [
        outcome
        if isinstance(outcome, (Transition, Complete, Retry, Fail))
        else complete(result=outcome)
        for outcome in mixed_outcomes
    ]
    apply_job_mutations = getattr(self.client, "apply_job_mutations", None)
    if not self._states[state_name].return_record and callable(apply_job_mutations):
        plan = MutationBatchPlan.build(
            self._job_mutation(job, outcome)
            for job, outcome in zip(jobs, normalized_outcomes, strict=True)
        )
        response = apply_job_mutations(plan.mutations)
        values = validate_many_result(
            response,
            len(plan),
            operation="Flow workflow mutation batch",
        )
        if materialize:
            return cast(builtins.list[FlowRecord | bytes], values)
        return len(plan)

    complete_job_mutations = getattr(self.client, "complete_job_mutations", None)
    if (
        not self._states[state_name].return_record
        and callable(complete_job_mutations)
        and all(isinstance(outcome, Complete) for outcome in normalized_outcomes)
    ):
        response = complete_job_mutations(
            [
                (
                    cast(ClaimedFlow, job),
                    complete_mutation_options(cast(Complete, outcome)),
                )
                for job, outcome in zip(jobs, normalized_outcomes, strict=True)
            ]
        )
        values = validate_many_result(
            response,
            len(jobs),
            operation="FLOW.COMPLETE batch",
        )
        if materialize:
            return cast(builtins.list[FlowRecord | bytes], values)
        return len(jobs)

    if materialize:
        return [
            self.apply(job, outcome, state_name=state_name)
            for job, outcome in zip(jobs, normalized_outcomes, strict=True)
        ]

    for job, outcome in zip(jobs, normalized_outcomes, strict=True):
        self.apply(job, outcome, state_name=state_name)
    return len(jobs)


def apply_uniform_batch(
    self: WorkflowExecutionHost,
    jobs: Sequence[FlowRecord | ClaimedFlow],
    state_name: str,
    outcome: Outcome,
    *,
    materialize: bool = True,
) -> builtins.list[FlowRecord | bytes] | int:
    if len(jobs) > FLOW_MANY_BATCH_LIMIT:
        if not materialize:
            total = 0
            for offset in range(0, len(jobs), FLOW_MANY_BATCH_LIMIT):
                total += cast(
                    int,
                    self._apply_uniform_batch(
                        jobs[offset : offset + FLOW_MANY_BATCH_LIMIT],
                        state_name,
                        outcome,
                        materialize=False,
                    ),
                )
            return total

        results: builtins.list[FlowRecord | bytes] = []
        for offset in range(0, len(jobs), FLOW_MANY_BATCH_LIMIT):
            chunk_response = cast(
                builtins.list[FlowRecord | bytes],
                self._apply_uniform_batch(
                    jobs[offset : offset + FLOW_MANY_BATCH_LIMIT],
                    state_name,
                    outcome,
                    materialize=True,
                ),
            )
            results.extend(chunk_response)
        return results

    partition_key = self._uniform_partition_key(jobs)

    if not isinstance(outcome, (Transition, Complete, Retry, Fail)):
        outcome = complete(result=outcome)

    if isinstance(outcome, Transition):
        self._validate_transition_policy(outcome)
        from_state = self._uniform_current_state(jobs)
        if from_state is None:
            if materialize:
                return [self.apply(job, outcome, state_name=state_name) for job in jobs]
            for job in jobs:
                self.apply(job, outcome, state_name=state_name)
            return len(jobs)
        response = self.client.transition_many(
            partition_key,
            from_state=from_state,
            to_state=outcome.to_state,
            items=cast(builtins.list[FencedItem], jobs),
            payload=outcome.payload,
            run_at_ms=outcome.run_at_ms,
            priority=outcome.priority,
            values=outcome.values,
            value_refs=outcome.value_refs,
            drop_values=outcome.drop_values,
            override_values=outcome.override_values,
            attributes_merge=outcome.attributes_merge,
            state_meta=outcome.state_meta,
            independent=True,
        )
        values = self._batch_response_list(
            response,
            len(jobs),
            operation="FLOW.TRANSITION_MANY",
        )
        if not materialize:
            return len(jobs)
        return values

    if isinstance(outcome, Complete):
        response = self.client.complete_many(
            partition_key,
            cast(builtins.list[ClaimedFlow], jobs),
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
        values = self._batch_response_list(
            response,
            len(jobs),
            operation="FLOW.COMPLETE_MANY",
        )
        if not materialize:
            return len(jobs)
        return values

    if isinstance(outcome, Retry):
        response = self.client.retry_many(
            partition_key,
            cast(builtins.list[ClaimedFlow], jobs),
            error=outcome.error,
            payload=outcome.payload,
            run_at_ms=outcome.run_at_ms,
            values=outcome.values,
            value_refs=outcome.value_refs,
            drop_values=outcome.drop_values,
            override_values=outcome.override_values,
            attributes_merge=outcome.attributes_merge,
            state_meta=outcome.state_meta,
            independent=True,
        )
        values = self._batch_response_list(
            response,
            len(jobs),
            operation="FLOW.RETRY_MANY",
        )
        if not materialize:
            return len(jobs)
        return values

    if isinstance(outcome, Fail):
        response = self.client.fail_many(
            partition_key,
            cast(builtins.list[ClaimedFlow], jobs),
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
        values = self._batch_response_list(
            response,
            len(jobs),
            operation="FLOW.FAIL_MANY",
        )
        if not materialize:
            return len(jobs)
        return values

    raise FerricStoreError(f"unknown workflow outcome: {outcome!r}")


__all__ = [
    "WorkflowExecutionHost",
    "apply_uniform_batch",
    "handle_known_state_batch",
]
