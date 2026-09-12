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
from ferricstore.workflow_applied import AppliedWorkflowStep
from ferricstore.workflow_mutations import complete_mutation_options
from ferricstore.workflow_types import (
    FLOW_MANY_BATCH_LIMIT,
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
    ) -> Any:
        pass

    def _handler_for(self, state_name: str) -> Handler:
        pass

    def _logical_state(self, job: FlowRecord | ClaimedFlow) -> str:
        pass

    def _run_handler_with_context(
        self,
        handler: Handler,
        ctx: Any,
        state_name: str,
        job: FlowRecord | ClaimedFlow,
    ) -> Outcome | AppliedWorkflowStep:
        pass

    def _exception_outcome(
        self,
        job: FlowRecord | ClaimedFlow,
        exc: Exception,
        *,
        state_name: str,
    ) -> Outcome:
        pass

    def _apply_uniform_batch(
        self,
        jobs: Sequence[FlowRecord | ClaimedFlow],
        state_name: str,
        outcome: Outcome,
        *,
        materialize: bool = True,
    ) -> builtins.list[FlowRecord | bytes] | int:
        pass

    def _job_mutation(
        self,
        job: FlowRecord | ClaimedFlow,
        outcome: Outcome,
    ) -> JobMutation:
        pass

    def apply(
        self,
        job: FlowRecord | ClaimedFlow,
        outcome: Outcome,
        *,
        state_name: str | None = None,
    ) -> FlowRecord | bytes:
        pass

    def _uniform_partition_key(
        self,
        jobs: Sequence[FlowRecord | ClaimedFlow],
    ) -> str | bytes | None:
        pass

    def _uniform_current_state(
        self,
        jobs: Sequence[FlowRecord | ClaimedFlow],
    ) -> str | None:
        pass

    def _validate_transition_policy(self, outcome: Transition) -> None:
        pass

    def _batch_response_list(
        self,
        response: Any,
        expected: int,
        *,
        operation: str,
    ) -> builtins.list[FlowRecord | bytes]:
        pass


def _required_first_outcome(outcome: Outcome | None) -> Outcome:
    if outcome is None:
        raise FerricStoreError("workflow batch planner lost its first handler outcome")
    return outcome


def _apply_continuation(
    self: WorkflowExecutionHost,
    marker: AppliedWorkflowStep,
    *,
    state_name: str,
) -> FlowRecord | bytes:
    if marker.error is not None:
        return b"OK"
    if not marker.has_continuation:
        raise FerricStoreError("applied workflow step lost its handler continuation")
    return self.apply(
        marker.job,
        cast(Outcome, marker.continuation),
        state_name=state_name,
    )


def handle_mixed_state_batch(
    self: WorkflowExecutionHost,
    jobs: Sequence[FlowRecord | ClaimedFlow],
) -> builtins.list[FlowRecord | bytes]:
    if not jobs:
        return []

    planned: builtins.list[tuple[FlowRecord | ClaimedFlow, str, Outcome | AppliedWorkflowStep]] = []
    for job in jobs:
        state_name = self._logical_state(job)
        handler = self._handler_for(state_name)
        ctx = self.context(job, state_name)
        try:
            outcome = self._run_handler_with_context(handler, ctx, state_name, job)
        except Exception as exc:
            outcome = self._exception_outcome(job, exc, state_name=state_name)
        planned.append((job, state_name, outcome))

    if any(isinstance(outcome, AppliedWorkflowStep) for _job, _state, outcome in planned):
        results = [
            _apply_continuation(self, outcome, state_name=state_name)
            if isinstance(outcome, AppliedWorkflowStep)
            else self.apply(job, outcome, state_name=state_name)
            for job, state_name, outcome in planned
        ]
        for _job, _state, outcome in planned:
            if isinstance(outcome, AppliedWorkflowStep) and outcome.error is not None:
                raise outcome.error
        return results

    normal_planned = cast(
        builtins.list[tuple[FlowRecord | ClaimedFlow, str, Outcome]],
        planned,
    )
    _first_job, first_state, first_outcome = normal_planned[0]
    first_matcher = BatchValueMatcher(first_outcome)
    if all(
        state_name == first_state and first_matcher.matches(outcome)
        for _job, state_name, outcome in normal_planned
    ):
        return cast(
            builtins.list[FlowRecord | bytes],
            self._apply_uniform_batch(
                [job for job, _state_name, _outcome in normal_planned],
                first_state,
                first_outcome,
            ),
        )

    return [
        self.apply(job, outcome, state_name=state_name)
        for job, state_name, outcome in normal_planned
    ]


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
    outcomes_with_applied: builtins.list[Outcome | AppliedWorkflowStep] | None = None
    first_matcher: BatchValueMatcher | None = None
    first_outcome: Outcome | None = None

    for idx, job in enumerate(jobs):
        ctx = self.context(job, state_name)
        try:
            outcome = self._run_handler_with_context(handler, ctx, state_name, job)
        except Exception as exc:
            outcome = self._exception_outcome(job, exc, state_name=state_name)

        if isinstance(outcome, AppliedWorkflowStep) or outcomes_with_applied is not None:
            if outcomes_with_applied is None:
                if idx == 0:
                    outcomes_with_applied = []
                elif mixed_outcomes is not None:
                    outcomes_with_applied = list(mixed_outcomes)
                else:
                    first = _required_first_outcome(first_outcome)
                    outcomes_with_applied = [first for _ in range(idx)]
            outcomes_with_applied.append(outcome)
            continue

        if idx == 0:
            first_outcome = outcome
            first_matcher = BatchValueMatcher(outcome)
            continue

        if mixed_outcomes is None:
            if first_matcher is not None and first_matcher.matches(outcome):
                continue
            first = _required_first_outcome(first_outcome)
            mixed_outcomes = [first for _ in range(idx)]

        mixed_outcomes.append(outcome)

    if outcomes_with_applied is not None:
        if materialize:
            materialized_results = [
                _apply_continuation(self, outcome, state_name=state_name)
                if isinstance(outcome, AppliedWorkflowStep)
                else self.apply(job, outcome, state_name=state_name)
                for job, outcome in zip(jobs, outcomes_with_applied, strict=True)
            ]
        else:
            for job, outcome in zip(jobs, outcomes_with_applied, strict=True):
                if isinstance(outcome, AppliedWorkflowStep):
                    _apply_continuation(self, outcome, state_name=state_name)
                else:
                    self.apply(job, outcome, state_name=state_name)
        for outcome in outcomes_with_applied:
            if isinstance(outcome, AppliedWorkflowStep) and outcome.error is not None:
                raise outcome.error
        if materialize:
            return materialized_results
        return len(jobs)

    if mixed_outcomes is None:
        return self._apply_uniform_batch(
            jobs,
            state_name,
            _required_first_outcome(first_outcome),
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
