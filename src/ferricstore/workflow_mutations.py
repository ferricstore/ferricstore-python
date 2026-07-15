from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from ferricstore.errors import FerricStoreError
from ferricstore.mutation_core import JobMutation, MutationKind
from ferricstore.types import ClaimedFlow, FlowRecord
from ferricstore.workflow_types import Complete, Fail, Outcome, Retry, Transition, complete

WorkflowJob = FlowRecord | ClaimedFlow
TransitionValidator = Callable[[Transition], None]


def complete_mutation_options(outcome: Complete) -> dict[str, Any]:
    """Translate a completed workflow outcome into batch mutation options."""
    return {
        "result": outcome.result,
        "payload": outcome.payload,
        "ttl_ms": outcome.ttl_ms,
        "values": outcome.values,
        "value_refs": outcome.value_refs,
        "drop_values": outcome.drop_values,
        "override_values": outcome.override_values,
        "attributes_merge": outcome.attributes_merge,
        "state_meta": outcome.state_meta,
    }


def build_job_mutation(
    job: WorkflowJob,
    outcome: Outcome,
    *,
    validate_transition: TransitionValidator,
) -> JobMutation:
    """Build one transport-independent fenced mutation from a workflow outcome."""
    if isinstance(outcome, Complete):
        return JobMutation(MutationKind.COMPLETE, job, complete_mutation_options(outcome))

    common = {
        "payload": outcome.payload,
        "values": outcome.values,
        "value_refs": outcome.value_refs,
        "drop_values": outcome.drop_values,
        "override_values": outcome.override_values,
        "attributes_merge": outcome.attributes_merge,
        "state_meta": outcome.state_meta,
    }
    if isinstance(outcome, Transition):
        validate_transition(outcome)
        return JobMutation(
            MutationKind.TRANSITION,
            job,
            {
                **common,
                "from_state": job.state,
                "to_state": outcome.to_state,
                "run_at_ms": outcome.run_at_ms,
                "priority": outcome.priority,
            },
        )
    if isinstance(outcome, Retry):
        return JobMutation(
            MutationKind.RETRY,
            job,
            {**common, "error": outcome.error, "run_at_ms": outcome.run_at_ms},
        )
    return JobMutation(
        MutationKind.FAIL,
        job,
        {**common, "error": outcome.error, "ttl_ms": outcome.ttl_ms},
    )


def apply_sync_outcome(
    client: Any,
    job: WorkflowJob,
    outcome: Any,
    *,
    return_record: bool,
    validate_transition: TransitionValidator,
) -> FlowRecord | bytes:
    """Apply one sync workflow outcome through the canonical client mutation API."""
    if not isinstance(outcome, (Transition, Complete, Retry, Fail)):
        outcome = complete(result=outcome)

    common: dict[str, Any] = {
        "lease_token": job.lease_token,
        "fencing_token": job.fencing_token,
        "partition_key": job.partition_key,
        "return_record": return_record,
    }
    if isinstance(outcome, Transition):
        validate_transition(outcome)
        return cast(
            FlowRecord | bytes,
            client.transition(
                job.id,
                from_state=job.state,
                to_state=outcome.to_state,
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                priority=outcome.priority,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                attributes_merge=outcome.attributes_merge,
                state_meta=outcome.state_meta,
                **common,
            ),
        )
    if isinstance(outcome, Complete):
        return cast(
            FlowRecord | bytes,
            client.complete(job.id, **complete_mutation_options(outcome), **common),
        )
    if isinstance(outcome, Retry):
        return cast(
            FlowRecord | bytes,
            client.retry(
                job.id,
                error=outcome.error,
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                attributes_merge=outcome.attributes_merge,
                state_meta=outcome.state_meta,
                **common,
            ),
        )
    if isinstance(outcome, Fail):
        return cast(
            FlowRecord | bytes,
            client.fail(
                job.id,
                error=outcome.error,
                payload=outcome.payload,
                ttl_ms=outcome.ttl_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                attributes_merge=outcome.attributes_merge,
                state_meta=outcome.state_meta,
                **common,
            ),
        )
    raise FerricStoreError(f"unknown workflow outcome: {outcome!r}")


__all__ = ["apply_sync_outcome", "build_job_mutation", "complete_mutation_options"]
