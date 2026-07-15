from __future__ import annotations

import builtins
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from ferricstore.retry_policy import RetryPolicy
    from ferricstore.types import BudgetPolicy, FlowStateMode


@dataclass(frozen=True, slots=True)
class StateConfig:
    name: str
    mode: str | None = None
    lease_ms: int = 30_000
    claim_payload: bool = True
    claim_record: bool = True
    claim_values: builtins.list[str] | None = None
    value_max_bytes: int | None = None
    on_error: str = "retry"
    retry: RetryPolicy | None = None
    return_record: bool = False
    budget: BudgetPolicy | None = None


@dataclass(frozen=True, slots=True)
class WorkflowWorkerResult:
    claimed: int = 0
    applied: int = 0
    claim_calls: int = 0
    empty_claims: int = 0


@dataclass(frozen=True, slots=True)
class Transition:
    to_state: str
    payload: Any = None
    run_at_ms: int | None = None
    priority: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None
    attributes_merge: dict[str, Any] | None = None
    state_meta: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class Complete:
    result: Any = None
    payload: Any = None
    ttl_ms: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None
    attributes_merge: dict[str, Any] | None = None
    state_meta: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class Retry:
    error: Any = None
    payload: Any = None
    run_at_ms: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None
    attributes_merge: dict[str, Any] | None = None
    state_meta: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class Fail:
    error: Any = None
    payload: Any = None
    ttl_ms: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None
    attributes_merge: dict[str, Any] | None = None
    state_meta: dict[str, Any] | None = None


# These public value objects were originally defined in ferricstore.workflow.
# Keep that identity so pickles remain readable across the module split.
for _pickle_type in (StateConfig, WorkflowWorkerResult, Transition, Complete, Retry, Fail):
    _pickle_type.__module__ = "ferricstore.workflow"
del _pickle_type


Outcome = Transition | Complete | Retry | Fail
Handler = Callable[[Any], Outcome]


def transition(
    to_state: str,
    *,
    payload: Any = None,
    run_at_ms: int | None = None,
    priority: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
    attributes_merge: dict[str, Any] | None = None,
    state_meta: dict[str, Any] | None = None,
) -> Transition:
    return Transition(
        to_state=to_state,
        payload=payload,
        run_at_ms=run_at_ms,
        priority=priority,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
        attributes_merge=attributes_merge,
        state_meta=state_meta,
    )


def complete(
    *,
    result: Any = None,
    payload: Any = None,
    ttl_ms: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
    attributes_merge: dict[str, Any] | None = None,
    state_meta: dict[str, Any] | None = None,
) -> Complete:
    return Complete(
        result=result,
        payload=payload,
        ttl_ms=ttl_ms,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
        attributes_merge=attributes_merge,
        state_meta=state_meta,
    )


def retry(
    *,
    error: Any = None,
    payload: Any = None,
    run_at_ms: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
    attributes_merge: dict[str, Any] | None = None,
    state_meta: dict[str, Any] | None = None,
) -> Retry:
    return Retry(
        error=error,
        payload=payload,
        run_at_ms=run_at_ms,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
        attributes_merge=attributes_merge,
        state_meta=state_meta,
    )


def fail(
    *,
    error: Any = None,
    payload: Any = None,
    ttl_ms: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
    attributes_merge: dict[str, Any] | None = None,
    state_meta: dict[str, Any] | None = None,
) -> Fail:
    return Fail(
        error=error,
        payload=payload,
        ttl_ms=ttl_ms,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
        attributes_merge=attributes_merge,
        state_meta=state_meta,
    )


def state(
    name: str,
    *,
    mode: FlowStateMode | str | None = None,
    lease_ms: int = 30_000,
    claim_payload: bool = True,
    claim_record: bool = True,
    claim_values: builtins.list[str] | None = None,
    value_max_bytes: int | None = None,
    exception_policy: str | None = None,
    on_error: str | None = None,
    retry_policy: RetryPolicy | None = None,
    retry: RetryPolicy | None = None,
    return_record: bool = False,
    budget: BudgetPolicy | None = None,
) -> Callable[[Handler], Handler]:
    if exception_policy is not None and on_error is not None:
        raise ValueError("exception_policy and on_error are mutually exclusive")
    if retry_policy is not None and retry is not None:
        raise ValueError("retry_policy and retry are mutually exclusive")
    from ferricstore.types import normalize_flow_state_mode
    from ferricstore.worker_config import normalize_exception_policy

    resolved_on_error = normalize_exception_policy(
        exception_policy if exception_policy is not None else on_error,
        argument="exception_policy" if exception_policy is not None else "on_error",
    )
    resolved_retry_policy = retry_policy if retry_policy is not None else retry
    resolved_mode = normalize_flow_state_mode(mode)

    def decorate(fn: Handler) -> Handler:
        cast(Any, fn).__ferric_state__ = StateConfig(
            name=name,
            mode=resolved_mode,
            lease_ms=lease_ms,
            claim_payload=claim_payload,
            claim_record=claim_record,
            claim_values=claim_values,
            value_max_bytes=value_max_bytes,
            on_error=resolved_on_error,
            retry=resolved_retry_policy,
            return_record=return_record,
            budget=budget,
        )
        return fn

    return decorate


__all__ = [
    "Complete",
    "Fail",
    "Handler",
    "Outcome",
    "Retry",
    "StateConfig",
    "Transition",
    "WorkflowWorkerResult",
    "complete",
    "fail",
    "retry",
    "state",
    "transition",
]
