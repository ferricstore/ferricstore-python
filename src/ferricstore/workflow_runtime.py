from __future__ import annotations

import builtins
import contextlib
from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

from ferricstore.batch_core import BatchValueMatcher
from ferricstore.client_core import FlowClient
from ferricstore.config_validation import validate_string_sequence
from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import (
    SyncCloseCoordinator,
    close_resources_sync,
    raise_primary_with_cleanup,
)
from ferricstore.mutation_core import JobMutation
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    BudgetPolicy,
    ChildSpec,
    ClaimedFlow,
    CreateItem,
    ExceptionPolicy,
    FlowRecord,
    FlowStateMode,
    FlowStatePolicy,
    FlowStatePolicyLike,
    ValueConfig,
    WorkerConfig,
    normalize_exception_policy,
    resolve_worker_connection_counts,
)
from ferricstore.worker_core import (
    validate_many_result,
)
from ferricstore.workflow_execution import apply_uniform_batch, handle_known_state_batch
from ferricstore.workflow_models import (
    WORKFLOW_WORKER_CONFIG_KEYS,
    WorkflowContext,
    _close_resource_safely,
)
from ferricstore.workflow_mutations import (
    apply_sync_outcome,
    build_job_mutation,
)
from ferricstore.workflow_producer import _WorkflowProducerMixin
from ferricstore.workflow_types import (
    Complete,
    Fail,
    Handler,
    Outcome,
    Retry,
    StateConfig,
    Transition,
    complete,
    state,
)

if TYPE_CHECKING:
    from ferricstore.workflow_worker import WorkflowWorker


class Workflow(_WorkflowProducerMixin):
    """Base class for explicit FerricFlow state workflows."""

    type: str
    initial_state = "queued"
    partition_by: tuple[str, ...] = ()
    retry_policy: RetryPolicy | None = None
    worker_config: WorkerConfig | None = None
    value_config: ValueConfig = ValueConfig()

    def __init__(
        self,
        client: FlowClient,
        *,
        claim_client: FlowClient | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> None:
        self.client = client
        self.claim_client = claim_client if claim_client is not None else client
        self.retry_policy = retry_policy if retry_policy is not None else self.retry_policy
        self.worker_config = worker_config if worker_config is not None else self.worker_config
        self.value_config = value_config if value_config is not None else self.value_config
        self._states, self._handlers = self._discover_state_handlers()
        default_exception_policy = (
            self.worker_config.exception_policy if self.worker_config is not None else None
        )
        if self.value_config.value_max_bytes is not None or default_exception_policy is not None:
            self._states = {
                name: replace(
                    config,
                    value_max_bytes=(
                        config.value_max_bytes
                        if config.value_max_bytes is not None
                        else self.value_config.value_max_bytes
                    ),
                    on_error=(
                        normalize_exception_policy(default_exception_policy)
                        if default_exception_policy is not None
                        and config.on_error == ExceptionPolicy.RETRY.value
                        else config.on_error
                    ),
                )
                for name, config in self._states.items()
            }

    def run_steps_many(
        self,
        items: builtins.list[str | dict[str, Any] | CreateItem],
        *,
        states: Sequence[str] | None = None,
        steps: int | None = None,
        worker: str,
        payload: Any = None,
        result: Any = None,
        **attrs: Any,
    ) -> bytes:
        partition_key = self._resolve_partition_key(attrs)
        return self.client.run_steps_many(
            items,
            type=self.type,
            states=states,
            steps=steps,
            worker=worker,
            payload=payload,
            result=result,
            partition_key=partition_key,
            **attrs,
        )

    def install_policy(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        indexed_state_meta: str | None = None,
        max_active_ms: int | float | str | None = None,
    ) -> Any:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        state_policies: dict[str, FlowStatePolicyLike] = {}
        for config in self._states.values():
            if config.mode is not None or config.retry is not None:
                state_policies[config.name] = FlowStatePolicy(
                    mode=config.mode,
                    retry=config.retry,
                )
        kwargs: dict[str, Any] = {"retry": resolved_retry_policy, "states": state_policies}
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.install_policy(self.type, **kwargs)

    def policy_get(self, *, state: str | None = None) -> dict[Any, Any]:
        return self.client.policy_get(self.type, state=state)

    def signal(self, id: str, **kwargs: Any) -> Any:
        return self.client.signal(id, **kwargs)

    def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return self.signal(id, **kwargs)

    def worker(self, **kwargs: Any) -> WorkflowWorker:
        from ferricstore.workflow_worker import WorkflowWorker

        worker_kwargs = (
            self.worker_config.to_kwargs(WORKFLOW_WORKER_CONFIG_KEYS)
            if self.worker_config is not None
            else {}
        )
        worker_kwargs.update(kwargs)
        return WorkflowWorker(self, **worker_kwargs)

    def claim_due(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        limit: int = 1,
        priority: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        block_ms: int | None = None,
    ) -> builtins.list[FlowRecord | ClaimedFlow]:
        config = self._states[state_name]
        claim_client = getattr(self, "claim_client", self.client)
        if not config.claim_record and not config.claim_payload and not config.claim_values:
            jobs = claim_client.claim_flows(
                self.type,
                state=state_name,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=config.lease_ms,
                limit=limit,
                priority=priority,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                block_ms=block_ms,
            )
            return cast(
                builtins.list[FlowRecord | ClaimedFlow],
                self._stamp_compact_jobs(jobs, state_name),
            )

        return cast(
            builtins.list[FlowRecord | ClaimedFlow],
            claim_client.claim_due(
                self.type,
                state=state_name,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=config.lease_ms,
                limit=limit,
                priority=priority,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                block_ms=block_ms,
                payload=config.claim_payload,
                values=config.claim_values,
                value_max_bytes=config.value_max_bytes,
            ),
        )

    def reclaim(
        self,
        *,
        worker: str,
        partition_key: str | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
    ) -> builtins.list[FlowRecord]:
        return cast(
            builtins.list[FlowRecord],
            self.client.reclaim(
                self.type,
                worker=worker,
                partition_key=partition_key,
                lease_ms=lease_ms,
                limit=limit,
            ),
        )

    def run_once(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        limit: int = 1,
        priority: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
    ) -> builtins.list[FlowRecord | bytes]:
        jobs = self.claim_due(
            state_name,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=limit,
            priority=priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
        )
        return [self.handle(job) for job in jobs]

    def run_batch_once(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        limit: int = 1,
        priority: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
    ) -> builtins.list[FlowRecord | bytes]:
        jobs = self.claim_due(
            state_name,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=limit,
            priority=priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
        )
        return self.handle_claimed_batch(state_name, jobs)

    def handle_claimed_batch(
        self,
        state_name: str,
        jobs: Sequence[FlowRecord | ClaimedFlow],
    ) -> builtins.list[FlowRecord | bytes]:
        return cast(
            builtins.list[FlowRecord | bytes],
            self._handle_known_state_batch(state_name, jobs),
        )

    def handle_claimed_batch_count(
        self,
        state_name: str,
        jobs: Sequence[FlowRecord | ClaimedFlow],
    ) -> int:
        return cast(int, self._handle_known_state_batch(state_name, jobs, materialize=False))

    def get(self, id: str, *, partition_key: str | None = None) -> FlowRecord | None:
        return self.client.get(id, partition_key=partition_key)

    def history(self, id: str, **kwargs: Any) -> builtins.list[Any]:
        return self.client.history(id, **kwargs)

    def list(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.list(self.type, **kwargs)

    def terminals(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.terminals(self.type, **kwargs)

    def failures(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.failures(self.type, **kwargs)

    def by_parent(self, parent_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.by_parent(parent_flow_id, **kwargs)

    def by_root(self, root_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.by_root(root_flow_id, **kwargs)

    def by_correlation(self, correlation_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.by_correlation(correlation_id, **kwargs)

    def info(self, **kwargs: Any) -> dict[Any, Any]:
        return self.client.info(self.type, **kwargs)

    def stuck(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.stuck(self.type, **kwargs)

    def cancel(self, id: str, **kwargs: Any) -> FlowRecord:
        return cast(FlowRecord, self.client.cancel(id, **kwargs))

    def rewind(self, id: str, **kwargs: Any) -> FlowRecord:
        return cast(FlowRecord, self.client.rewind(id, **kwargs))

    def spawn_children(
        self,
        parent: FlowRecord,
        children: builtins.list[ChildSpec],
        *,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        kwargs.setdefault("partition_key", parent.partition_key)
        kwargs.setdefault("lease_token", parent.lease_token)
        kwargs.setdefault("fencing_token", parent.fencing_token)
        return self.client.spawn_children(parent.id, children, **kwargs)

    def child(
        self,
        id: str,
        *,
        payload: Any = None,
        partition_key: str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
    ) -> ChildSpec:
        return ChildSpec(
            id=id,
            type=self.type,
            payload=payload,
            partition_key=partition_key,
            values=values,
            value_refs=value_refs,
        )

    def context(
        self, job: FlowRecord | ClaimedFlow, state_name: str | None = None
    ) -> WorkflowContext:
        return WorkflowContext(
            self,
            job,
            self._logical_state(job) if state_name is None else state_name,
        )

    def handle(self, job: FlowRecord | ClaimedFlow) -> FlowRecord | bytes:
        state_name = self._logical_state(job)
        handler = self._handler_for(state_name)
        ctx = self.context(job, state_name)
        try:
            outcome = self._run_handler_with_context(handler, ctx, state_name, job)
        except Exception as exc:
            return self._handle_exception(job, exc, state_name=state_name)
        return self.apply(job, outcome, state_name=state_name)

    def handle_batch(
        self, jobs: Sequence[FlowRecord | ClaimedFlow]
    ) -> builtins.list[FlowRecord | bytes]:
        if not jobs:
            return []

        planned: builtins.list[tuple[FlowRecord | ClaimedFlow, str, Outcome]] = []
        for job in jobs:
            state_name = self._logical_state(job)
            handler = self._handler_for(state_name)
            ctx = self.context(job, state_name)
            try:
                outcome = self._run_handler_with_context(handler, ctx, state_name, job)
            except Exception as exc:
                outcome = self._exception_outcome(job, exc, state_name=state_name)
            planned.append((job, state_name, outcome))

        _first_job, first_state, first_outcome = planned[0]
        first_matcher = BatchValueMatcher(first_outcome)
        if all(
            state_name == first_state and first_matcher.matches(outcome)
            for _job, state_name, outcome in planned
        ):
            return cast(
                builtins.list[FlowRecord | bytes],
                self._apply_uniform_batch(
                    [job for job, _state_name, _outcome in planned],
                    first_state,
                    first_outcome,
                ),
            )

        return [
            self.apply(job, outcome, state_name=state_name) for job, state_name, outcome in planned
        ]

    def _handle_known_state_batch(
        self,
        state_name: str,
        jobs: Sequence[FlowRecord | ClaimedFlow],
        *,
        materialize: bool = True,
    ) -> builtins.list[FlowRecord | bytes] | int:
        return handle_known_state_batch(
            self,
            state_name,
            jobs,
            materialize=materialize,
        )

    def _job_mutation(
        self,
        job: FlowRecord | ClaimedFlow,
        outcome: Outcome,
    ) -> JobMutation:
        return build_job_mutation(
            job,
            outcome,
            validate_transition=self._validate_transition_policy,
        )

    def apply(
        self,
        job: FlowRecord | ClaimedFlow,
        outcome: Outcome,
        *,
        state_name: str | None = None,
    ) -> FlowRecord | bytes:
        logical_state = self._logical_state(job) if state_name is None else state_name
        return apply_sync_outcome(
            self.client,
            job,
            outcome,
            return_record=self._states[logical_state].return_record,
            validate_transition=self._validate_transition_policy,
        )

    def _validate_transition_policy(self, outcome: Transition) -> None:
        target = self._states.get(outcome.to_state)
        if (
            target is not None
            and target.mode == FlowStateMode.FIFO.value
            and outcome.priority is not None
        ):
            raise ValueError("priority is not supported for fifo state")

    def _apply_uniform_batch(
        self,
        jobs: Sequence[FlowRecord | ClaimedFlow],
        state_name: str,
        outcome: Outcome,
        *,
        materialize: bool = True,
    ) -> builtins.list[FlowRecord | bytes] | int:
        return apply_uniform_batch(
            self,
            jobs,
            state_name,
            outcome,
            materialize=materialize,
        )

    def _run_handler_with_context(
        self,
        handler: Handler,
        ctx: WorkflowContext,
        state_name: str,
        job: FlowRecord | ClaimedFlow,
    ) -> Outcome:
        budget = ctx._state_budget(self._states[state_name].budget)
        try:
            if budget is not None:
                budget.__enter__()
            outcome = handler(ctx)
            if budget is not None:
                budget.commit()
        except BaseException as exc:
            cleanup_error: BaseException | None = None
            if budget is not None and budget.is_open:
                try:
                    budget.release()
                except BaseException as cleanup:
                    cleanup_error = cleanup
            if cleanup_error is not None:
                try:
                    raise_primary_with_cleanup(exc, exc.__traceback__, cleanup_error)
                except BaseException as preserved:
                    exc = preserved
            if not isinstance(exc, Exception):
                raise exc
            outcome = self._exception_outcome(job, exc, state_name=state_name)
            return self._merge_governance_attributes(outcome, ctx._governance_attributes)
        return self._merge_governance_attributes(outcome, ctx._governance_attributes)

    @staticmethod
    def _merge_governance_attributes(outcome: Any, attributes: dict[str, Any]) -> Outcome:
        if not isinstance(outcome, (Transition, Complete, Retry, Fail)):
            outcome = complete(result=outcome)
        if not attributes:
            return cast(Outcome, outcome)
        merged = dict(outcome.attributes_merge or {})
        merged.update(attributes)
        return cast(Outcome, replace(outcome, attributes_merge=merged))

    def _exception_outcome(
        self,
        job: FlowRecord | ClaimedFlow,
        exc: Exception,
        *,
        state_name: str,
    ) -> Outcome:
        config = self._states[state_name]
        if config.on_error == "raise":
            raise exc
        if config.on_error == "fail":
            return Fail(error=str(exc))
        return Retry(error=str(exc))

    def _handle_exception(
        self,
        job: FlowRecord | ClaimedFlow,
        exc: Exception,
        *,
        state_name: str | None = None,
    ) -> FlowRecord | bytes:
        logical_state = self._logical_state(job) if state_name is None else state_name
        return self.apply(
            job,
            self._exception_outcome(job, exc, state_name=logical_state),
            state_name=logical_state,
        )

    @staticmethod
    def _logical_state(job: FlowRecord | ClaimedFlow) -> str:
        return job.run_state or job.state

    @staticmethod
    def _uniform_partition_key(jobs: Sequence[FlowRecord | ClaimedFlow]) -> str | None:
        first = jobs[0].partition_key
        if first is not None and all(job.partition_key == first for job in jobs):
            return first
        return None

    @staticmethod
    def _uniform_current_state(jobs: Sequence[FlowRecord | ClaimedFlow]) -> str | None:
        first = jobs[0].state
        if all(job.state == first for job in jobs):
            return first
        return None

    @staticmethod
    def _batch_response_list(
        response: Any,
        count: int,
        *,
        operation: str = "Flow many command",
    ) -> builtins.list[FlowRecord | bytes]:
        return cast(
            builtins.list[FlowRecord | bytes],
            validate_many_result(response, count, operation=operation),
        )

    def _stamp_compact_jobs(
        self,
        jobs: builtins.list[ClaimedFlow],
        state_name: str,
    ) -> builtins.list[ClaimedFlow]:
        for job in jobs:
            object.__setattr__(job, "type", self.type)
            object.__setattr__(job, "state", "running")
            object.__setattr__(job, "run_state", state_name)
        return jobs

    def _discover_state_handlers(
        self,
    ) -> tuple[dict[str, StateConfig], dict[str, Handler]]:
        states: dict[str, StateConfig] = {}
        handlers: dict[str, Handler] = {}
        visible_members = dict(vars(self))
        for owner in type(self).__mro__:
            for name, descriptor in vars(owner).items():
                visible_members.setdefault(name, descriptor)

        for name, descriptor in visible_members.items():
            target = getattr(descriptor, "__func__", descriptor)
            config = getattr(target, "__ferric_state__", None)
            if isinstance(config, StateConfig):
                if config.name in states:
                    raise ValueError(f"duplicate workflow state: {config.name!r}")
                member = getattr(self, name)
                states[config.name] = config
                handlers[config.name] = member
        return states, handlers

    def _handler_for(self, state_name: str) -> Handler:
        handler = self._handlers.get(state_name)
        if handler is not None:
            return handler
        raise FerricStoreError(f"no handler for state {state_name!r}")


class FlowWorkflow(Workflow):
    """Constructor/decorator workflow API.

    This is the primary sync workflow API. It uses the same runtime as
    class-based ``Workflow`` subclasses, but keeps configuration in the
    constructor and registers handlers with ``@workflow.state(...)``.
    """

    def __init__(
        self,
        client: FlowClient | str,
        *,
        claim_client: FlowClient | str | None = None,
        type: str,
        initial_state: str = "queued",
        partition_by: Sequence[str] = (),
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> None:
        resolved_partition_by = validate_string_sequence(partition_by, name="partition_by")
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_max_connections = (
            1
            if worker_config is None or worker_config.command_connections is None
            else command_pool_size
        )
        with contextlib.ExitStack() as rollback:
            self._owns_client = isinstance(client, str)
            self.client = (
                FlowClient.from_url(client, max_connections=command_max_connections)
                if isinstance(client, str)
                else client
            )
            if self._owns_client:
                rollback.callback(_close_resource_safely, self.client)
            if claim_client is None:
                self.claim_client = (
                    FlowClient.from_url(client, max_connections=claim_pool_size)
                    if isinstance(client, str)
                    else self.client
                )
                self._owns_claim_client = isinstance(client, str)
            else:
                self.claim_client = (
                    FlowClient.from_url(claim_client, max_connections=claim_pool_size)
                    if isinstance(claim_client, str)
                    else claim_client
                )
                self._owns_claim_client = isinstance(claim_client, str)
            if self._owns_claim_client and self.claim_client is not self.client:
                rollback.callback(_close_resource_safely, self.claim_client)
            rollback.pop_all()
        self.type = type
        self.initial_state = initial_state
        self.partition_by = resolved_partition_by
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._states: dict[str, StateConfig] = {}
        self._handlers: dict[str, Handler] = {}
        self._close_coordinator = SyncCloseCoordinator()

    def close(self) -> None:
        self._close_coordinator.run(self._close_owned_clients)

    def _close_owned_clients(self) -> None:
        resources: list[Callable[[], Any]] = []
        if self._owns_claim_client and self.claim_client is not self.client:

            def close_claim_client() -> None:
                self.claim_client.close()
                self._owns_claim_client = False

            resources.append(close_claim_client)
        if self._owns_client:

            def close_client() -> None:
                self.client.close()
                self._owns_client = False

            resources.append(close_client)
        close_resources_sync(resources)

    def state(
        self,
        name: str,
        *,
        mode: FlowStateMode | str | None = None,
        lease_ms: int = 30_000,
        claim_payload: bool = True,
        claim_record: bool = True,
        claim_values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        exception_policy: ExceptionPolicy | str | None = None,
        on_error: ExceptionPolicy | str | None = None,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        return_record: bool = False,
        budget: BudgetPolicy | None = None,
    ) -> Callable[[Handler], Handler]:
        def decorate(fn: Handler) -> Handler:
            if name in self._states:
                raise ValueError(f"duplicate workflow state: {name!r}")
            handler = state(
                name,
                mode=mode,
                lease_ms=lease_ms,
                claim_payload=claim_payload,
                claim_record=claim_record,
                claim_values=claim_values,
                value_max_bytes=(
                    value_max_bytes
                    if value_max_bytes is not None
                    else self.value_config.value_max_bytes
                ),
                exception_policy=(
                    exception_policy
                    if exception_policy is not None
                    or on_error is not None
                    or self.worker_config is None
                    else self.worker_config.exception_policy
                ),
                on_error=on_error,
                retry_policy=retry_policy,
                retry=retry,
                return_record=return_record,
                budget=budget,
            )(fn)
            config = cast(Any, handler).__ferric_state__
            self._states[config.name] = config
            self._handlers[config.name] = handler
            return handler

        return decorate

    def on(
        self,
        name: str,
        **kwargs: Any,
    ) -> Callable[[Handler], Handler]:
        return self.state(name, **kwargs)

    def start(
        self,
        id: str,
        *,
        payload: Any = None,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> FlowRecord | bytes:
        return self.enqueue(
            id,
            payload=payload,
            max_active_ms=max_active_ms,
            **attrs,
        )


_WORKFLOW_CLIENT_EXPORTS = frozenset({"WorkflowClient"})


def __getattr__(name: str) -> Any:
    if name not in _WORKFLOW_CLIENT_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from ferricstore import workflow_client

    return getattr(workflow_client, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _WORKFLOW_CLIENT_EXPORTS)
