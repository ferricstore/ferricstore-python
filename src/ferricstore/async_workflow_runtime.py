from __future__ import annotations

import asyncio
import builtins
import contextlib
import warnings
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import replace
from types import TracebackType
from typing import Any, cast

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_ownership import (
    AsyncOwnedClose,
    close_owned_resources_async,
    resolve_async_client_pair,
)
from ferricstore.async_partitioning import (
    _validate_server_shards,
)
from ferricstore.async_producer import AsyncProducerLoop
from ferricstore.async_queue_runtime import (
    FLOW_MANY_BATCH_LIMIT,
    AsyncErrorMode,
    AsyncFlowJob,
    AsyncWorkflowHandler,
    _client_from,
    _close_async_resource,
)
from ferricstore.async_workflow_execution import apply_uniform, handle_claimed_batch
from ferricstore.async_workflow_producer import _AsyncWorkflowProducerMixin
from ferricstore.async_workflow_types import AsyncWorkflowWorkerResult
from ferricstore.client_core import FlowClient
from ferricstore.config_validation import (
    validate_bool,
    validate_nonnegative_int,
    validate_optional_flow_priority,
    validate_optional_nonnegative_int,
    validate_positive_int,
    validate_string_sequence,
)
from ferricstore.lifecycle_core import (
    AsyncCloseTaskRegistry,
    await_cancellation_safe,
    raise_primary_with_cleanup,
)
from ferricstore.mutation_core import JobMutation
from ferricstore.policy_types import PolicySnapshot
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    BudgetPolicy,
    ClaimedFlow,
    CreateItem,
    FlowStateMode,
    FlowStatePolicy,
    FlowStatePolicyLike,
    ValueConfig,
    normalize_exception_policy,
    normalize_flow_state_mode,
    resolve_worker_connection_counts,
)
from ferricstore.worker_core import (
    AsyncWorkerInvocationTracker,
    CloseDeadline,
    task_terminal_error,
    validate_worker_idle_timing,
)
from ferricstore.workflow_core import pop_workflow_partition_key
from ferricstore.workflow_mutations import build_job_mutation
from ferricstore.workflow_types import (
    Complete,
    Fail,
    Retry,
    Transition,
    complete,
)


class AsyncWorkflow(_AsyncWorkflowProducerMixin):
    """Simple async state-machine workflow runtime.

    Handlers receive compact claimed jobs by default and return one of:
    Transition, Complete, Retry, Fail, or any plain value to complete with that value.
    """

    def __init__(
        self,
        client: AsyncFlowClient | FlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | FlowClient | str | Any | None = None,
        type: str,
        states: Sequence[str] | None = None,
        initial_state: str | None = None,
        partition_by: Sequence[str] = (),
        workers: int = 1,
        concurrency: int = 1,
        command_connections: int | None = None,
        claim_connections: int | None = None,
        batch_size: int = 10,
        claim_partition_batch_size: int = 1,
        server_shards: int = 16,
        idle_sleep_s: float = 0.1,
        block_ms: int | None = None,
        producer_loop_thread: bool = False,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
        retry_policy: RetryPolicy | None = None,
        value_config: ValueConfig | None = None,
        priority: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        _producer_url: str | None = None,
        _producer_url_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        workers = validate_positive_int(workers, name="workers")
        server_shards = _validate_server_shards(server_shards)
        concurrency = validate_positive_int(concurrency, name="concurrency")
        batch_size = validate_positive_int(batch_size, name="batch_size")
        claim_partition_batch_size = validate_positive_int(
            claim_partition_batch_size,
            name="claim_partition_batch_size",
        )
        if block_ms is not None:
            block_ms = validate_nonnegative_int(block_ms, name="block_ms")
        producer_loop_thread = validate_bool(
            producer_loop_thread,
            name="producer_loop_thread",
        )
        priority = validate_optional_flow_priority(priority)
        value_max_bytes = validate_optional_nonnegative_int(
            value_max_bytes,
            name="value_max_bytes",
        )
        idle_sleep_s, _max_idle_sleep_s = validate_worker_idle_timing(idle_sleep_s, None)
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_on_error = normalize_exception_policy(
            exception_policy if exception_policy is not None else on_error,
            argument="exception_policy" if exception_policy is not None else "on_error",
        )
        state_names = (
            list(validate_string_sequence(states, name="states", allow_empty=False))
            if states is not None
            else [initial_state or "queued"]
        )
        initial_state = initial_state if initial_state is not None else state_names[0]
        if initial_state not in state_names:
            raise ValueError("initial_state must be included in states")
        resolved_partition_by = validate_string_sequence(partition_by, name="partition_by")
        resolved_value_config = value_config if value_config is not None else ValueConfig()
        resolved_claim_values = (
            list(validate_string_sequence(claim_values, name="claim_values"))
            if claim_values is not None
            else None
        )
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=workers,
            concurrency=concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
        self._url = _producer_url
        if self._url is None and isinstance(client, str):
            self._url = client
        self._producer_url_kwargs = dict(_producer_url_kwargs or {})
        command_max_connections = (
            1 if isinstance(client, str) and command_connections is None else command_pool_size
        )
        clients = resolve_async_client_pair(
            client,
            claim_client,
            from_url=AsyncFlowClient.from_url,
            normalize=_client_from,
            command_kwargs={"max_connections": command_max_connections},
            claim_kwargs={"max_connections": claim_pool_size},
        )
        self.client = clients.command
        self.claim_client = clients.claim
        self._owns_client = clients.owns_command
        self._owns_claim_client = clients.owns_claim
        self.type = type
        self.states = state_names
        self.initial_state = initial_state
        self.partition_by = resolved_partition_by
        self.workers = workers
        self.concurrency = concurrency
        self.batch_size = min(batch_size, FLOW_MANY_BATCH_LIMIT)
        self.claim_partition_batch_size = claim_partition_batch_size
        self.server_shards = server_shards
        self.idle_sleep_s = idle_sleep_s
        self.block_ms = block_ms
        self.producer_loop_thread = producer_loop_thread
        self._producer_loop = (
            AsyncProducerLoop(
                self._url,
                client_kwargs=self._producer_url_kwargs,
                client_factory=AsyncFlowClient.from_url,
            )
            if producer_loop_thread and self._url is not None
            else None
        )
        self.on_error = resolved_on_error
        self.retry_policy = retry_policy
        self.value_config = resolved_value_config
        self.priority = priority
        self.claim_values = resolved_claim_values
        self.value_max_bytes = (
            value_max_bytes if value_max_bytes is not None else self.value_config.value_max_bytes
        )
        self.handlers: dict[str, AsyncWorkflowHandler] = {}
        self.state_modes: dict[str, str] = {}
        self.error_modes: dict[str, AsyncErrorMode] = {}
        self.retry_policies: dict[str, RetryPolicy] = {}
        self.budget_policies: dict[str, BudgetPolicy] = {}
        self._partition_cursors = [0 for _ in range(workers)]
        self._state_cursors = [0 for _ in range(workers)]
        self._running = False
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._task_phases: dict[asyncio.Task[Any], str] = {}
        self._totals = AsyncWorkflowWorkerResult()
        self._close_started = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._invocations = AsyncWorkerInvocationTracker()
        self._close_operations = AsyncCloseTaskRegistry()

    def state(
        self,
        state_name: str,
        *,
        mode: FlowStateMode | str | None = None,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
        retry_policy: RetryPolicy | None = None,
        budget: BudgetPolicy | None = None,
    ) -> Callable[[AsyncWorkflowHandler], AsyncWorkflowHandler]:
        return self.on(
            state_name,
            mode=mode,
            exception_policy=exception_policy,
            on_error=on_error,
            retry_policy=retry_policy,
            budget=budget,
        )

    def on(
        self,
        state_name: str,
        *,
        mode: FlowStateMode | str | None = None,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
        retry_policy: RetryPolicy | None = None,
        budget: BudgetPolicy | None = None,
    ) -> Callable[[AsyncWorkflowHandler], AsyncWorkflowHandler]:
        if state_name not in self.states:
            raise ValueError(f"unknown workflow state: {state_name!r}")
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_on_error = (
            normalize_exception_policy(
                exception_policy if exception_policy is not None else on_error,
                argument="exception_policy" if exception_policy is not None else "on_error",
            )
            if exception_policy is not None or on_error is not None
            else self.on_error
        )
        resolved_mode = normalize_flow_state_mode(mode)

        def decorate(handler: AsyncWorkflowHandler) -> AsyncWorkflowHandler:
            if state_name in self.handlers:
                raise ValueError(f"duplicate workflow state: {state_name!r}")
            self.handlers[state_name] = handler
            if resolved_mode is not None:
                self.state_modes[state_name] = resolved_mode
            else:
                self.state_modes.pop(state_name, None)
            self.error_modes[state_name] = resolved_on_error
            if retry_policy is not None:
                self.retry_policies[state_name] = retry_policy
            else:
                self.retry_policies.pop(state_name, None)
            if budget is not None:
                self.budget_policies[state_name] = budget
            else:
                self.budget_policies.pop(state_name, None)
            return handler

        return decorate

    async def install_policy(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        indexed_state_meta: str | None = None,
        max_active_ms: int | float | str | None = None,
        replace: bool = True,
        expected_generation: int | None = None,
    ) -> PolicySnapshot:
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
        for state_name in set(self.state_modes) | set(self.retry_policies):
            state_policies[state_name] = FlowStatePolicy(
                mode=self.state_modes.get(state_name),
                retry=self.retry_policies.get(state_name),
            )
        kwargs: dict[str, Any] = {
            "retry": resolved_retry_policy,
            "states": state_policies,
            "replace": replace,
            "expected_generation": expected_generation,
        }
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self._invocations.run_while_open(
            lambda: self.client.install_policy(self.type, **kwargs),
            closed_message="workflow is closed",
        )

    async def run_steps_many(
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
        self._ensure_open()
        partition_key = pop_workflow_partition_key(attrs, self.partition_by)

        async def send(client: AsyncFlowClient) -> bytes:
            return await client.run_steps_many(
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

        return cast(
            bytes,
            await self._invocations.run_while_open(
                lambda: self._run_producer(send),
                closed_message="workflow is closed",
            ),
        )

    async def run_once(
        self,
        *,
        worker_index: int = 0,
        state: str | None = None,
    ) -> AsyncWorkflowWorkerResult:
        return await self._invocations.run_while_open(
            lambda: self._run_once_untracked(worker_index=worker_index, state=state),
            closed_message="workflow is closed",
        )

    async def _run_once_untracked(
        self,
        *,
        worker_index: int,
        state: str | None,
    ) -> AsyncWorkflowWorkerResult:
        self._set_current_phase("claim")
        worker_index = worker_index % self.workers
        if state is None and self._should_claim_any_state():
            return await self._run_once_any_state(worker_index)

        state_name = self._next_state(worker_index) if state is None else state
        if state_name not in self.handlers:
            raise ValueError(f"no handler for workflow state: {state_name!r}")
        partition_key, partition_keys = self._next_claim_partition(worker_index)
        limit = self.batch_size
        claim_kwargs = {
            "state": state_name,
            "worker": f"{self.type}:async-workflow:{worker_index}",
            "partition_key": partition_key,
            "partition_keys": partition_keys,
            "limit": limit,
            "priority": self.priority,
            "reclaim_expired": None,
            "block_ms": self.block_ms,
        }
        if self.claim_values is not None:
            jobs = cast(
                list[AsyncFlowJob],
                await self.claim_client.claim_due(
                    self.type,
                    **cast(Any, claim_kwargs),
                    payload=False,
                    values=self.claim_values,
                    value_max_bytes=self.value_max_bytes,
                ),
            )
        else:
            jobs = cast(
                list[AsyncFlowJob],
                await self.claim_client.claim_flows(self.type, **cast(Any, claim_kwargs)),
            )
        result = AsyncWorkflowWorkerResult(claim_calls=1)
        if not jobs:
            return self._merge(result, AsyncWorkflowWorkerResult(empty_claims=1))

        for job in jobs:
            object.__setattr__(job, "type", self.type)
            object.__setattr__(job, "state", "running")
            object.__setattr__(job, "run_state", state_name)

        self._set_current_phase("handle")
        applied = await self._handle_claimed_batch(state_name, jobs)
        return self._merge(
            result,
            AsyncWorkflowWorkerResult(claimed=len(jobs), applied=applied),
        )

    async def _run_once_any_state(self, worker_index: int) -> AsyncWorkflowWorkerResult:
        self._set_current_phase("claim")
        partition_key, partition_keys = self._next_claim_partition(worker_index)
        jobs = await self.claim_client.claim_flows(
            self.type,
            worker=f"{self.type}:async-workflow:{worker_index}",
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=self.batch_size,
            priority=self.priority,
            reclaim_expired=None,
            block_ms=self.block_ms,
            include_state=True,
        )
        result = AsyncWorkflowWorkerResult(claim_calls=1)
        if not jobs:
            return self._merge(result, AsyncWorkflowWorkerResult(empty_claims=1))

        for job in jobs:
            object.__setattr__(job, "type", self.type)
            object.__setattr__(job, "state", "running")

        self._set_current_phase("handle")
        applied = 0
        for state_name, state_jobs in self._group_jobs_by_run_state(jobs).items():
            applied += await self._handle_claimed_batch(state_name, state_jobs)

        return self._merge(
            result,
            AsyncWorkflowWorkerResult(claimed=len(jobs), applied=applied),
        )

    def _should_claim_any_state(self) -> bool:
        return (
            bool(self.block_ms)
            and len(self.states) > 1
            and self.claim_values is None
            and all(state_name in self.handlers for state_name in self.states)
        )

    def _group_jobs_by_run_state(self, jobs: list[ClaimedFlow]) -> dict[str, list[ClaimedFlow]]:
        grouped: dict[str, list[ClaimedFlow]] = {}
        for job in jobs:
            state_name = job.run_state
            if state_name not in self.handlers:
                raise ValueError(f"no handler for workflow state: {state_name!r}")
            grouped.setdefault(state_name, []).append(job)
        return grouped

    async def run(self) -> None:
        self.start_workers()
        await self.join()

    def start_workers(self) -> list[asyncio.Task[None]]:
        """Start workflow consumers and return their owned tasks."""
        self._ensure_open()
        if self._tasks:
            raise RuntimeError("workflow already started")
        self._stop_event.clear()
        self._running = True
        self._tasks = [asyncio.create_task(self._run_loop(index)) for index in range(self.workers)]
        return self._tasks

    def start(
        self, id: str | None = None, *, payload: Any = None, **attrs: Any
    ) -> list[asyncio.Task[None]] | Awaitable[Any]:
        warnings.warn(
            "AsyncWorkflow.start() is deprecated; use start_flow(id, ...) to create a flow "
            "or start_workers() to start consumers",
            DeprecationWarning,
            stacklevel=2,
        )
        self._ensure_open()
        if id is not None:
            return self.start_flow(id, payload=payload, **attrs)
        if payload is not None or attrs:
            raise TypeError("workflow worker start takes no payload/attrs unless id is provided")
        return self.start_workers()

    async def join(self) -> AsyncWorkflowWorkerResult:
        if self._tasks:
            joined = asyncio.gather(*self._tasks)
            try:
                await await_cancellation_safe(joined)
            except asyncio.CancelledError:
                self.stop()
                raise
            except BaseException:
                self.stop()
                await asyncio.gather(*self._tasks, return_exceptions=True)
                raise
        return self._totals

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        for task in self._tasks:
            if (
                task is not current
                and not task.done()
                and self._task_phases.get(task, "scheduled") in {"scheduled", "idle"}
            ):
                task.cancel()

    async def close(self, timeout: float | None = 5.0) -> None:
        deadline = CloseDeadline.start(timeout)
        if self._closed:
            return
        self._close_started = True
        self._invocations.begin_close()
        self.stop()
        close_task = self._close_task
        if close_task is None:
            close_task = asyncio.create_task(self._close_in_phases(CloseDeadline.start(None)))
            self._close_task = close_task
            close_task.add_done_callback(self._close_task_finished)
        await deadline.wait_task(close_task, "async workflow close timed out")

    async def _close_in_phases(self, deadline: CloseDeadline) -> None:
        timeout_message = "async workflow close timed out"
        self.stop()
        await deadline.wait_tasks(self._tasks, timeout_message)
        await self._invocations.wait_for_idle(deadline, timeout_message)
        worker_error, worker_traceback = self._consume_worker_tasks()
        cleanup_error = await self._close_owned_resources(deadline, timeout_message)

        if self._producer_loop is None and not self._owns_client and not self._owns_claim_client:
            self._closed = True

        if worker_error is not None:
            raise_primary_with_cleanup(worker_error, worker_traceback, cleanup_error)
        if cleanup_error is not None:
            raise cleanup_error
        deadline.check(timeout_message)

    def _consume_worker_tasks(self) -> tuple[BaseException | None, TracebackType | None]:
        worker_error: BaseException | None = None
        worker_traceback = None
        for task in self._tasks:
            task_error = task_terminal_error(task)
            if worker_error is None and task_error is not None:
                worker_error = task_error
                worker_traceback = task_error.__traceback__
        self._tasks.clear()
        self._task_phases.clear()
        return worker_error, worker_traceback

    async def _close_owned_resources(
        self,
        deadline: CloseDeadline,
        timeout_message: str,
    ) -> BaseException | None:
        resources: list[AsyncOwnedClose] = []
        if self._producer_loop is not None:
            resources.append(
                AsyncOwnedClose(
                    self._producer_loop,
                    lambda: setattr(self, "_producer_loop", None),
                )
            )
        if self.client is self.claim_client:
            if self._owns_client or self._owns_claim_client:
                resources.append(AsyncOwnedClose(self.client, self._release_shared_client))
        else:
            if self._owns_client:
                resources.append(
                    AsyncOwnedClose(
                        self.client,
                        lambda: setattr(self, "_owns_client", False),
                    )
                )
            if self._owns_claim_client:
                resources.append(
                    AsyncOwnedClose(
                        self.claim_client,
                        lambda: setattr(self, "_owns_claim_client", False),
                    )
                )

        async def close_resource(resource: Any) -> None:
            await _close_async_resource(
                resource,
                deadline,
                timeout_message,
                self._close_operations,
            )

        try:
            await close_owned_resources_async(
                resources,
                close_resource,
                max_concurrency=1,
            )
        except BaseException as exc:
            return exc
        return None

    def _release_shared_client(self) -> None:
        self._owns_client = False
        self._owns_claim_client = False

    def _close_task_finished(self, task: asyncio.Task[None]) -> None:
        if self._close_task is task and not self._closed:
            self._close_task = None

    def _ensure_open(self) -> None:
        if self._close_started:
            raise RuntimeError("workflow is closed")

    async def _run_loop(self, worker_index: int) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._task_phases[task] = "claim"
        try:
            while self._running:
                result = await self.run_once(worker_index=worker_index)
                self._totals = self._merge(self._totals, result)
                if result.claimed == 0:
                    self._set_current_phase("idle")
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            self._stop_event.wait(),
                            timeout=self.idle_sleep_s,
                        )
        except asyncio.CancelledError:
            if not self._stop_event.is_set():
                raise
        except BaseException:
            self.stop()
            raise
        finally:
            if task is not None:
                self._task_phases.pop(task, None)
            self._running = False

    def _set_current_phase(self, phase: str) -> None:
        task = asyncio.current_task()
        if task is not None and task in self._tasks:
            self._task_phases[task] = phase

    async def _handle_claimed_batch(
        self,
        state_name: str,
        jobs: Sequence[AsyncFlowJob],
    ) -> int:
        return await handle_claimed_batch(self, state_name, jobs)

    def _job_mutation(
        self,
        job: AsyncFlowJob,
        outcome: Transition | Complete | Retry | Fail,
    ) -> JobMutation:
        return build_job_mutation(
            job,
            outcome,
            validate_transition=self._validate_transition_policy,
        )

    def _validate_transition_policy(self, outcome: Transition) -> None:
        if (
            self.state_modes.get(outcome.to_state) == FlowStateMode.FIFO.value
            and outcome.priority is not None
        ):
            raise ValueError("priority is not supported for fifo state")

    def _normalize_outcome(self, value: Any) -> Transition | Complete | Retry | Fail:
        if isinstance(value, (Transition, Complete, Retry, Fail)):
            return value
        return complete(result=value)

    def _merge_governance_attributes(
        self, value: Any, attributes: dict[str, Any]
    ) -> Transition | Complete | Retry | Fail:
        outcome = self._normalize_outcome(value)
        if not attributes:
            return outcome
        merged = dict(outcome.attributes_merge or {})
        merged.update(attributes)
        return replace(outcome, attributes_merge=merged)

    async def _apply_uniform(
        self,
        state_name: str,
        jobs: list[ClaimedFlow],
        outcome: Transition | Complete | Retry | Fail,
    ) -> None:
        await apply_uniform(self, state_name, jobs, outcome)

    def _next_state(self, worker_index: int) -> str:
        state_name = self.states[self._state_cursors[worker_index] % len(self.states)]
        self._state_cursors[worker_index] += 1
        return state_name

    def _next_claim_partition(
        self, worker_index: int
    ) -> tuple[str | bytes | None, list[str | bytes] | None]:
        del worker_index
        return None, None

    @staticmethod
    def _uniform_partition_key(jobs: list[ClaimedFlow]) -> str | bytes | None:
        first = jobs[0].partition_key
        if first is not None and all(job.partition_key == first for job in jobs):
            return first
        return None

    @staticmethod
    def _merge(
        left: AsyncWorkflowWorkerResult,
        right: AsyncWorkflowWorkerResult,
    ) -> AsyncWorkflowWorkerResult:
        return AsyncWorkflowWorkerResult(
            claimed=left.claimed + right.claimed,
            applied=left.applied + right.applied,
            claim_calls=left.claim_calls + right.claim_calls,
            empty_claims=left.empty_claims + right.empty_claims,
        )


_ASYNC_WORKFLOW_CLIENT_EXPORTS = frozenset({"AsyncWorkflowClient"})


def __getattr__(name: str) -> Any:
    if name not in _ASYNC_WORKFLOW_CLIENT_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from ferricstore import async_workflow_client

    return getattr(async_workflow_client, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _ASYNC_WORKFLOW_CLIENT_EXPORTS)
