from __future__ import annotations

import asyncio
import builtins
import contextlib
import inspect
import uuid
import warnings
import zlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.async_client import AsyncFlowClient
from ferricstore.batch_core import BatchValueMatcher, run_async_fanout
from ferricstore.client import FlowClient
from ferricstore.command_core import (
    FLOW_AUTO_PARTITION_BUCKETS as AUTO_PARTITION_BUCKETS,
)
from ferricstore.command_core import (
    FLOW_AUTO_PARTITION_PREFIX,
    flow_auto_partition_index,
    flow_auto_partition_key_for_index,
)
from ferricstore.lifecycle_core import (
    AsyncCloseCoordinator,
    AsyncCloseTaskRegistry,
    await_cancellation_safe,
    close_resources_async,
    raise_primary_with_cleanup,
)
from ferricstore.mutation_core import JobMutation, MutationBatchPlan, MutationKind
from ferricstore.types import (
    BudgetPolicy,
    BudgetResult,
    ChildSpec,
    ClaimedFlow,
    CreateItem,
    EffectResult,
    ExceptionPolicy,
    FencedItem,
    FlowRecord,
    FlowStateMode,
    FlowStatePolicy,
    FlowStatePolicyLike,
    RetryPolicy,
    ValueConfig,
    WorkerConfig,
    normalize_exception_policy,
    normalize_flow_state_mode,
    resolve_worker_connection_counts,
)
from ferricstore.worker import QueueFlowWorkerResult
from ferricstore.worker_core import (
    AsyncWorkerInvocationTracker,
    CloseDeadline,
    WorkerIdleScheduler,
    can_fuse_complete_claim,
    task_terminal_error,
    validate_many_result,
)
from ferricstore.workflow import (
    Complete,
    Fail,
    Retry,
    Transition,
    complete,
    fail,
    retry,
)
from ferricstore.workflow_core import pop_workflow_partition_key

AsyncFlowJob = ClaimedFlow | FlowRecord
AsyncFlowHandler = Callable[[AsyncFlowJob], Any | Awaitable[Any]]
AsyncFlowBatchHandler = Callable[[list[AsyncFlowJob]], Any | Awaitable[Any]]
AsyncErrorMode = ExceptionPolicy | str
AsyncWorkflowHandler = Callable[[Any], Any | Awaitable[Any]]

AUTO_PARTITION_PREFIX = FLOW_AUTO_PARTITION_PREFIX
SERVER_SLOT_COUNT = 1024
FLOW_MANY_BATCH_LIMIT = 1000
_CURRENT_PARTITION = object()
ASYNC_QUEUE_WORKER_CONFIG_KEYS = frozenset(
    {
        "workers",
        "concurrency",
        "command_connections",
        "claim_connections",
        "batch_size",
        "claim_partition_batch_size",
        "complete_independent",
        "server_shards",
        "claim_values",
        "value_max_bytes",
        "block_ms",
        "idle_sleep_s",
        "max_idle_sleep_s",
        "producer_loop_thread",
        "protocol_wake_hints",
        "fuse_complete_claim",
        "exception_policy",
    }
)

_PROTOCOL_URL_SCHEMES = {"ferric", "ferrics"}


def _is_protocol_url(value: str) -> bool:
    return urlparse(value).scheme.lower() in _PROTOCOL_URL_SCHEMES


async def _close_async_resource(
    resource: Any,
    deadline: CloseDeadline,
    timeout_message: str,
    operations: AsyncCloseTaskRegistry,
) -> None:
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    await operations.run(
        resource,
        close,
        lambda task: deadline.wait_task(task, timeout_message),
    )


ASYNC_WORKFLOW_CONFIG_KEYS = frozenset(
    {
        "workers",
        "concurrency",
        "command_connections",
        "claim_connections",
        "batch_size",
        "claim_partition_batch_size",
        "server_shards",
        "idle_sleep_s",
        "block_ms",
        "producer_loop_thread",
        "exception_policy",
        "priority",
        "claim_values",
        "value_max_bytes",
    }
)


def _auto_partition_key(index: int) -> str:
    return flow_auto_partition_key_for_index(index)


def _auto_partition_index_for_id(id: str) -> int:
    return flow_auto_partition_index(id)


def _server_shard_for_slot(slot: int, server_shards: int) -> int:
    server_shards = max(server_shards, 1)
    slots_per_shard = SERVER_SLOT_COUNT // server_shards
    remainder = SERVER_SLOT_COUNT % server_shards
    wide_slots = (slots_per_shard + 1) * remainder
    slot = slot % SERVER_SLOT_COUNT
    if slot < wide_slots:
        return slot // (slots_per_shard + 1)
    return remainder + ((slot - wide_slots) // slots_per_shard)


def _auto_partition_server_shard(index: int, server_shards: int) -> int:
    tag = f"fa:{index % AUTO_PARTITION_BUCKETS}"
    slot = zlib.crc32(tag.encode()) & (SERVER_SLOT_COUNT - 1)
    return _server_shard_for_slot(slot, server_shards)


def _auto_partition_owner(index: int, workers: int, server_shards: int) -> int:
    workers = max(workers, 1)
    server_shards = max(server_shards, 1)
    shard = _auto_partition_server_shard(index, server_shards)
    if workers <= server_shards:
        return shard % workers
    shard_workers = [worker for worker in range(workers) if worker % server_shards == shard]
    if not shard_workers:
        return shard % workers
    return shard_workers[index % len(shard_workers)]


def _owned_auto_partition_keys(
    *,
    worker_index: int,
    workers: int,
    server_shards: int,
) -> list[str]:
    worker_index = worker_index % max(workers, 1)
    return [
        _auto_partition_key(index)
        for index in range(AUTO_PARTITION_BUCKETS)
        if _auto_partition_owner(index, workers, server_shards) == worker_index
    ]


def _client_from(client: AsyncFlowClient | FlowClient | str | Any) -> AsyncFlowClient:
    if isinstance(client, str):
        return AsyncFlowClient.from_url(client)
    if isinstance(client, FlowClient):
        raise TypeError(
            "async Flow SDK requires AsyncFlowClient, an async executor, or URL; "
            "FlowClient is sync-only"
        )
    if hasattr(client, "execute_command") and not hasattr(client, "claim_flows"):
        return AsyncFlowClient(client)
    return client


@dataclass
class _AsyncHandledBatch:
    jobs: list[AsyncFlowJob]
    first_result: Any = None
    mixed_results: list[tuple[AsyncFlowJob, Any]] | None = None
    failures: list[tuple[AsyncFlowJob, Exception]] | None = None


class AsyncQueueFlowWorker:
    """Async queue worker for the optimized FerricFlow queue path."""

    def __init__(
        self,
        client: AsyncFlowClient | FlowClient | str | Any,
        *,
        type: str,
        worker: str | None = None,
        state: str | None = None,
        states: Sequence[str] | None = None,
        concurrency: int = 1,
        batch_size: int = 10,
        lease_ms: int = 30_000,
        priority: int | None = 0,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        block_ms: int | None = None,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        protocol_wake_hints: bool = False,
        fuse_complete_claim: bool = False,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
        complete_independent: bool = True,
        partition_key: str | None = None,
        partition_keys: Sequence[str] | None = None,
        auto_partitions: bool = False,
        worker_index: int = 0,
        workers: int = 1,
        server_shards: int = 16,
        claim_partition_batch_size: int = 1,
        close_client: bool | None = None,
        claim_client: AsyncFlowClient | FlowClient | str | Any | None = None,
        command_connections: int | None = None,
        claim_connections: int | None = None,
    ) -> None:
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")
        if states is not None and not states:
            raise ValueError("states must be non-empty")
        if partition_keys is not None and not partition_keys:
            raise ValueError("partition_keys must be non-empty")
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if workers <= 0:
            raise ValueError("workers must be positive")
        if claim_partition_batch_size <= 0:
            raise ValueError("claim_partition_batch_size must be positive")
        if block_ms is not None and block_ms < 0:
            raise ValueError("block_ms must be non-negative")
        if idle_sleep_s < 0:
            raise ValueError("idle_sleep_s must be non-negative")
        if max_idle_sleep_s is not None and max_idle_sleep_s < 0:
            raise ValueError("max_idle_sleep_s must be non-negative")
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_on_error = normalize_exception_policy(
            exception_policy if exception_policy is not None else on_error,
            argument="exception_policy" if exception_policy is not None else "on_error",
        )

        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=workers,
            concurrency=concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
        command_max_connections = (
            1 if isinstance(client, str) and command_connections is None else command_pool_size
        )
        if isinstance(client, str):
            self.client = AsyncFlowClient.from_url(client, max_connections=command_max_connections)
            self._close_client = True if close_client is None else close_client
        else:
            self.client = _client_from(client)
            self._close_client = False if close_client is None else close_client
        if claim_client is None:
            if isinstance(client, str):
                self.claim_client = AsyncFlowClient.from_url(
                    client, max_connections=claim_pool_size
                )
                self._close_claim_client = True
            else:
                self.claim_client = self.client
                self._close_claim_client = False
        elif isinstance(claim_client, str):
            self.claim_client = AsyncFlowClient.from_url(
                claim_client, max_connections=claim_pool_size
            )
            self._close_claim_client = True
        else:
            self.claim_client = _client_from(claim_client)
            self._close_claim_client = False

        self.type = type
        self.worker = worker or f"{type}:async-worker:{uuid.uuid4().hex}"
        self.state = state
        self.states = list(states) if states is not None else None
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.lease_ms = lease_ms
        self.priority = priority
        self.reclaim_expired = reclaim_expired
        self.reclaim_ratio = reclaim_ratio
        self.claim_values = list(claim_values) if claim_values is not None else None
        self.value_max_bytes = value_max_bytes
        self.block_ms = block_ms
        self.idle_sleep_s = max(idle_sleep_s, 0.0)
        self.max_idle_sleep_s = max(
            self.idle_sleep_s if max_idle_sleep_s is None else max_idle_sleep_s,
            self.idle_sleep_s,
        )
        self.protocol_wake_hints = bool(protocol_wake_hints)
        self.fuse_complete_claim = bool(fuse_complete_claim)
        self.on_error = resolved_on_error
        self.complete_independent = complete_independent
        self.partition_key = partition_key
        self.partition_keys: list[str] | None
        if partition_keys is not None:
            self.partition_keys = list(partition_keys)
        elif partition_key is None and auto_partitions:
            self.partition_keys = _owned_auto_partition_keys(
                worker_index=worker_index,
                workers=workers,
                server_shards=server_shards,
            )
        else:
            self.partition_keys = None
        self.claim_partition_batch_size = claim_partition_batch_size
        self._partition_cursor = 0
        self._running = False
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._active_task: asyncio.Task[None] | None = None
        self._phase = "stopped"
        self._totals = QueueFlowWorkerResult()
        self._prefetched_jobs: list[AsyncFlowJob] = []
        self._protocol_wake_hints_enabled = False
        self._protocol_wake_hints_subscribed = False
        self._invocations = AsyncWorkerInvocationTracker()
        self._close_coordinator = AsyncCloseCoordinator()
        self._close_operations = AsyncCloseTaskRegistry()

    async def run(self, handler: AsyncFlowHandler) -> None:
        await self.run_forever(handler)

    async def run_forever(self, handler: AsyncFlowHandler) -> None:
        self._begin_run()
        await self._run_loop(handler, batch_handler=False)

    async def run_batch_forever(self, handler: AsyncFlowBatchHandler) -> None:
        self._begin_run()
        await self._run_loop(handler, batch_handler=True)

    def start(
        self,
        handler: AsyncFlowHandler | AsyncFlowBatchHandler,
        *,
        batch_handler: bool = False,
    ) -> asyncio.Task[None]:
        if self._task is not None and not self._task.done():
            raise RuntimeError("worker already started")
        self._begin_run()
        self._task = asyncio.create_task(self._run_loop(handler, batch_handler=batch_handler))
        return self._task

    async def join(self) -> QueueFlowWorkerResult:
        if self._task is not None:
            await await_cancellation_safe(self._task)
        return self.stats

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> QueueFlowWorkerResult:
        return self._totals

    def _begin_run(self) -> None:
        if self._invocations.closing:
            raise RuntimeError("async queue worker is closed")
        if self._active_task is not None and not self._active_task.done():
            raise RuntimeError("worker already running")
        self._stop_event.clear()
        self._running = True

    async def _run_loop(
        self,
        handler: AsyncFlowHandler | AsyncFlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> None:
        self._active_task = asyncio.current_task()
        idle = WorkerIdleScheduler(self.idle_sleep_s, self.max_idle_sleep_s)
        try:
            self._phase = "subscribe"
            await self._subscribe_protocol_wake_hints()
            while self._running or self._prefetched_jobs:
                result = (
                    await self._run_batch_once(
                        cast(AsyncFlowBatchHandler, handler), allow_fusion=True
                    )
                    if batch_handler
                    else await self._run_once(cast(AsyncFlowHandler, handler), allow_fusion=True)
                )
                self._totals = QueueFlowWorkerResult(
                    claimed=self._totals.claimed + result.claimed,
                    completed=self._totals.completed + result.completed,
                    retried=self._totals.retried + result.retried,
                    failed=self._totals.failed + result.failed,
                    claim_calls=self._totals.claim_calls + result.claim_calls,
                )
                if result.claimed == 0:
                    delay = idle.after_batch(0)
                    self._phase = "idle"
                    if not await self._wait_for_protocol_wake_hint(delay):
                        with contextlib.suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                else:
                    idle.after_batch(result.claimed)
        except asyncio.CancelledError:
            if not self._stop_event.is_set():
                raise
        finally:
            self._running = False
            self._phase = "stopped"
            self._active_task = None

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        task = self._active_task
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        if (
            task is not None
            and task is not current
            and not task.done()
            and self._phase in {"subscribe", "idle"}
        ):
            task.cancel()

    async def close(self, timeout: float | None = 5.0) -> None:
        deadline = CloseDeadline.start(timeout)
        self._invocations.begin_close()
        await self._close_coordinator.run(lambda: self._close_once(deadline))

    async def _close_once(self, deadline: CloseDeadline) -> None:
        timeout_message = "async queue worker close timed out"
        tasks = list(
            dict.fromkeys(task for task in (self._task, self._active_task) if task is not None)
        )
        self.stop()
        await deadline.wait_tasks(tasks, timeout_message)
        await self._invocations.wait_for_idle(deadline, timeout_message)
        error: BaseException | None = None
        for task in tasks:
            task_error = task_terminal_error(task)
            if error is None and task_error is not None:
                error = task_error
        for should_close, resource in (
            (self._close_client, self.client),
            (
                self._close_claim_client and self.claim_client is not self.client,
                self.claim_client,
            ),
        ):
            if not should_close:
                continue
            try:
                await _close_async_resource(
                    resource,
                    deadline,
                    timeout_message,
                    self._close_operations,
                )
            except BaseException as exc:
                if error is None:
                    error = exc
            else:
                if resource is self.client:
                    self._close_client = False
                    if self.claim_client is self.client:
                        self._close_claim_client = False
                if resource is self.claim_client:
                    self._close_claim_client = False
        deadline.check(timeout_message)
        if error is not None:
            raise error

    async def _subscribe_protocol_wake_hints(self) -> None:
        if self._protocol_wake_hints_subscribed or not self.protocol_wake_hints:
            return
        subscribe = getattr(self.claim_client, "subscribe_flow_wake", None)
        wait_event = getattr(self.claim_client, "wait_event", None)
        if not callable(subscribe) or not callable(wait_event):
            return
        result = subscribe(
            self.type,
            state=self.state,
            states=self.states,
            partition_key=self.partition_key,
            partition_keys=self.partition_keys,
            priority=self.priority,
            limit=self.batch_size,
        )
        if inspect.isawaitable(result):
            await result
        self._protocol_wake_hints_subscribed = True
        self._protocol_wake_hints_enabled = True

    async def _wait_for_protocol_wake_hint(self, timeout_s: float) -> bool:
        if not self._protocol_wake_hints_enabled or timeout_s <= 0:
            return False
        wait_event = getattr(self.claim_client, "wait_event", None)
        if not callable(wait_event):
            return False
        result = wait_event(timeout=timeout_s)
        if inspect.isawaitable(result):
            result = await result
        return result is not None

    async def run_once(self, handler: AsyncFlowHandler) -> QueueFlowWorkerResult:
        return await self._invocations.run_while_open(
            lambda: self._run_once(handler, allow_fusion=False),
            closed_message="async queue worker is closed",
        )

    async def _run_once(
        self,
        handler: AsyncFlowHandler,
        *,
        allow_fusion: bool,
    ) -> QueueFlowWorkerResult:
        self._phase = "claim"
        jobs, claim_calls = await self._next_jobs()
        result = QueueFlowWorkerResult(claim_calls=claim_calls)
        if not jobs:
            return result

        self._phase = "handle"
        handled = await self._run_handlers(jobs, handler)
        self._phase = "finish"
        finished, fused_claims = await self._finish_or_fuse(
            handled,
            allow_fusion=allow_fusion,
        )
        return QueueFlowWorkerResult(
            claimed=len(jobs),
            completed=finished.completed,
            retried=finished.retried,
            failed=finished.failed,
            claim_calls=claim_calls + fused_claims,
        )

    async def run_batch_once(self, handler: AsyncFlowBatchHandler) -> QueueFlowWorkerResult:
        return await self._invocations.run_while_open(
            lambda: self._run_batch_once(handler, allow_fusion=False),
            closed_message="async queue worker is closed",
        )

    async def _run_batch_once(
        self,
        handler: AsyncFlowBatchHandler,
        *,
        allow_fusion: bool,
    ) -> QueueFlowWorkerResult:
        self._phase = "claim"
        jobs, claim_calls = await self._next_jobs()
        result = QueueFlowWorkerResult(claim_calls=claim_calls)
        if not jobs:
            return result

        self._phase = "handle"
        handled = await self._run_batch_handler(jobs, handler)
        self._phase = "finish"
        finished, fused_claims = await self._finish_or_fuse(
            handled,
            allow_fusion=allow_fusion,
        )
        return QueueFlowWorkerResult(
            claimed=len(jobs),
            completed=finished.completed,
            retried=finished.retried,
            failed=finished.failed,
            claim_calls=claim_calls + fused_claims,
        )

    async def _next_jobs(self) -> tuple[list[AsyncFlowJob], int]:
        if self._prefetched_jobs:
            jobs = self._prefetched_jobs
            self._prefetched_jobs = []
            return jobs, 0

        partition_key, partition_keys = self._next_claim_partition()
        jobs = await self._claim_flows(
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=self.batch_size,
        )
        return jobs, 1

    async def _finish_or_fuse(
        self,
        handled: _AsyncHandledBatch,
        *,
        allow_fusion: bool,
    ) -> tuple[QueueFlowWorkerResult, int]:
        fuse = getattr(self.claim_client, "complete_flows_and_claim_flows", None)
        if can_fuse_complete_claim(
            enabled=(
                allow_fusion
                and self._running
                and not self._stop_event.is_set()
                and self.fuse_complete_claim
            ),
            has_jobs=bool(handled.jobs),
            has_mixed_results=handled.mixed_results is not None,
            has_failures=bool(handled.failures),
            claims_values=bool(self.claim_values),
            supported=callable(fuse),
        ):
            if not callable(fuse):
                return await self._finish_batch(handled), 0
            partition_key, partition_keys = self._next_claim_partition()
            self._phase = "claim"
            next_jobs = await fuse(
                cast(list[ClaimedFlow], handled.jobs),
                result=handled.first_result,
                independent=self.complete_independent,
                type=self.type,
                state=self.state,
                states=self.states,
                worker=self.worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=self.lease_ms,
                limit=self.batch_size,
                priority=self.priority,
                block_ms=self.block_ms,
                reclaim_expired=self.reclaim_expired,
                reclaim_ratio=self.reclaim_ratio,
            )
            self._prefetched_jobs = list(next_jobs)
            return QueueFlowWorkerResult(completed=len(handled.jobs)), 1

        return await self._finish_batch(handled), 0

    async def _claim_flows(
        self,
        *,
        partition_key: str | None,
        partition_keys: list[str] | None,
        limit: int,
    ) -> list[AsyncFlowJob]:
        if self.claim_values:
            return cast(
                list[AsyncFlowJob],
                await self.claim_client.claim_due(
                    self.type,
                    state=self.state,
                    states=self.states,
                    worker=self.worker,
                    partition_key=partition_key,
                    partition_keys=partition_keys,
                    lease_ms=self.lease_ms,
                    limit=limit,
                    priority=self.priority,
                    reclaim_expired=self.reclaim_expired,
                    reclaim_ratio=self.reclaim_ratio,
                    block_ms=self.block_ms,
                    payload=False,
                    values=self.claim_values,
                    value_max_bytes=self.value_max_bytes,
                ),
            )

        return cast(
            list[AsyncFlowJob],
            await self.claim_client.claim_flows(
                self.type,
                state=self.state,
                states=self.states,
                worker=self.worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=self.lease_ms,
                limit=limit,
                priority=self.priority,
                reclaim_expired=self.reclaim_expired,
                reclaim_ratio=self.reclaim_ratio,
                block_ms=self.block_ms,
            ),
        )

    async def _run_handlers(
        self,
        jobs: list[AsyncFlowJob],
        handler: AsyncFlowHandler,
    ) -> _AsyncHandledBatch:
        async def run_one(job: AsyncFlowJob) -> tuple[AsyncFlowJob, bool, Any]:
            try:
                value = handler(job)
                if inspect.isawaitable(value):
                    value = await value
                return job, True, value
            except Exception as exc:
                return job, False, exc

        results = await run_async_fanout(
            jobs,
            run_one,
            concurrent=True,
            max_concurrency=self.concurrency,
        )
        return self._handled_from_results(results)

    async def _run_batch_handler(
        self,
        jobs: list[AsyncFlowJob],
        handler: AsyncFlowBatchHandler,
    ) -> _AsyncHandledBatch:
        try:
            value = handler(jobs)
            if inspect.isawaitable(value):
                value = await value
        except Exception as exc:
            return _AsyncHandledBatch(jobs=[], failures=[(job, exc) for job in jobs])

        return _AsyncHandledBatch(jobs=jobs, first_result=value)

    @staticmethod
    def _handled_from_results(
        results: list[tuple[AsyncFlowJob, bool, Any]],
    ) -> _AsyncHandledBatch:
        success_jobs: list[AsyncFlowJob] = []
        failures: list[tuple[AsyncFlowJob, Exception]] = []
        first_result: Any = None
        first_result_set = False
        first_result_matcher: BatchValueMatcher | None = None
        mixed_results: list[tuple[AsyncFlowJob, Any]] | None = None

        for job, ok, value in results:
            if not ok:
                failures.append((job, value))
                continue

            if not first_result_set:
                first_result = value
                first_result_set = True
                first_result_matcher = BatchValueMatcher(value)
                success_jobs.append(job)
                continue

            if (
                mixed_results is None
                and first_result_matcher is not None
                and not first_result_matcher.matches(value)
            ):
                mixed_results = [(existing, first_result) for existing in success_jobs]

            success_jobs.append(job)
            if mixed_results is not None:
                mixed_results.append((job, value))

        return _AsyncHandledBatch(
            jobs=success_jobs,
            first_result=first_result,
            mixed_results=mixed_results,
            failures=failures,
        )

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

    def _next_claim_partition(self) -> tuple[str | None, list[str] | None]:
        if self.partition_key is not None:
            return self.partition_key, None
        if not self.partition_keys:
            return None, None

        count = min(self.claim_partition_batch_size, len(self.partition_keys))
        keys = [
            self.partition_keys[(self._partition_cursor + offset) % len(self.partition_keys)]
            for offset in range(count)
        ]
        self._partition_cursor = (self._partition_cursor + count) % len(self.partition_keys)
        if len(keys) == 1:
            return keys[0], None
        return None, keys


class AsyncQueueFlow:
    """Simple async queue facade that hides FerricFlow hot-path tuning."""

    def __init__(
        self,
        client: AsyncFlowClient | FlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | FlowClient | str | Any | None = None,
        type: str,
        state: str = "queued",
        workers: int = 1,
        concurrency: int = 1,
        command_connections: int | None = None,
        claim_connections: int | None = None,
        batch_size: int = 10,
        claim_partition_batch_size: int = 1,
        complete_independent: bool = True,
        server_shards: int = 16,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        protocol_wake_hints: bool = False,
        fuse_complete_claim: bool = False,
        block_ms: int | None = None,
        producer_loop_thread: bool = False,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
    ) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")
        if block_ms is not None and block_ms < 0:
            raise ValueError("block_ms must be non-negative")
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_on_error = normalize_exception_policy(
            exception_policy if exception_policy is not None else on_error,
            argument="exception_policy" if exception_policy is not None else "on_error",
        )
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=workers,
            concurrency=concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
        self._url = client if isinstance(client, str) else None
        command_max_connections = (
            1 if isinstance(client, str) and command_connections is None else command_pool_size
        )
        if isinstance(client, str):
            self._owns_client = True
            self.client = AsyncFlowClient.from_url(client, max_connections=command_max_connections)
        else:
            self._owns_client = False
            self.client = _client_from(client)
        if claim_client is None:
            if isinstance(client, str):
                self.claim_client = AsyncFlowClient.from_url(
                    client, max_connections=claim_pool_size
                )
                self._owns_claim_client = True
            else:
                self.claim_client = self.client
                self._owns_claim_client = False
        elif isinstance(claim_client, str):
            self.claim_client = AsyncFlowClient.from_url(
                claim_client, max_connections=claim_pool_size
            )
            self._owns_claim_client = True
        else:
            self.claim_client = _client_from(claim_client)
            self._owns_claim_client = False
        self.type = type
        self.state = state
        self.workers = workers
        self.concurrency = concurrency
        self.batch_size = min(batch_size, FLOW_MANY_BATCH_LIMIT)
        self.claim_partition_batch_size = claim_partition_batch_size
        self.complete_independent = complete_independent
        self.server_shards = server_shards
        self.claim_values = list(claim_values) if claim_values is not None else None
        self.value_max_bytes = value_max_bytes
        self.idle_sleep_s = idle_sleep_s
        self.max_idle_sleep_s = max_idle_sleep_s
        self.protocol_wake_hints = bool(protocol_wake_hints)
        self.fuse_complete_claim = bool(fuse_complete_claim)
        self.block_ms = block_ms
        self.producer_loop_thread = producer_loop_thread
        self.on_error = resolved_on_error
        self._workers: list[AsyncQueueFlowWorker] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._close_started = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._invocations = AsyncWorkerInvocationTracker()
        self._close_operations = AsyncCloseTaskRegistry()

    async def enqueue(self, id: str, *, payload: Any = None, **attrs: Any) -> Any:
        state = attrs.pop("state", self.state)
        partition_key = attrs.pop("partition_key", None)
        return_record = attrs.pop("return_record", False)

        async def send(client: AsyncFlowClient) -> Any:
            return await client.enqueue(
                id,
                type=self.type,
                state=state,
                payload=payload,
                partition_key=partition_key,
                return_record=return_record,
                **attrs,
            )

        result = await self._invocations.run_while_open(
            lambda: self._run_producer(send),
            closed_message="queue flow is closed",
        )
        return result

    async def signal(self, id: str, **kwargs: Any) -> Any:
        return await self._invocations.run_while_open(
            lambda: self.client.signal(id, **kwargs),
            closed_message="queue flow is closed",
        )

    async def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

    async def enqueue_many(
        self,
        items: Sequence[CreateItem | tuple[str, Any] | str],
        **attrs: Any,
    ) -> Any:
        state = attrs.pop("state", self.state)
        independent = attrs.pop("independent", True)
        create_items = [
            item
            if isinstance(item, CreateItem)
            else CreateItem(item[0], item[1])
            if isinstance(item, tuple)
            else CreateItem(item)
            for item in items
        ]

        async def send(client: AsyncFlowClient) -> Any:
            return await client.enqueue_many(
                create_items,
                type=self.type,
                state=state,
                independent=independent,
                **attrs,
            )

        result = await self._invocations.run_while_open(
            lambda: self._run_producer(send),
            closed_message="queue flow is closed",
        )
        return result

    async def run_once(
        self, handler: AsyncFlowHandler, *, worker_index: int = 0
    ) -> QueueFlowWorkerResult:
        async def run_worker_once() -> QueueFlowWorkerResult:
            worker = self._build_worker(worker_index)
            return await worker.run_once(handler)

        return await self._invocations.run_while_open(
            run_worker_once,
            closed_message="queue flow is closed",
        )

    async def run(self, handler: AsyncFlowHandler) -> None:
        self.start(handler)
        await self.join()

    def start(self, handler: AsyncFlowHandler) -> list[asyncio.Task[None]]:
        if self._close_started:
            raise RuntimeError("queue flow is closed")
        if self._tasks:
            raise RuntimeError("queue flow already started")
        self._workers = [self._build_worker(index) for index in range(self.workers)]
        self._tasks = [worker.start(handler) for worker in self._workers]
        return self._tasks

    async def join(self) -> list[QueueFlowWorkerResult]:
        tasks = [asyncio.create_task(worker.join()) for worker in self._workers]
        if not tasks:
            return []
        joined = asyncio.gather(*tasks)
        try:
            return await await_cancellation_safe(joined)
        except asyncio.CancelledError:
            self.stop()
            raise
        except BaseException:
            self.stop()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    def stop(self) -> None:
        for worker in self._workers:
            worker.stop()

    async def close(self, timeout: float | None = 5.0) -> None:
        deadline = CloseDeadline.start(timeout)
        if self._closed:
            return
        self._close_started = True
        self._invocations.begin_close()
        self.stop()
        close_task = self._close_task
        if close_task is None:
            close_task = asyncio.create_task(self._close_in_phases(deadline))
            self._close_task = close_task
            close_task.add_done_callback(self._close_task_finished)
        await await_cancellation_safe(close_task)

    async def _close_in_phases(self, deadline: CloseDeadline) -> None:
        timeout_message = "async queue flow close timed out"
        await self._invocations.wait_for_idle(deadline, timeout_message)
        worker_error: BaseException | None = None
        worker_traceback = None
        worker_resources: list[Callable[[], Awaitable[None]]] = []
        for worker in tuple(self._workers):

            async def close_worker(
                resource: AsyncQueueFlowWorker = worker,
            ) -> None:
                await _close_async_resource(
                    resource,
                    deadline,
                    timeout_message,
                    self._close_operations,
                )

            worker_resources.append(close_worker)
        try:
            await close_resources_async(worker_resources)
        except BaseException as exc:
            if self._contains_timeout(exc):
                raise
            worker_error = exc
            worker_traceback = exc.__traceback__

        # A non-timeout worker failure is terminal: every worker close was
        # attempted, so transports can now be retired without racing work.
        self._workers.clear()
        self._tasks.clear()

        client_error: BaseException | None = None
        client_resources: list[Callable[[], Any]] = []

        if self._owns_client:

            async def close_client() -> None:
                await _close_async_resource(
                    self.client,
                    deadline,
                    timeout_message,
                    self._close_operations,
                )
                self._owns_client = False

            client_resources.append(close_client)

        if self._owns_claim_client and self.claim_client is not self.client:

            async def close_claim_client() -> None:
                await _close_async_resource(
                    self.claim_client,
                    deadline,
                    timeout_message,
                    self._close_operations,
                )
                self._owns_claim_client = False

            client_resources.append(close_claim_client)
        elif self.claim_client is self.client:
            self._owns_claim_client = False

        try:
            await close_resources_async(client_resources)
        except BaseException as exc:
            client_error = exc

        if not self._owns_client and not self._owns_claim_client:
            self._closed = True

        if worker_error is not None:
            raise_primary_with_cleanup(worker_error, worker_traceback, client_error)
        if client_error is not None:
            raise client_error
        deadline.check(timeout_message)

    def _close_task_finished(self, task: asyncio.Task[None]) -> None:
        if self._close_task is task and not self._closed:
            self._close_task = None

    @staticmethod
    def _contains_timeout(exc: BaseException) -> bool:
        pending = [exc]
        seen: set[int] = set()
        while pending:
            current = pending.pop()
            if id(current) in seen:
                continue
            seen.add(id(current))
            if isinstance(current, TimeoutError):
                return True
            if current.__cause__ is not None:
                pending.append(current.__cause__)
            if current.__context__ is not None:
                pending.append(current.__context__)
        return False

    def _build_worker(self, worker_index: int) -> AsyncQueueFlowWorker:
        return AsyncQueueFlowWorker(
            self.client,
            claim_client=self.claim_client,
            type=self.type,
            state=self.state,
            concurrency=self.concurrency,
            batch_size=self.batch_size,
            complete_independent=self.complete_independent,
            exception_policy=self.on_error,
            claim_values=self.claim_values,
            value_max_bytes=self.value_max_bytes,
            auto_partitions=True,
            worker_index=worker_index,
            workers=self.workers,
            server_shards=self.server_shards,
            claim_partition_batch_size=self.claim_partition_batch_size,
            idle_sleep_s=self.idle_sleep_s,
            max_idle_sleep_s=self.max_idle_sleep_s,
            protocol_wake_hints=self.protocol_wake_hints,
            fuse_complete_claim=self.fuse_complete_claim,
            block_ms=self.block_ms,
            close_client=False,
        )

    async def _run_producer(self, send: Callable[[AsyncFlowClient], Awaitable[Any]]) -> Any:
        url = self._url
        if not self.producer_loop_thread or url is None:
            return await send(self.client)

        def run_thread() -> Any:
            async def run() -> Any:
                client = AsyncFlowClient.from_url(url)
                try:
                    return await send(client)
                finally:
                    await client.close()

            return asyncio.run(run())

        return await asyncio.to_thread(run_thread)


class AsyncQueue:
    """High-level async queue handle bound to one Flow type/state."""

    def __init__(
        self,
        client: AsyncFlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | str | Any | None = None,
        type: str,
        state: str = "queued",
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> None:
        self.client = _client_from(client)
        self.claim_client = self.client if claim_client is None else _client_from(claim_client)
        self.type = type
        self.state = state
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()

    async def enqueue(self, id: str, *, payload: Any = None, **attrs: Any) -> Any:
        return await self.client.enqueue(
            id,
            type=self.type,
            state=attrs.pop("state", self.state),
            payload=payload,
            **attrs,
        )

    async def enqueue_many(
        self, items: Sequence[CreateItem | tuple[str, Any] | str], **attrs: Any
    ) -> Any:
        create_items = [
            item
            if isinstance(item, CreateItem)
            else CreateItem(item[0], item[1])
            if isinstance(item, tuple)
            else CreateItem(item)
            for item in items
        ]
        return await self.client.enqueue_many(
            create_items,
            type=self.type,
            state=attrs.pop("state", self.state),
            **attrs,
        )

    def worker(self, **kwargs: Any) -> AsyncQueueFlow:
        worker_kwargs = (
            self.worker_config.to_kwargs(ASYNC_QUEUE_WORKER_CONFIG_KEYS)
            if self.worker_config is not None
            else {}
        )
        if self.value_config.value_max_bytes is not None and "value_max_bytes" not in worker_kwargs:
            worker_kwargs["value_max_bytes"] = self.value_config.value_max_bytes
        worker_kwargs.update(kwargs)
        worker_kwargs.setdefault("state", self.state)
        return AsyncQueueFlow(
            self.client,
            claim_client=self.claim_client,
            type=self.type,
            **worker_kwargs,
        )

    async def install_policy(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        indexed_state_meta: str | None = None,
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
        kwargs: dict[str, Any] = {"retry": resolved_retry_policy}
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        return await self.client.install_policy(self.type, **kwargs)


class AsyncQueueClient:
    """High-level async durable queue client."""

    def __init__(
        self,
        client: AsyncFlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | str | Any | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _owns_clients: bool = False,
    ) -> None:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        self._url = client if isinstance(client, str) else None
        self._base_url_kwargs: dict[str, Any] = {}
        self._claim_client_explicit = claim_client is not None
        self._owned_extra_claim_flows: list[AsyncFlowClient] = []
        self._claim_flows_by_size: dict[int, AsyncFlowClient] = {}
        self._claim_pool_size = claim_pool_size
        command_max_connections = (
            1
            if isinstance(client, str)
            and (worker_config is None or worker_config.command_connections is None)
            else command_pool_size
        )
        self.flow = (
            AsyncFlowClient.from_url(client, max_connections=command_max_connections)
            if isinstance(client, str)
            else _client_from(client)
        )
        if claim_client is None:
            self.claim_flow = (
                AsyncFlowClient.from_url(client, max_connections=claim_pool_size)
                if isinstance(client, str)
                else self.flow
            )
        else:
            self.claim_flow = (
                AsyncFlowClient.from_url(claim_client, max_connections=claim_pool_size)
                if isinstance(claim_client, str)
                else _client_from(claim_client)
            )
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._owns_flow = _owns_clients or isinstance(client, str)
        self._owns_claim_flow = self.claim_flow is not self.flow and (
            _owns_clients or isinstance(client, str) or isinstance(claim_client, str)
        )
        self._close_coordinator = AsyncCloseCoordinator()
        if self.claim_flow is not self.flow:
            self._claim_flows_by_size[claim_pool_size] = self.claim_flow

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> AsyncQueueClient:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_kwargs = dict(kwargs)
        if worker_config is None or worker_config.command_connections is None:
            command_kwargs.setdefault("max_connections", 1)
        else:
            command_kwargs.setdefault("max_connections", command_pool_size)
        claim_kwargs = dict(kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        instance = cls(
            AsyncFlowClient.from_url(url, **command_kwargs),
            claim_client=AsyncFlowClient.from_url(url, **claim_kwargs),
            retry_policy=retry_policy,
            worker_config=worker_config,
            value_config=value_config,
            _owns_clients=True,
        )
        instance._url = url
        instance._base_url_kwargs = dict(kwargs)
        instance._claim_client_explicit = False
        instance._claim_pool_size = claim_pool_size
        instance._claim_flows_by_size = {claim_pool_size: instance.claim_flow}
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> AsyncFlowClient:
        if self._close_coordinator.started:
            raise RuntimeError("queue client is closed")
        if self._claim_client_explicit or self._url is None:
            return self.claim_flow
        _, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        existing = self._claim_flows_by_size.get(claim_pool_size)
        if existing is not None:
            return existing
        claim_kwargs = dict(self._base_url_kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        claim_flow = AsyncFlowClient.from_url(self._url, **claim_kwargs)
        self._claim_flows_by_size[claim_pool_size] = claim_flow
        self._owned_extra_claim_flows.append(claim_flow)
        return claim_flow

    def queue(
        self,
        *,
        type: str,
        state: str = "queued",
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> AsyncQueue:
        resolved_worker_config = worker_config if worker_config is not None else self.worker_config
        return AsyncQueue(
            self.flow,
            claim_client=self._claim_flow_for_worker_config(resolved_worker_config),
            type=type,
            state=state,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            worker_config=resolved_worker_config,
            value_config=value_config if value_config is not None else self.value_config,
        )

    async def install_policy(
        self,
        type: str,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        states: dict[str, FlowStatePolicyLike] | None = None,
        indexed_state_meta: str | None = None,
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
        kwargs: dict[str, Any] = {"retry": resolved_retry_policy, "states": states}
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        return await self.flow.install_policy(type, **kwargs)

    async def close(self) -> None:
        await self._close_coordinator.run(self._close_owned_clients)

    async def _close_owned_clients(self) -> None:
        extra_claim_flows = tuple(self._owned_extra_claim_flows)
        self._claim_flows_by_size.clear()
        resources: list[Callable[[], Awaitable[None]]] = []
        for extra_claim_flow in extra_claim_flows:

            async def close_extra_claim_flow(
                flow: AsyncFlowClient = extra_claim_flow,
            ) -> None:
                await flow.close()
                self._owned_extra_claim_flows[:] = [
                    candidate
                    for candidate in self._owned_extra_claim_flows
                    if candidate is not flow
                ]

            resources.append(close_extra_claim_flow)
        if self._owns_claim_flow and self.claim_flow is not self.flow:

            async def close_claim_flow() -> None:
                await self.claim_flow.close()
                self._owns_claim_flow = False

            resources.append(close_claim_flow)
        if self._owns_flow:

            async def close_flow() -> None:
                await self.flow.close()
                self._owns_flow = False

            resources.append(close_flow)
        await close_resources_async(resources)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)


@dataclass(frozen=True)
class AsyncWorkflowWorkerResult:
    claimed: int = 0
    applied: int = 0
    claim_calls: int = 0
    empty_claims: int = 0


class AsyncWorkflowFlowCommands:
    """Async Flow command helper bound to the current workflow job."""

    __slots__ = ("_ctx",)

    def __init__(self, ctx: AsyncWorkflowContext) -> None:
        self._ctx = ctx

    @property
    def client(self) -> AsyncFlowClient:
        return self._ctx.client

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def _partition(self, partition_key: str | None | object) -> str | None:
        if partition_key is _CURRENT_PARTITION:
            return self._ctx.partition_key
        return cast(str | None, partition_key)

    def _type(self, type: str | None) -> str:
        return self._ctx.workflow.type if type is None else type

    def _state(self, type: str | None, state: str | None) -> str:
        if state is not None:
            return state
        if type is None or type == self._ctx.workflow.type:
            return self._ctx.workflow.initial_state
        return "queued"

    async def get(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> Any:
        return await self.client.get(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def history(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> Any:
        return await self.client.history(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def create(
        self,
        id: str,
        *,
        type: str | None = None,
        state: str | None = None,
        payload: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.create(
            id,
            type=self._type(type),
            state=self._state(type, state),
            payload=payload,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def enqueue(
        self,
        id: str,
        *,
        type: str | None = None,
        state: str | None = None,
        payload: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.enqueue(
            id,
            type=self._type(type),
            state=self._state(type, state),
            payload=payload,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def start_and_claim(
        self,
        id: str,
        *,
        type: str | None = None,
        initial_state: str | None = None,
        worker: str,
        payload: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> FlowRecord:
        return await self.client.start_and_claim(
            id,
            type=self._type(type),
            initial_state=self._state(type, None) if initial_state is None else initial_state,
            worker=worker,
            payload=payload,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def create_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str | None = None,
        state: str | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> builtins.list[FlowRecord] | Any:
        return await self.client.create_many(
            self._partition(partition_key),
            items,
            type=self._type(type),
            state=self._state(type, state),
            **kwargs,
        )

    async def enqueue_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str | None = None,
        state: str | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> builtins.list[Any] | Any:
        return await self.client.enqueue_many(
            items,
            type=self._type(type),
            state=self._state(type, state),
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def run_steps_many(
        self,
        items: builtins.list[str | dict[str, Any] | CreateItem],
        *,
        type: str | None = None,
        states: Sequence[str] | None = None,
        steps: int | None = None,
        worker: str,
        payload: Any = None,
        result: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> bytes:
        return await self.client.run_steps_many(
            items,
            type=self._type(type),
            states=states,
            steps=steps,
            worker=worker,
            payload=payload,
            result=result,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def claim_due(
        self, type: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        return await self.client.claim_due(self._type(type), **kwargs)

    async def claim_flows(
        self, type: str | None = None, **kwargs: Any
    ) -> builtins.list[ClaimedFlow]:
        return await self.client.claim_flows(self._type(type), **kwargs)

    async def claim_jobs(
        self, type: str | None = None, **kwargs: Any
    ) -> builtins.list[ClaimedFlow]:
        """Compatibility alias for claim_flows()."""
        return await self.claim_flows(type, **kwargs)

    async def signal(
        self,
        id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        return await self.client.signal(self._ctx.id if id is None else id, **kwargs)

    async def flow_signal(self, id: str | None = None, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

    async def reclaim(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return cast(
            builtins.list[FlowRecord],
            await self.client.reclaim(self._type(type), **kwargs),
        )

    async def extend_lease(
        self,
        id: str | None = None,
        lease_token: bytes | None = None,
        *,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> FlowRecord:
        return await self.client.extend_lease(
            self._ctx.id if id is None else id,
            self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def transition(
        self,
        to_state: str,
        *,
        id: str | None = None,
        from_state: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.transition(
            self._ctx.id if id is None else id,
            from_state=self._ctx.state if from_state is None else from_state,
            to_state=to_state,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def step_continue(
        self,
        to_state: str,
        *,
        id: str | None = None,
        from_state: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> FlowRecord | ClaimedFlow:
        return await self.client.step_continue(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            from_state=self._ctx.state if from_state is None else from_state,
            to_state=to_state,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def complete(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.complete(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def retry(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.retry(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def fail(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.fail(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def cancel(
        self,
        id: str | None = None,
        *,
        fencing_token: int | None = None,
        lease_token: bytes | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.cancel(
            self._ctx.id if id is None else id,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def rewind(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.rewind(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def list(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.list(self._type(type), **kwargs)

    async def terminals(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.terminals(self._type(type), **kwargs)

    async def failures(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.failures(self._type(type), **kwargs)

    async def by_parent(
        self, parent_flow_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        target = self._ctx.id if parent_flow_id is None else parent_flow_id
        return await self.client.by_parent(target, **kwargs)

    async def by_root(
        self, root_flow_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        root = root_flow_id
        if root is None:
            root = getattr(self._ctx, "root_flow_id", None) or self._ctx.id
        return await self.client.by_root(root, **kwargs)

    async def by_correlation(
        self, correlation_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        correlation = correlation_id
        if correlation is None:
            correlation = getattr(self._ctx, "correlation_id", None)
        if correlation is None:
            raise ValueError("correlation_id is required when current flow has no correlation_id")
        return await self.client.by_correlation(correlation, **kwargs)

    async def info(self, type: str | None = None, **kwargs: Any) -> dict[Any, Any]:
        return await self.client.info(self._type(type), **kwargs)

    async def stuck(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.stuck(self._type(type), **kwargs)

    async def value_put(self, value: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        kwargs.setdefault("owner_flow_id", self._ctx.id)
        return await self.client.value_put(value, **kwargs)

    async def put_value(self, name: str, value: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("name", name)
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        kwargs.setdefault("owner_flow_id", self._ctx.id)
        return await self.client.value_put(value, **kwargs)

    async def value_mget(
        self, refs: builtins.list[str], *, max_bytes: int | None = None
    ) -> builtins.list[Any]:
        return await self.client.value_mget(refs, max_bytes=max_bytes)

    async def value(self, name: str, default: Any = None, *, local_cache: bool = False) -> Any:
        return await self._ctx.value(name, default, local_cache=local_cache)

    async def values(
        self, names: builtins.list[str], *, local_cache: bool = False
    ) -> dict[str, Any]:
        return await self._ctx.value_many(names, local_cache=local_cache)

    async def spawn_children(
        self,
        children: builtins.list[ChildSpec],
        *,
        parent_id: str | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        **kwargs: Any,
    ) -> Any:
        return await self.client.spawn_children(
            self._ctx.id if parent_id is None else parent_id,
            children,
            partition_key=self._partition(partition_key),
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            **kwargs,
        )

    async def install_policy(self, type: str | None = None, **kwargs: Any) -> Any:
        return await self.client.install_policy(self._type(type), **kwargs)

    async def policy_get(self, type: str | None = None, **kwargs: Any) -> dict[Any, Any]:
        return await self.client.policy_get(self._type(type), **kwargs)


class AsyncWorkflowBudget:
    """Async budget reservation helper for workflow handlers."""

    def __init__(
        self,
        ctx: AsyncWorkflowContext,
        *,
        scope: str,
        amount: int,
        limit: int | None = None,
        window_ms: int | None = None,
        usage_key: str = "amount",
        attribute_prefix: str = "governance_budget",
    ) -> None:
        self.ctx = ctx
        self.scope = scope
        self.amount = amount
        self.limit = limit
        self.window_ms = window_ms
        self.usage_key = usage_key
        self.attribute_prefix = attribute_prefix
        self.reservation: BudgetResult | None = None
        self._closed = False
        self._result: BudgetResult | None = None
        self._settlement_task: asyncio.Task[BudgetResult] | None = None
        self._settlement_kind: str | None = None

    @property
    def reservation_id(self) -> str:
        if self.reservation is None or self.reservation.reservation_id is None:
            raise RuntimeError("budget reservation has not been opened")
        return self.reservation.reservation_id

    @property
    def is_open(self) -> bool:
        return (
            not self._closed
            and self.reservation is not None
            and self.reservation.reservation_id is not None
        )

    async def __aenter__(self) -> AsyncWorkflowBudget:
        self.reservation = await self.ctx.client.budget_reserve(
            self.scope,
            self.amount,
            limit=self.limit,
            window_ms=self.window_ms,
        )
        _ = self.reservation_id
        self.ctx._record_budget_result(self.attribute_prefix, self.reservation)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            try:
                await self.commit(self.amount)
            except BaseException as primary:
                cleanup_error: BaseException | None = None
                if self.is_open:
                    try:
                        await self.release()
                    except BaseException as cleanup:
                        cleanup_error = cleanup
                raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        else:
            try:
                await self.release()
            except BaseException as cleanup:
                if exc is not None:
                    raise_primary_with_cleanup(exc, tb, cleanup)
                raise

    async def commit(
        self, actual_amount: int | None = None, *, usage: dict[str, Any] | None = None
    ) -> BudgetResult:
        if self._closed:
            if self._result is None:
                raise RuntimeError("budget reservation is already closed")
            return self._result
        actual = self.amount if actual_amount is None else actual_amount

        async def commit_and_record() -> BudgetResult:
            result = await self.ctx.client.budget_commit(
                self.scope,
                self.reservation_id,
                actual,
                usage=usage if usage is not None else {self.usage_key: actual},
            )
            self._closed = True
            self._result = result
            self.ctx._record_budget_result(self.attribute_prefix, result)
            return result

        task = self._settlement_task
        if task is None:
            task = asyncio.create_task(commit_and_record())
            self._settlement_task = task
            self._settlement_kind = "commit"
        return cast(BudgetResult, await await_cancellation_safe(task))

    async def release(self) -> BudgetResult:
        if self._closed:
            if self._result is None:
                raise RuntimeError("budget reservation is already closed")
            return self._result

        async def release_and_record() -> BudgetResult:
            result = await self.ctx.client.budget_release(self.scope, self.reservation_id)
            self._closed = True
            self._result = result
            self.ctx._record_budget_result(self.attribute_prefix, result)
            return result

        while True:
            task = self._settlement_task
            kind = self._settlement_kind
            if task is None:
                task = asyncio.create_task(release_and_record())
                self._settlement_task = task
                self._settlement_kind = "release"
                kind = "release"
            try:
                return cast(BudgetResult, await await_cancellation_safe(task))
            except asyncio.CancelledError:
                if kind == "commit" and task.cancelled():
                    if self._settlement_task is task:
                        release_task = asyncio.create_task(release_and_record())
                        self._settlement_task = release_task
                        self._settlement_kind = "release"
                    continue
                if kind == "release" and task.cancelled() and self._settlement_task is task:
                    self._settlement_task = None
                    self._settlement_kind = None
                raise
            except BaseException:
                if kind != "commit":
                    if self._settlement_task is task:
                        self._settlement_task = None
                        self._settlement_kind = None
                    raise
                if self._settlement_task is task:
                    release_task = asyncio.create_task(release_and_record())
                    self._settlement_task = release_task
                    self._settlement_kind = "release"
                # Another waiter may already have installed the release task.
                continue


class AsyncWorkflowEffect:
    """Async external-effect helper bound to an async workflow job."""

    def __init__(
        self,
        ctx: AsyncWorkflowContext,
        effect_key: str,
        effect_type: str,
        *,
        operation_digest: str | None = None,
        idempotency_key: str | None = None,
        governance_scope: str | None = None,
        external_id: str | Callable[[Any], str | None] | None = None,
    ) -> None:
        self.ctx = ctx
        self.effect_key = effect_key
        self.effect_type = effect_type
        self.operation_digest = operation_digest or idempotency_key or f"{effect_type}:{effect_key}"
        self.idempotency_key = idempotency_key
        self.governance_scope = governance_scope
        self.external_id = external_id
        self.reservation: EffectResult | None = None
        self._result: EffectResult | None = None
        self._started_at: float | None = None
        self._closed = False
        self._reservation_task: asyncio.Task[EffectResult] | None = None
        self._settlement_task: asyncio.Task[EffectResult] | None = None
        self._settlement_kind: str | None = None

    async def reserve(self) -> EffectResult:
        if self.reservation is not None:
            return self.reservation

        async def reserve_and_record() -> EffectResult:
            reservation = await self.ctx.client.effect_reserve(
                self.ctx.id,
                self.effect_key,
                self.effect_type,
                partition_key=self.ctx.partition_key,
                lease_token=self.ctx.lease_token,
                fencing_token=self.ctx.fencing_token,
                operation_digest=self.operation_digest,
                idempotency_key=self.idempotency_key,
                governance_scope=self.governance_scope,
            )
            self.reservation = reservation
            self._started_at = asyncio.get_running_loop().time()
            return reservation

        task = self._reservation_task
        if task is None:
            task = asyncio.create_task(reserve_and_record())
            self._reservation_task = task
        try:
            return cast(EffectResult, await await_cancellation_safe(task))
        except asyncio.CancelledError:
            if task.cancelled() and self._reservation_task is task:
                self._reservation_task = None
            raise
        except BaseException:
            if self._reservation_task is task:
                self._reservation_task = None
            raise

    async def confirm(
        self,
        *,
        external_id: str | None = None,
        latency_ms: int | None = None,
    ) -> EffectResult:
        if self._closed and self._result is not None:
            return self._result
        await self.reserve()

        async def confirm_and_record() -> EffectResult:
            self._result = await self.ctx.client.effect_confirm(
                self.ctx.id,
                self.effect_key,
                partition_key=self.ctx.partition_key,
                lease_token=self.ctx.lease_token,
                fencing_token=self.ctx.fencing_token,
                external_id=external_id,
                latency_ms=self._latency_ms(latency_ms),
            )
            self._closed = True
            return self._result

        task = self._settlement_task
        if task is None:
            task = asyncio.create_task(confirm_and_record())
            self._settlement_task = task
            self._settlement_kind = "confirm"
        try:
            return cast(EffectResult, await await_cancellation_safe(task))
        except asyncio.CancelledError:
            if task.cancelled() and self._settlement_task is task:
                self._settlement_task = None
                self._settlement_kind = None
            raise
        except BaseException:
            if self._settlement_task is task:
                self._settlement_task = None
                self._settlement_kind = None
            raise

    async def fail(
        self,
        *,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
    ) -> EffectResult:
        if self._closed and self._result is not None:
            return self._result
        await self.reserve()

        async def fail_and_record() -> EffectResult:
            self._result = await self.ctx.client.effect_fail(
                self.ctx.id,
                self.effect_key,
                partition_key=self.ctx.partition_key,
                lease_token=self.ctx.lease_token,
                fencing_token=self.ctx.fencing_token,
                error=error,
                reason=reason,
                latency_ms=self._latency_ms(latency_ms),
            )
            self._closed = True
            return self._result

        while True:
            task = self._settlement_task
            kind = self._settlement_kind
            if task is None:
                task = asyncio.create_task(fail_and_record())
                self._settlement_task = task
                self._settlement_kind = "fail"
                kind = "fail"
            try:
                return cast(EffectResult, await await_cancellation_safe(task))
            except asyncio.CancelledError:
                if not task.cancelled():
                    raise
                if self._settlement_task is task:
                    self._settlement_task = None
                    self._settlement_kind = None
                if kind == "confirm":
                    continue
                raise
            except BaseException:
                if self._settlement_task is task:
                    self._settlement_task = None
                    self._settlement_kind = None
                if kind == "confirm":
                    continue
                raise

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        await self.reserve()
        try:
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except BaseException as exc:
            try:
                await self.fail(error=str(exc), reason=exc.__class__.__name__)
            except BaseException as cleanup:
                raise_primary_with_cleanup(exc, exc.__traceback__, cleanup)
            raise
        await self.confirm(external_id=self._resolve_external_id(result))
        return result

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.call(func, *args, **kwargs)

        return wrapper

    async def __aenter__(self) -> AsyncWorkflowEffect:
        await self.reserve()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            await self.confirm()
        else:
            try:
                await self.fail(
                    error=str(exc) if exc is not None else None,
                    reason=exc_type.__name__ if exc_type is not None else None,
                )
            except BaseException as cleanup:
                if exc is not None:
                    raise_primary_with_cleanup(exc, tb, cleanup)
                raise

    def _resolve_external_id(self, result: Any) -> str | None:
        if callable(self.external_id):
            return self.external_id(result)
        if isinstance(self.external_id, str):
            return self.external_id
        if isinstance(result, str):
            return result
        if isinstance(result, bytes):
            return result.decode()
        return None

    def _latency_ms(self, explicit: int | None = None) -> int | None:
        if explicit is not None:
            return explicit
        if self._started_at is None:
            return None
        return max(int((asyncio.get_running_loop().time() - self._started_at) * 1000), 0)


class AsyncWorkflowContext:
    """Async workflow handler context with value-ref helpers."""

    def __init__(self, workflow: AsyncWorkflow, job: AsyncFlowJob, state_name: str) -> None:
        self.workflow = workflow
        self.client = workflow.client
        self.job = job
        self.state_name = state_name
        self.flow = AsyncWorkflowFlowCommands(self)
        self._value_cache: dict[str, Any] = {}
        self._governance_attributes: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self.job, name)

    @property
    def id(self) -> str:
        return self.job.id

    @property
    def type(self) -> str:
        return getattr(self.job, "type", self.workflow.type)

    @property
    def state(self) -> str:
        return getattr(self.job, "state", "running")

    @property
    def run_state(self) -> str | None:
        return getattr(self.job, "run_state", self.state_name)

    @property
    def partition_key(self) -> str | None:
        return self.job.partition_key

    @property
    def lease_token(self) -> bytes:
        return self.job.lease_token

    @property
    def fencing_token(self) -> int:
        return self.job.fencing_token

    @property
    def values(self) -> dict[str, Any]:
        return getattr(self.job, "values", None) or {}

    @property
    def value_refs(self) -> dict[str, Any]:
        return getattr(self.job, "value_refs", None) or {}

    async def value(
        self, name: str, default: Any = None, *, local_cache: bool | None = None
    ) -> Any:
        values = await self.value_many([name], local_cache=local_cache)
        return values.get(name, default)

    async def value_many(
        self, names: list[str], *, local_cache: bool | None = None
    ) -> dict[str, Any]:
        use_local_cache = (
            self.workflow.value_config.local_cache if local_cache is None else local_cache
        )
        values: dict[str, Any] = {}
        pending_names: list[str] = []
        pending_refs: list[str] = []

        for name in names:
            if use_local_cache and name in self._value_cache:
                values[name] = self._value_cache[name]
                continue

            if name in self.values:
                value = self.values[name]
                values[name] = value
                if use_local_cache:
                    self._value_cache[name] = value
                continue

            meta = self.value_refs.get(name)
            ref = None
            if isinstance(meta, dict):
                ref = meta.get("ref") or meta.get(b"ref")
            elif isinstance(meta, str):
                ref = meta
            elif isinstance(meta, bytes):
                ref = meta.decode()

            if ref:
                pending_names.append(name)
                pending_refs.append(ref)

        if pending_refs:
            fetched = await self.client.value_mget(
                pending_refs, max_bytes=self.workflow.value_max_bytes
            )
            for name, value in zip(pending_names, fetched, strict=True):
                values[name] = value
                if use_local_cache:
                    self._value_cache[name] = value

        return values

    def budget(
        self,
        scope: str,
        amount: int,
        *,
        limit: int | None = None,
        window_ms: int | None = None,
        usage_key: str = "amount",
        attribute_prefix: str = "governance_budget",
    ) -> AsyncWorkflowBudget:
        return AsyncWorkflowBudget(
            self,
            scope=scope,
            amount=amount,
            limit=limit,
            window_ms=window_ms,
            usage_key=usage_key,
            attribute_prefix=attribute_prefix,
        )

    def effect(
        self,
        effect_key: str,
        effect_type: str,
        *,
        operation_digest: str | None = None,
        idempotency_key: str | None = None,
        governance_scope: str | None = None,
        external_id: str | Callable[[Any], str | None] | None = None,
    ) -> AsyncWorkflowEffect:
        return AsyncWorkflowEffect(
            self,
            effect_key,
            effect_type,
            operation_digest=operation_digest,
            idempotency_key=idempotency_key,
            governance_scope=governance_scope,
            external_id=external_id,
        )

    def _state_budget(self, policy: BudgetPolicy | None) -> AsyncWorkflowBudget | None:
        if policy is None:
            return None
        scope = policy.scope(self) if callable(policy.scope) else policy.scope
        return self.budget(
            scope,
            policy.amount,
            limit=policy.limit,
            window_ms=policy.window_ms,
            usage_key=policy.usage_key,
            attribute_prefix=policy.attribute_prefix,
        )

    def _record_budget_result(self, prefix: str, result: BudgetResult) -> None:
        attrs = {
            f"{prefix}_scope": result.scope,
            f"{prefix}_status": result.status,
            f"{prefix}_reservation_id": result.reservation_id,
            f"{prefix}_reserved_amount": result.reserved_amount,
            f"{prefix}_actual_amount": result.actual_amount,
            f"{prefix}_overage_amount": result.overage_amount,
            f"{prefix}_remaining": result.remaining,
            f"{prefix}_over_budget": result.over_budget,
        }
        self._governance_attributes.update(
            {key: value for key, value in attrs.items() if value is not None}
        )


class AsyncWorkflow:
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
    ) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if claim_partition_batch_size <= 0:
            raise ValueError("claim_partition_batch_size must be positive")
        if block_ms is not None and block_ms < 0:
            raise ValueError("block_ms must be non-negative")
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_on_error = normalize_exception_policy(
            exception_policy if exception_policy is not None else on_error,
            argument="exception_policy" if exception_policy is not None else "on_error",
        )
        state_names = list(states) if states is not None else [initial_state or "queued"]
        if not state_names:
            raise ValueError("states must be non-empty")
        initial_state = initial_state if initial_state is not None else state_names[0]
        if initial_state not in state_names:
            raise ValueError("initial_state must be included in states")
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=workers,
            concurrency=concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
        self._url = client if isinstance(client, str) else None
        command_max_connections = (
            1 if isinstance(client, str) and command_connections is None else command_pool_size
        )
        if isinstance(client, str):
            self._owns_client = True
            self.client = AsyncFlowClient.from_url(client, max_connections=command_max_connections)
        else:
            self._owns_client = False
            self.client = _client_from(client)
        if claim_client is None:
            if isinstance(client, str):
                self.claim_client = AsyncFlowClient.from_url(
                    client, max_connections=claim_pool_size
                )
                self._owns_claim_client = True
            else:
                self.claim_client = self.client
                self._owns_claim_client = False
        elif isinstance(claim_client, str):
            self.claim_client = AsyncFlowClient.from_url(
                claim_client, max_connections=claim_pool_size
            )
            self._owns_claim_client = True
        else:
            self.claim_client = _client_from(claim_client)
            self._owns_claim_client = False
        self.type = type
        self.states = state_names
        self.initial_state = initial_state
        self.partition_by = tuple(partition_by)
        self.workers = workers
        self.concurrency = concurrency
        self.batch_size = min(batch_size, FLOW_MANY_BATCH_LIMIT)
        self.claim_partition_batch_size = claim_partition_batch_size
        self.server_shards = server_shards
        self.idle_sleep_s = idle_sleep_s
        self.block_ms = block_ms
        self.producer_loop_thread = producer_loop_thread
        self.on_error = resolved_on_error
        self.retry_policy = retry_policy
        self.value_config = value_config or ValueConfig()
        self.priority = priority
        self.claim_values = list(claim_values) if claim_values is not None else None
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
        for state_name in set(self.state_modes) | set(self.retry_policies):
            state_policies[state_name] = FlowStatePolicy(
                mode=self.state_modes.get(state_name),
                retry=self.retry_policies.get(state_name),
            )
        kwargs: dict[str, Any] = {"retry": resolved_retry_policy, "states": state_policies}
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        return await self._invocations.run_while_open(
            lambda: self.client.install_policy(self.type, **kwargs),
            closed_message="workflow is closed",
        )

    async def enqueue(self, id: str, *, payload: Any = None, **attrs: Any) -> Any:
        self._ensure_open()
        partition_key = pop_workflow_partition_key(attrs, self.partition_by)
        state = attrs.pop("state", self.initial_state)
        return_record = attrs.pop("return_record", False)

        async def send(client: AsyncFlowClient) -> Any:
            return await client.enqueue(
                id,
                type=self.type,
                state=state,
                payload=payload,
                partition_key=partition_key,
                return_record=return_record,
                **attrs,
            )

        result = await self._invocations.run_while_open(
            lambda: self._run_producer(send),
            closed_message="workflow is closed",
        )
        return result

    async def start_flow(self, id: str, *, payload: Any = None, **attrs: Any) -> Any:
        return await self.enqueue(id, payload=payload, **attrs)

    async def signal(self, id: str, **kwargs: Any) -> Any:
        self._ensure_open()
        return await self._invocations.run_while_open(
            lambda: self.client.signal(id, **kwargs),
            closed_message="workflow is closed",
        )

    async def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

    async def enqueue_many(
        self,
        items: Sequence[CreateItem | tuple[str, Any] | str],
        **attrs: Any,
    ) -> Any:
        self._ensure_open()
        partition_key = pop_workflow_partition_key(attrs, self.partition_by)
        state = attrs.pop("state", self.initial_state)
        independent = attrs.pop("independent", True)
        create_items = [
            item
            if isinstance(item, CreateItem)
            else CreateItem(item[0], item[1])
            if isinstance(item, tuple)
            else CreateItem(item)
            for item in items
        ]

        async def send(client: AsyncFlowClient) -> Any:
            return await client.enqueue_many(
                create_items,
                type=self.type,
                state=state,
                partition_key=partition_key,
                independent=independent,
                **attrs,
            )

        result = await self._invocations.run_while_open(
            lambda: self._run_producer(send),
            closed_message="workflow is closed",
        )
        return result

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

        state_name = state or self._next_state(worker_index)
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
            close_task = asyncio.create_task(self._close_in_phases(deadline))
            self._close_task = close_task
            close_task.add_done_callback(self._close_task_finished)
        await await_cancellation_safe(close_task)

    async def _close_in_phases(self, deadline: CloseDeadline) -> None:
        timeout_message = "async workflow close timed out"
        self.stop()
        await deadline.wait_tasks(self._tasks, timeout_message)
        await self._invocations.wait_for_idle(deadline, timeout_message)
        worker_error: BaseException | None = None
        worker_traceback = None
        for task in self._tasks:
            task_error = task_terminal_error(task)
            if worker_error is None and task_error is not None:
                worker_error = task_error
                worker_traceback = task_error.__traceback__
        self._tasks.clear()
        self._task_phases.clear()

        cleanup_error: BaseException | None = None
        for should_close, resource in (
            (self._owns_client, self.client),
            (
                self._owns_claim_client and self.claim_client is not self.client,
                self.claim_client,
            ),
        ):
            if not should_close:
                continue
            try:
                await _close_async_resource(
                    resource,
                    deadline,
                    timeout_message,
                    self._close_operations,
                )
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
            else:
                if resource is self.client:
                    self._owns_client = False
                    if self.claim_client is self.client:
                        self._owns_claim_client = False
                if resource is self.claim_client:
                    self._owns_claim_client = False

        if not self._owns_client and not self._owns_claim_client:
            self._closed = True

        if worker_error is not None:
            raise_primary_with_cleanup(worker_error, worker_traceback, cleanup_error)
        if cleanup_error is not None:
            raise cleanup_error
        deadline.check(timeout_message)

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

    async def _handle_claimed_batch(self, state_name: str, jobs: Sequence[AsyncFlowJob]) -> int:
        handler = self.handlers.get(state_name)
        if handler is None:
            raise ValueError(f"no handler for workflow state: {state_name!r}")

        on_error = self.error_modes.get(state_name, self.on_error)

        async def run_one(job: AsyncFlowJob) -> Transition | Complete | Retry | Fail:
            ctx = AsyncWorkflowContext(self, job, state_name)
            budget = ctx._state_budget(self.budget_policies.get(state_name))
            try:
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
                self._job_mutation(job, outcome)
                for job, outcome in zip(jobs, outcomes, strict=True)
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
                        self._complete_mutation_options(cast(Complete, outcome)),
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

    @staticmethod
    def _complete_mutation_options(outcome: Complete) -> dict[str, Any]:
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

    def _job_mutation(
        self,
        job: AsyncFlowJob,
        outcome: Transition | Complete | Retry | Fail,
    ) -> JobMutation:
        if isinstance(outcome, Complete):
            return JobMutation(
                MutationKind.COMPLETE,
                job,
                self._complete_mutation_options(outcome),
            )
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
            if (
                self.state_modes.get(outcome.to_state) == FlowStateMode.FIFO.value
                and outcome.priority is not None
            ):
                raise ValueError("priority is not supported for fifo state")
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

    def _next_state(self, worker_index: int) -> str:
        state_name = self.states[self._state_cursors[worker_index] % len(self.states)]
        self._state_cursors[worker_index] += 1
        return state_name

    def _next_claim_partition(self, worker_index: int) -> tuple[str | None, list[str] | None]:
        keys = _owned_auto_partition_keys(
            worker_index=worker_index,
            workers=self.workers,
            server_shards=self.server_shards,
        )
        if not keys:
            return None, None
        count = min(self.claim_partition_batch_size, len(keys))
        cursor = self._partition_cursors[worker_index]
        selected = [keys[(cursor + offset) % len(keys)] for offset in range(count)]
        self._partition_cursors[worker_index] = (cursor + count) % len(keys)
        if len(selected) == 1:
            return selected[0], None
        return None, selected

    async def _run_producer(self, send: Callable[[AsyncFlowClient], Awaitable[Any]]) -> Any:
        url = self._url
        if not self.producer_loop_thread or url is None:
            return await send(self.client)

        def run_thread() -> Any:
            async def run() -> Any:
                client = AsyncFlowClient.from_url(url)
                try:
                    return await send(client)
                finally:
                    await client.close()

            return asyncio.run(run())

        return await asyncio.to_thread(run_thread)

    @staticmethod
    def _uniform_partition_key(jobs: list[ClaimedFlow]) -> str | None:
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


class AsyncWorkflowClient:
    """High-level async durable workflow client."""

    def __init__(
        self,
        client: AsyncFlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | str | Any | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _owns_clients: bool = False,
    ) -> None:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        self._url = client if isinstance(client, str) else None
        self._base_url_kwargs: dict[str, Any] = {}
        self._claim_client_explicit = claim_client is not None
        self._owned_extra_claim_flows: list[AsyncFlowClient] = []
        self._claim_flows_by_size: dict[int, AsyncFlowClient] = {}
        self._claim_pool_size = claim_pool_size
        command_max_connections = (
            1
            if isinstance(client, str)
            and (worker_config is None or worker_config.command_connections is None)
            else command_pool_size
        )
        self.flow = (
            AsyncFlowClient.from_url(client, max_connections=command_max_connections)
            if isinstance(client, str)
            else _client_from(client)
        )
        if claim_client is None:
            self.claim_flow = (
                AsyncFlowClient.from_url(client, max_connections=claim_pool_size)
                if isinstance(client, str)
                else self.flow
            )
        else:
            self.claim_flow = (
                AsyncFlowClient.from_url(claim_client, max_connections=claim_pool_size)
                if isinstance(claim_client, str)
                else _client_from(claim_client)
            )
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._owns_flow = _owns_clients or isinstance(client, str)
        self._owns_claim_flow = self.claim_flow is not self.flow and (
            _owns_clients or isinstance(client, str) or isinstance(claim_client, str)
        )
        self._close_coordinator = AsyncCloseCoordinator()
        if self.claim_flow is not self.flow:
            self._claim_flows_by_size[claim_pool_size] = self.claim_flow

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> AsyncWorkflowClient:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_kwargs = dict(kwargs)
        if worker_config is None or worker_config.command_connections is None:
            command_kwargs.setdefault("max_connections", 1)
        else:
            command_kwargs.setdefault("max_connections", command_pool_size)
        claim_kwargs = dict(kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        instance = cls(
            AsyncFlowClient.from_url(url, **command_kwargs),
            claim_client=AsyncFlowClient.from_url(url, **claim_kwargs),
            retry_policy=retry_policy,
            worker_config=worker_config,
            value_config=value_config,
            _owns_clients=True,
        )
        instance._url = url
        instance._base_url_kwargs = dict(kwargs)
        instance._claim_client_explicit = False
        instance._claim_pool_size = claim_pool_size
        instance._claim_flows_by_size = {claim_pool_size: instance.claim_flow}
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> AsyncFlowClient:
        if self._close_coordinator.started:
            raise RuntimeError("workflow client is closed")
        if self._claim_client_explicit or self._url is None:
            return self.claim_flow
        _, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        existing = self._claim_flows_by_size.get(claim_pool_size)
        if existing is not None:
            return existing
        claim_kwargs = dict(self._base_url_kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        claim_flow = AsyncFlowClient.from_url(self._url, **claim_kwargs)
        self._claim_flows_by_size[claim_pool_size] = claim_flow
        self._owned_extra_claim_flows.append(claim_flow)
        return claim_flow

    def workflow(
        self,
        *,
        type: str,
        states: Sequence[str] | None = None,
        initial_state: str = "queued",
        partition_by: Sequence[str] = (),
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> AsyncWorkflow:
        resolved_worker_config = worker_config if worker_config is not None else self.worker_config
        workflow_kwargs = (
            resolved_worker_config.to_kwargs(ASYNC_WORKFLOW_CONFIG_KEYS)
            if resolved_worker_config is not None
            else {}
        )
        workflow_kwargs.update(kwargs)
        return AsyncWorkflow(
            self.flow,
            claim_client=self._claim_flow_for_worker_config(resolved_worker_config),
            type=type,
            states=states,
            initial_state=initial_state,
            partition_by=partition_by,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            value_config=value_config if value_config is not None else self.value_config,
            **workflow_kwargs,
        )

    async def install_policy(
        self,
        type: str,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        states: dict[str, FlowStatePolicyLike] | None = None,
        indexed_state_meta: str | None = None,
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
        kwargs: dict[str, Any] = {"retry": resolved_retry_policy, "states": states}
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        return await self.flow.install_policy(type, **kwargs)

    async def close(self) -> None:
        await self._close_coordinator.run(self._close_owned_clients)

    async def _close_owned_clients(self) -> None:
        extra_claim_flows = tuple(self._owned_extra_claim_flows)
        self._claim_flows_by_size.clear()
        resources: list[Callable[[], Awaitable[None]]] = []
        for extra_claim_flow in extra_claim_flows:

            async def close_extra_claim_flow(
                flow: AsyncFlowClient = extra_claim_flow,
            ) -> None:
                await flow.close()
                self._owned_extra_claim_flows[:] = [
                    candidate
                    for candidate in self._owned_extra_claim_flows
                    if candidate is not flow
                ]

            resources.append(close_extra_claim_flow)
        if self._owns_claim_flow and self.claim_flow is not self.flow:

            async def close_claim_flow() -> None:
                await self.claim_flow.close()
                self._owns_claim_flow = False

            resources.append(close_claim_flow)
        if self._owns_flow:

            async def close_flow() -> None:
                await self.flow.close()
                self._owns_flow = False

            resources.append(close_flow)
        await close_resources_async(resources)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)
