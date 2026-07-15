from __future__ import annotations

import asyncio
import contextlib
import inspect
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_ownership import (
    resolve_async_client_pair,
    rollback_async_resources,
)
from ferricstore.async_partitioning import (
    _owned_auto_partition_keys,
    _validate_auto_partition_workers,
    _validate_server_shards,
)
from ferricstore.async_wake import AsyncFlowWakeCoordinator
from ferricstore.async_worker_completion import (
    AsyncFlowJob as AsyncFlowJob,
)
from ferricstore.async_worker_completion import (
    AsyncWorkerCompletionMixin,
    _AsyncHandledBatch,
)
from ferricstore.batch_core import BatchValueMatcher, run_async_fanout
from ferricstore.client_markers import SyncFlowClientMarker
from ferricstore.lifecycle_core import (
    AsyncCloseCoordinator,
    AsyncCloseTaskRegistry,
    await_cancellation_safe,
)
from ferricstore.types import (
    ClaimedFlow,
    ExceptionPolicy,
    resolve_worker_connection_counts,
)
from ferricstore.worker_core import (
    AsyncWorkerInvocationTracker,
    CloseDeadline,
    CloseTimeoutError,
    WorkerIdleScheduler,
    can_fuse_complete_claim,
    task_terminal_error,
)
from ferricstore.worker_models import QueueFlowWorkerResult
from ferricstore.worker_runtime_config import QueueWorkerRuntimeConfig

if TYPE_CHECKING:
    from ferricstore.client_core import FlowClient

AsyncFlowHandler = Callable[[AsyncFlowJob], Any | Awaitable[Any]]

AsyncFlowBatchHandler = Callable[[list[AsyncFlowJob]], Any | Awaitable[Any]]

AsyncErrorMode = ExceptionPolicy | str

AsyncWorkflowHandler = Callable[[Any], Any | Awaitable[Any]]

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


def _close_client_flag(owns_client: bool, requested: bool | None) -> bool:
    return owns_client if requested is None else requested


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


def _client_from(client: AsyncFlowClient | FlowClient | str | Any) -> AsyncFlowClient:
    if isinstance(client, str):
        return AsyncFlowClient.from_url(client)
    if isinstance(client, SyncFlowClientMarker):
        raise TypeError(
            "async Flow SDK requires AsyncFlowClient, an async executor, or URL; "
            "FlowClient is sync-only"
        )
    if hasattr(client, "execute_command") and not hasattr(client, "claim_flows"):
        return AsyncFlowClient(client)
    return client


class AsyncQueueFlowWorker(AsyncWorkerCompletionMixin):
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
        wake_coordinator: AsyncFlowWakeCoordinator | None = None,
    ) -> None:
        server_shards = _validate_server_shards(server_shards)
        runtime_config = QueueWorkerRuntimeConfig.build(
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            concurrency=concurrency,
            batch_size=batch_size,
            workers=workers,
            claim_partition_batch_size=claim_partition_batch_size,
            block_ms=block_ms,
            idle_sleep_s=idle_sleep_s,
            max_idle_sleep_s=max_idle_sleep_s,
            exception_policy=exception_policy,
            on_error=on_error,
            lease_ms=lease_ms,
            priority=priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            claim_values=claim_values,
            value_max_bytes=value_max_bytes,
            complete_independent=complete_independent,
            protocol_wake_hints=protocol_wake_hints,
            fuse_complete_claim=fuse_complete_claim,
            auto_partitions=auto_partitions,
            close_client=close_client,
        )
        if runtime_config.auto_partitions:
            _validate_auto_partition_workers(runtime_config.workers)
        if runtime_config.partition_keys is not None:
            resolved_partition_keys = runtime_config.partition_keys
        elif partition_key is None and runtime_config.auto_partitions:
            resolved_partition_keys = _owned_auto_partition_keys(
                worker_index=worker_index,
                workers=runtime_config.workers,
                server_shards=server_shards,
            )
            if not resolved_partition_keys:
                raise ValueError("worker owns no auto partitions")
        else:
            resolved_partition_keys = None

        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=runtime_config.workers,
            concurrency=runtime_config.concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
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
        self._close_client = _close_client_flag(clients.owns_command, runtime_config.close_client)
        self._close_claim_client = clients.owns_claim

        self.type = type
        self.worker = worker or f"{type}:async-worker:{uuid.uuid4().hex}"
        self.state = state
        self.states = runtime_config.states
        self.concurrency = runtime_config.concurrency
        self.batch_size = runtime_config.batch_size
        self.lease_ms = runtime_config.lease_ms
        self.priority = runtime_config.priority
        self.reclaim_expired = runtime_config.reclaim_expired
        self.reclaim_ratio = runtime_config.reclaim_ratio
        self.claim_values = runtime_config.claim_values
        self.value_max_bytes = runtime_config.value_max_bytes
        self.block_ms = runtime_config.block_ms
        self.idle_sleep_s = runtime_config.idle_sleep_s
        self.max_idle_sleep_s = runtime_config.max_idle_sleep_s
        self.protocol_wake_hints = runtime_config.protocol_wake_hints
        self.fuse_complete_claim = runtime_config.fuse_complete_claim
        self.on_error = runtime_config.on_error
        self.complete_independent = runtime_config.complete_independent
        self.partition_key = partition_key
        self.partition_keys = resolved_partition_keys
        self.claim_partition_batch_size = runtime_config.claim_partition_batch_size or 1
        try:
            self._initialize_runtime_state(wake_coordinator)
        except BaseException:
            rollback_async_resources(clients.owned_resources())
            raise

    def _initialize_runtime_state(
        self,
        wake_coordinator: AsyncFlowWakeCoordinator | None,
    ) -> None:
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
        self._protocol_wake_generation = 0
        self._wake_coordinator: AsyncFlowWakeCoordinator | None
        if self.protocol_wake_hints:
            self._wake_coordinator = wake_coordinator or AsyncFlowWakeCoordinator(
                self.claim_client,
                type=self.type,
                state=self.state,
                states=self.states,
                partition_key=self.partition_key,
                partition_keys=self.partition_keys,
                priority=self.priority,
                limit=self.batch_size,
                enabled=True,
            )
            self._owns_wake_coordinator = wake_coordinator is None
        else:
            self._wake_coordinator = None
            self._owns_wake_coordinator = False
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
        timeout_message = "async queue worker close timed out"
        existing_task = self._close_coordinator.current_task

        def close_operation() -> Awaitable[None]:
            return self._close_once(CloseDeadline.start(None))

        close_task = self._close_coordinator.task(close_operation)
        try:
            await deadline.wait_task(close_task, timeout_message)
        except CloseTimeoutError:
            raise
        except BaseException:
            if existing_task is not close_task:
                raise
            retry_task = self._close_coordinator.task(close_operation)
            if retry_task is close_task:
                raise
            await deadline.wait_task(retry_task, timeout_message)

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
            (self._owns_wake_coordinator, self._wake_coordinator),
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
                if resource is self._wake_coordinator:
                    self._owns_wake_coordinator = False
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
        coordinator = self._wake_coordinator
        if coordinator is None:
            return
        enabled = await coordinator.subscribe()
        self._protocol_wake_hints_subscribed = coordinator.subscribed
        self._protocol_wake_hints_enabled = enabled

    async def _wait_for_protocol_wake_hint(self, timeout_s: float) -> bool:
        if not self._protocol_wake_hints_enabled or timeout_s <= 0:
            return False
        coordinator = self._wake_coordinator
        if coordinator is None:
            return False
        woke, generation = await coordinator.wait(
            self._protocol_wake_generation,
            timeout_s,
        )
        self._protocol_wake_generation = generation
        return woke

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

    def _next_claim_partition(self) -> tuple[str | None, list[str] | None]:
        if self.partition_key is not None:
            return self.partition_key, None
        if self.partition_keys is None:
            return None, None
        if not self.partition_keys:
            raise RuntimeError("worker owns no auto partitions")

        count = min(self.claim_partition_batch_size, len(self.partition_keys))
        keys = [
            self.partition_keys[(self._partition_cursor + offset) % len(self.partition_keys)]
            for offset in range(count)
        ]
        self._partition_cursor = (self._partition_cursor + count) % len(self.partition_keys)
        if len(keys) == 1:
            return keys[0], None
        return None, keys


_ASYNC_QUEUE_API_EXPORTS = frozenset({"AsyncQueue", "AsyncQueueClient", "AsyncQueueFlow"})
_ASYNC_PARTITIONING_EXPORTS = frozenset(
    {
        "AUTO_PARTITION_BUCKETS",
        "AUTO_PARTITION_PREFIX",
        "SERVER_SLOT_COUNT",
        "_auto_partition_assignments",
        "_auto_partition_index_for_id",
        "_auto_partition_key",
        "_auto_partition_owner",
        "_auto_partition_owners",
        "_auto_partition_server_shard",
        "_server_shard_for_slot",
    }
)


def __getattr__(name: str) -> Any:
    if name in _ASYNC_QUEUE_API_EXPORTS:
        from ferricstore import async_queue_api

        return getattr(async_queue_api, name)
    if name in _ASYNC_PARTITIONING_EXPORTS:
        from ferricstore import async_partitioning

        return getattr(async_partitioning, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | _ASYNC_QUEUE_API_EXPORTS | _ASYNC_PARTITIONING_EXPORTS)
