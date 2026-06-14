from __future__ import annotations

import asyncio
import inspect
import uuid
import zlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.async_client import AsyncFlowClient
from ferricstore.client import FlowClient
from ferricstore.types import (
    ClaimedItem,
    CreateItem,
    ExceptionPolicy,
    FencedItem,
    FlowRecord,
    RetryPolicy,
    ValueConfig,
    WorkerConfig,
    normalize_exception_policy,
    resolve_worker_connection_counts,
)
from ferricstore.worker import QueueFlowWorkerResult
from ferricstore.workflow import (
    Complete,
    Fail,
    Retry,
    Transition,
    complete,
    fail,
    retry,
)

AsyncFlowJob = ClaimedItem | FlowRecord
AsyncFlowHandler = Callable[[AsyncFlowJob], Any | Awaitable[Any]]
AsyncFlowBatchHandler = Callable[[list[AsyncFlowJob]], Any | Awaitable[Any]]
AsyncErrorMode = ExceptionPolicy | str
AsyncWorkflowHandler = Callable[[Any], Any | Awaitable[Any]]

AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 256
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
        "producer_loop_thread",
        "exception_policy",
    }
)

_PROTOCOL_URL_SCHEMES = {"ferric", "ferrics"}


def _is_protocol_url(value: str) -> bool:
    return urlparse(value).scheme.lower() in _PROTOCOL_URL_SCHEMES


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
    return f"{AUTO_PARTITION_PREFIX}{index % AUTO_PARTITION_BUCKETS}"


def _auto_partition_index_for_id(id: str) -> int:
    return zlib.crc32(id.encode()) % AUTO_PARTITION_BUCKETS


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
    if hasattr(client, "execute_command") and not hasattr(client, "claim_jobs"):
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
        if isinstance(client, str):
            self.client = AsyncFlowClient.from_url(client, max_connections=command_pool_size)
            self._close_client = True if close_client is None else close_client
        else:
            self.client = _client_from(client)
            self._close_client = False if close_client is None else close_client
        if claim_client is None:
            if isinstance(client, str) and _is_protocol_url(client):
                self.claim_client = self.client
                self._close_claim_client = False
            elif isinstance(client, str):
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
        self._task: asyncio.Task[None] | None = None
        self._totals = QueueFlowWorkerResult()

    async def run(self, handler: AsyncFlowHandler) -> None:
        await self.run_forever(handler)

    async def run_forever(self, handler: AsyncFlowHandler) -> None:
        await self._run_loop(handler, batch_handler=False)

    async def run_batch_forever(self, handler: AsyncFlowBatchHandler) -> None:
        await self._run_loop(handler, batch_handler=True)

    def start(
        self,
        handler: AsyncFlowHandler | AsyncFlowBatchHandler,
        *,
        batch_handler: bool = False,
    ) -> asyncio.Task[None]:
        if self._task is not None and not self._task.done():
            raise RuntimeError("worker already started")
        self._task = asyncio.create_task(self._run_loop(handler, batch_handler=batch_handler))
        return self._task

    async def join(self) -> QueueFlowWorkerResult:
        if self._task is not None:
            await self._task
        return self.stats

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> QueueFlowWorkerResult:
        return self._totals

    async def _run_loop(
        self,
        handler: AsyncFlowHandler | AsyncFlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> None:
        self._running = True
        try:
            while self._running:
                result = (
                    await self.run_batch_once(cast(AsyncFlowBatchHandler, handler))
                    if batch_handler
                    else await self.run_once(cast(AsyncFlowHandler, handler))
                )
                self._totals = QueueFlowWorkerResult(
                    claimed=self._totals.claimed + result.claimed,
                    completed=self._totals.completed + result.completed,
                    retried=self._totals.retried + result.retried,
                    failed=self._totals.failed + result.failed,
                    claim_calls=self._totals.claim_calls + result.claim_calls,
                )
                if result.claimed == 0:
                    await asyncio.sleep(self.idle_sleep_s)
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        self.stop()
        if self._task is not None and not self._task.done():
            await self._task
        if self._close_client:
            close = getattr(self.client, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result
        if self._close_claim_client and self.claim_client is not self.client:
            close = getattr(self.claim_client, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result

    async def run_once(self, handler: AsyncFlowHandler) -> QueueFlowWorkerResult:
        partition_key, partition_keys = self._next_claim_partition()
        jobs = await self._claim_jobs(
            partition_key=partition_key, partition_keys=partition_keys, limit=self.batch_size
        )
        result = QueueFlowWorkerResult(claim_calls=1)
        if not jobs:
            return result

        handled = await self._run_handlers(jobs, handler)
        finished = await self._finish_batch(handled)
        return QueueFlowWorkerResult(
            claimed=len(jobs),
            completed=finished.completed,
            retried=finished.retried,
            failed=finished.failed,
            claim_calls=1,
        )

    async def run_batch_once(self, handler: AsyncFlowBatchHandler) -> QueueFlowWorkerResult:
        partition_key, partition_keys = self._next_claim_partition()
        jobs = await self._claim_jobs(
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=self.batch_size,
        )
        result = QueueFlowWorkerResult(claim_calls=1)
        if not jobs:
            return result

        handled = await self._run_batch_handler(jobs, handler)
        finished = await self._finish_batch(handled)
        return QueueFlowWorkerResult(
            claimed=len(jobs),
            completed=finished.completed,
            retried=finished.retried,
            failed=finished.failed,
            claim_calls=1,
        )

    async def _claim_jobs(
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
            await self.claim_client.claim_jobs(
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
        semaphore = asyncio.Semaphore(self.concurrency)

        async def run_one(job: AsyncFlowJob) -> tuple[AsyncFlowJob, bool, Any]:
            try:
                async with semaphore:
                    value = handler(job)
                    if inspect.isawaitable(value):
                        value = await value
                return job, True, value
            except Exception as exc:
                return job, False, exc

        results = await asyncio.gather(*(run_one(job) for job in jobs))
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
        mixed_results: list[tuple[AsyncFlowJob, Any]] | None = None

        for job, ok, value in results:
            if not ok:
                failures.append((job, value))
                continue

            if not first_result_set:
                first_result = value
                first_result_set = True
                success_jobs.append(job)
                continue

            if mixed_results is None and value != first_result:
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
            await self.client.complete_jobs(
                cast(list[ClaimedItem], handled.jobs),
                result=handled.first_result,
                independent=self.complete_independent,
            )
            return len(handled.jobs)

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

        if self.on_error == "fail":
            failed = 0
            for message, jobs in groups.items():
                await self.client.fail_many(
                    None,
                    cast(list[ClaimedItem], jobs),
                    error=message,
                    independent=self.complete_independent,
                )
                failed += len(jobs)
            return 0, failed

        retried_jobs: list[AsyncFlowJob] = []
        for message, jobs in groups.items():
            await self.client.retry_many(
                None,
                cast(list[ClaimedItem], jobs),
                error=message,
                independent=self.complete_independent,
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
        if isinstance(client, str):
            self._owns_client = True
            self.client = AsyncFlowClient.from_url(client, max_connections=command_pool_size)
        else:
            self._owns_client = False
            self.client = _client_from(client)
        if claim_client is None:
            if isinstance(client, str) and _is_protocol_url(client):
                self.claim_client = self.client
                self._owns_claim_client = False
            elif isinstance(client, str):
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
        self.block_ms = block_ms
        self.producer_loop_thread = producer_loop_thread
        self.on_error = resolved_on_error
        self._workers: list[AsyncQueueFlowWorker] = []
        self._tasks: list[asyncio.Task[None]] = []

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

        result = await self._run_producer(send)
        return result

    async def signal(self, id: str, **kwargs: Any) -> Any:
        return await self.client.signal(id, **kwargs)

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

        result = await self._run_producer(send)
        return result

    async def run_once(
        self, handler: AsyncFlowHandler, *, worker_index: int = 0
    ) -> QueueFlowWorkerResult:
        worker = self._build_worker(worker_index)
        return await worker.run_once(handler)

    async def run(self, handler: AsyncFlowHandler) -> None:
        self.start(handler)
        await self.join()

    def start(self, handler: AsyncFlowHandler) -> list[asyncio.Task[None]]:
        if self._tasks:
            raise RuntimeError("queue flow already started")
        self._workers = [self._build_worker(index) for index in range(self.workers)]
        self._tasks = [worker.start(handler) for worker in self._workers]
        return self._tasks

    async def join(self) -> list[QueueFlowWorkerResult]:
        tasks = [asyncio.create_task(worker.join()) for worker in self._workers]
        if not tasks:
            return []
        try:
            return await asyncio.gather(*tasks)
        except BaseException:
            self.stop()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    def stop(self) -> None:
        for worker in self._workers:
            worker.stop()

    async def close(self) -> None:
        self.stop()
        await asyncio.gather(*(worker.close() for worker in self._workers), return_exceptions=False)
        if self._owns_client:
            await self.client.close()
        if self._owns_claim_client and self.claim_client is not self.client:
            await self.claim_client.close()

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
        return await self.client.install_policy(self.type, retry=resolved_retry_policy)


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
        self._claim_pool_size = claim_pool_size
        self.flow = (
            AsyncFlowClient.from_url(client, max_connections=command_pool_size)
            if isinstance(client, str)
            else _client_from(client)
        )
        if claim_client is None:
            if isinstance(client, str) and not _is_protocol_url(client):
                self.claim_flow = AsyncFlowClient.from_url(client, max_connections=claim_pool_size)
            else:
                self.claim_flow = self.flow
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
        self._owns_claim_flow = (
            (_owns_clients and self.claim_flow is not self.flow)
            or isinstance(claim_client, str)
            or (claim_client is None and isinstance(client, str) and not _is_protocol_url(client))
        )

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
        command_kwargs.setdefault("max_connections", command_pool_size)
        claim_kwargs = dict(kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        if _is_protocol_url(url):
            instance = cls(
                AsyncFlowClient.from_url(url, **command_kwargs),
                retry_policy=retry_policy,
                worker_config=worker_config,
                value_config=value_config,
                _owns_clients=True,
            )
        else:
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
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> AsyncFlowClient:
        if (
            self._claim_client_explicit
            or self._url is None
            or worker_config is None
            or _is_protocol_url(self._url)
        ):
            return self.claim_flow
        _, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        if claim_pool_size == self._claim_pool_size:
            return self.claim_flow
        claim_kwargs = dict(self._base_url_kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        claim_flow = AsyncFlowClient.from_url(self._url, **claim_kwargs)
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
        states: dict[str, RetryPolicy] | None = None,
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
        return await self.flow.install_policy(type, retry=resolved_retry_policy, states=states)

    async def close(self) -> None:
        for claim_flow in self._owned_extra_claim_flows:
            await claim_flow.close()
        self._owned_extra_claim_flows.clear()
        if self._owns_claim_flow and self.claim_flow is not self.flow:
            await self.claim_flow.close()
        if self._owns_flow:
            await self.flow.close()

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

    def __init__(self, ctx: AsyncWorkflowContext) -> None:
        self._ctx = ctx

    def _partition(self, partition_key: str | None | object) -> str | None:
        if partition_key is _CURRENT_PARTITION:
            return self._ctx.partition_key
        return cast(str | None, partition_key)

    async def get(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> Any:
        return await self._ctx.client.get(
            id or self._ctx.id,
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
        return await self._ctx.client.history(
            id or self._ctx.id,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def signal(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> Any:
        return await self._ctx.client.signal(
            id or self._ctx.id,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def flow_signal(self, id: str | None = None, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

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
        return await self._ctx.client.transition(
            id or self._ctx.id,
            from_state=from_state or self._ctx.state,
            to_state=to_state,
            lease_token=lease_token or self._ctx.lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def complete(
        self,
        id: str | None = None,
        lease_token: bytes | None = None,
        *,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self._ctx.client.complete(
            id or self._ctx.id,
            lease_token=lease_token or self._ctx.lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )


class AsyncWorkflowContext:
    """Async workflow handler context with value-ref helpers."""

    def __init__(self, workflow: AsyncWorkflow, job: AsyncFlowJob, state_name: str) -> None:
        self.workflow = workflow
        self.client = workflow.client
        self.job = job
        self.state_name = state_name
        self.flow = AsyncWorkflowFlowCommands(self)
        self._value_cache: dict[str, Any] = {}

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
            for name, value in zip(pending_names, fetched, strict=False):
                values[name] = value
                if use_local_cache:
                    self._value_cache[name] = value

        return values


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
        if isinstance(client, str):
            self._owns_client = True
            self.client = AsyncFlowClient.from_url(client, max_connections=command_pool_size)
        else:
            self._owns_client = False
            self.client = _client_from(client)
        if claim_client is None:
            if isinstance(client, str) and _is_protocol_url(client):
                self.claim_client = self.client
                self._owns_claim_client = False
            elif isinstance(client, str):
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
        self.error_modes: dict[str, AsyncErrorMode] = {}
        self.retry_policies: dict[str, RetryPolicy] = {}
        self._partition_cursors = [0 for _ in range(workers)]
        self._state_cursors = [0 for _ in range(workers)]
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        self._totals = AsyncWorkflowWorkerResult()

    def state(
        self,
        state_name: str,
        *,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[[AsyncWorkflowHandler], AsyncWorkflowHandler]:
        return self.on(
            state_name,
            exception_policy=exception_policy,
            on_error=on_error,
            retry_policy=retry_policy,
        )

    def on(
        self,
        state_name: str,
        *,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
        retry_policy: RetryPolicy | None = None,
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

        def decorate(handler: AsyncWorkflowHandler) -> AsyncWorkflowHandler:
            self.handlers[state_name] = handler
            self.error_modes[state_name] = resolved_on_error
            if retry_policy is not None:
                self.retry_policies[state_name] = retry_policy
            else:
                self.retry_policies.pop(state_name, None)
            return handler

        return decorate

    async def install_policy(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
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
        return await self.client.install_policy(
            self.type,
            retry=resolved_retry_policy,
            states=self.retry_policies,
        )

    async def enqueue(self, id: str, *, payload: Any = None, **attrs: Any) -> Any:
        state = attrs.pop("state", self.initial_state)
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

        result = await self._run_producer(send)
        return result

    async def start_flow(self, id: str, *, payload: Any = None, **attrs: Any) -> Any:
        return await self.enqueue(id, payload=payload, **attrs)

    async def signal(self, id: str, **kwargs: Any) -> Any:
        return await self.client.signal(id, **kwargs)

    async def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

    async def enqueue_many(
        self,
        items: Sequence[CreateItem | tuple[str, Any] | str],
        **attrs: Any,
    ) -> Any:
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
                independent=independent,
                **attrs,
            )

        result = await self._run_producer(send)
        return result

    async def run_once(
        self,
        *,
        worker_index: int = 0,
        state: str | None = None,
    ) -> AsyncWorkflowWorkerResult:
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
                await self.claim_client.claim_jobs(self.type, **cast(Any, claim_kwargs)),
            )
        result = AsyncWorkflowWorkerResult(claim_calls=1)
        if not jobs:
            return self._merge(result, AsyncWorkflowWorkerResult(empty_claims=1))

        for job in jobs:
            object.__setattr__(job, "type", self.type)
            object.__setattr__(job, "state", "running")
            object.__setattr__(job, "run_state", state_name)

        applied = await self._handle_claimed_batch(state_name, jobs)
        return self._merge(
            result,
            AsyncWorkflowWorkerResult(claimed=len(jobs), applied=applied),
        )

    async def _run_once_any_state(self, worker_index: int) -> AsyncWorkflowWorkerResult:
        partition_key, partition_keys = self._next_claim_partition(worker_index)
        jobs = await self.claim_client.claim_jobs(
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

    def _group_jobs_by_run_state(self, jobs: list[ClaimedItem]) -> dict[str, list[ClaimedItem]]:
        grouped: dict[str, list[ClaimedItem]] = {}
        for job in jobs:
            state_name = job.run_state
            if state_name not in self.handlers:
                raise ValueError(f"no handler for workflow state: {state_name!r}")
            grouped.setdefault(state_name, []).append(job)
        return grouped

    async def run(self) -> None:
        self.start()
        await self.join()

    def start(
        self, id: str | None = None, *, payload: Any = None, **attrs: Any
    ) -> list[asyncio.Task[None]] | Awaitable[Any]:
        if id is not None:
            return self.start_flow(id, payload=payload, **attrs)
        if payload is not None or attrs:
            raise TypeError("workflow worker start takes no payload/attrs unless id is provided")
        if self._tasks:
            raise RuntimeError("workflow already started")
        self._running = True
        self._tasks = [asyncio.create_task(self._run_loop(index)) for index in range(self.workers)]
        return self._tasks

    async def join(self) -> AsyncWorkflowWorkerResult:
        if self._tasks:
            await asyncio.gather(*self._tasks)
        return self._totals

    def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        self.stop()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=False)
        if self._owns_client:
            await self.client.close()
        if self._owns_claim_client and self.claim_client is not self.client:
            await self.claim_client.close()

    async def _run_loop(self, worker_index: int) -> None:
        try:
            while self._running:
                result = await self.run_once(worker_index=worker_index)
                self._totals = self._merge(self._totals, result)
                if result.claimed == 0:
                    await asyncio.sleep(self.idle_sleep_s)
        finally:
            self._running = False

    async def _handle_claimed_batch(self, state_name: str, jobs: Sequence[AsyncFlowJob]) -> int:
        handler = self.handlers.get(state_name)
        if handler is None:
            raise ValueError(f"no handler for workflow state: {state_name!r}")

        semaphore = asyncio.Semaphore(self.concurrency)
        on_error = self.error_modes.get(state_name, self.on_error)

        async def run_one(job: AsyncFlowJob) -> Transition | Complete | Retry | Fail:
            try:
                async with semaphore:
                    value = handler(AsyncWorkflowContext(self, job, state_name))
                    if inspect.isawaitable(value):
                        value = await value
            except Exception as exc:
                if on_error == "raise":
                    raise
                value = fail(error=str(exc)) if on_error == "fail" else retry(error=str(exc))
            return self._normalize_outcome(value)

        outcomes = await asyncio.gather(*(run_one(job) for job in jobs))

        first = outcomes[0]
        if all(outcome == first for outcome in outcomes):
            await self._apply_uniform(state_name, cast(list[ClaimedItem], jobs), first)
            return len(jobs)

        for job, outcome in zip(jobs, outcomes, strict=False):
            await self._apply_uniform(state_name, [cast(ClaimedItem, job)], outcome)
        return len(jobs)

    def _normalize_outcome(self, value: Any) -> Transition | Complete | Retry | Fail:
        if isinstance(value, (Transition, Complete, Retry, Fail)):
            return value
        return complete(result=value)

    async def _apply_uniform(
        self,
        state_name: str,
        jobs: list[ClaimedItem],
        outcome: Transition | Complete | Retry | Fail,
    ) -> None:
        partition_key = self._uniform_partition_key(jobs)
        if isinstance(outcome, Transition):
            items = [
                FencedItem(
                    id=job.id,
                    lease_token=job.lease_token,
                    fencing_token=job.fencing_token,
                    partition_key=job.partition_key,
                )
                for job in jobs
            ]
            await self.client.transition_many(
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
                run_at_ms=outcome.run_at_ms,
                independent=True,
            )
            return
        if isinstance(outcome, Complete):
            await self.client.complete_many(
                partition_key,
                jobs,
                result=outcome.result,
                payload=outcome.payload,
                ttl_ms=outcome.ttl_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                independent=True,
            )
            return
        if isinstance(outcome, Retry):
            await self.client.retry_many(
                partition_key,
                jobs,
                error=outcome.error,
                payload=outcome.payload,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                run_at_ms=outcome.run_at_ms,
                independent=True,
            )
            return
        await self.client.fail_many(
            partition_key,
            jobs,
            error=outcome.error,
            payload=outcome.payload,
            ttl_ms=outcome.ttl_ms,
            values=outcome.values,
            value_refs=outcome.value_refs,
            drop_values=outcome.drop_values,
            override_values=outcome.override_values,
            independent=True,
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
    def _uniform_partition_key(jobs: list[ClaimedItem]) -> str | None:
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
        self._claim_pool_size = claim_pool_size
        self.flow = (
            AsyncFlowClient.from_url(client, max_connections=command_pool_size)
            if isinstance(client, str)
            else _client_from(client)
        )
        if claim_client is None:
            if isinstance(client, str) and not _is_protocol_url(client):
                self.claim_flow = AsyncFlowClient.from_url(client, max_connections=claim_pool_size)
            else:
                self.claim_flow = self.flow
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
        self._owns_claim_flow = (
            (_owns_clients and self.claim_flow is not self.flow)
            or isinstance(claim_client, str)
            or (claim_client is None and isinstance(client, str) and not _is_protocol_url(client))
        )

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
        command_kwargs.setdefault("max_connections", command_pool_size)
        claim_kwargs = dict(kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        if _is_protocol_url(url):
            instance = cls(
                AsyncFlowClient.from_url(url, **command_kwargs),
                retry_policy=retry_policy,
                worker_config=worker_config,
                value_config=value_config,
                _owns_clients=True,
            )
        else:
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
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> AsyncFlowClient:
        if (
            self._claim_client_explicit
            or self._url is None
            or worker_config is None
            or _is_protocol_url(self._url)
        ):
            return self.claim_flow
        _, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        if claim_pool_size == self._claim_pool_size:
            return self.claim_flow
        claim_kwargs = dict(self._base_url_kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        claim_flow = AsyncFlowClient.from_url(self._url, **claim_kwargs)
        self._owned_extra_claim_flows.append(claim_flow)
        return claim_flow

    def workflow(
        self,
        *,
        type: str,
        states: Sequence[str] | None = None,
        initial_state: str = "queued",
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
        states: dict[str, RetryPolicy] | None = None,
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
        return await self.flow.install_policy(type, retry=resolved_retry_policy, states=states)

    async def close(self) -> None:
        for claim_flow in self._owned_extra_claim_flows:
            await claim_flow.close()
        self._owned_extra_claim_flows.clear()
        if self._owns_claim_flow and self.claim_flow is not self.flow:
            await self.claim_flow.close()
        if self._owns_flow:
            await self.flow.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)
