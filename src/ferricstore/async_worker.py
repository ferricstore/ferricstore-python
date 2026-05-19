from __future__ import annotations

import asyncio
import inspect
import uuid
import zlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from ferricstore.async_client import AsyncFlowClient
from ferricstore.client import FlowClient
from ferricstore.types import ClaimedItem, CreateItem, FencedItem, FlowRecord
from ferricstore.worker import QueueFlowWorkerResult
from ferricstore.workflow import Complete, Fail, Retry, Transition, complete, fail, retry, transition


AsyncFlowJob = ClaimedItem | FlowRecord
AsyncFlowHandler = Callable[[AsyncFlowJob], Any | Awaitable[Any]]
AsyncFlowBatchHandler = Callable[[list[AsyncFlowJob]], Any | Awaitable[Any]]
AsyncErrorMode = Literal["retry", "fail", "raise"]
AsyncWorkflowHandler = Callable[[ClaimedItem], Any | Awaitable[Any]]

AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 256
SERVER_SLOT_COUNT = 1024
FLOW_MANY_BATCH_LIMIT = 1000


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
        if _auto_partition_server_shard(index, server_shards) % max(workers, 1) == worker_index
    ]


class AsyncPartitionWakeCoordinator:
    def __init__(self, workers: int, owner_fn=None) -> None:
        self.workers = max(workers, 1)
        self.owner_fn = owner_fn
        self.queues = [asyncio.Queue() for _ in range(self.workers)]
        self.pending = [set() for _ in range(self.workers)]
        self.credits = [dict() for _ in range(self.workers)]
        self.locks = [asyncio.Lock() for _ in range(self.workers)]
        self.notifications = 0
        self.notified_jobs = 0

    def owner_for(self, partition_index: int) -> int:
        if self.owner_fn is not None:
            return self.owner_fn(partition_index) % self.workers
        return partition_index % self.workers

    async def notify_partition(self, partition_index: int, count: int = 1) -> None:
        count = max(int(count), 0)
        if count == 0:
            return

        owner = self.owner_for(partition_index)
        should_queue = False
        async with self.locks[owner]:
            self.credits[owner][partition_index] = self.credits[owner].get(partition_index, 0) + count
            self.notified_jobs += count
            if partition_index not in self.pending[owner]:
                self.pending[owner].add(partition_index)
                self.notifications += 1
                should_queue = True

        if should_queue:
            self.queues[owner].put_nowait(partition_index)

    async def next_partitions(
        self,
        worker_index: int,
        *,
        timeout_s: float,
        max_partitions: int,
        max_credit: int,
        same_group=None,
    ) -> tuple[list[int], int]:
        if max_partitions <= 0 or max_credit <= 0:
            return [], 0

        if timeout_s <= 0:
            try:
                partition_index = self.queues[worker_index].get_nowait()
            except asyncio.QueueEmpty:
                return [], 0
        else:
            try:
                partition_index = await asyncio.wait_for(
                    self.queues[worker_index].get(),
                    timeout=timeout_s,
                )
            except asyncio.TimeoutError:
                return [], 0

        async with self.locks[worker_index]:
            self.pending[worker_index].discard(partition_index)
            credit = self.credits[worker_index].pop(partition_index, 0)

        partitions = [partition_index]
        total_credit = credit

        while len(partitions) < max_partitions and total_credit < max_credit:
            try:
                partition_index = self.queues[worker_index].get_nowait()
            except asyncio.QueueEmpty:
                break

            async with self.locks[worker_index]:
                self.pending[worker_index].discard(partition_index)
                credit = self.credits[worker_index].pop(partition_index, 0)

            if credit <= 0:
                continue
            if same_group is not None and not same_group(partitions[0], partition_index):
                await self.return_credit(worker_index, partition_index, credit)
                break
            partitions.append(partition_index)
            total_credit += credit

        return partitions, total_credit

    async def return_credit(self, worker_index: int, partition_index: int, credit: int) -> None:
        if credit <= 0:
            return
        should_queue = False
        async with self.locks[worker_index]:
            self.credits[worker_index][partition_index] = self.credits[worker_index].get(partition_index, 0) + credit
            if partition_index not in self.pending[worker_index]:
                self.pending[worker_index].add(partition_index)
                should_queue = True
        if should_queue:
            self.queues[worker_index].put_nowait(partition_index)

    async def total_credit(self) -> int:
        total = 0
        for worker_index in range(self.workers):
            async with self.locks[worker_index]:
                total += sum(self.credits[worker_index].values())
        return total


class AsyncStateWakeCoordinator:
    def __init__(self, states: Sequence[str], workers: int, owner_fn=None) -> None:
        self.by_state = {
            state: AsyncPartitionWakeCoordinator(workers, owner_fn=owner_fn)
            for state in states
        }

    @property
    def notifications(self) -> int:
        return sum(coordinator.notifications for coordinator in self.by_state.values())

    @property
    def notified_jobs(self) -> int:
        return sum(coordinator.notified_jobs for coordinator in self.by_state.values())

    async def notify(self, state: str, partition_index: int, count: int = 1) -> None:
        coordinator = self.by_state.get(state)
        if coordinator is not None:
            await coordinator.notify_partition(partition_index, count)

    async def next_ready(
        self,
        state: str,
        worker_index: int,
        *,
        timeout_s: float,
        max_partitions: int,
        max_credit: int,
        same_group=None,
    ) -> tuple[list[int], int]:
        coordinator = self.by_state.get(state)
        if coordinator is None:
            return [], 0
        return await coordinator.next_partitions(
            worker_index,
            timeout_s=timeout_s,
            max_partitions=max_partitions,
            max_credit=max_credit,
            same_group=same_group,
        )


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
        batch_size: int = 100,
        lease_ms: int = 30_000,
        priority: int | None = 0,
        reclaim_expired: bool | None = False,
        reclaim_ratio: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        idle_sleep_s: float = 0.1,
        on_error: AsyncErrorMode = "retry",
        complete_independent: bool = True,
        partition_key: str | None = None,
        partition_keys: Sequence[str] | None = None,
        auto_partitions: bool = True,
        worker_index: int = 0,
        workers: int = 1,
        server_shards: int = 16,
        claim_partition_batch_size: int = 16,
        wake_source: AsyncPartitionWakeCoordinator | None = None,
        wake_worker_index: int | None = None,
        wake_same_group: Callable[[int, int], bool] | None = None,
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
        if on_error not in {"retry", "fail", "raise"}:
            raise ValueError("on_error must be 'retry', 'fail', or 'raise'")

        self.client = _client_from(client)

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
        self.idle_sleep_s = max(idle_sleep_s, 0.0)
        self.on_error = on_error
        self.complete_independent = complete_independent
        self.partition_key = partition_key
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
        self.wake_source = wake_source
        self.wake_worker_index = wake_worker_index
        self.wake_same_group = wake_same_group
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
        while self._running:
            result = (
                await self.run_batch_once(handler)
                if batch_handler
                else await self.run_once(handler)
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

    def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        self.stop()
        if self._task is not None and not self._task.done():
            await self._task
        close = getattr(self.client, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result

    async def run_once(self, handler: AsyncFlowHandler) -> QueueFlowWorkerResult:
        partition_key, partition_keys = self._next_claim_partition()
        if self.wake_source is not None:
            partition_indices, credit = await self.wake_source.next_partitions(
                self.wake_worker_index or 0,
                timeout_s=self.idle_sleep_s,
                max_partitions=self.claim_partition_batch_size,
                max_credit=self.batch_size,
                same_group=self.wake_same_group,
            )
            if not partition_indices:
                return QueueFlowWorkerResult()
            partition_keys = [_auto_partition_key(index) for index in partition_indices]
            partition_key = partition_keys[0] if len(partition_keys) == 1 else None
            partition_keys = None if partition_key is not None else partition_keys
            limit = max(1, min(self.batch_size, credit))
        else:
            limit = self.batch_size
        jobs = await self._claim_jobs(partition_key=partition_key, partition_keys=partition_keys, limit=limit)
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
            return await self.client.claim_due(
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
                payload=False,
                values=self.claim_values,
                value_max_bytes=self.value_max_bytes,
            )

        return await self.client.claim_jobs(
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
                handled.jobs,
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

        jobs = [job for job, _exc in failures]
        message = str(failures[0][1])

        if self.on_error == "raise":
            raise failures[0][1]
        if self.on_error == "fail":
            await self.client.fail_many(
                None,
                jobs,
                error=message,
                independent=self.complete_independent,
            )
            return 0, len(jobs)

        await self.client.retry_many(
            None,
            jobs,
            error=message,
            independent=self.complete_independent,
        )
        return len(jobs), 0

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
        type: str,
        state: str = "queued",
        workers: int = 16,
        concurrency: int = 500,
        batch_size: int = 1000,
        claim_partition_batch_size: int = 16,
        complete_independent: bool = True,
        server_shards: int = 16,
        idle_sleep_s: float = 0.001,
        producer_loop_thread: bool = True,
        owner_wakeup: bool = True,
    ) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")
        self._url = client if isinstance(client, str) else None
        self.client = _client_from(client)
        self.type = type
        self.state = state
        self.workers = workers
        self.concurrency = concurrency
        self.batch_size = min(batch_size, FLOW_MANY_BATCH_LIMIT)
        self.claim_partition_batch_size = claim_partition_batch_size
        self.complete_independent = complete_independent
        self.server_shards = server_shards
        self.idle_sleep_s = idle_sleep_s
        self.producer_loop_thread = producer_loop_thread
        self.owner_wakeup = owner_wakeup
        self.wake_source = (
            AsyncPartitionWakeCoordinator(
                workers,
                owner_fn=lambda partition_index: _auto_partition_server_shard(
                    partition_index,
                    server_shards,
                ),
            )
            if owner_wakeup
            else None
        )
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
        await self._notify_created([id], partition_key=partition_key)
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
        await self._notify_created([item.id for item in create_items])
        return result

    async def run_once(self, handler: AsyncFlowHandler, *, worker_index: int = 0) -> QueueFlowWorkerResult:
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
        return [await worker.join() for worker in self._workers]

    def stop(self) -> None:
        for worker in self._workers:
            worker.stop()

    async def close(self) -> None:
        self.stop()
        await asyncio.gather(*(worker.close() for worker in self._workers), return_exceptions=False)

    def _build_worker(self, worker_index: int) -> AsyncQueueFlowWorker:
        return AsyncQueueFlowWorker(
            self.client,
            type=self.type,
            state=self.state,
            concurrency=self.concurrency,
            batch_size=self.batch_size,
            complete_independent=self.complete_independent,
            worker_index=worker_index,
            workers=self.workers,
            server_shards=self.server_shards,
            claim_partition_batch_size=self.claim_partition_batch_size,
            idle_sleep_s=self.idle_sleep_s,
            wake_source=self.wake_source,
            wake_worker_index=worker_index,
            wake_same_group=lambda first, candidate: (
                _auto_partition_server_shard(first, self.server_shards)
                == _auto_partition_server_shard(candidate, self.server_shards)
            ),
        )

    async def _run_producer(self, send: Callable[[AsyncFlowClient], Awaitable[Any]]) -> Any:
        if not self.producer_loop_thread or self._url is None:
            return await send(self.client)

        def run_thread() -> Any:
            async def run() -> Any:
                client = AsyncFlowClient.from_url(self._url)
                try:
                    return await send(client)
                finally:
                    await client.close()

            return asyncio.run(run())

        return await asyncio.to_thread(run_thread)

    async def _notify_created(
        self,
        ids: Sequence[str],
        *,
        partition_key: str | None = None,
    ) -> None:
        if self.wake_source is None:
            return
        counts: dict[int, int] = {}
        for id in ids:
            if partition_key is not None and partition_key.startswith(AUTO_PARTITION_PREFIX):
                try:
                    partition_index = int(partition_key[len(AUTO_PARTITION_PREFIX) :]) % AUTO_PARTITION_BUCKETS
                except ValueError:
                    partition_index = _auto_partition_index_for_id(id)
            else:
                partition_index = _auto_partition_index_for_id(id)
            counts[partition_index] = counts.get(partition_index, 0) + 1
        for partition_index, count in counts.items():
            await self.wake_source.notify_partition(partition_index, count)


@dataclass(frozen=True)
class AsyncWorkflowWorkerResult:
    claimed: int = 0
    applied: int = 0
    claim_calls: int = 0
    empty_claims: int = 0


class AsyncWorkflow:
    """Simple async state-machine workflow runtime.

    Handlers receive compact claimed jobs by default and return one of:
    Transition, Complete, Retry, Fail, or any plain value to complete with that value.
    """

    def __init__(
        self,
        client: AsyncFlowClient | FlowClient | str | Any,
        *,
        type: str,
        states: Sequence[str] | None = None,
        initial_state: str = "queued",
        workers: int = 16,
        concurrency: int = 500,
        batch_size: int = 1000,
        claim_partition_batch_size: int = 16,
        server_shards: int = 16,
        idle_sleep_s: float = 0.001,
        producer_loop_thread: bool = True,
        owner_wakeup: bool = True,
    ) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if claim_partition_batch_size <= 0:
            raise ValueError("claim_partition_batch_size must be positive")
        self._url = client if isinstance(client, str) else None
        self.client = _client_from(client)
        self.type = type
        self.states = list(states) if states is not None else [initial_state]
        if not self.states:
            raise ValueError("states must be non-empty")
        self.initial_state = initial_state
        self.workers = workers
        self.concurrency = concurrency
        self.batch_size = min(batch_size, FLOW_MANY_BATCH_LIMIT)
        self.claim_partition_batch_size = claim_partition_batch_size
        self.server_shards = server_shards
        self.idle_sleep_s = idle_sleep_s
        self.producer_loop_thread = producer_loop_thread
        self.owner_wakeup = owner_wakeup
        self.wake_source = (
            AsyncStateWakeCoordinator(
                self.states,
                workers,
                owner_fn=lambda partition_index: _auto_partition_server_shard(
                    partition_index,
                    server_shards,
                ),
            )
            if owner_wakeup
            else None
        )
        self.handlers: dict[str, AsyncWorkflowHandler] = {}
        self._partition_cursors = [0 for _ in range(workers)]
        self._state_cursors = [0 for _ in range(workers)]
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        self._totals = AsyncWorkflowWorkerResult()

    def on(self, state_name: str) -> Callable[[AsyncWorkflowHandler], AsyncWorkflowHandler]:
        if state_name not in self.states:
            raise ValueError(f"unknown workflow state: {state_name!r}")

        def decorate(handler: AsyncWorkflowHandler) -> AsyncWorkflowHandler:
            self.handlers[state_name] = handler
            return handler

        return decorate

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
        await self._notify_ids(state, [id], partition_key=partition_key)
        return result

    async def signal(self, id: str, **kwargs: Any) -> Any:
        result = await self.client.signal(id, **kwargs)
        transition_to = kwargs.get("transition_to")
        if transition_to is not None:
            await self._notify_ids(
                transition_to,
                [id],
                partition_key=kwargs.get("partition_key"),
            )
        return result

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
        await self._notify_ids(state, [item.id for item in create_items])
        return result

    async def run_once(
        self,
        *,
        worker_index: int = 0,
        state: str | None = None,
    ) -> AsyncWorkflowWorkerResult:
        worker_index = worker_index % self.workers
        state_name = state or self._next_state(worker_index)
        if self.wake_source is not None:
            partition_indices, credit = await self.wake_source.next_ready(
                state_name,
                worker_index,
                timeout_s=self.idle_sleep_s,
                max_partitions=self.claim_partition_batch_size,
                max_credit=self.batch_size,
                same_group=lambda first, candidate: (
                    _auto_partition_server_shard(first, self.server_shards)
                    == _auto_partition_server_shard(candidate, self.server_shards)
                ),
            )
            if not partition_indices:
                return AsyncWorkflowWorkerResult()
            partition_keys = [_auto_partition_key(index) for index in partition_indices]
            partition_key = partition_keys[0] if len(partition_keys) == 1 else None
            partition_keys = None if partition_key is not None else partition_keys
            limit = max(1, min(self.batch_size, credit))
        else:
            partition_key, partition_keys = self._next_claim_partition(worker_index)
            limit = self.batch_size
        jobs = await self.client.claim_jobs(
            self.type,
            state=state_name,
            worker=f"{self.type}:async-workflow:{worker_index}",
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=limit,
            priority=0,
            reclaim_expired=False,
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

    async def run(self) -> None:
        self.start()
        await self.join()

    def start(self) -> list[asyncio.Task[None]]:
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

    async def _run_loop(self, worker_index: int) -> None:
        while self._running:
            result = await self.run_once(worker_index=worker_index)
            self._totals = self._merge(self._totals, result)
            if result.claimed == 0:
                await asyncio.sleep(self.idle_sleep_s)

    async def _handle_claimed_batch(self, state_name: str, jobs: list[ClaimedItem]) -> int:
        handler = self.handlers.get(state_name)
        if handler is None:
            raise ValueError(f"no handler for workflow state: {state_name!r}")

        semaphore = asyncio.Semaphore(self.concurrency)

        async def run_one(job: ClaimedItem) -> Transition | Complete | Retry | Fail:
            try:
                async with semaphore:
                    value = handler(job)
                    if inspect.isawaitable(value):
                        value = await value
            except Exception as exc:
                value = retry(error=str(exc))
            return self._normalize_outcome(value)

        outcomes = await asyncio.gather(*(run_one(job) for job in jobs))

        first = outcomes[0]
        if all(outcome == first for outcome in outcomes):
            await self._apply_uniform(jobs, first)
            return len(jobs)

        for job, outcome in zip(jobs, outcomes):
            await self._apply_uniform([job], outcome)
        return len(jobs)

    def _normalize_outcome(self, value: Any) -> Transition | Complete | Retry | Fail:
        if isinstance(value, (Transition, Complete, Retry, Fail)):
            return value
        return complete(result=value)

    async def _apply_uniform(
        self,
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
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                run_at_ms=outcome.run_at_ms,
                independent=True,
            )
            await self._notify_jobs(outcome.to_state, jobs)
            return
        if isinstance(outcome, Complete):
            await self.client.complete_many(
                partition_key,
                jobs,
                result=outcome.result,
                payload=outcome.payload,
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
        count = min(self.claim_partition_batch_size, len(keys))
        cursor = self._partition_cursors[worker_index]
        selected = [keys[(cursor + offset) % len(keys)] for offset in range(count)]
        self._partition_cursors[worker_index] = (cursor + count) % len(keys)
        if len(selected) == 1:
            return selected[0], None
        return None, selected

    async def _run_producer(self, send: Callable[[AsyncFlowClient], Awaitable[Any]]) -> Any:
        if not self.producer_loop_thread or self._url is None:
            return await send(self.client)

        def run_thread() -> Any:
            async def run() -> Any:
                client = AsyncFlowClient.from_url(self._url)
                try:
                    return await send(client)
                finally:
                    await client.close()

            return asyncio.run(run())

        return await asyncio.to_thread(run_thread)

    async def _notify_ids(
        self,
        state: str,
        ids: Sequence[str],
        *,
        partition_key: str | None = None,
    ) -> None:
        if self.wake_source is None:
            return
        counts: dict[int, int] = {}
        for id in ids:
            if partition_key is not None and partition_key.startswith(AUTO_PARTITION_PREFIX):
                try:
                    partition_index = int(partition_key[len(AUTO_PARTITION_PREFIX) :]) % AUTO_PARTITION_BUCKETS
                except ValueError:
                    partition_index = _auto_partition_index_for_id(id)
            else:
                partition_index = _auto_partition_index_for_id(id)
            counts[partition_index] = counts.get(partition_index, 0) + 1
        for partition_index, count in counts.items():
            await self.wake_source.notify(state, partition_index, count)

    async def _notify_jobs(self, state: str, jobs: Sequence[ClaimedItem]) -> None:
        if self.wake_source is None:
            return
        counts: dict[int, int] = {}
        for job in jobs:
            if job.partition_key and job.partition_key.startswith(AUTO_PARTITION_PREFIX):
                try:
                    partition_index = int(job.partition_key[len(AUTO_PARTITION_PREFIX) :]) % AUTO_PARTITION_BUCKETS
                except ValueError:
                    partition_index = _auto_partition_index_for_id(job.id)
            else:
                partition_index = _auto_partition_index_for_id(job.id)
            counts[partition_index] = counts.get(partition_index, 0) + 1
        for partition_index, count in counts.items():
            await self.wake_source.notify(state, partition_index, count)

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
