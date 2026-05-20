from __future__ import annotations

import asyncio
import inspect
import time
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
AsyncWorkflowHandler = Callable[[Any], Any | Awaitable[Any]]

AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 256
SERVER_SLOT_COUNT = 1024
FLOW_MANY_BATCH_LIMIT = 1000
WakePartition = int | str
_CURRENT_PARTITION = object()


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


def _same_auto_server_shard(first: WakePartition, candidate: WakePartition, server_shards: int) -> bool:
    if isinstance(first, int) and isinstance(candidate, int):
        return _auto_partition_server_shard(first, server_shards) == _auto_partition_server_shard(
            candidate,
            server_shards,
        )
    return first == candidate


def _partition_token_from_key(id: str, partition_key: str | None = None) -> WakePartition:
    if partition_key is None:
        return _auto_partition_index_for_id(id)
    if partition_key.startswith(AUTO_PARTITION_PREFIX):
        try:
            return int(partition_key[len(AUTO_PARTITION_PREFIX) :]) % AUTO_PARTITION_BUCKETS
        except ValueError:
            return partition_key
    return partition_key


def _partition_token_to_key(token: WakePartition) -> str:
    if isinstance(token, int):
        return _auto_partition_key(token)
    return token


def _run_at_delay_s(run_at_ms: int | None) -> float:
    if run_at_ms is None:
        return 0.0
    return max(0.0, (int(run_at_ms) - int(time.time() * 1000)) / 1000.0)


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

    def owner_for(self, partition_index: WakePartition) -> int:
        if isinstance(partition_index, str):
            return zlib.crc32(partition_index.encode()) % self.workers
        if self.owner_fn is not None:
            return self.owner_fn(partition_index) % self.workers
        return partition_index % self.workers

    async def notify_partition(self, partition_index: WakePartition, count: int = 1) -> None:
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
    ) -> tuple[list[WakePartition], int]:
        partition_credits, total_credit = await self.next_partition_credits(
            worker_index,
            timeout_s=timeout_s,
            max_partitions=max_partitions,
            max_credit=max_credit,
            same_group=same_group,
        )
        return [partition for partition, _credit in partition_credits], total_credit

    async def next_partition_credits(
        self,
        worker_index: int,
        *,
        timeout_s: float,
        max_partitions: int,
        max_credit: int,
        same_group=None,
    ) -> tuple[list[tuple[WakePartition, int]], int]:
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

        take = min(credit, max_credit)
        if credit > take:
            await self.return_credit(worker_index, partition_index, credit - take)
        if take <= 0:
            return [], 0

        partitions = [(partition_index, take)]
        total_credit = take

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
            if same_group is not None and not same_group(partitions[0][0], partition_index):
                await self.return_credit(worker_index, partition_index, credit)
                break
            remaining = max_credit - total_credit
            take = min(credit, remaining)
            if take <= 0:
                await self.return_credit(worker_index, partition_index, credit)
                break
            partitions.append((partition_index, take))
            total_credit += take
            if credit > take:
                await self.return_credit(worker_index, partition_index, credit - take)
                break

        return partitions, total_credit

    async def return_credit(self, worker_index: int, partition_index: WakePartition, credit: int) -> None:
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

    async def notify(self, state: str, partition_index: WakePartition, count: int = 1) -> None:
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
    ) -> tuple[list[WakePartition], int]:
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
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        idle_sleep_s: float = 0.1,
        on_error: AsyncErrorMode = "retry",
        complete_independent: bool = True,
        partition_key: str | None = None,
        partition_keys: Sequence[str] | None = None,
        auto_partitions: bool = False,
        worker_index: int = 0,
        workers: int = 1,
        server_shards: int = 16,
        claim_partition_batch_size: int = 16,
        wake_source: AsyncPartitionWakeCoordinator | None = None,
        wake_worker_index: int | None = None,
        wake_same_group: Callable[[WakePartition, WakePartition], bool] | None = None,
        allow_wake_partitions_outside_claim_set: bool = False,
        wake_broad_poll_interval_s: float | None = None,
        close_client: bool | None = None,
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
        if wake_broad_poll_interval_s is not None and wake_broad_poll_interval_s < 0:
            raise ValueError("wake_broad_poll_interval_s must be non-negative")
        if on_error not in {"retry", "fail", "raise"}:
            raise ValueError("on_error must be 'retry', 'fail', or 'raise'")

        self.client = _client_from(client)
        self._close_client = isinstance(client, str) if close_client is None else close_client

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
        self.wake_worker_index = (worker_index if wake_worker_index is None else wake_worker_index) % max(
            workers,
            1,
        )
        self.wake_same_group = wake_same_group
        self.allow_wake_partitions_outside_claim_set = allow_wake_partitions_outside_claim_set
        self.wake_broad_poll_interval_s = (
            0.05 if wake_broad_poll_interval_s is None else wake_broad_poll_interval_s
        )
        self._last_wake_broad_poll_s = 0.0
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

    async def run_once(self, handler: AsyncFlowHandler) -> QueueFlowWorkerResult:
        partition_key, partition_keys, limit = await self._next_wake_or_claim_plan()
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
        partition_key, partition_keys, limit = await self._next_wake_or_claim_plan()
        jobs = await self._claim_jobs(
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=limit,
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

    async def _next_wake_or_claim_plan(self) -> tuple[str | None, list[str] | None, int]:
        if self.wake_source is None:
            partition_key, partition_keys = self._next_claim_partition()
            return partition_key, partition_keys, self.batch_size

        next_partition_credits = getattr(self.wake_source, "next_partition_credits", None)
        if callable(next_partition_credits):
            partition_credits, _total_credit = await next_partition_credits(
                self.wake_worker_index,
                timeout_s=self.idle_sleep_s,
                max_partitions=self.claim_partition_batch_size,
                max_credit=self.batch_size,
                same_group=self.wake_same_group,
            )
        else:
            partition_tokens, credit = await self.wake_source.next_partitions(
                self.wake_worker_index,
                timeout_s=self.idle_sleep_s,
                max_partitions=self.claim_partition_batch_size,
                max_credit=self.batch_size,
                same_group=self.wake_same_group,
            )
            if partition_tokens:
                base = credit // len(partition_tokens)
                remainder = credit % len(partition_tokens)
                partition_credits = [
                    (token, base + (1 if idx < remainder else 0))
                    for idx, token in enumerate(partition_tokens)
                ]
            else:
                partition_credits = []

        if partition_credits:
            return await self._wake_claim_plan(partition_credits, self.wake_worker_index)

        if self.allow_wake_partitions_outside_claim_set and self._should_broad_poll():
            return None, None, self.batch_size

        partition_key, partition_keys = self._next_claim_partition()
        return partition_key, partition_keys, self.batch_size

    def _should_broad_poll(self) -> bool:
        interval = self.wake_broad_poll_interval_s
        if interval <= 0:
            return True
        now = asyncio.get_running_loop().time()
        if self._last_wake_broad_poll_s == 0.0 or now - self._last_wake_broad_poll_s >= interval:
            self._last_wake_broad_poll_s = now
            return True
        return False

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
                    jobs,
                    error=message,
                    independent=self.complete_independent,
                )
                failed += len(jobs)
            return 0, failed

        retried_jobs: list[AsyncFlowJob] = []
        for message, jobs in groups.items():
            await self.client.retry_many(
                None,
                jobs,
                error=message,
                independent=self.complete_independent,
            )
            retried_jobs.extend(jobs)
        await self._notify_retry_jobs(retried_jobs)
        return len(retried_jobs), 0

    async def _notify_retry_jobs(self, jobs: Sequence[AsyncFlowJob]) -> None:
        if self.wake_source is None:
            return
        notify_partition = getattr(self.wake_source, "notify_partition", None)
        if not callable(notify_partition):
            return
        counts: dict[WakePartition, int] = {}
        for job in jobs:
            token = _partition_token_from_key(job.id, job.partition_key)
            counts[token] = counts.get(token, 0) + 1
        for token, count in counts.items():
            await notify_partition(token, count)

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

    async def _wake_claim_plan(
        self,
        partition_credits: Sequence[tuple[WakePartition, int]],
        worker_index: int,
    ) -> tuple[str | None, list[str] | None, int]:
        pairs = [
            (token, _partition_token_to_key(token), max(int(credit), 0))
            for token, credit in partition_credits
            if credit > 0
        ]
        if self.partition_key is not None:
            allowed_pairs = [
                (token, key, credit)
                for token, key, credit in pairs
                if key == self.partition_key
            ]
        elif self.partition_keys is not None and not self.allow_wake_partitions_outside_claim_set:
            allowed = set(self.partition_keys)
            allowed_pairs = [
                (token, key, credit)
                for token, key, credit in pairs
                if key in allowed
            ]
        else:
            allowed_pairs = pairs

        allowed_tokens = {token for token, _key, _credit in allowed_pairs}
        rejected_pairs = [
            (token, credit)
            for token, _key, credit in pairs
            if token not in allowed_tokens
        ]
        await self._return_exact_wake_credit(worker_index, rejected_pairs)

        partition_keys = [key for _token, key, _credit in allowed_pairs]
        if not partition_keys:
            partition_key, fallback_keys = self._next_claim_partition()
            return partition_key, fallback_keys, self.batch_size

        usable_credit = max(1, sum(credit for _token, _key, credit in allowed_pairs))
        partition_key = partition_keys[0] if len(partition_keys) == 1 else None
        return (
            partition_key,
            None if partition_key is not None else partition_keys,
            max(1, min(self.batch_size, usable_credit)),
        )

    async def _return_exact_wake_credit(
        self,
        worker_index: int,
        partition_credits: Sequence[tuple[WakePartition, int]],
    ) -> None:
        if not partition_credits or self.wake_source is None:
            return

        return_credit = getattr(self.wake_source, "return_credit", None)
        if not callable(return_credit):
            return

        for token, credit in partition_credits:
            if credit <= 0:
                continue
            result = return_credit(worker_index, token, credit)
            if inspect.isawaitable(result):
                await result

    async def _return_filtered_wake_credit(
        self,
        worker_index: int,
        partition_tokens: Sequence[WakePartition],
        total_credit: int,
        total_partitions: int,
    ) -> int:
        if not partition_tokens or self.wake_source is None or total_credit <= 0 or total_partitions <= 0:
            return 0

        return_credit = getattr(self.wake_source, "return_credit", None)
        if not callable(return_credit):
            return 0

        base = total_credit // total_partitions
        remainder = total_credit % total_partitions
        returned = 0
        for idx, token in enumerate(partition_tokens):
            credit = base + (1 if idx < remainder else 0)
            if credit <= 0:
                continue
            result = return_credit(worker_index, token, credit)
            if inspect.isawaitable(result):
                await result
            returned += credit
        return returned


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
        on_error: AsyncErrorMode = "retry",
    ) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")
        if on_error not in {"retry", "fail", "raise"}:
            raise ValueError("on_error must be 'retry', 'fail', or 'raise'")
        self._url = client if isinstance(client, str) else None
        self._owns_client = isinstance(client, str)
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
        self.on_error = on_error
        self.wake_source = (
            AsyncPartitionWakeCoordinator(
                workers,
                owner_fn=lambda partition_index: _auto_partition_owner(
                    partition_index,
                    workers,
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
        if state == self.state:
            await self._notify_created_at(
                [id],
                partition_key=partition_key,
                run_at_ms=attrs.get("run_at_ms"),
            )
        return result

    async def signal(self, id: str, **kwargs: Any) -> Any:
        result = await self.client.signal(id, **kwargs)
        if kwargs.get("transition_to") == self.state:
            await self._notify_created_at(
                [id],
                partition_key=kwargs.get("partition_key"),
                run_at_ms=kwargs.get("run_at_ms"),
            )
        return result

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
        if state == self.state:
            await self._notify_create_items_at(
                create_items,
                partition_key=attrs.get("partition_key"),
                run_at_ms=attrs.get("run_at_ms"),
            )
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

    def _build_worker(self, worker_index: int) -> AsyncQueueFlowWorker:
        return AsyncQueueFlowWorker(
            self.client,
            type=self.type,
            state=self.state,
            concurrency=self.concurrency,
            batch_size=self.batch_size,
            complete_independent=self.complete_independent,
            on_error=self.on_error,
            auto_partitions=True,
            worker_index=worker_index,
            workers=self.workers,
            server_shards=self.server_shards,
            claim_partition_batch_size=self.claim_partition_batch_size,
            idle_sleep_s=self.idle_sleep_s,
            wake_source=self.wake_source,
            wake_worker_index=worker_index,
            wake_same_group=lambda first, candidate: _same_auto_server_shard(
                first,
                candidate,
                self.server_shards,
            ),
            allow_wake_partitions_outside_claim_set=True,
            close_client=False,
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
        counts: dict[WakePartition, int] = {}
        for id in ids:
            token = _partition_token_from_key(id, partition_key)
            counts[token] = counts.get(token, 0) + 1
        for token, count in counts.items():
            await self.wake_source.notify_partition(token, count)

    async def _notify_created_at(
        self,
        ids: Sequence[str],
        *,
        partition_key: str | None = None,
        run_at_ms: int | None = None,
    ) -> None:
        delay_s = _run_at_delay_s(run_at_ms)
        if delay_s <= 0:
            await self._notify_created(ids, partition_key=partition_key)
            return
        asyncio.create_task(self._delayed_notify_created(delay_s, ids, partition_key))

    async def _delayed_notify_created(
        self,
        delay_s: float,
        ids: Sequence[str],
        partition_key: str | None,
    ) -> None:
        await asyncio.sleep(delay_s)
        await self._notify_created(ids, partition_key=partition_key)

    async def _notify_create_items(
        self,
        items: Sequence[CreateItem],
        *,
        partition_key: str | None = None,
    ) -> None:
        if self.wake_source is None:
            return
        counts: dict[WakePartition, int] = {}
        for item in items:
            token = _partition_token_from_key(
                item.id,
                partition_key if partition_key is not None else item.partition_key,
            )
            counts[token] = counts.get(token, 0) + 1
        for token, count in counts.items():
            await self.wake_source.notify_partition(token, count)

    async def _notify_create_items_at(
        self,
        items: Sequence[CreateItem],
        *,
        partition_key: str | None = None,
        run_at_ms: int | None = None,
    ) -> None:
        delay_s = _run_at_delay_s(run_at_ms)
        if delay_s <= 0:
            await self._notify_create_items(items, partition_key=partition_key)
            return
        asyncio.create_task(self._delayed_notify_create_items(delay_s, items, partition_key))

    async def _delayed_notify_create_items(
        self,
        delay_s: float,
        items: Sequence[CreateItem],
        partition_key: str | None,
    ) -> None:
        await asyncio.sleep(delay_s)
        await self._notify_create_items(items, partition_key=partition_key)


@dataclass(frozen=True)
class AsyncWorkflowWorkerResult:
    claimed: int = 0
    applied: int = 0
    claim_calls: int = 0
    empty_claims: int = 0


class AsyncWorkflowFlowCommands:
    """Async Flow command helper bound to the current workflow job."""

    def __init__(self, ctx: "AsyncWorkflowContext") -> None:
        self._ctx = ctx

    def _partition(self, partition_key: str | None | object) -> str | None:
        return self._ctx.partition_key if partition_key is _CURRENT_PARTITION else partition_key

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
            lease_token or self._ctx.lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )


class AsyncWorkflowContext:
    """Async workflow handler context with value-ref helpers."""

    def __init__(self, workflow: "AsyncWorkflow", job: AsyncFlowJob, state_name: str) -> None:
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

    async def value(self, name: str, default: Any = None, *, local_cache: bool = False) -> Any:
        values = await self.value_many([name], local_cache=local_cache)
        return values.get(name, default)

    async def value_many(self, names: list[str], *, local_cache: bool = False) -> dict[str, Any]:
        values: dict[str, Any] = {}
        pending_names: list[str] = []
        pending_refs: list[str] = []

        for name in names:
            if local_cache and name in self._value_cache:
                values[name] = self._value_cache[name]
                continue

            if name in self.values:
                value = self.values[name]
                values[name] = value
                if local_cache:
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
            fetched = await self.client.value_mget(pending_refs, max_bytes=self.workflow.value_max_bytes)
            for name, value in zip(pending_names, fetched):
                values[name] = value
                if local_cache:
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
        type: str,
        states: Sequence[str] | None = None,
        initial_state: str | None = None,
        workers: int = 16,
        concurrency: int = 500,
        batch_size: int = 1000,
        claim_partition_batch_size: int = 16,
        server_shards: int = 16,
        idle_sleep_s: float = 0.001,
        producer_loop_thread: bool = True,
        owner_wakeup: bool = True,
        on_error: AsyncErrorMode = "retry",
        priority: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        wake_broad_poll_interval_s: float | None = None,
    ) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if claim_partition_batch_size <= 0:
            raise ValueError("claim_partition_batch_size must be positive")
        if wake_broad_poll_interval_s is not None and wake_broad_poll_interval_s < 0:
            raise ValueError("wake_broad_poll_interval_s must be non-negative")
        if on_error not in {"retry", "fail", "raise"}:
            raise ValueError("on_error must be 'retry', 'fail', or 'raise'")
        state_names = list(states) if states is not None else [initial_state or "queued"]
        if not state_names:
            raise ValueError("states must be non-empty")
        initial_state = initial_state if initial_state is not None else state_names[0]
        if initial_state not in state_names:
            raise ValueError("initial_state must be included in states")
        self._url = client if isinstance(client, str) else None
        self._owns_client = isinstance(client, str)
        self.client = _client_from(client)
        self.type = type
        self.states = state_names
        self.initial_state = initial_state
        self.workers = workers
        self.concurrency = concurrency
        self.batch_size = min(batch_size, FLOW_MANY_BATCH_LIMIT)
        self.claim_partition_batch_size = claim_partition_batch_size
        self.server_shards = server_shards
        self.idle_sleep_s = idle_sleep_s
        self.producer_loop_thread = producer_loop_thread
        self.owner_wakeup = owner_wakeup
        self.on_error = on_error
        self.priority = priority
        self.claim_values = list(claim_values) if claim_values is not None else None
        self.value_max_bytes = value_max_bytes
        self.wake_broad_poll_interval_s = (
            0.05 if wake_broad_poll_interval_s is None else wake_broad_poll_interval_s
        )
        self.wake_source = (
            AsyncStateWakeCoordinator(
                self.states,
                workers,
                owner_fn=lambda partition_index: _auto_partition_owner(
                    partition_index,
                    workers,
                    server_shards,
                ),
            )
            if owner_wakeup
            else None
        )
        self.handlers: dict[str, AsyncWorkflowHandler] = {}
        self.error_modes: dict[str, AsyncErrorMode] = {}
        self._partition_cursors = [0 for _ in range(workers)]
        self._state_cursors = [0 for _ in range(workers)]
        self._last_wake_broad_poll_s = [0.0 for _ in range(workers)]
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        self._totals = AsyncWorkflowWorkerResult()

    def on(
        self,
        state_name: str,
        *,
        on_error: AsyncErrorMode | None = None,
    ) -> Callable[[AsyncWorkflowHandler], AsyncWorkflowHandler]:
        if state_name not in self.states:
            raise ValueError(f"unknown workflow state: {state_name!r}")
        if on_error is not None and on_error not in {"retry", "fail", "raise"}:
            raise ValueError("on_error must be 'retry', 'fail', or 'raise'")

        def decorate(handler: AsyncWorkflowHandler) -> AsyncWorkflowHandler:
            self.handlers[state_name] = handler
            self.error_modes[state_name] = on_error or self.on_error
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
        await self._notify_ids_at(
            state,
            [id],
            partition_key=partition_key,
            run_at_ms=attrs.get("run_at_ms"),
        )
        return result

    async def signal(self, id: str, **kwargs: Any) -> Any:
        result = await self.client.signal(id, **kwargs)
        transition_to = kwargs.get("transition_to")
        if transition_to is not None:
            await self._notify_ids_at(
                transition_to,
                [id],
                partition_key=kwargs.get("partition_key"),
                run_at_ms=kwargs.get("run_at_ms"),
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
        await self._notify_create_items_at(
            state,
            create_items,
            partition_key=attrs.get("partition_key"),
            run_at_ms=attrs.get("run_at_ms"),
        )
        return result

    async def run_once(
        self,
        *,
        worker_index: int = 0,
        state: str | None = None,
    ) -> AsyncWorkflowWorkerResult:
        worker_index = worker_index % self.workers
        state_name = state or self._next_state(worker_index)
        if state_name not in self.handlers:
            raise ValueError(f"no handler for workflow state: {state_name!r}")
        if self.wake_source is not None:
            partition_tokens, credit = await self.wake_source.next_ready(
                state_name,
                worker_index,
                timeout_s=self.idle_sleep_s,
                max_partitions=self.claim_partition_batch_size,
                max_credit=self.batch_size,
                same_group=lambda first, candidate: _same_auto_server_shard(
                    first,
                    candidate,
                    self.server_shards,
                ),
            )
            if partition_tokens:
                partition_keys = [_partition_token_to_key(token) for token in partition_tokens]
                partition_key = partition_keys[0] if len(partition_keys) == 1 else None
                partition_keys = None if partition_key is not None else partition_keys
                limit = max(1, min(self.batch_size, credit))
            elif self._should_broad_poll(worker_index):
                partition_key, partition_keys = None, None
                limit = self.batch_size
            else:
                partition_key, partition_keys = self._next_claim_partition(worker_index)
                limit = self.batch_size
        else:
            partition_key, partition_keys = self._next_claim_partition(worker_index)
            limit = self.batch_size
        claim_kwargs = dict(
            state=state_name,
            worker=f"{self.type}:async-workflow:{worker_index}",
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=limit,
            priority=self.priority,
            reclaim_expired=None,
        )
        if self.claim_values is not None:
            jobs = await self.client.claim_due(
                self.type,
                **claim_kwargs,
                payload=False,
                values=self.claim_values,
                value_max_bytes=self.value_max_bytes,
            )
        else:
            jobs = await self.client.claim_jobs(self.type, **claim_kwargs)
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
        if self._owns_client:
            await self.client.close()

    async def _run_loop(self, worker_index: int) -> None:
        try:
            while self._running:
                result = await self.run_once(worker_index=worker_index)
                self._totals = self._merge(self._totals, result)
                if result.claimed == 0:
                    await asyncio.sleep(self.idle_sleep_s)
        finally:
            self._running = False

    async def _handle_claimed_batch(self, state_name: str, jobs: list[AsyncFlowJob]) -> int:
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
                if on_error == "fail":
                    value = fail(error=str(exc))
                else:
                    value = retry(error=str(exc))
            return self._normalize_outcome(value)

        outcomes = await asyncio.gather(*(run_one(job) for job in jobs))

        first = outcomes[0]
        if all(outcome == first for outcome in outcomes):
            await self._apply_uniform(state_name, jobs, first)
            return len(jobs)

        for job, outcome in zip(jobs, outcomes):
            await self._apply_uniform(state_name, [job], outcome)
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
            await self._notify_jobs_at(outcome.to_state, jobs, run_at_ms=outcome.run_at_ms)
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
            await self._notify_jobs_at(state_name, jobs, run_at_ms=outcome.run_at_ms)
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

    def _should_broad_poll(self, worker_index: int) -> bool:
        interval = self.wake_broad_poll_interval_s
        if interval <= 0:
            return True
        now = asyncio.get_running_loop().time()
        last = self._last_wake_broad_poll_s[worker_index]
        if last == 0.0 or now - last >= interval:
            self._last_wake_broad_poll_s[worker_index] = now
            return True
        return False

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
        counts: dict[WakePartition, int] = {}
        for id in ids:
            token = _partition_token_from_key(id, partition_key)
            counts[token] = counts.get(token, 0) + 1
        for token, count in counts.items():
            await self.wake_source.notify(state, token, count)

    async def _notify_ids_at(
        self,
        state: str,
        ids: Sequence[str],
        *,
        partition_key: str | None = None,
        run_at_ms: int | None = None,
    ) -> None:
        delay_s = _run_at_delay_s(run_at_ms)
        if delay_s <= 0:
            await self._notify_ids(state, ids, partition_key=partition_key)
            return
        asyncio.create_task(self._delayed_notify_ids(delay_s, state, ids, partition_key))

    async def _delayed_notify_ids(
        self,
        delay_s: float,
        state: str,
        ids: Sequence[str],
        partition_key: str | None,
    ) -> None:
        await asyncio.sleep(delay_s)
        await self._notify_ids(state, ids, partition_key=partition_key)

    async def _notify_create_items(
        self,
        state: str,
        items: Sequence[CreateItem],
        *,
        partition_key: str | None = None,
    ) -> None:
        if self.wake_source is None:
            return
        counts: dict[WakePartition, int] = {}
        for item in items:
            token = _partition_token_from_key(
                item.id,
                partition_key if partition_key is not None else item.partition_key,
            )
            counts[token] = counts.get(token, 0) + 1
        for token, count in counts.items():
            await self.wake_source.notify(state, token, count)

    async def _notify_create_items_at(
        self,
        state: str,
        items: Sequence[CreateItem],
        *,
        partition_key: str | None = None,
        run_at_ms: int | None = None,
    ) -> None:
        delay_s = _run_at_delay_s(run_at_ms)
        if delay_s <= 0:
            await self._notify_create_items(state, items, partition_key=partition_key)
            return
        asyncio.create_task(self._delayed_notify_create_items(delay_s, state, items, partition_key))

    async def _delayed_notify_create_items(
        self,
        delay_s: float,
        state: str,
        items: Sequence[CreateItem],
        partition_key: str | None,
    ) -> None:
        await asyncio.sleep(delay_s)
        await self._notify_create_items(state, items, partition_key=partition_key)

    async def _notify_jobs(self, state: str, jobs: Sequence[ClaimedItem]) -> None:
        if self.wake_source is None:
            return
        counts: dict[WakePartition, int] = {}
        for job in jobs:
            token = _partition_token_from_key(job.id, job.partition_key)
            counts[token] = counts.get(token, 0) + 1
        for token, count in counts.items():
            await self.wake_source.notify(state, token, count)

    async def _notify_jobs_at(
        self,
        state: str,
        jobs: Sequence[ClaimedItem],
        *,
        run_at_ms: int | None = None,
    ) -> None:
        delay_s = _run_at_delay_s(run_at_ms)
        if delay_s <= 0:
            await self._notify_jobs(state, jobs)
            return
        asyncio.create_task(self._delayed_notify_jobs(delay_s, state, jobs))

    async def _delayed_notify_jobs(
        self,
        delay_s: float,
        state: str,
        jobs: Sequence[ClaimedItem],
    ) -> None:
        await asyncio.sleep(delay_s)
        await self._notify_jobs(state, jobs)

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
