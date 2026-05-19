import argparse
import asyncio
import math
import threading
import time
import uuid
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ferricstore import AsyncFlowClient, ClaimedItem, CreateItem, FencedItem


AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 256
SERVER_SLOT_COUNT = 1024
DEFAULT_STATE = "queued"


def chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def now_ms() -> int:
    return int(time.time() * 1000)


def payload_bytes(size: int) -> bytes:
    if size <= 0:
        return b""
    return b"x" * size


def flow_id(run_id: str, index: int) -> str:
    return f"{run_id}:flow:{index}"


def workflow_states(steps: int) -> list[str]:
    if steps <= 1:
        return [DEFAULT_STATE]
    return [DEFAULT_STATE] + [f"step_{idx}" for idx in range(1, steps)]


def explicit_partition_for(index: int, partitions: int, run_id: str) -> str:
    return f"{run_id}:partition:{index % max(partitions, 1)}"


def auto_partition_index_for_flow_id(id: str) -> int:
    return zlib.crc32(id.encode()) % AUTO_PARTITION_BUCKETS


def auto_partition_key_for_index(index: int) -> str:
    return f"{AUTO_PARTITION_PREFIX}{index % AUTO_PARTITION_BUCKETS}"


def auto_partition_index_from_key(partition_key: str | None) -> int | None:
    if not partition_key or not partition_key.startswith(AUTO_PARTITION_PREFIX):
        return None
    try:
        return int(partition_key[len(AUTO_PARTITION_PREFIX) :]) % AUTO_PARTITION_BUCKETS
    except ValueError:
        return None


def partition_key_for_create(
    *,
    partition_mode: str,
    index: int,
    partitions: int,
    run_id: str,
) -> str | None:
    if partition_mode == "auto":
        return None
    return explicit_partition_for(index, partitions, run_id)


def claim_partition_key(
    *,
    partition_mode: str,
    partition_index: int,
    partitions: int,
    run_id: str,
) -> str:
    if partition_mode == "auto":
        return auto_partition_key_for_index(partition_index)
    return explicit_partition_for(partition_index, partitions, run_id)


def partition_index_for_created_flow(
    *,
    partition_mode: str,
    run_id: str,
    index: int,
    partitions: int,
) -> int:
    if partition_mode == "auto":
        return auto_partition_index_for_flow_id(flow_id(run_id, index))
    return index % max(partitions, 1)


def server_shard_for_slot(slot: int, server_shards: int) -> int:
    server_shards = max(server_shards, 1)
    slots_per_shard = SERVER_SLOT_COUNT // server_shards
    remainder = SERVER_SLOT_COUNT % server_shards
    wide_slots = (slots_per_shard + 1) * remainder
    slot = slot % SERVER_SLOT_COUNT
    if slot < wide_slots:
        return slot // (slots_per_shard + 1)
    return remainder + ((slot - wide_slots) // slots_per_shard)


def auto_partition_server_shard_for_index(index: int, server_shards: int) -> int:
    tag = f"fa:{index % AUTO_PARTITION_BUCKETS}"
    slot = zlib.crc32(tag.encode()) & (SERVER_SLOT_COUNT - 1)
    return server_shard_for_slot(slot, server_shards)


def claimed_partition_counts(jobs: list[ClaimedItem]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job in jobs:
        if job.partition_key:
            counts[job.partition_key] = counts.get(job.partition_key, 0) + 1
    return counts


def partition_index_from_claimed_key(partition_key: str, partition_mode: str, partitions: int) -> int | None:
    if partition_mode == "auto":
        return auto_partition_index_from_key(partition_key)
    marker = ":partition:"
    if marker not in partition_key:
        return None
    try:
        return int(partition_key.rsplit(marker, 1)[1]) % max(partitions, 1)
    except ValueError:
        return None


@dataclass
class AsyncCounters:
    workflows: int
    completed: int = 0
    claimed_actions: int = 0

    def __post_init__(self) -> None:
        self.lock = asyncio.Lock()

    async def add(self, *, completed: int, claimed_actions: int) -> None:
        async with self.lock:
            self.completed += completed
            self.claimed_actions += claimed_actions

    async def snapshot(self) -> tuple[int, int]:
        async with self.lock:
            return self.completed, self.claimed_actions

    async def done(self) -> bool:
        async with self.lock:
            return self.completed >= self.workflows

    async def all_actions_claimed(self, expected_actions: int) -> bool:
        async with self.lock:
            return self.claimed_actions >= expected_actions


class AsyncFlowReadyCoordinator:
    def __init__(self, workers: int, owner_fn=None) -> None:
        self.workers = max(workers, 1)
        self.owner_fn = owner_fn
        self.queues: dict[tuple[int, str], asyncio.Queue[int]] = {}
        self.pending: dict[tuple[int, str], set[int]] = {}
        self.credits: dict[tuple[int, str], dict[int, int]] = {}
        self.locks: dict[tuple[int, str], asyncio.Lock] = {}
        self.notifications = 0
        self.notified_jobs = 0

    def owner_for(self, partition_index: int) -> int:
        if self.owner_fn is not None:
            return self.owner_fn(partition_index) % self.workers
        return partition_index % self.workers

    def _bucket(self, worker_index: int, state_name: str) -> tuple[int, str]:
        key = (worker_index, state_name)
        if key not in self.queues:
            self.queues[key] = asyncio.Queue()
            self.pending[key] = set()
            self.credits[key] = {}
            self.locks[key] = asyncio.Lock()
        return key

    async def notify(self, *, state_name: str, partition_index: int, count: int) -> None:
        count = max(int(count), 0)
        if count == 0:
            return
        worker_index = self.owner_for(partition_index)
        key = self._bucket(worker_index, state_name)
        should_queue = False
        async with self.locks[key]:
            self.credits[key][partition_index] = self.credits[key].get(partition_index, 0) + count
            self.notified_jobs += count
            if partition_index not in self.pending[key]:
                self.pending[key].add(partition_index)
                self.notifications += 1
                should_queue = True
        if should_queue:
            self.queues[key].put_nowait(partition_index)

    async def notify_partition_counts(
        self,
        *,
        state_name: str,
        partition_counts: dict[str, int],
        partition_mode: str,
        partitions: int,
    ) -> None:
        for partition_key, count in partition_counts.items():
            partition_index = partition_index_from_claimed_key(partition_key, partition_mode, partitions)
            if partition_index is not None:
                await self.notify(state_name=state_name, partition_index=partition_index, count=count)

    async def next_ready(
        self,
        *,
        worker_index: int,
        state_name: str,
        timeout_s: float,
        max_partitions: int,
        max_credit: int,
        same_group=None,
    ) -> tuple[list[int], int]:
        key = self._bucket(worker_index, state_name)
        if max_partitions <= 0 or max_credit <= 0:
            return [], 0
        if timeout_s <= 0:
            try:
                partition_index = self.queues[key].get_nowait()
            except asyncio.QueueEmpty:
                return [], 0
        else:
            try:
                partition_index = await asyncio.wait_for(
                    self.queues[key].get(),
                    timeout=timeout_s,
                )
            except asyncio.TimeoutError:
                return [], 0
        async with self.locks[key]:
            self.pending[key].discard(partition_index)
            credit = self.credits[key].pop(partition_index, 0)

        partitions = [partition_index]
        total_credit = credit
        while len(partitions) < max_partitions and total_credit < max_credit:
            try:
                partition_index = self.queues[key].get_nowait()
            except asyncio.QueueEmpty:
                break
            async with self.locks[key]:
                self.pending[key].discard(partition_index)
                credit = self.credits[key].pop(partition_index, 0)
            if credit <= 0:
                continue
            if same_group is not None and not same_group(partitions[0], partition_index):
                await self.return_credit(
                    worker_index=worker_index,
                    state_name=state_name,
                    partition_index=partition_index,
                    count=credit,
                )
                break
            partitions.append(partition_index)
            total_credit += credit
        return partitions, total_credit

    async def return_credit(
        self,
        *,
        worker_index: int,
        state_name: str,
        partition_index: int,
        count: int,
    ) -> None:
        count = max(int(count), 0)
        if count == 0:
            return
        key = self._bucket(worker_index, state_name)
        should_queue = False
        async with self.locks[key]:
            self.credits[key][partition_index] = self.credits[key].get(partition_index, 0) + count
            if partition_index not in self.pending[key]:
                self.pending[key].add(partition_index)
                should_queue = True
        if should_queue:
            self.queues[key].put_nowait(partition_index)


class CrossLoopFlowReadyCoordinator:
    def __init__(
        self,
        workers: int,
        states: list[str],
        loop: asyncio.AbstractEventLoop,
        owner_fn=None,
    ) -> None:
        self.workers = max(workers, 1)
        self.owner_fn = owner_fn
        self.loop = loop
        self.queues: dict[tuple[int, str], asyncio.Queue[int]] = {}
        self.pending: dict[tuple[int, str], set[int]] = {}
        self.credits: dict[tuple[int, str], dict[int, int]] = {}
        self.locks: dict[tuple[int, str], threading.Lock] = {}
        self.notifications = 0
        self.notified_jobs = 0
        for worker_index in range(self.workers):
            for state_name in states:
                self._bucket(worker_index, state_name)

    def owner_for(self, partition_index: int) -> int:
        if self.owner_fn is not None:
            return self.owner_fn(partition_index) % self.workers
        return partition_index % self.workers

    def _bucket(self, worker_index: int, state_name: str) -> tuple[int, str]:
        key = (worker_index, state_name)
        if key not in self.queues:
            self.queues[key] = asyncio.Queue()
            self.pending[key] = set()
            self.credits[key] = {}
            self.locks[key] = threading.Lock()
        return key

    async def notify(self, *, state_name: str, partition_index: int, count: int) -> None:
        count = max(int(count), 0)
        if count == 0:
            return
        worker_index = self.owner_for(partition_index)
        key = self._bucket(worker_index, state_name)
        should_queue = False
        with self.locks[key]:
            self.credits[key][partition_index] = self.credits[key].get(partition_index, 0) + count
            self.notified_jobs += count
            if partition_index not in self.pending[key]:
                self.pending[key].add(partition_index)
                self.notifications += 1
                should_queue = True
        if should_queue:
            self.loop.call_soon_threadsafe(
                self.queues[key].put_nowait,
                partition_index,
            )

    async def notify_partition_counts(
        self,
        *,
        state_name: str,
        partition_counts: dict[str, int],
        partition_mode: str,
        partitions: int,
    ) -> None:
        for partition_key, count in partition_counts.items():
            partition_index = partition_index_from_claimed_key(partition_key, partition_mode, partitions)
            if partition_index is not None:
                await self.notify(state_name=state_name, partition_index=partition_index, count=count)

    async def next_ready(
        self,
        *,
        worker_index: int,
        state_name: str,
        timeout_s: float,
        max_partitions: int,
        max_credit: int,
        same_group=None,
    ) -> tuple[list[int], int]:
        key = self._bucket(worker_index, state_name)
        if max_partitions <= 0 or max_credit <= 0:
            return [], 0
        if timeout_s <= 0:
            try:
                partition_index = self.queues[key].get_nowait()
            except asyncio.QueueEmpty:
                return [], 0
        else:
            try:
                partition_index = await asyncio.wait_for(
                    self.queues[key].get(),
                    timeout=timeout_s,
                )
            except asyncio.TimeoutError:
                return [], 0
        with self.locks[key]:
            self.pending[key].discard(partition_index)
            credit = self.credits[key].pop(partition_index, 0)

        partitions = [partition_index]
        total_credit = credit
        while len(partitions) < max_partitions and total_credit < max_credit:
            try:
                partition_index = self.queues[key].get_nowait()
            except asyncio.QueueEmpty:
                break
            with self.locks[key]:
                self.pending[key].discard(partition_index)
                credit = self.credits[key].pop(partition_index, 0)
            if credit <= 0:
                continue
            if same_group is not None and not same_group(partitions[0], partition_index):
                await self.return_credit(
                    worker_index=worker_index,
                    state_name=state_name,
                    partition_index=partition_index,
                    count=credit,
                )
                break
            partitions.append(partition_index)
            total_credit += credit
        return partitions, total_credit

    async def return_credit(
        self,
        *,
        worker_index: int,
        state_name: str,
        partition_index: int,
        count: int,
    ) -> None:
        count = max(int(count), 0)
        if count == 0:
            return
        key = self._bucket(worker_index, state_name)
        should_queue = False
        with self.locks[key]:
            self.credits[key][partition_index] = self.credits[key].get(partition_index, 0) + count
            if partition_index not in self.pending[key]:
                self.pending[key].add(partition_index)
                should_queue = True
        if should_queue:
            self.loop.call_soon_threadsafe(
                self.queues[key].put_nowait,
                partition_index,
            )


FlowReadyWakeCoordinator = AsyncFlowReadyCoordinator | CrossLoopFlowReadyCoordinator


async def create_workflows(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    indices: list[int],
    partitions: int,
    partition_mode: str,
    payload: bytes,
    create_batch_size: int,
    create_inflight: int,
    create_mode: str,
    independent_many: bool,
    wake_coordinator: FlowReadyWakeCoordinator | None,
) -> dict[str, int]:
    client = AsyncFlowClient.from_url(url)
    created = 0

    async def notify(batch: list[int]) -> None:
        if wake_coordinator is None:
            return
        counts: dict[int, int] = {}
        for index in batch:
            partition_index = partition_index_for_created_flow(
                partition_mode=partition_mode,
                run_id=run_id,
                index=index,
                partitions=partitions,
            )
            counts[partition_index] = counts.get(partition_index, 0) + 1
        for partition_index, count in counts.items():
            await wake_coordinator.notify(
                state_name=DEFAULT_STATE,
                partition_index=partition_index,
                count=count,
            )

    try:
        if create_mode == "single":
            for index in indices:
                await client.enqueue(
                    flow_id(run_id, index),
                    type=flow_type,
                    state=DEFAULT_STATE,
                    payload=payload,
                    partition_key=partition_key_for_create(
                        partition_mode=partition_mode,
                        index=index,
                        partitions=partitions,
                        run_id=run_id,
                    ),
                    return_record=False,
                )
                created += 1
                await notify([index])
            return {"created": created}

        if create_mode == "pipeline":
            for batch in chunks(indices, max(create_batch_size, 1)):
                pipe = client.pipeline()
                ts = now_ms()
                for index in batch:
                    args: list[Any] = [
                        "FLOW.CREATE",
                        flow_id(run_id, index),
                        "TYPE",
                        flow_type,
                        "STATE",
                        DEFAULT_STATE,
                        "NOW",
                        ts,
                    ]
                    partition_key = partition_key_for_create(
                        partition_mode=partition_mode,
                        index=index,
                        partitions=partitions,
                        run_id=run_id,
                    )
                    if partition_key is not None:
                        args.extend(["PARTITION", partition_key])
                    if payload:
                        args.extend(["PAYLOAD", payload])
                    args.extend(["RUN_AT", ts, "PRIORITY", 0])
                    pipe.command(*args)
                await pipe.execute()
                created += len(batch)
                await notify(batch)
            return {"created": created}

        if partition_mode == "auto":
            auto_buffers: dict[int, list[int]] = {}
            pending: list[asyncio.Task[tuple[int, list[int]]]] = []
            create_inflight = max(create_inflight, 1)

            async def send_auto_batch(batch: list[int]) -> tuple[int, list[int]]:
                items = [CreateItem(flow_id(run_id, index), payload) for index in batch]
                await client.enqueue_many(
                    items,
                    type=flow_type,
                    state=DEFAULT_STATE,
                    independent=independent_many,
                )
                return len(items), batch

            async def drain_one() -> None:
                nonlocal created
                done, pending_set = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                pending[:] = list(pending_set)
                for task in done:
                    count, batch = task.result()
                    created += count
                    await notify(batch)

            async def flush_auto_bucket(partition_index: int) -> None:
                batch = auto_buffers.get(partition_index)
                if not batch:
                    return
                pending.append(asyncio.create_task(send_auto_batch(batch)))
                auto_buffers[partition_index] = []
                if len(pending) >= create_inflight:
                    await drain_one()

            for index in indices:
                partition_index = auto_partition_index_for_flow_id(flow_id(run_id, index))
                bucket = auto_buffers.setdefault(partition_index, [])
                bucket.append(index)
                if len(bucket) >= max(create_batch_size, 1):
                    await flush_auto_bucket(partition_index)

            for partition_index in list(auto_buffers):
                await flush_auto_bucket(partition_index)
            while pending:
                await drain_one()
            return {"created": created}

        for batch in chunks(indices, max(create_batch_size, 1)):
            items = [
                CreateItem(
                    flow_id(run_id, index),
                    payload,
                    explicit_partition_for(index, partitions, run_id),
                )
                for index in batch
            ]
            await client.enqueue_many(
                items,
                type=flow_type,
                state=DEFAULT_STATE,
                independent=independent_many,
            )
            created += len(items)
            await notify(batch)
        return {"created": created}
    finally:
        await client.close()


async def run_workflow_worker(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    worker_index: int,
    workers: int,
    partitions: int,
    partition_mode: str,
    states: list[str],
    claim_batch_size: int,
    claim_partition_batch_size: int,
    apply_inflight: int,
    idle_sleep_ms: float,
    max_idle_sleep_ms: float,
    result_payload: bytes | None,
    independent_many: bool,
    counters: AsyncCounters,
    producers_done: asyncio.Event,
    wake_coordinator: FlowReadyWakeCoordinator | None,
    wake_coalesce_ms: float,
    server_shards: int,
) -> dict[str, int | float]:
    client = AsyncFlowClient.from_url(url)
    partition_count = AUTO_PARTITION_BUCKETS if partition_mode == "auto" else partitions
    def owner_for_partition(partition_index: int) -> int:
        if partition_mode == "auto":
            return auto_partition_server_shard_for_index(partition_index, server_shards) % workers
        return partition_index % workers

    owned_partitions = [
        p for p in range(partition_count) if owner_for_partition(p) == worker_index
    ]
    if partition_mode == "auto":
        owned_partitions.sort(
            key=lambda idx: (auto_partition_server_shard_for_index(idx, server_shards), idx)
        )
    if not owned_partitions:
        return {
            "claimed_actions": 0,
            "completed": 0,
            "claim_calls": 0,
            "empty_claims": 0,
            "max_claim_batch": 0,
            "wake_coalesce_sleeps": 0,
            "wake_coalesce_ms": 0.0,
        }

    final_state = states[-1]
    next_state_by_state = {
        state_name: states[idx + 1]
        for idx, state_name in enumerate(states[:-1])
    }
    state_cursor = 0
    partition_cursor = 0
    claim_calls = 0
    empty_claims = 0
    claimed_actions = 0
    completed_workflows = 0
    max_claim_batch = 0
    wake_idle_rounds = 0
    last_claimed_seen = 0
    wake_coalesce_sleeps = 0
    wake_coalesce_seconds = 0.0
    idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
    max_idle_sleep_s = max(max_idle_sleep_ms, idle_sleep_ms, 0.0) / 1000.0
    current_sleep = idle_sleep_s
    wake_coalesce_s = max(wake_coalesce_ms, 0.0) / 1000.0
    claim_partition_batch_size = max(claim_partition_batch_size, 1)
    apply_inflight = max(apply_inflight, 0)
    pending_applies: list[asyncio.Task[tuple[int, int, str | None, list[ClaimedItem]]]] = []
    expected_actions = counters.workflows * len(states)

    same_group = None
    if partition_mode == "auto":
        same_group = lambda first, candidate: (
            auto_partition_server_shard_for_index(first, server_shards)
            == auto_partition_server_shard_for_index(candidate, server_shards)
        )

    def partition_keys_from_indices(indices: list[int]) -> tuple[str | None, list[str] | None]:
        keys = [
            claim_partition_key(
                partition_mode=partition_mode,
                partition_index=index,
                partitions=partitions,
                run_id=run_id,
            )
            for index in indices
        ]
        if len(keys) == 1:
            return keys[0], None
        return None, keys

    async def notify_next_state(state_name: str, jobs: list[ClaimedItem]) -> None:
        if wake_coordinator is None:
            return
        await wake_coordinator.notify_partition_counts(
            state_name=state_name,
            partition_counts=claimed_partition_counts(jobs),
            partition_mode=partition_mode,
            partitions=partitions,
        )

    async def apply_jobs(
        state_name: str,
        jobs: list[ClaimedItem],
    ) -> tuple[int, int, str | None, list[ClaimedItem]]:
        batch_size = len(jobs)
        if state_name == final_state:
            await client.complete_jobs(
                jobs,
                result=result_payload,
                independent=independent_many,
            )
            return batch_size, batch_size, None, []

        next_state = next_state_by_state[state_name]
        fenced_items = [
            FencedItem(
                id=job.id,
                fencing_token=job.fencing_token,
                lease_token=job.lease_token,
                partition_key=job.partition_key,
            )
            for job in jobs
        ]
        await client.transition_many(
            None,
            from_state="running",
            to_state=next_state,
            items=fenced_items,
            independent=independent_many,
        )
        return 0, batch_size, next_state, jobs

    async def drain_applies(*, block: bool = False, limit: int | None = None) -> int:
        nonlocal completed_workflows
        drained = 0
        while pending_applies and (limit is None or drained < limit):
            if block:
                done, pending_set = await asyncio.wait(
                    pending_applies,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            else:
                done = {task for task in pending_applies if task.done()}
                if not done:
                    break
                pending_set = set(pending_applies) - done
            pending_applies[:] = list(pending_set)
            for task in done:
                completed_count, claimed_count, next_state, jobs = task.result()
                completed_workflows += completed_count
                await counters.add(completed=completed_count, claimed_actions=0)
                if next_state is not None:
                    await notify_next_state(next_state, jobs)
                drained += 1
        return drained

    try:
        while True:
            await drain_applies(block=False)
            if await counters.done():
                break
            if await counters.all_actions_claimed(expected_actions):
                if pending_applies:
                    await drain_applies(block=True, limit=1)
                else:
                    await asyncio.sleep(current_sleep)
                continue

            state_name = states[state_cursor]
            state_cursor = (state_cursor + 1) % len(states)
            limit = claim_batch_size
            claim_credit = claim_batch_size
            if wake_coordinator is not None:
                selected_partitions, credit = await wake_coordinator.next_ready(
                    worker_index=worker_index,
                    state_name=state_name,
                    timeout_s=current_sleep,
                    max_partitions=claim_partition_batch_size,
                    max_credit=claim_batch_size,
                    same_group=same_group,
                )
                if not selected_partitions:
                    if producers_done.is_set():
                        if await counters.done():
                            break
                        _completed, observed_claimed = await counters.snapshot()
                        if observed_claimed >= expected_actions:
                            if pending_applies:
                                await drain_applies(block=True, limit=1)
                            else:
                                await asyncio.sleep(current_sleep)
                            continue
                        if observed_claimed != last_claimed_seen:
                            last_claimed_seen = observed_claimed
                            wake_idle_rounds = 0
                            continue
                        wake_idle_rounds += 1
                        if wake_idle_rounds < 3:
                            continue
                        selected_partitions = [owned_partitions[partition_cursor % len(owned_partitions)]]
                        partition_cursor += 1
                    else:
                        continue
                else:
                    wake_idle_rounds = 0
                    claim_credit = max(credit, 1)
                    limit = min(claim_batch_size, claim_credit)
                    if wake_coalesce_s > 0 and credit < claim_batch_size and not producers_done.is_set():
                        sleep_s = min(wake_coalesce_s, 0.002)
                        await asyncio.sleep(sleep_s)
                        wake_coalesce_sleeps += 1
                        wake_coalesce_seconds += sleep_s
                        extra_partitions, extra_credit = await wake_coordinator.next_ready(
                            worker_index=worker_index,
                            state_name=state_name,
                            timeout_s=0,
                            max_partitions=max(
                                claim_partition_batch_size - len(selected_partitions),
                                0,
                            ),
                            max_credit=max(claim_batch_size - credit, 0),
                            same_group=same_group,
                        )
                        selected_partitions.extend(extra_partitions)
                        credit += extra_credit
                        claim_credit = max(credit, 1)
                        limit = min(claim_batch_size, claim_credit)
            else:
                selected_partitions = []
                for _ in range(min(claim_partition_batch_size, len(owned_partitions))):
                    selected_partitions.append(owned_partitions[partition_cursor % len(owned_partitions)])
                    partition_cursor += 1

            partition_key, partition_keys = partition_keys_from_indices(selected_partitions)
            remaining_credit = max(claim_credit, 1)
            total_claimed_this_round = 0
            while remaining_credit > 0 and not await counters.done():
                claim_limit = min(claim_batch_size, remaining_credit)
                claim_calls += 1
                jobs = await client.claim_jobs(
                    flow_type,
                    state=state_name,
                    worker=f"{run_id}:worker:{worker_index}",
                    partition_key=partition_key,
                    partition_keys=partition_keys,
                    limit=claim_limit,
                    priority=0,
                    reclaim_expired=False,
                )
                if not jobs:
                    empty_claims += 1
                    break

                current_sleep = idle_sleep_s
                batch_size = len(jobs)
                total_claimed_this_round += batch_size
                claimed_actions += batch_size
                max_claim_batch = max(max_claim_batch, batch_size)
                await counters.add(completed=0, claimed_actions=batch_size)

                if apply_inflight > 0:
                    pending_applies.append(asyncio.create_task(apply_jobs(state_name, jobs)))
                    while len(pending_applies) >= apply_inflight:
                        await drain_applies(block=True, limit=1)
                else:
                    completed_count, claimed_count, next_state, applied_jobs = await apply_jobs(
                        state_name,
                        jobs,
                    )
                    completed_workflows += completed_count
                    await counters.add(completed=completed_count, claimed_actions=0)
                    if next_state is not None:
                        await notify_next_state(next_state, applied_jobs)

                remaining_credit -= batch_size
                if batch_size < claim_limit:
                    break

            if total_claimed_this_round == 0:
                if producers_done.is_set() and await counters.done():
                    break
                if wake_coordinator is None and current_sleep > 0:
                    await asyncio.sleep(current_sleep)
                    current_sleep = min(max_idle_sleep_s, max(current_sleep * 2, idle_sleep_s))

        while pending_applies:
            await drain_applies(block=True)

        return {
            "claimed_actions": claimed_actions,
            "completed": completed_workflows,
            "claim_calls": claim_calls,
            "empty_claims": empty_claims,
            "max_claim_batch": max_claim_batch,
            "wake_coalesce_sleeps": wake_coalesce_sleeps,
            "wake_coalesce_ms": wake_coalesce_seconds * 1000.0,
        }
    finally:
        await client.close()


async def run_state_machine_throughput(args: argparse.Namespace) -> dict[str, Any]:
    run_id = f"py-async-sm-bench-{uuid.uuid4().hex}"
    flow_type = f"async_state_machine_bench:{run_id}"
    states = workflow_states(args.steps)
    indices = list(range(args.flows))
    payload = payload_bytes(args.payload_bytes)
    result_payload = payload_bytes(args.result_bytes) if args.result_bytes > 0 else None
    counters = AsyncCounters(args.flows)
    producers_done = asyncio.Event()
    worker_mode = "polling" if args.worker_mode == "auto" else args.worker_mode
    wake_coordinator = (
        CrossLoopFlowReadyCoordinator(
            args.workers,
            states,
            asyncio.get_running_loop(),
            owner_fn=(
                lambda partition_index: auto_partition_server_shard_for_index(
                    partition_index,
                    args.server_shards,
                )
                if args.partition_mode == "auto"
                else partition_index
            ),
        )
        if worker_mode == "owner-wakeup" and args.producer_loop_thread
        else AsyncFlowReadyCoordinator(
            args.workers,
            owner_fn=(
                lambda partition_index: auto_partition_server_shard_for_index(
                    partition_index,
                    args.server_shards,
                )
                if args.partition_mode == "auto"
                else partition_index
            ),
        )
        if worker_mode == "owner-wakeup"
        else None
    )

    if args.partition_mode == "auto" and args.create_mode == "many":
        create_ranges = [[] for _ in range(args.producers)]
        for index in indices:
            owner = auto_partition_index_for_flow_id(flow_id(run_id, index)) % args.producers
            create_ranges[owner].append(index)
    else:
        create_ranges = [indices[offset :: args.producers] for offset in range(args.producers)]

    async def create_task(batch: list[int]) -> dict[str, int]:
        return await create_workflows(
            url=args.url,
            run_id=run_id,
            flow_type=flow_type,
            indices=batch,
            partitions=args.partitions,
            partition_mode=args.partition_mode,
            payload=payload,
                create_batch_size=args.create_batch_size,
                create_inflight=args.create_inflight,
            create_mode=args.create_mode,
            independent_many=args.independent_many,
            wake_coordinator=wake_coordinator,
        )

    async def worker_task(worker_index: int) -> dict[str, int | float]:
        return await run_workflow_worker(
            url=args.url,
            run_id=run_id,
            flow_type=flow_type,
            worker_index=worker_index,
            workers=args.workers,
            partitions=AUTO_PARTITION_BUCKETS if args.partition_mode == "auto" else args.partitions,
            partition_mode=args.partition_mode,
            states=states,
            claim_batch_size=args.claim_batch_size,
            claim_partition_batch_size=args.claim_partition_batch_size,
            apply_inflight=args.apply_inflight,
            idle_sleep_ms=args.idle_sleep_ms,
            max_idle_sleep_ms=args.max_idle_sleep_ms,
            result_payload=result_payload,
            independent_many=args.independent_many,
            counters=counters,
            producers_done=producers_done,
            wake_coordinator=wake_coordinator,
            wake_coalesce_ms=args.wake_coalesce_ms,
            server_shards=args.server_shards,
        )

    started = time.perf_counter()
    create_started = started
    create_finished = started
    process_started = started
    process_finished = started

    if args.shape == "preloaded":
        create_started = time.perf_counter()
        create_results = await asyncio.gather(*(create_task(batch) for batch in create_ranges))
        create_finished = time.perf_counter()
        producers_done.set()
        process_started = time.perf_counter()
        worker_results = await asyncio.gather(*(worker_task(idx) for idx in range(args.workers)))
        process_finished = time.perf_counter()
    else:
        process_started = time.perf_counter()
        worker_tasks = [asyncio.create_task(worker_task(idx)) for idx in range(args.workers)]
        create_started = time.perf_counter()
        if args.producer_loop_thread:
            def run_create_thread() -> list[dict[str, int]]:
                async def run_all() -> list[dict[str, int]]:
                    return await asyncio.gather(*(create_task(batch) for batch in create_ranges))

                return asyncio.run(run_all())

            create_results = await asyncio.to_thread(run_create_thread)
        else:
            create_results = await asyncio.gather(*(create_task(batch) for batch in create_ranges))
        create_finished = time.perf_counter()
        producers_done.set()
        worker_results = await asyncio.gather(*worker_tasks)
        process_finished = time.perf_counter()

    created = sum(result["created"] for result in create_results)
    completed, claimed_actions = await counters.snapshot()
    claim_calls = sum(int(result["claim_calls"]) for result in worker_results)
    empty_claims = sum(int(result["empty_claims"]) for result in worker_results)
    max_claim_batch = max((int(result["max_claim_batch"]) for result in worker_results), default=0)
    wake_coalesce_sleeps = sum(int(result["wake_coalesce_sleeps"]) for result in worker_results)
    wake_coalesce_ms = sum(float(result["wake_coalesce_ms"]) for result in worker_results)
    create_seconds = create_finished - create_started
    process_seconds = process_finished - process_started
    total_seconds = process_finished - started

    return {
        "mode": "async-state-machine-workflow",
        "shape": args.shape,
        "flow_type": flow_type,
        "flows": args.flows,
        "steps": args.steps,
        "created": created,
        "completed": completed,
        "claimed_actions": claimed_actions,
        "expected_actions": completed * args.steps,
        "workers": args.workers,
        "producers": args.producers,
        "partitions": args.partitions,
        "partition_mode": args.partition_mode,
        "create_mode": args.create_mode,
        "create_batch_size": args.create_batch_size,
        "create_inflight": args.create_inflight,
        "producer_loop_thread": args.producer_loop_thread,
        "claim_batch_size": args.claim_batch_size,
        "claim_partition_batch_size": args.claim_partition_batch_size,
        "apply_inflight": args.apply_inflight,
        "worker_mode": worker_mode,
        "wake_coalesce_ms": args.wake_coalesce_ms,
        "wake_notifications": wake_coordinator.notifications if wake_coordinator is not None else 0,
        "wake_credits": wake_coordinator.notified_jobs if wake_coordinator is not None else 0,
        "wake_coalesce_sleeps": wake_coalesce_sleeps,
        "wake_coalesce_sleep_ms": wake_coalesce_ms,
        "independent_many": args.independent_many,
        "payload_bytes": args.payload_bytes,
        "result_bytes": args.result_bytes,
        "claim_calls": claim_calls,
        "empty_claims": empty_claims,
        "empty_claim_ratio": empty_claims / claim_calls if claim_calls > 0 else 0.0,
        "avg_claim_batch": claimed_actions / claim_calls if claim_calls > 0 else 0.0,
        "max_claim_batch": max_claim_batch,
        "create_seconds": create_seconds,
        "process_seconds": process_seconds,
        "total_seconds": total_seconds,
        "create_flows_per_sec": created / create_seconds if create_seconds > 0 else 0.0,
        "workflow_completions_per_sec": completed / process_seconds if process_seconds > 0 else 0.0,
        "state_actions_per_sec": claimed_actions / process_seconds if process_seconds > 0 else 0.0,
        "end_to_end_workflows_per_sec": completed / total_seconds if total_seconds > 0 else 0.0,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FerricFlow true-async state-machine workflow benchmark")
    parser.add_argument("--url", default="redis://127.0.0.1:7379")
    parser.add_argument("--shape", choices=("live", "preloaded"), default="live")
    parser.add_argument("--flows", type=int, default=10_000)
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--partition-mode", choices=("explicit", "auto"), default="auto")
    parser.add_argument("--create-mode", choices=("many", "pipeline", "single"), default="pipeline")
    parser.add_argument("--create-batch-size", type=int, default=500)
    parser.add_argument("--create-inflight", type=int, default=32)
    parser.add_argument("--producer-loop-thread", action="store_true")
    parser.add_argument("--claim-batch-size", type=int, default=500)
    parser.add_argument("--claim-partition-batch-size", type=int, default=32)
    parser.add_argument("--apply-inflight", type=int, default=0)
    parser.add_argument("--worker-mode", choices=("auto", "owner-wakeup", "polling"), default="auto")
    parser.add_argument("--wake-coalesce-ms", type=float, default=1.0)
    parser.add_argument("--idle-sleep-ms", type=float, default=1.0)
    parser.add_argument("--max-idle-sleep-ms", type=float, default=10.0)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--result-bytes", type=int, default=0)
    parser.add_argument("--server-shards", type=int, default=16)
    parser.add_argument("--independent-many", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def print_metrics(metrics: dict[str, Any]) -> None:
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"{key}: {value:.6f}")
        else:
            print(f"{key}: {value}")


def main() -> None:
    args = parse_args()
    metrics = asyncio.run(run_state_machine_throughput(args))
    print_metrics(metrics)


if __name__ == "__main__":
    main()
