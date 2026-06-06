import argparse
import asyncio
import contextlib
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
ANY_STATE_WAKE_BUCKET = "__any__"


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


def partition_index_from_claimed_key(
    partition_key: str, partition_mode: str, partitions: int
) -> int | None:
    if partition_mode == "auto":
        return auto_partition_index_from_key(partition_key)
    marker = ":partition:"
    if marker not in partition_key:
        return None
    try:
        return int(partition_key.rsplit(marker, 1)[1]) % max(partitions, 1)
    except ValueError:
        return None


def claim_states_for_mode(claim_states_mode: str, states: list[str]) -> list[str] | None:
    return states if claim_states_mode == "all" else None


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


class ProgressCounters:
    def __init__(self) -> None:
        self.created = 0
        self.claim_calls = 0
        self.empty_claims = 0
        self.max_claim_batch = 0
        self.lock = asyncio.Lock()

    async def add_created(self, count: int) -> None:
        async with self.lock:
            self.created += count

    async def add_claim_result(self, claimed: int) -> None:
        async with self.lock:
            self.claim_calls += 1
            if claimed == 0:
                self.empty_claims += 1
            else:
                self.max_claim_batch = max(self.max_claim_batch, claimed)

    async def snapshot(self) -> tuple[int, int, int, int]:
        async with self.lock:
            return (
                self.created,
                self.claim_calls,
                self.empty_claims,
                self.max_claim_batch,
            )


async def progress_logger(
    *,
    interval_s: float,
    progress: ProgressCounters,
    counters: AsyncCounters,
    stop: asyncio.Event,
    started: float,
) -> None:
    interval_s = max(interval_s, 0.1)
    while not stop.is_set():
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        created, claim_calls, empty_claims, max_claim_batch = await progress.snapshot()
        completed, claimed_actions = await counters.snapshot()
        elapsed_s = time.perf_counter() - started
        avg_claim_batch = claimed_actions / claim_calls if claim_calls > 0 else 0.0
        print(
            "progress "
            f"elapsed_s={elapsed_s:.1f} created={created} completed={completed} "
            f"claimed_actions={claimed_actions} claim_calls={claim_calls} "
            f"empty_claims={empty_claims} avg_claim_batch={avg_claim_batch:.2f} "
            f"max_claim_batch={max_claim_batch}",
            flush=True,
        )


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
    create_rate_per_sec: float,
    create_mode: str,
    independent_many: bool,
    retention_ttl_ms: int,
    run_at_delay_ms: int,
    create_now_ms: int | None,
    wake_coordinator: object | None,
    progress: ProgressCounters | None,
) -> dict[str, int]:
    client = AsyncFlowClient.from_url(url, max_connections=max(create_inflight, 1))
    created = 0
    scheduled = 0
    started = time.perf_counter()

    async def throttle(count: int) -> None:
        nonlocal scheduled
        if create_rate_per_sec <= 0:
            return
        scheduled += count
        target_elapsed = scheduled / create_rate_per_sec
        actual_elapsed = time.perf_counter() - started
        if target_elapsed > actual_elapsed:
            await asyncio.sleep(target_elapsed - actual_elapsed)

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

    def create_command_now_ms() -> int:
        return create_now_ms if create_now_ms is not None else now_ms()

    def create_run_at_ms(command_now_ms: int | None = None) -> int:
        base_now_ms = command_now_ms if command_now_ms is not None else create_command_now_ms()
        return base_now_ms + max(run_at_delay_ms, 0)

    try:
        if create_mode == "single":
            for index in indices:
                command_now_ms = create_command_now_ms()
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
                    run_at_ms=create_run_at_ms(command_now_ms),
                    now_ms=command_now_ms,
                    return_record=False,
                    retention_ttl_ms=retention_ttl_ms if retention_ttl_ms > 0 else None,
                )
                created += 1
                if progress is not None:
                    await progress.add_created(1)
                await notify([index])
                await throttle(1)
            return {"created": created}

        if create_mode == "pipeline":
            pending: list[asyncio.Task[tuple[int, list[int]]]] = []
            create_inflight = max(create_inflight, 1)

            async def send_pipeline_batch(batch: list[int]) -> tuple[int, list[int]]:
                pipe = client.pipeline()
                ts = create_command_now_ms()
                run_at_ms = create_run_at_ms(ts)
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
                    if retention_ttl_ms > 0:
                        args.extend(["RETENTION_TTL_MS", retention_ttl_ms])
                    args.extend(["RUN_AT", run_at_ms, "PRIORITY", 0])
                    pipe.command(*args)
                await pipe.execute()
                return len(batch), batch

            async def drain_one() -> None:
                nonlocal created
                done, pending_set = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                pending[:] = list(pending_set)
                for task in done:
                    count, batch = task.result()
                    created += count
                    if progress is not None:
                        await progress.add_created(count)
                    await notify(batch)

            for batch in chunks(indices, max(create_batch_size, 1)):
                await throttle(len(batch))
                pending.append(asyncio.create_task(send_pipeline_batch(batch)))
                if len(pending) >= create_inflight:
                    await drain_one()
            while pending:
                await drain_one()
            return {"created": created}

        if partition_mode == "auto":
            auto_buffers: dict[int, list[int]] = {}
            pending: list[asyncio.Task[tuple[int, list[int]]]] = []
            create_inflight = max(create_inflight, 1)

            async def send_auto_batch(batch: list[int]) -> tuple[int, list[int]]:
                items = [CreateItem(flow_id(run_id, index), payload) for index in batch]
                command_now_ms = create_command_now_ms()
                await client.enqueue_many(
                    items,
                    type=flow_type,
                    state=DEFAULT_STATE,
                    run_at_ms=create_run_at_ms(command_now_ms),
                    now_ms=command_now_ms,
                    independent=independent_many,
                    retention_ttl_ms=retention_ttl_ms if retention_ttl_ms > 0 else None,
                )
                return len(items), batch

            async def drain_one() -> None:
                nonlocal created
                done, pending_set = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                pending[:] = list(pending_set)
                for task in done:
                    count, batch = task.result()
                    created += count
                    if progress is not None:
                        await progress.add_created(count)
                    await notify(batch)

            async def flush_auto_bucket(partition_index: int) -> None:
                batch = auto_buffers.get(partition_index)
                if not batch:
                    return
                await throttle(len(batch))
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
            await throttle(len(batch))
            command_now_ms = create_command_now_ms()
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
                run_at_ms=create_run_at_ms(command_now_ms),
                now_ms=command_now_ms,
                independent=independent_many,
                retention_ttl_ms=retention_ttl_ms if retention_ttl_ms > 0 else None,
            )
            created += len(items)
            if progress is not None:
                await progress.add_created(len(items))
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
    transition_payload: bytes | None,
    terminal_payload: bytes | None,
    terminal_mode: str,
    result_payload: bytes | None,
    claim_states_mode: str,
    reclaim_expired: bool,
    independent_many: bool,
    counters: AsyncCounters,
    producers_done: asyncio.Event,
    wake_coordinator: object | None,
    wake_coalesce_ms: float,
    claim_block_ms: int | None,
    server_shards: int,
    claim_now_ms: int | None,
    progress: ProgressCounters | None,
) -> dict[str, int | float]:
    client = AsyncFlowClient.from_url(url, max_connections=max(apply_inflight + 1, 2))
    claim_client = AsyncFlowClient.from_url(url, max_connections=1)
    partition_count = AUTO_PARTITION_BUCKETS if partition_mode == "auto" else partitions

    def owner_for_partition(partition_index: int) -> int:
        if partition_mode == "auto":
            return auto_partition_server_shard_for_index(partition_index, server_shards) % workers
        return partition_index % workers

    owned_partitions = [p for p in range(partition_count) if owner_for_partition(p) == worker_index]
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
        state_name: states[idx + 1] for idx, state_name in enumerate(states[:-1])
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

        def same_group(first: int, candidate: int) -> bool:
            return auto_partition_server_shard_for_index(
                first, server_shards
            ) == auto_partition_server_shard_for_index(candidate, server_shards)

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
            if terminal_mode == "fail":
                await client.fail_many(
                    None,
                    jobs,
                    error=result_payload,
                    payload=terminal_payload,
                    independent=independent_many,
                )
            else:
                await client.complete_jobs(
                    jobs,
                    result=result_payload,
                    payload=terminal_payload,
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
            payload=transition_payload,
            independent=independent_many,
        )
        return 0, batch_size, next_state, jobs

    async def apply_claimed_jobs(
        state_name: str,
        jobs: list[ClaimedItem],
    ) -> tuple[int, int, str | None, list[ClaimedItem]]:
        if claim_states_mode == "cursor":
            return await apply_jobs(state_name, jobs)

        completed_count = 0
        claimed_count = 0
        next_jobs: list[ClaimedItem] = []
        next_state: str | None = None
        grouped: dict[str, list[ClaimedItem]] = {}
        for job in jobs:
            grouped.setdefault(job.run_state or job.state, []).append(job)

        for claimed_state, claimed_jobs in grouped.items():
            group_completed, group_claimed, group_next_state, group_jobs = await apply_jobs(
                claimed_state,
                claimed_jobs,
            )
            completed_count += group_completed
            claimed_count += group_claimed
            if group_next_state is not None:
                next_state = group_next_state
                next_jobs.extend(group_jobs)

        return completed_count, claimed_count, next_state, next_jobs

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
                completed_count, _claimed_count, next_state, jobs = task.result()
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
            if claim_states_mode == "cursor":
                state_cursor = (state_cursor + 1) % len(states)
            claim_credit = claim_batch_size
            if wake_coordinator is not None:
                if claim_states_mode in ("all", "any"):
                    selected_partitions, credit = await wake_coordinator.next_ready_any(
                        worker_index=worker_index,
                        timeout_s=current_sleep,
                        max_partitions=claim_partition_batch_size,
                        max_credit=claim_batch_size,
                        same_group=same_group,
                    )
                else:
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
                        selected_partitions = [
                            owned_partitions[partition_cursor % len(owned_partitions)]
                        ]
                        partition_cursor += 1
                    else:
                        continue
                else:
                    wake_idle_rounds = 0
                    claim_credit = max(credit, 1)
                    if (
                        wake_coalesce_s > 0
                        and credit < claim_batch_size
                        and not producers_done.is_set()
                    ):
                        sleep_s = min(wake_coalesce_s, 0.002)
                        await asyncio.sleep(sleep_s)
                        wake_coalesce_sleeps += 1
                        wake_coalesce_seconds += sleep_s
                        if claim_states_mode == "cursor":
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
                        else:
                            extra_partitions, extra_credit = await wake_coordinator.next_ready_any(
                                worker_index=worker_index,
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
            else:
                selected_partitions = []
                for _ in range(min(claim_partition_batch_size, len(owned_partitions))):
                    selected_partitions.append(
                        owned_partitions[partition_cursor % len(owned_partitions)]
                    )
                    partition_cursor += 1

            partition_key, partition_keys = partition_keys_from_indices(selected_partitions)
            remaining_credit = max(claim_credit, 1)
            total_claimed_this_round = 0
            while remaining_credit > 0 and not await counters.done():
                claim_limit = min(claim_batch_size, remaining_credit)
                command_now_ms = claim_now_ms
                claim_calls += 1
                if claim_states_mode in ("all", "any"):
                    jobs = await claim_client.claim_jobs(
                        flow_type,
                        states=claim_states_for_mode(claim_states_mode, states),
                        worker=f"{run_id}:worker:{worker_index}",
                        partition_key=partition_key,
                        partition_keys=partition_keys,
                        limit=claim_limit,
                        priority=0,
                        now_ms=command_now_ms,
                        reclaim_expired=reclaim_expired,
                        block_ms=claim_block_ms,
                        include_state=True,
                    )
                else:
                    jobs = await claim_client.claim_jobs(
                        flow_type,
                        state=state_name,
                        worker=f"{run_id}:worker:{worker_index}",
                        partition_key=partition_key,
                        partition_keys=partition_keys,
                        limit=claim_limit,
                        priority=0,
                        now_ms=command_now_ms,
                        reclaim_expired=reclaim_expired,
                        block_ms=claim_block_ms,
                    )
                if progress is not None:
                    await progress.add_claim_result(len(jobs))
                if not jobs:
                    empty_claims += 1
                    break

                current_sleep = idle_sleep_s
                batch_size = len(jobs)
                total_claimed_this_round += batch_size
                claimed_actions += batch_size
                max_claim_batch = max(max_claim_batch, batch_size)
                await counters.add(completed=0, claimed_actions=batch_size)
                all_actions_claimed = await counters.all_actions_claimed(expected_actions)

                if apply_inflight > 0:
                    pending_applies.append(
                        asyncio.create_task(apply_claimed_jobs(state_name, jobs))
                    )
                    while len(pending_applies) >= apply_inflight:
                        await drain_applies(block=True, limit=1)
                else:
                    (
                        completed_count,
                        _claimed_count,
                        next_state,
                        applied_jobs,
                    ) = await apply_claimed_jobs(
                        state_name,
                        jobs,
                    )
                    completed_workflows += completed_count
                    await counters.add(completed=completed_count, claimed_actions=0)
                    if next_state is not None:
                        await notify_next_state(next_state, applied_jobs)

                remaining_credit -= batch_size
                if all_actions_claimed or batch_size < claim_limit:
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
        await claim_client.close()
        await client.close()


async def run_state_machine_throughput(args: argparse.Namespace) -> dict[str, Any]:
    run_id = f"py-async-sm-bench-{uuid.uuid4().hex}"
    flow_type = f"async_state_machine_bench:{run_id}"
    states = workflow_states(args.steps)
    indices = list(range(args.flows))
    payload = payload_bytes(args.payload_bytes)
    transition_payload = (
        payload_bytes(args.transition_payload_bytes) if args.transition_payload_bytes > 0 else None
    )
    terminal_payload = (
        payload_bytes(args.terminal_payload_bytes) if args.terminal_payload_bytes > 0 else None
    )
    result_payload = payload_bytes(args.result_bytes) if args.result_bytes > 0 else None
    counters = AsyncCounters(args.flows)
    producers_done = asyncio.Event()
    worker_mode = "blocking" if args.worker_mode == "auto" else args.worker_mode
    claim_block_ms = effective_claim_block_ms(worker_mode, args.claim_block_ms)
    wake_coordinator = None
    progress = ProgressCounters() if args.progress_interval_s > 0 else None
    progress_stop = asyncio.Event()

    if args.partition_mode == "auto" and args.create_mode == "many":
        create_ranges = [[] for _ in range(args.producers)]
        for index in indices:
            owner = auto_partition_index_for_flow_id(flow_id(run_id, index)) % args.producers
            create_ranges[owner].append(index)
    else:
        create_ranges = [indices[offset :: args.producers] for offset in range(args.producers)]

    async def create_task(batch: list[int]) -> dict[str, int]:
        create_task_count = max(len(create_ranges), 1)

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
            create_rate_per_sec=args.create_rate_per_sec / create_task_count,
            create_mode=args.create_mode,
            independent_many=args.independent_many,
            retention_ttl_ms=args.retention_ttl_ms,
            run_at_delay_ms=args.run_at_delay_ms,
            create_now_ms=args.create_now_ms,
            wake_coordinator=wake_coordinator,
            progress=progress,
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
            transition_payload=transition_payload,
            terminal_payload=terminal_payload,
            terminal_mode=args.terminal_mode,
            result_payload=result_payload,
            claim_states_mode=args.claim_states_mode,
            reclaim_expired=args.reclaim_expired,
            independent_many=args.independent_many,
            counters=counters,
            producers_done=producers_done,
            wake_coordinator=wake_coordinator,
            wake_coalesce_ms=args.wake_coalesce_ms,
            claim_block_ms=claim_block_ms,
            server_shards=args.server_shards,
            claim_now_ms=args.claim_now_ms,
            progress=progress,
        )

    started = time.perf_counter()
    create_started = started
    create_finished = started
    process_started = started
    process_finished = started

    progress_task = (
        asyncio.create_task(
            progress_logger(
                interval_s=args.progress_interval_s,
                progress=progress,
                counters=counters,
                stop=progress_stop,
                started=started,
            )
        )
        if progress is not None
        else None
    )
    if progress is not None:
        print(
            "benchmark_run "
            f"run_id={run_id} flow_type={flow_type} shape={args.shape} "
            f"partition_mode={args.partition_mode} create_mode={args.create_mode} "
            f"claim_states_mode={args.claim_states_mode} worker_mode={worker_mode} "
            f"run_at_delay_ms={args.run_at_delay_ms}",
            flush=True,
        )

    try:
        if args.shape == "preloaded":
            create_started = time.perf_counter()
            create_results = await asyncio.gather(*(create_task(batch) for batch in create_ranges))
            create_finished = time.perf_counter()
            producers_done.set()
            process_started = time.perf_counter()
            worker_results = await asyncio.gather(
                *(worker_task(idx) for idx in range(args.workers))
            )
            process_finished = time.perf_counter()
        else:
            process_started = time.perf_counter()
            worker_tasks = [asyncio.create_task(worker_task(idx)) for idx in range(args.workers)]
            create_started = time.perf_counter()
            if args.producer_loop_thread:

                def run_create_thread() -> list[dict[str, int]]:
                    async def run_all() -> list[dict[str, int]]:
                        return await asyncio.gather(
                            *(create_task(batch) for batch in create_ranges)
                        )

                    return asyncio.run(run_all())

                create_results = await asyncio.to_thread(run_create_thread)
            else:
                create_results = await asyncio.gather(
                    *(create_task(batch) for batch in create_ranges)
                )
            create_finished = time.perf_counter()
            producers_done.set()
            worker_results = await asyncio.gather(*worker_tasks)
            process_finished = time.perf_counter()
    finally:
        if progress_task is not None:
            progress_stop.set()
            await progress_task

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
        "create_rate_per_sec": args.create_rate_per_sec,
        "producer_loop_thread": args.producer_loop_thread,
        "claim_batch_size": args.claim_batch_size,
        "claim_partition_batch_size": args.claim_partition_batch_size,
        "apply_inflight": args.apply_inflight,
        "worker_mode": worker_mode,
        "claim_block_ms": claim_block_ms,
        "claim_states_mode": args.claim_states_mode,
        "reclaim_expired": args.reclaim_expired,
        "wake_coalesce_ms": args.wake_coalesce_ms,
        "wake_notifications": wake_coordinator.notifications if wake_coordinator is not None else 0,
        "wake_credits": wake_coordinator.notified_jobs if wake_coordinator is not None else 0,
        "wake_coalesce_sleeps": wake_coalesce_sleeps,
        "wake_coalesce_sleep_ms": wake_coalesce_ms,
        "independent_many": args.independent_many,
        "payload_bytes": args.payload_bytes,
        "transition_payload_bytes": args.transition_payload_bytes,
        "terminal_payload_bytes": args.terminal_payload_bytes,
        "retention_ttl_ms": args.retention_ttl_ms,
        "run_at_delay_ms": args.run_at_delay_ms,
        "create_now_ms": args.create_now_ms,
        "claim_now_ms": args.claim_now_ms,
        "terminal_mode": args.terminal_mode,
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


def effective_claim_block_ms(worker_mode: str, configured_block_ms: int | None) -> int | None:
    if worker_mode != "blocking":
        return None
    if configured_block_ms is None or configured_block_ms < 0:
        return None
    return configured_block_ms


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FerricFlow true-async state-machine workflow benchmark"
    )
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
    parser.add_argument("--create-rate-per-sec", type=float, default=0.0)
    parser.add_argument("--producer-loop-thread", action="store_true")
    parser.add_argument("--claim-batch-size", type=int, default=500)
    parser.add_argument("--claim-partition-batch-size", type=int, default=32)
    parser.add_argument("--apply-inflight", type=int, default=0)
    parser.add_argument("--worker-mode", choices=("auto", "blocking", "polling"), default="auto")
    parser.add_argument("--wake-coalesce-ms", type=float, default=1.0)
    parser.add_argument("--claim-block-ms", type=int, default=-1)
    parser.add_argument("--idle-sleep-ms", type=float, default=1.0)
    parser.add_argument("--max-idle-sleep-ms", type=float, default=10.0)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--transition-payload-bytes", type=int, default=0)
    parser.add_argument("--terminal-payload-bytes", type=int, default=0)
    parser.add_argument("--retention-ttl-ms", type=int, default=0)
    parser.add_argument("--run-at-delay-ms", type=int, default=0)
    parser.add_argument("--create-now-ms", type=int, default=None)
    parser.add_argument("--claim-now-ms", type=int, default=None)
    parser.add_argument("--terminal-mode", choices=("complete", "fail"), default="complete")
    parser.add_argument("--claim-states-mode", choices=("cursor", "all", "any"), default="cursor")
    parser.add_argument("--reclaim-expired", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--result-bytes", type=int, default=0)
    parser.add_argument("--server-shards", type=int, default=16)
    parser.add_argument("--independent-many", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--progress-interval-s", type=float, default=0.0)
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
