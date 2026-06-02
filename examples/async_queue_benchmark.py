import argparse
import asyncio
import math
import time
import uuid
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ferricstore import AsyncFlowClient, ClaimedItem, CreateItem

FLOW_TYPE = "dbos_python_sdk_async_bench"
QUEUE_STATE = "queued"
AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 256
SERVER_SLOT_COUNT = 1024


def chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def now_ms() -> int:
    return int(time.time() * 1000)


def payload_bytes(size: int) -> bytes:
    if size <= 0:
        return b""
    return b"x" * size


def parse_claim_states(value: str | None) -> list[str] | None:
    if value is None:
        return None
    states = [part.strip() for part in value.split(",") if part.strip()]
    if not states:
        raise ValueError("--claim-states must contain at least one state")
    return states


def flow_id(run_id: str, index: int) -> str:
    return f"{run_id}:flow:{index}"


def explicit_partition_for(index: int, partitions: int, run_id: str) -> str:
    return f"{run_id}:partition:{index % max(partitions, 1)}"


def auto_partition_index_for_flow_id(id: str) -> int:
    return zlib.crc32(id.encode()) % AUTO_PARTITION_BUCKETS


def auto_partition_key_for_index(index: int) -> str:
    return f"{AUTO_PARTITION_PREFIX}{index % AUTO_PARTITION_BUCKETS}"


def partition_key_for_index(
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


@dataclass
class AsyncCounters:
    flows: int
    completed: int = 0
    claimed: int = 0

    def __post_init__(self) -> None:
        self.lock = asyncio.Lock()

    async def snapshot(self) -> tuple[int, int]:
        async with self.lock:
            return self.completed, self.claimed

    async def add(self, *, completed: int, claimed: int) -> None:
        async with self.lock:
            self.completed += completed
            self.claimed += claimed

    async def done(self) -> bool:
        async with self.lock:
            return self.completed >= self.flows

    async def all_claimed(self) -> bool:
        async with self.lock:
            return self.claimed >= self.flows


async def create_flows(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    indices: list[int],
    partitions: int,
    partition_mode: str,
    create_mode: str,
    create_batch_size: int,
    create_inflight: int,
    create_backpressure_credit: int,
    payload: bytes,
    independent_many: bool,
    wake_coordinator: object | None,
) -> dict[str, int]:
    client = AsyncFlowClient.from_url(url)
    created = 0
    pipeline_flushes = 0
    pipeline_commands = 0
    pipeline_max_depth = 0

    async def notify(indices_batch: list[int]) -> None:
        if wake_coordinator is None:
            return
        counts: dict[int, int] = {}
        for index in indices_batch:
            partition_index = partition_index_for_created_flow(
                partition_mode=partition_mode,
                run_id=run_id,
                index=index,
                partitions=partitions,
            )
            counts[partition_index] = counts.get(partition_index, 0) + 1
        for partition_index, count in counts.items():
            await wake_coordinator.notify_partition(partition_index, count)

    async def apply_backpressure() -> None:
        if create_backpressure_credit <= 0 or wake_coordinator is None:
            return
        while await wake_coordinator.total_credit() > create_backpressure_credit:
            await asyncio.sleep(0.001)

    try:
        if create_mode == "single":
            for index in indices:
                await client.enqueue(
                    flow_id(run_id, index),
                    type=flow_type,
                    state=QUEUE_STATE,
                    payload=payload,
                    partition_key=partition_key_for_index(
                        partition_mode=partition_mode,
                        index=index,
                        partitions=partitions,
                        run_id=run_id,
                    ),
                    return_record=False,
                )
                created += 1
                await notify([index])
                await apply_backpressure()
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
                        QUEUE_STATE,
                        "NOW",
                        ts,
                    ]
                    partition_key = partition_key_for_index(
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
                pipeline_flushes += 1
                pipeline_commands += len(batch)
                pipeline_max_depth = max(pipeline_max_depth, len(batch))
                await notify(batch)
                await apply_backpressure()
            return {
                "created": created,
                "create_pipeline_flushes": pipeline_flushes,
                "create_pipeline_commands": pipeline_commands,
                "create_pipeline_max_depth": pipeline_max_depth,
            }

        if partition_mode == "auto":
            auto_buffers: dict[int, list[int]] = {}
            pending: list[asyncio.Task[tuple[int, list[int]]]] = []
            create_inflight = max(create_inflight, 1)

            async def send_auto_batch(batch: list[int]) -> tuple[int, list[int]]:
                items = [CreateItem(flow_id(run_id, index), payload) for index in batch]
                await client.enqueue_many(
                    items,
                    type=flow_type,
                    state=QUEUE_STATE,
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
                    await apply_backpressure()

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
            pending = []
            items = [
                CreateItem(
                    flow_id(run_id, index),
                    payload,
                    explicit_partition_for(index, partitions, run_id),
                )
                for index in batch
            ]
            pending.append(
                asyncio.create_task(
                    client.enqueue_many(
                        items,
                        type=flow_type,
                        state=QUEUE_STATE,
                        independent=independent_many,
                    )
                )
            )
            for task in pending:
                await task
            created += len(items)
            await notify(batch)
            await apply_backpressure()
        return {"created": created}
    finally:
        await client.close()


async def run_worker(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    worker_index: int,
    workers: int,
    partitions: int,
    partition_mode: str,
    claim_any: bool,
    claim_batch_size: int,
    claim_partition_batch_size: int,
    idle_sleep_ms: float,
    max_idle_sleep_ms: float,
    claim_state: str | None,
    claim_states: list[str] | None,
    claim_priority: int | None,
    reclaim_expired: bool,
    reclaim_ratio: int,
    independent_many: bool,
    result: bytes | None,
    complete_inflight: int,
    counters: AsyncCounters,
    producers_done: asyncio.Event,
    wake_coordinator: object | None,
    wake_coalesce_ms: float,
    claim_block_ms: int | None,
    server_shards: int,
) -> dict[str, int | float]:
    client = AsyncFlowClient.from_url(url)
    local_completed = 0
    claimed_items = 0
    claim_calls = 0
    empty_claims = 0
    max_claim_batch = 0
    wake_coalesce_sleeps = 0
    wake_coalesce_seconds = 0.0
    idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
    max_idle_sleep_s = max(max_idle_sleep_ms, idle_sleep_ms, 0.0) / 1000.0
    current_sleep = idle_sleep_s
    wake_coalesce_s = max(wake_coalesce_ms, 0.0) / 1000.0
    fallback_idle_rounds = 0
    fallback_round = 0
    fallback_claims = 0
    last_claimed_seen = 0
    pending_completions: list[asyncio.Task[int]] = []
    complete_inflight = max(complete_inflight, 0)
    claim_partition_batch_size = max(claim_partition_batch_size, 1)

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
    if not owned_partitions and not claim_any:
        return {
            "completed": 0,
            "claimed_items": 0,
            "claim_calls": 0,
            "empty_claims": 0,
            "max_claim_batch": 0,
            "fallback_claims": 0,
            "wake_coalesce_sleeps": 0,
            "wake_coalesce_ms": 0.0,
        }

    same_group = None
    if partition_mode == "auto":

        def same_group(first: int, candidate: int) -> bool:
            return auto_partition_server_shard_for_index(
                first, server_shards
            ) == auto_partition_server_shard_for_index(candidate, server_shards)

    async def drain_completions(*, block: bool = False, limit: int | None = None) -> int:
        drained_count = 0
        while pending_completions and (limit is None or drained_count < limit):
            if block:
                done, pending_set = await asyncio.wait(
                    pending_completions,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            else:
                done = {task for task in pending_completions if task.done()}
                if not done:
                    break
                pending_set = set(pending_completions) - done
            pending_completions[:] = list(pending_set)
            for task in done:
                count = task.result()
                await counters.add(completed=count, claimed=0)
                drained_count += 1
        return drained_count

    async def complete_batch(jobs: list[ClaimedItem]) -> int:
        await client.complete_jobs(
            jobs,
            result=result,
            independent=independent_many,
        )
        return len(jobs)

    async def claim_and_complete(partition_indices: list[int], limit: int) -> int:
        nonlocal claim_calls, empty_claims, claimed_items, max_claim_batch, local_completed
        await drain_completions(block=False)
        if await counters.done():
            return 0
        partition_keys = [
            claim_partition_key(
                partition_mode=partition_mode,
                partition_index=index,
                partitions=partitions,
                run_id=run_id,
            )
            for index in partition_indices
        ]
        partition_key = partition_keys[0] if len(partition_keys) == 1 else None
        multi_partition_keys = None if partition_key is not None else partition_keys
        claim_calls += 1
        jobs = await client.claim_jobs(
            flow_type,
            state=claim_state,
            states=claim_states,
            worker=f"{run_id}:worker:{worker_index}",
            partition_key=None if claim_any else partition_key,
            partition_keys=None if claim_any else multi_partition_keys,
            limit=limit,
            priority=claim_priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            block_ms=claim_block_ms,
        )
        if not jobs:
            empty_claims += 1
            return 0
        batch_size = len(jobs)
        claimed_items += batch_size
        local_completed += batch_size
        max_claim_batch = max(max_claim_batch, batch_size)
        await counters.add(completed=0, claimed=batch_size)
        if complete_inflight > 0:
            pending_completions.append(asyncio.create_task(complete_batch(jobs)))
            while len(pending_completions) >= complete_inflight:
                await drain_completions(block=True, limit=1)
        else:
            await complete_batch(jobs)
            await counters.add(completed=batch_size, claimed=0)
        return batch_size

    try:
        while True:
            await drain_completions(block=False)
            if await counters.done():
                break
            if await counters.all_claimed():
                if pending_completions:
                    await drain_completions(block=True, limit=1)
                else:
                    await asyncio.sleep(current_sleep)
                continue

            partition_indices: list[int]
            claim_credit = claim_batch_size
            if claim_any:
                partition_indices = []
            elif wake_coordinator is not None:
                partition_indices, credit = await wake_coordinator.next_partitions(
                    worker_index,
                    timeout_s=current_sleep,
                    max_partitions=claim_partition_batch_size,
                    max_credit=claim_batch_size,
                    same_group=same_group,
                )
                if not partition_indices:
                    if producers_done.is_set():
                        completed, claimed = await counters.snapshot()
                        if completed >= counters.flows:
                            break
                        if claimed >= counters.flows:
                            if pending_completions:
                                await drain_completions(block=True, limit=1)
                            else:
                                await asyncio.sleep(current_sleep)
                            continue
                        if (
                            await wake_coordinator.total_credit() > 0
                            or claimed != last_claimed_seen
                        ):
                            last_claimed_seen = claimed
                            fallback_idle_rounds = 0
                            continue
                        fallback_idle_rounds += 1
                        if fallback_idle_rounds < 3:
                            continue
                        partition_indices = [
                            owned_partitions[fallback_round % len(owned_partitions)]
                        ]
                        fallback_round += 1
                        fallback_claims += 1
                    else:
                        continue
                else:
                    fallback_idle_rounds = 0
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
                        extra_partitions, extra_credit = await wake_coordinator.next_partitions(
                            worker_index,
                            timeout_s=0,
                            max_partitions=max(
                                claim_partition_batch_size - len(partition_indices),
                                0,
                            ),
                            max_credit=max(claim_batch_size - credit, 0),
                            same_group=same_group,
                        )
                        partition_indices.extend(extra_partitions)
                        credit += extra_credit
                        claim_credit = max(credit, 1)
            else:
                partition_indices = []
                for _ in range(min(claim_partition_batch_size, len(owned_partitions))):
                    partition_indices.append(
                        owned_partitions[fallback_round % len(owned_partitions)]
                    )
                    fallback_round += 1

            remaining_credit = max(claim_credit, 1)
            total_completed = 0
            while remaining_credit > 0 and not await counters.done():
                claim_limit = min(claim_batch_size, remaining_credit)
                completed_count = await claim_and_complete(partition_indices, claim_limit)
                if completed_count <= 0:
                    break
                total_completed += completed_count
                remaining_credit -= completed_count
                if completed_count < claim_limit:
                    break

            if total_completed > 0:
                current_sleep = idle_sleep_s
                continue

            if producers_done.is_set():
                completed, claimed = await counters.snapshot()
                if completed >= counters.flows:
                    break
            if wake_coordinator is None and current_sleep > 0:
                await asyncio.sleep(current_sleep)
                current_sleep = min(max_idle_sleep_s, max(current_sleep * 2, idle_sleep_s))

        while pending_completions:
            await drain_completions(block=True)

        return {
            "completed": local_completed,
            "claimed_items": claimed_items,
            "claim_calls": claim_calls,
            "empty_claims": empty_claims,
            "max_claim_batch": max_claim_batch,
            "fallback_claims": fallback_claims,
            "wake_coalesce_sleeps": wake_coalesce_sleeps,
            "wake_coalesce_ms": wake_coalesce_seconds * 1000.0,
        }
    finally:
        await client.close()


async def run_queued_throughput(args: argparse.Namespace) -> dict[str, Any]:
    run_id = f"py-async-queue-bench-{uuid.uuid4().hex}"
    flow_type = f"{FLOW_TYPE}:{run_id}"
    partition_mode = (
        "auto" if args.claim_any and args.partition_mode == "explicit" else args.partition_mode
    )
    worker_partitions = AUTO_PARTITION_BUCKETS if partition_mode == "auto" else args.partitions
    payload = payload_bytes(args.payload_bytes)
    result = payload_bytes(args.result_bytes) if args.result_bytes > 0 else None
    claim_states = parse_claim_states(args.claim_states)
    claim_state = (
        None if claim_states is not None or args.claim_state == "omitted" else args.claim_state
    )
    counters = AsyncCounters(args.flows)
    producers_done = asyncio.Event()
    wake_coordinator = None

    indices = list(range(args.flows))
    if partition_mode == "auto" and args.create_mode == "many":
        create_ranges = [[] for _ in range(args.producers)]
        for index in indices:
            owner = auto_partition_index_for_flow_id(flow_id(run_id, index)) % args.producers
            create_ranges[owner].append(index)
    else:
        create_ranges = [indices[offset :: args.producers] for offset in range(args.producers)]

    async def create_task(
        batch: list[int],
        wake_source: object | None,
    ) -> dict[str, int]:
        return await create_flows(
            url=args.url,
            run_id=run_id,
            flow_type=flow_type,
            indices=batch,
            partitions=args.partitions,
            partition_mode=partition_mode,
            create_mode=args.create_mode,
            create_batch_size=args.create_batch_size,
            create_inflight=args.create_inflight,
            create_backpressure_credit=args.create_backpressure_credit,
            payload=payload,
            independent_many=args.independent_many,
            wake_coordinator=wake_source,
        )

    async def worker_task(worker_index: int) -> dict[str, int | float]:
        return await run_worker(
            url=args.url,
            run_id=run_id,
            flow_type=flow_type,
            worker_index=worker_index,
            workers=args.workers,
            partitions=worker_partitions,
            partition_mode=partition_mode,
            claim_any=args.claim_any,
            claim_batch_size=args.claim_batch_size,
            claim_partition_batch_size=args.claim_partition_batch_size,
            idle_sleep_ms=args.idle_sleep_ms,
            max_idle_sleep_ms=args.max_idle_sleep_ms,
            claim_state=claim_state,
            claim_states=claim_states,
            claim_priority=args.claim_priority,
            reclaim_expired=args.reclaim_expired,
            reclaim_ratio=args.reclaim_ratio,
            independent_many=args.independent_many,
            result=result,
            complete_inflight=args.complete_inflight,
            counters=counters,
            producers_done=producers_done,
            wake_coordinator=wake_coordinator,
            wake_coalesce_ms=args.wake_coalesce_ms,
            claim_block_ms=args.claim_block_ms,
            server_shards=args.server_shards,
        )

    started = time.perf_counter()
    create_started = started
    create_finished = started
    process_started = started
    process_finished = started

    if args.shape == "preloaded":
        create_started = time.perf_counter()
        create_results = await asyncio.gather(
            *(create_task(batch, wake_coordinator) for batch in create_ranges)
        )
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
                    return await asyncio.gather(
                        *(create_task(batch, wake_coordinator) for batch in create_ranges)
                    )

                return asyncio.run(run_all())

            create_results = await asyncio.to_thread(run_create_thread)
        else:
            create_results = await asyncio.gather(
                *(create_task(batch, wake_coordinator) for batch in create_ranges)
            )
        create_finished = time.perf_counter()
        producers_done.set()
        worker_results = await asyncio.gather(*worker_tasks)
        process_finished = time.perf_counter()

    created = sum(int(result.get("created", 0)) for result in create_results)
    completed, claimed_total = await counters.snapshot()
    claim_calls = sum(int(result["claim_calls"]) for result in worker_results)
    empty_claims = sum(int(result["empty_claims"]) for result in worker_results)
    max_claim_batch = max((int(result["max_claim_batch"]) for result in worker_results), default=0)
    fallback_claims = sum(int(result["fallback_claims"]) for result in worker_results)
    wake_coalesce_sleeps = sum(int(result["wake_coalesce_sleeps"]) for result in worker_results)
    wake_coalesce_ms = sum(float(result["wake_coalesce_ms"]) for result in worker_results)
    create_pipeline_flushes = sum(
        int(result.get("create_pipeline_flushes", 0)) for result in create_results
    )
    create_pipeline_commands = sum(
        int(result.get("create_pipeline_commands", 0)) for result in create_results
    )
    create_pipeline_max_depth = max(
        (int(result.get("create_pipeline_max_depth", 0)) for result in create_results), default=0
    )
    create_seconds = create_finished - create_started
    process_seconds = process_finished - process_started
    total_seconds = process_finished - started

    return {
        "mode": "async-queued",
        "shape": args.shape,
        "flow_type": flow_type,
        "flows": args.flows,
        "created": created,
        "completed": completed,
        "claimed_items": claimed_total,
        "workers": args.workers,
        "producers": args.producers,
        "partitions": args.partitions,
        "partition_mode": partition_mode,
        "worker_partitions": worker_partitions,
        "worker_mode": args.worker_mode,
        "create_mode": args.create_mode,
        "create_batch_size": args.create_batch_size,
        "create_inflight": args.create_inflight,
        "create_backpressure_credit": args.create_backpressure_credit,
        "producer_loop_thread": args.producer_loop_thread,
        "claim_batch_size": args.claim_batch_size,
        "claim_partition_batch_size": args.claim_partition_batch_size,
        "complete_inflight": args.complete_inflight,
        "claim_any": args.claim_any,
        "claim_state": claim_state or "omitted",
        "claim_states": ",".join(claim_states) if claim_states else "",
        "claim_priority": args.claim_priority,
        "reclaim_expired": args.reclaim_expired,
        "reclaim_ratio": args.reclaim_ratio,
        "independent_many": args.independent_many,
        "payload_bytes": args.payload_bytes,
        "result_bytes": args.result_bytes,
        "claim_calls": claim_calls,
        "empty_claims": empty_claims,
        "empty_claim_ratio": empty_claims / claim_calls if claim_calls > 0 else 0.0,
        "avg_claim_batch": claimed_total / claim_calls if claim_calls > 0 else 0.0,
        "max_claim_batch": max_claim_batch,
        "fallback_claims": fallback_claims,
        "wake_notifications": wake_coordinator.notifications if wake_coordinator is not None else 0,
        "wake_credits": wake_coordinator.notified_jobs if wake_coordinator is not None else 0,
        "wake_coalesce_sleeps": wake_coalesce_sleeps,
        "wake_coalesce_ms": wake_coalesce_ms,
        "create_pipeline_flushes": create_pipeline_flushes,
        "create_pipeline_commands": create_pipeline_commands,
        "create_pipeline_max_depth": create_pipeline_max_depth,
        "create_seconds": create_seconds,
        "process_seconds": process_seconds,
        "total_seconds": total_seconds,
        "create_flows_per_sec": created / create_seconds if create_seconds > 0 else 0.0,
        "process_flows_per_sec": completed / process_seconds if process_seconds > 0 else 0.0,
        "end_to_end_flows_per_sec": completed / total_seconds if total_seconds > 0 else 0.0,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FerricFlow true-async queue throughput benchmark")
    parser.add_argument("--url", default="redis://127.0.0.1:7379")
    parser.add_argument("--shape", choices=("live", "preloaded"), default="live")
    parser.add_argument("--flows", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--partition-mode", choices=("explicit", "auto"), default="auto")
    parser.add_argument("--create-mode", choices=("many", "pipeline", "single"), default="pipeline")
    parser.add_argument("--create-batch-size", type=int, default=500)
    parser.add_argument("--create-inflight", type=int, default=32)
    parser.add_argument("--create-backpressure-credit", type=int, default=0)
    parser.add_argument("--producer-loop-thread", action="store_true")
    parser.add_argument("--claim-batch-size", type=int, default=500)
    parser.add_argument("--claim-partition-batch-size", type=int, default=32)
    parser.add_argument("--complete-inflight", type=int, default=32)
    parser.add_argument("--claim-any", action="store_true")
    parser.add_argument("--claim-state", default=QUEUE_STATE)
    parser.add_argument("--claim-states")
    parser.add_argument("--claim-priority", type=int, default=0)
    parser.add_argument("--reclaim-expired", action="store_true")
    parser.add_argument("--reclaim-ratio", type=int, default=4)
    parser.add_argument("--worker-mode", choices=("blocking", "polling"), default="blocking")
    parser.add_argument("--wake-coalesce-ms", type=float, default=1.0)
    parser.add_argument("--claim-block-ms", type=int, default=-1)
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
    if args.claim_block_ms < 0:
        args.claim_block_ms = None
    metrics = asyncio.run(run_queued_throughput(args))
    print_metrics(metrics)


if __name__ == "__main__":
    main()
