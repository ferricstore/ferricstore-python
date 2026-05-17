import argparse
import math
import queue
import threading
import time
import uuid
import zlib
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from ferricstore import ClaimedItem, CreateItem, FlowClient


FLOW_TYPE = "dbos_python_sdk_bench"
QUEUE_STATE = "queued"
AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 16


def chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def payload_bytes(size: int) -> bytes:
    if size <= 0:
        return b""
    return b"x" * size


def partition_for(index: int, partitions: int, prefix: str) -> str:
    return f"{prefix}:partition:{index % max(partitions, 1)}"


def auto_partition_index_for_flow_id(id: str) -> int:
    return zlib.crc32(id.encode()) % AUTO_PARTITION_BUCKETS


def auto_partition_key_for_index(index: int) -> str:
    return f"{AUTO_PARTITION_PREFIX}{index % AUTO_PARTITION_BUCKETS}"


def benchmark_partition_key(
    *,
    partition_mode: str,
    partition_index: int,
    partitions: int,
    run_id: str,
) -> str:
    if partition_mode == "auto":
        return auto_partition_key_for_index(partition_index)
    return partition_for(partition_index, partitions, run_id)


class BufferedRedisExecutor:
    def __init__(self, redis_client) -> None:
        self.redis_client = redis_client
        self.commands: list[tuple] = []
        self.flushes = 0
        self.commands_sent = 0
        self.max_depth = 0

    def execute_command(self, *args):
        self.commands.append(args)
        return b"QUEUED"

    def flush(self):
        if not self.commands:
            return []

        commands = self.commands
        self.commands = []
        pipe = self.redis_client.pipeline(transaction=False)
        for command in commands:
            pipe.execute_command(*command)

        results = pipe.execute()
        depth = len(commands)
        self.flushes += 1
        self.commands_sent += depth
        self.max_depth = max(self.max_depth, depth)
        return results


class BenchFlowClient:
    def __init__(self, url: str, transport: str, batch_size: int = 100) -> None:
        self.transport = transport
        base = FlowClient.from_url(url)
        self.read_client = base
        self.redis_client = getattr(base.executor, "client", None)
        self.autobatch_client = None
        if transport == "pipeline":
            if self.redis_client is None:
                raise RuntimeError("pipeline transport requires RedisAdapter")
            self.executor = BufferedRedisExecutor(self.redis_client)
            self.client = FlowClient(self.executor)
            return
        if transport == "autobatch":
            self.executor = None
            self.autobatch_client = base.autobatch(max_batch=batch_size, max_delay_ms=1.0)
            self.client = self.autobatch_client
            return
        self.executor = None
        self.client = base

    def enqueue_many(
        self,
        *,
        run_id: str,
        flow_type: str,
        indices: list[int],
        partitions: int,
        payload: bytes,
        partition_mode: str,
        independent_many: bool,
    ) -> int:
        auto_partition = partition_mode == "auto"
        if len(indices) == 1:
            index = indices[0]
            self.client.create(
                f"{run_id}:flow:{index}",
                type=flow_type,
                state=QUEUE_STATE,
                partition_key=None if auto_partition else partition_for(index, partitions, run_id),
                payload=payload,
                return_record=False,
            )
            self.flush()
            return len(indices)

        if auto_partition and self.transport == "many":
            items = [
                CreateItem(
                    f"{run_id}:flow:{index}",
                    payload,
                )
                for index in indices
            ]
            self.client.enqueue_many(
                items,
                type=flow_type,
                state=QUEUE_STATE,
                independent=independent_many,
            )
            return len(items)

        if self.transport == "autobatch" and not auto_partition:
            futures = [
                self.autobatch_client.create_async(
                    f"{run_id}:flow:{index}",
                    type=flow_type,
                    state=QUEUE_STATE,
                    partition_key=partition_for(index, partitions, run_id),
                    payload=payload,
                    return_record=False,
                )
                for index in indices
            ]
            self.flush()
            for future in futures:
                future.result()
            return len(indices)

        if self.transport == "pipeline" or auto_partition:
            for index in indices:
                self.client.create(
                    f"{run_id}:flow:{index}",
                    type=flow_type,
                    state=QUEUE_STATE,
                    partition_key=None if auto_partition else partition_for(index, partitions, run_id),
                    payload=payload,
                    return_record=False,
                )
            self.flush()
            return len(indices)

        items = [
            CreateItem(
                f"{run_id}:flow:{index}",
                payload,
                partition_key=partition_for(index, partitions, run_id),
            )
            for index in indices
        ]
        self.client.create_many(
            None,
            items,
            type=flow_type,
            state=QUEUE_STATE,
            independent=independent_many,
        )
        return len(items)

    def claim_due(
        self,
        *,
        flow_type: str,
        worker: str,
        partition_key: str | None,
        limit: int,
        reclaim_expired: bool,
        reclaim_ratio: int,
        claim_priority: int | None,
        claim_job_only: bool,
    ):
        opts = {
            "state": QUEUE_STATE,
            "worker": worker,
            "partition_key": partition_key,
            "limit": limit,
            "reclaim_expired": reclaim_expired,
            "reclaim_ratio": reclaim_ratio,
        }
        if claim_priority is not None:
            opts["priority"] = claim_priority
        return self.read_client.claim_due(flow_type, job_only=claim_job_only, **opts)

    def complete_claimed(
        self,
        jobs,
        *,
        partition_key: str | None,
        use_many: bool,
        independent_many: bool,
        result: bytes | None,
    ) -> None:
        if self.transport == "autobatch":
            futures = [
                self.autobatch_client.complete_async(
                    job.id,
                    lease_token=job.lease_token,
                    fencing_token=job.fencing_token,
                    partition_key=job.partition_key,
                    result=result,
                    return_record=False,
                )
                for job in jobs
            ]
            self.flush()
            for future in futures:
                future.result()
            return

        if self.transport == "pipeline":
            for job in jobs:
                kwargs = {
                    "lease_token": job.lease_token,
                    "fencing_token": job.fencing_token,
                    "partition_key": job.partition_key,
                    "return_record": False,
                }
                if result is not None:
                    kwargs["result"] = result
                self.client.complete(job.id, **kwargs)
            self.flush()
            return

        if use_many and len(jobs) > 1:
            items = [
                ClaimedItem(
                    job.id,
                    job.lease_token,
                    job.fencing_token,
                    partition_key=job.partition_key,
                )
                for job in jobs
            ]
            self.client.complete_many(
                partition_key,
                items,
                result=result,
                independent=independent_many,
            )
            return

        for job in jobs:
            kwargs = {
                "lease_token": job.lease_token,
                "fencing_token": job.fencing_token,
                "partition_key": job.partition_key,
                "return_record": False,
            }
            if result is not None:
                kwargs["result"] = result
            self.client.complete(job.id, **kwargs)

    def do_work(self, command: str, run_id: str, jobs) -> None:
        if command != "incr":
            return
        for _job in jobs:
            self.client.executor.execute_command("INCR", f"{run_id}:counter")

    def flush(self):
        if self.autobatch_client is not None:
            self.autobatch_client.flush()
            return []
        if self.executor is None:
            return []
        return self.executor.flush()

    def pipeline_stats(self, prefix: str) -> dict[str, int]:
        if self.executor is None:
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }
        return {
            f"{prefix}_pipeline_flushes": self.executor.flushes,
            f"{prefix}_pipeline_commands": self.executor.commands_sent,
            f"{prefix}_pipeline_max_depth": self.executor.max_depth,
        }


class PartitionWakeCoordinator:
    def __init__(self, workers: int, partitions: int) -> None:
        self.workers = workers
        self.partitions = partitions
        self.queues = [queue.Queue() for _ in range(workers)]
        self.pending = [set() for _ in range(workers)]
        self.locks = [threading.Lock() for _ in range(workers)]
        self.notifications = 0

    def owner_for(self, partition_index: int) -> int:
        return partition_index % self.workers

    def notify_partition(self, partition_index: int) -> None:
        owner = self.owner_for(partition_index)
        with self.locks[owner]:
            if partition_index in self.pending[owner]:
                return
            self.pending[owner].add(partition_index)
            self.notifications += 1
        self.queues[owner].put(partition_index)

    def next_partition(self, worker_index: int, timeout_s: float) -> int:
        partition_index = self.queues[worker_index].get(timeout=timeout_s)
        with self.locks[worker_index]:
            self.pending[worker_index].discard(partition_index)
        return partition_index


def create_flows(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    indices: list[int],
    partitions: int,
    create_batch_size: int,
    payload: bytes,
    transport: str,
    partition_mode: str,
    independent_many: bool,
    wake_coordinator: PartitionWakeCoordinator | None,
) -> dict[str, int]:
    flow = BenchFlowClient(url, transport, create_batch_size)
    created = 0

    if partition_mode == "auto" and transport == "many":
        auto_buffers: dict[int, list[int]] = {}

        def flush_auto_bucket(partition_index: int) -> None:
            nonlocal created
            batch = auto_buffers.get(partition_index)
            if not batch:
                return
            created += flow.enqueue_many(
                run_id=run_id,
                flow_type=flow_type,
                indices=batch,
                partitions=partitions,
                payload=payload,
                partition_mode=partition_mode,
                independent_many=independent_many,
            )
            auto_buffers[partition_index] = []
            if wake_coordinator is not None:
                wake_coordinator.notify_partition(partition_index)

        for index in indices:
            partition_index = auto_partition_index_for_flow_id(f"{run_id}:flow:{index}")
            bucket = auto_buffers.setdefault(partition_index, [])
            bucket.append(index)
            if len(bucket) >= max(create_batch_size, 1):
                flush_auto_bucket(partition_index)

        for partition_index in list(auto_buffers):
            flush_auto_bucket(partition_index)

        return {"created": created, **flow.pipeline_stats("create")}

    for batch in chunks(indices, max(create_batch_size, 1)):
        created += flow.enqueue_many(
            run_id=run_id,
            flow_type=flow_type,
            indices=batch,
            partitions=partitions,
            payload=payload,
            partition_mode=partition_mode,
            independent_many=independent_many,
        )
        if wake_coordinator is not None and partition_mode == "auto":
            for partition_index in {
                auto_partition_index_for_flow_id(f"{run_id}:flow:{index}") for index in batch
            }:
                wake_coordinator.notify_partition(partition_index)
        elif wake_coordinator is not None:
            for partition_index in {index % partitions for index in batch}:
                wake_coordinator.notify_partition(partition_index)
    return {"created": created, **flow.pipeline_stats("create")}


def run_claim_worker(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    worker_index: int,
    worker_count: int,
    partitions: int,
    partition_mode: str,
    claim_any: bool,
    claim_batch_size: int,
    complete_batch: bool,
    complete_async_depth: int,
    independent_many: bool,
    transport: str,
    work_command: str,
    result: bytes | None,
    total_flows: int,
    idle_sleep_ms: float,
    max_idle_sleep_ms: float,
    wake_coalesce_ms: float,
    partial_claim_retries: int,
    partial_claim_delay_ms: float,
    reclaim_expired: bool,
    reclaim_ratio: int,
    claim_priority: int | None,
    claim_job_only: bool,
    producers_done: threading.Event,
    claimed_total: list[int],
    completed: list[int],
    completed_ids: set[str],
    duplicate_completions: list[int],
    completed_lock: threading.Lock,
    wake_coordinator: PartitionWakeCoordinator | None,
) -> dict[str, int]:
    flow = BenchFlowClient(url, transport, claim_batch_size)
    worker = f"{run_id}:worker:{worker_index}"
    local_completed = 0
    claim_round = 0
    claim_calls = 0
    empty_claims = 0
    claimed_items = 0
    local_duplicate_completions = 0
    max_claim_batch = 0
    base_idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
    max_idle_sleep_s = max(max_idle_sleep_ms, idle_sleep_ms, 0.0) / 1000.0
    idle_sleep_s = base_idle_sleep_s
    wake_coalesce_s = max(wake_coalesce_ms, 0.0) / 1000.0
    partial_claim_delay_s = max(partial_claim_delay_ms, 0.0) / 1000.0
    partial_claim_retries = max(partial_claim_retries, 0)
    owned_partitions = [p for p in range(partitions) if p % worker_count == worker_index]
    fallback_round = 0
    complete_executor = (
        ThreadPoolExecutor(max_workers=complete_async_depth) if complete_async_depth > 0 else None
    )
    complete_clients = [
        BenchFlowClient(url, transport, claim_batch_size) for _ in range(max(complete_async_depth, 0))
    ]
    complete_client_index = 0
    pending_completions = []

    def done() -> bool:
        with completed_lock:
            return completed[0] >= total_flows

    def all_claimed() -> bool:
        with completed_lock:
            return claimed_total[0] >= total_flows

    def record_completed_jobs(jobs) -> None:
        nonlocal local_completed, local_duplicate_completions
        unique_jobs = 0
        duplicate_jobs = 0
        with completed_lock:
            for job in jobs:
                if job.id in completed_ids:
                    duplicate_jobs += 1
                else:
                    completed_ids.add(job.id)
                    unique_jobs += 1
            completed[0] += unique_jobs
            duplicate_completions[0] += duplicate_jobs
        local_completed += unique_jobs
        local_duplicate_completions += duplicate_jobs

    def complete_jobs(client: BenchFlowClient, jobs, partition_key: str | None):
        client.complete_claimed(
            jobs,
            partition_key=None if claim_any else partition_key,
            use_many=complete_batch,
            independent_many=independent_many,
            result=result,
        )
        return jobs

    def drain_completed_completions(block: bool = False) -> None:
        if not pending_completions:
            return

        if block:
            future = pending_completions.pop(0)
            record_completed_jobs(future.result())
            return

        ready = []
        remaining = []
        for future in pending_completions:
            if future.done():
                ready.append(future)
            else:
                remaining.append(future)
        pending_completions[:] = remaining
        for future in ready:
            record_completed_jobs(future.result())

    def drain_all_completions() -> None:
        while pending_completions:
            drain_completed_completions(block=True)

    def finish() -> dict[str, int]:
        drain_all_completions()
        if complete_executor is not None:
            complete_executor.shutdown(wait=True)
        return {
            "completed": local_completed,
            "duplicate_completions": local_duplicate_completions,
            "claim_calls": claim_calls,
            "empty_claims": empty_claims,
            "claimed_items": claimed_items,
            "max_claim_batch": max_claim_batch,
            **flow.pipeline_stats("process"),
        }

    def handle_jobs(jobs, partition_key: str | None) -> None:
        nonlocal claimed_items, complete_client_index, max_claim_batch
        max_claim_batch = max(max_claim_batch, len(jobs))
        claimed_items += len(jobs)
        with completed_lock:
            claimed_total[0] += len(jobs)
        flow.do_work(work_command, run_id, jobs)

        if complete_executor is None:
            complete_jobs(flow, jobs, partition_key)
            record_completed_jobs(jobs)
            return

        while len(pending_completions) >= complete_async_depth:
            drain_completed_completions(block=True)

        client = complete_clients[complete_client_index % complete_async_depth]
        complete_client_index += 1
        pending_completions.append(
            complete_executor.submit(complete_jobs, client, jobs, partition_key)
        )

    while True:
        drain_completed_completions()
        if done():
            return finish()
        if all_claimed():
            if pending_completions:
                drain_completed_completions(block=True)
            elif idle_sleep_s > 0:
                time.sleep(idle_sleep_s)
            continue

        partition_index = None
        partition_key = None

        if wake_coordinator is not None and not claim_any:
            try:
                partition_index = wake_coordinator.next_partition(worker_index, idle_sleep_s)
            except queue.Empty:
                if producers_done.is_set() and owned_partitions:
                    partition_index = owned_partitions[fallback_round % len(owned_partitions)]
                    fallback_round += 1
                else:
                    continue
            if wake_coalesce_s > 0 and not producers_done.is_set():
                time.sleep(wake_coalesce_s)
            partition_key = benchmark_partition_key(
                partition_mode=partition_mode,
                partition_index=partition_index,
                partitions=partitions,
                run_id=run_id,
            )

            while True:
                if done():
                    return finish()
                claim_calls += 1
                jobs = flow.claim_due(
                    flow_type=flow_type,
                    worker=worker,
                    partition_key=partition_key,
                    limit=claim_batch_size,
                    reclaim_expired=reclaim_expired,
                    reclaim_ratio=reclaim_ratio,
                    claim_priority=claim_priority,
                    claim_job_only=claim_job_only,
                )
                if not jobs:
                    empty_claims += 1
                    break
                handle_jobs(jobs, partition_key)
                if len(jobs) < claim_batch_size:
                    retried = 0
                    continue_partition = False
                    while not producers_done.is_set() and retried < partial_claim_retries:
                        if partial_claim_delay_s > 0:
                            time.sleep(partial_claim_delay_s)
                        claim_calls += 1
                        more_jobs = flow.claim_due(
                            flow_type=flow_type,
                            worker=worker,
                            partition_key=partition_key,
                            limit=claim_batch_size,
                            reclaim_expired=reclaim_expired,
                            reclaim_ratio=reclaim_ratio,
                            claim_priority=claim_priority,
                            claim_job_only=claim_job_only,
                        )
                        if not more_jobs:
                            empty_claims += 1
                            break
                        handle_jobs(more_jobs, partition_key)
                        if len(more_jobs) >= claim_batch_size:
                            continue_partition = True
                            break
                        retried += 1
                    if continue_partition:
                        continue
                    break
            continue

        if not claim_any:
            partition_index = partition_index_for_claim(
                worker_index,
                worker_count,
                partitions,
                claim_round,
            )
            claim_round += 1
            partition_key = benchmark_partition_key(
                partition_mode=partition_mode,
                partition_index=partition_index,
                partitions=partitions,
                run_id=run_id,
            )

        claim_calls += 1
        jobs = flow.claim_due(
            flow_type=flow_type,
            worker=worker,
            partition_key=partition_key,
            limit=claim_batch_size,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            claim_priority=claim_priority,
            claim_job_only=claim_job_only,
        )
        if not jobs:
            empty_claims += 1
            if idle_sleep_s > 0:
                time.sleep(idle_sleep_s)
                idle_sleep_s = min(max_idle_sleep_s, max(idle_sleep_s * 2, base_idle_sleep_s))
            continue

        idle_sleep_s = base_idle_sleep_s
        handle_jobs(jobs, partition_key)


def partition_index_for_claim(
    worker_index: int,
    worker_count: int,
    partitions: int,
    claim_round: int,
) -> int:
    if partitions <= 0:
        return 0
    if worker_count >= partitions:
        return worker_index % partitions
    return (worker_index + claim_round * worker_count) % partitions


def run_queued_throughput(args: argparse.Namespace) -> dict[str, float | int | str | bool]:
    run_id = f"py-sdk-bench-{uuid.uuid4().hex}"
    flow_type = f"{FLOW_TYPE}:{run_id}"
    indices = list(range(args.flows))
    payload = payload_bytes(args.payload_bytes)
    result = payload_bytes(args.result_bytes) if args.result_bytes > 0 else None
    completed = [0]
    claimed_total = [0]
    completed_ids: set[str] = set()
    duplicate_completions = [0]
    completed_lock = threading.Lock()
    producers_done = threading.Event()
    effective_worker_mode = "polling" if args.claim_any else args.worker_mode
    partition_mode = args.partition_mode
    if args.claim_any and partition_mode == "explicit":
        partition_mode = "auto"
    worker_partitions = AUTO_PARTITION_BUCKETS if partition_mode == "auto" else args.partitions
    wake_coordinator = (
        PartitionWakeCoordinator(args.workers, worker_partitions)
        if effective_worker_mode == "owner-wakeup"
        else None
    )

    if partition_mode == "auto" and args.transport == "many":
        create_ranges = [[] for _ in range(args.producers)]
        for index in indices:
            owner = auto_partition_index_for_flow_id(f"{run_id}:flow:{index}") % args.producers
            create_ranges[owner].append(index)
    else:
        create_ranges = [indices[offset:: args.producers] for offset in range(args.producers)]
    create_started = None
    create_finished = None
    process_started = None
    process_finished = None
    started = time.perf_counter()

    create_results = []
    worker_results = []

    def submit_create_jobs(executor: ThreadPoolExecutor):
        return [
            executor.submit(
                create_flows,
                url=args.url,
                run_id=run_id,
                flow_type=flow_type,
                indices=batch,
                partitions=args.partitions,
                create_batch_size=args.create_batch_size,
                payload=payload,
                transport=args.transport,
                partition_mode=partition_mode,
                independent_many=args.independent_many,
                wake_coordinator=wake_coordinator,
            )
            for batch in create_ranges
        ]

    def submit_worker_jobs(executor: ThreadPoolExecutor):
        return [
            executor.submit(
                run_claim_worker,
                url=args.url,
                run_id=run_id,
                flow_type=flow_type,
                worker_index=worker_index,
                worker_count=args.workers,
                partitions=worker_partitions,
                partition_mode=partition_mode,
                claim_any=args.claim_any,
                claim_batch_size=args.claim_batch_size,
                complete_batch=args.complete_batch,
                complete_async_depth=args.complete_async_depth,
                independent_many=args.independent_many,
                transport=args.transport,
                work_command=args.work_command,
                result=result,
                total_flows=args.flows,
                idle_sleep_ms=args.idle_sleep_ms,
                max_idle_sleep_ms=args.max_idle_sleep_ms,
                wake_coalesce_ms=args.wake_coalesce_ms,
                partial_claim_retries=args.partial_claim_retries,
                partial_claim_delay_ms=args.partial_claim_delay_ms,
                reclaim_expired=args.reclaim_expired,
                reclaim_ratio=args.reclaim_ratio,
                claim_priority=args.claim_priority,
                claim_job_only=args.claim_job_only,
                producers_done=producers_done,
                claimed_total=claimed_total,
                completed=completed,
                completed_ids=completed_ids,
                duplicate_completions=duplicate_completions,
                completed_lock=completed_lock,
                wake_coordinator=wake_coordinator,
            )
            for worker_index in range(args.workers)
        ]

    if args.queued_shape == "preloaded":
        create_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.producers) as executor:
            create_futures = submit_create_jobs(executor)
            for future in as_completed(create_futures):
                create_results.append(future.result())
        create_finished = time.perf_counter()
        producers_done.set()

        process_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            worker_futures = submit_worker_jobs(executor)
            for future in as_completed(worker_futures):
                worker_results.append(future.result())
        process_finished = time.perf_counter()
    else:
        with ThreadPoolExecutor(max_workers=args.producers + args.workers) as executor:
            worker_futures = submit_worker_jobs(executor)

            process_started = time.perf_counter()
            create_started = time.perf_counter()
            create_futures = submit_create_jobs(executor)

            for future in as_completed(create_futures):
                create_results.append(future.result())
            create_finished = time.perf_counter()
            producers_done.set()

            for future in as_completed(worker_futures):
                worker_results.append(future.result())
            process_finished = time.perf_counter()

    total_seconds = process_finished - started
    create_seconds = create_finished - create_started
    process_seconds = process_finished - process_started
    created = sum(result["created"] for result in create_results)
    processed = completed[0]
    duplicate_completed = sum(result["duplicate_completions"] for result in worker_results)

    create_pipeline_flushes = sum(result["create_pipeline_flushes"] for result in create_results)
    create_pipeline_commands = sum(result["create_pipeline_commands"] for result in create_results)
    create_pipeline_max_depth = max(
        (result["create_pipeline_max_depth"] for result in create_results),
        default=0,
    )
    process_pipeline_flushes = sum(result["process_pipeline_flushes"] for result in worker_results)
    process_pipeline_commands = sum(result["process_pipeline_commands"] for result in worker_results)
    process_pipeline_max_depth = max(
        (result["process_pipeline_max_depth"] for result in worker_results),
        default=0,
    )
    process_claim_calls = sum(result["claim_calls"] for result in worker_results)
    process_empty_claims = sum(result["empty_claims"] for result in worker_results)
    process_claimed_items = sum(result["claimed_items"] for result in worker_results)
    process_max_claim_batch = max(
        (result["max_claim_batch"] for result in worker_results),
        default=0,
    )
    process_avg_claim_batch = (
        process_claimed_items / process_claim_calls if process_claim_calls > 0 else 0.0
    )

    return {
        "mode": "queued",
        "queued_shape": args.queued_shape,
        "flow_type": flow_type,
        "flows": args.flows,
        "created": created,
        "completed": processed,
        "claimed_items": process_claimed_items,
        "duplicate_completions": duplicate_completed,
        "workers": args.workers,
        "producers": args.producers,
        "partitions": args.partitions,
        "claim_any": args.claim_any,
        "partition_mode": partition_mode,
        "worker_mode": effective_worker_mode,
        "claim_batch_size": args.claim_batch_size,
        "create_batch_size": args.create_batch_size,
        "complete_batch": args.complete_batch,
        "complete_async_depth": args.complete_async_depth,
        "independent_many": args.independent_many,
        "transport": args.transport,
        "payload_bytes": args.payload_bytes,
        "result_bytes": args.result_bytes,
        "work_command": args.work_command,
        "idle_sleep_ms": args.idle_sleep_ms,
        "max_idle_sleep_ms": args.max_idle_sleep_ms,
        "wake_coalesce_ms": args.wake_coalesce_ms,
        "partial_claim_retries": args.partial_claim_retries,
        "partial_claim_delay_ms": args.partial_claim_delay_ms,
        "reclaim_expired": args.reclaim_expired,
        "reclaim_ratio": args.reclaim_ratio,
        "claim_priority": args.claim_priority,
        "claim_job_only": args.claim_job_only,
        "wake_notifications": wake_coordinator.notifications if wake_coordinator is not None else 0,
        "process_claim_calls": process_claim_calls,
        "process_empty_claims": process_empty_claims,
        "process_avg_claim_batch": process_avg_claim_batch,
        "process_max_claim_batch": process_max_claim_batch,
        "create_pipeline_flushes": create_pipeline_flushes,
        "create_pipeline_commands": create_pipeline_commands,
        "create_pipeline_max_depth": create_pipeline_max_depth,
        "process_pipeline_flushes": process_pipeline_flushes,
        "process_pipeline_commands": process_pipeline_commands,
        "process_pipeline_max_depth": process_pipeline_max_depth,
        "create_seconds": create_seconds,
        "process_seconds": process_seconds,
        "total_seconds": total_seconds,
        "create_flows_per_sec": created / create_seconds if create_seconds > 0 else 0.0,
        "process_flows_per_sec": processed / process_seconds if process_seconds > 0 else 0.0,
        "end_to_end_flows_per_sec": processed / total_seconds if total_seconds > 0 else 0.0,
    }


def run_serial_latency_once(client: FlowClient, steps: int) -> float:
    run_id = f"py-sdk-bench-{uuid.uuid4().hex}"
    partition = partition_for(0, 1, run_id)
    started = time.perf_counter()
    client.create(
        f"{run_id}:flow",
        type=FLOW_TYPE,
        state="step_1",
        partition_key=partition,
        return_record=False,
    )

    for step in range(1, steps + 1):
        jobs = client.claim_due(
            FLOW_TYPE,
            state=f"step_{step}",
            worker=f"{run_id}:worker",
            partition_key=partition,
            limit=1,
        )
        job = jobs[0]
        client.executor.execute_command("INCR", f"{run_id}:counter")
        if step == steps:
            client.complete(
                job.id,
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=job.partition_key,
                result=b"ok",
                return_record=False,
            )
        else:
            client.transition(
                job.id,
                from_state=job.state,
                to_state=f"step_{step + 1}",
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=job.partition_key,
                return_record=False,
            )
    return (time.perf_counter() - started) * 1000.0


def run_serial_latency(args: argparse.Namespace) -> dict[str, float | int | str]:
    client = FlowClient.from_url(args.url)
    runtimes = [run_serial_latency_once(client, args.steps) for _ in range(args.iterations)]
    return {
        "mode": "serial-latency",
        "steps": args.steps,
        "iterations": args.iterations,
        "avg_ms": sum(runtimes) / len(runtimes),
        "p50_ms": percentile(runtimes, 50),
        "p95_ms": percentile(runtimes, 95),
        "p99_ms": percentile(runtimes, 99),
        "min_ms": min(runtimes),
        "max_ms": max(runtimes),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--mode", choices=("queued", "serial-latency"), default="queued")
    parser.add_argument("--queued-shape", choices=("live", "preloaded"), default="live")

    parser.add_argument("--flows", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--claim-batch-size", type=int, default=100)
    parser.add_argument("--create-batch-size", type=int, default=100)
    parser.add_argument("--transport", choices=("many", "pipeline", "autobatch"), default="pipeline")
    parser.add_argument("--partition-mode", choices=("explicit", "auto"), default="explicit")
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--result-bytes", type=int, default=0)
    parser.add_argument("--work-command", choices=("none", "incr"), default="none")
    parser.add_argument("--idle-sleep-ms", type=float, default=10.0)
    parser.add_argument("--max-idle-sleep-ms", type=float, default=50.0)
    parser.add_argument("--worker-mode", choices=("owner-wakeup", "polling"), default="owner-wakeup")
    parser.add_argument("--wake-coalesce-ms", type=float, default=5.0)
    parser.add_argument("--partial-claim-retries", type=int, default=1)
    parser.add_argument("--partial-claim-delay-ms", type=float, default=1.0)
    parser.add_argument("--reclaim-expired", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--reclaim-ratio", type=int, default=25)
    parser.add_argument("--claim-priority", type=int, default=0)
    parser.add_argument("--claim-job-only", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--claim-any", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--complete-batch", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--complete-async-depth", type=int, default=0)
    parser.add_argument("--independent-many", action=argparse.BooleanOptionalAction, default=True)

    parser.add_argument("--steps", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()

    if args.flows <= 0:
        parser.error("--flows must be positive")
    if args.workers <= 0:
        parser.error("--workers must be positive")
    if args.producers <= 0:
        parser.error("--producers must be positive")
    if args.partitions <= 0:
        parser.error("--partitions must be positive")
    if args.claim_batch_size <= 0:
        parser.error("--claim-batch-size must be positive")
    if args.create_batch_size <= 0:
        parser.error("--create-batch-size must be positive")
    if args.complete_async_depth < 0:
        parser.error("--complete-async-depth must be non-negative")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
    if args.result_bytes < 0:
        parser.error("--result-bytes must be non-negative")
    if args.idle_sleep_ms < 0:
        parser.error("--idle-sleep-ms must be non-negative")
    if args.max_idle_sleep_ms < 0:
        parser.error("--max-idle-sleep-ms must be non-negative")
    if args.wake_coalesce_ms < 0:
        parser.error("--wake-coalesce-ms must be non-negative")
    if args.partial_claim_retries < 0:
        parser.error("--partial-claim-retries must be non-negative")
    if args.partial_claim_delay_ms < 0:
        parser.error("--partial-claim-delay-ms must be non-negative")
    if args.reclaim_ratio < 0 or args.reclaim_ratio > 100:
        parser.error("--reclaim-ratio must be between 0 and 100")
    if args.claim_priority < 0:
        args.claim_priority = None
    if args.steps <= 0:
        parser.error("--steps must be positive")
    if args.iterations <= 0:
        parser.error("--iterations must be positive")

    if args.mode == "serial-latency":
        print(run_serial_latency(args))
        return

    print(run_queued_throughput(args))


if __name__ == "__main__":
    main()
