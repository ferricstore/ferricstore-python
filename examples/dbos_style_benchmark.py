import argparse
import math
import queue
import threading
import time
import uuid
import zlib
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from urllib.parse import urlparse

from ferricstore import (
    CreateItem,
    FlowClient,
    QueueFlowWorker,
    QueueFlowWorkerResult,
)

FLOW_TYPE = "dbos_python_sdk_bench"
QUEUE_STATE = "queued"
AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 256
SERVER_SLOT_COUNT = 1024
PROTOCOL_URL_SCHEMES = {"ferric", "ferrics"}

DBOS_QUEUE_DEFAULTS = {
    "flows": 1_000_000,
    "workers": 1,
    "producers": 32,
    "partitions": 16,
    "claim_batch_size": 500,
    "create_batch_size": 500,
    "transport": "many",
    "partition_mode": "auto",
    "worker_mode": "polling",
    "worker_api": "queue",
    "claim_partition_batch_size": 16,
    "claim_drain_batches": 2,
    "claim_prefetch": 0,
    "complete_async_depth": 4,
    "fuse_complete_claim": False,
    "claim_job_only": True,
    "server_shards": 16,
    "protocol_worker_connections": 1,
    "protocol_lanes": 32,
    "protocol_create_inflight_batches": 2,
    "protocol_wake_hints": False,
    "latency_sample_rate": 100,
    "adaptive_producer_backpressure": True,
    "producer_target_queue_latency_ms": 75.0,
    "producer_min_rate_per_sec": 50_000.0,
    "producer_max_rate_per_sec": 0.0,
    "producer_max_pending_credits": 0,
}


def chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def latency_summary(prefix: str, values: list[float]) -> dict[str, float | int]:
    count = len(values)
    return {
        f"{prefix}_sample_count": count,
        f"{prefix}_avg_ms": sum(values) / count if count > 0 else 0.0,
        f"{prefix}_p50_ms": percentile(values, 50),
        f"{prefix}_p95_ms": percentile(values, 95),
        f"{prefix}_p99_ms": percentile(values, 99),
        f"{prefix}_max_ms": max(values) if values else 0.0,
    }


class QueueLatencyRecorder:
    def __init__(self, sample_rate: int) -> None:
        self.sample_rate = max(int(sample_rate), 0)
        self._created_at_ns: dict[str, int] = {}
        self._queue_latency_ms: list[float] = []
        self._tracked = 0
        self._lock = threading.Lock()
        self._observers: list[Callable[[list[float]], None]] = []

    def enabled(self) -> bool:
        return self.sample_rate > 0

    def add_observer(self, observer: Callable[[list[float]], None]) -> None:
        self._observers.append(observer)

    def mark_created_indices(self, run_id: str, indices: list[int]) -> None:
        if not self.enabled():
            return

        now_ns = time.perf_counter_ns()
        sampled = {
            f"{run_id}:flow:{index}": now_ns for index in indices if index % self.sample_rate == 0
        }
        if not sampled:
            return

        with self._lock:
            self._created_at_ns.update(sampled)
            self._tracked += len(sampled)

    def mark_claimed(self, jobs) -> None:
        if not self.enabled():
            return

        now_ns = time.perf_counter_ns()
        latencies: list[float] = []
        with self._lock:
            for job in jobs:
                job_id = getattr(job, "id", None)
                if job_id is None:
                    continue
                created_at_ns = self._created_at_ns.pop(job_id, None)
                if created_at_ns is not None:
                    latencies.append((now_ns - created_at_ns) / 1_000_000.0)
            self._queue_latency_ms.extend(latencies)

        if latencies:
            for observer in self._observers:
                observer(latencies)

    def summary(self) -> dict[str, float | int]:
        with self._lock:
            samples = list(self._queue_latency_ms)
            tracked = self._tracked
            pending = len(self._created_at_ns)

        return {
            "queue_latency_tracked": tracked,
            "queue_latency_pending": pending,
            **latency_summary("queue_latency", samples),
        }


class AdaptiveProducerBackpressure:
    def __init__(
        self,
        *,
        enabled: bool,
        target_queue_latency_ms: float,
        min_rate_per_sec: float,
        max_rate_per_sec: float,
        decrease_ratio: float = 0.8,
        increase_ratio: float = 1.08,
    ) -> None:
        self.enabled = enabled
        self.target_queue_latency_ms = max(target_queue_latency_ms, 1.0)
        self.min_rate_per_sec = max(min_rate_per_sec, 1.0)
        self.max_rate_per_sec = max(max_rate_per_sec, 0.0)
        self.decrease_ratio = min(max(decrease_ratio, 0.1), 0.99)
        self.increase_ratio = max(increase_ratio, 1.01)
        self._lock = threading.Lock()
        self._rate_per_sec: float | None = None
        self._tokens = 0.0
        self._updated_at = time.perf_counter()
        self._latency_ewma_ms = 0.0
        self._waits = 0
        self._wait_seconds = 0.0
        self._limited_batches = 0

    def observe_queue_latencies(self, latencies_ms: list[float]) -> None:
        if not self.enabled or not latencies_ms:
            return

        sample_ms = sum(latencies_ms) / len(latencies_ms)
        with self._lock:
            if self._latency_ewma_ms <= 0:
                self._latency_ewma_ms = sample_ms
            else:
                self._latency_ewma_ms = self._latency_ewma_ms * 0.85 + sample_ms * 0.15

            high_pressure = (
                sample_ms > self.target_queue_latency_ms * 1.25
                or self._latency_ewma_ms > self.target_queue_latency_ms
            )
            low_pressure = self._latency_ewma_ms < self.target_queue_latency_ms * 0.6

            if high_pressure:
                base_rate = (
                    self._rate_per_sec
                    or self.max_rate_per_sec
                    or max(self.min_rate_per_sec * 8.0, 100_000.0)
                )
                self._rate_per_sec = max(self.min_rate_per_sec, base_rate * self.decrease_ratio)
                self._tokens = min(self._tokens, self._rate_per_sec * 0.05)
                self._limited_batches += 1
            elif low_pressure and self._rate_per_sec is not None:
                next_rate = self._rate_per_sec * self.increase_ratio
                if self.max_rate_per_sec > 0 and next_rate >= self.max_rate_per_sec:
                    self._rate_per_sec = None
                    self._tokens = 0.0
                elif self.max_rate_per_sec == 0 and next_rate >= self._unlimited_release_rate():
                    self._rate_per_sec = None
                    self._tokens = 0.0
                else:
                    self._rate_per_sec = next_rate

    def _unlimited_release_rate(self) -> float:
        return max(self.min_rate_per_sec * 8.0, 100_000.0)

    def wait_for_creates(self, count: int) -> None:
        if not self.enabled or count <= 0:
            return

        requested = float(count)
        while True:
            with self._lock:
                rate = self._rate_per_sec
                if rate is None:
                    return

                now = time.perf_counter()
                elapsed = max(now - self._updated_at, 0.0)
                self._updated_at = now
                self._tokens = min(rate * 0.25, self._tokens + elapsed * rate)

                if self._tokens >= requested:
                    self._tokens -= requested
                    return

                wait_s = (requested - self._tokens) / rate
                self._waits += 1
                self._wait_seconds += wait_s

            time.sleep(min(wait_s, 0.01))

    def wait_for_pending_credits(
        self,
        pending_credits: Callable[[], int],
        *,
        max_pending_credits: int,
    ) -> None:
        if not self.enabled or max_pending_credits <= 0:
            return

        while pending_credits() > max_pending_credits:
            wait_s = 0.001
            with self._lock:
                self._waits += 1
                self._wait_seconds += wait_s
            time.sleep(wait_s)

    def summary(self) -> dict[str, float | int | bool]:
        with self._lock:
            rate = self._rate_per_sec or 0.0
            return {
                "adaptive_producer_backpressure": self.enabled,
                "producer_backpressure_rate_per_sec": rate,
                "producer_backpressure_waits": self._waits,
                "producer_backpressure_wait_ms": self._wait_seconds * 1000.0,
                "producer_backpressure_limited_batches": self._limited_batches,
                "producer_queue_latency_ewma_ms": self._latency_ewma_ms,
            }


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


def is_protocol_url(url: str) -> bool:
    return urlparse(url).scheme.lower() in PROTOCOL_URL_SCHEMES


def protocol_queue_worker_lanes(
    *,
    url: str,
    worker_api: str,
    workers: int,
    claim_any: bool,
    partitions: int,
    server_shards: int,
) -> int:
    if worker_api != "queue" or not is_protocol_url(url) or workers != 1 or claim_any:
        return workers
    return max(1, min(partitions, server_shards, 16))


def partition_for(index: int, partitions: int, prefix: str) -> str:
    return f"{prefix}:partition:{index % max(partitions, 1)}"


def auto_partition_index_for_flow_id(id: str) -> int:
    return zlib.crc32(id.encode()) % AUTO_PARTITION_BUCKETS


def auto_partition_key_for_index(index: int) -> str:
    return f"{AUTO_PARTITION_PREFIX}{index % AUTO_PARTITION_BUCKETS}"


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


def auto_partition_owner(index: int, workers: int, server_shards: int) -> int:
    workers = max(workers, 1)
    server_shards = max(server_shards, 1)
    shard = auto_partition_server_shard_for_index(index, server_shards)
    if workers <= server_shards:
        return shard % workers
    shard_workers = [worker for worker in range(workers) if worker % server_shards == shard]
    if not shard_workers:
        return shard % workers
    return shard_workers[index % len(shard_workers)]


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
    def __init__(
        self,
        url: str,
        transport: str,
        batch_size: int = 100,
        *,
        client_kwargs: dict[str, object] | None = None,
        base_client: FlowClient | None = None,
    ) -> None:
        self.transport = transport
        base = base_client or FlowClient.from_url(url, **(client_kwargs or {}))
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
        retention_ttl_ms: int | None = None,
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
                retention_ttl_ms=retention_ttl_ms,
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
                return_ok_on_success=True,
                retention_ttl_ms=retention_ttl_ms,
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
                    retention_ttl_ms=retention_ttl_ms,
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
                    partition_key=None
                    if auto_partition
                    else partition_for(index, partitions, run_id),
                    payload=payload,
                    retention_ttl_ms=retention_ttl_ms,
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
        self.client.enqueue_many(
            items,
            type=flow_type,
            state=QUEUE_STATE,
            independent=independent_many,
            return_ok_on_success=True,
            retention_ttl_ms=retention_ttl_ms,
        )
        return len(items)

    def enqueue_many_future(
        self,
        *,
        run_id: str,
        flow_type: str,
        indices: list[int],
        partitions: int,
        payload: bytes,
        partition_mode: str,
        independent_many: bool,
        retention_ttl_ms: int | None = None,
    ) -> Future[int] | None:
        if self.transport != "many" or not indices:
            return None

        submit_create_many = getattr(self.client, "submit_create_many", None)
        if not callable(submit_create_many):
            return None

        auto_partition = partition_mode == "auto"
        flow_ids = [f"{run_id}:flow:{index}" for index in indices]

        if auto_partition:
            partition_index = auto_partition_index_for_flow_id(flow_ids[0])
            if any(
                auto_partition_index_for_flow_id(flow_id) != partition_index for flow_id in flow_ids
            ):
                return None
            partition_key = auto_partition_key_for_index(partition_index)
        else:
            partition_index = indices[0] % max(partitions, 1)
            if any(index % max(partitions, 1) != partition_index for index in indices):
                return None
            partition_key = partition_for(partition_index, partitions, run_id)

        items = [CreateItem(flow_id, payload) for flow_id in flow_ids]
        source = submit_create_many(
            partition_key,
            items,
            type=flow_type,
            state=QUEUE_STATE,
            independent=independent_many,
            return_ok_on_success=True,
            retention_ttl_ms=retention_ttl_ms,
        )
        result: Future[int] = Future()
        count = len(items)

        def complete(source_future: Future) -> None:
            if result.cancelled():
                return
            try:
                source_future.result()
                result.set_result(count)
            except Exception as exc:
                result.set_exception(exc)

        source.add_done_callback(complete)
        return result

    def claim_due(
        self,
        *,
        flow_type: str,
        state: str | None,
        states: list[str] | None,
        worker: str,
        partition_key: str | None,
        partition_keys: list[str] | None = None,
        limit: int,
        reclaim_expired: bool,
        reclaim_ratio: int,
        claim_priority: int | None,
        claim_job_only: bool,
        claim_block_ms: int | None,
    ):
        opts = {
            "state": state,
            "states": states,
            "worker": worker,
            "partition_key": partition_key,
            "partition_keys": partition_keys,
            "limit": limit,
            "reclaim_expired": reclaim_expired,
            "reclaim_ratio": reclaim_ratio,
            "block_ms": claim_block_ms,
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
            self.client.complete_many(
                partition_key,
                jobs,
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
    def __init__(
        self,
        workers: int,
        partitions: int,
        owner_for: Callable[[int], int] | None = None,
    ) -> None:
        self.workers = workers
        self.partitions = partitions
        self._owner_for = owner_for
        self.queues = [queue.Queue() for _ in range(workers)]
        self.pending = [set() for _ in range(workers)]
        self.credits = [{} for _ in range(workers)]
        self.locks = [threading.Lock() for _ in range(workers)]
        self.notifications = 0
        self.notified_jobs = 0

    def owner_for(self, partition_index: int) -> int:
        if self._owner_for is not None:
            return self._owner_for(partition_index) % self.workers
        return partition_index % self.workers

    def notify_partition(self, partition_index: int, count: int = 1) -> None:
        count = max(int(count), 0)
        if count == 0:
            return

        owner = self.owner_for(partition_index)
        should_queue = False
        with self.locks[owner]:
            self.credits[owner][partition_index] = (
                self.credits[owner].get(partition_index, 0) + count
            )
            self.notified_jobs += count
            if partition_index not in self.pending[owner]:
                self.pending[owner].add(partition_index)
                self.notifications += 1
                should_queue = True

        if should_queue:
            self.queues[owner].put(partition_index)

    def next_partition(self, worker_index: int, timeout_s: float) -> tuple[int, int]:
        partition_index = self.queues[worker_index].get(timeout=timeout_s)
        with self.locks[worker_index]:
            self.pending[worker_index].discard(partition_index)
            credit = self.credits[worker_index].pop(partition_index, 0)
        return partition_index, credit

    def next_partitions(
        self,
        worker_index: int,
        timeout_s: float,
        max_partitions: int,
        max_credit: int,
        same_group=None,
    ) -> tuple[list[int], int]:
        if max_partitions <= 0 or max_credit <= 0:
            return [], 0

        deadline = time.monotonic() + timeout_s
        partitions: list[int] = []
        total_credit = 0

        while not partitions:
            try:
                wait_s = 0.0 if timeout_s <= 0 else max(deadline - time.monotonic(), 0.0)
                partition_index = self.queues[worker_index].get(timeout=wait_s)
            except queue.Empty:
                if timeout_s <= 0:
                    return [], 0
                raise

            credit = self._take_credit(worker_index, partition_index, max_credit)
            if credit > 0:
                partitions.append(partition_index)
                total_credit += credit
                break

            if timeout_s <= 0 or time.monotonic() >= deadline:
                return [], 0

        while len(partitions) < max_partitions and total_credit < max_credit:
            try:
                partition_index = self.queues[worker_index].get_nowait()
            except queue.Empty:
                break

            if same_group is not None and not same_group(partitions[0], partition_index):
                self.queues[worker_index].put(partition_index)
                break

            credit = self._take_credit(worker_index, partition_index, max_credit - total_credit)
            if credit <= 0:
                continue

            partitions.append(partition_index)
            total_credit += credit

        return partitions, total_credit

    def _take_credit(self, worker_index: int, partition_index: int, max_credit: int) -> int:
        if max_credit <= 0:
            return 0

        requeue = False
        with self.locks[worker_index]:
            credit = self.credits[worker_index].get(partition_index, 0)
            if credit <= 0:
                self.pending[worker_index].discard(partition_index)
                return 0

            taken = min(credit, max_credit)
            remaining = credit - taken
            if remaining > 0:
                self.credits[worker_index][partition_index] = remaining
                requeue = True
            else:
                self.credits[worker_index].pop(partition_index, None)
                self.pending[worker_index].discard(partition_index)

        if requeue:
            self.queues[worker_index].put(partition_index)

        return taken

    def return_credit(self, worker_index: int, partition_index: int, credit: int) -> None:
        if credit <= 0:
            return

        should_queue = False
        with self.locks[worker_index]:
            self.credits[worker_index][partition_index] = (
                self.credits[worker_index].get(partition_index, 0) + credit
            )
            if partition_index not in self.pending[worker_index]:
                self.pending[worker_index].add(partition_index)
                should_queue = True

        if should_queue:
            self.queues[worker_index].put(partition_index)

    def total_credit(self) -> int:
        total = 0
        for worker_index in range(self.workers):
            with self.locks[worker_index]:
                total += sum(self.credits[worker_index].values())
        return total

    def worker_credit(self, worker_index: int) -> int:
        with self.locks[worker_index]:
            return sum(self.credits[worker_index].values())

    def take_credit(self, worker_index: int, partition_index: int) -> int:
        with self.locks[worker_index]:
            self.pending[worker_index].discard(partition_index)
            return self.credits[worker_index].pop(partition_index, 0)


class AdaptiveFallbackScheduler:
    def __init__(
        self,
        *,
        min_interval_s: float,
        max_interval_s: float,
        initial_interval_s: float,
    ) -> None:
        self.min_interval_s = max(min_interval_s, 0.001)
        self.max_interval_s = max(max_interval_s, self.min_interval_s)
        self.interval_s = min(
            max(initial_interval_s, self.min_interval_s),
            self.max_interval_s,
        )
        self.last_scan_s = time.perf_counter()

    def should_scan(self, now_s: float) -> bool:
        return now_s - self.last_scan_s >= self.interval_s

    def record_scan(self, now_s: float, *, claimed: int) -> None:
        self.last_scan_s = now_s
        if claimed > 0:
            self.interval_s = max(self.min_interval_s, self.interval_s * 0.5)
            return

        self.interval_s = min(
            self.max_interval_s,
            max(self.interval_s + self.min_interval_s, self.interval_s * 1.5),
        )


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
    retention_ttl_ms: int | None = None,
    latency_recorder: QueueLatencyRecorder | None = None,
    producer_backpressure: AdaptiveProducerBackpressure | None = None,
    protocol_create_inflight_batches: int = 2,
    max_pending_claim_credits: int = 0,
    client_kwargs: dict[str, object] | None = None,
) -> dict[str, int]:
    flow = BenchFlowClient(url, transport, create_batch_size, client_kwargs=client_kwargs)
    created = 0
    pending_creates: list[tuple[Future[int], list[int], int]] = []
    max_inflight = max(1, protocol_create_inflight_batches if is_protocol_url(url) else 1)

    def publish_created(batch: list[int], batch_count: int) -> None:
        if latency_recorder is not None:
            latency_recorder.mark_created_indices(run_id, batch)
        if wake_coordinator is not None:
            if partition_mode == "auto":
                partition_counts: dict[int, int] = {}
                for index in batch:
                    partition_index = auto_partition_index_for_flow_id(f"{run_id}:flow:{index}")
                    partition_counts[partition_index] = partition_counts.get(partition_index, 0) + 1
                for partition_index, count in partition_counts.items():
                    wake_coordinator.notify_partition(partition_index, count)
            else:
                partition_counts = {}
                for index in batch:
                    partition_index = index % max(partitions, 1)
                    partition_counts[partition_index] = partition_counts.get(partition_index, 0) + 1
                for partition_index, count in partition_counts.items():
                    wake_coordinator.notify_partition(partition_index, count)

    def finish_create_future(future: Future[int], batch: list[int], batch_count: int) -> None:
        nonlocal created
        created += future.result()
        publish_created(batch, batch_count)

    def drain_one_create() -> None:
        nonlocal pending_creates
        if not pending_creates:
            return
        done, _pending = wait(
            [future for future, _batch, _batch_count in pending_creates],
            return_when=FIRST_COMPLETED,
        )
        remaining = []
        for item in pending_creates:
            future, batch, batch_count = item
            if future in done:
                finish_create_future(future, batch, batch_count)
            else:
                remaining.append(item)
        pending_creates = remaining

    def drain_all_creates() -> None:
        nonlocal pending_creates
        by_future = {future: (batch, batch_count) for future, batch, batch_count in pending_creates}
        for future in as_completed(by_future):
            batch, batch_count = by_future[future]
            finish_create_future(future, batch, batch_count)
        pending_creates = []

    def submit_or_create_sync(batch: list[int]) -> None:
        nonlocal created
        batch_count = len(batch)
        if producer_backpressure is not None:
            if wake_coordinator is not None and max_pending_claim_credits > 0:
                producer_backpressure.wait_for_pending_credits(
                    wake_coordinator.total_credit,
                    max_pending_credits=max_pending_claim_credits,
                )
            producer_backpressure.wait_for_creates(batch_count)
        future = (
            flow.enqueue_many_future(
                run_id=run_id,
                flow_type=flow_type,
                indices=batch,
                partitions=partitions,
                payload=payload,
                partition_mode=partition_mode,
                independent_many=independent_many,
                retention_ttl_ms=retention_ttl_ms,
            )
            if max_inflight > 1
            else None
        )
        if future is None:
            created += flow.enqueue_many(
                run_id=run_id,
                flow_type=flow_type,
                indices=batch,
                partitions=partitions,
                payload=payload,
                partition_mode=partition_mode,
                independent_many=independent_many,
                retention_ttl_ms=retention_ttl_ms,
            )
            publish_created(batch, batch_count)
            return

        pending_creates.append((future, list(batch), batch_count))
        while len(pending_creates) >= max_inflight:
            drain_one_create()

    if partition_mode in {"auto", "explicit"} and transport == "many":
        partition_buffers: dict[int, list[int]] = {}

        def flush_partition(partition_index: int) -> None:
            nonlocal created
            batch = partition_buffers.get(partition_index)
            if not batch:
                return
            submit_or_create_sync(list(batch))
            partition_buffers[partition_index] = []

        for index in indices:
            partition_index = (
                auto_partition_index_for_flow_id(f"{run_id}:flow:{index}")
                if partition_mode == "auto"
                else index % max(partitions, 1)
            )
            bucket = partition_buffers.setdefault(partition_index, [])
            bucket.append(index)
            if len(bucket) >= max(create_batch_size, 1):
                flush_partition(partition_index)

        for partition_index in list(partition_buffers):
            flush_partition(partition_index)

        drain_all_creates()
        return {"created": created, **flow.pipeline_stats("create")}

    for batch in chunks(indices, max(create_batch_size, 1)):
        submit_or_create_sync(list(batch))
    drain_all_creates()
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
    complete_independent_many: bool,
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
    claim_block_ms: int | None,
    claim_drain_block_ms: int | None,
    claim_state: str | None,
    claim_states: list[str] | None,
    claim_partition_batch_size: int,
    claim_drain_batches: int,
    worker_capacity: int,
    server_shards: int,
    producers_done: threading.Event,
    claimed_total: list[int],
    completed: list[int],
    completed_ids: set[str],
    duplicate_completions: list[int],
    completed_lock: threading.Lock,
    wake_coordinator: PartitionWakeCoordinator | None,
    track_duplicates: bool,
    claim_prefetch: int = 0,
    protocol_wake_hints: bool = False,
    fuse_complete_claim: bool = False,
    shared_client: FlowClient | None = None,
    latency_recorder: QueueLatencyRecorder | None = None,
    client_kwargs: dict[str, object] | None = None,
) -> dict[str, int]:
    flow = BenchFlowClient(
        url,
        transport,
        claim_batch_size,
        client_kwargs=client_kwargs,
        base_client=shared_client,
    )
    worker = f"{run_id}:worker:{worker_index}"
    local_completed = 0
    claim_round = 0
    claim_calls = 0
    empty_claims = 0
    claimed_items = 0
    local_duplicate_completions = 0
    max_claim_batch = 0
    fallback_claims = 0
    fallback_idle_rounds = 0
    last_claimed_seen = 0
    wake_coalesce_sleeps = 0
    wake_coalesce_seconds = 0.0
    base_idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
    max_idle_sleep_s = max(max_idle_sleep_ms, idle_sleep_ms, 0.0) / 1000.0
    idle_sleep_s = base_idle_sleep_s
    wake_coalesce_s = max(wake_coalesce_ms, 0.0) / 1000.0
    partial_claim_retries = max(partial_claim_retries, 0)
    capacity_enabled = worker_capacity > 0
    worker_capacity = max(worker_capacity, 1) if capacity_enabled else 0
    if partition_mode == "auto":
        owned_partitions = [
            p
            for p in range(partitions)
            if auto_partition_owner(p, worker_count, server_shards) == worker_index
        ]
    else:
        owned_partitions = [p for p in range(partitions) if p % worker_count == worker_index]
    owned_partition_keys = [
        benchmark_partition_key(
            partition_mode=partition_mode,
            partition_index=index,
            partitions=partitions,
            run_id=run_id,
        )
        for index in owned_partitions
    ]
    same_partition_group = None
    if partition_mode == "auto":

        def same_partition_group(first: int, candidate: int) -> bool:
            return auto_partition_server_shard_for_index(
                first, server_shards
            ) == auto_partition_server_shard_for_index(candidate, server_shards)

    fallback_round = 0
    complete_executor = (
        ThreadPoolExecutor(max_workers=complete_async_depth) if complete_async_depth > 0 else None
    )
    complete_clients = (
        [flow]
        if is_protocol_url(url) and complete_async_depth > 0
        else [
            BenchFlowClient(url, transport, claim_batch_size)
            for _ in range(max(complete_async_depth, 0))
        ]
    )
    complete_client_index = 0
    pending_completions = []

    def done() -> bool:
        with completed_lock:
            return completed[0] >= total_flows

    def all_claimed() -> bool:
        with completed_lock:
            return claimed_total[0] >= total_flows

    def current_claim_block_ms() -> int | None:
        value = claim_drain_block_ms if producers_done.is_set() else claim_block_ms
        return value if value is not None and value > 0 else None

    def should_scan_owned_partitions() -> bool:
        block_ms = current_claim_block_ms()
        return (
            block_ms is not None and block_ms > 0 and not claim_any and bool(owned_partition_keys)
        )

    def should_block_after_owned_partition_scan() -> bool:
        return should_scan_owned_partitions() and producers_done.is_set()

    def owned_partition_scan_pages() -> int:
        if not owned_partition_keys:
            return 1
        return max(
            1,
            (len(owned_partition_keys) + max(claim_partition_batch_size, 1) - 1)
            // max(claim_partition_batch_size, 1),
        )

    def claim_partition_page():
        nonlocal claim_round
        if owned_partitions:
            partition_indices = partition_indices_from_owned(
                owned_partitions,
                claim_round,
                max(claim_partition_batch_size, 1),
            )
        else:
            partition_indices = partition_indices_for_claim(
                worker_index,
                worker_count,
                partitions,
                claim_round,
                max(claim_partition_batch_size, 1),
            )
        claim_round += max(len(partition_indices), 1)
        keys = [
            benchmark_partition_key(
                partition_mode=partition_mode,
                partition_index=index,
                partitions=partitions,
                run_id=run_id,
            )
            for index in partition_indices
        ]
        key = keys[0] if len(keys) == 1 else None
        return key, None if key is not None else keys

    def claim_owned_partition_block():
        if len(owned_partition_keys) == 1:
            return owned_partition_keys[0], None
        return None, list(owned_partition_keys)

    def claim_once(partition_key, partition_keys, limit, block_ms):
        return flow.claim_due(
            flow_type=flow_type,
            state=claim_state,
            states=claim_states,
            worker=worker,
            partition_key=partition_key,
            partition_keys=None if partition_key is not None else partition_keys,
            limit=limit,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            claim_priority=claim_priority,
            claim_job_only=claim_job_only,
            claim_block_ms=block_ms,
        )

    def record_completed_jobs(jobs) -> None:
        nonlocal local_completed, local_duplicate_completions
        if not track_duplicates:
            count = len(jobs)
            with completed_lock:
                completed[0] += count
            local_completed += count
            return

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
            independent_many=complete_independent_many,
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
            "fallback_claims": fallback_claims,
            "worker_capacity": worker_capacity,
            "wake_coalesce_sleeps": wake_coalesce_sleeps,
            "wake_coalesce_ms": wake_coalesce_seconds * 1000.0,
            **flow.pipeline_stats("process"),
        }

    def handle_jobs(jobs, partition_key: str | None) -> None:
        nonlocal claimed_items, complete_client_index, max_claim_batch
        max_claim_batch = max(max_claim_batch, len(jobs))
        claimed_items += len(jobs)
        with completed_lock:
            claimed_total[0] += len(jobs)
        if latency_recorder is not None:
            latency_recorder.mark_claimed(jobs)
        flow.do_work(work_command, run_id, jobs)

        if complete_executor is None:
            complete_jobs(flow, jobs, partition_key)
            record_completed_jobs(jobs)
            return

        while len(pending_completions) >= complete_async_depth:
            drain_completed_completions(block=True)

        client = complete_clients[complete_client_index % len(complete_clients)]
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

        if capacity_enabled:
            available_capacity = worker_capacity - (claimed_items - local_completed)
            if available_capacity <= 0:
                if pending_completions:
                    drain_completed_completions(block=True)
                elif idle_sleep_s > 0:
                    time.sleep(idle_sleep_s)
                continue
            claim_credit_limit = available_capacity
        else:
            available_capacity = claim_batch_size
            claim_credit_limit = claim_batch_size
        partition_index = None
        partition_key = None
        partition_keys = None
        partition_credit = 0

        if wake_coordinator is not None and not claim_any:
            try:
                partition_indices, partition_credit = wake_coordinator.next_partitions(
                    worker_index,
                    idle_sleep_s,
                    max(claim_partition_batch_size, 1),
                    claim_credit_limit,
                    same_group=same_partition_group,
                )
                partition_index = partition_indices[0]
                fallback_idle_rounds = 0
            except queue.Empty:
                if producers_done.is_set() and owned_partitions:
                    with completed_lock:
                        observed_claimed = claimed_total[0]

                    if wake_coordinator.total_credit() > 0 or observed_claimed != last_claimed_seen:
                        last_claimed_seen = observed_claimed
                        fallback_idle_rounds = 0
                        continue

                    fallback_idle_rounds += 1
                    if fallback_idle_rounds < 3:
                        continue

                    partition_index = owned_partitions[fallback_round % len(owned_partitions)]
                    partition_indices = [partition_index]
                    partition_credit = min(claim_batch_size, available_capacity)
                    fallback_round += 1
                    fallback_claims += 1
                else:
                    continue
            if partition_credit <= 0:
                continue
            if wake_coalesce_s > 0 and not producers_done.is_set():
                if partition_credit >= claim_batch_size:
                    coalesce_sleep_s = 0.0
                elif partition_credit >= max(claim_batch_size // 2, 1):
                    coalesce_sleep_s = min(wake_coalesce_s, 0.001)
                else:
                    coalesce_sleep_s = min(wake_coalesce_s, 0.002)

                if coalesce_sleep_s > 0:
                    time.sleep(coalesce_sleep_s)
                    wake_coalesce_sleeps += 1
                    wake_coalesce_seconds += coalesce_sleep_s
                    partition_credit += wake_coordinator.take_credit(worker_index, partition_index)
                    extra_indices, extra_credit = (
                        wake_coordinator.next_partitions(
                            worker_index,
                            0,
                            max(claim_partition_batch_size - len(partition_indices), 0),
                            claim_credit_limit - partition_credit,
                            same_group=same_partition_group,
                        )
                        if len(partition_indices) < claim_partition_batch_size
                        else ([], 0)
                    )
                    partition_indices.extend(extra_indices)
                    partition_credit += extra_credit
            partition_keys = [
                benchmark_partition_key(
                    partition_mode=partition_mode,
                    partition_index=index,
                    partitions=partitions,
                    run_id=run_id,
                )
                for index in partition_indices
            ]
            partition_key = partition_keys[0] if len(partition_keys) == 1 else None

            remaining_credit = max(partition_credit, 1)

            while remaining_credit > 0:
                if done():
                    return finish()
                if capacity_enabled:
                    available_capacity = worker_capacity - (claimed_items - local_completed)
                    if available_capacity <= 0:
                        break
                else:
                    available_capacity = claim_batch_size
                limit = min(claim_batch_size, remaining_credit, available_capacity)
                claim_calls += 1
                jobs = flow.claim_due(
                    flow_type=flow_type,
                    state=claim_state,
                    states=claim_states,
                    worker=worker,
                    partition_key=partition_key,
                    partition_keys=None if partition_key is not None else partition_keys,
                    limit=limit,
                    reclaim_expired=reclaim_expired,
                    reclaim_ratio=reclaim_ratio,
                    claim_priority=claim_priority,
                    claim_job_only=claim_job_only,
                    claim_block_ms=current_claim_block_ms(),
                )
                if not jobs:
                    empty_claims += 1
                    break
                handle_jobs(jobs, partition_key)
                remaining_credit -= len(jobs)
                if len(jobs) < limit:
                    break
            continue

        scan_owned_partitions = should_scan_owned_partitions()
        block_after_scan = should_block_after_owned_partition_scan()
        scan_pages = owned_partition_scan_pages() if scan_owned_partitions else 1
        jobs = []

        for _ in range(scan_pages):
            if not claim_any:
                partition_key, partition_keys = claim_partition_page()

            claim_calls += 1
            jobs = claim_once(
                partition_key,
                partition_keys,
                min(claim_batch_size, available_capacity),
                None if scan_owned_partitions else current_claim_block_ms(),
            )
            if jobs:
                break
            empty_claims += 1

        if not jobs and block_after_scan:
            partition_key, partition_keys = claim_owned_partition_block()
            claim_calls += 1
            jobs = claim_once(
                partition_key,
                partition_keys,
                min(claim_batch_size, available_capacity),
                current_claim_block_ms(),
            )
            if not jobs:
                empty_claims += 1

        if not jobs:
            if idle_sleep_s > 0:
                time.sleep(idle_sleep_s)
                idle_sleep_s = min(max_idle_sleep_s, max(idle_sleep_s * 2, base_idle_sleep_s))
            continue

        idle_sleep_s = base_idle_sleep_s
        handle_jobs(jobs, partition_key)


def run_queue_api_worker(
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
    complete_independent_many: bool,
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
    claim_block_ms: int | None,
    claim_state: str | None,
    claim_states: list[str] | None,
    claim_drain_block_ms: int | None,
    claim_partition_batch_size: int,
    claim_drain_batches: int,
    worker_capacity: int,
    server_shards: int,
    producers_done: threading.Event,
    claimed_total: list[int],
    completed: list[int],
    completed_ids: set[str],
    duplicate_completions: list[int],
    completed_lock: threading.Lock,
    wake_coordinator: PartitionWakeCoordinator | None,
    track_duplicates: bool,
    claim_prefetch: int = 0,
    protocol_wake_hints: bool = False,
    fuse_complete_claim: bool = False,
    shared_client: FlowClient | None = None,
    latency_recorder: QueueLatencyRecorder | None = None,
    client_kwargs: dict[str, object] | None = None,
) -> dict[str, int]:
    del (
        complete_batch,
        transport,
        partial_claim_retries,
        partial_claim_delay_ms,
        claim_job_only,
    )

    client = (
        shared_client
        if shared_client is not None
        else FlowClient.from_url(url, **(client_kwargs or {}))
    )
    partition_keys = None
    owned_partitions = None
    if not claim_any:
        if partition_mode == "auto":
            owned_partitions = [
                p
                for p in range(partitions)
                if auto_partition_owner(p, worker_count, server_shards) == worker_index
            ]
        else:
            owned_partitions = [p for p in range(partitions) if p % worker_count == worker_index]
        if partition_mode == "auto":
            owned_partitions.sort(
                key=lambda partition_index: (
                    auto_partition_server_shard_for_index(partition_index, server_shards),
                    partition_index,
                )
            )
        if not owned_partitions:
            return {
                "completed": 0,
                "duplicate_completions": 0,
                "claim_calls": 0,
                "empty_claims": 0,
                "claimed_items": 0,
                "max_claim_batch": 0,
                "fallback_claims": 0,
                "worker_capacity": 0,
                "wake_coalesce_sleeps": 0,
                "wake_coalesce_ms": 0.0,
                "process_pipeline_flushes": 0,
                "process_pipeline_commands": 0,
                "process_pipeline_max_depth": 0,
            }
        partition_keys = [
            benchmark_partition_key(
                partition_mode=partition_mode,
                partition_index=partition_index,
                partitions=partitions,
                run_id=run_id,
            )
            for partition_index in owned_partitions
        ]

    completion_clients = (
        None
        if is_protocol_url(url) or complete_async_depth <= 0
        else [FlowClient.from_url(url) for _ in range(complete_async_depth)]
    )
    effective_concurrency = worker_capacity if worker_capacity > 0 else claim_batch_size
    effective_claim_partition_batch_size = claim_partition_batch_size
    if is_protocol_url(url) and partition_keys:
        effective_claim_partition_batch_size = max(
            claim_partition_batch_size,
            len(partition_keys),
        )
    effective_claim_prefetch = 0 if wake_coordinator is not None else claim_prefetch
    effective_block_ms = None if wake_coordinator is not None else claim_block_ms

    worker = QueueFlowWorker(
        client,
        type=flow_type,
        worker=f"{run_id}:worker:{worker_index}",
        state=claim_state,
        states=claim_states,
        concurrency=effective_concurrency,
        batch_size=claim_batch_size,
        priority=claim_priority,
        reclaim_expired=reclaim_expired,
        reclaim_ratio=reclaim_ratio,
        idle_sleep_s=max(idle_sleep_ms, 0.0) / 1000.0,
        max_idle_sleep_s=max(max_idle_sleep_ms, idle_sleep_ms, 0.0) / 1000.0,
        complete_independent=complete_independent_many,
        partition_keys=partition_keys,
        claim_partition_batch_size=effective_claim_partition_batch_size,
        claim_drain_batches=claim_drain_batches,
        claim_prefetch=effective_claim_prefetch,
        protocol_wake_hints=protocol_wake_hints and wake_coordinator is None,
        block_ms=effective_block_ms,
        claim_scan_block_ms=claim_drain_block_ms,
        complete_async_depth=complete_async_depth,
        fuse_complete_claim=fuse_complete_claim and is_protocol_url(url),
        completion_clients=completion_clients,
    )
    local_completed = 0
    local_duplicate_completions = 0
    claim_calls = 0
    empty_claims = 0
    claimed_items = 0
    max_claim_batch = 0
    fallback_claims = 0
    idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
    max_idle_sleep_s = max(max_idle_sleep_ms, idle_sleep_ms, 0.0) / 1000.0
    wake_coalesce_s = max(wake_coalesce_ms, 0.0) / 1000.0
    fallback_scheduler = AdaptiveFallbackScheduler(
        min_interval_s=max(idle_sleep_s, 0.001),
        max_interval_s=max(max_idle_sleep_s, 0.25),
        initial_interval_s=max(max_idle_sleep_s, idle_sleep_s, 0.01),
    )
    capacity_limit = (
        worker_capacity
        if worker_capacity > 0
        else claim_batch_size * max(1, complete_async_depth + 1)
    )
    claim_credit_batch_limit = claim_batch_size * max(claim_drain_batches, 1)

    def done() -> bool:
        with completed_lock:
            return completed[0] >= total_flows

    def all_claimed() -> bool:
        with completed_lock:
            return claimed_total[0] >= total_flows

    def handle_batch(jobs):
        if latency_recorder is not None:
            latency_recorder.mark_claimed(jobs)
        if work_command == "incr":
            for _job in jobs:
                client.executor.execute_command("INCR", f"{run_id}:counter")
        return result

    def record_batch(batch: QueueFlowWorkerResult) -> None:
        nonlocal claimed_items, max_claim_batch, local_completed
        if batch.claimed > 0:
            claimed_items += batch.claimed
            max_claim_batch = max(max_claim_batch, batch.claimed)

        if batch.claimed == 0 and batch.completed == 0:
            return

        with completed_lock:
            claimed_total[0] += batch.claimed
            if not track_duplicates:
                completed[0] += batch.completed
                local_completed += batch.completed
            else:
                completed[0] += batch.completed
                local_completed += batch.completed

    def outstanding_claimed() -> int:
        return max(claimed_items - local_completed, 0)

    def available_worker_capacity() -> int:
        return max(capacity_limit - outstanding_claimed(), 0)

    try:
        while True:
            if done():
                break
            if producers_done.is_set() and all_claimed():
                break

            available_capacity = available_worker_capacity()
            if available_capacity <= 0:
                flushed = worker.flush()
                if (
                    flushed.claimed > 0
                    or flushed.completed > 0
                    or flushed.retried > 0
                    or flushed.failed > 0
                ):
                    claim_calls += flushed.claim_calls
                    record_batch(flushed)
                    continue
                if idle_sleep_s > 0:
                    time.sleep(min(idle_sleep_s, 0.001))
                continue

            if wake_coordinator is not None and not claim_any:
                claim_credit_limit = min(claim_credit_batch_limit, available_capacity)
                try:
                    partition_indices, partition_credit = wake_coordinator.next_partitions(
                        worker_index,
                        idle_sleep_s,
                        max(claim_partition_batch_size, 1),
                        claim_credit_limit,
                        same_group=(
                            (
                                lambda first, other: (
                                    auto_partition_server_shard_for_index(first, server_shards)
                                    == auto_partition_server_shard_for_index(other, server_shards)
                                )
                            )
                            if partition_mode == "auto"
                            else None
                        ),
                    )
                except queue.Empty:
                    if producers_done.is_set() and all_claimed():
                        break

                    now = time.perf_counter()
                    if wake_coordinator.worker_credit(worker_index) > 0:
                        continue

                    if not producers_done.is_set():
                        idle_sleep_s = min(
                            max_idle_sleep_s,
                            max(idle_sleep_s * 2, idle_sleep_s + 0.001),
                        )
                        continue

                    if not fallback_scheduler.should_scan(now):
                        idle_sleep_s = min(
                            max_idle_sleep_s,
                            max(idle_sleep_s * 2, idle_sleep_s + 0.001),
                        )
                        continue

                    fallback_claims += 1
                    batch = worker.run_batch_once(handle_batch)
                    fallback_scheduler.record_scan(now, claimed=batch.claimed)
                    claim_calls += batch.claim_calls
                    if (
                        batch.claimed == 0
                        and batch.completed == 0
                        and batch.retried == 0
                        and batch.failed == 0
                    ):
                        if batch.claim_calls > 0:
                            empty_claims += 1
                        continue
                    if batch.claimed > 0:
                        idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
                    record_batch(batch)
                    continue

                if partition_credit <= 0:
                    continue

                if (
                    wake_coalesce_s > 0
                    and not producers_done.is_set()
                    and partition_credit < min(claim_batch_size, claim_credit_limit)
                    and len(partition_indices) < max(claim_partition_batch_size, 1)
                ):
                    time.sleep(min(wake_coalesce_s, 0.001))
                    extra_indices, extra_credit = wake_coordinator.next_partitions(
                        worker_index,
                        0.0,
                        max(claim_partition_batch_size, 1) - len(partition_indices),
                        claim_credit_limit - partition_credit,
                        same_group=(
                            (
                                lambda first, other: (
                                    auto_partition_server_shard_for_index(first, server_shards)
                                    == auto_partition_server_shard_for_index(other, server_shards)
                                )
                            )
                            if partition_mode == "auto"
                            else None
                        ),
                    )
                    partition_indices.extend(extra_indices)
                    partition_credit += extra_credit

                partition_keys = [
                    benchmark_partition_key(
                        partition_mode=partition_mode,
                        partition_index=index,
                        partitions=partitions,
                        run_id=run_id,
                    )
                    for index in partition_indices
                ]
                batch = worker.run_batch_once_for_partition_keys(
                    handle_batch,
                    partition_keys,
                    claim_credit=min(partition_credit, available_worker_capacity()),
                    block_ms=None,
                )
            else:
                batch = worker.run_batch_once(handle_batch)
            claim_calls += batch.claim_calls
            if (
                batch.claimed == 0
                and batch.completed == 0
                and batch.retried == 0
                and batch.failed == 0
            ):
                if batch.claim_calls > 0:
                    empty_claims += 1
                if producers_done.is_set() and all_claimed():
                    break
                if wake_coordinator is not None:
                    continue
                if idle_sleep_s > 0:
                    time.sleep(idle_sleep_s)
                    idle_sleep_s = min(max_idle_sleep_s, max(idle_sleep_s * 2, idle_sleep_s))
                continue

            if batch.claimed > 0:
                idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
            record_batch(batch)

        record_batch(worker.flush())
        return {
            "completed": local_completed,
            "duplicate_completions": local_duplicate_completions,
            "claim_calls": claim_calls,
            "empty_claims": empty_claims,
            "claimed_items": claimed_items,
            "max_claim_batch": max_claim_batch,
            "fallback_claims": fallback_claims,
            "worker_capacity": effective_concurrency,
            "wake_coalesce_sleeps": 0,
            "wake_coalesce_ms": 0.0,
            "fallback_scan_interval_ms": fallback_scheduler.interval_s * 1000.0,
            "process_pipeline_flushes": 0,
            "process_pipeline_commands": 0,
            "process_pipeline_max_depth": 0,
            "effective_claim_prefetch": effective_claim_prefetch,
            "protocol_wake_hints": int(protocol_wake_hints and wake_coordinator is None),
        }
    finally:
        worker.close()
        if shared_client is None:
            client.close()


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


def partition_indices_for_claim(
    worker_index: int,
    worker_count: int,
    partitions: int,
    claim_round: int,
    count: int,
) -> list[int]:
    if count <= 1:
        return [partition_index_for_claim(worker_index, worker_count, partitions, claim_round)]

    seen: set[int] = set()
    indices: list[int] = []
    max_attempts = max(partitions, count, 1)

    for offset in range(max_attempts):
        index = partition_index_for_claim(
            worker_index,
            worker_count,
            partitions,
            claim_round + offset,
        )
        if index in seen:
            continue
        seen.add(index)
        indices.append(index)
        if len(indices) >= count:
            break

    return indices


def partition_indices_from_owned(
    owned_partitions: list[int],
    claim_round: int,
    count: int,
) -> list[int]:
    if not owned_partitions:
        return []

    count = min(max(count, 1), len(owned_partitions))
    start = claim_round % len(owned_partitions)
    return [owned_partitions[(start + offset) % len(owned_partitions)] for offset in range(count)]


def run_queued_throughput(args: argparse.Namespace) -> dict[str, float | int | str | bool]:
    cpu_started = time.process_time()
    run_id = f"py-sdk-bench-{uuid.uuid4().hex}"
    flow_type = f"{FLOW_TYPE}:{run_id}"
    claim_states = parse_claim_states(args.claim_states)
    claim_state = (
        None if claim_states is not None or args.claim_state == "omitted" else args.claim_state
    )
    indices = list(range(args.flows))
    payload = payload_bytes(args.payload_bytes)
    result = payload_bytes(args.result_bytes) if args.result_bytes > 0 else None
    completed = [0]
    claimed_total = [0]
    completed_ids: set[str] = set()
    duplicate_completions = [0]
    completed_lock = threading.Lock()
    producers_done = threading.Event()
    latency_sample_rate = getattr(
        args,
        "latency_sample_rate",
        DBOS_QUEUE_DEFAULTS["latency_sample_rate"],
    )
    latency_recorder = QueueLatencyRecorder(latency_sample_rate)
    producer_backpressure = AdaptiveProducerBackpressure(
        enabled=getattr(
            args,
            "adaptive_producer_backpressure",
            DBOS_QUEUE_DEFAULTS["adaptive_producer_backpressure"],
        ),
        target_queue_latency_ms=getattr(
            args,
            "producer_target_queue_latency_ms",
            DBOS_QUEUE_DEFAULTS["producer_target_queue_latency_ms"],
        ),
        min_rate_per_sec=getattr(
            args,
            "producer_min_rate_per_sec",
            DBOS_QUEUE_DEFAULTS["producer_min_rate_per_sec"],
        ),
        max_rate_per_sec=getattr(
            args,
            "producer_max_rate_per_sec",
            DBOS_QUEUE_DEFAULTS["producer_max_rate_per_sec"],
        ),
    )
    latency_recorder.add_observer(producer_backpressure.observe_queue_latencies)
    effective_worker_mode = (
        "queue-api"
        if args.worker_api == "queue"
        else "polling"
        if args.claim_any
        else args.worker_mode
    )
    partition_mode = args.partition_mode
    if args.claim_any and partition_mode == "explicit":
        partition_mode = "auto"
    worker_partitions = AUTO_PARTITION_BUCKETS if partition_mode == "auto" else args.partitions
    worker_capacity = args.worker_capacity
    worker_lanes = protocol_queue_worker_lanes(
        url=args.url,
        worker_api=args.worker_api,
        workers=args.workers,
        claim_any=args.claim_any,
        partitions=worker_partitions,
        server_shards=args.server_shards,
    )
    producer_max_pending_credits = getattr(
        args,
        "producer_max_pending_credits",
        DBOS_QUEUE_DEFAULTS["producer_max_pending_credits"],
    )
    protocol_kwargs: dict[str, object] = (
        {
            "lanes": getattr(args, "protocol_lanes", DBOS_QUEUE_DEFAULTS["protocol_lanes"]),
            "max_connections": max(
                1,
                getattr(
                    args,
                    "protocol_worker_connections",
                    DBOS_QUEUE_DEFAULTS["protocol_worker_connections"],
                ),
            ),
        }
        if is_protocol_url(args.url)
        else {}
    )
    shared_worker_client = (
        FlowClient.from_url(
            args.url,
            **protocol_kwargs,
        )
        if is_protocol_url(args.url)
        else None
    )
    wake_coordinator = None
    if (
        effective_worker_mode in {"blocking", "queue-api"}
        and not args.claim_any
        and worker_partitions > 0
    ):
        if partition_mode == "auto":
            wake_owner = lambda partition_index: auto_partition_owner(  # noqa: E731
                partition_index,
                worker_lanes,
                args.server_shards,
            )
        else:
            wake_owner = lambda partition_index: partition_index % max(worker_lanes, 1)  # noqa: E731
        wake_coordinator = PartitionWakeCoordinator(
            worker_lanes,
            worker_partitions,
            owner_for=wake_owner,
        )

    if partition_mode == "auto" and args.transport == "many":
        create_ranges = [[] for _ in range(args.producers)]
        for index in indices:
            owner = auto_partition_index_for_flow_id(f"{run_id}:flow:{index}") % args.producers
            create_ranges[owner].append(index)
    else:
        create_ranges = [indices[offset :: args.producers] for offset in range(args.producers)]
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
                retention_ttl_ms=getattr(args, "retention_ttl_ms", 0) or None,
                latency_recorder=latency_recorder,
                producer_backpressure=producer_backpressure,
                protocol_create_inflight_batches=getattr(
                    args,
                    "protocol_create_inflight_batches",
                    DBOS_QUEUE_DEFAULTS["protocol_create_inflight_batches"],
                ),
                max_pending_claim_credits=producer_max_pending_credits,
                client_kwargs=protocol_kwargs,
            )
            for batch in create_ranges
        ]

    def submit_worker_jobs(executor: ThreadPoolExecutor):
        worker_fn = run_queue_api_worker if args.worker_api == "queue" else run_claim_worker
        return [
            executor.submit(
                worker_fn,
                url=args.url,
                run_id=run_id,
                flow_type=flow_type,
                worker_index=worker_index,
                worker_count=worker_lanes,
                partitions=worker_partitions,
                partition_mode=partition_mode,
                claim_any=args.claim_any,
                claim_batch_size=args.claim_batch_size,
                complete_batch=args.complete_batch,
                complete_async_depth=args.complete_async_depth,
                independent_many=args.independent_many,
                complete_independent_many=args.complete_independent_many,
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
                claim_block_ms=args.claim_block_ms,
                claim_drain_block_ms=args.claim_drain_block_ms,
                claim_state=claim_state,
                claim_states=claim_states,
                claim_partition_batch_size=args.claim_partition_batch_size,
                claim_drain_batches=args.claim_drain_batches,
                claim_prefetch=getattr(
                    args,
                    "claim_prefetch",
                    DBOS_QUEUE_DEFAULTS["claim_prefetch"],
                ),
                protocol_wake_hints=getattr(
                    args,
                    "protocol_wake_hints",
                    DBOS_QUEUE_DEFAULTS["protocol_wake_hints"],
                ),
                fuse_complete_claim=getattr(
                    args,
                    "fuse_complete_claim",
                    DBOS_QUEUE_DEFAULTS["fuse_complete_claim"],
                ),
                client_kwargs=protocol_kwargs,
                worker_capacity=worker_capacity,
                server_shards=args.server_shards,
                producers_done=producers_done,
                claimed_total=claimed_total,
                completed=completed,
                completed_ids=completed_ids,
                duplicate_completions=duplicate_completions,
                completed_lock=completed_lock,
                wake_coordinator=wake_coordinator,
                track_duplicates=args.track_duplicates,
                shared_client=shared_worker_client,
                latency_recorder=latency_recorder,
            )
            for worker_index in range(worker_lanes)
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
        with ThreadPoolExecutor(max_workers=worker_lanes) as executor:
            worker_futures = submit_worker_jobs(executor)
            for future in as_completed(worker_futures):
                worker_results.append(future.result())
        process_finished = time.perf_counter()
    else:
        with ThreadPoolExecutor(max_workers=args.producers + worker_lanes) as executor:
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

    if shared_worker_client is not None:
        shared_worker_client.close()

    total_seconds = process_finished - started
    client_cpu_seconds = max(time.process_time() - cpu_started, 0.0)
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
    process_pipeline_commands = sum(
        result["process_pipeline_commands"] for result in worker_results
    )
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
    process_wake_coalesce_sleeps = sum(result["wake_coalesce_sleeps"] for result in worker_results)
    process_wake_coalesce_ms = sum(result["wake_coalesce_ms"] for result in worker_results)
    process_fallback_claims = sum(result["fallback_claims"] for result in worker_results)
    process_worker_capacity = max(
        (result["worker_capacity"] for result in worker_results),
        default=worker_capacity,
    )
    effective_claim_prefetch = max(
        (result.get("effective_claim_prefetch", 0) for result in worker_results),
        default=0,
    )
    queue_latency_stats = latency_recorder.summary()

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
        "worker_lanes": worker_lanes,
        "producers": args.producers,
        "partitions": args.partitions,
        "claim_any": args.claim_any,
        "partition_mode": partition_mode,
        "worker_mode": effective_worker_mode,
        "worker_api": args.worker_api,
        "claim_batch_size": args.claim_batch_size,
        "worker_capacity": process_worker_capacity,
        "create_batch_size": args.create_batch_size,
        "complete_batch": args.complete_batch,
        "complete_async_depth": args.complete_async_depth,
        "fuse_complete_claim": getattr(
            args,
            "fuse_complete_claim",
            DBOS_QUEUE_DEFAULTS["fuse_complete_claim"],
        ),
        "independent_many": args.independent_many,
        "complete_independent_many": args.complete_independent_many,
        "transport": args.transport,
        "payload_bytes": args.payload_bytes,
        "result_bytes": args.result_bytes,
        "retention_ttl_ms": getattr(args, "retention_ttl_ms", 0) or 0,
        "work_command": args.work_command,
        "idle_sleep_ms": args.idle_sleep_ms,
        "max_idle_sleep_ms": args.max_idle_sleep_ms,
        "wake_coalesce_ms": args.wake_coalesce_ms,
        "partial_claim_retries": args.partial_claim_retries,
        "partial_claim_delay_ms": args.partial_claim_delay_ms,
        "reclaim_expired": args.reclaim_expired,
        "reclaim_ratio": args.reclaim_ratio,
        "claim_priority": args.claim_priority,
        "claim_state": args.claim_state,
        "claim_states": args.claim_states or "",
        "claim_job_only": args.claim_job_only,
        "claim_block_ms": args.claim_block_ms if args.claim_block_ms is not None else -1,
        "claim_drain_block_ms": (
            args.claim_drain_block_ms if args.claim_drain_block_ms is not None else -1
        ),
        "track_duplicates": args.track_duplicates,
        "claim_partition_batch_size": args.claim_partition_batch_size,
        "claim_drain_batches": args.claim_drain_batches,
        "claim_prefetch": getattr(args, "claim_prefetch", DBOS_QUEUE_DEFAULTS["claim_prefetch"]),
        "effective_claim_prefetch": effective_claim_prefetch,
        "server_shards": args.server_shards,
        "protocol_worker_connections": getattr(
            args,
            "protocol_worker_connections",
            DBOS_QUEUE_DEFAULTS["protocol_worker_connections"],
        ),
        "protocol_lanes": getattr(args, "protocol_lanes", DBOS_QUEUE_DEFAULTS["protocol_lanes"]),
        "protocol_create_inflight_batches": getattr(
            args,
            "protocol_create_inflight_batches",
            DBOS_QUEUE_DEFAULTS["protocol_create_inflight_batches"],
        ),
        "producer_max_pending_credits": producer_max_pending_credits,
        "protocol_wake_hints": getattr(
            args,
            "protocol_wake_hints",
            DBOS_QUEUE_DEFAULTS["protocol_wake_hints"],
        ),
        "latency_sample_rate": latency_sample_rate,
        **producer_backpressure.summary(),
        **queue_latency_stats,
        "wake_notifications": wake_coordinator.notifications if wake_coordinator is not None else 0,
        "wake_credits": wake_coordinator.notified_jobs if wake_coordinator is not None else 0,
        "process_wake_coalesce_sleeps": process_wake_coalesce_sleeps,
        "process_wake_coalesce_ms": process_wake_coalesce_ms,
        "process_claim_calls": process_claim_calls,
        "process_empty_claims": process_empty_claims,
        "process_fallback_claims": process_fallback_claims,
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
        "client_cpu_seconds": client_cpu_seconds,
        "client_cpu_percent": (client_cpu_seconds / total_seconds) * 100.0
        if total_seconds > 0
        else 0.0,
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

    parser.add_argument("--flows", type=int, default=DBOS_QUEUE_DEFAULTS["flows"])
    parser.add_argument("--workers", type=int, default=DBOS_QUEUE_DEFAULTS["workers"])
    parser.add_argument("--producers", type=int, default=DBOS_QUEUE_DEFAULTS["producers"])
    parser.add_argument("--partitions", type=int, default=DBOS_QUEUE_DEFAULTS["partitions"])
    parser.add_argument(
        "--claim-batch-size", type=int, default=DBOS_QUEUE_DEFAULTS["claim_batch_size"]
    )
    parser.add_argument("--worker-capacity", type=int, default=0)
    parser.add_argument(
        "--create-batch-size", type=int, default=DBOS_QUEUE_DEFAULTS["create_batch_size"]
    )
    parser.add_argument(
        "--transport",
        choices=("many", "pipeline", "autobatch"),
        default=DBOS_QUEUE_DEFAULTS["transport"],
    )
    parser.add_argument(
        "--partition-mode",
        choices=("explicit", "auto"),
        default=DBOS_QUEUE_DEFAULTS["partition_mode"],
    )
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--result-bytes", type=int, default=0)
    parser.add_argument(
        "--retention-ttl-ms",
        type=int,
        default=0,
        help="Optional terminal Flow retention TTL for created workflows; 0 leaves server default.",
    )
    parser.add_argument("--work-command", choices=("none", "incr"), default="none")
    parser.add_argument("--idle-sleep-ms", type=float, default=10.0)
    parser.add_argument("--max-idle-sleep-ms", type=float, default=50.0)
    parser.add_argument(
        "--worker-mode",
        choices=("polling", "blocking"),
        default=DBOS_QUEUE_DEFAULTS["worker_mode"],
    )
    parser.add_argument(
        "--worker-api",
        choices=("lowlevel", "queue"),
        default=DBOS_QUEUE_DEFAULTS["worker_api"],
    )
    parser.add_argument("--wake-coalesce-ms", type=float, default=5.0)
    parser.add_argument("--partial-claim-retries", type=int, default=1)
    parser.add_argument("--partial-claim-delay-ms", type=float, default=1.0)
    parser.add_argument("--reclaim-expired", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--reclaim-ratio", type=int, default=25)
    parser.add_argument("--claim-priority", type=int, default=0)
    parser.add_argument("--claim-state", choices=("queued", "any", "omitted"), default="queued")
    parser.add_argument("--claim-states", default=None)
    parser.add_argument(
        "--claim-job-only",
        action=argparse.BooleanOptionalAction,
        default=DBOS_QUEUE_DEFAULTS["claim_job_only"],
    )
    parser.add_argument("--claim-block-ms", type=int, default=-1)
    parser.add_argument("--claim-drain-block-ms", type=int, default=-1)
    parser.add_argument("--claim-any", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--complete-batch", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--complete-async-depth", type=int, default=DBOS_QUEUE_DEFAULTS["complete_async_depth"]
    )
    parser.add_argument(
        "--fuse-complete-claim",
        action=argparse.BooleanOptionalAction,
        default=DBOS_QUEUE_DEFAULTS["fuse_complete_claim"],
        help="For protocol queue workers, submit completion and next claim together.",
    )
    parser.add_argument("--independent-many", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--complete-independent-many",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--track-duplicates", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--claim-partition-batch-size",
        type=int,
        default=DBOS_QUEUE_DEFAULTS["claim_partition_batch_size"],
    )
    parser.add_argument(
        "--claim-drain-batches",
        type=int,
        default=DBOS_QUEUE_DEFAULTS["claim_drain_batches"],
    )
    parser.add_argument("--claim-prefetch", type=int, default=DBOS_QUEUE_DEFAULTS["claim_prefetch"])
    parser.add_argument(
        "--protocol-wake-hints",
        action=argparse.BooleanOptionalAction,
        default=DBOS_QUEUE_DEFAULTS["protocol_wake_hints"],
    )
    parser.add_argument("--server-shards", type=int, default=DBOS_QUEUE_DEFAULTS["server_shards"])
    parser.add_argument(
        "--protocol-worker-connections",
        type=int,
        default=DBOS_QUEUE_DEFAULTS["protocol_worker_connections"],
    )
    parser.add_argument("--protocol-lanes", type=int, default=DBOS_QUEUE_DEFAULTS["protocol_lanes"])
    parser.add_argument(
        "--protocol-create-inflight-batches",
        type=int,
        default=DBOS_QUEUE_DEFAULTS["protocol_create_inflight_batches"],
    )
    parser.add_argument(
        "--latency-sample-rate",
        type=int,
        default=DBOS_QUEUE_DEFAULTS["latency_sample_rate"],
        help="Sample every Nth created flow for create-ACK to handler-start latency; 0 disables.",
    )
    parser.add_argument(
        "--adaptive-producer-backpressure",
        action=argparse.BooleanOptionalAction,
        default=DBOS_QUEUE_DEFAULTS["adaptive_producer_backpressure"],
    )
    parser.add_argument(
        "--producer-target-queue-latency-ms",
        type=float,
        default=DBOS_QUEUE_DEFAULTS["producer_target_queue_latency_ms"],
    )
    parser.add_argument(
        "--producer-min-rate-per-sec",
        type=float,
        default=DBOS_QUEUE_DEFAULTS["producer_min_rate_per_sec"],
    )
    parser.add_argument(
        "--producer-max-rate-per-sec",
        type=float,
        default=DBOS_QUEUE_DEFAULTS["producer_max_rate_per_sec"],
        help="0 means unbounded after pressure clears.",
    )
    parser.add_argument(
        "--producer-max-pending-credits",
        type=int,
        default=DBOS_QUEUE_DEFAULTS["producer_max_pending_credits"],
        help="Max created-but-unclaimed wake credits before producers pause; 0 disables.",
    )

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
    if args.worker_capacity < 0:
        parser.error("--worker-capacity must be non-negative")
    if args.claim_partition_batch_size <= 0:
        parser.error("--claim-partition-batch-size must be positive")
    if args.claim_drain_batches <= 0:
        parser.error("--claim-drain-batches must be positive")
    if args.claim_prefetch < 0:
        parser.error("--claim-prefetch must be non-negative")
    if args.server_shards <= 0:
        parser.error("--server-shards must be positive")
    if args.protocol_worker_connections <= 0:
        parser.error("--protocol-worker-connections must be positive")
    if args.protocol_lanes <= 0:
        parser.error("--protocol-lanes must be positive")
    if args.protocol_create_inflight_batches <= 0:
        parser.error("--protocol-create-inflight-batches must be positive")
    if args.latency_sample_rate < 0:
        parser.error("--latency-sample-rate must be non-negative")
    if args.producer_target_queue_latency_ms <= 0:
        parser.error("--producer-target-queue-latency-ms must be positive")
    if args.producer_min_rate_per_sec <= 0:
        parser.error("--producer-min-rate-per-sec must be positive")
    if args.producer_max_rate_per_sec < 0:
        parser.error("--producer-max-rate-per-sec must be non-negative")
    if args.producer_max_pending_credits < 0:
        parser.error("--producer-max-pending-credits must be non-negative")
    if args.create_batch_size <= 0:
        parser.error("--create-batch-size must be positive")
    if args.complete_async_depth < 0:
        parser.error("--complete-async-depth must be non-negative")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
    if args.result_bytes < 0:
        parser.error("--result-bytes must be non-negative")
    if args.retention_ttl_ms < 0:
        parser.error("--retention-ttl-ms must be non-negative")
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
    if args.claim_block_ms < 0:
        args.claim_block_ms = None
    if args.claim_drain_block_ms < 0:
        args.claim_drain_block_ms = None
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
