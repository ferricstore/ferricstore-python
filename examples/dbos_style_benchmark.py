import argparse
import math
import threading
import time
import uuid
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor

from ferricstore import ClaimedItem, CreateItem, FlowClient


FLOW_TYPE = "dbos_python_sdk_bench"
QUEUE_STATE = "queued"


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


class BenchFlowClient:
    def __init__(self, url: str, transport: str) -> None:
        self.client = FlowClient.from_url(url)
        self.transport = transport
        self.redis_client = getattr(self.client.executor, "client", None)

    def enqueue_many(
        self,
        *,
        run_id: str,
        indices: list[int],
        partitions: int,
        payload: bytes,
    ) -> int:
        if self.transport == "pipeline":
            self._pipeline_create(run_id=run_id, indices=indices, partitions=partitions, payload=payload)
            return len(indices)

        if len(indices) == 1:
            index = indices[0]
            self.client.create(
                f"{run_id}:flow:{index}",
                type=FLOW_TYPE,
                state=QUEUE_STATE,
                partition_key=partition_for(index, partitions, run_id),
                payload=payload,
                return_record=False,
            )
            return 1

        items = [
            CreateItem(
                f"{run_id}:flow:{index}",
                payload,
                partition_key=partition_for(index, partitions, run_id),
            )
            for index in indices
        ]
        self.client.create_many(None, items, type=FLOW_TYPE, state=QUEUE_STATE)
        return len(items)

    def claim_due(
        self,
        *,
        worker: str,
        partition_key: str | None,
        limit: int,
    ):
        return self.client.claim_due(
            FLOW_TYPE,
            state=QUEUE_STATE,
            worker=worker,
            partition_key=partition_key,
            limit=limit,
        )

    def complete_claimed(
        self,
        jobs,
        *,
        partition_key: str | None,
        use_many: bool,
    ) -> None:
        if self.transport == "pipeline":
            self._pipeline_complete(jobs)
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
            self.client.complete_many(partition_key, items, result=b"ok")
            return

        for job in jobs:
            self.client.complete(
                job.id,
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=job.partition_key,
                result=b"ok",
                return_record=False,
            )

    def do_work(self, command: str, run_id: str, jobs) -> None:
        if command != "incr":
            return
        for _job in jobs:
            self.client.executor.execute_command("INCR", f"{run_id}:counter")

    def _pipeline_create(
        self,
        *,
        run_id: str,
        indices: list[int],
        partitions: int,
        payload: bytes,
    ) -> None:
        if self.redis_client is None:
            raise RuntimeError("pipeline transport requires RedisAdapter")
        pipe = self.redis_client.pipeline(transaction=False)
        now_ms = int(time.time() * 1000)
        for index in indices:
            pipe.execute_command(
                "FLOW.CREATE",
                f"{run_id}:flow:{index}",
                "TYPE",
                FLOW_TYPE,
                "STATE",
                QUEUE_STATE,
                "NOW",
                now_ms,
                "PARTITION",
                partition_for(index, partitions, run_id),
                "RUN_AT",
                now_ms,
                "PAYLOAD",
                payload,
            )
        pipe.execute()

    def _pipeline_complete(self, jobs) -> None:
        if self.redis_client is None:
            raise RuntimeError("pipeline transport requires RedisAdapter")
        pipe = self.redis_client.pipeline(transaction=False)
        now_ms = int(time.time() * 1000)
        for job in jobs:
            pipe.execute_command(
                "FLOW.COMPLETE",
                job.id,
                job.lease_token,
                "FENCING",
                job.fencing_token,
                "NOW",
                now_ms,
                "PARTITION",
                job.partition_key,
                "RESULT",
                b"ok",
            )
        pipe.execute()


def create_flows(
    *,
    url: str,
    run_id: str,
    indices: list[int],
    partitions: int,
    create_batch_size: int,
    payload: bytes,
    transport: str,
) -> int:
    flow = BenchFlowClient(url, transport)
    created = 0
    for batch in chunks(indices, max(create_batch_size, 1)):
        created += flow.enqueue_many(
            run_id=run_id,
            indices=batch,
            partitions=partitions,
            payload=payload,
        )
    return created


def run_claim_worker(
    *,
    url: str,
    run_id: str,
    worker_index: int,
    worker_count: int,
    partitions: int,
    claim_any: bool,
    claim_batch_size: int,
    complete_batch: bool,
    transport: str,
    work_command: str,
    total_flows: int,
    completed: list[int],
    completed_lock: threading.Lock,
) -> int:
    flow = BenchFlowClient(url, transport)
    worker = f"{run_id}:worker:{worker_index}"
    local_completed = 0
    claim_round = 0
    idle_sleep_s = 0.001

    while True:
        with completed_lock:
            if completed[0] >= total_flows:
                return local_completed

        partition_key = None
        if not claim_any:
            partition_index = partition_index_for_claim(
                worker_index,
                worker_count,
                partitions,
                claim_round,
            )
            claim_round += 1
            partition_key = partition_for(partition_index, partitions, run_id)

        jobs = flow.claim_due(
            worker=worker,
            partition_key=partition_key,
            limit=claim_batch_size,
        )
        if not jobs:
            time.sleep(idle_sleep_s)
            continue

        flow.do_work(work_command, run_id, jobs)
        flow.complete_claimed(
            jobs,
            partition_key=None if claim_any else partition_key,
            use_many=complete_batch,
        )

        local_completed += len(jobs)
        with completed_lock:
            completed[0] += len(jobs)


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
    indices = list(range(args.flows))
    payload = payload_bytes(args.payload_bytes)
    create_started = time.perf_counter()

    create_ranges = [indices[offset:: args.producers] for offset in range(args.producers)]
    with ThreadPoolExecutor(max_workers=args.producers) as executor:
        created = sum(
            executor.map(
                lambda batch: create_flows(
                    url=args.url,
                    run_id=run_id,
                    indices=batch,
                    partitions=args.partitions,
                    create_batch_size=args.create_batch_size,
                    payload=payload,
                    transport=args.transport,
                ),
                create_ranges,
            )
        )

    create_seconds = time.perf_counter() - create_started
    process_started = time.perf_counter()
    completed = [0]
    completed_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        processed = sum(
            executor.map(
                lambda worker_index: run_claim_worker(
                    url=args.url,
                    run_id=run_id,
                    worker_index=worker_index,
                    worker_count=args.workers,
                    partitions=args.partitions,
                    claim_any=args.claim_any,
                    claim_batch_size=args.claim_batch_size,
                    complete_batch=args.complete_batch,
                    transport=args.transport,
                    work_command=args.work_command,
                    total_flows=args.flows,
                    completed=completed,
                    completed_lock=completed_lock,
                ),
                range(args.workers),
            )
        )

    process_seconds = time.perf_counter() - process_started
    total_seconds = create_seconds + process_seconds
    return {
        "mode": "queued",
        "flows": args.flows,
        "created": created,
        "completed": processed,
        "workers": args.workers,
        "producers": args.producers,
        "partitions": args.partitions,
        "claim_any": args.claim_any,
        "claim_batch_size": args.claim_batch_size,
        "create_batch_size": args.create_batch_size,
        "complete_batch": args.complete_batch,
        "transport": args.transport,
        "payload_bytes": args.payload_bytes,
        "work_command": args.work_command,
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

    parser.add_argument("--flows", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--claim-batch-size", type=int, default=100)
    parser.add_argument("--create-batch-size", type=int, default=100)
    parser.add_argument("--transport", choices=("many", "pipeline"), default="pipeline")
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--work-command", choices=("none", "incr"), default="none")
    parser.add_argument("--claim-any", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--complete-batch", action=argparse.BooleanOptionalAction, default=True)

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
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
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
