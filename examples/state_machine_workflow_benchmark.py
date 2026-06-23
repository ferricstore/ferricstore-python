import argparse
import queue
import threading
import time
import uuid
import zlib
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from ferricstore.client import FlowClient
from ferricstore.types import CreateItem
from ferricstore.workflow import Workflow, complete, state, transition

AUTO_PARTITION_PREFIX = "__flow_auto__:"
AUTO_PARTITION_BUCKETS = 256
SERVER_SLOT_COUNT = 1024
DEFAULT_STATE = "queued"


def chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def payload_bytes(size: int) -> bytes:
    if size <= 0:
        return b""
    return b"x" * size


def auto_partition_index_for_flow_id(id: str) -> int:
    return zlib.crc32(id.encode()) % AUTO_PARTITION_BUCKETS


def auto_partition_key_for_index(index: int) -> str:
    return f"{AUTO_PARTITION_PREFIX}{index % AUTO_PARTITION_BUCKETS}"


def auto_partition_index_from_key(partition_key: str) -> int | None:
    if not partition_key.startswith(AUTO_PARTITION_PREFIX):
        return None
    try:
        return int(partition_key[len(AUTO_PARTITION_PREFIX) :]) % AUTO_PARTITION_BUCKETS
    except ValueError:
        return None


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


def explicit_partition_for(index: int, partitions: int, run_id: str) -> str:
    return f"{run_id}:partition:{index % max(partitions, 1)}"


def flow_id(run_id: str, index: int) -> str:
    return f"{run_id}:flow:{index}"


def workflow_states(steps: int) -> list[str]:
    if steps <= 1:
        return [DEFAULT_STATE]
    return [DEFAULT_STATE] + [f"step_{idx}" for idx in range(1, steps)]


def build_workflow_class(
    flow_type: str,
    states: list[str],
    *,
    claim_payload: bool,
    claim_record: bool,
    return_record: bool,
    result_payload: bytes | None,
) -> type[Workflow]:
    attrs: dict[str, Any] = {
        "type": flow_type,
        "initial_state": states[0],
    }

    for idx, state_name in enumerate(states):
        next_state = states[idx + 1] if idx + 1 < len(states) else None

        def make_handler(source_state: str, target_state: str | None):
            @state(
                source_state,
                claim_payload=claim_payload,
                claim_record=claim_record,
                return_record=return_record,
            )
            def handle(self, job):
                if target_state is None:
                    return complete(result=result_payload)
                return transition(target_state)

            return handle

        attrs[f"handle_{idx}_{state_name}"] = make_handler(state_name, next_state)

    return type("BenchmarkWorkflow", (Workflow,), attrs)


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


def worker_partition_keys(
    *,
    partition_mode: str,
    worker_index: int,
    workers: int,
    partitions: int,
    run_id: str,
) -> list[str | None]:
    if partition_mode == "auto":
        return [
            auto_partition_key_for_index(idx)
            for idx in range(AUTO_PARTITION_BUCKETS)
            if idx % workers == worker_index
        ]

    return [
        explicit_partition_for(idx, partitions, run_id)
        for idx in range(partitions)
        if idx % workers == worker_index
    ] or [None]


def notify_ready_counts(
    wake_coordinator: object | None,
    *,
    flow_type: str,
    state_name: str,
    partition_counts: dict[str, int],
    server_shards: int,
) -> None:
    return


def created_partition_counts(
    *,
    run_id: str,
    indices: list[int],
    partitions: int,
    partition_mode: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for index in indices:
        if partition_mode == "auto":
            partition_index = auto_partition_index_for_flow_id(flow_id(run_id, index))
            partition_key = auto_partition_key_for_index(partition_index)
        else:
            partition_key = explicit_partition_for(index, partitions, run_id)
        counts[partition_key] = counts.get(partition_key, 0) + 1
    return counts


def claimed_partition_counts(jobs) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job in jobs:
        if not job.partition_key:
            continue
        counts[job.partition_key] = counts.get(job.partition_key, 0) + 1
    return counts


def create_workflows(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    indices: list[int],
    partitions: int,
    partition_mode: str,
    payload: bytes,
    create_batch_size: int,
    create_mode: str,
    independent_many: bool,
    wake_coordinator: object | None,
    server_shards: int,
) -> dict[str, int]:
    client = FlowClient.from_url(url)
    created = 0

    if partition_mode == "auto" and create_mode == "many":
        auto_buffers: dict[int, list[int]] = {}

        def flush_auto_bucket(partition_index: int) -> None:
            nonlocal created
            batch = auto_buffers.get(partition_index)
            if not batch:
                return

            items = [
                CreateItem(
                    flow_id(run_id, index),
                    payload,
                )
                for index in batch
            ]
            client.enqueue_many(
                items,
                type=flow_type,
                state=DEFAULT_STATE,
                independent=independent_many,
            )
            created += len(items)
            notify_ready_counts(
                wake_coordinator,
                flow_type=flow_type,
                state_name=DEFAULT_STATE,
                partition_counts=created_partition_counts(
                    run_id=run_id,
                    indices=batch,
                    partitions=partitions,
                    partition_mode=partition_mode,
                ),
                server_shards=server_shards,
            )
            auto_buffers[partition_index] = []

        for index in indices:
            partition_index = auto_partition_index_for_flow_id(flow_id(run_id, index))
            bucket = auto_buffers.setdefault(partition_index, [])
            bucket.append(index)
            if len(bucket) >= max(create_batch_size, 1):
                flush_auto_bucket(partition_index)

        for partition_index in list(auto_buffers):
            flush_auto_bucket(partition_index)

        return {"created": created}

    if create_mode == "single":
        for index in indices:
            client.enqueue(
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
            notify_ready_counts(
                wake_coordinator,
                flow_type=flow_type,
                state_name=DEFAULT_STATE,
                partition_counts=created_partition_counts(
                    run_id=run_id,
                    indices=[index],
                    partitions=partitions,
                    partition_mode=partition_mode,
                ),
                server_shards=server_shards,
            )
        return {"created": created}

    for batch in chunks(indices, max(create_batch_size, 1)):
        items = [
            CreateItem(
                flow_id(run_id, index),
                payload,
                partition_key_for_create(
                    partition_mode=partition_mode,
                    index=index,
                    partitions=partitions,
                    run_id=run_id,
                ),
            )
            for index in batch
        ]
        client.enqueue_many(
            items,
            type=flow_type,
            state=DEFAULT_STATE,
            independent=independent_many,
        )
        created += len(items)
        notify_ready_counts(
            wake_coordinator,
            flow_type=flow_type,
            state_name=DEFAULT_STATE,
            partition_counts=created_partition_counts(
                run_id=run_id,
                indices=batch,
                partitions=partitions,
                partition_mode=partition_mode,
            ),
            server_shards=server_shards,
        )

    return {"created": created}


def run_workflow_worker(
    *,
    url: str,
    run_id: str,
    flow_type: str,
    worker_index: int,
    workers: int,
    partitions: int,
    partition_mode: str,
    states: list[str],
    flows: int,
    claim_batch_size: int,
    claim_partition_batch_size: int,
    idle_sleep_ms: float,
    max_idle_sleep_ms: float,
    claim_payload: bool,
    claim_record: bool,
    return_record: bool,
    result_payload: bytes | None,
    apply_async_depth: int,
    completed: list[int],
    completed_lock: threading.Lock,
    producers_done: threading.Event,
    wake_coordinator: object | None,
    wake_coalesce_ms: float,
    server_shards: int,
) -> dict[str, int]:
    client = FlowClient.from_url(url)
    workflow_cls = build_workflow_class(
        flow_type,
        states,
        claim_payload=claim_payload,
        claim_record=claim_record,
        return_record=return_record,
        result_payload=result_payload,
    )
    workflow = workflow_cls(client)
    partitions_to_scan = worker_partition_keys(
        partition_mode=partition_mode,
        worker_index=worker_index,
        workers=workers,
        partitions=partitions,
        run_id=run_id,
    )

    claimed_actions = 0
    completed_workflows = 0
    claim_calls = 0
    empty_claims = 0
    max_claim_batch = 0
    state_cursor = 0
    partition_cursor = 0
    claim_partition_batch_size = max(claim_partition_batch_size, 1)
    idle_sleep_s = max(idle_sleep_ms, 0.0) / 1000.0
    max_idle_sleep_s = max(max_idle_sleep_ms, idle_sleep_ms, 0.0) / 1000.0
    current_sleep = idle_sleep_s
    final_state = states[-1]
    next_state_by_state = {
        state_name: states[idx + 1] for idx, state_name in enumerate(states[:-1])
    }
    allowed_partition_keys = [key for key in partitions_to_scan if key is not None]
    wake_idle_rounds = 0
    wake_coalesce_s = max(wake_coalesce_ms, 0.0) / 1000.0
    apply_executor = (
        ThreadPoolExecutor(max_workers=apply_async_depth) if apply_async_depth > 0 else None
    )
    pending_applies = []

    def record_completed(count: int) -> None:
        nonlocal completed_workflows
        completed_workflows += count
        with completed_lock:
            completed[0] += count

    def drain_pending_applies(*, block: bool = False, limit: int | None = None) -> None:
        drained = 0
        while pending_applies and (limit is None or drained < limit):
            ready_index = None
            for idx, future in enumerate(pending_applies):
                if block or future.done():
                    ready_index = idx
                    break
            if ready_index is None:
                return
            future = pending_applies.pop(ready_index)
            record_completed(future.result())
            drained += 1

    try:
        while True:
            drain_pending_applies(block=False)
            with completed_lock:
                if completed[0] >= flows:
                    break

            state_name = states[state_cursor]
            claim_limit = claim_batch_size
            if wake_coordinator is not None and allowed_partition_keys:
                try:
                    selected_partitions, partition_credit = wake_coordinator.next_ready(
                        type=flow_type,
                        state=state_name,
                        states=None,
                        priority=0,
                        partition_keys=allowed_partition_keys,
                        timeout_s=idle_sleep_s,
                        max_partitions=claim_partition_batch_size,
                        max_credit=claim_batch_size,
                    )
                    wake_idle_rounds = 0
                except queue.Empty:
                    wake_idle_rounds += 1
                    if not producers_done.is_set() or wake_idle_rounds < 3:
                        continue
                    selected_partitions = []
                    partition_credit = claim_batch_size

                if selected_partitions:
                    if (
                        wake_coalesce_s > 0
                        and partition_credit < claim_batch_size
                        and not producers_done.is_set()
                    ):
                        time.sleep(min(wake_coalesce_s, 0.002))
                        remaining_partitions = max(
                            claim_partition_batch_size - len(selected_partitions),
                            0,
                        )
                        remaining_credit = max(claim_batch_size - partition_credit, 0)
                        if remaining_partitions > 0 and remaining_credit > 0:
                            try:
                                extra_partitions, extra_credit = wake_coordinator.next_ready(
                                    type=flow_type,
                                    state=state_name,
                                    states=None,
                                    priority=0,
                                    partition_keys=allowed_partition_keys,
                                    timeout_s=0,
                                    max_partitions=remaining_partitions,
                                    max_credit=remaining_credit,
                                )
                            except queue.Empty:
                                extra_partitions = []
                                extra_credit = 0
                            selected_partitions.extend(extra_partitions)
                            partition_credit += extra_credit
                    partition_key = (
                        selected_partitions[0] if len(selected_partitions) == 1 else None
                    )
                    partition_keys = selected_partitions if len(selected_partitions) > 1 else None
                    claim_limit = min(claim_batch_size, max(partition_credit, 1))
                elif claim_partition_batch_size == 1:
                    partition_key = partitions_to_scan[partition_cursor]
                    partition_cursor = (partition_cursor + 1) % len(partitions_to_scan)
                    partition_keys = None
                else:
                    selected_partitions = []
                    for _ in range(min(claim_partition_batch_size, len(partitions_to_scan))):
                        selected_partitions.append(partitions_to_scan[partition_cursor])
                        partition_cursor = (partition_cursor + 1) % len(partitions_to_scan)
                    partition_key = (
                        selected_partitions[0] if len(selected_partitions) == 1 else None
                    )
                    partition_keys = selected_partitions if len(selected_partitions) > 1 else None
            elif claim_partition_batch_size == 1:
                partition_key = partitions_to_scan[partition_cursor]
                partition_cursor = (partition_cursor + 1) % len(partitions_to_scan)
                partition_keys = None
            else:
                selected_partitions = []
                for _ in range(min(claim_partition_batch_size, len(partitions_to_scan))):
                    selected_partitions.append(partitions_to_scan[partition_cursor])
                    partition_cursor = (partition_cursor + 1) % len(partitions_to_scan)
                partition_key = selected_partitions[0] if len(selected_partitions) == 1 else None
                partition_keys = selected_partitions if len(selected_partitions) > 1 else None

            state_cursor = (state_cursor + 1) % len(states)

            jobs = workflow.claim_due(
                state_name,
                worker=f"{run_id}:worker:{worker_index}",
                partition_key=partition_key,
                partition_keys=partition_keys,
                limit=claim_limit,
                priority=0,
                reclaim_expired=False,
            )
            claim_calls += 1

            if not jobs:
                empty_claims += 1
                if producers_done.is_set():
                    with completed_lock:
                        done = completed[0] >= flows
                    if done:
                        break
                if current_sleep > 0:
                    time.sleep(current_sleep)
                    current_sleep = min(max_idle_sleep_s, max(current_sleep * 2, idle_sleep_s))
                continue

            batch_size = len(jobs)
            current_sleep = idle_sleep_s
            claimed_actions += batch_size
            max_claim_batch = max(max_claim_batch, batch_size)

            if state_name == final_state and apply_executor is not None and not return_record:
                pending_applies.append(
                    apply_executor.submit(workflow.handle_claimed_batch_count, state_name, jobs)
                )
                while len(pending_applies) >= apply_async_depth:
                    drain_pending_applies(block=True, limit=1)
                continue

            if return_record:
                results = workflow.handle_claimed_batch(state_name, jobs)
                applied_count = len(results)
            else:
                applied_count = workflow.handle_claimed_batch_count(state_name, jobs)

            if state_name == final_state:
                record_completed(applied_count)
            else:
                next_state = next_state_by_state.get(state_name)
                if next_state is not None:
                    notify_ready_counts(
                        wake_coordinator,
                        flow_type=flow_type,
                        state_name=next_state,
                        partition_counts=claimed_partition_counts(jobs),
                        server_shards=server_shards,
                    )
    finally:
        drain_pending_applies(block=True)
        if apply_executor is not None:
            apply_executor.shutdown(wait=True)

    return {
        "claimed_actions": claimed_actions,
        "completed": completed_workflows,
        "claim_calls": claim_calls,
        "empty_claims": empty_claims,
        "max_claim_batch": max_claim_batch,
    }


def run_state_machine_throughput(args: argparse.Namespace) -> dict[str, float | int | str | bool]:
    run_id = f"py-sm-bench-{uuid.uuid4().hex}"
    flow_type = f"state_machine_bench:{run_id}"
    states = workflow_states(args.steps)
    indices = list(range(args.flows))
    payload = payload_bytes(args.payload_bytes)
    result_payload = payload_bytes(args.result_bytes) if args.result_bytes > 0 else None
    completed = [0]
    completed_lock = threading.Lock()
    producers_done = threading.Event()
    worker_mode = "blocking"
    wake_coordinator = None

    if args.partition_mode == "auto" and args.create_mode == "many":
        create_ranges = [[] for _ in range(args.producers)]
        for index in indices:
            owner = auto_partition_index_for_flow_id(flow_id(run_id, index)) % args.producers
            create_ranges[owner].append(index)
    else:
        create_ranges = [indices[offset :: args.producers] for offset in range(args.producers)]

    started = time.perf_counter()
    create_started = started
    create_finished = started
    process_started = started
    process_finished = started
    create_results = []
    worker_results = []

    def submit_create_jobs(executor: ThreadPoolExecutor):
        return [
            executor.submit(
                create_workflows,
                url=args.url,
                run_id=run_id,
                flow_type=flow_type,
                indices=batch,
                partitions=args.partitions,
                partition_mode=args.partition_mode,
                payload=payload,
                create_batch_size=args.create_batch_size,
                create_mode=args.create_mode,
                independent_many=args.independent_many,
                wake_coordinator=wake_coordinator,
                server_shards=args.server_shards,
            )
            for batch in create_ranges
        ]

    def submit_worker_jobs(executor: ThreadPoolExecutor):
        return [
            executor.submit(
                run_workflow_worker,
                url=args.url,
                run_id=run_id,
                flow_type=flow_type,
                worker_index=worker_index,
                workers=args.workers,
                partitions=AUTO_PARTITION_BUCKETS
                if args.partition_mode == "auto"
                else args.partitions,
                partition_mode=args.partition_mode,
                states=states,
                flows=args.flows,
                claim_batch_size=args.claim_batch_size,
                claim_partition_batch_size=args.claim_partition_batch_size,
                idle_sleep_ms=args.idle_sleep_ms,
                max_idle_sleep_ms=args.max_idle_sleep_ms,
                claim_payload=args.claim_payload,
                claim_record=args.claim_record,
                return_record=args.return_record,
                result_payload=result_payload,
                apply_async_depth=args.apply_async_depth,
                completed=completed,
                completed_lock=completed_lock,
                producers_done=producers_done,
                wake_coordinator=wake_coordinator,
                wake_coalesce_ms=args.wake_coalesce_ms,
                server_shards=args.server_shards,
            )
            for worker_index in range(args.workers)
        ]

    if args.shape == "preloaded":
        create_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.producers) as executor:
            for future in as_completed(submit_create_jobs(executor)):
                create_results.append(future.result())
        create_finished = time.perf_counter()
        producers_done.set()

        process_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for future in as_completed(submit_worker_jobs(executor)):
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

    created = sum(result["created"] for result in create_results)
    completed_count = completed[0]
    claimed_actions = sum(result["claimed_actions"] for result in worker_results)
    claim_calls = sum(result["claim_calls"] for result in worker_results)
    empty_claims = sum(result["empty_claims"] for result in worker_results)
    max_claim_batch = max((result["max_claim_batch"] for result in worker_results), default=0)
    create_seconds = create_finished - create_started
    process_seconds = process_finished - process_started
    total_seconds = process_finished - started

    return {
        "mode": "state-machine-workflow",
        "shape": args.shape,
        "flow_type": flow_type,
        "flows": args.flows,
        "steps": args.steps,
        "created": created,
        "completed": completed_count,
        "claimed_actions": claimed_actions,
        "expected_actions": completed_count * args.steps,
        "workers": args.workers,
        "producers": args.producers,
        "partitions": args.partitions,
        "partition_mode": args.partition_mode,
        "create_mode": args.create_mode,
        "create_batch_size": args.create_batch_size,
        "claim_batch_size": args.claim_batch_size,
        "claim_partition_batch_size": args.claim_partition_batch_size,
        "claim_payload": args.claim_payload,
        "claim_record": args.claim_record,
        "return_record": args.return_record,
        "apply_async_depth": args.apply_async_depth,
        "worker_mode": worker_mode,
        "wake_coalesce_ms": args.wake_coalesce_ms,
        "wake_notifications": wake_coordinator.notifications if wake_coordinator is not None else 0,
        "wake_credits": wake_coordinator.notified_jobs if wake_coordinator is not None else 0,
        "independent_many": args.independent_many,
        "payload_bytes": args.payload_bytes,
        "result_bytes": args.result_bytes,
        "claim_calls": claim_calls,
        "empty_claims": empty_claims,
        "avg_claim_batch": claimed_actions / claim_calls if claim_calls > 0 else 0.0,
        "max_claim_batch": max_claim_batch,
        "create_seconds": create_seconds,
        "process_seconds": process_seconds,
        "total_seconds": total_seconds,
        "create_flows_per_sec": created / create_seconds if create_seconds > 0 else 0.0,
        "workflow_completions_per_sec": completed_count / process_seconds
        if process_seconds > 0
        else 0.0,
        "state_actions_per_sec": claimed_actions / process_seconds if process_seconds > 0 else 0.0,
        "end_to_end_workflows_per_sec": completed_count / total_seconds
        if total_seconds > 0
        else 0.0,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FerricFlow state-machine Workflow API benchmark")
    parser.add_argument("--url", default="ferric://127.0.0.1:6388")
    parser.add_argument("--shape", choices=("live", "preloaded"), default="live")
    parser.add_argument("--flows", type=int, default=10_000)
    parser.add_argument("--steps", type=int, default=1)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--producers", type=int, default=32)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--partition-mode", choices=("explicit", "auto"), default="auto")
    parser.add_argument("--create-mode", choices=("single", "many"), default="many")
    parser.add_argument("--create-batch-size", type=int, default=5000)
    parser.add_argument("--claim-batch-size", type=int, default=1000)
    parser.add_argument("--claim-partition-batch-size", type=int, default=1)
    parser.add_argument("--worker-mode", choices=("blocking", "polling"), default="blocking")
    parser.add_argument("--wake-coalesce-ms", type=float, default=2.0)
    parser.add_argument("--apply-async-depth", type=int, default=4)
    parser.add_argument("--server-shards", type=int, default=16)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--result-bytes", type=int, default=0)
    parser.add_argument("--idle-sleep-ms", type=float, default=1.0)
    parser.add_argument("--max-idle-sleep-ms", type=float, default=10.0)
    parser.add_argument("--claim-payload", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--claim-record", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--return-record", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--independent-many", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    if args.flows <= 0:
        raise ValueError("--flows must be positive")
    if args.steps <= 0:
        raise ValueError("--steps must be positive")
    if args.workers <= 0:
        raise ValueError("--workers must be positive")
    if args.producers <= 0:
        raise ValueError("--producers must be positive")
    if args.claim_batch_size <= 0:
        raise ValueError("--claim-batch-size must be positive")
    if args.claim_partition_batch_size <= 0:
        raise ValueError("--claim-partition-batch-size must be positive")
    if args.create_batch_size <= 0:
        raise ValueError("--create-batch-size must be positive")
    if args.wake_coalesce_ms < 0:
        raise ValueError("--wake-coalesce-ms must be non-negative")
    if args.server_shards <= 0:
        raise ValueError("--server-shards must be positive")
    if args.apply_async_depth < 0:
        raise ValueError("--apply-async-depth must be non-negative")
    return args


if __name__ == "__main__":
    print(run_state_machine_throughput(parse_args()))
