from __future__ import annotations

import argparse
import json
import math
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any, Callable

from ferricstore.async_worker import (
    _auto_partition_index_for_id,
    _auto_partition_key,
    _auto_partition_server_shard,
)
from ferricstore.client import (
    FlowClient,
    _append,
    _append_encoded,
    _auto_partition_key_for_id,
)
from ferricstore.types import ClaimedFlow

DEFAULT_URL = "ferric://127.0.0.1:6388"
DEFAULT_TYPE_PREFIX = "restate_latency"

RESTATE_HIGH_LOAD_TARGETS = {
    1: {"rps": 23_131.0, "p50_ms": 16.0, "p99_ms": 40.0},
    3: {"rps": 16_844.0, "p50_ms": 58.0, "p90_ms": 76.0, "p99_ms": 98.0},
    9: {"rps": 8_571.0, "p50_ms": 116.0, "p99_ms": 163.0},
}

RESTATE_LOW_LOAD_TARGETS = {
    3: {"rps": 549.0, "p50_ms": 15.0, "p99_ms": 69.0},
    9: {"rps": 303.0, "p50_ms": 31.0, "p99_ms": 93.0},
}
DEFAULT_BATCH_SIZE = 500
DEFAULT_INFLIGHT_BATCHES = 1

# Named latency benchmark shapes. These preserve the public-SDK protocol
# arguments we use for Restate-style latency comparisons. They are benchmark
# settings, not SDK defaults.
RESTATE_LATENCY_DEFAULT_ARGS = {
    "workflows": 10_000,
    "warmup_workflows": 0,
    "steps": 3,
    "execution_mode": "wave",
    "profile": "default",
    "concurrency": 128,
    "target_rps": 0.0,
    "target_catch_up": False,
    "partitions": 16,
    "partition_mode": "auto",
    "shard_local_submit_concurrency": 0,
    "protocol_connections": 1,
    "protocol_lanes": 64,
    "payload_bytes": 0,
    "result_bytes": 0,
    "lease_ms": 30_000,
    "timeout": 10.0,
    "startup_settle_seconds": 0.0,
    "readiness_probes": 0,
    "verify_sample": 0,
    "slow_wave_count": 0,
    "slow_wave_min_ms": 0.0,
    "trace_sample_count": 0,
    "trace_min_ms": 0.0,
    "type_prefix": DEFAULT_TYPE_PREFIX,
    "stop_on_error": True,
}

RESTATE_HIGH_LOAD_PROFILE = {
    1: {
        "batch_size": 250,
        "inflight_batches": 4,
        "shard_local_submit_concurrency": 8,
    },
    3: {"batch_size": 500, "inflight_batches": 4},
    9: {"batch_size": 500, "inflight_batches": 1},
}


def latency_default(key: str):
    return RESTATE_LATENCY_DEFAULT_ARGS[key]


@dataclass(frozen=True)
class WorkflowSpec:
    run_id: str
    flow_type: str
    index: int
    steps: int
    partition_key: str | None
    worker: str


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def target_latency_pass(
    target: dict[str, float] | None, key: str, observed_ms: float
) -> bool | None:
    if not target or key not in target:
        return None
    return observed_ms < target[key]


def target_all_latency_passes(target: dict[str, float] | None, observed: dict[str, float]) -> bool:
    if not target:
        return False

    latency_keys = ("p50_ms", "p90_ms", "p99_ms")
    required = [key for key in latency_keys if key in target]
    return bool(required) and all(observed[key] < target[key] for key in required)


def next_no_catch_up_due_s(previous_due_s: float, interval_s: float, now_s: float) -> float:
    return max(previous_due_s + interval_s, now_s)


def state_name(step: int) -> str:
    return f"step_{step}"


def flow_id_for(spec: WorkflowSpec) -> str:
    return f"{spec.run_id}:flow:{spec.index}"


def effective_partition_key(spec: WorkflowSpec) -> str:
    return spec.partition_key or _auto_partition_key_for_id(flow_id_for(spec))


def predicted_lease_token(worker: str, now_ms: int, fencing_token: int) -> bytes:
    return f"{worker}:{now_ms}:{fencing_token}".encode()


def partition_key_for(index: int, partitions: int, run_id: str, mode: str) -> str | None:
    if mode == "auto":
        return None
    return f"{run_id}:partition:{index % max(partitions, 1)}"


def workflow_spec(
    *,
    run_id: str,
    flow_type: str,
    index: int,
    steps: int,
    partitions: int,
    partition_mode: str,
    worker_count: int,
) -> WorkflowSpec:
    return WorkflowSpec(
        run_id=run_id,
        flow_type=flow_type,
        index=index,
        steps=steps,
        partition_key=partition_key_for(index, partitions, run_id, partition_mode),
        worker=f"{run_id}:worker:{index % max(worker_count, 1)}",
    )


def run_direct_workflow(
    client: FlowClient,
    spec: WorkflowSpec,
    *,
    payload: bytes | None,
    result: bytes | None,
    lease_ms: int,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> int:
    if spec.steps <= 0:
        raise ValueError("steps must be positive")

    flow_id = flow_id_for(spec)
    started_ns = clock_ns()
    record = client.start_and_claim(
        flow_id,
        type=spec.flow_type,
        initial_state=state_name(1),
        worker=spec.worker,
        partition_key=spec.partition_key,
        lease_ms=lease_ms,
        payload=payload,
    )

    for step in range(1, spec.steps):
        record = client.step_continue(
            flow_id,
            lease_token=record.lease_token,
            from_state=state_name(step),
            to_state=state_name(step + 1),
            fencing_token=record.fencing_token,
            lease_ms=lease_ms,
            partition_key=spec.partition_key,
            worker=spec.worker,
        )

    client.complete(
        flow_id,
        lease_token=record.lease_token,
        fencing_token=record.fencing_token,
        partition_key=spec.partition_key,
        result=result,
        return_record=False,
    )
    return clock_ns() - started_ns


def response_job(value: Any) -> ClaimedFlow:
    if isinstance(value, ClaimedFlow):
        return value
    if isinstance(value, (tuple, list)) and len(value) >= 4:
        return ClaimedFlow(
            str(value[0].decode() if isinstance(value[0], bytes) else value[0]),
            bytes(value[2]),
            int(value[3]),
            partition_key=None
            if value[1] in (None, b"", "")
            else str(value[1].decode() if isinstance(value[1], bytes) else value[1]),
        )
    if isinstance(value, dict):
        raw = {key.decode() if isinstance(key, bytes) else key: item for key, item in value.items()}
        return ClaimedFlow(
            str(raw["id"].decode() if isinstance(raw["id"], bytes) else raw["id"]),
            bytes(raw["lease_token"]),
            int(raw["fencing_token"]),
            partition_key=None
            if raw.get("partition_key") in (None, b"", "")
            else str(
                raw["partition_key"].decode()
                if isinstance(raw["partition_key"], bytes)
                else raw["partition_key"]
            ),
            run_state=None
            if raw.get("run_state") in (None, b"", "")
            else str(
                raw["run_state"].decode()
                if isinstance(raw["run_state"], bytes)
                else raw["run_state"]
            ),
        )
    return ClaimedFlow(
        str(value.id),
        bytes(value.lease_token),
        int(value.fencing_token),
        partition_key=getattr(value, "partition_key", None),
        run_state=getattr(value, "run_state", None),
    )


def start_and_claim_command(
    spec: WorkflowSpec,
    *,
    payload: bytes | None,
    lease_ms: int,
    now_ms: int,
) -> tuple[Any, ...]:
    command: list[Any] = [
        "FLOW.START_AND_CLAIM",
        flow_id_for(spec),
        "TYPE",
        spec.flow_type,
        "INITIAL_STATE",
        state_name(1),
        "WORKER",
        spec.worker,
        "LEASE_MS",
        lease_ms,
        "NOW",
        now_ms,
        "PARTITION",
        effective_partition_key(spec),
        "RETURN",
        "JOBS_COMPACT",
    ]
    if payload is not None:
        command.extend(["PAYLOAD", payload])
    return tuple(command)


def step_continue_command(
    job: ClaimedFlow,
    *,
    from_step: int,
    to_step: int,
    lease_ms: int,
    now_ms: int,
    worker: str,
) -> tuple[Any, ...]:
    command: list[Any] = [
        "FLOW.STEP_CONTINUE",
        job.id,
        job.lease_token,
        state_name(from_step),
        state_name(to_step),
        "FENCING",
        job.fencing_token,
        "LEASE_MS",
        lease_ms,
    ]
    if job.partition_key is not None:
        command.extend(["PARTITION", job.partition_key])
    command.extend(["RETURN", "JOBS_COMPACT"])
    command.extend(["NOW", now_ms])
    return tuple(command)


def predicted_step_job_for(spec: WorkflowSpec, *, start_now_ms: int, step: int) -> ClaimedFlow:
    token_now_ms = start_now_ms if step <= 1 else start_now_ms + step - 1
    return ClaimedFlow(
        flow_id_for(spec),
        predicted_lease_token(spec.worker, token_now_ms, step),
        step,
        partition_key=effective_partition_key(spec),
        run_state=state_name(step),
    )


def predicted_terminal_job_for(spec: WorkflowSpec, *, start_now_ms: int, steps: int) -> ClaimedFlow:
    fencing_token = max(steps, 1)
    token_now_ms = start_now_ms if steps <= 1 else start_now_ms + steps - 1
    return ClaimedFlow(
        flow_id_for(spec),
        predicted_lease_token(spec.worker, token_now_ms, fencing_token),
        fencing_token,
        partition_key=effective_partition_key(spec),
        run_state=state_name(steps),
    )


def complete_command(
    job: ClaimedFlow,
    *,
    result: bytes | None,
    now_ms: int,
) -> tuple[Any, ...]:
    command: list[Any] = [
        "FLOW.COMPLETE",
        job.id,
        job.lease_token,
        "FENCING",
        job.fencing_token,
        "NOW",
        now_ms,
    ]
    if job.partition_key is not None:
        command.extend(["PARTITION", job.partition_key])
    if result is not None:
        command.extend(["RESULT", result])
    return tuple(command)


def execute_batch(client: FlowClient, commands: list[tuple[Any, ...]]) -> list[Any]:
    return list(client._execute_command_batch(commands))


def run_wave_batch(
    client: FlowClient,
    specs: list[WorkflowSpec],
    *,
    payload: bytes | None,
    result: bytes | None,
    lease_ms: int,
    chain_submit_mode: str = "sequential",
) -> None:
    if not specs:
        return
    if chain_submit_mode == "predicted-pipeline":
        run_predicted_pipeline_wave_batch(
            client,
            specs,
            payload=payload,
            result=result,
            lease_ms=lease_ms,
        )
        return
    if chain_submit_mode == "run-steps-many":
        run_steps_many_wave_batch(
            client,
            specs,
            payload=payload,
            result=result,
            lease_ms=lease_ms,
        )
        return

    now_ms = int(time.time() * 1000)
    jobs = [
        response_job(value)
        for value in execute_batch(
            client,
            [
                start_and_claim_command(
                    spec,
                    payload=payload,
                    lease_ms=lease_ms,
                    now_ms=now_ms,
                )
                for spec in specs
            ],
        )
    ]

    for step in range(1, specs[0].steps):
        now_ms = int(time.time() * 1000)
        jobs = [
            response_job(value)
            for value in execute_batch(
                client,
                [
                    step_continue_command(
                        job,
                        from_step=step,
                        to_step=step + 1,
                        lease_ms=lease_ms,
                        now_ms=now_ms,
                        worker=specs[index].worker,
                    )
                    for index, job in enumerate(jobs)
                ],
            )
        ]

    client.complete_jobs(jobs, result=result, independent=True, return_ok_on_success=True)


def run_predicted_pipeline_wave_batch(
    client: FlowClient,
    specs: list[WorkflowSpec],
    *,
    payload: bytes | None,
    result: bytes | None,
    lease_ms: int,
) -> None:
    start_now_ms = int(time.time() * 1000)
    commands: list[tuple[Any, ...]] = [
        start_and_claim_command(
            spec,
            payload=payload,
            lease_ms=lease_ms,
            now_ms=start_now_ms,
        )
        for spec in specs
    ]

    steps = specs[0].steps
    for step in range(1, steps):
        step_now_ms = start_now_ms + step
        commands.extend(
            step_continue_command(
                predicted_step_job_for(spec, start_now_ms=start_now_ms, step=step),
                from_step=step,
                to_step=step + 1,
                lease_ms=lease_ms,
                now_ms=step_now_ms,
                worker=spec.worker,
            )
            for spec in specs
        )

    commands.extend(
        complete_command(
            predicted_terminal_job_for(spec, start_now_ms=start_now_ms, steps=steps),
            result=result,
            now_ms=start_now_ms + steps,
        )
        for spec in specs
    )
    execute_batch(client, commands)


def run_steps_many_wave_batch(
    client: FlowClient,
    specs: list[WorkflowSpec],
    *,
    payload: bytes | None,
    result: bytes | None,
    lease_ms: int,
) -> None:
    if not specs:
        return

    groups: dict[tuple[str, str, int], list[WorkflowSpec]] = {}
    for spec in specs:
        if spec.steps <= 0:
            raise ValueError("steps must be positive")
        groups.setdefault((spec.flow_type, spec.worker, spec.steps), []).append(spec)

    now_ms = int(time.time() * 1000)
    for (flow_type, worker, steps), group_specs in groups.items():
        states = [state_name(step) for step in range(1, steps + 1)]
        items: list[dict[str, str]] = []
        for spec in group_specs:
            item = {"id": flow_id_for(spec)}
            if spec.partition_key is not None:
                item["partition_key"] = spec.partition_key
            items.append(item)

        client.run_steps_many(
            items,
            type=flow_type,
            states=states,
            worker=worker,
            lease_ms=lease_ms,
            now_ms=now_ms,
            payload=payload,
            result=result,
        )


def run_steps_many_auto_id_wave_batch(
    client: FlowClient,
    *,
    run_id: str,
    flow_type: str,
    start: int,
    count: int,
    steps: int,
    worker: str,
    payload: bytes | None,
    result: bytes | None,
    lease_ms: int,
) -> None:
    if count <= 0:
        return
    if steps <= 0:
        raise ValueError("steps must be positive")

    client.run_steps_many(
        [f"{run_id}:flow:{start + offset}" for offset in range(count)],
        type=flow_type,
        states=[state_name(step) for step in range(1, steps + 1)],
        worker=worker,
        lease_ms=lease_ms,
        now_ms=int(time.time() * 1000),
        payload=payload,
        result=result,
    )


def run_steps_many_command_args(
    client: FlowClient,
    items: list[str | dict[str, Any]],
    *,
    flow_type: str,
    states: list[str],
    worker: str,
    lease_ms: int,
    now_ms: int,
    payload: bytes | None,
    result: bytes | None,
) -> tuple[Any, ...]:
    args: list[Any] = ["FLOW.RUN_STEPS_MANY", "TYPE", flow_type]
    args.extend(["STATES", states])
    args.extend(["WORKER", worker, "LEASE_MS", lease_ms, "NOW", now_ms])
    _append_encoded(args, "PAYLOAD", client.codec, payload)
    _append_encoded(args, "RESULT", client.codec, result)
    _append(args, "ITEMS", client._run_steps_many_items(items, None))
    return tuple(args)


def run_steps_many_auto_shard_local_wave_batches(
    client: FlowClient,
    *,
    run_id: str,
    flow_type: str,
    start: int,
    count: int,
    steps: int,
    worker: str,
    payload: bytes | None,
    result: bytes | None,
    lease_ms: int,
    server_shards: int,
    executor: ThreadPoolExecutor | None = None,
    trace_recorder: Callable[[dict[str, Any]], None] | None = None,
) -> list[tuple[int, int, int]]:
    if count <= 0:
        return []
    if steps <= 0:
        raise ValueError("steps must be positive")

    shard_count = max(server_shards, 1)
    groups: dict[int, list[tuple[int, dict[str, str]]]] = {}
    for offset in range(count):
        index = start + offset
        flow_id = f"{run_id}:flow:{index}"
        partition_index = _auto_partition_index_for_id(flow_id)
        shard = _auto_partition_server_shard(partition_index, shard_count)
        groups.setdefault(shard, []).append(
            (
                index,
                {
                    "id": flow_id,
                    "partition_key": _auto_partition_key(partition_index),
                },
            )
        )

    states = [state_name(step) for step in range(1, steps + 1)]

    def submit_group(group: list[tuple[int, dict[str, str]]]) -> tuple[int, int, int]:
        started_ns = time.perf_counter_ns()
        items = [item for _index, item in group]
        if trace_recorder is None:
            client.run_steps_many(
                items,
                type=flow_type,
                states=states,
                worker=worker,
                lease_ms=lease_ms,
                now_ms=int(time.time() * 1000),
                payload=payload,
                result=result,
            )
            elapsed_ns = time.perf_counter_ns() - started_ns
        else:
            trace_result = client.executor.execute_command_with_trace(
                *run_steps_many_command_args(
                    client,
                    items,
                    flow_type=flow_type,
                    states=states,
                    worker=worker,
                    lease_ms=lease_ms,
                    now_ms=int(time.time() * 1000),
                    payload=payload,
                    result=result,
                )
            )
            elapsed_ns = time.perf_counter_ns() - started_ns
            trace_recorder(
                {
                    "start": group[0][0],
                    "count": len(group),
                    "latency_ms": elapsed_ns / 1_000_000.0,
                    "trace": trace_result.get("trace", {}),
                }
            )
        return group[0][0], len(group), elapsed_ns

    ordered_groups = [groups[shard] for shard in sorted(groups)]
    if len(ordered_groups) == 1:
        return [submit_group(ordered_groups[0])]

    if executor is not None:
        futures = [executor.submit(submit_group, group) for group in ordered_groups]
        return [future.result() for future in futures]

    with ThreadPoolExecutor(max_workers=min(len(ordered_groups), shard_count)) as local_executor:
        futures = [local_executor.submit(submit_group, group) for group in ordered_groups]
        return [future.result() for future in futures]


def wave_partition_key_for_index(
    *,
    index: int,
    run_id: str,
    batch_size: int,
    partitions: int,
    partition_mode: str,
) -> str | None:
    if partition_mode == "auto":
        return None
    return f"{run_id}:partition:{(index // max(batch_size, 1)) % max(partitions, 1)}"


def run_readiness_probes(
    client: FlowClient,
    args: argparse.Namespace,
    *,
    run_id: str,
    flow_type: str,
) -> int:
    if args.readiness_probes <= 0:
        return 0

    states = [state_name(1)]
    worker = f"{run_id}:readiness-worker"
    now_ms = int(time.time() * 1000)
    for index in range(args.readiness_probes):
        item: dict[str, str] = {"id": f"{run_id}:readiness:{index}"}
        partition_key = wave_partition_key_for_index(
            index=index,
            run_id=run_id,
            batch_size=1,
            partitions=args.partitions,
            partition_mode=args.partition_mode,
        )
        if partition_key is not None:
            item["partition_key"] = partition_key

        client.run_steps_many(
            [item],
            type=f"{flow_type}:readiness",
            states=states,
            worker=worker,
            lease_ms=args.lease_ms,
            now_ms=now_ms + index,
            result=b"ok",
        )

    return args.readiness_probes


def verify_sampled_results(
    client: FlowClient,
    args: argparse.Namespace,
    *,
    run_id: str,
    flow_type: str,
) -> dict[str, Any]:
    requested = max(args.verify_sample, 0)
    if requested == 0 or args.workflows <= 0:
        return {"requested": requested, "checked": 0, "errors": 0}

    sample_count = min(requested, args.workflows)
    if sample_count == 1:
        indexes = [args.workflows - 1]
    else:
        indexes = sorted(
            {
                round((args.workflows - 1) * offset / (sample_count - 1))
                for offset in range(sample_count)
            }
        )

    errors = 0
    for index in indexes:
        flow_id = f"{run_id}:flow:{index}"
        partition_key = wave_partition_key_for_index(
            index=index,
            run_id=run_id,
            batch_size=args.batch_size,
            partitions=args.partitions,
            partition_mode=args.partition_mode,
        )
        record = client.get(flow_id, partition_key=partition_key)
        if record is None:
            errors += 1
            continue
        if record.type != flow_type or record.state != "completed":
            errors += 1
            continue
        if record.version < args.steps + 1:
            errors += 1

    if errors and args.stop_on_error:
        raise RuntimeError(f"verification failed for {errors} of {len(indexes)} sampled workflows")

    return {"requested": requested, "checked": len(indexes), "errors": errors}


def wave_diagnostics_enabled(args: argparse.Namespace) -> bool:
    return int(getattr(args, "slow_wave_count", 0) or 0) > 0


def trace_diagnostics_enabled(args: argparse.Namespace) -> bool:
    return int(getattr(args, "trace_sample_count", 0) or 0) > 0


def init_wave_diagnostics(args: argparse.Namespace) -> None:
    if wave_diagnostics_enabled(args):
        args._slow_waves = []
        args._schedule_lag_ns = []
    if trace_diagnostics_enabled(args):
        args._trace_samples = []
        args._trace_lock = threading.Lock()


def make_trace_recorder(args: argparse.Namespace) -> Callable[[dict[str, Any]], None] | None:
    if not trace_diagnostics_enabled(args):
        return None

    def record(sample: dict[str, Any]) -> None:
        if sample["latency_ms"] < float(getattr(args, "trace_min_ms", 0.0) or 0.0):
            return
        lock = getattr(args, "_trace_lock")
        with lock:
            getattr(args, "_trace_samples").append(sample)

    return record


def record_wave_diagnostic(
    args: argparse.Namespace,
    *,
    start: int,
    count: int,
    elapsed_ns: int,
) -> None:
    slow_waves = getattr(args, "_slow_waves", None)
    if slow_waves is None:
        return

    elapsed_ms = elapsed_ns / 1_000_000.0
    if elapsed_ms < float(getattr(args, "slow_wave_min_ms", 0.0) or 0.0):
        return

    slow_waves.append(
        {
            "start": start,
            "count": count,
            "latency_ms": elapsed_ms,
            "service_latency_ms": elapsed_ms / max(count, 1),
        }
    )


def record_schedule_lag(args: argparse.Namespace, due_s: float) -> None:
    schedule_lag_ns = getattr(args, "_schedule_lag_ns", None)
    if schedule_lag_ns is None:
        return
    schedule_lag_ns.append(max(0, int((time.perf_counter() - due_s) * 1_000_000_000)))


def wave_diagnostics_result(args: argparse.Namespace) -> dict[str, Any]:
    limit = int(getattr(args, "slow_wave_count", 0) or 0)
    slow_waves = sorted(
        getattr(args, "_slow_waves", []),
        key=lambda item: item["latency_ms"],
        reverse=True,
    )[:limit]
    schedule_lag_ms = [value / 1_000_000.0 for value in getattr(args, "_schedule_lag_ns", [])]
    trace_limit = int(getattr(args, "trace_sample_count", 0) or 0)
    trace_samples = sorted(
        getattr(args, "_trace_samples", []),
        key=lambda item: item["latency_ms"],
        reverse=True,
    )[:trace_limit]

    return {
        "slow_wave_count": limit,
        "slow_wave_min_ms": float(getattr(args, "slow_wave_min_ms", 0.0) or 0.0),
        "slow_waves": slow_waves,
        "schedule_lag_p99_ms": percentile(schedule_lag_ms, 99),
        "schedule_lag_max_ms": max(schedule_lag_ms) if schedule_lag_ms else 0.0,
        "trace_sample_count": trace_limit,
        "trace_min_ms": float(getattr(args, "trace_min_ms", 0.0) or 0.0),
        "trace_samples": trace_samples,
    }


def run_wave_benchmark(
    args: argparse.Namespace,
    *,
    run_id: str,
    flow_type: str,
    payload: bytes | None,
    result: bytes | None,
    client: FlowClient,
) -> tuple[int, int, list[int], list[int]]:
    completed = 0
    errors = 0
    latencies_ns: list[int] = []
    service_latencies_ns: list[int] = []
    batch_size = max(args.batch_size, 1)
    inflight_batches = max(args.inflight_batches, 1)
    shard_local_workers = (
        args.shard_local_submit_concurrency
        if args.shard_local_submit_concurrency > 0
        else args.partitions
    )
    shard_local_executor = (
        ThreadPoolExecutor(max_workers=max(shard_local_workers, 1))
        if args.chain_submit_mode == "run-steps-many-shard-local" and args.partition_mode == "auto"
        else None
    )
    init_wave_diagnostics(args)
    trace_recorder = make_trace_recorder(args)

    def run_one_wave(start: int) -> list[tuple[int, int, int]]:
        count = min(batch_size, args.workflows - start)
        started_ns = time.perf_counter_ns()

        if args.chain_submit_mode == "run-steps-many" and args.partition_mode == "auto":
            run_steps_many_auto_id_wave_batch(
                client,
                run_id=run_id,
                flow_type=flow_type,
                start=start,
                count=count,
                steps=args.steps,
                worker=f"{run_id}:worker:0",
                payload=payload,
                result=result,
                lease_ms=args.lease_ms,
            )
            return [(start, count, time.perf_counter_ns() - started_ns)]

        if args.chain_submit_mode == "run-steps-many-shard-local" and args.partition_mode == "auto":
            return run_steps_many_auto_shard_local_wave_batches(
                client,
                run_id=run_id,
                flow_type=flow_type,
                start=start,
                count=count,
                steps=args.steps,
                worker=f"{run_id}:worker:0",
                payload=payload,
                result=result,
                lease_ms=args.lease_ms,
                server_shards=args.partitions,
                executor=shard_local_executor,
                trace_recorder=trace_recorder,
            )

        batch_partition_key = (
            None
            if args.partition_mode == "auto"
            else f"{run_id}:partition:{(start // batch_size) % args.partitions}"
        )
        specs = [
            WorkflowSpec(
                run_id=run_id,
                flow_type=flow_type,
                index=start + offset,
                steps=args.steps,
                partition_key=batch_partition_key,
                worker=f"{run_id}:worker:0",
            )
            for offset in range(count)
        ]
        run_wave_batch(
            client,
            specs,
            payload=payload,
            result=result,
            lease_ms=args.lease_ms,
            chain_submit_mode=args.chain_submit_mode,
        )
        return [(start, count, time.perf_counter_ns() - started_ns)]

    def collect_future(future: Future[list[tuple[int, int, int]]], fallback_count: int) -> None:
        nonlocal completed, errors
        try:
            for start, count, elapsed_ns in future.result():
                latencies_ns.extend([elapsed_ns] * count)
                service_latency_ns = elapsed_ns // max(count, 1)
                service_latencies_ns.extend([service_latency_ns] * count)
                completed += count
                record_wave_diagnostic(args, start=start, count=count, elapsed_ns=elapsed_ns)
        except Exception:
            errors += fallback_count
            if args.stop_on_error:
                raise

    target_rps = float(getattr(args, "target_rps", 0.0) or 0.0)
    if target_rps > 0.0:
        batch_interval_s = batch_size / target_rps
        submitted_starts = 0
        batch_index = 0
        scheduled_base_s = time.perf_counter()
        next_due_s = scheduled_base_s

        with ThreadPoolExecutor(max_workers=inflight_batches) as executor:
            pending: set[Future[list[tuple[int, int, int]]]] = set()

            while submitted_starts < args.workflows:
                due_s = (
                    scheduled_base_s + batch_index * batch_interval_s
                    if args.target_catch_up
                    else next_due_s
                )
                sleep_s = due_s - time.perf_counter()
                if sleep_s > 0:
                    time.sleep(sleep_s)

                while len(pending) >= inflight_batches:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        collect_future(future, batch_size)

                record_schedule_lag(args, due_s)
                pending.add(executor.submit(run_one_wave, submitted_starts))
                submitted_starts += batch_size
                batch_index += 1
                if not args.target_catch_up:
                    next_due_s = next_no_catch_up_due_s(
                        next_due_s,
                        batch_interval_s,
                        time.perf_counter(),
                    )

            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    collect_future(future, batch_size)

        if shard_local_executor is not None:
            shard_local_executor.shutdown(wait=True)
        return completed, errors, latencies_ns, service_latencies_ns

    if inflight_batches == 1:
        for start in range(0, args.workflows, batch_size):
            try:
                for wave_start, count, elapsed_ns in run_one_wave(start):
                    latencies_ns.extend([elapsed_ns] * count)
                    service_latency_ns = elapsed_ns // max(count, 1)
                    service_latencies_ns.extend([service_latency_ns] * count)
                    completed += count
                    record_wave_diagnostic(
                        args,
                        start=wave_start,
                        count=count,
                        elapsed_ns=elapsed_ns,
                    )
            except Exception:
                errors += min(batch_size, args.workflows - start)
                if args.stop_on_error:
                    raise
        if shard_local_executor is not None:
            shard_local_executor.shutdown(wait=True)
        return completed, errors, latencies_ns, service_latencies_ns

    submitted_starts = 0
    with ThreadPoolExecutor(max_workers=inflight_batches) as executor:
        pending: set[Future[list[tuple[int, int, int]]]] = set()
        while submitted_starts < args.workflows and len(pending) < inflight_batches:
            pending.add(executor.submit(run_one_wave, submitted_starts))
            submitted_starts += batch_size

        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                collect_future(future, batch_size)

                while submitted_starts < args.workflows and len(pending) < inflight_batches:
                    pending.add(executor.submit(run_one_wave, submitted_starts))
                    submitted_starts += batch_size

    if shard_local_executor is not None:
        shard_local_executor.shutdown(wait=True)
    return completed, errors, latencies_ns, service_latencies_ns


def run_serial_benchmark(
    args: argparse.Namespace,
    *,
    run_id: str,
    flow_type: str,
    payload: bytes | None,
    result: bytes | None,
    client: FlowClient,
) -> tuple[int, int, list[int], list[int]]:
    completed = 0
    submitted = 0
    errors = 0
    latencies_ns: list[int] = []

    def submit_one(executor: ThreadPoolExecutor, index: int) -> Future[int]:
        spec = workflow_spec(
            run_id=run_id,
            flow_type=flow_type,
            index=index,
            steps=args.steps,
            partitions=args.partitions,
            partition_mode=args.partition_mode,
            worker_count=args.concurrency,
        )
        return executor.submit(
            run_direct_workflow,
            client,
            spec,
            payload=payload,
            result=result,
            lease_ms=args.lease_ms,
        )

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        pending: set[Future[int]] = set()
        while submitted < args.workflows and len(pending) < args.concurrency:
            pending.add(submit_one(executor, submitted))
            submitted += 1

        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                try:
                    latencies_ns.append(future.result())
                    completed += 1
                except Exception:
                    errors += 1
                    if args.stop_on_error:
                        raise

                while submitted < args.workflows and len(pending) < args.concurrency:
                    pending.add(submit_one(executor, submitted))
                    submitted += 1

    return completed, errors, latencies_ns, latencies_ns


def make_payload(size: int) -> bytes | None:
    if size <= 0:
        return None
    return b"x" * size


def make_client(url: str, *, connections: int, lanes: int, timeout: float) -> FlowClient:
    return FlowClient.from_url(
        url,
        max_connections=max(connections, 1),
        lanes=max(lanes, 1),
        timeout=timeout,
    )


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or f"restate-latency-{uuid.uuid4().hex}"
    flow_type = f"{args.type_prefix}:{run_id}"
    payload = make_payload(args.payload_bytes)
    result = make_payload(args.result_bytes)
    if args.startup_settle_seconds > 0:
        time.sleep(args.startup_settle_seconds)
    client = make_client(
        args.url,
        connections=args.protocol_connections,
        lanes=args.protocol_lanes,
        timeout=args.timeout,
    )
    readiness_completed = run_readiness_probes(
        client,
        args,
        run_id=f"{run_id}-readiness",
        flow_type=flow_type,
    )

    def run_once(
        run_args: argparse.Namespace, *, once_run_id: str, once_flow_type: str
    ) -> tuple[int, int, list[int], list[int]]:
        if run_args.execution_mode == "wave":
            return run_wave_benchmark(
                run_args,
                run_id=once_run_id,
                flow_type=once_flow_type,
                payload=payload,
                result=result,
                client=client,
            )
        return run_serial_benchmark(
            run_args,
            run_id=once_run_id,
            flow_type=once_flow_type,
            payload=payload,
            result=result,
            client=client,
        )

    warmup_completed = 0
    warmup_errors = 0
    if args.warmup_workflows > 0:
        warmup_args = argparse.Namespace(**vars(args))
        warmup_args.workflows = args.warmup_workflows
        warmup_completed, warmup_errors, _warmup_latencies, _warmup_service_latencies = run_once(
            warmup_args,
            once_run_id=f"{run_id}-warmup",
            once_flow_type=f"{flow_type}:warmup",
        )

    started_s = time.perf_counter()
    completed, errors, latencies_ns, service_latencies_ns = run_once(
        args,
        once_run_id=run_id,
        once_flow_type=flow_type,
    )

    elapsed_s = time.perf_counter() - started_s
    verification = verify_sampled_results(
        client,
        args,
        run_id=run_id,
        flow_type=flow_type,
    )
    latencies_ms = [value / 1_000_000.0 for value in latencies_ns]
    service_latencies_ms = [value / 1_000_000.0 for value in service_latencies_ns]
    rps = completed / elapsed_s if elapsed_s > 0 else 0.0
    target_rps = float(args.target_rps or 0.0)
    latency_p50_ms = percentile(latencies_ms, 50)
    latency_p75_ms = percentile(latencies_ms, 75)
    latency_p90_ms = percentile(latencies_ms, 90)
    latency_p95_ms = percentile(latencies_ms, 95)
    latency_p99_ms = percentile(latencies_ms, 99)
    observed_latency_targets = {
        "p50_ms": latency_p50_ms,
        "p90_ms": latency_p90_ms,
        "p99_ms": latency_p99_ms,
    }
    durable_commands_per_workflow = durable_commands_per_workflow_for(args)
    durable_commands = completed * durable_commands_per_workflow
    high_target = RESTATE_HIGH_LOAD_TARGETS.get(args.steps)
    low_target = RESTATE_LOW_LOAD_TARGETS.get(args.steps)
    high_all_latency = target_all_latency_passes(high_target, observed_latency_targets)
    low_all_latency = target_all_latency_passes(low_target, observed_latency_targets)
    diagnostics = wave_diagnostics_result(args)

    return {
        "mode": "protocol-restate-latency",
        "execution_mode": args.execution_mode,
        "chain_submit_mode": args.chain_submit_mode,
        "profile": args.profile,
        "url": args.url,
        "run_id": run_id,
        "flow_type": flow_type,
        "workflows": args.workflows,
        "warmup_workflows": args.warmup_workflows,
        "readiness_probes": args.readiness_probes,
        "readiness_completed": readiness_completed,
        "warmup_completed": warmup_completed,
        "warmup_errors": warmup_errors,
        "verify_sample_requested": verification["requested"],
        "verify_sample_checked": verification["checked"],
        "verify_sample_errors": verification["errors"],
        "completed": completed,
        "errors": errors,
        "steps": args.steps,
        "durable_commands_per_workflow": durable_commands_per_workflow,
        "durable_commands": durable_commands,
        "concurrency": args.concurrency,
        "batch_size": args.batch_size,
        "inflight_batches": args.inflight_batches,
        "target_rps": target_rps,
        "target_achieved_ratio": rps / target_rps if target_rps > 0 else None,
        "partitions": args.partitions,
        "partition_mode": args.partition_mode,
        "shard_local_submit_concurrency": args.shard_local_submit_concurrency,
        "protocol_connections": args.protocol_connections,
        "protocol_lanes": args.protocol_lanes,
        "startup_settle_seconds": args.startup_settle_seconds,
        "payload_bytes": args.payload_bytes,
        "result_bytes": args.result_bytes,
        "elapsed_seconds": elapsed_s,
        "workflows_per_sec": rps,
        "durable_commands_per_sec": durable_commands / elapsed_s if elapsed_s > 0 else 0.0,
        "latency_avg_ms": sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0.0,
        "latency_min_ms": min(latencies_ms) if latencies_ms else 0.0,
        "latency_p50_ms": latency_p50_ms,
        "latency_p75_ms": latency_p75_ms,
        "latency_p90_ms": latency_p90_ms,
        "latency_p95_ms": latency_p95_ms,
        "latency_p99_ms": latency_p99_ms,
        "latency_max_ms": max(latencies_ms) if latencies_ms else 0.0,
        "workflow_service_latency_avg_ms": (
            sum(service_latencies_ms) / len(service_latencies_ms) if service_latencies_ms else 0.0
        ),
        "workflow_service_latency_min_ms": min(service_latencies_ms)
        if service_latencies_ms
        else 0.0,
        "workflow_service_latency_p50_ms": percentile(service_latencies_ms, 50),
        "workflow_service_latency_p75_ms": percentile(service_latencies_ms, 75),
        "workflow_service_latency_p90_ms": percentile(service_latencies_ms, 90),
        "workflow_service_latency_p95_ms": percentile(service_latencies_ms, 95),
        "workflow_service_latency_p99_ms": percentile(service_latencies_ms, 99),
        "workflow_service_latency_max_ms": max(service_latencies_ms)
        if service_latencies_ms
        else 0.0,
        "slow_wave_count": diagnostics["slow_wave_count"],
        "slow_wave_min_ms": diagnostics["slow_wave_min_ms"],
        "slow_waves": diagnostics["slow_waves"],
        "schedule_lag_p99_ms": diagnostics["schedule_lag_p99_ms"],
        "schedule_lag_max_ms": diagnostics["schedule_lag_max_ms"],
        "trace_sample_count": diagnostics["trace_sample_count"],
        "trace_min_ms": diagnostics["trace_min_ms"],
        "trace_samples": diagnostics["trace_samples"],
        "restate_high_load_target": high_target,
        "beats_restate_high_load_p50_latency": target_latency_pass(
            high_target, "p50_ms", latency_p50_ms
        ),
        "beats_restate_high_load_p90_latency": target_latency_pass(
            high_target, "p90_ms", latency_p90_ms
        ),
        "beats_restate_high_load_p99_latency": target_latency_pass(
            high_target, "p99_ms", latency_p99_ms
        ),
        "beats_restate_high_load_all_latency": high_all_latency,
        "beats_restate_high_load_all": (
            bool(high_target) and rps >= high_target["rps"] and high_all_latency
        ),
        "beats_restate_high_load_latency_only": (
            bool(high_target) and latency_p99_ms < high_target["p99_ms"]
        ),
        "beats_restate_high_load_p99": (
            bool(high_target)
            and rps >= high_target["rps"]
            and latency_p99_ms < high_target["p99_ms"]
        ),
        "restate_low_load_target": low_target,
        "beats_restate_low_load_p50_latency": target_latency_pass(
            low_target, "p50_ms", latency_p50_ms
        ),
        "beats_restate_low_load_p90_latency": target_latency_pass(
            low_target, "p90_ms", latency_p90_ms
        ),
        "beats_restate_low_load_p99_latency": target_latency_pass(
            low_target, "p99_ms", latency_p99_ms
        ),
        "beats_restate_low_load_all_latency": low_all_latency,
        "beats_restate_low_load_all": (
            bool(low_target) and rps >= low_target["rps"] and low_all_latency
        ),
        "beats_restate_low_load_latency_only": (
            bool(low_target) and latency_p99_ms < low_target["p99_ms"]
        ),
        "beats_restate_low_load_p99": (
            bool(low_target) and rps >= low_target["rps"] and latency_p99_ms < low_target["p99_ms"]
        ),
    }


def durable_commands_per_workflow_for(args: argparse.Namespace) -> int:
    if args.chain_submit_mode in ("run-steps-many", "run-steps-many-shard-local"):
        return 1
    return args.steps + 1


def apply_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    if args.profile == "restate-high-load":
        profile = RESTATE_HIGH_LOAD_PROFILE[args.steps]
        if args.batch_size is None:
            args.batch_size = profile["batch_size"]
        if args.inflight_batches is None:
            args.inflight_batches = profile["inflight_batches"]
        if args.shard_local_submit_concurrency == 0:
            args.shard_local_submit_concurrency = profile.get(
                "shard_local_submit_concurrency",
                0,
            )
        if args.chain_submit_mode is None:
            args.chain_submit_mode = (
                "run-steps-many-shard-local" if args.steps == 1 else "run-steps-many"
            )
    else:
        if args.batch_size is None:
            args.batch_size = DEFAULT_BATCH_SIZE
        if args.inflight_batches is None:
            args.inflight_batches = DEFAULT_INFLIGHT_BATCHES
        if args.chain_submit_mode is None:
            args.chain_submit_mode = "sequential"
    return args


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FerricFlow native direct-step benchmark shaped like Restate workflow latency tests"
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--workflows", type=int, default=latency_default("workflows"))
    parser.add_argument("--warmup-workflows", type=int, default=latency_default("warmup_workflows"))
    parser.add_argument("--steps", type=int, choices=(1, 3, 9), default=latency_default("steps"))
    parser.add_argument(
        "--execution-mode",
        choices=("wave", "serial"),
        default=latency_default("execution_mode"),
    )
    parser.add_argument(
        "--profile",
        choices=("default", "restate-high-load"),
        default=latency_default("profile"),
        help=(
            "Optional benchmark profile. restate-high-load applies the tuned public-SDK "
            "run-steps-many shape used for Restate high-load comparisons. It does not "
            "pace requests unless --target-rps is explicitly supplied."
        ),
    )
    parser.add_argument(
        "--chain-submit-mode",
        choices=(
            "sequential",
            "predicted-pipeline",
            "run-steps-many",
            "run-steps-many-shard-local",
        ),
        default=None,
        help=(
            "sequential waits for each step batch response; predicted-pipeline sends a "
            "deterministic no-IO step chain as one ordered protocol pipeline; "
            "run-steps-many asks the server to run the deterministic chain in one durable command. "
            "run-steps-many-shard-local preserves that command shape but splits auto-partitioned "
            "waves into shard-local sub-batches for lower tail latency."
        ),
    )
    parser.add_argument("--concurrency", type=int, default=latency_default("concurrency"))
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--inflight-batches", type=int)
    parser.add_argument("--target-rps", type=float, default=latency_default("target_rps"))
    parser.add_argument(
        "--target-catch-up",
        action=argparse.BooleanOptionalAction,
        default=latency_default("target_catch_up"),
        help=(
            "When target pacing falls behind, catch up by submitting back-to-back batches. "
            "Disabled by default because local sleep jitter can create artificial bursts "
            "and inflated Ra queue latency."
        ),
    )
    parser.add_argument("--partitions", type=int, default=latency_default("partitions"))
    parser.add_argument(
        "--partition-mode",
        choices=("auto", "explicit"),
        default=latency_default("partition_mode"),
    )
    parser.add_argument(
        "--shard-local-submit-concurrency",
        type=int,
        default=latency_default("shard_local_submit_concurrency"),
        help=(
            "For run-steps-many-shard-local, limit concurrent per-shard sub-batch "
            "submissions. 0 means use --partitions."
        ),
    )
    parser.add_argument(
        "--protocol-connections", type=int, default=latency_default("protocol_connections")
    )
    parser.add_argument("--protocol-lanes", type=int, default=latency_default("protocol_lanes"))
    parser.add_argument("--payload-bytes", type=int, default=latency_default("payload_bytes"))
    parser.add_argument("--result-bytes", type=int, default=latency_default("result_bytes"))
    parser.add_argument("--lease-ms", type=int, default=latency_default("lease_ms"))
    parser.add_argument("--timeout", type=float, default=latency_default("timeout"))
    parser.add_argument(
        "--startup-settle-seconds",
        type=float,
        default=latency_default("startup_settle_seconds"),
        help=(
            "Sleep before warmup/measurement. Useful when the native listener is open but "
            "Raft shards are still electing leaders after a fresh server start."
        ),
    )
    parser.add_argument(
        "--readiness-probes",
        type=int,
        default=latency_default("readiness_probes"),
        help=(
            "Run this many one-item FLOW.RUN_STEPS_MANY probe writes before warmup and "
            "measurement. Useful for proving the public protocol path can accept durable work."
        ),
    )
    parser.add_argument(
        "--verify-sample",
        type=int,
        default=latency_default("verify_sample"),
        help=(
            "After measurement, sample this many workflow ids with FLOW.GET and verify they "
            "reached terminal completed state. Runs outside the timed benchmark window."
        ),
    )
    parser.add_argument(
        "--slow-wave-count",
        type=int,
        default=latency_default("slow_wave_count"),
        help=(
            "Record and report the N slowest benchmark waves. Disabled by default to avoid "
            "extra per-wave allocations in normal benchmark runs."
        ),
    )
    parser.add_argument(
        "--slow-wave-min-ms",
        type=float,
        default=latency_default("slow_wave_min_ms"),
        help="Only record slow-wave diagnostics for waves at or above this latency.",
    )
    parser.add_argument(
        "--trace-sample-count",
        type=int,
        default=latency_default("trace_sample_count"),
        help=(
            "Debug only: trace RUN_STEPS_MANY shard-local sub-batches and report the N "
            "slowest traced samples. This adds trace overhead and should not be used for "
            "published benchmark numbers."
        ),
    )
    parser.add_argument(
        "--trace-min-ms",
        type=float,
        default=latency_default("trace_min_ms"),
        help="Only keep traced sub-batch samples at or above this latency.",
    )
    parser.add_argument("--type-prefix", default=latency_default("type_prefix"))
    parser.add_argument("--run-id")
    parser.add_argument(
        "--stop-on-error",
        action=argparse.BooleanOptionalAction,
        default=latency_default("stop_on_error"),
    )
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)
    apply_profile_defaults(args)

    if args.workflows <= 0:
        parser.error("--workflows must be positive")
    if args.warmup_workflows < 0:
        parser.error("--warmup-workflows must be non-negative")
    if args.concurrency <= 0:
        parser.error("--concurrency must be positive")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.inflight_batches <= 0:
        parser.error("--inflight-batches must be positive")
    if args.target_rps < 0:
        parser.error("--target-rps must be non-negative")
    if args.partitions <= 0:
        parser.error("--partitions must be positive")
    if args.shard_local_submit_concurrency < 0:
        parser.error("--shard-local-submit-concurrency must be non-negative")
    if args.protocol_connections <= 0:
        parser.error("--protocol-connections must be positive")
    if args.protocol_lanes <= 0:
        parser.error("--protocol-lanes must be positive")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
    if args.result_bytes < 0:
        parser.error("--result-bytes must be non-negative")
    if args.lease_ms <= 0:
        parser.error("--lease-ms must be positive")
    if args.startup_settle_seconds < 0:
        parser.error("--startup-settle-seconds must be non-negative")
    if args.readiness_probes < 0:
        parser.error("--readiness-probes must be non-negative")
    if args.verify_sample < 0:
        parser.error("--verify-sample must be non-negative")
    if args.slow_wave_count < 0:
        parser.error("--slow-wave-count must be non-negative")
    if args.slow_wave_min_ms < 0:
        parser.error("--slow-wave-min-ms must be non-negative")
    if args.trace_sample_count < 0:
        parser.error("--trace-sample-count must be non-negative")
    if args.trace_min_ms < 0:
        parser.error("--trace-min-ms must be non-negative")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run_benchmark(args)
    if args.pretty:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
