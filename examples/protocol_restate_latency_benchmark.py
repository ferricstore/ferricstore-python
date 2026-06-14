from __future__ import annotations

import argparse
import json
import math
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any, Callable

from ferricstore.client import FlowClient, _auto_partition_key_for_id
from ferricstore.types import ClaimedItem

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


def target_all_latency_passes(
    target: dict[str, float] | None, observed: dict[str, float]
) -> bool:
    if not target:
        return False

    latency_keys = ("p50_ms", "p90_ms", "p99_ms")
    required = [key for key in latency_keys if key in target]
    return bool(required) and all(observed[key] < target[key] for key in required)


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


def response_job(value: Any) -> ClaimedItem:
    if isinstance(value, ClaimedItem):
        return value
    if isinstance(value, (tuple, list)) and len(value) >= 4:
        return ClaimedItem(
            str(value[0].decode() if isinstance(value[0], bytes) else value[0]),
            bytes(value[2]),
            int(value[3]),
            partition_key=None
            if value[1] in (None, b"", "")
            else str(value[1].decode() if isinstance(value[1], bytes) else value[1]),
        )
    if isinstance(value, dict):
        raw = {key.decode() if isinstance(key, bytes) else key: item for key, item in value.items()}
        return ClaimedItem(
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
            else str(raw["run_state"].decode() if isinstance(raw["run_state"], bytes) else raw["run_state"]),
        )
    return ClaimedItem(
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
    job: ClaimedItem,
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


def predicted_step_job_for(spec: WorkflowSpec, *, start_now_ms: int, step: int) -> ClaimedItem:
    token_now_ms = start_now_ms if step <= 1 else start_now_ms + step - 1
    return ClaimedItem(
        flow_id_for(spec),
        predicted_lease_token(spec.worker, token_now_ms, step),
        step,
        partition_key=effective_partition_key(spec),
        run_state=state_name(step),
    )


def predicted_terminal_job_for(
    spec: WorkflowSpec, *, start_now_ms: int, steps: int
) -> ClaimedItem:
    fencing_token = max(steps, 1)
    token_now_ms = start_now_ms if steps <= 1 else start_now_ms + steps - 1
    return ClaimedItem(
        flow_id_for(spec),
        predicted_lease_token(spec.worker, token_now_ms, fencing_token),
        fencing_token,
        partition_key=effective_partition_key(spec),
        run_state=state_name(steps),
    )


def complete_command(
    job: ClaimedItem,
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

    def run_one_wave(start: int) -> tuple[int, int]:
        count = min(batch_size, args.workflows - start)
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
        started_ns = time.perf_counter_ns()
        run_wave_batch(
            client,
            specs,
            payload=payload,
            result=result,
            lease_ms=args.lease_ms,
            chain_submit_mode=args.chain_submit_mode,
        )
        return count, time.perf_counter_ns() - started_ns

    def collect_future(future: Future[tuple[int, int]], fallback_count: int) -> None:
        nonlocal completed, errors
        try:
            count, elapsed_ns = future.result()
            latencies_ns.extend([elapsed_ns] * count)
            service_latency_ns = elapsed_ns // max(count, 1)
            service_latencies_ns.extend([service_latency_ns] * count)
            completed += count
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

        with ThreadPoolExecutor(max_workers=inflight_batches) as executor:
            pending: set[Future[tuple[int, int]]] = set()

            while submitted_starts < args.workflows:
                due_s = scheduled_base_s + batch_index * batch_interval_s
                sleep_s = due_s - time.perf_counter()
                if sleep_s > 0:
                    time.sleep(sleep_s)

                while len(pending) >= inflight_batches:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        collect_future(future, batch_size)

                pending.add(executor.submit(run_one_wave, submitted_starts))
                submitted_starts += batch_size
                batch_index += 1

            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    collect_future(future, batch_size)

        return completed, errors, latencies_ns, service_latencies_ns

    if inflight_batches == 1:
        for start in range(0, args.workflows, batch_size):
            try:
                count, elapsed_ns = run_one_wave(start)
                latencies_ns.extend([elapsed_ns] * count)
                service_latency_ns = elapsed_ns // max(count, 1)
                service_latencies_ns.extend([service_latency_ns] * count)
                completed += count
            except Exception:
                errors += min(batch_size, args.workflows - start)
                if args.stop_on_error:
                    raise
        return completed, errors, latencies_ns, service_latencies_ns

    submitted_starts = 0
    with ThreadPoolExecutor(max_workers=inflight_batches) as executor:
        pending: set[Future[tuple[int, int]]] = set()
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
    client = make_client(
        args.url,
        connections=args.protocol_connections,
        lanes=args.protocol_lanes,
        timeout=args.timeout,
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

    return {
        "mode": "protocol-restate-latency",
        "execution_mode": args.execution_mode,
        "chain_submit_mode": args.chain_submit_mode,
        "url": args.url,
        "run_id": run_id,
        "flow_type": flow_type,
        "workflows": args.workflows,
        "warmup_workflows": args.warmup_workflows,
        "warmup_completed": warmup_completed,
        "warmup_errors": warmup_errors,
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
        "protocol_connections": args.protocol_connections,
        "protocol_lanes": args.protocol_lanes,
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
            bool(low_target)
            and rps >= low_target["rps"]
            and latency_p99_ms < low_target["p99_ms"]
        ),
    }


def durable_commands_per_workflow_for(args: argparse.Namespace) -> int:
    if args.chain_submit_mode == "run-steps-many":
        return 1
    return args.steps + 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FerricFlow native direct-step benchmark shaped like Restate workflow latency tests"
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--workflows", type=int, default=10_000)
    parser.add_argument("--warmup-workflows", type=int, default=0)
    parser.add_argument("--steps", type=int, choices=(1, 3, 9), default=3)
    parser.add_argument("--execution-mode", choices=("wave", "serial"), default="wave")
    parser.add_argument(
        "--chain-submit-mode",
        choices=("sequential", "predicted-pipeline", "run-steps-many"),
        default="sequential",
        help=(
            "sequential waits for each step batch response; predicted-pipeline sends a "
            "deterministic no-IO step chain as one ordered protocol pipeline; "
            "run-steps-many asks the server to run the deterministic chain in one durable command."
        ),
    )
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--inflight-batches", type=int, default=1)
    parser.add_argument("--target-rps", type=float, default=0.0)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--partition-mode", choices=("auto", "explicit"), default="auto")
    parser.add_argument("--protocol-connections", type=int, default=1)
    parser.add_argument("--protocol-lanes", type=int, default=64)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--result-bytes", type=int, default=0)
    parser.add_argument("--lease-ms", type=int, default=30_000)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--type-prefix", default=DEFAULT_TYPE_PREFIX)
    parser.add_argument("--run-id")
    parser.add_argument("--stop-on-error", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)

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
