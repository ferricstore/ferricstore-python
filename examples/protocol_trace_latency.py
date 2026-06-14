from __future__ import annotations

import argparse
import math
import time
import uuid
from collections import defaultdict
from typing import Any

from ferricstore import ClaimedItem
from ferricstore.protocol import ProtocolAdapterPool


STAGE_ORDER = [
    "client.encode_us",
    "client.socket_write_us",
    "server.server_decode_us",
    "server.server_route_us",
    "server.server_lane_queue_wait_us",
    "server.server_body_decode_us",
    "server.server_ra_wait_us",
    "server.server_apply_us",
    "server.server_bitcask_append_us",
    "server.server_pending_locations_us",
    "server.server_flow_index_update_us",
    "server.server_zset_index_update_us",
    "server.server_command_execute_us",
    "server.server_response_encode_us",
    "client.response_read_us",
    "client.decode_us",
]


def percentile(values: list[int], pct: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def flatten_trace(trace: dict[str, Any]) -> dict[str, int]:
    flattened: dict[str, int] = {}
    for side in ("client", "server"):
        values = trace.get(side, {})
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            if isinstance(value, int):
                flattened[f"{side}.{key}"] = value
    return flattened


def record_trace(samples: dict[str, list[int]], trace: dict[str, Any]) -> None:
    for key, value in flatten_trace(trace).items():
        samples[key].append(value)


def now_ms() -> int:
    return int(time.time() * 1000)


def run_set(adapter: ProtocolAdapterPool, key: str, value: bytes) -> dict[str, Any]:
    return adapter.execute_command_with_trace("SET", key, value)


def run_get(adapter: ProtocolAdapterPool, key: str) -> dict[str, Any]:
    return adapter.execute_command_with_trace("GET", key)


def run_flow_once(
    adapter: ProtocolAdapterPool,
    flow_type: str,
    partition_key: str,
    flow_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    created_at_ms = now_ms()

    create = adapter.execute_command_with_trace(
        "FLOW.CREATE",
        flow_id,
        "TYPE",
        flow_type,
        "STATE",
        "queued",
        "PARTITION",
        partition_key,
        "NOW",
        created_at_ms,
        "RUN_AT",
        created_at_ms,
    )

    claim = adapter.execute_command_with_trace(
        "FLOW.CLAIM_DUE",
        flow_type,
        "STATE",
        "queued",
        "WORKER",
        "trace-worker",
        "LEASE_MS",
        30_000,
        "LIMIT",
        1,
        "PARTITION",
        partition_key,
        "NOW",
        now_ms(),
        "RETURN",
        "JOBS_COMPACT",
        "RECLAIM_EXPIRED",
        "false",
    )

    jobs = claim["value"]
    if not jobs:
        raise RuntimeError(f"FLOW.CLAIM_DUE returned no jobs for {flow_id}")

    job = ClaimedItem.from_resp(jobs[0])
    complete_args: list[Any] = [
        "FLOW.COMPLETE",
        job.id,
        job.lease_token,
        "FENCING",
        job.fencing_token,
        "NOW",
        now_ms(),
        "RESULT",
        b"ok",
    ]
    if job.partition_key:
        complete_args.extend(["PARTITION", job.partition_key])

    complete = adapter.execute_command_with_trace(*complete_args)
    return [("flow_create", create), ("flow_claim_due", claim), ("flow_complete", complete)]


def print_summary(title: str, samples: dict[str, list[int]]) -> None:
    print(f"\n## {title}")
    print(f"{'stage':42} {'n':>6} {'avg_us':>10} {'p50_us':>10} {'p95_us':>10} {'p99_us':>10} {'max_us':>10}")
    print("-" * 104)

    keys = [key for key in STAGE_ORDER if key in samples]
    keys.extend(sorted(key for key in samples if key not in set(keys)))

    for key in keys:
        values = samples[key]
        if not values:
            continue
        avg = int(sum(values) / len(values))
        print(
            f"{key:42} {len(values):6d} {avg:10d} "
            f"{percentile(values, 50):10d} {percentile(values, 95):10d} "
            f"{percentile(values, 99):10d} {max(values):10d}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sample FerricStore protocol per-stage latency trace")
    parser.add_argument("--url", default="ferric://127.0.0.1:6388")
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--op", choices=["set", "get", "flow"], default="set")
    parser.add_argument("--value-bytes", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    adapter = ProtocolAdapterPool.from_url(args.url, max_connections=1, timeout=args.timeout)
    samples: dict[str, list[int]] = defaultdict(list)

    run_id = uuid.uuid4().hex[:12]
    key = f"trace:{run_id}:key"
    value = b"x" * args.value_bytes

    try:
        if args.op == "get":
            adapter.execute_command("SET", key, value)

        total = args.warmup + args.samples
        for index in range(total):
            if args.op == "set":
                result = run_set(adapter, f"{key}:{index}", value)
                label = "SET"
            elif args.op == "get":
                result = run_get(adapter, key)
                label = "GET"
            else:
                traces = run_flow_once(
                    adapter,
                    flow_type=f"trace_{run_id}",
                    partition_key=f"trace-partition-{run_id}",
                    flow_id=f"trace-{run_id}-{index}",
                )
                if index >= args.warmup:
                    by_command: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
                    for command_label, command_result in traces:
                        record_trace(by_command[command_label], command_result["trace"])
                    for command_label, command_samples in by_command.items():
                        for stage, values in command_samples.items():
                            samples[f"{command_label}.{stage}"].extend(values)
                continue

            if index >= args.warmup:
                record_trace(samples, result["trace"])

        print_summary(args.op.upper() if args.op != "flow" else "FLOW create/claim/complete", samples)
    finally:
        adapter.close()


if __name__ == "__main__":
    main()
