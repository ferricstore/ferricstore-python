#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
import uuid
from concurrent.futures import Future
from typing import Any

from ferricstore.protocol import ProtocolAdapter


def make_payload(size: int) -> bytes:
    return b"x" * size


def flow_id(flow_type: str, index: int) -> str:
    return f"{flow_type}:{index}"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0

    ordered = sorted(values)
    k = (len(ordered) - 1) * pct / 100.0
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - k) + ordered[hi] * (k - lo)


def create_commands(
    flow_type: str, start: int, end: int, payload: bytes, partition_key: str | None = None
) -> list[tuple[Any, ...]]:
    commands: list[tuple[Any, ...]] = []

    for index in range(start, end):
        command: tuple[Any, ...] = (
            "FLOW.CREATE",
            flow_id(flow_type, index),
            "TYPE",
            flow_type,
            "STATE",
            "queued",
            "PAYLOAD",
            payload,
        )
        if partition_key is not None:
            command = (*command[:6], "PARTITION", partition_key, *command[6:])
        commands.append(command)

    return commands


def value_put_commands(
    flow_type: str, start: int, end: int, payload: bytes, partition_key: str | None = None
) -> list[tuple[Any, ...]]:
    commands: list[tuple[Any, ...]] = []

    for index in range(start, end):
        command: tuple[Any, ...] = (
            "FLOW.VALUE.PUT",
            payload,
            "OWNER_FLOW_ID",
            flow_id(flow_type, index),
            "NAME",
            "bench-value",
        )
        if partition_key is not None:
            command += ("PARTITION", partition_key)
        commands.append(command)

    return commands


def flow_get_commands(
    mode: str,
    flow_type: str,
    sequence: int,
    count: int,
    total_flows: int,
    partition_key: str | None = None,
) -> list[tuple[Any, ...]]:
    if mode == "flow-get":
        return [
            flow_get_command(flow_type, (sequence + item) % total_flows, partition_key, False)
            for item in range(count)
        ]

    if mode == "flow-get-meta":
        return [
            flow_get_command(flow_type, (sequence + item) % total_flows, partition_key, True)
            for item in range(count)
        ]

    raise ValueError(f"unsupported Flow GET mode: {mode}")


def flow_get_command(
    flow_type: str, index: int, partition_key: str | None, meta: bool
) -> tuple[Any, ...]:
    command: tuple[Any, ...] = ("FLOW.GET", flow_id(flow_type, index))
    if partition_key is not None:
        command += ("PARTITION", partition_key)
    if meta:
        command += ("RETURN", "META")
    return command


def flow_value_mget_command(
    refs: list[Any], sequence: int, count: int, max_bytes: int
) -> tuple[Any, ...]:
    selected = [refs[(sequence + item) % len(refs)] for item in range(count)]
    return ("FLOW.VALUE.MGET", *selected, "MAX_BYTES", max_bytes)


def wait_some(pending: list[tuple[int, Future[Any], int]]) -> tuple[int, list[float]]:
    started_ns, future, expected_items = pending.pop(0)
    result = future.result()

    if isinstance(result, list) and expected_items > 1 and len(result) != expected_items:
        raise RuntimeError(f"expected {expected_items} results, got {len(result)}")

    elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
    return expected_items, [elapsed_ms]


def warmup_flows(
    adapter: ProtocolAdapter, args: argparse.Namespace, flow_type: str, payload: bytes
) -> float:
    start_time = time.perf_counter()
    pending: list[Future[Any]] = []

    for start in range(0, args.flows, args.create_batch_size):
        end = min(start + args.create_batch_size, args.flows)
        pending.append(
            adapter.submit_batch(
                create_commands(flow_type, start, end, payload, args.partition_key)
            )
        )

        if len(pending) >= args.inflight_batches:
            pending.pop(0).result()

    for future in pending:
        future.result()

    return time.perf_counter() - start_time


def warmup_value_refs(
    adapter: ProtocolAdapter, args: argparse.Namespace, flow_type: str, payload: bytes
) -> tuple[float, list[Any]]:
    start_time = time.perf_counter()
    pending: list[Future[Any]] = []
    refs: list[Any] = []

    for start in range(0, args.flows, args.create_batch_size):
        end = min(start + args.create_batch_size, args.flows)
        pending.append(
            adapter.submit_batch(
                value_put_commands(flow_type, start, end, payload, args.partition_key)
            )
        )

        if len(pending) >= args.inflight_batches:
            refs.extend(value_refs_from_results(pending.pop(0).result()))

    for future in pending:
        refs.extend(value_refs_from_results(future.result()))

    return time.perf_counter() - start_time, refs


def value_refs_from_results(results: list[Any]) -> list[Any]:
    refs: list[Any] = []

    for result in results:
        if isinstance(result, dict):
            ref = result.get(b"ref")
            if ref is None:
                ref = result.get("ref")
            if ref is None:
                raise RuntimeError("FLOW.VALUE.PUT result did not include ref")
            refs.append(ref)
        else:
            refs.append(result)

    return refs


def run_batched_flow_get(
    adapter: ProtocolAdapter, args: argparse.Namespace, flow_type: str
) -> dict[str, Any]:
    deadline = time.perf_counter() + args.test_time
    sequence = 0
    requests = 0
    latencies_ms: list[float] = []
    pending: list[tuple[int, Future[Any], int]] = []

    while time.perf_counter() < deadline or pending:
        while time.perf_counter() < deadline and len(pending) < args.inflight_batches:
            commands = flow_get_commands(
                args.mode, flow_type, sequence, args.read_batch_size, args.flows, args.partition_key
            )
            sequence += args.read_batch_size
            pending.append(
                (time.perf_counter_ns(), adapter.submit_batch(commands), args.read_batch_size)
            )

        completed, latencies = wait_some(pending)
        requests += completed
        latencies_ms.extend(latencies)

    return benchmark_result(args, requests, requests, latencies_ms)


def run_flow_value_mget(
    adapter: ProtocolAdapter, args: argparse.Namespace, refs: list[Any]
) -> dict[str, Any]:
    deadline = time.perf_counter() + args.test_time
    sequence = 0
    requests = 0
    values = 0
    latencies_ms: list[float] = []
    pending: list[tuple[int, Future[Any], int]] = []

    while time.perf_counter() < deadline or pending:
        while time.perf_counter() < deadline and len(pending) < args.inflight_batches:
            command = flow_value_mget_command(
                refs, sequence, args.read_batch_size, args.value_max_bytes
            )
            sequence += args.read_batch_size
            pending.append(
                (time.perf_counter_ns(), adapter.submit_command(*command), args.read_batch_size)
            )

        completed, latencies = wait_some(pending)
        requests += 1
        values += completed
        latencies_ms.extend(latencies)

    return benchmark_result(args, requests, values, latencies_ms)


def run_flow_list_meta(
    adapter: ProtocolAdapter, args: argparse.Namespace, flow_type: str
) -> dict[str, Any]:
    deadline = time.perf_counter() + args.test_time
    requests = 0
    records = 0
    latencies_ms: list[float] = []
    pending: list[tuple[int, Future[Any], int]] = []

    while time.perf_counter() < deadline or pending:
        while time.perf_counter() < deadline and len(pending) < args.inflight_batches:
            future = adapter.submit_command(
                "FLOW.LIST",
                flow_type,
                "STATE",
                "queued",
                "COUNT",
                args.list_count,
                *(("PARTITION", args.partition_key) if args.partition_key is not None else ()),
                "RETURN",
                "META",
            )
            pending.append((time.perf_counter_ns(), future, 1))

        started_ns, future, _expected_items = pending.pop(0)
        result = future.result()
        if not isinstance(result, list):
            raise RuntimeError(f"expected FLOW.LIST result list, got {type(result)!r}")

        requests += 1
        records += len(result)
        latencies_ms.append((time.perf_counter_ns() - started_ns) / 1_000_000)

    return benchmark_result(args, requests, records, latencies_ms)


def benchmark_result(
    args: argparse.Namespace, requests: int, items: int, latencies_ms: list[float]
) -> dict[str, Any]:
    return {
        "benchmark": "protocol_flow_read",
        "mode": args.mode,
        "url": args.url,
        "flows": args.flows,
        "test_time": args.test_time,
        "requests": requests,
        "requests_per_sec": requests / args.test_time if args.test_time > 0 else 0.0,
        "items": items,
        "items_per_sec": items / args.test_time if args.test_time > 0 else 0.0,
        "create_batch_size": args.create_batch_size,
        "read_batch_size": args.read_batch_size,
        "list_count": args.list_count if args.mode == "flow-list-meta" else None,
        "inflight_batches": args.inflight_batches,
        "value_bytes": args.value_bytes,
        "partition_key": args.partition_key,
        "batch_latency_p50_ms": percentile(latencies_ms, 50),
        "batch_latency_p95_ms": percentile(latencies_ms, 95),
        "batch_latency_p99_ms": percentile(latencies_ms, 99),
        "batch_latency_max_ms": max(latencies_ms) if latencies_ms else 0.0,
        "batch_latency_samples": len(latencies_ms),
    }


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    payload = make_payload(args.value_bytes)
    flow_type = args.flow_type or f"protocol-flow-read:{args.mode}:{uuid.uuid4().hex}"

    adapter = ProtocolAdapter.from_url(args.url, timeout=args.timeout)
    try:
        create_seconds = warmup_flows(adapter, args, flow_type, payload)
        result: dict[str, Any]

        if args.mode in {"flow-get", "flow-get-meta"}:
            result = run_batched_flow_get(adapter, args, flow_type)
        elif args.mode == "flow-list-meta":
            result = run_flow_list_meta(adapter, args, flow_type)
        elif args.mode == "flow-value-mget":
            value_seconds, refs = warmup_value_refs(adapter, args, flow_type, payload)
            result = run_flow_value_mget(adapter, args, refs)
            result["value_warmup_seconds"] = value_seconds
        else:
            raise ValueError(f"unsupported mode: {args.mode}")

        result["flow_type"] = flow_type
        result["create_seconds"] = create_seconds
        return result
    finally:
        adapter.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FerricStore native protocol Flow read benchmark")
    parser.add_argument(
        "--mode",
        choices=("flow-get", "flow-get-meta", "flow-list-meta", "flow-value-mget"),
        default="flow-get-meta",
    )
    parser.add_argument("--url", default="ferric://127.0.0.1:6388")
    parser.add_argument("--flow-type", default=None)
    parser.add_argument("--partition-key", default=None)
    parser.add_argument("--flows", type=int, default=100_000)
    parser.add_argument("--test-time", type=float, default=30.0)
    parser.add_argument("--create-batch-size", type=int, default=500)
    parser.add_argument("--read-batch-size", type=int, default=500)
    parser.add_argument("--list-count", type=int, default=100)
    parser.add_argument("--inflight-batches", type=int, default=64)
    parser.add_argument("--value-bytes", type=int, default=32)
    parser.add_argument("--value-max-bytes", type=int, default=64 * 1024)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--pretty", action="store_true")

    args = parser.parse_args(argv)

    if args.flows <= 0:
        parser.error("--flows must be positive")
    if args.test_time <= 0:
        parser.error("--test-time must be positive")
    if args.create_batch_size <= 0:
        parser.error("--create-batch-size must be positive")
    if args.read_batch_size <= 0:
        parser.error("--read-batch-size must be positive")
    if args.list_count <= 0:
        parser.error("--list-count must be positive")
    if args.inflight_batches <= 0:
        parser.error("--inflight-batches must be positive")
    if args.value_bytes < 0:
        parser.error("--value-bytes must be non-negative")
    if args.value_max_bytes < 0:
        parser.error("--value-max-bytes must be non-negative")

    return args


def main() -> None:
    args = parse_args()
    result = run_benchmark(args)
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
