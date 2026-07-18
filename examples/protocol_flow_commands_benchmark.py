import argparse
import json
import math
import struct
import sys
import time
import uuid
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from ferricstore.protocol import ProtocolAdapterPool

DEFAULT_URL = "ferric://127.0.0.1:6388"
PROTOCOL_SCHEMES = {"ferric", "ferrics"}
FLOW_STATE = "queued"
RUNNING_STATE = "running"
NEXT_STATE = "next"
PARTITION_PREFIX = "protocol-flow-benchmark"
_COMPACT_U32 = struct.Struct(">I")
_COMPACT_I64 = struct.Struct(">q")
_COMPACT_PIPELINE_HEADER = struct.Struct(">BBI")
_COMPACT_PIPELINE_REQUEST = 0x94
_COMPACT_CREATE_MANY_PARTITION_REQUEST = 0x96
_COMPACT_COMPLETE_MANY_OK_REQUEST = 0x93
_COMPACT_RETRY_MANY_OK_REQUEST = 0x98
_COMPACT_TRANSITION_MANY_OK_REQUEST = 0x9C
_COMPACT_FLOW_VALUE_MGET_REQUEST = 0x9D
_COMPACT_BOOL_TRUE = 2
_COMPACT_BOOL_FALSE = 1
_COMPACT_RETURN_OK_ON_SUCCESS = 1
_NULL_U32 = 0xFFFFFFFF
_I64_MIN = -(1 << 63)


ClaimedJob = tuple[str, str | None, bytes, int]


def now_ms() -> int:
    return int(time.time() * 1000)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def payload_bytes(size: int) -> bytes:
    return b"x" * max(size, 0)


def flow_id(run_id: str, index: int) -> str:
    return f"{run_id}:flow:{index}"


def partition_key(index: int, partitions: int) -> str:
    return f"{PARTITION_PREFIX}:{index % max(partitions, 1)}"


def partition_keys(partitions: int) -> list[str]:
    return [partition_key(index, partitions) for index in range(max(partitions, 1))]


def compact_binary(value: bytes) -> bytes:
    return _COMPACT_U32.pack(len(value)) + value


def compact_optional_binary(value: bytes | None) -> bytes:
    if value is None:
        return _COMPACT_U32.pack(_NULL_U32)
    return compact_binary(value)


def value_mget_payload_batch(refs: list[str], *, max_bytes: int = _I64_MIN) -> bytes | None:
    if not refs:
        return None

    parts = [
        bytes([_COMPACT_FLOW_VALUE_MGET_REQUEST]),
        _COMPACT_I64.pack(max_bytes),
        _COMPACT_U32.pack(len(refs)),
    ]
    for ref in refs:
        parts.append(compact_binary(ref.encode()))
    return b"".join(parts)


def flow_id_bytes(run_id: str, index: int) -> bytes:
    return f"{run_id}:flow:{index}".encode()


def create_many_payload_batch(
    *,
    run_id: str,
    flow_type: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
    payload: bytes,
    retention_ttl_ms: int,
) -> bytes | None:
    if count <= 0 or retention_ttl_ms > 0:
        return None

    timestamp = now_ms()
    partition = partition_key(start // max(batch_size, 1), partitions).encode()
    parts = [
        bytes([_COMPACT_CREATE_MANY_PARTITION_REQUEST]),
        compact_binary(flow_type.encode()),
        compact_binary(FLOW_STATE.encode()),
        compact_binary(partition),
        struct.pack(
            ">qqBBI",
            timestamp,
            timestamp,
            _COMPACT_BOOL_TRUE,
            _COMPACT_RETURN_OK_ON_SUCCESS,
            count,
        ),
    ]
    item_payload = compact_binary(payload)
    for offset in range(count):
        parts.append(compact_binary(flow_id_bytes(run_id, start + offset)))
        parts.append(item_payload)
    return b"".join(parts)


def create_many_command(
    *,
    run_id: str,
    flow_type: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
    payload: bytes,
    retention_ttl_ms: int,
) -> tuple[Any, ...]:
    timestamp = now_ms()
    args: list[Any] = [
        "FLOW.CREATE_MANY",
        partition_key(start // max(batch_size, 1), partitions),
        "TYPE",
        flow_type,
        "STATE",
        FLOW_STATE,
        "NOW",
        timestamp,
        "RUN_AT",
        timestamp,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
    ]
    if retention_ttl_ms > 0:
        args.extend(["RETENTION_TTL_MS", retention_ttl_ms])
    args.append("ITEMS")
    for offset in range(count):
        args.extend([flow_id(run_id, start + offset), payload])
    return tuple(args)


def claim_due_command(
    *,
    flow_type: str,
    worker: str,
    partitions: int,
    batch_size: int,
) -> tuple[Any, ...]:
    keys = partition_keys(partitions)
    return (
        "FLOW.CLAIM_DUE",
        flow_type,
        "STATE",
        FLOW_STATE,
        "WORKER",
        worker,
        "LEASE_MS",
        30_000,
        "LIMIT",
        batch_size,
        "RETURN",
        "JOBS_COMPACT",
        "PARTITIONS",
        len(keys),
        *keys,
        "BLOCK",
        -1,
        "RECLAIM_EXPIRED",
        "false",
        "RECLAIM_RATIO",
        0,
    )


def complete_many_command(jobs: list[ClaimedJob]) -> tuple[Any, ...]:
    args: list[Any] = [
        "FLOW.COMPLETE_MANY",
        "MIXED",
        "NOW",
        now_ms(),
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
    ]
    for id_value, partition, lease, fencing in jobs:
        args.extend([id_value, partition or "-", lease, fencing])
    return tuple(args)


def retry_many_command(jobs: list[ClaimedJob]) -> tuple[Any, ...]:
    args: list[Any] = [
        "FLOW.RETRY_MANY",
        "MIXED",
        "NOW",
        now_ms(),
        "RUN_AT",
        now_ms(),
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
    ]
    for id_value, partition, lease, fencing in jobs:
        args.extend([id_value, partition or "-", lease, fencing])
    return tuple(args)


def fail_many_command(jobs: list[ClaimedJob]) -> tuple[Any, ...]:
    args: list[Any] = [
        "FLOW.FAIL_MANY",
        "MIXED",
        "NOW",
        now_ms(),
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
    ]
    for id_value, partition, lease, fencing in jobs:
        args.extend([id_value, partition or "-", lease, fencing])
    return tuple(args)


def cancel_many_command(jobs: list[ClaimedJob]) -> tuple[Any, ...]:
    args: list[Any] = [
        "FLOW.CANCEL_MANY",
        "MIXED",
        "NOW",
        now_ms(),
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
    ]
    for id_value, partition, _lease, fencing in jobs:
        args.extend([id_value, partition or "-", fencing])
    return tuple(args)


def cancel_many_created_command(
    *,
    run_id: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
) -> tuple[Any, ...]:
    args: list[Any] = [
        "FLOW.CANCEL_MANY",
        "MIXED",
        "NOW",
        now_ms(),
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
    ]
    for offset in range(count):
        index = start + offset
        args.extend(
            [flow_id(run_id, index), partition_key(index // max(batch_size, 1), partitions), 0]
        )
    return tuple(args)


def transition_many_command(
    *,
    run_id: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
) -> tuple[Any, ...]:
    timestamp = now_ms()
    args: list[Any] = [
        "FLOW.TRANSITION_MANY",
        "MIXED",
        FLOW_STATE,
        NEXT_STATE,
        "NOW",
        timestamp,
        "RUN_AT",
        timestamp,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
    ]
    for offset in range(count):
        index = start + offset
        args.extend(
            [
                flow_id(run_id, index),
                partition_key(index // max(batch_size, 1), partitions),
                0,
                None,
            ]
        )
    return tuple(args)


def transition_many_payload_batch(
    *,
    run_id: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
) -> bytes | None:
    if count <= 0:
        return None

    timestamp = now_ms()
    parts = [
        bytes([_COMPACT_TRANSITION_MANY_OK_REQUEST]),
        compact_binary(FLOW_STATE.encode()),
        compact_binary(NEXT_STATE.encode()),
        compact_optional_binary(None),
        _COMPACT_I64.pack(timestamp),
        _COMPACT_I64.pack(timestamp),
        bytes([_COMPACT_BOOL_TRUE]),
        _COMPACT_U32.pack(count),
    ]
    for offset in range(count):
        index = start + offset
        parts.append(compact_binary(flow_id_bytes(run_id, index)))
        parts.append(
            compact_optional_binary(partition_key(index // max(batch_size, 1), partitions).encode())
        )
        parts.append(_COMPACT_I64.pack(0))
        parts.append(compact_optional_binary(None))
    return b"".join(parts)


def value_put_command(_index: int, value: bytes) -> tuple[Any, ...]:
    return ("FLOW.VALUE.PUT", value, "NOW", now_ms())


def value_put_ok_command(_index: int, value: bytes) -> tuple[Any, ...]:
    return ("FLOW.VALUE.PUT", value, "NOW", now_ms(), "RETURN", "OK_ON_SUCCESS")


def shared_value_put_payload_batch(
    *,
    value: bytes,
    count: int,
    return_ok: bool,
) -> bytes | None:
    if count <= 0:
        return None

    mode = 15 if return_ok else 7
    item = compact_binary(value) + _COMPACT_I64.pack(now_ms())
    return _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | mode, count) + (
        item * count
    )


def owned_value_put_command(
    *,
    run_id: str,
    index: int,
    batch_size: int,
    partitions: int,
    value: bytes,
) -> tuple[Any, ...]:
    return (
        "FLOW.VALUE.PUT",
        value,
        "OWNER_FLOW_ID",
        flow_id(run_id, index),
        "NAME",
        "bench_value",
        "PARTITION",
        partition_key(index // max(batch_size, 1), partitions),
        "NOW",
        now_ms(),
        "RETURN",
        "OK_ON_SUCCESS",
    )


def owned_value_put_payload_batch(
    *,
    run_id: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
    value: bytes,
) -> bytes | None:
    if count <= 0:
        return None

    timestamp = now_ms()
    parts = [
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | 14, count),
    ]
    value_fragment = compact_binary(value)
    name_fragment = compact_binary(b"bench_value")
    for offset in range(count):
        index = start + offset
        parts.append(value_fragment)
        parts.append(compact_binary(flow_id_bytes(run_id, index)))
        parts.append(name_fragment)
        parts.append(
            compact_optional_binary(partition_key(index // max(batch_size, 1), partitions).encode())
        )
        parts.append(_COMPACT_I64.pack(timestamp))
    return b"".join(parts)


def value_mget_command(refs: list[str]) -> tuple[Any, ...]:
    return ("FLOW.VALUE.MGET", *refs)


def flow_get_command(run_id: str, index: int, batch_size: int, partitions: int) -> tuple[Any, ...]:
    return (
        "FLOW.GET",
        flow_id(run_id, index),
        "PARTITION",
        partition_key(index // max(batch_size, 1), partitions),
    )


def flow_get_meta_command(
    run_id: str, index: int, batch_size: int, partitions: int
) -> tuple[Any, ...]:
    return (
        "FLOW.GET",
        flow_id(run_id, index),
        "PARTITION",
        partition_key(index // max(batch_size, 1), partitions),
        "RETURN",
        "META",
    )


def flow_get_payload_batch(
    *,
    run_id: str,
    start: int,
    count: int,
    item_count: int,
    batch_size: int,
    partitions: int,
    return_meta: bool,
) -> bytes | None:
    if count <= 0 or item_count <= 0:
        return None

    mode = 17 if return_meta else 16
    parts = [_COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | mode, count)]
    for offset in range(count):
        index = (start + offset) % item_count
        parts.append(compact_binary(flow_id_bytes(run_id, index)))
        parts.append(
            compact_optional_binary(partition_key(index // max(batch_size, 1), partitions).encode())
        )
    return b"".join(parts)


def flow_history_command(
    run_id: str,
    index: int,
    batch_size: int,
    partitions: int,
    *,
    consistent_projection: bool = True,
    include_cold: bool = False,
) -> tuple[Any, ...]:
    command: list[Any] = [
        "FLOW.HISTORY",
        flow_id(run_id, index),
        "COUNT",
        10,
        "PARTITION",
        partition_key(index // max(batch_size, 1), partitions),
    ]
    if not include_cold:
        command.extend(("INCLUDE_COLD", False))
    if not consistent_projection:
        command.extend(("CONSISTENT_PROJECTION", False))
    return tuple(command)


def flow_history_payload_batch(
    *,
    run_id: str,
    start: int,
    count: int,
    item_count: int,
    batch_size: int,
    partitions: int,
    history_count: int,
    include_cold: bool,
    consistent_projection: bool,
) -> bytes | None:
    if count <= 0 or item_count <= 0:
        return None

    parts = [
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | 10, count),
        struct.pack(
            ">qBB",
            history_count,
            _COMPACT_BOOL_TRUE if include_cold else _COMPACT_BOOL_FALSE,
            _COMPACT_BOOL_TRUE if consistent_projection else _COMPACT_BOOL_FALSE,
        ),
    ]
    for offset in range(count):
        index = (start + offset) % item_count
        parts.append(compact_binary(flow_id_bytes(run_id, index)))
        parts.append(
            compact_optional_binary(partition_key(index // max(batch_size, 1), partitions).encode())
        )
    return b"".join(parts)


def flow_list_command(flow_type: str, count: int, *, return_meta: bool = False) -> tuple[Any, ...]:
    command: list[Any] = ["FLOW.LIST", flow_type, "STATE", FLOW_STATE, "COUNT", count]
    if return_meta:
        command.extend(("RETURN", "META"))
    return tuple(command)


def warm_flow_history_projection(
    adapter: Any,
    args: argparse.Namespace,
    run_id: str,
) -> float:
    started = time.perf_counter()
    count = min(args.partitions, args.flows)
    setup_batch_size = effective_setup_batch_size(args)
    commands = [
        flow_history_command(
            run_id,
            min(partition * max(setup_batch_size, 1), args.flows - 1),
            setup_batch_size,
            args.partitions,
            consistent_projection=True,
            include_cold=args.flow_history_include_cold,
        )
        for partition in range(count)
    ]
    if commands:
        adapter.execute_batch(commands)
    return time.perf_counter() - started


def signal_command(run_id: str, index: int, batch_size: int, partitions: int) -> tuple[Any, ...]:
    return (
        "FLOW.SIGNAL",
        flow_id(run_id, index),
        "SIGNAL",
        "bench_signal",
        "PARTITION",
        partition_key(index // max(batch_size, 1), partitions),
        "IF_STATE",
        FLOW_STATE,
        "TRANSITION_TO",
        NEXT_STATE,
        "NOW",
        now_ms(),
    )


def signal_payload_batch(
    *,
    run_id: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
) -> bytes | None:
    if count <= 0:
        return None

    timestamp = now_ms()
    parts = [
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | 11, count),
        compact_binary(b"bench_signal"),
        compact_binary(FLOW_STATE.encode()),
        compact_binary(NEXT_STATE.encode()),
    ]
    for offset in range(count):
        index = start + offset
        parts.append(compact_binary(flow_id_bytes(run_id, index)))
        parts.append(
            compact_optional_binary(partition_key(index // max(batch_size, 1), partitions).encode())
        )
        parts.append(_COMPACT_I64.pack(timestamp))
    return b"".join(parts)


def start_and_claim_command(
    *,
    run_id: str,
    flow_type: str,
    index: int,
    batch_size: int,
    partitions: int,
    payload: bytes,
    job_only: bool = False,
) -> tuple[Any, ...]:
    command: list[Any] = [
        "FLOW.START_AND_CLAIM",
        flow_id(run_id, index),
        "TYPE",
        flow_type,
        "INITIAL_STATE",
        FLOW_STATE,
        "WORKER",
        "protocol-flow-bench",
        "LEASE_MS",
        30_000,
        "NOW",
        now_ms(),
        "PARTITION",
        partition_key(index // max(batch_size, 1), partitions),
        "PAYLOAD",
        payload,
    ]
    if job_only:
        command.extend(["RETURN", "JOBS_COMPACT"])
    return tuple(command)


def start_and_claim_payload_batch(
    *,
    run_id: str,
    flow_type: str,
    start: int,
    count: int,
    batch_size: int,
    partitions: int,
    payload: bytes,
    job_only: bool | None = None,
    include_record: bool | None = None,
) -> bytes | None:
    if count <= 0:
        return None

    if include_record is not None:
        job_only = not include_record
    job_only = bool(job_only)

    mode = 13 if job_only else 12
    timestamp = now_ms()
    parts = [
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | mode, count),
        compact_binary(flow_type.encode()),
        compact_binary(FLOW_STATE.encode()),
        compact_binary(b"protocol-flow-bench"),
        _COMPACT_I64.pack(30_000),
    ]
    payload_fragment = compact_optional_binary(payload)
    for offset in range(count):
        index = start + offset
        parts.append(compact_binary(flow_id_bytes(run_id, index)))
        parts.append(
            compact_optional_binary(partition_key(index // max(batch_size, 1), partitions).encode())
        )
        parts.append(payload_fragment)
        parts.append(_COMPACT_I64.pack(timestamp))
    return b"".join(parts)


def step_continue_command(record: Any) -> tuple[Any, ...]:
    if isinstance(record, (tuple, list)) and len(record) >= 4:
        flow_id_value, partition_key_value, lease_token_value, fencing_token_value = record[:4]
        return (
            "FLOW.STEP_CONTINUE",
            _as_text(flow_id_value),
            _as_bytes(lease_token_value),
            FLOW_STATE,
            NEXT_STATE,
            "FENCING",
            int(fencing_token_value),
            "LEASE_MS",
            30_000,
            "PARTITION",
            None if partition_key_value in (None, b"", "") else _as_text(partition_key_value),
            "NOW",
            now_ms(),
        )

    raw = {key.decode() if isinstance(key, bytes) else key: value for key, value in record.items()}
    return (
        "FLOW.STEP_CONTINUE",
        _as_text(raw["id"]),
        _as_bytes(raw["lease_token"]),
        FLOW_STATE,
        NEXT_STATE,
        "FENCING",
        int(raw["fencing_token"]),
        "LEASE_MS",
        30_000,
        "PARTITION",
        _as_text(raw["partition_key"]),
        "NOW",
        now_ms(),
    )


def step_continue_payload_batch(
    records: list[Any],
    *,
    start: int,
    count: int,
) -> bytes | None:
    if count <= 0:
        return None

    timestamp = now_ms()
    parts = [
        _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | 6, count),
        compact_binary(FLOW_STATE.encode()),
        compact_binary(NEXT_STATE.encode()),
        _COMPACT_I64.pack(30_000),
    ]
    for offset in range(count):
        record = records[start + offset]
        if isinstance(record, (tuple, list)) and len(record) >= 4:
            flow_id_value, partition_key_value, lease_token_value, fencing_token_value = record[:4]
            partition_value = (
                None if partition_key_value in (None, b"", "") else _as_bytes(partition_key_value)
            )
        else:
            raw = {
                key.decode() if isinstance(key, bytes) else key: value
                for key, value in record.items()
            }
            flow_id_value = raw["id"]
            partition_raw = raw["partition_key"]
            partition_value = None if partition_raw in (None, b"", "") else _as_bytes(partition_raw)
            lease_token_value = raw["lease_token"]
            fencing_token_value = raw["fencing_token"]

        parts.append(compact_binary(_as_bytes(flow_id_value)))
        parts.append(compact_optional_binary(partition_value))
        parts.append(compact_binary(_as_bytes(lease_token_value)))
        parts.append(_COMPACT_I64.pack(int(fencing_token_value)))
        parts.append(_COMPACT_I64.pack(timestamp))
    return b"".join(parts)


def setup_create(
    adapter: Any,
    *,
    run_id: str,
    flow_type: str,
    flows: int,
    batch_size: int,
    partitions: int,
    payload: bytes,
    retention_ttl_ms: int,
) -> float:
    started = time.perf_counter()
    for start in range(0, flows, batch_size):
        count = min(batch_size, flows - start)
        adapter.execute_command(
            *create_many_command(
                run_id=run_id,
                flow_type=flow_type,
                start=start,
                count=count,
                batch_size=batch_size,
                partitions=partitions,
                payload=payload,
                retention_ttl_ms=retention_ttl_ms,
            )
        )
    return time.perf_counter() - started


def setup_claim(
    adapter: Any,
    *,
    flow_type: str,
    flows: int,
    batch_size: int,
    partitions: int,
) -> tuple[list[ClaimedJob], float, int]:
    worker = f"protocol-flow-bench:{uuid.uuid4().hex[:8]}"
    jobs: list[ClaimedJob] = []
    empty_claims = 0
    started = time.perf_counter()
    while len(jobs) < flows:
        response = adapter.execute_command(
            *claim_due_command(
                flow_type=flow_type,
                worker=worker,
                partitions=partitions,
                batch_size=min(batch_size, flows - len(jobs)),
            )
        )
        claimed = normalize_jobs(response)
        if not claimed:
            empty_claims += 1
            if empty_claims > 100:
                break
            time.sleep(0.001)
            continue
        jobs.extend(claimed)
    return jobs, time.perf_counter() - started, empty_claims


def run_submit_command_batches(
    adapter: Any,
    *,
    total_items: int,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int, int], tuple[Any, ...]],
) -> tuple[int, int, list[float]]:
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []

    while issued < total_items or pending:
        while issued < total_items and len(pending) < inflight_batches:
            count = min(batch_size, total_items - issued)
            command = build(issued, count)
            pending.append((time.perf_counter(), adapter.submit_command(*command), count))
            issued += count

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return completed, errors, latencies


def run_submit_command_batches_for_duration(
    adapter: Any,
    *,
    duration_seconds: float,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int, int], tuple[Any, ...]],
    clock: Callable[[], float] | None = None,
) -> tuple[int, int, list[float]]:
    now = clock or time.perf_counter
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []
    deadline = now() + duration_seconds

    while now() < deadline or pending:
        while now() < deadline and len(pending) < inflight_batches:
            command = build(issued, batch_size)
            pending.append((now(), adapter.submit_command(*command), batch_size))
            issued += batch_size

        if not pending:
            continue

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((now() - started) * 1000.0)

    return completed, errors, latencies


def run_flow_list_reads(
    adapter: Any,
    args: argparse.Namespace,
    flow_type: str,
    *,
    clock: Callable[[], float] | None = None,
) -> tuple[int, int, list[float]]:
    def build(_start: int, count: int) -> tuple[Any, ...]:
        return flow_list_command(flow_type, count, return_meta=args.operation == "flow-list-meta")

    if args.read_duration > 0:
        return run_submit_command_batches_for_duration(
            adapter,
            duration_seconds=args.read_duration,
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            build=build,
            clock=clock,
        )
    return run_submit_command_batches(
        adapter,
        total_items=args.flows,
        batch_size=args.batch_size,
        inflight_batches=args.inflight_batches,
        build=build,
    )


def run_submit_value_mget_payload_batches(
    adapter: Any,
    *,
    total_items: int,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int, int], bytes | None],
) -> tuple[int, int, list[float]]:
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []

    while issued < total_items or pending:
        while issued < total_items and len(pending) < inflight_batches:
            count = min(batch_size, total_items - issued)
            payload = build(issued, count)
            if payload is None:
                issued += count
                continue
            pending.append(
                (time.perf_counter(), adapter.submit_flow_value_mget_payload(payload), count)
            )
            issued += count

        if not pending:
            continue

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return completed, errors, latencies


def run_submit_value_mget_payload_batches_for_duration(
    adapter: Any,
    *,
    duration_seconds: float,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int, int], bytes | None],
) -> tuple[int, int, list[float]]:
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []
    deadline = time.perf_counter() + duration_seconds

    while time.perf_counter() < deadline or pending:
        while time.perf_counter() < deadline and len(pending) < inflight_batches:
            payload = build(issued, batch_size)
            if payload is None:
                issued += batch_size
                continue
            pending.append(
                (time.perf_counter(), adapter.submit_flow_value_mget_payload(payload), batch_size)
            )
            issued += batch_size

        if not pending:
            continue

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return completed, errors, latencies


def run_create_many(
    adapter: Any,
    args: argparse.Namespace,
    *,
    run_id: str,
    flow_type: str,
    payload: bytes,
) -> tuple[int, int, list[float]]:
    if (
        getattr(args, "prebuild_payloads", True)
        and hasattr(adapter, "submit_flow_many_payload")
        and args.retention_ttl_ms == 0
    ):
        return run_submit_flow_payload_batches(
            adapter,
            total_items=args.flows,
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            command="FLOW.CREATE_MANY",
            build=lambda start, count: create_many_payload_batch(
                run_id=run_id,
                flow_type=flow_type,
                start=start,
                count=count,
                batch_size=args.batch_size,
                partitions=args.partitions,
                payload=payload,
                retention_ttl_ms=args.retention_ttl_ms,
            ),
        )

    return run_submit_command_batches(
        adapter,
        total_items=args.flows,
        batch_size=args.batch_size,
        inflight_batches=args.inflight_batches,
        build=lambda start, count: create_many_command(
            run_id=run_id,
            flow_type=flow_type,
            start=start,
            count=count,
            batch_size=args.batch_size,
            partitions=args.partitions,
            payload=payload,
            retention_ttl_ms=args.retention_ttl_ms,
        ),
    )


def run_submit_flow_payload_batches(
    adapter: Any,
    *,
    total_items: int,
    batch_size: int,
    inflight_batches: int,
    command: str,
    build: Callable[[int, int], bytes | None],
) -> tuple[int, int, list[float]]:
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []

    while issued < total_items or pending:
        while issued < total_items and len(pending) < inflight_batches:
            count = min(batch_size, total_items - issued)
            payload = build(issued, count)
            if payload is None:
                return completed, total_items - completed, latencies
            pending.append(
                (
                    time.perf_counter(),
                    adapter.submit_flow_many_payload(command, payload, count),
                    count,
                )
            )
            issued += count

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return completed, errors, latencies


def claimed_many_payload_batch(
    jobs: list[ClaimedJob],
    *,
    operation: str,
) -> bytes | None:
    if not jobs:
        return None

    timestamp = now_ms()
    if operation == "retry-many":
        header = bytes([_COMPACT_RETRY_MANY_OK_REQUEST]) + compact_optional_binary(None)
        header += struct.pack(">qqBI", timestamp, timestamp, _COMPACT_BOOL_TRUE, len(jobs))
    elif operation in {"complete-many", "fail-many"}:
        header = bytes([_COMPACT_COMPLETE_MANY_OK_REQUEST]) + compact_optional_binary(None)
        header += struct.pack(">qBI", timestamp, _COMPACT_BOOL_TRUE, len(jobs))
    else:
        return None

    parts = [header]
    for id_value, partition, lease, fencing in jobs:
        partition_bytes = (partition or "-").encode()
        parts.append(compact_binary(id_value.encode()))
        parts.append(compact_binary(partition_bytes))
        parts.append(compact_binary(lease))
        parts.append(_COMPACT_I64.pack(fencing))
    return b"".join(parts)


def run_claimed_many(
    adapter: Any,
    args: argparse.Namespace,
    jobs: list[ClaimedJob],
    *,
    operation: str,
) -> tuple[int, int, list[float]]:
    if getattr(args, "prebuild_payloads", True) and hasattr(adapter, "submit_flow_many_payload"):
        return run_submit_flow_payload_batches(
            adapter,
            total_items=len(jobs),
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            command=operation_to_flow_many_command(operation),
            build=lambda start, count: claimed_many_payload_batch(
                jobs[start : start + count], operation=operation
            ),
        )

    builders = {
        "complete-many": lambda batch: complete_many_command(batch),
        "retry-many": lambda batch: retry_many_command(batch),
        "fail-many": lambda batch: fail_many_command(batch),
    }
    build_many = builders[operation]
    return run_submit_command_batches(
        adapter,
        total_items=len(jobs),
        batch_size=args.batch_size,
        inflight_batches=args.inflight_batches,
        build=lambda start, count: build_many(jobs[start : start + count]),
    )


def operation_to_flow_many_command(operation: str) -> str:
    return {
        "complete-many": "FLOW.COMPLETE_MANY",
        "retry-many": "FLOW.RETRY_MANY",
        "fail-many": "FLOW.FAIL_MANY",
    }[operation]


def run_cancel_many_created(
    adapter: Any,
    args: argparse.Namespace,
    *,
    run_id: str,
) -> tuple[int, int, list[float]]:
    setup_batch_size = effective_setup_batch_size(args)
    return run_submit_command_batches(
        adapter,
        total_items=args.flows,
        batch_size=args.batch_size,
        inflight_batches=args.inflight_batches,
        build=lambda start, count: cancel_many_created_command(
            run_id=run_id,
            start=start,
            count=count,
            batch_size=setup_batch_size,
            partitions=args.partitions,
        ),
    )


def run_submit_pipeline_batches(
    adapter: Any,
    *,
    total_items: int,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int], tuple[Any, ...]],
) -> tuple[int, int, list[float]]:
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []

    while issued < total_items or pending:
        while issued < total_items and len(pending) < inflight_batches:
            count = min(batch_size, total_items - issued)
            commands = [build(issued + offset) for offset in range(count)]
            pending.append((time.perf_counter(), adapter.submit_batch(commands), count))
            issued += count

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return completed, errors, latencies


def run_submit_pipeline_batches_for_duration(
    adapter: Any,
    *,
    duration_seconds: float,
    batch_size: int,
    inflight_batches: int,
    item_count: int,
    build: Callable[[int], tuple[Any, ...]],
    clock: Callable[[], float] | None = None,
) -> tuple[int, int, list[float]]:
    if item_count <= 0:
        return 0, 0, []

    now = clock or time.perf_counter
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []
    deadline = now() + duration_seconds

    while now() < deadline or pending:
        while now() < deadline and len(pending) < inflight_batches:
            commands = [build((issued + offset) % item_count) for offset in range(batch_size)]
            pending.append((now(), adapter.submit_batch(commands), batch_size))
            issued += batch_size

        if not pending:
            continue

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((now() - started) * 1000.0)

    return completed, errors, latencies


def run_submit_pipeline_payload_batches(
    adapter: Any,
    *,
    total_items: int,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int, int], bytes | None],
) -> tuple[int, int, list[float]]:
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []

    while issued < total_items or pending:
        while issued < total_items and len(pending) < inflight_batches:
            count = min(batch_size, total_items - issued)
            payload = build(issued, count)
            if payload is None:
                issued += count
                continue
            pending.append(
                (time.perf_counter(), adapter.submit_pipeline_payload(payload, count), count)
            )
            issued += count

        if not pending:
            continue

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return completed, errors, latencies


def run_submit_pipeline_payload_batches_for_duration(
    adapter: Any,
    *,
    duration_seconds: float,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int, int], bytes | None],
) -> tuple[int, int, list[float]]:
    issued = 0
    completed = 0
    errors = 0
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []
    deadline = time.perf_counter() + duration_seconds

    while time.perf_counter() < deadline or pending:
        while time.perf_counter() < deadline and len(pending) < inflight_batches:
            payload = build(issued, batch_size)
            if payload is None:
                issued += batch_size
                continue
            pending.append(
                (
                    time.perf_counter(),
                    adapter.submit_pipeline_payload(payload, batch_size),
                    batch_size,
                )
            )
            issued += batch_size

        if not pending:
            continue

        started, future, count = pending.pop(0)
        try:
            future.result()
            completed += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return completed, errors, latencies


def run_submit_pipeline_batches_collect(
    adapter: Any,
    *,
    total_items: int,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int], tuple[Any, ...]],
) -> tuple[list[Any], int, list[float]]:
    issued = 0
    errors = 0
    results: list[Any] = []
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []

    while issued < total_items or pending:
        while issued < total_items and len(pending) < inflight_batches:
            count = min(batch_size, total_items - issued)
            commands = [build(issued + offset) for offset in range(count)]
            pending.append((time.perf_counter(), adapter.submit_batch(commands), count))
            issued += count

        started, future, count = pending.pop(0)
        try:
            batch_results = future.result()
            if isinstance(batch_results, list):
                results.extend(batch_results)
            else:
                errors += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return results, errors, latencies


def run_submit_pipeline_payload_batches_collect(
    adapter: Any,
    *,
    total_items: int,
    batch_size: int,
    inflight_batches: int,
    build: Callable[[int, int], bytes | None],
) -> tuple[list[Any], int, list[float]]:
    issued = 0
    errors = 0
    results: list[Any] = []
    pending: list[tuple[float, Any, int]] = []
    latencies: list[float] = []

    while issued < total_items or pending:
        while issued < total_items and len(pending) < inflight_batches:
            count = min(batch_size, total_items - issued)
            payload = build(issued, count)
            if payload is None:
                issued += count
                errors += count
                continue
            pending.append(
                (time.perf_counter(), adapter.submit_pipeline_payload(payload, count), count)
            )
            issued += count

        if not pending:
            continue

        started, future, count = pending.pop(0)
        try:
            batch_results = future.result()
            if isinstance(batch_results, list):
                results.extend(batch_results)
            else:
                errors += count
        except Exception:
            errors += count
        latencies.append((time.perf_counter() - started) * 1000.0)

    return results, errors, latencies


def setup_value_refs(
    adapter: Any, args: argparse.Namespace, value: bytes
) -> tuple[list[str], int, list[float]]:
    raw_refs, errors, latencies = run_submit_pipeline_batches_collect(
        adapter,
        total_items=args.flows,
        batch_size=args.batch_size,
        inflight_batches=args.inflight_batches,
        build=lambda index: value_put_command(index, value),
    )
    return [_as_text(ref) for ref in raw_refs], errors, latencies


def cyclic_refs(refs: list[str], start: int, count: int) -> list[str]:
    size = len(refs)
    if size == 0:
        return []

    offset = start % size
    end = offset + count
    if end <= size:
        return refs[offset:end]

    wrapped = end % size
    return refs[offset:] + refs[:wrapped]


def run_claim_due(
    adapter: Any, args: argparse.Namespace, flow_type: str
) -> tuple[int, int, list[float]]:
    claimed = 0
    errors = 0
    empty = 0
    latencies: list[float] = []
    pending: list[tuple[float, Any, int]] = []
    pending_limit = 0
    worker = f"protocol-flow-bench:{uuid.uuid4().hex[:8]}"

    while claimed + errors < args.flows or pending:
        while (
            claimed + errors + pending_limit < args.flows and len(pending) < args.inflight_batches
        ):
            limit = min(args.batch_size, args.flows - claimed - errors - pending_limit)
            pending.append(
                (
                    time.perf_counter(),
                    adapter.submit_command(
                        *claim_due_command(
                            flow_type=flow_type,
                            worker=worker,
                            partitions=args.partitions,
                            batch_size=limit,
                        )
                    ),
                    limit,
                )
            )
            pending_limit += limit

        if not pending:
            break

        started, future, requested = pending.pop(0)
        pending_limit -= requested

        try:
            response = future.result()
            jobs = normalize_jobs(response)
            if not jobs:
                empty += 1
                if empty > 100:
                    break
                time.sleep(0.001)
                continue
            claimed += len(jobs)
            empty = 0
        except Exception:
            errors += requested
        finally:
            latencies.append((time.perf_counter() - started) * 1000.0)

    return claimed, errors, latencies


def run_claim_due_serial(
    adapter: Any, args: argparse.Namespace, flow_type: str
) -> tuple[int, int, list[float]]:
    claimed = 0
    errors = 0
    empty = 0
    latencies: list[float] = []
    worker = f"protocol-flow-bench:{uuid.uuid4().hex[:8]}"
    while claimed + errors < args.flows:
        started = time.perf_counter()
        try:
            response = adapter.execute_command(
                *claim_due_command(
                    flow_type=flow_type,
                    worker=worker,
                    partitions=args.partitions,
                    batch_size=min(args.batch_size, args.flows - claimed),
                )
            )
            jobs = normalize_jobs(response)
            if not jobs:
                empty += 1
                if empty > 100:
                    break
                time.sleep(0.001)
                continue
            claimed += len(jobs)
        except Exception:
            errors += args.batch_size
        finally:
            latencies.append((time.perf_counter() - started) * 1000.0)
    return claimed, errors, latencies


def run_value_mget(
    adapter: Any, args: argparse.Namespace, value: bytes
) -> tuple[int, int, list[float], float]:
    setup_started = time.perf_counter()
    refs, setup_errors, _setup_latencies = setup_value_refs(adapter, args, value)
    setup_seconds = time.perf_counter() - setup_started
    if setup_errors or len(refs) != args.flows:
        raise RuntimeError(
            f"value-mget setup wrote {len(refs)} / {args.flows} refs with {setup_errors} errors"
        )

    if getattr(args, "prebuild_payloads", True) and hasattr(
        adapter, "submit_flow_value_mget_payload"
    ):
        if args.read_duration > 0:
            completed, errors, latencies = run_submit_value_mget_payload_batches_for_duration(
                adapter,
                duration_seconds=args.read_duration,
                batch_size=args.batch_size,
                inflight_batches=args.inflight_batches,
                build=lambda start, count: value_mget_payload_batch(
                    cyclic_refs(refs, start, count)
                ),
            )
        else:
            completed, errors, latencies = run_submit_value_mget_payload_batches(
                adapter,
                total_items=len(refs),
                batch_size=args.batch_size,
                inflight_batches=args.inflight_batches,
                build=lambda start, count: value_mget_payload_batch(refs[start : start + count]),
            )
    elif args.read_duration > 0:
        completed, errors, latencies = run_submit_command_batches_for_duration(
            adapter,
            duration_seconds=args.read_duration,
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            build=lambda start, count: value_mget_command(cyclic_refs(refs, start, count)),
        )
    else:
        completed, errors, latencies = run_submit_command_batches(
            adapter,
            total_items=len(refs),
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            build=lambda start, count: value_mget_command(refs[start : start + count]),
        )
    return completed, errors, latencies, setup_seconds


def run_value_put(
    adapter: Any,
    args: argparse.Namespace,
    value: bytes,
    builder: Callable[[int, bytes], tuple[Any, ...]],
    *,
    clock: Callable[[], float] | None = None,
) -> tuple[int, int, list[float]]:
    if getattr(args, "prebuild_payloads", True) and hasattr(adapter, "submit_pipeline_payload"):
        return_ok = builder is value_put_ok_command

        def build_payload(_start: int, count: int) -> bytes:
            return shared_value_put_payload_batch(
                value=value,
                count=count,
                return_ok=return_ok,
            )

        if args.read_duration > 0:
            return run_submit_pipeline_payload_batches_for_duration(
                adapter,
                duration_seconds=args.read_duration,
                batch_size=args.batch_size,
                inflight_batches=args.inflight_batches,
                build=build_payload,
            )

        return run_submit_pipeline_payload_batches(
            adapter,
            total_items=args.flows,
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            build=build_payload,
        )

    if args.read_duration > 0:
        return run_submit_pipeline_batches_for_duration(
            adapter,
            duration_seconds=args.read_duration,
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            item_count=args.flows,
            build=lambda index: builder(index, value),
            clock=clock,
        )

    return run_submit_pipeline_batches(
        adapter,
        total_items=args.flows,
        batch_size=args.batch_size,
        inflight_batches=args.inflight_batches,
        build=lambda index: builder(index, value),
    )


def run_owned_value_put(
    adapter: Any,
    args: argparse.Namespace,
    *,
    run_id: str,
    payload: bytes,
) -> tuple[int, int, list[float]]:
    setup_batch_size = effective_setup_batch_size(args)
    if getattr(args, "prebuild_payloads", True) and hasattr(adapter, "submit_pipeline_payload"):
        return run_submit_pipeline_payload_batches(
            adapter,
            total_items=args.flows,
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            build=lambda start, count: owned_value_put_payload_batch(
                run_id=run_id,
                start=start,
                count=count,
                batch_size=setup_batch_size,
                partitions=args.partitions,
                value=payload,
            ),
        )

    return run_submit_pipeline_batches(
        adapter,
        total_items=args.flows,
        batch_size=args.batch_size,
        inflight_batches=args.inflight_batches,
        build=lambda index: owned_value_put_command(
            run_id=run_id,
            index=index,
            batch_size=setup_batch_size,
            partitions=args.partitions,
            value=payload,
        ),
    )


def run_step(
    adapter: Any, args: argparse.Namespace, run_id: str, flow_type: str, payload: bytes
) -> tuple[int, int, list[float], float]:
    setup_started = time.perf_counter()
    setup_batch_size = effective_setup_batch_size(args)
    started_records, setup_errors, _setup_latencies = run_submit_pipeline_batches_collect(
        adapter,
        total_items=args.flows,
        batch_size=setup_batch_size,
        inflight_batches=args.inflight_batches,
        build=lambda index: start_and_claim_command(
            run_id=f"{run_id}:step-record",
            flow_type=flow_type,
            index=index,
            batch_size=setup_batch_size,
            partitions=args.partitions,
            payload=payload,
            job_only=True,
        ),
    )
    setup_seconds = time.perf_counter() - setup_started
    if setup_errors or len(started_records) != args.flows:
        raise RuntimeError(
            "step setup started "
            f"{len(started_records)} / {args.flows} records with {setup_errors} errors"
        )

    if getattr(args, "prebuild_payloads", True) and hasattr(adapter, "submit_pipeline_payload"):
        completed, errors, latencies = run_submit_pipeline_payload_batches(
            adapter,
            total_items=len(started_records),
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            build=lambda start, count: step_continue_payload_batch(
                started_records,
                start=start,
                count=count,
            ),
        )
    else:
        completed, errors, latencies = run_submit_pipeline_batches(
            adapter,
            total_items=len(started_records),
            batch_size=args.batch_size,
            inflight_batches=args.inflight_batches,
            build=lambda index: step_continue_command(started_records[index]),
        )
    return completed, errors, latencies, setup_seconds


def normalize_jobs(response: Any) -> list[ClaimedJob]:
    jobs: list[ClaimedJob] = []
    for item in response or []:
        jobs.append(
            (
                _as_text(item[0]),
                None if item[1] in (None, b"", "") else _as_text(item[1]),
                _as_bytes(item[2]),
                int(item[3]),
            )
        )
    return jobs


def _as_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _as_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    scheme = urlparse(args.url).scheme.lower()
    if scheme not in PROTOCOL_SCHEMES:
        raise ValueError("protocol Flow benchmark requires ferric:// or ferrics:// URL")

    total_started = time.perf_counter()
    cpu_started = time.process_time()
    run_id = args.run_id or f"protocol-flow-{uuid.uuid4().hex[:10]}"
    flow_type = f"{args.flow_type_prefix}_{run_id}"
    payload = payload_bytes(args.payload_bytes)
    setup_batch_size = effective_setup_batch_size(args)
    adapter = ProtocolAdapterPool.from_url(
        args.url,
        max_connections=args.connections,
        lanes=args.protocol_lanes,
        timeout=args.timeout,
        client_name="ferricstore-protocol-flow-bench",
    )
    setup_seconds = 0.0
    setup_claim_seconds = 0.0
    setup_empty_claims = 0

    try:
        if args.operation == "create-many":
            started = time.perf_counter()
            completed, errors, latencies = run_create_many(
                adapter,
                args,
                run_id=run_id,
                flow_type=flow_type,
                payload=payload,
            )
            measured_seconds = time.perf_counter() - started
        elif args.operation in {"value-put", "value-put-ok"}:
            builder = (
                value_put_ok_command if args.operation == "value-put-ok" else value_put_command
            )
            started = time.perf_counter()
            completed, errors, latencies = run_value_put(adapter, args, payload, builder)
            measured_seconds = time.perf_counter() - started
        elif args.operation == "value-put-owned":
            setup_seconds = setup_create(
                adapter,
                run_id=run_id,
                flow_type=flow_type,
                flows=args.flows,
                batch_size=setup_batch_size,
                partitions=args.partitions,
                payload=payload,
                retention_ttl_ms=args.retention_ttl_ms,
            )
            started = time.perf_counter()
            completed, errors, latencies = run_owned_value_put(
                adapter,
                args,
                run_id=run_id,
                payload=payload,
            )
            measured_seconds = time.perf_counter() - started
        elif args.operation == "value-mget":
            started = time.perf_counter()
            completed, errors, latencies, setup_seconds = run_value_mget(adapter, args, payload)
            measured_seconds = time.perf_counter() - started - setup_seconds
        elif args.operation == "start-and-claim":
            started = time.perf_counter()
            if getattr(args, "prebuild_payloads", True) and hasattr(
                adapter, "submit_pipeline_payload"
            ):
                started_records, errors, latencies = run_submit_pipeline_payload_batches_collect(
                    adapter,
                    total_items=args.flows,
                    batch_size=args.batch_size,
                    inflight_batches=args.inflight_batches,
                    build=lambda start, count: start_and_claim_payload_batch(
                        run_id=run_id,
                        flow_type=flow_type,
                        start=start,
                        count=count,
                        batch_size=args.batch_size,
                        partitions=args.partitions,
                        payload=payload,
                        job_only=True,
                    ),
                )
            else:
                started_records, errors, latencies = run_submit_pipeline_batches_collect(
                    adapter,
                    total_items=args.flows,
                    batch_size=args.batch_size,
                    inflight_batches=args.inflight_batches,
                    build=lambda index: start_and_claim_command(
                        run_id=run_id,
                        flow_type=flow_type,
                        index=index,
                        batch_size=args.batch_size,
                        partitions=args.partitions,
                        payload=payload,
                        job_only=True,
                    ),
                )
            completed = len(started_records)
            measured_seconds = time.perf_counter() - started
        elif args.operation == "step":
            started = time.perf_counter()
            completed, errors, latencies, setup_seconds = run_step(
                adapter, args, run_id, flow_type, payload
            )
            measured_seconds = time.perf_counter() - started - setup_seconds
        elif args.operation in {
            "flow-get",
            "flow-get-meta",
            "flow-history",
            "flow-list",
            "flow-list-meta",
        }:
            setup_seconds = setup_create(
                adapter,
                run_id=run_id,
                flow_type=flow_type,
                flows=args.flows,
                batch_size=setup_batch_size,
                partitions=args.partitions,
                payload=payload,
                retention_ttl_ms=args.retention_ttl_ms,
            )
            if (
                args.operation == "flow-history"
                and args.flow_read_consistency == "eventual"
                and args.flow_history_include_cold
            ):
                setup_seconds += warm_flow_history_projection(adapter, args, run_id)
            started = time.perf_counter()
            if args.operation in {"flow-list", "flow-list-meta"}:
                completed, errors, latencies = run_flow_list_reads(adapter, args, flow_type)
            elif args.operation in {"flow-get", "flow-get-meta"}:
                build_flow_get = (
                    flow_get_meta_command if args.operation == "flow-get-meta" else flow_get_command
                )
                if getattr(args, "prebuild_payloads", True) and hasattr(
                    adapter, "submit_pipeline_payload"
                ):

                    def build_payload(start: int, count: int) -> bytes:
                        return flow_get_payload_batch(
                            run_id=run_id,
                            start=start,
                            count=count,
                            item_count=args.flows,
                            batch_size=setup_batch_size,
                            partitions=args.partitions,
                            return_meta=args.operation == "flow-get-meta",
                        )

                    if args.read_duration > 0:
                        completed, errors, latencies = (
                            run_submit_pipeline_payload_batches_for_duration(
                                adapter,
                                duration_seconds=args.read_duration,
                                batch_size=args.batch_size,
                                inflight_batches=args.inflight_batches,
                                build=build_payload,
                            )
                        )
                    else:
                        completed, errors, latencies = run_submit_pipeline_payload_batches(
                            adapter,
                            total_items=args.flows,
                            batch_size=args.batch_size,
                            inflight_batches=args.inflight_batches,
                            build=build_payload,
                        )
                elif args.read_duration > 0:
                    completed, errors, latencies = run_submit_pipeline_batches_for_duration(
                        adapter,
                        duration_seconds=args.read_duration,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        item_count=args.flows,
                        build=lambda index: build_flow_get(
                            run_id, index, setup_batch_size, args.partitions
                        ),
                    )
                else:
                    completed, errors, latencies = run_submit_pipeline_batches(
                        adapter,
                        total_items=args.flows,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        build=lambda index: build_flow_get(
                            run_id, index, setup_batch_size, args.partitions
                        ),
                    )
            else:

                def build_flow_history(index: int) -> tuple[Any, ...]:
                    return flow_history_command(
                        run_id,
                        index,
                        setup_batch_size,
                        args.partitions,
                        consistent_projection=args.flow_read_consistency == "consistent",
                        include_cold=args.flow_history_include_cold,
                    )

                if getattr(args, "prebuild_payloads", True) and hasattr(
                    adapter, "submit_pipeline_payload"
                ):

                    def build_history_payload(start: int, count: int) -> bytes:
                        return flow_history_payload_batch(
                            run_id=run_id,
                            start=start,
                            count=count,
                            item_count=args.flows,
                            batch_size=setup_batch_size,
                            partitions=args.partitions,
                            history_count=10,
                            include_cold=args.flow_history_include_cold,
                            consistent_projection=args.flow_read_consistency == "consistent",
                        )

                    if args.read_duration > 0:
                        completed, errors, latencies = (
                            run_submit_pipeline_payload_batches_for_duration(
                                adapter,
                                duration_seconds=args.read_duration,
                                batch_size=args.batch_size,
                                inflight_batches=args.inflight_batches,
                                build=build_history_payload,
                            )
                        )
                    else:
                        completed, errors, latencies = run_submit_pipeline_payload_batches(
                            adapter,
                            total_items=args.flows,
                            batch_size=args.batch_size,
                            inflight_batches=args.inflight_batches,
                            build=build_history_payload,
                        )
                elif args.read_duration > 0:
                    completed, errors, latencies = run_submit_pipeline_batches_for_duration(
                        adapter,
                        duration_seconds=args.read_duration,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        item_count=args.flows,
                        build=build_flow_history,
                    )
                else:
                    completed, errors, latencies = run_submit_pipeline_batches(
                        adapter,
                        total_items=args.flows,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        build=build_flow_history,
                    )
            measured_seconds = time.perf_counter() - started
        else:
            setup_seconds = setup_create(
                adapter,
                run_id=run_id,
                flow_type=flow_type,
                flows=args.flows,
                batch_size=setup_batch_size,
                partitions=args.partitions,
                payload=payload,
                retention_ttl_ms=args.retention_ttl_ms,
            )

            if args.operation == "signal":
                started = time.perf_counter()
                if getattr(args, "prebuild_payloads", True) and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    completed, errors, latencies = run_submit_pipeline_payload_batches(
                        adapter,
                        total_items=args.flows,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        build=lambda start, count: signal_payload_batch(
                            run_id=run_id,
                            start=start,
                            count=count,
                            batch_size=setup_batch_size,
                            partitions=args.partitions,
                        ),
                    )
                else:
                    completed, errors, latencies = run_submit_pipeline_batches(
                        adapter,
                        total_items=args.flows,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        build=lambda index: signal_command(
                            run_id, index, setup_batch_size, args.partitions
                        ),
                    )
                measured_seconds = time.perf_counter() - started
            elif args.operation == "transition-many":
                started = time.perf_counter()
                if getattr(args, "prebuild_payloads", True) and hasattr(
                    adapter, "submit_flow_many_payload"
                ):
                    completed, errors, latencies = run_submit_flow_payload_batches(
                        adapter,
                        total_items=args.flows,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        command="FLOW.TRANSITION_MANY",
                        build=lambda start, count: transition_many_payload_batch(
                            run_id=run_id,
                            start=start,
                            count=count,
                            batch_size=setup_batch_size,
                            partitions=args.partitions,
                        ),
                    )
                else:
                    completed, errors, latencies = run_submit_command_batches(
                        adapter,
                        total_items=args.flows,
                        batch_size=args.batch_size,
                        inflight_batches=args.inflight_batches,
                        build=lambda start, count: transition_many_command(
                            run_id=run_id,
                            start=start,
                            count=count,
                            batch_size=setup_batch_size,
                            partitions=args.partitions,
                        ),
                    )
                measured_seconds = time.perf_counter() - started
            elif args.operation == "cancel-many":
                started = time.perf_counter()
                completed, errors, latencies = run_cancel_many_created(adapter, args, run_id=run_id)
                measured_seconds = time.perf_counter() - started
            elif args.operation == "claim-due":
                started = time.perf_counter()
                if args.claim_mode == "serial":
                    completed, errors, latencies = run_claim_due_serial(adapter, args, flow_type)
                else:
                    completed, errors, latencies = run_claim_due(adapter, args, flow_type)
                measured_seconds = time.perf_counter() - started
            else:
                jobs, setup_claim_seconds, setup_empty_claims = setup_claim(
                    adapter,
                    flow_type=flow_type,
                    flows=args.flows,
                    batch_size=args.batch_size,
                    partitions=args.partitions,
                )
                if len(jobs) < args.flows:
                    raise RuntimeError(f"setup claim got only {len(jobs)} / {args.flows} jobs")

                started = time.perf_counter()
                completed, errors, latencies = run_claimed_many(
                    adapter, args, jobs, operation=args.operation
                )
                measured_seconds = time.perf_counter() - started
    finally:
        adapter.close()

    measured_seconds = max(measured_seconds, 1e-9)
    total_seconds = max(time.perf_counter() - total_started, 1e-9)
    client_cpu_seconds = max(time.process_time() - cpu_started, 0.0)
    return {
        "benchmark": "protocol_flow_commands",
        "operation": args.operation,
        "url": args.url,
        "run_id": run_id,
        "flows": args.flows,
        "completed": completed,
        "errors": errors,
        "seconds": measured_seconds,
        "total_seconds": total_seconds,
        "client_cpu_seconds": client_cpu_seconds,
        "client_cpu_percent": (client_cpu_seconds / total_seconds) * 100.0,
        "items_per_sec": completed / measured_seconds,
        "batch_size": args.batch_size,
        "setup_batch_size": setup_batch_size,
        "read_duration": args.read_duration,
        "inflight_batches": args.inflight_batches,
        "connections": args.connections,
        "protocol_lanes": args.protocol_lanes,
        "flow_read_consistency": args.flow_read_consistency,
        "flow_history_include_cold": args.flow_history_include_cold,
        "claim_mode": args.claim_mode,
        "prebuild_payloads": args.prebuild_payloads,
        "partitions": args.partitions,
        "payload_bytes": args.payload_bytes,
        "retention_ttl_ms": args.retention_ttl_ms,
        "setup_seconds": setup_seconds,
        "setup_claim_seconds": setup_claim_seconds,
        "setup_empty_claims": setup_empty_claims,
        "batch_latency_samples": len(latencies),
        "batch_latency_avg_ms": sum(latencies) / len(latencies) if latencies else 0.0,
        "batch_latency_p50_ms": percentile(latencies, 50),
        "batch_latency_p95_ms": percentile(latencies, 95),
        "batch_latency_p99_ms": percentile(latencies, 99),
        "batch_latency_max_ms": max(latencies) if latencies else 0.0,
    }


def effective_setup_batch_size(args: argparse.Namespace) -> int:
    return args.setup_batch_size or max(args.batch_size, 500)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FerricStore ferric:// Flow command benchmark for protocol command coverage"
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument(
        "--operation",
        choices=(
            "create-many",
            "claim-due",
            "complete-many",
            "transition-many",
            "retry-many",
            "fail-many",
            "cancel-many",
            "value-put",
            "value-put-ok",
            "value-put-owned",
            "value-mget",
            "start-and-claim",
            "flow-get",
            "flow-get-meta",
            "flow-history",
            "flow-list",
            "flow-list-meta",
            "signal",
            "step",
        ),
        default="create-many",
    )
    parser.add_argument("--flows", type=int, default=100_000)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--setup-batch-size", type=int, default=None)
    parser.add_argument("--inflight-batches", type=int, default=64)
    parser.add_argument("--connections", type=int, default=1)
    parser.add_argument("--protocol-lanes", type=int, default=32)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--payload-bytes", type=int, default=16)
    parser.add_argument("--retention-ttl-ms", type=int, default=0)
    parser.add_argument(
        "--flow-read-consistency",
        choices=("eventual", "consistent"),
        default="eventual",
        help=(
            "Flow history benchmark mode: eventual warms projection once before timing; "
            "consistent includes per-read projection consistency cost."
        ),
    )
    parser.add_argument(
        "--flow-history-include-cold",
        action="store_true",
        help=(
            "Include LMDB/cold history in FLOW.HISTORY benchmark. "
            "Default is hot recent history only."
        ),
    )
    parser.add_argument(
        "--claim-mode",
        choices=("multiplexed", "serial"),
        default="multiplexed",
        help=(
            "FLOW.CLAIM_DUE benchmark mode. Multiplexed uses native lanes; "
            "serial matches a blocking pull loop."
        ),
    )
    parser.add_argument("--flow-type-prefix", default="protocol_flow_bench")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument(
        "--read-duration",
        type=float,
        default=0.0,
        help=(
            "For duration benchmarks such as value-mget, value-put, and Flow reads, "
            "prepare/reuse --flows records where needed and measure for this many seconds."
        ),
    )
    parser.add_argument("--prebuild-payloads", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)

    if args.flows <= 0:
        parser.error("--flows must be positive")
    if args.batch_size is None:
        args.batch_size = {
            "step": 100,
            "start-and-claim": 250,
            "flow-get": 250,
            "flow-get-meta": 250,
            "flow-history": 250,
            "flow-list": 250,
            "flow-list-meta": 250,
            "value-put": 100,
            "value-put-ok": 500,
            "value-put-owned": 100,
        }.get(args.operation, 500)
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.setup_batch_size is not None and args.setup_batch_size <= 0:
        parser.error("--setup-batch-size must be positive")
    if args.inflight_batches <= 0:
        parser.error("--inflight-batches must be positive")
    if args.connections <= 0:
        parser.error("--connections must be positive")
    if args.protocol_lanes <= 0:
        parser.error("--protocol-lanes must be positive")
    if args.partitions <= 0:
        parser.error("--partitions must be positive")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
    if args.retention_ttl_ms < 0:
        parser.error("--retention-ttl-ms must be non-negative")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.read_duration < 0:
        parser.error("--read-duration must be non-negative")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    result = run_benchmark(args)
    print(json.dumps(result, sort_keys=True, indent=2 if args.pretty else None))


if __name__ == "__main__":
    main(sys.argv[1:])
