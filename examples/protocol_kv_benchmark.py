import argparse
import json
import math
import struct
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlparse

from ferricstore.protocol import ProtocolAdapter

DEFAULT_URL = "ferric://127.0.0.1:6388"
PROTOCOL_SCHEMES = {"ferric", "ferrics"}
DEFAULT_KEY_PREFIX = "protocol-kv"
LARGE_RESPONSE_VALUES_PER_BATCH = 10_000
_COMPACT_PIPELINE_REQUEST = 0x94
_COMPACT_PIPELINE_HEADER = struct.Struct(">BBI")
_COMPACT_U32 = struct.Struct(">I")
_COMPACT_I64 = struct.Struct(">q")
_COMPACT_F64 = struct.Struct(">d")
_COMPACT_HGET_PIPELINE_MODE = 18
_COMPACT_SISMEMBER_PIPELINE_MODE = 19
_COMPACT_LRANGE_PIPELINE_MODE = 20
_COMPACT_ZRANGE_PIPELINE_MODE = 21
_COMPACT_HSET_PIPELINE_MODE = 22
_COMPACT_LPUSH_PIPELINE_MODE = 23
_COMPACT_RPUSH_PIPELINE_MODE = 24
_COMPACT_SADD_PIPELINE_MODE = 25
_COMPACT_ZADD_PIPELINE_MODE = 26
_COMPACT_SMEMBERS_PIPELINE_MODE = 27
_COMPACT_HMGET_PIPELINE_MODE = 28
_COMPACT_ZSCORE_PIPELINE_MODE = 29
_COMPACT_HGETALL_PIPELINE_MODE = 30
_COMPACT_SREM_PIPELINE_MODE = 31
_COMPACT_ZREM_PIPELINE_MODE = 32
PRESETS = {
    "get-latency": {
        "command": "get",
        "request_mode": "pipeline",
        "pipeline": 10,
        "clients": 1,
        "threads": 1,
        "inflight_batches": 64,
        "protocol_lanes": 1,
        "test_time": 30.0,
    },
    "get-throughput": {
        "command": "get",
        "request_mode": "many",
        "pipeline": 1000,
        "clients": 1,
        "threads": 1,
        "inflight_batches": 64,
        "protocol_lanes": 64,
        "test_time": 30.0,
        "prebuild_keys": True,
    },
    "get-balanced": {
        "command": "get",
        "request_mode": "many",
        "pipeline": 500,
        "clients": 1,
        "threads": 1,
        "inflight_batches": 64,
        "protocol_lanes": 64,
        "test_time": 30.0,
        "prebuild_keys": True,
    },
    "get-low-latency": {
        "command": "get",
        "request_mode": "many",
        "pipeline": 100,
        "clients": 1,
        "threads": 1,
        "inflight_batches": 64,
        "protocol_lanes": 64,
        "test_time": 30.0,
        "prebuild_keys": True,
    },
    "set-throughput": {
        "command": "set",
        "request_mode": "many",
        "pipeline": 500,
        "clients": 1,
        "threads": 1,
        "inflight_batches": 64,
        "protocol_lanes": 64,
        "test_time": 30.0,
        "prebuild_keys": True,
    },
    "set-latency": {
        "command": "set",
        "request_mode": "many",
        "pipeline": 100,
        "clients": 1,
        "threads": 1,
        "inflight_batches": 64,
        "protocol_lanes": 64,
        "test_time": 30.0,
        "prebuild_keys": True,
    },
    "mixed-throughput": {
        "command": "mixed",
        "request_mode": "pipeline",
        "pipeline": 500,
        "clients": 1,
        "threads": 1,
        "inflight_batches": 64,
        "protocol_lanes": 64,
        "test_time": 30.0,
    },
}

DATA_STRUCTURE_COMMANDS = {
    "hset",
    "hget",
    "hmget",
    "hgetall",
    "lpush",
    "rpush",
    "lrange",
    "lpop",
    "rpop",
    "sadd",
    "srem",
    "smembers",
    "sismember",
    "zadd",
    "zrem",
    "zrange",
    "zscore",
}
READ_WARMUP_COMMANDS = {
    "get",
    "hget",
    "hmget",
    "hgetall",
    "lrange",
    "lpop",
    "rpop",
    "smembers",
    "sismember",
    "srem",
    "zrange",
    "zscore",
    "zrem",
}


def default_key_prefix(command: str) -> str:
    if command in DATA_STRUCTURE_COMMANDS:
        return f"{DEFAULT_KEY_PREFIX}:{command}"
    return DEFAULT_KEY_PREFIX


def make_value(size: int) -> bytes:
    return b"x" * max(size, 0)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def build_command(
    command: str,
    key_prefix: str,
    sequence: int,
    key_count: int,
    value: bytes,
    read_percent: int,
    binary_keys: bool = False,
    range_start: int = 0,
    range_stop: int = 0,
) -> tuple[Any, ...]:
    key = benchmark_key(key_prefix, sequence, key_count, binary_keys)
    if command == "set":
        return ("SET", key, value)
    if command == "get":
        return ("GET", key)
    if command == "hset":
        return ("HSET", key, "field", value)
    if command == "hget":
        return ("HGET", key, "field")
    if command == "hmget":
        return ("HMGET", key, "field")
    if command == "hgetall":
        return ("HGETALL", key)
    if command == "lpush":
        return ("LPUSH", key, value)
    if command == "rpush":
        return ("RPUSH", key, value)
    if command == "lrange":
        return ("LRANGE", key, range_start, range_stop)
    if command == "lpop":
        return ("LPOP", key)
    if command == "rpop":
        return ("RPOP", key)
    if command == "sadd":
        return ("SADD", key, value)
    if command == "srem":
        return ("SREM", key, value)
    if command == "smembers":
        return ("SMEMBERS", key)
    if command == "sismember":
        return ("SISMEMBER", key, value)
    if command == "zadd":
        return ("ZADD", key, float(sequence % key_count), value)
    if command == "zrem":
        return ("ZREM", key, value)
    if command == "zrange":
        return ("ZRANGE", key, range_start, range_stop)
    if command == "zscore":
        return ("ZSCORE", key, value)
    if command != "mixed":
        raise ValueError(f"unsupported command: {command}")

    if read_percent <= 0:
        return ("SET", key, value)
    if read_percent >= 100:
        return ("GET", key)
    if read_percent == 50:
        return ("GET", key) if sequence % 2 == 0 else ("SET", key, value)
    return ("GET", key) if sequence % 100 < read_percent else ("SET", key, value)


def benchmark_key(key_prefix: str, sequence: int, key_count: int, binary_keys: bool) -> str | bytes:
    key = f"{key_prefix}:{sequence % key_count}"
    return key.encode() if binary_keys else key


def build_key_pool(key_prefix: str, key_count: int, binary_keys: bool) -> tuple[str | bytes, ...]:
    return tuple(
        benchmark_key(key_prefix, sequence, key_count, binary_keys) for sequence in range(key_count)
    )


def _wire_key_fragment(key: str | bytes) -> bytes:
    if isinstance(key, str):
        key = key.encode()
    return _COMPACT_U32.pack(len(key)) + key


def _wire_value_fragment(value: bytes) -> bytes:
    return _COMPACT_U32.pack(len(value)) + value


def build_wire_key_pool(key_prefix: str, key_count: int, binary_keys: bool) -> tuple[bytes, ...]:
    return tuple(
        _wire_key_fragment(benchmark_key(key_prefix, sequence, key_count, binary_keys))
        for sequence in range(key_count)
    )


def key_batch(
    key_pool: tuple[str | bytes, ...], sequence: int, count: int
) -> tuple[str | bytes, ...]:
    size = len(key_pool)
    if count <= 0 or size == 0:
        return ()
    start = sequence % size
    end = start + count
    if end <= size:
        return key_pool[start:end]

    keys: list[str | bytes] = []
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        keys.extend(key_pool[cursor : cursor + take])
        remaining -= take
        cursor = 0
    return tuple(keys)


def wire_key_payload_batch(wire_key_pool: tuple[bytes, ...], sequence: int, count: int) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 2, count)
    start = sequence % size
    end = start + count
    if end <= size:
        return header + b"".join(wire_key_pool[start:end])

    parts = [header]
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        parts.extend(wire_key_pool[cursor : cursor + take])
        remaining -= take
        cursor = 0
    return b"".join(parts)


def wire_key_only_payload_batch(
    wire_key_pool: tuple[bytes, ...], sequence: int, count: int, mode: int
) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | mode, count)
    start = sequence % size
    end = start + count
    if end <= size:
        return header + b"".join(wire_key_pool[start:end])

    parts = [header]
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        parts.extend(wire_key_pool[cursor : cursor + take])
        remaining -= take
        cursor = 0
    return b"".join(parts)


def wire_key_value_payload_batch(
    wire_key_pool: tuple[bytes, ...],
    sequence: int,
    count: int,
    value_fragment: bytes,
    mode: int = 1,
) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, mode, count)
    start = sequence % size
    parts = [header]
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        for key_fragment in wire_key_pool[cursor : cursor + take]:
            parts.append(key_fragment)
            parts.append(value_fragment)
        remaining -= take
        cursor = 0
    return b"".join(parts)


def wire_range_payload_batch(
    wire_key_pool: tuple[bytes, ...],
    sequence: int,
    count: int,
    mode: int,
    start_index: int,
    stop_index: int,
    include_zrange_scores_flag: bool,
) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | mode, count)
    start = sequence % size
    index_fragment = _COMPACT_I64.pack(start_index) + _COMPACT_I64.pack(stop_index)
    score_flag = b"\x00" if include_zrange_scores_flag else b""
    parts = [header]
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        for key_fragment in wire_key_pool[cursor : cursor + take]:
            parts.append(key_fragment)
            parts.append(index_fragment)
            parts.append(score_flag)
        remaining -= take
        cursor = 0
    return b"".join(parts)


def wire_hset_payload_batch(
    wire_key_pool: tuple[bytes, ...],
    sequence: int,
    count: int,
    field_fragment: bytes,
    value_fragment: bytes,
) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(
        _COMPACT_PIPELINE_REQUEST, 0x80 | _COMPACT_HSET_PIPELINE_MODE, count
    )
    start = sequence % size
    parts = [header]
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        for key_fragment in wire_key_pool[cursor : cursor + take]:
            parts.append(key_fragment)
            parts.append(field_fragment)
            parts.append(value_fragment)
        remaining -= take
        cursor = 0
    return b"".join(parts)


def wire_two_binary_payload_batch(
    wire_key_pool: tuple[bytes, ...],
    sequence: int,
    count: int,
    mode: int,
    item_fragment: bytes,
) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(_COMPACT_PIPELINE_REQUEST, 0x80 | mode, count)
    start = sequence % size
    parts = [header]
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        for key_fragment in wire_key_pool[cursor : cursor + take]:
            parts.append(key_fragment)
            parts.append(item_fragment)
        remaining -= take
        cursor = 0
    return b"".join(parts)


def wire_hmget_payload_batch(
    wire_key_pool: tuple[bytes, ...],
    sequence: int,
    count: int,
    field_fragment: bytes,
) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(
        _COMPACT_PIPELINE_REQUEST, 0x80 | _COMPACT_HMGET_PIPELINE_MODE, count
    )
    field_count = _COMPACT_U32.pack(1)
    start = sequence % size
    parts = [header]
    remaining = count
    cursor = start
    while remaining > 0:
        take = min(remaining, size - cursor)
        for key_fragment in wire_key_pool[cursor : cursor + take]:
            parts.append(key_fragment)
            parts.append(field_count)
            parts.append(field_fragment)
        remaining -= take
        cursor = 0
    return b"".join(parts)


def wire_zadd_payload_batch(
    wire_key_pool: tuple[bytes, ...],
    sequence: int,
    count: int,
    key_count: int,
    member_fragment: bytes,
) -> bytes:
    size = len(wire_key_pool)
    if count <= 0 or size == 0:
        return b""

    header = _COMPACT_PIPELINE_HEADER.pack(
        _COMPACT_PIPELINE_REQUEST, 0x80 | _COMPACT_ZADD_PIPELINE_MODE, count
    )
    start = sequence % size
    parts = [header]
    remaining = count
    cursor = start
    score_sequence = sequence
    while remaining > 0:
        take = min(remaining, size - cursor)
        for key_fragment in wire_key_pool[cursor : cursor + take]:
            parts.append(key_fragment)
            parts.append(_COMPACT_F64.pack(float(score_sequence % key_count)))
            parts.append(member_fragment)
            score_sequence += 1
        remaining -= take
        cursor = 0
    return b"".join(parts)


def build_many_command(
    command: str,
    key_prefix: str,
    sequence: int,
    count: int,
    key_count: int,
    value: bytes,
    read_percent: int,
    binary_keys: bool = False,
) -> tuple[Any, ...]:
    if command == "set":
        args: list[Any] = ["MSET"]
        for item in range(count):
            args.extend((benchmark_key(key_prefix, sequence + item, key_count, binary_keys), value))
        return tuple(args)
    if command == "get":
        return tuple(
            ["MGET"]
            + [
                benchmark_key(key_prefix, sequence + item, key_count, binary_keys)
                for item in range(count)
            ]
        )
    if command != "mixed":
        raise ValueError(f"unsupported command: {command}")

    if sequence % 100 < read_percent:
        return build_many_command(
            "get", key_prefix, sequence, count, key_count, value, read_percent, binary_keys
        )
    return build_many_command(
        "set", key_prefix, sequence, count, key_count, value, read_percent, binary_keys
    )


def _split_counts(total: int, parts: int) -> list[int]:
    base, remainder = divmod(total, parts)
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def _wait_futures(futures) -> int:
    errors = 0
    for future in futures:
        try:
            future.result()
        except Exception:
            errors += 1
    return errors


def _execute_batch(adapter: ProtocolAdapter, commands: list[tuple[Any, ...]]) -> int:
    try:
        adapter.execute_batch(commands)
        return 0
    except Exception:
        return len(commands)


def warmup_command(
    command: str, key_prefix: str, sequence: int, key_count: int, value: bytes, binary_keys: bool
) -> tuple[Any, ...]:
    key = benchmark_key(key_prefix, sequence, key_count, binary_keys)
    if command in {"hget", "hmget", "hgetall"}:
        return ("HSET", key, "field", value)
    if command in {"lrange", "lpop", "rpop"}:
        return ("RPUSH", key, value)
    if command == "smembers":
        return ("SADD", key, value)
    if command in {"sismember", "srem"}:
        return ("SADD", key, value)
    if command == "zrange":
        return ("ZADD", key, float(sequence % key_count), value)
    if command in {"zscore", "zrem"}:
        return ("ZADD", key, float(sequence % key_count), value)
    raise ValueError(f"unsupported warmup command: {command}")


def zrange_warmup_commands(
    key_prefix: str,
    sequence: int,
    key_count: int,
    value: bytes,
    binary_keys: bool,
    members_per_key: int,
) -> list[tuple[Any, ...]]:
    key = benchmark_key(key_prefix, sequence, key_count, binary_keys)
    return [
        ("ZADD", key, float(member), value + b":" + str(member).encode())
        for member in range(max(1, members_per_key))
    ]


def estimate_zrange_items_per_request(
    range_start: int, range_stop: int, members_per_key: int
) -> int:
    members = max(1, members_per_key)
    start = members + range_start if range_start < 0 else range_start
    stop = members + range_stop if range_stop < 0 else range_stop
    start = max(start, 0)
    stop = min(stop, members - 1)

    if start >= members or start > stop:
        return 0
    return stop - start + 1


def response_shape(args: argparse.Namespace) -> dict[str, Any]:
    if args.command != "zrange":
        return {
            "response_items_per_request_estimate": None,
            "response_items_per_batch_estimate": None,
            "large_response_warning": None,
        }

    per_request = estimate_zrange_items_per_request(
        args.range_start, args.range_stop, args.zset_members_per_key
    )
    per_batch = per_request * args.pipeline
    warning = None

    if per_batch >= LARGE_RESPONSE_VALUES_PER_BATCH:
        warning = (
            "large_zrange_response_batch: pipeline * estimated returned members "
            f"= {per_batch}; expect rejection by native collection-response guard "
            "or high latency/CPU if the guard is disabled"
        )

    return {
        "response_items_per_request_estimate": per_request,
        "response_items_per_batch_estimate": per_batch,
        "large_response_warning": warning,
    }


def _warmup(args: argparse.Namespace, value: bytes) -> int:
    if not args.warmup or args.command not in READ_WARMUP_COMMANDS:
        return 0

    warmup_batch_size = max(args.pipeline, 1000)
    adapter = ProtocolAdapter.from_url(
        args.url,
        lanes=args.protocol_lanes,
        timeout=args.timeout,
        client_name="ferricstore-protocol-kv-warmup",
    )
    warmed = 0
    try:
        for offset in range(0, args.key_count, warmup_batch_size):
            count = min(warmup_batch_size, args.key_count - offset)
            if args.command == "get":
                command: list[Any] = ["MSET"]
                for item in range(count):
                    key = benchmark_key(
                        args.key_prefix,
                        offset + item,
                        args.key_count,
                        args.binary_keys,
                    )
                    command.extend((key, value))
                adapter.execute_command(*command)
            elif args.command == "zrange" and args.zset_members_per_key > 1:
                commands = []
                for item in range(count):
                    commands.extend(
                        zrange_warmup_commands(
                            args.key_prefix,
                            offset + item,
                            args.key_count,
                            value,
                            args.binary_keys,
                            args.zset_members_per_key,
                        )
                    )
                    if len(commands) >= warmup_batch_size:
                        adapter.execute_batch(commands)
                        commands.clear()
                if commands:
                    adapter.execute_batch(commands)
            else:
                commands = [
                    warmup_command(
                        args.command,
                        args.key_prefix,
                        offset + item,
                        args.key_count,
                        value,
                        args.binary_keys,
                    )
                    for item in range(count)
                ]
                adapter.execute_batch(commands)
            warmed += count
    finally:
        adapter.close()
    return warmed


def _run_thread(
    *,
    args: argparse.Namespace,
    thread_index: int,
    request_count: int,
    sequence_start: int,
    value: bytes,
    deadline: float | None = None,
) -> dict[str, Any]:
    adapters = [
        ProtocolAdapter.from_url(
            args.url,
            lanes=args.protocol_lanes,
            timeout=args.timeout,
            client_name=f"ferricstore-protocol-kv-{thread_index}-{client_index}",
        )
        for client_index in range(args.clients)
    ]
    batch_latencies_ms: list[float] = []
    errors = 0
    issued = 0
    sequence = sequence_start
    max_pending_batches = max(1, args.inflight_batches) * len(adapters)
    pending_batches = []
    key_pool = (
        build_key_pool(args.key_prefix, args.key_count, args.binary_keys)
        if args.prebuild_keys and args.command in {"get", "set"}
        else None
    )
    wire_key_pool = (
        build_wire_key_pool(args.key_prefix, args.key_count, args.binary_keys)
        if args.prebuild_keys
        and args.request_mode in {"pipeline", "many"}
        and args.command
        in {
            "get",
            "set",
            "hget",
            "hmget",
            "hgetall",
            "hset",
            "lrange",
            "lpush",
            "rpush",
            "sadd",
            "srem",
            "sismember",
            "smembers",
            "zadd",
            "zrem",
            "zrange",
            "zscore",
        }
        else None
    )
    wire_field = _wire_value_fragment(b"field")
    wire_value = _wire_value_fragment(value)

    try:
        while _should_issue(issued, request_count, deadline) or pending_batches:
            while (
                args.request_mode in {"submit", "pipeline", "many"}
                and _should_issue(issued, request_count, deadline)
                and len(pending_batches) < max_pending_batches
            ):
                adapter = adapters[(issued // max(args.pipeline, 1)) % len(adapters)]
                remaining = args.pipeline if deadline is not None else request_count - issued
                batch_size = min(args.pipeline, remaining)
                if (
                    wire_key_pool is not None
                    and args.request_mode == "pipeline"
                    and args.command in {"get", "set"}
                    and hasattr(adapter, "submit_pipeline_payload")
                ):
                    if args.command == "get":
                        payload = wire_key_only_payload_batch(wire_key_pool, sequence, batch_size, 2)
                    else:
                        payload = wire_key_value_payload_batch(
                            wire_key_pool, sequence, batch_size, wire_value, 0x80 | 1
                        )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command == "hset" and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    payload = wire_hset_payload_batch(
                        wire_key_pool, sequence, batch_size, wire_field, wire_value
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command in {"hgetall", "smembers"} and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    mode = (
                        _COMPACT_HGETALL_PIPELINE_MODE
                        if args.command == "hgetall"
                        else _COMPACT_SMEMBERS_PIPELINE_MODE
                    )
                    payload = wire_key_only_payload_batch(wire_key_pool, sequence, batch_size, mode)
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command == "hget" and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    payload = wire_two_binary_payload_batch(
                        wire_key_pool, sequence, batch_size, _COMPACT_HGET_PIPELINE_MODE, wire_field
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command == "hmget" and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    payload = wire_hmget_payload_batch(
                        wire_key_pool,
                        sequence,
                        batch_size,
                        wire_field,
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command in {"sismember", "zscore"} and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    mode = (
                        _COMPACT_SISMEMBER_PIPELINE_MODE
                        if args.command == "sismember"
                        else _COMPACT_ZSCORE_PIPELINE_MODE
                    )
                    payload = wire_two_binary_payload_batch(
                        wire_key_pool, sequence, batch_size, mode, wire_value
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command in {"lrange", "zrange"} and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    mode = (
                        _COMPACT_LRANGE_PIPELINE_MODE
                        if args.command == "lrange"
                        else _COMPACT_ZRANGE_PIPELINE_MODE
                    )
                    payload = wire_range_payload_batch(
                        wire_key_pool,
                        sequence,
                        batch_size,
                        mode,
                        args.range_start,
                        args.range_stop,
                        args.command == "zrange",
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command in {
                    "lpush",
                    "rpush",
                    "sadd",
                    "srem",
                    "zrem",
                } and hasattr(adapter, "submit_pipeline_payload"):
                    mode = {
                        "lpush": _COMPACT_LPUSH_PIPELINE_MODE,
                        "rpush": _COMPACT_RPUSH_PIPELINE_MODE,
                        "sadd": _COMPACT_SADD_PIPELINE_MODE,
                        "srem": _COMPACT_SREM_PIPELINE_MODE,
                        "zrem": _COMPACT_ZREM_PIPELINE_MODE,
                    }[args.command]
                    payload = wire_two_binary_payload_batch(
                        wire_key_pool, sequence, batch_size, mode, wire_value
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if wire_key_pool is not None and args.command == "zadd" and hasattr(
                    adapter, "submit_pipeline_payload"
                ):
                    payload = wire_zadd_payload_batch(
                        wire_key_pool, sequence, batch_size, args.key_count, wire_value
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    future = adapter.submit_pipeline_payload(payload, batch_size)
                    pending_batches.append((started_ns, [future], batch_size))
                    continue

                if args.request_mode == "many" and args.command in {"get", "set", "mixed"}:
                    if wire_key_pool is not None and args.command == "get" and hasattr(
                        adapter, "submit_mget_payload"
                    ):
                        payload = wire_key_payload_batch(wire_key_pool, sequence, batch_size)
                        sequence += batch_size
                        issued += batch_size
                        started_ns = time.perf_counter_ns()
                        future = adapter.submit_mget_payload(payload)
                        pending_batches.append((started_ns, [future], batch_size))
                        continue

                    if wire_key_pool is not None and args.command == "set" and hasattr(
                        adapter, "submit_mset_payload"
                    ):
                        payload = wire_key_value_payload_batch(
                            wire_key_pool, sequence, batch_size, wire_value
                        )
                        sequence += batch_size
                        issued += batch_size
                        started_ns = time.perf_counter_ns()
                        future = adapter.submit_mset_payload(payload)
                        pending_batches.append((started_ns, [future], batch_size))
                        continue

                    if key_pool is not None:
                        keys = key_batch(key_pool, sequence, batch_size)
                        sequence += batch_size
                        issued += batch_size
                        started_ns = time.perf_counter_ns()
                        if args.command == "get":
                            future = adapter.submit_mget(keys)
                        else:
                            future = adapter.submit_mset_same_value(keys, value)
                        pending_batches.append((started_ns, [future], batch_size))
                        continue

                    command = build_many_command(
                        args.command,
                        args.key_prefix,
                        sequence,
                        batch_size,
                        args.key_count,
                        value,
                        args.read_percent,
                        args.binary_keys,
                    )
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    pending_batches.append(
                        (started_ns, [adapter.submit_command(*command)], batch_size)
                    )
                    continue

                commands = [
                    build_command(
                        args.command,
                        args.key_prefix,
                        sequence + item,
                        args.key_count,
                        value,
                        args.read_percent,
                        args.binary_keys,
                        args.range_start,
                        args.range_stop,
                    )
                    for item in range(batch_size)
                ]
                sequence += batch_size
                issued += batch_size
                started_ns = time.perf_counter_ns()
                if args.request_mode in {"pipeline", "many"}:
                    pending_batches.append(
                        (started_ns, [adapter.submit_batch(commands)], batch_size)
                    )
                else:
                    pending_batches.append(
                        (started_ns, adapter.submit_commands(commands), batch_size)
                    )

            if args.request_mode in {"submit", "pipeline", "many"} and pending_batches:
                started_ns, futures, batch_size = pending_batches.pop(0)
                batch_errors = _wait_futures(futures)
                errors += batch_size if batch_errors else 0
                elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
                batch_latencies_ms.append(elapsed_ms)
            elif args.request_mode == "batch" and _should_issue(issued, request_count, deadline):
                for adapter in adapters:
                    if not _should_issue(issued, request_count, deadline):
                        break
                    remaining = args.pipeline if deadline is not None else request_count - issued
                    batch_size = min(args.pipeline, remaining)
                    commands = [
                        build_command(
                            args.command,
                            args.key_prefix,
                            sequence + item,
                            args.key_count,
                            value,
                            args.read_percent,
                            args.binary_keys,
                            args.range_start,
                            args.range_stop,
                        )
                        for item in range(batch_size)
                    ]
                    sequence += batch_size
                    issued += batch_size
                    started_ns = time.perf_counter_ns()
                    errors += _execute_batch(adapter, commands)
                    elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
                    batch_latencies_ms.append(elapsed_ms)
    finally:
        for adapter in adapters:
            adapter.close()

    return {
        "requests": issued,
        "errors": errors,
        "batch_latencies_ms": batch_latencies_ms,
    }


def _should_issue(issued: int, request_count: int, deadline: float | None) -> bool:
    if deadline is not None:
        return time.perf_counter() < deadline
    return issued < request_count


def _run_process(
    args: argparse.Namespace,
    process_index: int,
    request_count: int,
    sequence_start: int,
    value: bytes,
) -> dict[str, Any]:
    deadline = None
    if args.test_time is not None:
        counts = [sys.maxsize for _ in range(args.threads)]
        deadline = time.perf_counter() + args.test_time
    else:
        counts = _split_counts(request_count, args.threads)

    starts = []
    cursor = sequence_start
    for count in counts:
        starts.append(cursor)
        cursor += count

    started = time.perf_counter()
    cpu_started = time.process_time()
    results = []
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [
            executor.submit(
                _run_thread,
                args=args,
                thread_index=index,
                request_count=count,
                sequence_start=starts[index],
                value=value,
                deadline=deadline,
            )
            for index, count in enumerate(counts)
            if count > 0
        ]
        for future in as_completed(futures):
            results.append(future.result())
    elapsed_s = max(time.perf_counter() - started, 1e-9)
    cpu_seconds = max(time.process_time() - cpu_started, 0.0)

    return {
        "process_index": process_index,
        "requests": sum(int(result["requests"]) for result in results),
        "errors": sum(int(result["errors"]) for result in results),
        "batch_latencies_ms": [
            latency for result in results for latency in result["batch_latencies_ms"]
        ],
        "seconds": elapsed_s,
        "client_cpu_seconds": cpu_seconds,
    }


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    scheme = urlparse(args.url).scheme.lower()
    if scheme not in PROTOCOL_SCHEMES:
        raise ValueError("protocol KV benchmark requires ferric:// or ferrics:// URL")

    shape = response_shape(args)
    if shape["large_response_warning"] and not args.allow_large_response_batches:
        raise ValueError(
            f"{shape['large_response_warning']}; pass --allow-large-response-batches "
            "to intentionally benchmark guarded large collection responses"
        )

    value = make_value(args.value_bytes)
    warmed_keys = _warmup(args, value)

    if args.test_time is not None:
        process_counts = [sys.maxsize for _ in range(args.processes)]
    else:
        process_counts = _split_counts(args.requests, args.processes)

    process_starts = []
    cursor = 0
    for count in process_counts:
        process_starts.append(cursor)
        cursor += count

    started = time.perf_counter()
    if args.processes == 1:
        process_results = [_run_process(args, 0, process_counts[0], process_starts[0], value)]
    else:
        with ProcessPoolExecutor(max_workers=args.processes) as executor:
            futures = [
                executor.submit(
                    _run_process,
                    args,
                    index,
                    count,
                    process_starts[index],
                    value,
                )
                for index, count in enumerate(process_counts)
                if count > 0
            ]
            process_results = [future.result() for future in as_completed(futures)]

    elapsed_s = max(time.perf_counter() - started, 1e-9)
    batch_latencies = [
        latency for result in process_results for latency in result["batch_latencies_ms"]
    ]
    requests = sum(int(result["requests"]) for result in process_results)
    errors = sum(int(result["errors"]) for result in process_results)
    cpu_seconds = sum(float(result["client_cpu_seconds"]) for result in process_results)

    return {
        "benchmark": "protocol_kv",
        "preset": args.preset,
        "url": args.url,
        "command": args.command,
        "requests": requests,
        "configured_requests": None if args.test_time is not None else args.requests,
        "test_time": args.test_time,
        "errors": errors,
        "seconds": elapsed_s,
        "client_cpu_seconds": cpu_seconds,
        "client_cpu_percent": (cpu_seconds / elapsed_s) * 100.0,
        "requests_per_sec": requests / elapsed_s,
        "processes": args.processes,
        "threads": args.threads,
        "clients_per_thread": args.clients,
        "total_connections": args.processes * args.threads * args.clients,
        "pipeline": args.pipeline,
        "request_mode": args.request_mode,
        "inflight_batches": args.inflight_batches,
        "protocol_lanes": args.protocol_lanes,
        "key_count": args.key_count,
        "binary_keys": args.binary_keys,
        "prebuild_keys": args.prebuild_keys,
        "value_bytes": args.value_bytes,
        "read_percent": args.read_percent if args.command == "mixed" else None,
        "range_start": args.range_start if args.command in {"lrange", "zrange"} else None,
        "range_stop": args.range_stop if args.command in {"lrange", "zrange"} else None,
        "zset_members_per_key": args.zset_members_per_key if args.command == "zrange" else None,
        **shape,
        "warmed_keys": warmed_keys,
        "batch_latency_samples": len(batch_latencies),
        "batch_latency_avg_ms": (
            sum(batch_latencies) / len(batch_latencies) if batch_latencies else 0.0
        ),
        "batch_latency_p50_ms": percentile(batch_latencies, 50),
        "batch_latency_p95_ms": percentile(batch_latencies, 95),
        "batch_latency_p99_ms": percentile(batch_latencies, 99),
        "batch_latency_max_ms": max(batch_latencies) if batch_latencies else 0.0,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    preset_parser = argparse.ArgumentParser(add_help=False)
    preset_parser.add_argument("--preset", choices=tuple(PRESETS), default=None)
    preset_args, _unknown = preset_parser.parse_known_args(argv)
    preset_defaults = PRESETS.get(preset_args.preset, {})

    parser = argparse.ArgumentParser(
        description="FerricStore protocol SET/GET benchmark with memtier-shaped knobs"
    )
    parser.add_argument(
        "--preset",
        choices=tuple(PRESETS),
        default=None,
        help="Apply a measured native protocol benchmark shape.",
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument(
        "--command",
        choices=(
            "set",
            "get",
            "mixed",
            "hset",
            "hget",
            "hmget",
            "hgetall",
            "lpush",
            "rpush",
            "lrange",
            "lpop",
            "rpop",
            "sadd",
            "srem",
            "smembers",
            "sismember",
            "zadd",
            "zrem",
            "zrange",
            "zscore",
        ),
        default=preset_defaults.get("command", "set"),
    )
    parser.add_argument("--requests", type=int, default=100_000)
    parser.add_argument("--test-time", type=float, default=preset_defaults.get("test_time"))
    parser.add_argument("--threads", type=int, default=preset_defaults.get("threads", 1))
    parser.add_argument("--processes", type=int, default=preset_defaults.get("processes", 1))
    parser.add_argument(
        "--clients",
        type=int,
        default=preset_defaults.get("clients", 1),
        help="Connections per thread",
    )
    parser.add_argument("--pipeline", type=int, default=preset_defaults.get("pipeline", 50))
    parser.add_argument(
        "--request-mode",
        choices=("batch", "submit", "pipeline", "many"),
        default=preset_defaults.get("request_mode", "batch"),
        help=(
            "batch waits for each pipeline frame; pipeline keeps pipeline frames in flight; "
            "submit returns per-command futures; many uses explicit bulk commands."
        ),
    )
    parser.add_argument(
        "--inflight-batches",
        type=int,
        default=preset_defaults.get("inflight_batches", 64),
        help="Max submitted-but-not-yet-awaited batches per connection.",
    )
    parser.add_argument(
        "--protocol-lanes",
        type=int,
        default=preset_defaults.get("protocol_lanes", 64),
    )
    parser.add_argument(
        "--key-prefix",
        default=None,
        help=(
            "Benchmark key prefix. Defaults to protocol-kv for KV commands and "
            "protocol-kv:<command> for data-structure commands to avoid WRONGTYPE "
            "collisions during sequential sweeps."
        ),
    )
    parser.add_argument("--key-count", type=int, default=100_000)
    parser.add_argument(
        "--binary-keys",
        action="store_true",
        help=(
            "Build benchmark keys as bytes to measure protocol capacity without "
            "Python string encoding."
        ),
    )
    parser.add_argument(
        "--prebuild-keys",
        action=argparse.BooleanOptionalAction,
        default=preset_defaults.get("prebuild_keys", False),
        help="Prebuild benchmark key objects once per worker to reduce load-generator CPU.",
    )
    parser.add_argument("--value-bytes", type=int, default=16)
    parser.add_argument("--read-percent", type=int, default=50)
    parser.add_argument(
        "--range-start",
        type=int,
        default=0,
        help="Start index for LRANGE/ZRANGE benchmark commands.",
    )
    parser.add_argument(
        "--range-stop",
        type=int,
        default=0,
        help=(
            "Stop index for LRANGE/ZRANGE benchmark commands. Defaults to 0 "
            "so protocol sweeps measure bounded range reads; pass -1 to "
            "measure full collection materialization."
        ),
    )
    parser.add_argument(
        "--zset-members-per-key",
        type=int,
        default=1,
        help="For zrange warmup, create this many sorted-set members per key.",
    )
    parser.add_argument(
        "--allow-large-response-batches",
        action="store_true",
        help=(
            "Allow ZRANGE benchmark shapes where pipeline * estimated returned "
            "members exceeds the native collection-response guard. By default "
            "these fail fast because they mostly measure guarded rejections or "
            "large response materialization, not useful command throughput."
        ),
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--warmup", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)

    if args.requests <= 0:
        parser.error("--requests must be positive")
    if args.test_time is not None and args.test_time <= 0:
        parser.error("--test-time must be positive")
    if args.threads <= 0:
        parser.error("--threads must be positive")
    if args.processes <= 0:
        parser.error("--processes must be positive")
    if args.clients <= 0:
        parser.error("--clients must be positive")
    if args.pipeline <= 0:
        parser.error("--pipeline must be positive")
    if args.inflight_batches <= 0:
        parser.error("--inflight-batches must be positive")
    if args.protocol_lanes <= 0:
        parser.error("--protocol-lanes must be positive")
    if args.key_count <= 0:
        parser.error("--key-count must be positive")
    if args.value_bytes < 0:
        parser.error("--value-bytes must be non-negative")
    if args.read_percent < 0 or args.read_percent > 100:
        parser.error("--read-percent must be between 0 and 100")
    if args.zset_members_per_key <= 0:
        parser.error("--zset-members-per-key must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.key_prefix is None:
        args.key_prefix = default_key_prefix(args.command)
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        result = run_benchmark(args)
    except ValueError as exc:
        raise SystemExit(f"error: {exc}") from None
    indent = 2 if args.pretty else None
    print(json.dumps(result, sort_keys=True, indent=indent))


if __name__ == "__main__":
    main(sys.argv[1:])
