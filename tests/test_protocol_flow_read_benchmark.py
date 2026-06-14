from __future__ import annotations

import importlib.util
from pathlib import Path


_BENCH_PATH = Path(__file__).resolve().parents[1] / "examples" / "protocol_flow_read_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("protocol_flow_read_benchmark", _BENCH_PATH)
assert _SPEC is not None
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_protocol_flow_read_defaults_are_native_flow_read_shape():
    args = bench.parse_args([])

    assert args.mode == "flow-get-meta"
    assert args.url == "ferric://127.0.0.1:6388"
    assert args.flows == 100_000
    assert args.test_time == 30.0
    assert args.create_batch_size == 500
    assert args.read_batch_size == 500
    assert args.inflight_batches == 64


def test_protocol_flow_read_builds_create_commands():
    commands = bench.create_commands("email", 0, 2, b"payload")

    assert commands == [
        ("FLOW.CREATE", "email:0", "TYPE", "email", "STATE", "queued", "PAYLOAD", b"payload"),
        ("FLOW.CREATE", "email:1", "TYPE", "email", "STATE", "queued", "PAYLOAD", b"payload"),
    ]

    assert bench.create_commands("email", 0, 1, b"payload", partition_key="tenant-a") == [
        (
            "FLOW.CREATE",
            "email:0",
            "TYPE",
            "email",
            "STATE",
            "queued",
            "PARTITION",
            "tenant-a",
            "PAYLOAD",
            b"payload",
        )
    ]


def test_protocol_flow_read_builds_get_commands():
    assert bench.flow_get_commands("flow-get", "email", 0, 2, 10) == [
        ("FLOW.GET", "email:0"),
        ("FLOW.GET", "email:1"),
    ]

    assert bench.flow_get_commands("flow-get-meta", "email", 9, 3, 10) == [
        ("FLOW.GET", "email:9", "RETURN", "META"),
        ("FLOW.GET", "email:0", "RETURN", "META"),
        ("FLOW.GET", "email:1", "RETURN", "META"),
    ]

    assert bench.flow_get_commands(
        "flow-get-meta", "email", 0, 1, 10, partition_key="tenant-a"
    ) == [("FLOW.GET", "email:0", "PARTITION", "tenant-a", "RETURN", "META")]


def test_protocol_flow_read_builds_value_commands():
    assert bench.value_put_commands("email", 0, 1, b"value") == [
        (
            "FLOW.VALUE.PUT",
            b"value",
            "OWNER_FLOW_ID",
            "email:0",
            "NAME",
            "bench-value",
        )
    ]

    assert bench.value_put_commands("email", 0, 1, b"value", partition_key="tenant-a") == [
        (
            "FLOW.VALUE.PUT",
            b"value",
            "OWNER_FLOW_ID",
            "email:0",
            "NAME",
            "bench-value",
            "PARTITION",
            "tenant-a",
        )
    ]

    assert bench.flow_value_mget_command(["ref-0", "ref-1"], 1, 3, 1024) == (
        "FLOW.VALUE.MGET",
        "ref-1",
        "ref-0",
        "ref-1",
        "MAX_BYTES",
        1024,
    )


def test_protocol_flow_read_extracts_value_refs_from_put_results():
    assert bench.value_refs_from_results(
        [
            {b"ref": b"ref-1", b"name": b"bench-value"},
            {"ref": "ref-2"},
            b"ref-3",
        ]
    ) == [b"ref-1", "ref-2", b"ref-3"]


def test_protocol_flow_read_result_reports_item_rate():
    args = bench.parse_args(["--mode", "flow-list-meta", "--test-time", "2", "--list-count", "50"])
    result = bench.benchmark_result(args, requests=4, items=200, latencies_ms=[1.0, 2.0, 3.0])

    assert result["requests_per_sec"] == 2.0
    assert result["items_per_sec"] == 100.0
    assert result["list_count"] == 50
    assert result["batch_latency_p50_ms"] == 2.0
