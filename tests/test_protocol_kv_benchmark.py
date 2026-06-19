import importlib.util
from concurrent.futures import Future
from pathlib import Path

import pytest


_BENCH_PATH = Path(__file__).resolve().parents[1] / "examples" / "protocol_kv_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("protocol_kv_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_protocol_kv_defaults_are_ferric_and_memtier_shaped():
    args = bench.parse_args([])

    assert args.preset is None
    assert args.url == "ferric://127.0.0.1:6388"
    assert args.command == "set"
    assert args.processes == 1
    assert args.threads == 1
    assert args.clients == 1
    assert args.pipeline == 50
    assert args.request_mode == "batch"
    assert args.inflight_batches == 64
    assert args.protocol_lanes == 64
    assert args.test_time is None
    assert args.range_start == 0
    assert args.range_stop == 0
    assert args.zset_members_per_key == 1


def test_protocol_kv_preset_applies_measured_native_shape():
    args = bench.parse_args(["--preset", "get-latency"])

    assert args.preset == "get-latency"
    assert args.command == "get"
    assert args.request_mode == "many"
    assert args.pipeline == 10
    assert args.clients == 1
    assert args.threads == 1
    assert args.inflight_batches == 8
    assert args.protocol_lanes == 8
    assert args.test_time == 30.0
    assert args.prebuild_keys is True


def test_protocol_kv_preset_allows_explicit_overrides():
    args = bench.parse_args(
        [
            "--preset",
            "get-throughput",
            "--threads",
            "4",
            "--processes",
            "3",
            "--clients",
            "50",
            "--pipeline",
            "200",
        ]
    )

    assert args.command == "get"
    assert args.request_mode == "many"
    assert args.processes == 3
    assert args.threads == 4
    assert args.clients == 50
    assert args.pipeline == 200
    assert args.protocol_lanes == 64
    assert args.test_time == 30.0
    assert args.prebuild_keys is True


def test_protocol_kv_get_balanced_preset_uses_measured_latency_shape():
    args = bench.parse_args(["--preset", "get-balanced"])

    assert args.command == "get"
    assert args.request_mode == "many"
    assert args.pipeline == 500
    assert args.protocol_lanes == 64
    assert args.inflight_batches == 64
    assert args.prebuild_keys is True


def test_protocol_kv_get_low_latency_preset_uses_small_bulk_batches():
    args = bench.parse_args(["--preset", "get-low-latency"])

    assert args.command == "get"
    assert args.request_mode == "many"
    assert args.pipeline == 100
    assert args.protocol_lanes == 64
    assert args.inflight_batches == 64
    assert args.prebuild_keys is True


def test_protocol_kv_set_latency_preset_uses_small_durable_batches():
    args = bench.parse_args(["--preset", "set-latency"])

    assert args.command == "set"
    assert args.request_mode == "many"
    assert args.pipeline == 10
    assert args.protocol_lanes == 8
    assert args.inflight_batches == 8
    assert args.prebuild_keys is True


def test_protocol_kv_default_key_prefix_is_namespaced_for_data_structures():
    args = bench.parse_args(["--command", "lrange"])

    assert args.key_prefix == "protocol-kv:lrange"


def test_protocol_kv_explicit_key_prefix_is_preserved_for_data_structures():
    args = bench.parse_args(["--command", "zrange", "--key-prefix", "custom"])

    assert args.key_prefix == "custom"


def test_protocol_kv_builds_set_get_and_mixed_commands():
    value = bench.make_value(3)

    assert bench.build_command("set", "bench", 7, 100, value, 50) == (
        "SET",
        "bench:7",
        b"xxx",
    )
    assert bench.build_command("get", "bench", 8, 100, value, 50) == ("GET", "bench:8")
    assert bench.build_command("mixed", "bench", 10, 100, value, 50) == (
        "GET",
        "bench:10",
    )
    assert bench.build_command("mixed", "bench", 11, 100, value, 50) == (
        "SET",
        "bench:11",
        b"xxx",
    )


def test_protocol_kv_builds_data_structure_commands():
    value = bench.make_value(2)

    assert bench.build_command("hset", "bench", 7, 100, value, 50) == (
        "HSET",
        "bench:7",
        "field",
        b"xx",
    )
    assert bench.build_command("hget", "bench", 7, 100, value, 50) == (
        "HGET",
        "bench:7",
        "field",
    )
    assert bench.build_command("hmget", "bench", 7, 100, value, 50) == (
        "HMGET",
        "bench:7",
        "field",
    )
    assert bench.build_command("hgetall", "bench", 7, 100, value, 50) == (
        "HGETALL",
        "bench:7",
    )
    assert bench.build_command("lpush", "bench", 7, 100, value, 50) == (
        "LPUSH",
        "bench:7",
        b"xx",
    )
    assert bench.build_command("rpush", "bench", 7, 100, value, 50) == (
        "RPUSH",
        "bench:7",
        b"xx",
    )
    assert bench.build_command("lrange", "bench", 7, 100, value, 50) == (
        "LRANGE",
        "bench:7",
        0,
        0,
    )
    assert bench.build_command("lpop", "bench", 7, 100, value, 50) == (
        "LPOP",
        "bench:7",
    )
    assert bench.build_command("rpop", "bench", 7, 100, value, 50) == (
        "RPOP",
        "bench:7",
    )
    assert bench.build_command("sadd", "bench", 7, 100, value, 50) == (
        "SADD",
        "bench:7",
        b"xx",
    )
    assert bench.build_command("srem", "bench", 7, 100, value, 50) == (
        "SREM",
        "bench:7",
        b"xx",
    )
    assert bench.build_command("smembers", "bench", 7, 100, value, 50) == (
        "SMEMBERS",
        "bench:7",
    )
    assert bench.build_command("sismember", "bench", 7, 100, value, 50) == (
        "SISMEMBER",
        "bench:7",
        b"xx",
    )
    assert bench.build_command("zadd", "bench", 7, 100, value, 50) == (
        "ZADD",
        "bench:7",
        7.0,
        b"xx",
    )
    assert bench.build_command("zrem", "bench", 7, 100, value, 50) == (
        "ZREM",
        "bench:7",
        b"xx",
    )
    assert bench.build_command("zrange", "bench", 7, 100, value, 50) == (
        "ZRANGE",
        "bench:7",
        0,
        0,
    )
    assert bench.build_command("zscore", "bench", 7, 100, value, 50) == (
        "ZSCORE",
        "bench:7",
        b"xx",
    )


def test_protocol_kv_range_commands_support_explicit_window():
    value = bench.make_value(2)

    assert bench.build_command(
        "lrange",
        "bench",
        7,
        100,
        value,
        50,
        range_start=2,
        range_stop=4,
    ) == ("LRANGE", "bench:7", 2, 4)
    assert bench.build_command(
        "zrange",
        "bench",
        7,
        100,
        value,
        50,
        range_start=2,
        range_stop=4,
    ) == ("ZRANGE", "bench:7", 2, 4)


def test_protocol_kv_result_reports_range_window(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.batches = []
            created.append(self)

        def submit_batch(self, commands):
            self.batches.append(list(commands))
            future = Future()
            future.set_result([[b"value"] for _command in commands])
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "3",
            "--pipeline",
            "2",
            "--command",
            "zrange",
            "--request-mode",
            "pipeline",
            "--range-start",
            "1",
            "--range-stop",
            "3",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["range_start"] == 1
    assert result["range_stop"] == 3
    assert result["zset_members_per_key"] == 1
    assert result["response_items_per_request_estimate"] == 0
    assert result["response_items_per_batch_estimate"] == 0
    assert result["large_response_warning"] is None
    assert created[0].batches[0][0] == ("ZRANGE", "protocol-kv:zrange:0", 1, 3)


def test_protocol_kv_builds_data_structure_warmup_commands():
    value = bench.make_value(2)

    assert bench.warmup_command("hget", "bench", 7, 100, value, False) == (
        "HSET",
        "bench:7",
        "field",
        b"xx",
    )
    assert bench.warmup_command("hmget", "bench", 7, 100, value, False) == (
        "HSET",
        "bench:7",
        "field",
        b"xx",
    )
    assert bench.warmup_command("hgetall", "bench", 7, 100, value, False) == (
        "HSET",
        "bench:7",
        "field",
        b"xx",
    )
    assert bench.warmup_command("lrange", "bench", 7, 100, value, False) == (
        "RPUSH",
        "bench:7",
        b"xx",
    )
    assert bench.warmup_command("lpop", "bench", 7, 100, value, False) == (
        "RPUSH",
        "bench:7",
        b"xx",
    )
    assert bench.warmup_command("rpop", "bench", 7, 100, value, False) == (
        "RPUSH",
        "bench:7",
        b"xx",
    )
    assert bench.warmup_command("sismember", "bench", 7, 100, value, False) == (
        "SADD",
        "bench:7",
        b"xx",
    )
    assert bench.warmup_command("srem", "bench", 7, 100, value, False) == (
        "SADD",
        "bench:7",
        b"xx",
    )
    assert bench.warmup_command("zrange", "bench", 7, 100, value, False) == (
        "ZADD",
        "bench:7",
        7.0,
        b"xx",
    )
    assert bench.warmup_command("zscore", "bench", 7, 100, value, False) == (
        "ZADD",
        "bench:7",
        7.0,
        b"xx",
    )
    assert bench.warmup_command("zrem", "bench", 7, 100, value, False) == (
        "ZADD",
        "bench:7",
        7.0,
        b"xx",
    )


def test_protocol_kv_builds_multi_member_zrange_warmup_commands():
    value = bench.make_value(2)

    assert bench.zrange_warmup_commands("bench", 7, 100, value, False, 3) == [
        ("ZADD", "bench:7", 0.0, b"xx:0"),
        ("ZADD", "bench:7", 1.0, b"xx:1"),
        ("ZADD", "bench:7", 2.0, b"xx:2"),
    ]


def test_protocol_kv_estimates_zrange_response_shape():
    assert bench.estimate_zrange_items_per_request(0, 0, 100) == 1
    assert bench.estimate_zrange_items_per_request(0, -1, 100) == 100
    assert bench.estimate_zrange_items_per_request(-10, -1, 100) == 10
    assert bench.estimate_zrange_items_per_request(200, -1, 100) == 0


def test_protocol_kv_reports_large_zrange_response_warning(monkeypatch):
    class FakeAdapter:
        def submit_batch(self, commands):
            future = Future()
            future.set_result([[b"value"] for _command in commands])
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "1",
            "--command",
            "zrange",
            "--request-mode",
            "pipeline",
            "--pipeline",
            "1000",
            "--range-start",
            "0",
            "--range-stop",
            "-1",
            "--zset-members-per-key",
            "100",
            "--allow-large-response-batches",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["response_items_per_request_estimate"] == 100
    assert result["response_items_per_batch_estimate"] == 100_000
    assert result["large_response_warning"].startswith("large_zrange_response_batch")


def test_protocol_kv_rejects_unsafe_zrange_response_batch_by_default(monkeypatch):
    class FakeAdapter:
        def submit_batch(self, commands):
            raise AssertionError(f"benchmark should fail before execution: {commands!r}")

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "1",
            "--command",
            "zrange",
            "--request-mode",
            "pipeline",
            "--pipeline",
            "1000",
            "--range-start",
            "0",
            "--range-stop",
            "-1",
            "--zset-members-per-key",
            "100",
            "--no-warmup",
        ]
    )

    with pytest.raises(ValueError, match="large_zrange_response_batch"):
        bench.run_benchmark(args)


def test_protocol_kv_zrange_warmup_can_create_multiple_members_per_key(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.batches = []
            created.append(self)

        def execute_batch(self, commands):
            self.batches.append(list(commands))
            return [b"OK" for _command in commands]

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "1",
            "--command",
            "zrange",
            "--key-count",
            "2",
            "--zset-members-per-key",
            "3",
        ]
    )

    warmed = bench._warmup(args, bench.make_value(1))

    assert warmed == 2
    assert created[0].batches == [
        [
            ("ZADD", "protocol-kv:zrange:0", 0.0, b"x:0"),
            ("ZADD", "protocol-kv:zrange:0", 1.0, b"x:1"),
            ("ZADD", "protocol-kv:zrange:0", 2.0, b"x:2"),
            ("ZADD", "protocol-kv:zrange:1", 0.0, b"x:0"),
            ("ZADD", "protocol-kv:zrange:1", 1.0, b"x:1"),
            ("ZADD", "protocol-kv:zrange:1", 2.0, b"x:2"),
        ]
    ]


def test_protocol_kv_builds_many_commands():
    value = bench.make_value(2)

    assert bench.build_many_command("get", "bench", 3, 2, 100, value, 50) == (
        "MGET",
        "bench:3",
        "bench:4",
    )
    assert bench.build_many_command("set", "bench", 3, 2, 100, value, 50) == (
        "MSET",
        "bench:3",
        b"xx",
        "bench:4",
        b"xx",
    )


def test_protocol_kv_key_batch_wraps_prebuilt_key_pool():
    pool = bench.build_key_pool("bench", 4, False)

    assert pool == ("bench:0", "bench:1", "bench:2", "bench:3")
    assert bench.key_batch(pool, 2, 5) == (
        "bench:2",
        "bench:3",
        "bench:0",
        "bench:1",
        "bench:2",
    )


def test_protocol_kv_run_uses_submit_commands_pipeline(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.batches = []
            self.closed = False
            created.append(self)

        def submit_commands(self, commands):
            self.batches.append(list(commands))
            futures = []
            for command in commands:
                future = Future()
                future.set_result(b"OK" if command[0] == "SET" else b"value")
                futures.append(future)
            return futures

        def execute_batch(self, commands):
            self.batches.append(list(commands))
            return [b"OK" if command[0] == "SET" else b"value" for command in commands]

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "mixed",
            "--request-mode",
            "submit",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["requests"] == 5
    assert result["total_connections"] == 1
    assert result["pipeline"] == 2
    assert result["command"] == "mixed"
    assert result["errors"] == 0
    assert [len(batch) for batch in created[0].batches] == [2, 2, 1]
    assert created[0].closed is True


def test_protocol_kv_processes_scale_total_connections(monkeypatch):
    class FakeProcessPool:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def submit(self, fn, *args):
            future = Future()
            future.set_result(fn(*args))
            return future

    class FakeAdapter:
        def __init__(self):
            self.closed = False

        def submit_command(self, *command):
            future = Future()
            future.set_result([b"value"] * (len(command) - 1))
            return future

        def close(self):
            self.closed = True

    monkeypatch.setattr(bench, "ProcessPoolExecutor", FakeProcessPool)
    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "12",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--processes",
            "3",
            "--command",
            "get",
            "--request-mode",
            "many",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["processes"] == 3
    assert result["total_connections"] == 3
    assert result["requests"] == 12
    assert result["errors"] == 0


def test_protocol_kv_pipeline_mode_uses_submit_batch(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.batches = []
            self.closed = False
            created.append(self)

        def submit_batch(self, commands):
            self.batches.append(list(commands))
            future = Future()
            future.set_result([b"OK" for _command in commands])
            return future

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "set",
            "--request-mode",
            "pipeline",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["request_mode"] == "pipeline"
    assert result["errors"] == 0
    assert [len(batch) for batch in created[0].batches] == [2, 2, 1]
    assert created[0].closed is True


@pytest.mark.parametrize(
    ("command", "mode", "expected_value"),
    [
        ("get", 0x80 | 2, None),
        ("set", 0x80 | 1, b"x" * 5),
    ],
)
def test_protocol_kv_pipeline_mode_uses_preencoded_get_set_payload_when_available(
    monkeypatch, command, mode, expected_value
):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([b"OK" if command == "set" else b"value"] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            command,
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--value-bytes",
            "5",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    def item(key: bytes) -> bytes:
        if expected_value is None:
            return binary(key)
        return binary(key) + binary(expected_value)

    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, mode, 2)
            + item(b"protocol-kv:0")
            + item(b"protocol-kv:1"),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, mode, 2)
            + item(b"protocol-kv:2")
            + item(b"protocol-kv:3"),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, mode, 1)
            + item(b"protocol-kv:4"),
            1,
        ),
    ]


def test_protocol_kv_batch_mode_uses_protocol_batch(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.batches = []
            created.append(self)

        def execute_batch(self, commands):
            self.batches.append(list(commands))
            return [b"OK" for _command in commands]

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "set",
            "--request-mode",
            "batch",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["request_mode"] == "batch"
    assert result["errors"] == 0
    assert [len(batch) for batch in created[0].batches] == [2, 2, 1]


def test_protocol_kv_test_time_runs_until_deadline(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.batches = []
            created.append(self)

        def submit_batch(self, commands):
            self.batches.append(list(commands))
            future = Future()
            future.set_result([b"OK" for _command in commands])
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--test-time",
            "0.01",
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "set",
            "--request-mode",
            "pipeline",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["test_time"] == 0.01
    assert result["configured_requests"] is None
    assert result["requests"] >= 5
    assert all(len(batch) == 2 for batch in created[0].batches)


def test_protocol_kv_many_mode_uses_bulk_commands(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.commands = []
            created.append(self)

        def submit_command(self, *command):
            self.commands.append(command)
            future = Future()
            future.set_result([b"value"] * (len(command) - 1))
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "get",
            "--request-mode",
            "many",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["request_mode"] == "many"
    assert result["errors"] == 0
    assert [command[0] for command in created[0].commands] == [
        "MGET",
        "MGET",
        "MGET",
    ]


def test_protocol_kv_many_mode_uses_prebuilt_fast_bulk_methods(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.mgets = []
            created.append(self)

        def submit_command(self, *command):
            raise AssertionError(f"generic submit_command should not be used: {command!r}")

        def submit_mget(self, keys):
            self.mgets.append(tuple(keys))
            future = Future()
            future.set_result([b"value"] * len(keys))
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "get",
            "--request-mode",
            "many",
            "--prebuild-keys",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0
    assert created[0].mgets == [
        ("protocol-kv:0", "protocol-kv:1"),
        ("protocol-kv:2", "protocol-kv:3"),
        ("protocol-kv:4",),
    ]


def test_protocol_kv_many_mode_uses_preencoded_mget_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_command(self, *command):
            raise AssertionError(f"generic submit_command should not be used: {command!r}")

        def submit_mget(self, keys):
            raise AssertionError(f"submit_mget should not be used: {keys!r}")

        def submit_mget_payload(self, payload):
            self.payloads.append(payload)
            future = Future()
            future.set_result([b"value"])
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "get",
            "--request-mode",
            "many",
            "--prebuild-keys",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    assert created[0].payloads == [
        bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, 2, 2)
        + binary(b"protocol-kv:0")
        + binary(b"protocol-kv:1"),
        bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, 2, 2)
        + binary(b"protocol-kv:2")
        + binary(b"protocol-kv:3"),
        bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, 2, 1)
        + binary(b"protocol-kv:4"),
    ]


def test_protocol_kv_many_mode_uses_preencoded_mset_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_command(self, *command):
            raise AssertionError(f"generic submit_command should not be used: {command!r}")

        def submit_mset_same_value(self, keys, value):
            raise AssertionError(f"submit_mset_same_value should not be used: {keys!r} {value!r}")

        def submit_mset_payload(self, payload):
            self.payloads.append(payload)
            future = Future()
            future.set_result(b"OK")
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "set",
            "--request-mode",
            "many",
            "--prebuild-keys",
            "--value-bytes",
            "5",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    value = b"x" * 5
    assert created[0].payloads == [
        bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, 1, 2)
        + binary(b"protocol-kv:0")
        + binary(value)
        + binary(b"protocol-kv:1")
        + binary(value),
        bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, 1, 2)
        + binary(b"protocol-kv:2")
        + binary(value)
        + binary(b"protocol-kv:3")
        + binary(value),
        bench._COMPACT_PIPELINE_HEADER.pack(bench._COMPACT_PIPELINE_REQUEST, 1, 1)
        + binary(b"protocol-kv:4")
        + binary(value),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_hset_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([1] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "hset",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--value-bytes",
            "5",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    value = b"x" * 5
    field = b"field"
    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HSET_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:hset:0")
            + binary(field)
            + binary(value)
            + binary(b"protocol-kv:hset:1")
            + binary(field)
            + binary(value),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HSET_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:hset:2")
            + binary(field)
            + binary(value)
            + binary(b"protocol-kv:hset:3")
            + binary(field)
            + binary(value),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HSET_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:hset:4")
            + binary(field)
            + binary(value),
            1,
        ),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_hget_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([b"value"] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "5",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "hget",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    field = b"field"
    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HGET_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:hget:0")
            + binary(field)
            + binary(b"protocol-kv:hget:1")
            + binary(field),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HGET_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:hget:2")
            + binary(field)
            + binary(b"protocol-kv:hget:3")
            + binary(field),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HGET_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:hget:4")
            + binary(field),
            1,
        ),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_hgetall_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([[b"field", b"value"]] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "3",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "hgetall",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HGETALL_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:hgetall:0")
            + binary(b"protocol-kv:hgetall:1"),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HGETALL_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:hgetall:2"),
            1,
        ),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_zscore_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([b"1"] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "3",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "zscore",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--value-bytes",
            "3",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    member = b"x" * 3
    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_ZSCORE_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:zscore:0")
            + binary(member)
            + binary(b"protocol-kv:zscore:1")
            + binary(member),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_ZSCORE_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:zscore:2")
            + binary(member),
            1,
        ),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_hmget_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([[b"value"]] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "3",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "hmget",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    field = binary(b"field")
    field_count = bench._COMPACT_U32.pack(1)
    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HMGET_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:hmget:0")
            + field_count
            + field
            + binary(b"protocol-kv:hmget:1")
            + field_count
            + field,
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_HMGET_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:hmget:2")
            + field_count
            + field,
            1,
        ),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_zrange_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([[b"member"]] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "3",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "zrange",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--range-start",
            "-2",
            "--range-stop",
            "-1",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    bounds = bench._COMPACT_I64.pack(-2) + bench._COMPACT_I64.pack(-1) + b"\x00"
    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_ZRANGE_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:zrange:0")
            + bounds
            + binary(b"protocol-kv:zrange:1")
            + bounds,
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_ZRANGE_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:zrange:2")
            + bounds,
            1,
        ),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_sadd_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([1] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "3",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "sadd",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--value-bytes",
            "3",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    member = b"x" * 3
    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_SADD_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:sadd:0")
            + binary(member)
            + binary(b"protocol-kv:sadd:1")
            + binary(member),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_SADD_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:sadd:2")
            + binary(member),
            1,
        ),
    ]


def test_protocol_kv_pipeline_mode_uses_preencoded_zadd_payload_when_available(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.payloads = []
            created.append(self)

        def submit_batch(self, commands):
            raise AssertionError(f"submit_batch should not be used: {commands!r}")

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([1] * count)
            return future

        def close(self):
            pass

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--requests",
            "3",
            "--pipeline",
            "2",
            "--clients",
            "1",
            "--threads",
            "1",
            "--command",
            "zadd",
            "--request-mode",
            "pipeline",
            "--prebuild-keys",
            "--value-bytes",
            "3",
            "--no-warmup",
        ]
    )

    result = bench.run_benchmark(args)

    assert result["prebuild_keys"] is True
    assert result["errors"] == 0

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    member = b"x" * 3
    assert created[0].payloads == [
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_ZADD_PIPELINE_MODE, 2
            )
            + binary(b"protocol-kv:zadd:0")
            + bench._COMPACT_F64.pack(0.0)
            + binary(member)
            + binary(b"protocol-kv:zadd:1")
            + bench._COMPACT_F64.pack(1.0)
            + binary(member),
            2,
        ),
        (
            bench._COMPACT_PIPELINE_HEADER.pack(
                bench._COMPACT_PIPELINE_REQUEST, 0x80 | bench._COMPACT_ZADD_PIPELINE_MODE, 1
            )
            + binary(b"protocol-kv:zadd:2")
            + bench._COMPACT_F64.pack(2.0)
            + binary(member),
            1,
        ),
    ]


def test_protocol_kv_warmup_uses_mset_chunks_not_measured_pipeline(monkeypatch):
    created = []

    class FakeAdapter:
        def __init__(self):
            self.commands = []
            self.closed = False
            created.append(self)

        def execute_command(self, *command):
            self.commands.append(command)
            return b"OK"

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        bench.ProtocolAdapter,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: FakeAdapter()),
    )

    args = bench.parse_args(
        [
            "--command",
            "get",
            "--requests",
            "1",
            "--pipeline",
            "10",
            "--key-count",
            "2500",
        ]
    )

    warmed = bench._warmup(args, b"x")

    assert warmed == 2500
    assert len(created[0].commands) == 3
    assert [command[0] for command in created[0].commands] == ["MSET", "MSET", "MSET"]
    assert [(len(command) - 1) // 2 for command in created[0].commands] == [1000, 1000, 500]
    assert created[0].closed is True
