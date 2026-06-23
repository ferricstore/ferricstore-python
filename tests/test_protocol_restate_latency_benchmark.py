from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

_BENCH_PATH = (
    Path(__file__).resolve().parents[1] / "examples" / "protocol_restate_latency_benchmark.py"
)
_SPEC = importlib.util.spec_from_file_location("protocol_restate_latency_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
sys.modules[_SPEC.name] = bench
_SPEC.loader.exec_module(bench)


class FakeClient:
    def __init__(self) -> None:
        self.calls = []
        self.fencing = 0

    def _record(self, state: str) -> SimpleNamespace:
        self.fencing += 1
        return SimpleNamespace(
            lease_token=f"lease-{self.fencing}".encode(),
            fencing_token=self.fencing,
            partition_key="p1",
            run_state=state,
        )

    def start_and_claim(self, id, **kwargs):
        self.calls.append(("start_and_claim", id, kwargs))
        return self._record(kwargs["initial_state"])

    def step_continue(self, id, **kwargs):
        self.calls.append(("step_continue", id, kwargs))
        return self._record(kwargs["to_state"])

    def complete(self, id, **kwargs):
        self.calls.append(("complete", id, kwargs))
        return b"OK"


def test_run_direct_workflow_executes_start_steps_and_terminal_complete():
    ticks = iter([1_000_000_000, 1_123_000_000])
    client = FakeClient()
    spec = bench.WorkflowSpec(
        run_id="run",
        flow_type="order",
        index=7,
        steps=3,
        partition_key="p1",
        worker="worker-1",
    )

    elapsed_ns = bench.run_direct_workflow(
        client,
        spec,
        payload=b"payload",
        result=b"ok",
        lease_ms=30_000,
        clock_ns=lambda: next(ticks),
    )

    assert elapsed_ns == 123_000_000
    assert [call[0] for call in client.calls] == [
        "start_and_claim",
        "step_continue",
        "step_continue",
        "complete",
    ]
    assert client.calls[0][2]["initial_state"] == "step_1"
    assert client.calls[1][2]["from_state"] == "step_1"
    assert client.calls[1][2]["to_state"] == "step_2"
    assert client.calls[2][2]["from_state"] == "step_2"
    assert client.calls[2][2]["to_state"] == "step_3"
    assert client.calls[3][2]["return_record"] is False


def test_workflow_spec_uses_auto_or_explicit_partitioning():
    auto = bench.workflow_spec(
        run_id="run",
        flow_type="order",
        index=5,
        steps=1,
        partitions=4,
        partition_mode="auto",
        worker_count=2,
    )
    explicit = bench.workflow_spec(
        run_id="run",
        flow_type="order",
        index=5,
        steps=1,
        partitions=4,
        partition_mode="explicit",
        worker_count=2,
    )

    assert auto.partition_key is None
    assert explicit.partition_key == "run:partition:1"
    assert auto.worker == "run:worker:1"


def test_run_wave_batch_batches_start_step_and_complete_jobs():
    class WaveClient:
        def __init__(self) -> None:
            self.batches = []
            self.completed = []

        def _execute_command_batch(self, commands):
            self.batches.append(commands)
            if commands[0][0] == "FLOW.START_AND_CLAIM":
                return [[command[1], "p1", b"lease-start-1", 1] for command in commands]
            return [[command[1], "p1", b"lease-step-2", 2] for command in commands]

        def complete_jobs(self, jobs, **kwargs):
            self.completed.append((jobs, kwargs))
            return b"OK"

    specs = [
        bench.WorkflowSpec("run", "order", 1, 2, "p1", "worker-1"),
        bench.WorkflowSpec("run", "order", 2, 2, "p1", "worker-2"),
    ]
    client = WaveClient()

    bench.run_wave_batch(client, specs, payload=None, result=b"ok", lease_ms=30_000)

    assert [batch[0][0] for batch in client.batches] == [
        "FLOW.START_AND_CLAIM",
        "FLOW.STEP_CONTINUE",
    ]
    assert "RETURN" in client.batches[0][0]
    assert "JOBS_COMPACT" in client.batches[0][0]
    assert "PARTITION" in client.batches[0][0]
    assert "PAYLOAD" not in client.batches[0][0]
    assert client.batches[1][0][3:5] == ("step_1", "step_2")
    assert "RETURN" in client.batches[1][0]
    assert "JOBS_COMPACT" in client.batches[1][0]
    jobs, kwargs = client.completed[0]
    assert [job.id for job in jobs] == ["run:flow:1", "run:flow:2"]
    assert kwargs["independent"] is True
    assert kwargs["return_ok_on_success"] is True


def test_predicted_pipeline_wave_batch_sends_one_ordered_chain(monkeypatch):
    class WaveClient:
        def __init__(self) -> None:
            self.batches = []

        def _execute_command_batch(self, commands):
            self.batches.append(commands)
            return [b"OK"] * len(commands)

    monkeypatch.setattr(bench.time, "time", lambda: 1000.0)
    specs = [
        bench.WorkflowSpec("run", "order", 1, 3, "p1", "worker-1"),
        bench.WorkflowSpec("run", "order", 2, 3, None, "worker-2"),
    ]
    client = WaveClient()

    bench.run_wave_batch(
        client,
        specs,
        payload=None,
        result=b"ok",
        lease_ms=30_000,
        chain_submit_mode="predicted-pipeline",
    )

    assert len(client.batches) == 1
    commands = client.batches[0]
    assert [command[0] for command in commands] == [
        "FLOW.START_AND_CLAIM",
        "FLOW.START_AND_CLAIM",
        "FLOW.STEP_CONTINUE",
        "FLOW.STEP_CONTINUE",
        "FLOW.STEP_CONTINUE",
        "FLOW.STEP_CONTINUE",
        "FLOW.COMPLETE",
        "FLOW.COMPLETE",
    ]
    assert commands[0][commands[0].index("PARTITION") + 1] == "p1"
    assert commands[1][commands[1].index("PARTITION") + 1].startswith("__flow_auto__:")
    assert commands[2][2] == b"worker-1:1000000:1"
    assert commands[2][commands[2].index("FENCING") + 1] == 1
    assert commands[4][2] == b"worker-1:1000001:2"
    assert commands[4][commands[4].index("FENCING") + 1] == 2
    complete1 = commands[-2]
    complete2 = commands[-1]
    assert complete1[:3] == ("FLOW.COMPLETE", "run:flow:1", b"worker-1:1000002:3")
    assert complete1[complete1.index("FENCING") + 1] == 3
    assert complete2[:3] == ("FLOW.COMPLETE", "run:flow:2", b"worker-2:1000002:3")
    assert complete2[complete2.index("FENCING") + 1] == 3


def test_run_steps_many_wave_batch_sends_one_server_chain_command(monkeypatch):
    class WaveClient:
        def __init__(self) -> None:
            self.commands = []

        def run_steps_many(self, items, **kwargs):
            self.commands.append((items, kwargs))
            return b"OK"

    monkeypatch.setattr(bench.time, "time", lambda: 1000.0)
    specs = [
        bench.WorkflowSpec("run", "order", 1, 3, "p1", "worker-1"),
        bench.WorkflowSpec("run", "order", 2, 3, None, "worker-1"),
    ]
    client = WaveClient()

    bench.run_wave_batch(
        client,
        specs,
        payload=b"payload",
        result=b"ok",
        lease_ms=30_000,
        chain_submit_mode="run-steps-many",
    )

    assert len(client.commands) == 1
    items, kwargs = client.commands[0]
    assert kwargs == {
        "type": "order",
        "states": ["step_1", "step_2", "step_3"],
        "worker": "worker-1",
        "lease_ms": 30_000,
        "now_ms": 1_000_000,
        "payload": b"payload",
        "result": b"ok",
    }
    assert items == [
        {"id": "run:flow:1", "partition_key": "p1"},
        {"id": "run:flow:2"},
    ]


def test_run_steps_many_wave_batch_splits_shared_command_shape(monkeypatch):
    class WaveClient:
        def __init__(self) -> None:
            self.commands = []

        def run_steps_many(self, items, **kwargs):
            self.commands.append((items, kwargs))
            return b"OK"

    monkeypatch.setattr(bench.time, "time", lambda: 1000.0)
    specs = [
        bench.WorkflowSpec("run", "order", 1, 3, "p1", "worker-1"),
        bench.WorkflowSpec("run", "order", 2, 3, "p2", "worker-2"),
        bench.WorkflowSpec("run", "order", 3, 3, "p3", "worker-1"),
    ]
    client = WaveClient()

    bench.run_wave_batch(
        client,
        specs,
        payload=None,
        result=None,
        lease_ms=30_000,
        chain_submit_mode="run-steps-many",
    )

    assert len(client.commands) == 2
    first, second = client.commands
    first_items, first_kwargs = first
    second_items, second_kwargs = second
    assert first_kwargs["worker"] == "worker-1"
    assert first_items == [
        {"id": "run:flow:1", "partition_key": "p1"},
        {"id": "run:flow:3", "partition_key": "p3"},
    ]
    assert second_kwargs["worker"] == "worker-2"
    assert second_items == [{"id": "run:flow:2", "partition_key": "p2"}]


def test_run_steps_many_auto_id_wave_batch_uses_plain_ids(monkeypatch):
    class WaveClient:
        def __init__(self) -> None:
            self.commands = []

        def run_steps_many(self, items, **kwargs):
            self.commands.append((items, kwargs))
            return b"OK"

    monkeypatch.setattr(bench.time, "time", lambda: 1000.0)
    client = WaveClient()

    bench.run_steps_many_auto_id_wave_batch(
        client,
        run_id="run",
        flow_type="order",
        start=10,
        count=3,
        steps=2,
        worker="worker-1",
        payload=None,
        result=b"ok",
        lease_ms=30_000,
    )

    assert client.commands == [
        (
            ["run:flow:10", "run:flow:11", "run:flow:12"],
            {
                "type": "order",
                "states": ["step_1", "step_2"],
                "worker": "worker-1",
                "lease_ms": 30_000,
                "now_ms": 1_000_000,
                "payload": None,
                "result": b"ok",
            },
        )
    ]


def test_next_no_catch_up_due_never_schedules_in_the_past():
    assert bench.next_no_catch_up_due_s(10.0, 0.5, 10.1) == 10.5
    assert bench.next_no_catch_up_due_s(10.0, 0.5, 11.0) == 11.0


def test_parse_shard_local_submit_concurrency():
    args = bench.parse_args(["--shard-local-submit-concurrency", "8"])
    assert args.shard_local_submit_concurrency == 8


def test_restate_high_load_profile_keeps_explicit_shard_local_submit_concurrency():
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--profile",
            "restate-high-load",
            "--shard-local-submit-concurrency",
            "4",
        ]
    )

    assert args.shard_local_submit_concurrency == 4


def test_restate_high_load_profile_keeps_explicit_chain_submit_mode():
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--profile",
            "restate-high-load",
            "--chain-submit-mode",
            "run-steps-many",
        ]
    )

    assert args.chain_submit_mode == "run-steps-many"


def test_run_steps_many_auto_shard_local_wave_batch_groups_by_server_shard(monkeypatch):
    class WaveClient:
        def __init__(self) -> None:
            self.commands = []

        def run_steps_many(self, items, **kwargs):
            self.commands.append((items, kwargs))
            return b"OK"

    monkeypatch.setattr(bench.time, "time", lambda: 1000.0)
    monotonic_ticks = iter(range(0, 100_000_000, 1_000_000))
    monkeypatch.setattr(bench.time, "perf_counter_ns", lambda: next(monotonic_ticks))
    client = WaveClient()

    samples = bench.run_steps_many_auto_shard_local_wave_batches(
        client,
        run_id="run",
        flow_type="order",
        start=10,
        count=24,
        steps=2,
        worker="worker-1",
        payload=None,
        result=b"ok",
        lease_ms=30_000,
        server_shards=4,
    )

    assert sum(sample[1] for sample in samples) == 24
    assert sum(len(items) for items, _kwargs in client.commands) == 24
    for items, kwargs in client.commands:
        assert kwargs == {
            "type": "order",
            "states": ["step_1", "step_2"],
            "worker": "worker-1",
            "lease_ms": 30_000,
            "now_ms": 1_000_000,
            "payload": None,
            "result": b"ok",
        }
        shards = {
            bench._auto_partition_server_shard(
                bench._auto_partition_index_for_id(item["id"]),
                4,
            )
            for item in items
        }
        assert len(shards) == 1
        for item in items:
            partition_index = bench._auto_partition_index_for_id(item["id"])
            assert item["partition_key"] == bench._auto_partition_key(partition_index)


def test_run_steps_many_auto_shard_local_wave_batch_can_trace_sub_batches(monkeypatch):
    class TraceExecutor:
        def __init__(self) -> None:
            self.commands = []

        def execute_command_with_trace(self, *args):
            self.commands.append(args)
            return {
                "value": b"OK",
                "trace": {
                    "server": {"server_ra_wait_us": 123},
                    "client": {"request_total_us": 456},
                },
            }

    class WaveClient:
        codec = object()
        _run_steps_many_items = staticmethod(bench.FlowClient._run_steps_many_items)

        def __init__(self) -> None:
            self.executor = TraceExecutor()

        def run_steps_many(self, *_args, **_kwargs):
            raise AssertionError("trace path should use execute_command_with_trace")

    samples = []
    monotonic_ticks = iter(range(0, 100_000_000, 1_000_000))
    monkeypatch.setattr(bench.time, "perf_counter_ns", lambda: next(monotonic_ticks))
    monkeypatch.setattr(bench.time, "time", lambda: 1000.0)
    client = WaveClient()

    result = bench.run_steps_many_auto_shard_local_wave_batches(
        client,
        run_id="run",
        flow_type="order",
        start=0,
        count=8,
        steps=1,
        worker="worker-1",
        payload=None,
        result=None,
        lease_ms=30_000,
        server_shards=4,
        trace_recorder=samples.append,
    )

    assert sum(count for _start, count, _elapsed in result) == 8
    assert len(samples) == len(client.executor.commands)
    assert all(command[0] == "FLOW.RUN_STEPS_MANY" for command in client.executor.commands)
    assert all(sample["trace"]["server"]["server_ra_wait_us"] == 123 for sample in samples)
    assert all(sample["trace"]["client"]["request_total_us"] == 456 for sample in samples)


def test_wave_benchmark_uses_auto_id_fast_path_for_run_steps_many(monkeypatch):
    calls = []
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--workflows",
            "3",
            "--batch-size",
            "2",
            "--inflight-batches",
            "1",
            "--chain-submit-mode",
            "run-steps-many",
            "--partition-mode",
            "auto",
        ]
    )

    monkeypatch.setattr(
        bench,
        "run_steps_many_auto_id_wave_batch",
        lambda *positional, **kwargs: calls.append((positional, kwargs)),
    )
    monkeypatch.setattr(
        bench,
        "run_wave_batch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("slow path used")),
    )

    completed, errors, latencies, service_latencies = bench.run_wave_benchmark(
        args,
        run_id="run",
        flow_type="type",
        payload=None,
        result=None,
        client=object(),
    )

    assert completed == 3
    assert errors == 0
    assert len(latencies) == 3
    assert len(service_latencies) == 3
    assert [call[1]["start"] for call in calls] == [0, 2]
    assert [call[1]["count"] for call in calls] == [2, 1]


def test_wave_benchmark_uses_shard_local_fast_path_for_run_steps_many(monkeypatch):
    calls = []
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--workflows",
            "3",
            "--batch-size",
            "2",
            "--inflight-batches",
            "1",
            "--chain-submit-mode",
            "run-steps-many-shard-local",
            "--partition-mode",
            "auto",
        ]
    )

    monkeypatch.setattr(
        bench,
        "run_steps_many_auto_shard_local_wave_batches",
        lambda *positional, **kwargs: (
            calls.append((positional, kwargs)) or [(kwargs["start"], kwargs["count"], 1_000_000)]
        ),
    )
    monkeypatch.setattr(
        bench,
        "run_steps_many_auto_id_wave_batch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("plain fast path used")),
    )
    monkeypatch.setattr(
        bench,
        "run_wave_batch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("slow path used")),
    )

    completed, errors, latencies, service_latencies = bench.run_wave_benchmark(
        args,
        run_id="run",
        flow_type="type",
        payload=None,
        result=None,
        client=object(),
    )

    assert completed == 3
    assert errors == 0
    assert len(latencies) == 3
    assert len(service_latencies) == 3
    assert [call[1]["start"] for call in calls] == [0, 2]
    assert [call[1]["count"] for call in calls] == [2, 1]
    assert all(call[1]["server_shards"] == args.partitions for call in calls)


def test_wave_benchmark_records_slowest_waves_when_enabled(monkeypatch):
    perf_ticks = iter(
        [
            0,
            10_000_000,
            10_000_000,
            50_000_000,
            50_000_000,
            70_000_000,
        ]
    )
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--workflows",
            "6",
            "--batch-size",
            "2",
            "--inflight-batches",
            "1",
            "--chain-submit-mode",
            "run-steps-many",
            "--partition-mode",
            "auto",
            "--slow-wave-count",
            "2",
        ]
    )

    monkeypatch.setattr(bench.time, "perf_counter_ns", lambda: next(perf_ticks))
    monkeypatch.setattr(bench, "run_steps_many_auto_id_wave_batch", lambda *_, **__: None)

    completed, errors, latencies, _service_latencies = bench.run_wave_benchmark(
        args,
        run_id="run",
        flow_type="type",
        payload=None,
        result=None,
        client=object(),
    )
    diagnostics = bench.wave_diagnostics_result(args)

    assert completed == 6
    assert errors == 0
    assert latencies == [10_000_000, 10_000_000, 40_000_000, 40_000_000, 20_000_000, 20_000_000]
    assert diagnostics["slow_wave_count"] == 2
    assert diagnostics["slow_waves"] == [
        {"start": 2, "count": 2, "latency_ms": 40.0, "service_latency_ms": 20.0},
        {"start": 4, "count": 2, "latency_ms": 20.0, "service_latency_ms": 10.0},
    ]


def test_wave_benchmark_records_target_schedule_lag(monkeypatch):
    perf_values = iter(
        [
            100.0,
            100.0,
            100.002,
            100.004,
            100.006,
            100.008,
            100.010,
            100.012,
        ]
    )
    perf_ns_values = iter([0, 1_000_000, 1_000_000, 2_000_000])
    sleeps = []
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--workflows",
            "2",
            "--batch-size",
            "1",
            "--inflight-batches",
            "1",
            "--target-rps",
            "1000",
            "--chain-submit-mode",
            "run-steps-many",
            "--slow-wave-count",
            "1",
        ]
    )

    monkeypatch.setattr(bench.time, "perf_counter", lambda: next(perf_values))
    monkeypatch.setattr(bench.time, "perf_counter_ns", lambda: next(perf_ns_values))
    monkeypatch.setattr(bench.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(bench, "run_steps_many_auto_id_wave_batch", lambda *_, **__: None)

    bench.run_wave_benchmark(
        args,
        run_id="run",
        flow_type="type",
        payload=None,
        result=None,
        client=object(),
    )
    diagnostics = bench.wave_diagnostics_result(args)

    assert diagnostics["schedule_lag_max_ms"] > 0
    assert diagnostics["schedule_lag_p99_ms"] > 0


def test_restate_targets_encode_public_high_load_thresholds():
    assert bench.RESTATE_HIGH_LOAD_TARGETS[1]["rps"] == 23_131.0
    assert bench.RESTATE_HIGH_LOAD_TARGETS[1]["p99_ms"] == 40.0
    assert bench.RESTATE_HIGH_LOAD_TARGETS[3]["p99_ms"] == 98.0
    assert bench.RESTATE_HIGH_LOAD_TARGETS[9]["p99_ms"] == 163.0


def test_restate_targets_encode_public_low_load_thresholds():
    assert bench.RESTATE_LOW_LOAD_TARGETS[3]["rps"] == 549.0
    assert bench.RESTATE_LOW_LOAD_TARGETS[3]["p50_ms"] == 15.0
    assert bench.RESTATE_LOW_LOAD_TARGETS[3]["p99_ms"] == 69.0
    assert bench.RESTATE_LOW_LOAD_TARGETS[9]["rps"] == 303.0
    assert bench.RESTATE_LOW_LOAD_TARGETS[9]["p50_ms"] == 31.0
    assert bench.RESTATE_LOW_LOAD_TARGETS[9]["p99_ms"] == 93.0


def test_result_reports_low_load_pass_flags(monkeypatch):
    args = bench.parse_args(
        [
            "--steps",
            "3",
            "--workflows",
            "2",
            "--warmup-workflows",
            "0",
            "--target-rps",
            "549",
            "--chain-submit-mode",
            "run-steps-many",
        ]
    )

    monkeypatch.setattr(
        bench,
        "run_wave_benchmark",
        lambda *_args, **_kwargs: (2, 0, [1_000_000, 2_000_000], [500_000, 1_000_000]),
    )
    monkeypatch.setattr(bench, "make_client", lambda *_args, **_kwargs: object())

    result = bench.run_benchmark(args)

    assert result["beats_restate_low_load_latency_only"] is True
    assert result["beats_restate_low_load_p99"] is True
    assert result["beats_restate_low_load_p50_latency"] is True
    assert result["beats_restate_low_load_p90_latency"] is None
    assert result["beats_restate_low_load_p99_latency"] is True
    assert result["beats_restate_low_load_all_latency"] is True
    assert result["beats_restate_low_load_all"] is True
    assert result["latency_p50_ms"] == 1.0
    assert result["workflow_service_latency_p50_ms"] == 0.5


def test_result_reports_high_load_full_latency_failure(monkeypatch):
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--workflows",
            "2",
            "--warmup-workflows",
            "0",
            "--chain-submit-mode",
            "run-steps-many",
        ]
    )

    monkeypatch.setattr(
        bench,
        "run_wave_benchmark",
        lambda *_args, **_kwargs: (2, 0, [30_000_000, 35_000_000], [166_000, 194_000]),
    )
    monkeypatch.setattr(bench, "make_client", lambda *_args, **_kwargs: object())

    result = bench.run_benchmark(args)

    assert result["beats_restate_high_load_p50_latency"] is False
    assert result["beats_restate_high_load_p90_latency"] is None
    assert result["beats_restate_high_load_p99_latency"] is True
    assert result["beats_restate_high_load_all_latency"] is False
    assert result["beats_restate_high_load_all"] is False


def test_run_steps_many_reports_one_durable_command_per_workflow():
    args = bench.parse_args(["--steps", "3", "--chain-submit-mode", "run-steps-many"])
    assert bench.durable_commands_per_workflow_for(args) == 1


def test_sequential_reports_step_plus_terminal_durable_commands_per_workflow():
    args = bench.parse_args(["--steps", "3", "--chain-submit-mode", "sequential"])
    assert bench.durable_commands_per_workflow_for(args) == 4


def test_parse_args_accepts_wave_inflight_batches():
    args = bench.parse_args(
        [
            "--execution-mode",
            "wave",
            "--inflight-batches",
            "4",
            "--warmup-workflows",
            "100",
            "--target-rps",
            "1234.5",
            "--chain-submit-mode",
            "run-steps-many",
        ]
    )

    assert args.execution_mode == "wave"
    assert args.inflight_batches == 4
    assert args.warmup_workflows == 100
    assert args.target_rps == 1234.5
    assert args.chain_submit_mode == "run-steps-many"


def test_restate_high_load_profile_applies_step_specific_defaults():
    one_step = bench.parse_args(["--steps", "1", "--profile", "restate-high-load"])
    three_step = bench.parse_args(["--steps", "3", "--profile", "restate-high-load"])
    nine_step = bench.parse_args(["--steps", "9", "--profile", "restate-high-load"])

    assert (one_step.batch_size, one_step.inflight_batches) == (250, 4)
    assert (three_step.batch_size, three_step.inflight_batches) == (500, 4)
    assert (nine_step.batch_size, nine_step.inflight_batches) == (500, 1)
    assert one_step.shard_local_submit_concurrency == 8
    assert three_step.shard_local_submit_concurrency == 0
    assert nine_step.shard_local_submit_concurrency == 0
    assert one_step.chain_submit_mode == "run-steps-many-shard-local"
    assert three_step.chain_submit_mode == "run-steps-many"
    assert nine_step.chain_submit_mode == "run-steps-many"
    assert one_step.target_rps == 0.0
    assert three_step.target_rps == 0.0
    assert nine_step.target_rps == 0.0


def test_restate_latency_default_arguments_are_saved_in_profile_map():
    args = bench.parse_args([])

    assert args.profile == bench.RESTATE_LATENCY_DEFAULT_ARGS["profile"]
    assert args.workflows == bench.RESTATE_LATENCY_DEFAULT_ARGS["workflows"]
    assert args.steps == bench.RESTATE_LATENCY_DEFAULT_ARGS["steps"]
    assert args.protocol_connections == bench.RESTATE_LATENCY_DEFAULT_ARGS["protocol_connections"]
    assert args.protocol_lanes == bench.RESTATE_LATENCY_DEFAULT_ARGS["protocol_lanes"]
    assert args.execution_mode == bench.RESTATE_LATENCY_DEFAULT_ARGS["execution_mode"]


def test_restate_high_load_profile_keeps_explicit_batch_controls():
    args = bench.parse_args(
        [
            "--steps",
            "1",
            "--profile",
            "restate-high-load",
            "--batch-size",
            "320",
            "--inflight-batches",
            "2",
            "--target-rps",
            "1000",
        ]
    )

    assert args.batch_size == 320
    assert args.inflight_batches == 2
    assert args.target_rps == 1000


def test_run_benchmark_reports_profile_and_startup_settle(monkeypatch):
    sleeps = []
    args = bench.parse_args(
        [
            "--steps",
            "3",
            "--workflows",
            "2",
            "--warmup-workflows",
            "0",
            "--profile",
            "restate-high-load",
            "--chain-submit-mode",
            "run-steps-many",
            "--startup-settle-seconds",
            "0.25",
        ]
    )

    monkeypatch.setattr(bench.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        bench,
        "run_wave_benchmark",
        lambda *_args, **_kwargs: (2, 0, [1_000_000, 2_000_000], [500_000, 1_000_000]),
    )
    monkeypatch.setattr(bench, "make_client", lambda *_args, **_kwargs: object())

    result = bench.run_benchmark(args)

    assert sleeps == [0.25]
    assert result["profile"] == "restate-high-load"
    assert result["batch_size"] == 500
    assert result["inflight_batches"] == 4
    assert result["shard_local_submit_concurrency"] == 0
    assert result["startup_settle_seconds"] == 0.25


def test_readiness_probes_use_public_run_steps_many_path(monkeypatch):
    class ProbeClient:
        def __init__(self) -> None:
            self.calls = []

        def run_steps_many(self, items, **kwargs):
            self.calls.append((items, kwargs))
            return b"OK"

    monkeypatch.setattr(bench.time, "time", lambda: 1000.0)
    args = bench.parse_args(
        [
            "--readiness-probes",
            "3",
            "--partition-mode",
            "explicit",
            "--partitions",
            "2",
        ]
    )
    client = ProbeClient()

    completed = bench.run_readiness_probes(
        client,
        args,
        run_id="run",
        flow_type="type",
    )

    assert completed == 3
    assert len(client.calls) == 3
    first_items, first_kwargs = client.calls[0]
    third_items, third_kwargs = client.calls[2]
    assert first_items == [{"id": "run:readiness:0", "partition_key": "run:partition:0"}]
    assert third_items == [{"id": "run:readiness:2", "partition_key": "run:partition:0"}]
    assert first_kwargs["type"] == "type:readiness"
    assert first_kwargs["states"] == ["step_1"]
    assert first_kwargs["worker"] == "run:readiness-worker"
    assert first_kwargs["now_ms"] == 1_000_000
    assert third_kwargs["now_ms"] == 1_000_002


def test_verify_sampled_results_checks_terminal_completed_state():
    class VerifyClient:
        def __init__(self) -> None:
            self.calls = []

        def get(self, id, **kwargs):
            self.calls.append((id, kwargs))
            return SimpleNamespace(type="type", state="completed", version=4)

    args = bench.parse_args(
        [
            "--workflows",
            "101",
            "--steps",
            "3",
            "--batch-size",
            "10",
            "--partition-mode",
            "explicit",
            "--partitions",
            "4",
            "--verify-sample",
            "3",
        ]
    )
    client = VerifyClient()

    result = bench.verify_sampled_results(
        client,
        args,
        run_id="run",
        flow_type="type",
    )

    assert result == {"requested": 3, "checked": 3, "errors": 0}
    assert client.calls == [
        ("run:flow:0", {"partition_key": "run:partition:0"}),
        ("run:flow:50", {"partition_key": "run:partition:1"}),
        ("run:flow:100", {"partition_key": "run:partition:2"}),
    ]


def test_verify_sampled_results_fails_on_incomplete_state():
    class VerifyClient:
        def get(self, id, **kwargs):
            return SimpleNamespace(type="type", state="step_3", version=3)

    args = bench.parse_args(
        [
            "--workflows",
            "10",
            "--steps",
            "3",
            "--verify-sample",
            "1",
            "--no-stop-on-error",
        ]
    )

    result = bench.verify_sampled_results(
        VerifyClient(),
        args,
        run_id="run",
        flow_type="type",
    )

    assert result == {"requested": 1, "checked": 1, "errors": 1}
