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
                return [
                    [command[1], "p1", b"lease-start-1", 1]
                    for command in commands
                ]
            return [
                [command[1], "p1", b"lease-step-2", 2]
                for command in commands
            ]

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
    assert second_items == [
        {"id": "run:flow:2", "partition_key": "p2"}
    ]



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
