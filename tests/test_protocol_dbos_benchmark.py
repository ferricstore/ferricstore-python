import importlib.util
from pathlib import Path


_BENCH_PATH = Path(__file__).resolve().parents[1] / "examples" / "protocol_dbos_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("protocol_dbos_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_protocol_dbos_wrapper_uses_ferric_url_and_protocol_defaults():
    args = bench.parse_args([])
    command = bench.build_command(args)

    assert command[0].endswith("python") or "python" in command[0]
    assert "dbos_style_benchmark.py" in command[1]
    assert "--url" in command
    assert command[command.index("--url") + 1] == "ferric://127.0.0.1:6388"
    assert command[command.index("--workers") + 1] == "16"
    assert command[command.index("--producers") + 1] == "4"
    assert command[command.index("--claim-batch-size") + 1] == "500"
    assert command[command.index("--claim-partition-batch-size") + 1] == "16"
    assert command[command.index("--claim-drain-batches") + 1] == "2"
    assert command[command.index("--complete-async-depth") + 1] == "4"
    assert "--no-fuse-complete-claim" in command
    assert command[command.index("--retention-ttl-ms") + 1] == "0"
    assert command[command.index("--protocol-worker-connections") + 1] == "1"
    assert command[command.index("--protocol-lanes") + 1] == "32"
    assert command[command.index("--protocol-create-inflight-batches") + 1] == "2"
    assert command[command.index("--producer-max-pending-credits") + 1] == "0"
    assert command[command.index("--producer-target-queue-latency-ms") + 1] == "75.0"
    assert command[command.index("--producer-min-rate-per-sec") + 1] == "50000.0"
    assert command[command.index("--producer-max-rate-per-sec") + 1] == "0.0"
    assert "--claim-job-only" in command
    assert "--no-reclaim-expired" in command


def test_protocol_dbos_wrapper_allows_passthrough_args():
    args = bench.parse_args(["--flows", "10000", "--", "--claim-state", "any"])
    command = bench.build_command(args)

    assert command[command.index("--flows") + 1] == "10000"
    assert command[-2:] == ["--claim-state", "any"]


def test_protocol_dbos_wrapper_passes_retention_ttl():
    args = bench.parse_args(["--retention-ttl-ms", "60000"])
    command = bench.build_command(args)

    assert command[command.index("--retention-ttl-ms") + 1] == "60000"


def test_protocol_dbos_wrapper_can_enable_fused_complete_claim():
    args = bench.parse_args(["--fuse-complete-claim"])
    command = bench.build_command(args)

    assert "--fuse-complete-claim" in command
    assert "--no-fuse-complete-claim" not in command


def test_protocol_dbos_wrapper_passes_producer_backpressure_knobs():
    args = bench.parse_args(
        [
            "--producer-target-queue-latency-ms",
            "40",
            "--producer-min-rate-per-sec",
            "25000",
            "--producer-max-rate-per-sec",
            "120000",
        ]
    )
    command = bench.build_command(args)

    assert command[command.index("--producer-target-queue-latency-ms") + 1] == "40.0"
    assert command[command.index("--producer-min-rate-per-sec") + 1] == "25000.0"
    assert command[command.index("--producer-max-rate-per-sec") + 1] == "120000.0"
