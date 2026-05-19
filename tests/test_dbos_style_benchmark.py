from argparse import Namespace
import importlib.util
from pathlib import Path


_BENCH_PATH = Path(__file__).resolve().parents[1] / "examples" / "dbos_style_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("dbos_style_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_queue_api_passes_claim_drain_batches_to_worker(monkeypatch):
    seen_drain_batches = []

    def fake_create_flows(**kwargs):
        return {
            "created": len(kwargs["indices"]),
            "create_pipeline_flushes": 0,
            "create_pipeline_commands": 0,
            "create_pipeline_max_depth": 0,
        }

    def fake_queue_worker(**kwargs):
        seen_drain_batches.append(kwargs.get("claim_drain_batches"))
        if kwargs["worker_index"] == 0:
            with kwargs["completed_lock"]:
                kwargs["claimed_total"][0] = kwargs["total_flows"]
                kwargs["completed"][0] = kwargs["total_flows"]
            completed = kwargs["total_flows"]
        else:
            completed = 0
        return {
            "completed": completed,
            "duplicate_completions": 0,
            "claim_calls": 1 if completed else 0,
            "empty_claims": 0,
            "claimed_items": completed,
            "max_claim_batch": completed,
            "fallback_claims": 0,
            "worker_capacity": 0,
            "wake_coalesce_sleeps": 0,
            "wake_coalesce_ms": 0.0,
            "process_pipeline_flushes": 0,
            "process_pipeline_commands": 0,
            "process_pipeline_max_depth": 0,
        }

    monkeypatch.setattr(bench, "create_flows", fake_create_flows)
    monkeypatch.setattr(bench, "run_queue_api_worker", fake_queue_worker)

    bench.run_queued_throughput(
        Namespace(
            url="redis://127.0.0.1:7379",
            queued_shape="live",
            flows=4,
            workers=2,
            producers=1,
            partitions=16,
            claim_any=False,
            partition_mode="auto",
            worker_capacity=0,
            worker_mode="polling",
            worker_api="queue",
            create_batch_size=4,
            claim_batch_size=500,
            claim_drain_batches=3,
            transport="many",
            payload_bytes=0,
            result_bytes=0,
            work_command="none",
            idle_sleep_ms=10.0,
            max_idle_sleep_ms=50.0,
            wake_coalesce_ms=5.0,
            partial_claim_retries=1,
            partial_claim_delay_ms=1.0,
            reclaim_expired=False,
            reclaim_ratio=25,
            claim_priority=0,
            claim_state="queued",
            claim_states=None,
            claim_job_only=True,
            complete_batch=True,
            complete_async_depth=0,
            independent_many=True,
            track_duplicates=False,
            claim_partition_batch_size=2,
            server_shards=16,
        )
    )

    assert seen_drain_batches == [3, 3]
