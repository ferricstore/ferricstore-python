import importlib.util
from concurrent.futures import Future
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

_BENCH_PATH = Path(__file__).resolve().parents[1] / "examples" / "dbos_style_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("dbos_style_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_dbos_queue_defaults_use_measured_claim_grouping():
    assert bench.DBOS_QUEUE_DEFAULTS["workers"] == 1
    assert bench.DBOS_QUEUE_DEFAULTS["worker_api"] == "queue"
    assert bench.DBOS_QUEUE_DEFAULTS["claim_partition_batch_size"] == 16
    assert bench.DBOS_QUEUE_DEFAULTS["claim_drain_batches"] == 2
    assert bench.DBOS_QUEUE_DEFAULTS["claim_prefetch"] == 0
    assert bench.DBOS_QUEUE_DEFAULTS["fuse_complete_claim"] is False
    assert bench.DBOS_QUEUE_DEFAULTS["protocol_wake_hints"] is False
    assert bench.DBOS_QUEUE_DEFAULTS["protocol_worker_connections"] == 1
    assert bench.DBOS_QUEUE_DEFAULTS["protocol_lanes"] == 32
    assert bench.DBOS_QUEUE_DEFAULTS["protocol_create_inflight_batches"] == 2
    assert bench.DBOS_QUEUE_DEFAULTS["latency_sample_rate"] == 100
    assert bench.DBOS_QUEUE_DEFAULTS["adaptive_producer_backpressure"] is True
    assert bench.DBOS_QUEUE_DEFAULTS["producer_target_queue_latency_ms"] == 75.0
    assert bench.DBOS_QUEUE_DEFAULTS["producer_max_pending_credits"] == 0


def test_queue_latency_recorder_samples_create_ack_to_claim_start(monkeypatch):
    ticks = iter([1_000_000_000, 1_007_500_000])
    monkeypatch.setattr(bench.time, "perf_counter_ns", lambda: next(ticks))

    recorder = bench.QueueLatencyRecorder(sample_rate=2)
    recorder.mark_created_indices("run", [0, 1, 2])
    recorder.mark_claimed(
        [
            SimpleNamespace(id="run:flow:0"),
            SimpleNamespace(id="run:flow:1"),
            SimpleNamespace(id="run:flow:2"),
        ]
    )

    summary = recorder.summary()
    assert summary["queue_latency_tracked"] == 2
    assert summary["queue_latency_pending"] == 0
    assert summary["queue_latency_sample_count"] == 2
    assert summary["queue_latency_avg_ms"] == 7.5
    assert summary["queue_latency_p50_ms"] == 7.5
    assert summary["queue_latency_p99_ms"] == 7.5


def test_queue_latency_recorder_notifies_adaptive_backpressure_observer(monkeypatch):
    ticks = iter([1_000_000_000, 1_050_000_000])
    monkeypatch.setattr(bench.time, "perf_counter_ns", lambda: next(ticks))
    seen = []

    recorder = bench.QueueLatencyRecorder(sample_rate=1)
    recorder.add_observer(seen.append)
    recorder.mark_created_indices("run", [1])
    recorder.mark_claimed([SimpleNamespace(id="run:flow:1")])

    assert seen == [[50.0]]


def test_adaptive_producer_backpressure_limits_after_high_queue_latency():
    controller = bench.AdaptiveProducerBackpressure(
        enabled=True,
        target_queue_latency_ms=10.0,
        min_rate_per_sec=100.0,
        max_rate_per_sec=1_000.0,
    )

    controller.observe_queue_latencies([100.0])

    summary = controller.summary()
    assert summary["producer_backpressure_rate_per_sec"] == 800.0
    assert summary["producer_backpressure_limited_batches"] == 1


def test_adaptive_producer_backpressure_unlimited_max_releases_after_recovery():
    controller = bench.AdaptiveProducerBackpressure(
        enabled=True,
        target_queue_latency_ms=10.0,
        min_rate_per_sec=50_000.0,
        max_rate_per_sec=0.0,
    )

    controller.observe_queue_latencies([100.0])

    for _ in range(80):
        controller.observe_queue_latencies([1.0])

    summary = controller.summary()
    assert summary["producer_backpressure_rate_per_sec"] == 0.0
    assert summary["producer_backpressure_limited_batches"] >= 1


def test_adaptive_producer_backpressure_waits_for_pending_claim_credits(monkeypatch):
    sleeps = []
    pending = iter([10, 9, 5])
    monkeypatch.setattr(bench.time, "sleep", sleeps.append)
    controller = bench.AdaptiveProducerBackpressure(
        enabled=True,
        target_queue_latency_ms=10.0,
        min_rate_per_sec=100.0,
        max_rate_per_sec=0.0,
    )

    controller.wait_for_pending_credits(lambda: next(pending), max_pending_credits=5)

    assert sleeps == [0.001, 0.001]
    assert controller.summary()["producer_backpressure_waits"] == 2


def test_create_flows_waits_on_adaptive_backpressure(monkeypatch):
    waited = []

    class FakeBackpressure:
        def wait_for_creates(self, count):
            waited.append(count)

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def enqueue_many_future(self, **_kwargs):
            return None

        def enqueue_many(self, **kwargs):
            return len(kwargs["indices"])

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    result = bench.create_flows(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        indices=[0, 1, 2, 3],
        partitions=2,
        create_batch_size=2,
        payload=b"",
        transport="many",
        partition_mode="explicit",
        independent_many=True,
        wake_coordinator=None,
        producer_backpressure=FakeBackpressure(),
    )

    assert result["created"] == 4
    assert waited == [2, 2]


def test_create_flows_waits_on_pending_claim_credits(monkeypatch):
    waits = []

    class FakeBackpressure:
        def wait_for_pending_credits(self, pending_credits, *, max_pending_credits):
            waits.append((pending_credits(), max_pending_credits))

        def wait_for_creates(self, count):
            pass

    class FakeWakeCoordinator:
        def total_credit(self):
            return 8

        def notify_partition(self, _partition_index, _count):
            pass

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def enqueue_many_future(self, **_kwargs):
            return None

        def enqueue_many(self, **kwargs):
            return len(kwargs["indices"])

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    result = bench.create_flows(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        indices=[0, 1, 2, 3],
        partitions=2,
        create_batch_size=2,
        payload=b"",
        transport="many",
        partition_mode="explicit",
        independent_many=True,
        wake_coordinator=FakeWakeCoordinator(),
        producer_backpressure=FakeBackpressure(),
        max_pending_claim_credits=5,
    )

    assert result["created"] == 4
    assert waits == [(8, 5), (8, 5)]


def test_create_flows_uses_sync_create_when_protocol_inflight_is_one(monkeypatch):
    calls = []

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def enqueue_many_future(self, **_kwargs):
            raise AssertionError("inflight=1 should use the sync create path")

        def enqueue_many(self, **kwargs):
            calls.append(kwargs["indices"])
            return len(kwargs["indices"])

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    result = bench.create_flows(
        url="ferric://127.0.0.1:6388",
        run_id="run",
        flow_type="email",
        indices=[0, 1],
        partitions=16,
        create_batch_size=2,
        payload=b"",
        transport="many",
        partition_mode="auto",
        independent_many=True,
        wake_coordinator=None,
        protocol_create_inflight_batches=1,
    )

    assert result["created"] == 2
    assert calls == [[0], [1]]


def test_create_flows_passes_retention_ttl(monkeypatch):
    seen = []

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def enqueue_many_future(self, **_kwargs):
            return None

        def enqueue_many(self, **kwargs):
            seen.append(kwargs)
            return len(kwargs["indices"])

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    result = bench.create_flows(
        url="ferric://127.0.0.1:6388",
        run_id="run",
        flow_type="email",
        indices=[0, 1],
        partitions=16,
        create_batch_size=2,
        payload=b"",
        transport="many",
        partition_mode="auto",
        independent_many=True,
        wake_coordinator=None,
        retention_ttl_ms=60_000,
    )

    assert result["created"] == 2
    assert seen[0]["retention_ttl_ms"] == 60_000


def test_create_flows_uses_bounded_async_protocol_creates(monkeypatch):
    submitted = []
    sync_calls = []

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def enqueue_many_future(self, **kwargs):
            submitted.append(kwargs)
            future = Future()
            future.set_result(len(kwargs["indices"]))
            return future

        def enqueue_many(self, **kwargs):
            sync_calls.append(kwargs)
            return len(kwargs["indices"])

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    result = bench.create_flows(
        url="ferric://127.0.0.1:6388",
        run_id="run",
        flow_type="email",
        indices=[0, 1, 2, 3],
        partitions=16,
        create_batch_size=1,
        payload=b"",
        transport="many",
        partition_mode="auto",
        independent_many=True,
        wake_coordinator=None,
        protocol_create_inflight_batches=2,
    )

    assert result["created"] == 4
    assert len(submitted) == 4
    assert sync_calls == []


def test_bench_flow_client_uses_enqueue_many_for_explicit_partition_items(monkeypatch):
    calls = []

    class FakeFlowClient:
        executor = SimpleNamespace(client=None)

        @classmethod
        def from_url(cls, *_args, **_kwargs):
            return cls()

        def enqueue_many(self, items, **kwargs):
            calls.append(("enqueue_many", items, kwargs))
            return [b"OK"] * len(items)

        def create_many(self, *_args, **_kwargs):
            raise AssertionError("explicit queue benchmark must not use mixed create_many")

    monkeypatch.setattr(bench, "FlowClient", FakeFlowClient)

    client = bench.BenchFlowClient("ferric://127.0.0.1:6388", "many", 100)
    created = client.enqueue_many(
        run_id="run",
        flow_type="email",
        indices=[0, 1, 2],
        partitions=2,
        payload=b"",
        partition_mode="explicit",
        independent_many=True,
    )

    assert created == 3
    assert len(calls) == 1
    command, items, kwargs = calls[0]
    assert command == "enqueue_many"
    assert [item.partition_key for item in items] == [
        "run:partition:0",
        "run:partition:1",
        "run:partition:0",
    ]
    assert kwargs["type"] == "email"
    assert kwargs["return_ok_on_success"] is True


def test_protocol_url_detection_accepts_ferric_scheme():
    assert bench.is_protocol_url("ferric://127.0.0.1:6388")
    assert bench.is_protocol_url("ferrics://example:6388")
    assert not bench.is_protocol_url("redis://127.0.0.1:6379")


def test_protocol_queue_default_uses_internal_worker_lanes():
    assert (
        bench.protocol_queue_worker_lanes(
            url="ferric://127.0.0.1:6388",
            worker_api="queue",
            workers=1,
            claim_any=False,
            partitions=16,
            server_shards=16,
        )
        == 16
    )
    assert (
        bench.protocol_queue_worker_lanes(
            url="redis://127.0.0.1:6379",
            worker_api="queue",
            workers=1,
            claim_any=False,
            partitions=16,
            server_shards=16,
        )
        == 1
    )


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

    result = bench.run_queued_throughput(
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
            claim_block_ms=5000,
            claim_drain_block_ms=50,
            complete_batch=True,
            complete_async_depth=0,
            independent_many=True,
            complete_independent_many=False,
            track_duplicates=False,
            claim_partition_batch_size=2,
            server_shards=16,
            claim_prefetch=8,
        )
    )

    assert seen_drain_batches == [3, 3]
    assert result["claim_prefetch"] == 8
    assert result["effective_claim_prefetch"] == 0


def test_lowlevel_blocking_auto_many_enables_wake_coordinator(monkeypatch):
    seen_wake_coordinators = []

    def fake_create_flows(**kwargs):
        assert kwargs["wake_coordinator"] is not None
        kwargs["wake_coordinator"].notify_partition(0, len(kwargs["indices"]))
        return {
            "created": len(kwargs["indices"]),
            "create_pipeline_flushes": 0,
            "create_pipeline_commands": 0,
            "create_pipeline_max_depth": 0,
        }

    def fake_worker(**kwargs):
        seen_wake_coordinators.append(kwargs["wake_coordinator"])
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
    monkeypatch.setattr(bench, "run_claim_worker", fake_worker)

    result = bench.run_queued_throughput(
        Namespace(
            url="redis://127.0.0.1:7379",
            queued_shape="live",
            flows=8,
            workers=2,
            producers=1,
            partitions=16,
            claim_any=False,
            partition_mode="auto",
            worker_capacity=0,
            worker_mode="blocking",
            worker_api="lowlevel",
            create_batch_size=4,
            claim_batch_size=500,
            claim_drain_batches=1,
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
            claim_block_ms=5000,
            claim_drain_block_ms=50,
            complete_batch=True,
            complete_async_depth=0,
            independent_many=True,
            complete_independent_many=False,
            track_duplicates=False,
            claim_partition_batch_size=2,
            server_shards=2,
        )
    )

    assert all(coordinator is not None for coordinator in seen_wake_coordinators)
    assert result["wake_notifications"] > 0
    assert result["wake_credits"] == 8


def test_queue_api_auto_many_enables_wake_coordinator(monkeypatch):
    seen_wake_coordinators = []

    def fake_create_flows(**kwargs):
        assert kwargs["wake_coordinator"] is not None
        kwargs["wake_coordinator"].notify_partition(0, len(kwargs["indices"]))
        return {
            "created": len(kwargs["indices"]),
            "create_pipeline_flushes": 0,
            "create_pipeline_commands": 0,
            "create_pipeline_max_depth": 0,
        }

    def fake_queue_worker(**kwargs):
        seen_wake_coordinators.append(kwargs["wake_coordinator"])
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

    result = bench.run_queued_throughput(
        Namespace(
            url="redis://127.0.0.1:7379",
            queued_shape="live",
            flows=8,
            workers=2,
            producers=1,
            partitions=16,
            claim_any=False,
            partition_mode="auto",
            worker_capacity=0,
            worker_mode="blocking",
            worker_api="queue",
            create_batch_size=4,
            claim_batch_size=500,
            claim_drain_batches=1,
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
            claim_block_ms=5000,
            claim_drain_block_ms=50,
            complete_batch=True,
            complete_async_depth=0,
            independent_many=True,
            complete_independent_many=False,
            track_duplicates=False,
            claim_partition_batch_size=2,
            server_shards=2,
        )
    )

    assert all(coordinator is not None for coordinator in seen_wake_coordinators)
    assert result["wake_notifications"] > 0
    assert result["wake_credits"] == 8


def test_partition_wake_coordinator_preserves_partial_credit():
    coordinator = bench.PartitionWakeCoordinator(workers=1, partitions=1)
    coordinator.notify_partition(0, 2500)

    assert coordinator.worker_credit(0) == 2500
    assert coordinator.next_partitions(0, 0.0, 16, 1000) == ([0], 1000)
    assert coordinator.worker_credit(0) == 1500
    assert coordinator.next_partitions(0, 0.0, 16, 1000) == ([0], 1000)
    assert coordinator.next_partitions(0, 0.0, 16, 1000) == ([0], 500)
    assert coordinator.total_credit() == 0
    assert coordinator.worker_credit(0) == 0


def test_partition_wake_coordinator_keeps_other_group_credit_queued():
    coordinator = bench.PartitionWakeCoordinator(workers=1, partitions=2)
    coordinator.notify_partition(0, 100)
    coordinator.notify_partition(1, 100)

    parts, credit = coordinator.next_partitions(
        0,
        0.0,
        16,
        1000,
        same_group=lambda first, other: first % 2 == other % 2,
    )
    assert parts == [0]
    assert credit == 100

    assert coordinator.next_partitions(0, 0.0, 16, 1000) == ([1], 100)


def test_partition_wake_coordinator_uses_custom_owner():
    coordinator = bench.PartitionWakeCoordinator(
        workers=4,
        partitions=16,
        owner_for=lambda partition_index: bench.auto_partition_owner(
            partition_index,
            workers=4,
            server_shards=4,
        ),
    )
    partition_index = next(
        index
        for index in range(16)
        if bench.auto_partition_owner(index, workers=4, server_shards=4) == 2
    )

    coordinator.notify_partition(partition_index, 7)

    assert coordinator.next_partitions(2, 0, 1, 100) == ([partition_index], 7)
    assert coordinator.total_credit() == 0


def test_adaptive_fallback_scheduler_backs_off_empty_and_tightens_on_hit(monkeypatch):
    monkeypatch.setattr(bench.time, "perf_counter", lambda: 0.0)
    scheduler = bench.AdaptiveFallbackScheduler(
        min_interval_s=0.01,
        max_interval_s=0.25,
        initial_interval_s=0.05,
    )

    assert scheduler.should_scan(0.04) is False
    assert scheduler.should_scan(0.05) is True

    scheduler.record_scan(0.05, claimed=0)
    assert scheduler.interval_s == pytest.approx(0.075)
    assert scheduler.should_scan(0.12) is False
    assert scheduler.should_scan(0.126) is True

    scheduler.record_scan(0.125, claimed=10)
    assert scheduler.interval_s == pytest.approx(0.0375)


def test_queue_api_worker_caps_wake_credit_by_capacity_and_drain_batches(monkeypatch):
    seen_claim_credits = []

    class FakeWorker:
        def __init__(self, *_args, **_kwargs):
            pass

        def run_batch_once_for_partition_keys(
            self,
            _handler,
            _partition_keys,
            *,
            claim_credit,
            block_ms,
        ):
            seen_claim_credits.append((claim_credit, block_ms))
            return bench.QueueFlowWorkerResult(
                claimed=claim_credit,
                completed=claim_credit,
                claim_calls=1,
            )

        def run_batch_once(self, _handler):
            return bench.QueueFlowWorkerResult()

        def flush(self):
            return bench.QueueFlowWorkerResult()

        def close(self):
            return None

    monkeypatch.setattr(bench, "QueueFlowWorker", FakeWorker)
    coordinator = bench.PartitionWakeCoordinator(workers=1, partitions=16)
    coordinator.notify_partition(0, 2_000)

    result = bench.run_queue_api_worker(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        worker_index=0,
        worker_count=1,
        partitions=16,
        partition_mode="auto",
        claim_any=False,
        claim_batch_size=500,
        complete_batch=True,
        complete_async_depth=0,
        independent_many=True,
        complete_independent_many=False,
        transport="many",
        work_command="none",
        result=None,
        total_flows=750,
        idle_sleep_ms=0.0,
        max_idle_sleep_ms=0.0,
        wake_coalesce_ms=0.0,
        partial_claim_retries=0,
        partial_claim_delay_ms=0.0,
        reclaim_expired=False,
        reclaim_ratio=25,
        claim_priority=0,
        claim_job_only=True,
        claim_block_ms=5000,
        claim_state="queued",
        claim_states=None,
        claim_drain_block_ms=50,
        claim_partition_batch_size=16,
        claim_drain_batches=2,
        worker_capacity=750,
        server_shards=16,
        producers_done=bench.threading.Event(),
        claimed_total=[0],
        completed=[0],
        completed_ids=set(),
        duplicate_completions=[0],
        completed_lock=bench.threading.Lock(),
        wake_coordinator=coordinator,
        track_duplicates=False,
        shared_client=object(),
    )

    assert result["completed"] == 750
    assert seen_claim_credits == [(750, None)]


def test_queue_api_worker_avoids_fallback_scan_while_waiting_for_wake_credit(monkeypatch):
    fallback_calls = []
    claimed_total = [0]
    completed = [0]

    class FakeWorker:
        def __init__(self, *_args, **_kwargs):
            pass

        def run_batch_once_for_partition_keys(self, *_args, **_kwargs):
            raise AssertionError("partition claim should not run without wake credit")

        def run_batch_once(self, _handler):
            fallback_calls.append(True)
            return bench.QueueFlowWorkerResult()

        def flush(self):
            return bench.QueueFlowWorkerResult()

        def close(self):
            return None

    class NoCreditCoordinator:
        def next_partitions(self, *_args, **_kwargs):
            claimed_total[0] = 1
            completed[0] = 1
            raise bench.queue.Empty()

        def worker_credit(self, _worker_index):
            return 0

    class ProducersDone:
        def __init__(self):
            self.calls = 0

        def is_set(self):
            self.calls += 1
            return self.calls >= 4

    monkeypatch.setattr(bench, "QueueFlowWorker", FakeWorker)

    result = bench.run_queue_api_worker(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        worker_index=0,
        worker_count=1,
        partitions=16,
        partition_mode="auto",
        claim_any=False,
        claim_batch_size=500,
        complete_batch=True,
        complete_async_depth=0,
        independent_many=True,
        complete_independent_many=False,
        transport="many",
        work_command="none",
        result=None,
        total_flows=1,
        idle_sleep_ms=0.0,
        max_idle_sleep_ms=0.0,
        wake_coalesce_ms=0.0,
        partial_claim_retries=0,
        partial_claim_delay_ms=0.0,
        reclaim_expired=False,
        reclaim_ratio=25,
        claim_priority=0,
        claim_job_only=True,
        claim_block_ms=5000,
        claim_state="queued",
        claim_states=None,
        claim_drain_block_ms=50,
        claim_partition_batch_size=16,
        claim_drain_batches=2,
        worker_capacity=500,
        server_shards=16,
        producers_done=ProducersDone(),
        claimed_total=claimed_total,
        completed=completed,
        completed_ids=set(),
        duplicate_completions=[0],
        completed_lock=bench.threading.Lock(),
        wake_coordinator=NoCreditCoordinator(),
        track_duplicates=False,
        shared_client=object(),
    )

    assert result["fallback_claims"] == 0
    assert result["claim_calls"] == 0
    assert fallback_calls == []


def test_lowlevel_blocking_worker_claims_partition_batches(monkeypatch):
    seen_claims = []

    class Job:
        id = "job-1"
        lease_token = "lease-1"
        fencing_token = 1

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            self._claimed = False

        def claim_due(self, **kwargs):
            seen_claims.append(kwargs)
            if self._claimed:
                return []
            self._claimed = True
            return [Job()]

        def do_work(self, *_args, **_kwargs):
            return None

        def complete_claimed(self, *_args, **_kwargs):
            return None

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    result = bench.run_claim_worker(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        worker_index=0,
        worker_count=1,
        partitions=8,
        partition_mode="auto",
        claim_any=False,
        claim_batch_size=100,
        complete_batch=True,
        complete_async_depth=0,
        independent_many=True,
        complete_independent_many=False,
        transport="many",
        work_command="none",
        result=None,
        total_flows=1,
        idle_sleep_ms=0.0,
        max_idle_sleep_ms=0.0,
        wake_coalesce_ms=0.0,
        partial_claim_retries=0,
        partial_claim_delay_ms=0.0,
        reclaim_expired=False,
        reclaim_ratio=25,
        claim_priority=0,
        claim_job_only=True,
        claim_block_ms=5000,
        claim_drain_block_ms=50,
        claim_state="queued",
        claim_states=None,
        claim_partition_batch_size=4,
        claim_drain_batches=1,
        worker_capacity=0,
        server_shards=16,
        producers_done=bench.threading.Event(),
        claimed_total=[0],
        completed=[0],
        completed_ids=set(),
        duplicate_completions=[0],
        completed_lock=bench.threading.Lock(),
        wake_coordinator=None,
        track_duplicates=False,
    )

    assert result["completed"] == 1
    assert seen_claims[0]["partition_key"] is None
    assert seen_claims[0]["partition_keys"] == [
        "__flow_auto__:0",
        "__flow_auto__:1",
        "__flow_auto__:2",
        "__flow_auto__:3",
    ]


def test_lowlevel_auto_worker_claim_pages_stay_on_one_server_shard(monkeypatch):
    seen_claims = []

    class Job:
        id = "job-1"
        lease_token = "lease-1"
        fencing_token = 1

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            self._claimed = False

        def claim_due(self, **kwargs):
            seen_claims.append(kwargs)
            if self._claimed:
                return []
            self._claimed = True
            return [Job()]

        def do_work(self, *_args, **_kwargs):
            return None

        def complete_claimed(self, *_args, **_kwargs):
            return None

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    bench.run_claim_worker(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        worker_index=0,
        worker_count=4,
        partitions=bench.AUTO_PARTITION_BUCKETS,
        partition_mode="auto",
        claim_any=False,
        claim_batch_size=100,
        complete_batch=True,
        complete_async_depth=0,
        independent_many=True,
        complete_independent_many=False,
        transport="many",
        work_command="none",
        result=None,
        total_flows=1,
        idle_sleep_ms=0.0,
        max_idle_sleep_ms=0.0,
        wake_coalesce_ms=0.0,
        partial_claim_retries=0,
        partial_claim_delay_ms=0.0,
        reclaim_expired=False,
        reclaim_ratio=25,
        claim_priority=0,
        claim_job_only=True,
        claim_block_ms=5000,
        claim_drain_block_ms=50,
        claim_state="queued",
        claim_states=None,
        claim_partition_batch_size=8,
        claim_drain_batches=1,
        worker_capacity=0,
        server_shards=4,
        producers_done=bench.threading.Event(),
        claimed_total=[0],
        completed=[0],
        completed_ids=set(),
        duplicate_completions=[0],
        completed_lock=bench.threading.Lock(),
        wake_coordinator=None,
        track_duplicates=False,
    )

    partition_keys = seen_claims[0]["partition_keys"]
    assert partition_keys is not None
    assert {
        bench.auto_partition_owner(int(key.rsplit(":", 1)[1]), workers=4, server_shards=4)
        for key in partition_keys
    } == {0}
    shard_ids = {
        bench.auto_partition_server_shard_for_index(int(key.rsplit(":", 1)[1]), 4)
        for key in partition_keys
    }
    assert len(shard_ids) == 1


def test_lowlevel_worker_scans_owned_partitions_before_drain_block(monkeypatch):
    seen_claims = []

    class Job:
        id = "job-1"
        lease_token = "lease-1"
        fencing_token = 1

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            self._calls = 0

        def claim_due(self, **kwargs):
            seen_claims.append(kwargs)
            self._calls += 1
            if self._calls < 3:
                return []
            return [Job()]

        def do_work(self, *_args, **_kwargs):
            return None

        def complete_claimed(self, *_args, **_kwargs):
            return None

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    producers_done = bench.threading.Event()
    producers_done.set()
    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    bench.run_claim_worker(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        worker_index=0,
        worker_count=1,
        partitions=8,
        partition_mode="auto",
        claim_any=False,
        claim_batch_size=100,
        complete_batch=True,
        complete_async_depth=0,
        independent_many=True,
        complete_independent_many=False,
        transport="many",
        work_command="none",
        result=None,
        total_flows=1,
        idle_sleep_ms=0.0,
        max_idle_sleep_ms=0.0,
        wake_coalesce_ms=0.0,
        partial_claim_retries=0,
        partial_claim_delay_ms=0.0,
        reclaim_expired=False,
        reclaim_ratio=25,
        claim_priority=0,
        claim_job_only=True,
        claim_block_ms=5000,
        claim_drain_block_ms=50,
        claim_state="queued",
        claim_states=None,
        claim_partition_batch_size=4,
        claim_drain_batches=1,
        worker_capacity=0,
        server_shards=16,
        producers_done=producers_done,
        claimed_total=[0],
        completed=[0],
        completed_ids=set(),
        duplicate_completions=[0],
        completed_lock=bench.threading.Lock(),
        wake_coordinator=None,
        track_duplicates=False,
    )

    assert [call["partition_keys"] for call in seen_claims] == [
        [
            "__flow_auto__:0",
            "__flow_auto__:1",
            "__flow_auto__:2",
            "__flow_auto__:3",
        ],
        [
            "__flow_auto__:4",
            "__flow_auto__:5",
            "__flow_auto__:6",
            "__flow_auto__:7",
        ],
        [
            "__flow_auto__:0",
            "__flow_auto__:1",
            "__flow_auto__:2",
            "__flow_auto__:3",
            "__flow_auto__:4",
            "__flow_auto__:5",
            "__flow_auto__:6",
            "__flow_auto__:7",
        ],
    ]
    assert [call["claim_block_ms"] for call in seen_claims] == [None, None, 50]


def test_lowlevel_worker_does_not_block_while_producers_are_active(monkeypatch):
    seen_claims = []

    class Job:
        id = "job-1"
        lease_token = "lease-1"
        fencing_token = 1

    class FakeBenchFlowClient:
        def __init__(self, *_args, **_kwargs):
            self._calls = 0

        def claim_due(self, **kwargs):
            seen_claims.append(kwargs)
            self._calls += 1
            if self._calls < 3:
                return []
            return [Job()]

        def do_work(self, *_args, **_kwargs):
            return None

        def complete_claimed(self, *_args, **_kwargs):
            return None

        def pipeline_stats(self, prefix):
            return {
                f"{prefix}_pipeline_flushes": 0,
                f"{prefix}_pipeline_commands": 0,
                f"{prefix}_pipeline_max_depth": 0,
            }

    monkeypatch.setattr(bench, "BenchFlowClient", FakeBenchFlowClient)

    bench.run_claim_worker(
        url="redis://127.0.0.1:7379",
        run_id="run",
        flow_type="email",
        worker_index=0,
        worker_count=1,
        partitions=8,
        partition_mode="auto",
        claim_any=False,
        claim_batch_size=100,
        complete_batch=True,
        complete_async_depth=0,
        independent_many=True,
        complete_independent_many=False,
        transport="many",
        work_command="none",
        result=None,
        total_flows=1,
        idle_sleep_ms=0.0,
        max_idle_sleep_ms=0.0,
        wake_coalesce_ms=0.0,
        partial_claim_retries=0,
        partial_claim_delay_ms=0.0,
        reclaim_expired=False,
        reclaim_ratio=25,
        claim_priority=0,
        claim_job_only=True,
        claim_block_ms=5000,
        claim_drain_block_ms=50,
        claim_state="queued",
        claim_states=None,
        claim_partition_batch_size=4,
        claim_drain_batches=1,
        worker_capacity=0,
        server_shards=16,
        producers_done=bench.threading.Event(),
        claimed_total=[0],
        completed=[0],
        completed_ids=set(),
        duplicate_completions=[0],
        completed_lock=bench.threading.Lock(),
        wake_coordinator=None,
        track_duplicates=False,
    )

    assert [call["partition_keys"] for call in seen_claims] == [
        [
            "__flow_auto__:0",
            "__flow_auto__:1",
            "__flow_auto__:2",
            "__flow_auto__:3",
        ],
        [
            "__flow_auto__:4",
            "__flow_auto__:5",
            "__flow_auto__:6",
            "__flow_auto__:7",
        ],
        [
            "__flow_auto__:0",
            "__flow_auto__:1",
            "__flow_auto__:2",
            "__flow_auto__:3",
        ],
    ]
    assert [call["claim_block_ms"] for call in seen_claims] == [None, None, None]
