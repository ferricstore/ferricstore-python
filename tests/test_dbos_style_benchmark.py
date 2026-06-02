import importlib.util
from argparse import Namespace
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
            claim_block_ms=5000,
            claim_drain_block_ms=50,
            complete_batch=True,
            complete_async_depth=0,
            independent_many=True,
            track_duplicates=False,
            claim_partition_batch_size=2,
            server_shards=16,
        )
    )

    assert seen_drain_batches == [3, 3]


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

    assert coordinator.next_partitions(0, 0.0, 16, 1000) == ([0], 1000)
    assert coordinator.next_partitions(0, 0.0, 16, 1000) == ([0], 1000)
    assert coordinator.next_partitions(0, 0.0, 16, 1000) == ([0], 500)
    assert coordinator.total_credit() == 0


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
