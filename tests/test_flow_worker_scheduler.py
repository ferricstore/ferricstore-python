import queue

import pytest

from ferricstore.types import ClaimedItem
from ferricstore.worker import FlowReadyCoordinator, FlowReadySignal, QueueFlowWorker


class FakeFlowClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.claim_calls = []
        self.completed = []
        self.retried = []
        self.failed = []

    def claim_jobs(self, type, **kwargs):
        self.claim_calls.append((type, kwargs))
        if not self.responses:
            return []
        return self.responses.pop(0)

    def complete_jobs(self, jobs, **kwargs):
        self.completed.append((list(jobs), kwargs))
        return []

    def retry_many(self, partition_key, jobs, **kwargs):
        self.retried.append((partition_key, list(jobs), kwargs))
        return []

    def fail_many(self, partition_key, jobs, **kwargs):
        self.failed.append((partition_key, list(jobs), kwargs))
        return []


class FakeWakeSource:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def next_partitions(self, worker_index, timeout_s, max_partitions, max_credit, same_group=None):
        self.calls.append((worker_index, timeout_s, max_partitions, max_credit, same_group))
        if not self.responses:
            raise queue.Empty
        response = self.responses.pop(0)
        if response is None:
            raise queue.Empty
        return response

    def take_credit(self, worker_index, partition_index):
        return 0


def test_flow_worker_drains_same_partition_group_while_batches_are_full():
    client = FakeFlowClient(
        [
            [object(), object()],
            [object(), object()],
            [object()],
        ]
    )
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=2,
        partition_keys=["bucket-0", "bucket-1"],
        claim_partition_batch_size=1,
        claim_drain_batches=4,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 5
    assert result.completed == 5
    assert len(client.completed) == 3
    assert [call[1]["partition_key"] for call in client.claim_calls] == [
        "bucket-0",
        "bucket-0",
        "bucket-0",
    ]


def test_flow_worker_rejects_invalid_claim_drain_batches():
    with pytest.raises(ValueError, match="claim_drain_batches"):
        QueueFlowWorker(
            FakeFlowClient([]),
            type="email",
            batch_size=10,
            claim_drain_batches=0,
        )


def test_flow_worker_uses_wake_credit_to_claim_owned_partition():
    client = FakeFlowClient([[object(), object()]])
    wake_source = FakeWakeSource([([0], 2)])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=2,
        idle_sleep_s=0.01,
        partition_keys=["bucket-0"],
        partition_indices=[0],
        wake_source=wake_source,
        wake_worker_index=7,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 2
    assert result.completed == 2
    assert result.claim_calls == 1
    assert wake_source.calls[0][:4] == (7, 0.01, 1, 2)
    assert client.claim_calls[0][1]["partition_key"] == "bucket-0"
    assert client.claim_calls[0][1]["limit"] == 2


def test_flow_worker_does_not_disable_reclaim_expired_by_default():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(client, type="email", state="queued", batch_size=1)

    worker.run_once(lambda _job: None)

    assert client.claim_calls[0][1]["reclaim_expired"] is None


def test_flow_worker_wake_source_respects_single_partition_key():
    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(type="email", state="queued", priority=0, partition_key="bucket-2", count=1)
    )
    coordinator.notify(
        FlowReadySignal(type="email", state="queued", priority=0, partition_key="bucket-1", count=1)
    )
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_key="bucket-1",
        wake_source=coordinator,
        wake_worker_index=0,
        idle_sleep_s=0,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 1
    assert client.claim_calls[0][1]["partition_key"] == "bucket-1"


def test_flow_worker_retries_rewake_owner_wakeup_partition():
    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(type="email", state="queued", priority=0, partition_key="bucket-1", count=1)
    )
    job = ClaimedItem(id="flow-1", lease_token=b"lease", fencing_token=1, partition_key="bucket-1")
    client = FakeFlowClient([[job]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-1"],
        wake_source=coordinator,
        wake_worker_index=0,
        idle_sleep_s=0,
    )

    result = worker.run_once(lambda _job: (_ for _ in ()).throw(RuntimeError("boom")))

    assert result.retried == 1
    assert len(client.retried) == 1
    keys, credit = coordinator.next_ready(
        type="email",
        state="queued",
        states=None,
        priority=0,
        partition_keys=["bucket-1"],
        timeout_s=0,
        max_partitions=1,
        max_credit=1,
    )
    assert keys == ["bucket-1"]
    assert credit == 1


def test_flow_worker_cools_empty_partition_before_retrying_it():
    client = FakeFlowClient([[], [object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-0", "bucket-1"],
        claim_partition_batch_size=1,
        empty_claim_cooldown_s=1.0,
    )

    first = worker.run_once(lambda _job: None)
    second = worker.run_once(lambda _job: None)

    assert first.claimed == 0
    assert second.claimed == 1
    assert [call[1]["partition_key"] for call in client.claim_calls] == [
        "bucket-0",
        "bucket-1",
    ]


def test_flow_worker_batch_handler_completes_claimed_batch_once():
    jobs = [object(), object(), object()]
    client = FakeFlowClient([jobs])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=10,
    )

    seen_batches = []
    result = worker.run_batch_once(lambda claimed: seen_batches.append(claimed) or "ok")

    assert result.claimed == 3
    assert result.completed == 3
    assert seen_batches == [jobs]
    assert client.completed == [
        (
            jobs,
            {
                "result": "ok",
                "independent": True,
            },
        )
    ]


def test_flow_worker_start_stop_join_tracks_stats():
    client = FakeFlowClient([[object()], []])
    worker = QueueFlowWorker(client, type="email", state="queued", idle_sleep_s=0.001)

    def handler(_job):
        worker.stop()

    worker.start(handler)
    stats = worker.join(timeout=1)

    assert stats.claimed == 1
    assert stats.completed == 1
    assert worker.is_running is False


def test_flow_ready_coordinator_filters_by_worker_claim_shape():
    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(
            type="sms",
            state="queued",
            priority=0,
            partition_key="bucket-wrong-type",
            count=10,
        )
    )
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="waiting",
            priority=0,
            partition_key="bucket-wrong-state",
            count=10,
        )
    )
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-ok",
            count=7,
        )
    )

    keys, credit = coordinator.next_ready(
        type="email",
        state="queued",
        states=None,
        priority=0,
        partition_keys=["bucket-ok", "bucket-wrong-state", "bucket-wrong-type"],
        timeout_s=0,
        max_partitions=2,
        max_credit=500,
    )

    assert keys == ["bucket-ok"]
    assert credit == 7


def test_flow_ready_coordinator_defers_future_due_signal_without_consuming_credit():
    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-future",
            count=1,
            due_at_ms=9_999_999_999_999,
        )
    )

    with pytest.raises(queue.Empty):
        coordinator.next_ready(
            type="email",
            state="queued",
            states=None,
            priority=0,
            partition_keys=["bucket-future"],
            timeout_s=0,
            max_partitions=1,
            max_credit=1,
        )

    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-ready",
            count=1,
            due_at_ms=0,
        )
    )

    keys, credit = coordinator.next_ready(
        type="email",
        state="queued",
        states=None,
        priority=0,
        partition_keys=["bucket-future", "bucket-ready"],
        timeout_s=0,
        max_partitions=1,
        max_credit=1,
    )

    assert keys == ["bucket-ready"]
    assert credit == 1


def test_flow_ready_coordinator_keeps_ready_credit_when_future_signal_shares_partition():
    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-1",
            count=1,
            due_at_ms=0,
        )
    )
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-1",
            count=1,
            due_at_ms=9_999_999_999_999,
        )
    )

    keys, credit = coordinator.next_ready(
        type="email",
        state="queued",
        states=None,
        priority=0,
        partition_keys=["bucket-1"],
        timeout_s=0,
        max_partitions=1,
        max_credit=1,
    )

    assert keys == ["bucket-1"]
    assert credit == 1


def test_flow_ready_coordinator_does_not_consume_future_credit_after_ready_signal():
    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-1",
            count=1,
            due_at_ms=9_999_999_999_999,
        )
    )
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-1",
            count=1,
            due_at_ms=0,
        )
    )

    keys, credit = coordinator.next_ready(
        type="email",
        state="queued",
        states=None,
        priority=0,
        partition_keys=["bucket-1"],
        timeout_s=0,
        max_partitions=1,
        max_credit=1,
    )

    assert keys == ["bucket-1"]
    assert credit == 1

    with pytest.raises(queue.Empty):
        coordinator.next_ready(
            type="email",
            state="queued",
            states=None,
            priority=0,
            partition_keys=["bucket-1"],
            timeout_s=0,
            max_partitions=1,
            max_credit=1,
        )


def test_flow_worker_does_not_coalesce_wake_when_producers_are_done(monkeypatch):
    sleeps = []
    monkeypatch.setattr("ferricstore.worker.time.sleep", sleeps.append)

    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(
            type="email",
            state="queued",
            priority=0,
            partition_key="bucket-ok",
            count=2,
        )
    )
    client = FakeFlowClient([[object(), object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=500,
        partition_keys=["bucket-ok"],
        wake_source=coordinator,
        wake_worker_index=0,
        wake_producers_done=lambda: True,
        wake_coalesce_s=0.005,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 2
    assert sleeps == []


def test_flow_worker_wake_coalesce_respects_single_partition_key(monkeypatch):
    sleeps = []
    monkeypatch.setattr("ferricstore.worker.time.sleep", sleeps.append)

    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(type="email", state="queued", priority=0, partition_key="bucket-1", count=1)
    )
    coordinator.notify(
        FlowReadySignal(type="email", state="queued", priority=0, partition_key="bucket-2", count=1)
    )
    worker = QueueFlowWorker(
        FakeFlowClient([]),
        type="email",
        state="queued",
        batch_size=2,
        partition_key="bucket-1",
        wake_source=coordinator,
        wake_worker_index=0,
        idle_sleep_s=0,
        wake_coalesce_s=0.005,
        claim_partition_batch_size=2,
    )

    assert worker._next_wake_claim() == ("bucket-1", None, 1)


def test_flow_worker_wake_wait_ignores_unrelated_credit_for_filtered_worker():
    coordinator = FlowReadyCoordinator()
    coordinator.notify(
        FlowReadySignal(type="email", state="queued", priority=0, partition_key="bucket-2", count=1)
    )
    worker = QueueFlowWorker(
        FakeFlowClient([]),
        type="email",
        state="queued",
        batch_size=1,
        partition_key="bucket-1",
        wake_source=coordinator,
        wake_worker_index=0,
    )

    assert worker._wake_should_wait_for_signal() is False


def test_flow_worker_wake_fallback_polls_while_producers_are_active():
    coordinator = FlowReadyCoordinator()
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        wake_source=coordinator,
        wake_worker_index=0,
        wake_producers_done=lambda: False,
        wake_fallback_after=2,
        idle_sleep_s=0,
    )

    first = worker.run_once(lambda _job: None)
    second = worker.run_once(lambda _job: None)

    assert first.claimed == 0
    assert second.claimed == 1
    assert len(client.claim_calls) == 1


def test_flow_worker_retry_wake_uses_job_run_state():
    coordinator = FlowReadyCoordinator()
    worker = QueueFlowWorker(
        FakeFlowClient([]),
        type="email",
        states=["queued", "retry"],
        batch_size=1,
        partition_keys=["bucket-1"],
        wake_source=coordinator,
        wake_worker_index=0,
    )
    job = ClaimedItem(
        id="flow-1",
        state="running",
        run_state="retry",
        lease_token=b"lease",
        fencing_token=1,
        partition_key="bucket-1",
    )

    worker._notify_retried_jobs([job])
    keys, credit = coordinator.next_ready(
        type="email",
        state="retry",
        states=None,
        priority=0,
        partition_keys=["bucket-1"],
        timeout_s=0,
        max_partitions=1,
        max_credit=1,
    )

    assert keys == ["bucket-1"]
    assert credit == 1


def test_flow_worker_retry_wake_without_run_state_signals_all_configured_states():
    coordinator = FlowReadyCoordinator()
    worker = QueueFlowWorker(
        FakeFlowClient([]),
        type="email",
        states=["queued", "retry"],
        batch_size=1,
        partition_keys=["bucket-1"],
        wake_source=coordinator,
        wake_worker_index=0,
    )
    job = ClaimedItem(
        id="flow-1",
        lease_token=b"lease",
        fencing_token=1,
        partition_key="bucket-1",
    )

    worker._notify_retried_jobs([job])
    queued_keys, queued_credit = coordinator.next_ready(
        type="email",
        state="queued",
        states=None,
        priority=0,
        partition_keys=["bucket-1"],
        timeout_s=0,
        max_partitions=1,
        max_credit=1,
    )
    retry_keys, retry_credit = coordinator.next_ready(
        type="email",
        state="retry",
        states=None,
        priority=0,
        partition_keys=["bucket-1"],
        timeout_s=0,
        max_partitions=1,
        max_credit=1,
    )

    assert queued_keys == ["bucket-1"]
    assert queued_credit == 1
    assert retry_keys == ["bucket-1"]
    assert retry_credit == 1


def test_flow_worker_close_merges_async_completion_stats():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        complete_async_depth=1,
        idle_sleep_s=0.001,
    )

    def handler(_job):
        worker.stop()

    worker.start(handler)
    stats_before_close = worker.join(timeout=1)
    worker.close()

    assert stats_before_close.claimed == 1
    assert worker.stats.completed == 1


def test_flow_worker_resets_running_after_loop_exception():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        on_error="raise",
        idle_sleep_s=0,
    )

    with pytest.raises(RuntimeError, match="boom"):
        worker._run_loop(
            lambda _job: (_ for _ in ()).throw(RuntimeError("boom")),
            batch_handler=False,
        )

    assert worker.is_running is False


def test_flow_worker_preserves_distinct_failure_messages_in_batch_retry():
    jobs = [
        ClaimedItem("f1", b"lease-1", 1, partition_key="p1"),
        ClaimedItem("f2", b"lease-2", 2, partition_key="p1"),
    ]
    client = FakeFlowClient([jobs])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=2,
        on_error="retry",
    )

    def handler(job):
        raise RuntimeError(f"boom-{job.id}")

    result = worker.run_once(handler)

    assert result.retried == 2
    assert len(client.retried) == 2
    assert client.retried[0][2]["error"] == "boom-f1"
    assert client.retried[1][2]["error"] == "boom-f2"
