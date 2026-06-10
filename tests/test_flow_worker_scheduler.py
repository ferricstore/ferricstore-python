from concurrent.futures import Future

import pytest

from ferricstore import ExceptionPolicy, QueueClient, RetryPolicy, ValueConfig, WorkerConfig
from ferricstore.types import ClaimedItem, resolve_worker_connection_counts
from ferricstore.worker import QueueFlowWorker


class FakeFlowClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.claim_calls = []
        self.completed = []
        self.retried = []
        self.failed = []
        self.policies = []

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

    def enqueue(self, id, **kwargs):
        self.enqueued = getattr(self, "enqueued", [])
        self.enqueued.append((id, kwargs))
        return b"OK"

    def enqueue_many(self, items, **kwargs):
        self.enqueued_many = getattr(self, "enqueued_many", [])
        self.enqueued_many.append((list(items), kwargs))
        return [b"OK" for _ in items]

    def command(self, *args):
        self.commands = getattr(self, "commands", [])
        self.commands.append(args)
        return b"OK"

    def install_policy(self, type, **kwargs):
        self.policies.append((type, kwargs))
        return b"OK"

    def close(self):
        self.closed = True


class FakeFutureClaimFlowClient(FakeFlowClient):
    def __init__(self, futures):
        super().__init__([])
        self.future_responses = list(futures)
        self.future_claim_calls = []

    def claim_jobs_future(self, type, **kwargs):
        self.future_claim_calls.append((type, kwargs))
        if not self.future_responses:
            future = Future()
            future.set_result([])
            return future
        return self.future_responses.pop(0)


class FakeWakeFlowClient(FakeFlowClient):
    def __init__(self, responses, events):
        super().__init__(responses)
        self.events = list(events)
        self.wake_subscriptions = []
        self.wait_timeouts = []

    def subscribe_flow_wake(self, type, **kwargs):
        self.wake_subscriptions.append((type, kwargs))
        return {"subscribed": ["FLOW_WAKE"]}

    def wait_event(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if not self.events:
            return None
        return self.events.pop(0)


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
        scan_before_blocking=True,
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


def test_flow_worker_rejects_old_owner_wakeup_options():
    with pytest.raises(TypeError):
        QueueFlowWorker(FakeFlowClient([]), type="email", wake_source=object())

    with pytest.raises(TypeError):
        WorkerConfig(owner_wakeup=True)


def test_flow_worker_does_not_disable_reclaim_expired_by_default():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(client, type="email", state="queued", batch_size=1)

    worker.run_once(lambda _job: None)

    assert client.claim_calls[0][1]["reclaim_expired"] is None


def test_flow_worker_uses_nonblocking_claim_by_default():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(client, type="email", state="queued", batch_size=1)

    worker.run_once(lambda _job: None)

    assert client.claim_calls[0][1]["block_ms"] is None


def test_flow_worker_defaults_match_easy_startup_profile():
    worker = QueueFlowWorker(FakeFlowClient([]), type="email")

    assert worker.batch_size == 10
    assert worker.block_ms is None
    assert worker.claim_partition_batch_size == 1
    assert worker.scan_before_blocking is False
    assert worker._complete_async_depth == 0


def test_worker_config_defaults_match_easy_startup_profile():
    kwargs = WorkerConfig().to_kwargs()

    assert kwargs["batch_size"] == 10
    assert "block_ms" not in kwargs
    assert kwargs["claim_partition_batch_size"] == 1
    assert "complete_async_depth" not in kwargs
    assert kwargs["apply_async_depth"] == 0


def test_worker_config_exposes_native_claim_prefetch_when_explicit():
    kwargs = WorkerConfig(claim_prefetch=8).to_kwargs()

    assert kwargs["claim_prefetch"] == 8


def test_worker_config_exposes_native_wake_hints_when_explicit():
    kwargs = WorkerConfig(native_wake_hints=True).to_kwargs()

    assert kwargs["native_wake_hints"] is True


def test_flow_worker_rejects_invalid_claim_prefetch():
    with pytest.raises(ValueError, match="claim_prefetch"):
        QueueFlowWorker(FakeFlowClient([]), type="email", claim_prefetch=-1)


def test_flow_worker_native_wake_hints_subscribes_to_claim_filter():
    client = FakeWakeFlowClient([], [{"event": "FLOW_WAKE"}])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        partition_keys=["bucket-0", "bucket-1"],
        priority=0,
        batch_size=500,
        native_wake_hints=True,
    )

    assert client.wake_subscriptions == [
        (
            "email",
            {
                "state": "queued",
                "states": None,
                "partition_key": None,
                "partition_keys": ["bucket-0", "bucket-1"],
                "priority": 0,
                "limit": 500,
            },
        )
    ]
    assert worker._wait_for_native_wake_hint(0.01) is True
    assert client.wait_timeouts == [0.01]


def test_flow_worker_prefetches_blocking_claims_with_native_future_client():
    job = ClaimedItem("flow-1", b"lease-1", 1, partition_key="bucket-0")
    first = Future()
    first.set_result([job])
    second = Future()
    second.set_result([])
    client = FakeFutureClaimFlowClient([first, second])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        block_ms=5000,
        claim_prefetch=2,
        idle_sleep_s=0,
    )

    result = worker.run_once(lambda _job: "ok")

    assert result.claimed == 1
    assert result.completed == 1
    assert result.claim_calls == 1
    assert client.claim_calls == []
    assert len(client.future_claim_calls) == 3
    assert [call[1]["block_ms"] for call in client.future_claim_calls] == [5000, 5000, 5000]
    assert client.completed == [([job], {"result": "ok", "independent": True})]


def test_flow_worker_blocks_on_all_owned_partitions_by_default():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-0", "bucket-1", "bucket-2", "bucket-3"],
        claim_partition_batch_size=2,
        block_ms=5000,
        idle_sleep_s=0,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 1
    assert len(client.claim_calls) == 1
    assert client.claim_calls[0][1]["partition_keys"] == [
        "bucket-0",
        "bucket-1",
        "bucket-2",
        "bucket-3",
    ]
    assert client.claim_calls[0][1]["block_ms"] == 5000


def test_flow_worker_scans_partition_pages_before_blocking():
    client = FakeFlowClient([[], [], [object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-0", "bucket-1", "bucket-2", "bucket-3"],
        claim_partition_batch_size=2,
        block_ms=5000,
        scan_before_blocking=True,
        idle_sleep_s=0,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 1
    assert [call[1]["partition_keys"] for call in client.claim_calls] == [
        ["bucket-0", "bucket-1"],
        ["bucket-2", "bucket-3"],
        ["bucket-0", "bucket-1", "bucket-2", "bucket-3"],
    ]
    assert [call[1]["block_ms"] for call in client.claim_calls] == [None, None, 5000]


def test_flow_worker_can_use_short_block_after_partition_scan():
    client = FakeFlowClient([[], [], [object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-0", "bucket-1", "bucket-2", "bucket-3"],
        claim_partition_batch_size=2,
        block_ms=5000,
        claim_scan_block_ms=50,
        scan_before_blocking=True,
        idle_sleep_s=0,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 1
    assert [call[1]["block_ms"] for call in client.claim_calls] == [None, None, 50]


def test_flow_worker_rejects_invalid_claim_scan_block_ms():
    with pytest.raises(ValueError, match="claim_scan_block_ms"):
        QueueFlowWorker(
            FakeFlowClient([]),
            type="email",
            partition_keys=["bucket-0"],
            claim_scan_block_ms=-1,
        )


def test_flow_worker_does_not_block_when_scan_finds_ready_partition():
    client = FakeFlowClient([[], [object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-0", "bucket-1", "bucket-2", "bucket-3"],
        claim_partition_batch_size=2,
        block_ms=5000,
        scan_before_blocking=True,
        idle_sleep_s=0,
    )

    result = worker.run_once(lambda _job: None)

    assert result.claimed == 1
    assert [call[1]["partition_keys"] for call in client.claim_calls] == [
        ["bucket-0", "bucket-1"],
        ["bucket-2", "bucket-3"],
    ]
    assert [call[1]["block_ms"] for call in client.claim_calls] == [None, None]


def test_flow_worker_batches_partition_keys_by_default():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-0", "bucket-1", "bucket-2"],
    )

    worker.run_once(lambda _job: None)

    assert client.claim_calls[0][1]["partition_key"] is None
    assert client.claim_calls[0][1]["partition_keys"] == [
        "bucket-0",
        "bucket-1",
        "bucket-2",
    ]


def test_flow_worker_uses_separate_claim_client_when_provided():
    command_client = FakeFlowClient([])
    claim_client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        command_client,
        claim_client=claim_client,
        type="email",
        state="queued",
        batch_size=1,
    )

    result = worker.run_once(lambda _job: "ok")

    assert result.claimed == 1
    assert command_client.claim_calls == []
    assert len(claim_client.claim_calls) == 1
    assert len(command_client.completed) == 1
    assert claim_client.completed == []


def test_flow_worker_cools_empty_partition_before_retrying_it():
    client = FakeFlowClient([[], [object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        partition_keys=["bucket-0", "bucket-1"],
        claim_partition_batch_size=1,
        block_ms=None,
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


def test_flow_worker_can_run_batch_for_explicit_partition_keys():
    client = FakeFlowClient([[object(), object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=10,
        partition_keys=["bucket-ignored"],
        block_ms=5000,
    )

    result = worker.run_batch_once_for_partition_keys(
        lambda claimed: "ok",
        ["bucket-1", "bucket-2"],
        claim_credit=2,
        block_ms=None,
    )

    assert result.claimed == 2
    assert result.completed == 2
    assert client.claim_calls[0][1]["partition_keys"] == ["bucket-1", "bucket-2"]
    assert client.claim_calls[0][1]["limit"] == 2
    assert client.claim_calls[0][1]["block_ms"] is None


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
        exception_policy=ExceptionPolicy.RAISE,
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


def test_queue_client_creates_queue_and_delegates_flow_commands():
    client = FakeFlowClient([])
    queue_client = QueueClient(client)
    queue = queue_client.queue(type="email", state="queued")

    assert queue.enqueue("email-1", payload=b"p") == b"OK"
    worker = queue.worker(batch_size=10)
    assert worker.type == "email"
    assert worker.state == "queued"
    assert queue_client.command("PING") == b"OK"
    assert client.enqueued[0] == ("email-1", {"type": "email", "state": "queued", "payload": b"p"})
    assert client.commands[0] == ("PING",)


def test_queue_client_retry_policy_is_inherited_and_can_be_overridden():
    client = FakeFlowClient([])
    default_policy = RetryPolicy(max_retries=5)
    queue_policy = RetryPolicy(max_retries=2)
    queue_client = QueueClient(client, retry_policy=default_policy)
    queue = queue_client.queue(type="email", retry_policy=queue_policy)

    queue.install_policy()
    queue_client.install_policy("sms")

    assert client.policies[0] == ("email", {"retry": queue_policy})
    assert client.policies[1] == ("sms", {"retry": default_policy, "states": None})


def test_queue_client_worker_and_value_config_are_inherited_and_overridable():
    client = FakeFlowClient([])
    queue_client = QueueClient(
        client,
        worker_config=WorkerConfig(batch_size=50, concurrency=4, idle_sleep_s=0.01),
        value_config=ValueConfig(value_max_bytes=64_000),
    )
    queue = queue_client.queue(type="email")

    worker = queue.worker(batch_size=10)

    assert worker.batch_size == 10
    assert worker.concurrency == 4
    assert worker.idle_sleep_s == 0.01
    assert worker.value_max_bytes == 64_000


def test_queue_client_from_url_creates_bounded_command_and_claim_pools(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        calls.append((url, kwargs))
        return FakeFlowClient([])

    monkeypatch.setattr("ferricstore.worker.FlowClient.from_url", staticmethod(from_url))

    queue_client = QueueClient.from_url(
        "redis://example/0",
        worker_config=WorkerConfig(workers=3),
    )

    worker = queue_client.queue(type="email").worker()

    assert calls == [
        ("redis://example/0", {"max_connections": 3}),
        ("redis://example/0", {"max_connections": 3}),
    ]
    assert worker.client is queue_client.flow
    assert worker.claim_client is queue_client.claim_flow


def test_queue_client_from_native_url_reuses_multiplexed_claim_client(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = FakeFlowClient([])
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.worker.FlowClient.from_url", staticmethod(from_url))

    queue_client = QueueClient.from_url(
        "ferric://example:6388",
        worker_config=WorkerConfig(workers=3),
    )
    worker = queue_client.queue(type="email").worker()

    assert len(calls) == 1
    assert calls[0][1] == {"max_connections": 3}
    assert queue_client.claim_flow is queue_client.flow
    assert worker.client is queue_client.flow
    assert worker.claim_client is queue_client.flow


def test_flow_worker_from_native_url_reuses_multiplexed_claim_client(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = FakeFlowClient([])
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.worker.FlowClient.from_url", staticmethod(from_url))

    worker = QueueFlowWorker(
        "ferric://example:6388",
        type="email",
        state="queued",
        concurrency=50,
    )

    assert len(calls) == 1
    assert calls[0][1] == {"max_connections": 2}
    assert worker.claim_client is worker.client


def test_queue_worker_config_at_queue_time_resizes_claim_pool(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = FakeFlowClient([])
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.worker.FlowClient.from_url", staticmethod(from_url))

    queue_client = QueueClient.from_url("redis://example/0")
    worker = queue_client.queue(
        type="email",
        worker_config=WorkerConfig(workers=16),
    ).worker()

    assert calls[0][1]["max_connections"] == 2
    assert calls[1][1]["max_connections"] == 1
    assert calls[2][1]["max_connections"] == 16
    assert worker.client is queue_client.flow
    assert worker.claim_client is calls[2][2]


def test_queue_worker_config_does_not_resize_native_claim_pool(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = FakeFlowClient([])
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.worker.FlowClient.from_url", staticmethod(from_url))

    queue_client = QueueClient.from_url("ferric://example:6388")
    worker = queue_client.queue(
        type="email",
        worker_config=WorkerConfig(workers=16),
    ).worker()

    assert len(calls) == 1
    assert worker.client is queue_client.flow
    assert worker.claim_client is queue_client.flow


def test_queue_client_close_does_not_close_externally_owned_clients():
    flow = FakeFlowClient([])
    claim_flow = FakeFlowClient([])

    queue_client = QueueClient(flow, claim_client=claim_flow)
    queue_client.close()

    assert not hasattr(flow, "closed")
    assert not hasattr(claim_flow, "closed")


def test_worker_connection_counts_reject_zero_limits():
    with pytest.raises(ValueError, match="command_connections"):
        resolve_worker_connection_counts(workers=4, command_connections=0)

    with pytest.raises(ValueError, match="claim_connections"):
        resolve_worker_connection_counts(workers=4, claim_connections=0)
