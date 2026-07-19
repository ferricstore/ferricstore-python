import threading
import time
from collections import deque
from concurrent.futures import Future

import pytest

import ferricstore.worker as worker_module
from ferricstore import ExceptionPolicy, QueueClient, RetryPolicy, ValueConfig, WorkerConfig
from ferricstore.types import ClaimedFlow, resolve_worker_connection_counts
from ferricstore.worker import QueueFlowWorker, QueueFlowWorkerResult, Worker
from ferricstore.worker_core import validate_worker_idle_timing


class FakeFlowClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.claim_calls = []
        self.completed = []
        self.retried = []
        self.failed = []
        self.policies = []

    def claim_flows(self, type, **kwargs):
        self.claim_calls.append((type, kwargs))
        if not self.responses:
            return []
        return self.responses.pop(0)

    def complete_jobs(self, jobs, **kwargs):
        self.completed.append((list(jobs), kwargs))
        return [b"OK"] * len(jobs)

    def retry_many(self, partition_key, jobs, **kwargs):
        self.retried.append((partition_key, list(jobs), kwargs))
        return [b"OK"] * len(jobs)

    def fail_many(self, partition_key, jobs, **kwargs):
        self.failed.append((partition_key, list(jobs), kwargs))
        return [b"OK"] * len(jobs)

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


class FusedFakeFlowClient(FakeFlowClient):
    def __init__(self, responses):
        super().__init__(responses)
        self.complete_claim_calls = []

    def complete_flows_and_claim_flows(self, jobs, **kwargs):
        self.complete_claim_calls.append((list(jobs), kwargs))
        if not self.responses:
            return []
        return self.responses.pop(0)


class AsyncFusedFakeFlowClient(FusedFakeFlowClient):
    def submit_complete_flows_and_claim_flows(self, jobs, **kwargs):
        self.complete_claim_calls.append((list(jobs), kwargs))
        complete_future = Future()
        claim_future = Future()
        complete_future.set_result(len(jobs))
        claim_future.set_result(self.responses.pop(0) if self.responses else [])
        return complete_future, claim_future


class FakeFutureClaimFlowClient(FakeFlowClient):
    def __init__(self, futures):
        super().__init__([])
        self.future_responses = list(futures)
        self.future_claim_calls = []

    def claim_flows_future(self, type, **kwargs):
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


@pytest.mark.parametrize(
    "kwargs",
    [
        {"idle_sleep_s": -0.001},
        {"max_idle_sleep_s": -0.001},
    ],
)
def test_queue_flow_worker_rejects_negative_idle_timing(kwargs) -> None:
    with pytest.raises(ValueError, match="idle_sleep_s must be non-negative"):
        QueueFlowWorker(FakeFlowClient([]), type="email", **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"idle_sleep_s": -0.001},
        {"max_idle_sleep_s": -0.001},
    ],
)
def test_legacy_worker_rejects_negative_idle_timing(kwargs) -> None:
    workflow = type("FakeWorkflow", (), {"_states": {"queued": object()}})()

    with pytest.raises(ValueError, match="idle_sleep_s must be non-negative"):
        Worker(workflow, worker="worker-1", **kwargs)


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), -1.0, True])
@pytest.mark.parametrize("field", ["idle_sleep_s", "max_idle_sleep_s"])
def test_worker_idle_timing_rejects_non_finite_negative_or_boolean_values(
    field: str, invalid: float
) -> None:
    values = {"idle_sleep_s": 0.1, "max_idle_sleep_s": 1.0}
    values[field] = invalid

    with pytest.raises(ValueError, match=rf"{field} must be non-negative and finite"):
        validate_worker_idle_timing(**values)


@pytest.mark.parametrize("field", ["empty_claim_cooldown_s", "partial_claim_cooldown_s"])
@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), -1.0, True])
def test_queue_worker_claim_cooldowns_require_finite_nonnegative_values(
    field: str,
    invalid: float,
) -> None:
    with pytest.raises(ValueError, match=rf"{field} must be non-negative and finite"):
        QueueFlowWorker(FakeFlowClient([]), type="email", **{field: invalid})


def test_flow_worker_validates_completion_clients_before_opening_owned_clients(monkeypatch):
    opened: list[str] = []

    def from_url(url, **_kwargs):
        opened.append(url)
        return FakeFlowClient([])

    monkeypatch.setattr(worker_module.FlowClient, "from_url", staticmethod(from_url))

    with pytest.raises(ValueError, match="completion_clients must be non-empty"):
        QueueFlowWorker(
            "ferric://seed.local:6388",
            type="email",
            completion_clients=[],
        )

    assert opened == []


def test_flow_worker_closes_first_owned_client_when_second_connection_fails(monkeypatch):
    opened: list[FakeFlowClient] = []

    def from_url(_url, **_kwargs):
        if opened:
            raise OSError("claim connection failed")
        client = FakeFlowClient([])
        client.closed = False
        opened.append(client)
        return client

    monkeypatch.setattr(worker_module.FlowClient, "from_url", staticmethod(from_url))

    with pytest.raises(OSError, match="claim connection failed"):
        QueueFlowWorker("ferric://seed.local:6388", type="email")

    assert len(opened) == 1
    assert opened[0].closed is True


@pytest.mark.parametrize("constructor", ["direct", "from_url"])
def test_queue_client_closes_first_owned_client_when_second_connection_fails(
    monkeypatch, constructor
):
    opened: list[FakeFlowClient] = []

    def from_url(_url, **_kwargs):
        if opened:
            raise OSError("claim connection failed")
        client = FakeFlowClient([])
        client.closed = False
        opened.append(client)
        return client

    monkeypatch.setattr(worker_module.FlowClient, "from_url", staticmethod(from_url))

    with pytest.raises(OSError, match="claim connection failed"):
        if constructor == "direct":
            QueueClient("ferric://seed.local:6388")
        else:
            QueueClient.from_url("ferric://seed.local:6388")

    assert len(opened) == 1
    assert opened[0].closed is True


def test_flow_worker_rolls_back_all_owned_resources_after_late_startup_failure(monkeypatch):
    opened: list[FakeFlowClient] = []
    executors = []

    class FailingSubscriptionClient(FakeFlowClient):
        def __init__(self) -> None:
            super().__init__([])
            self.closed = False

        def subscribe_flow_wake(self, *_args, **_kwargs):
            raise OSError("subscription failed")

        def wait_event(self, timeout=None):
            return None

    class FakeExecutor:
        def __init__(self, max_workers):
            self.max_workers = max_workers
            self.shutdown_called = False
            executors.append(self)

        def shutdown(self, *, wait, cancel_futures):
            assert wait is False
            assert cancel_futures is True
            self.shutdown_called = True

    def from_url(_url, **_kwargs):
        client = FailingSubscriptionClient()
        opened.append(client)
        return client

    monkeypatch.setattr(worker_module.FlowClient, "from_url", staticmethod(from_url))
    monkeypatch.setattr(worker_module, "ThreadPoolExecutor", FakeExecutor)

    with pytest.raises(OSError, match="subscription failed"):
        QueueFlowWorker(
            "ferric://seed.local:6388",
            type="email",
            concurrency=2,
            complete_async_depth=1,
            protocol_wake_hints=True,
        )

    assert len(opened) == 2
    assert all(client.closed for client in opened)
    assert len(executors) == 2
    assert all(executor.shutdown_called for executor in executors)


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


def test_flow_worker_preserves_binary_derived_partition_keys():
    partition = b"fpk:2:\x00\xff"
    worker = QueueFlowWorker(
        FakeFlowClient([]),
        type="email",
        partition_keys=[partition],
        priority=None,
    )

    try:
        assert worker.partition_keys == [partition]
    finally:
        worker.close()


def test_flow_worker_stop_before_thread_loop_starts_is_not_overwritten():
    entered = threading.Event()
    release = threading.Event()

    class DelayedWorker(QueueFlowWorker):
        def _run_loop(self, handler, *, batch_handler):
            entered.set()
            release.wait()
            super()._run_loop(handler, batch_handler=batch_handler)

    worker = DelayedWorker(FakeFlowClient([]), type="email", idle_sleep_s=0.001)
    worker.start(lambda _job: None)
    assert entered.wait(1)
    worker.stop()
    release.set()

    worker.join(timeout=0.2)
    stopped_before_cleanup = not worker.is_running and not worker._thread.is_alive()
    if not stopped_before_cleanup:
        worker.stop()
        worker.join(timeout=0.2)

    assert stopped_before_cleanup


def test_flow_worker_close_has_bounded_wait_for_blocking_claim():
    entered = threading.Event()
    release = threading.Event()

    class BlockingClient(FakeFlowClient):
        def claim_flows(self, type, **kwargs):
            self.claim_calls.append((type, kwargs))
            entered.set()
            release.wait()
            return []

    worker = QueueFlowWorker(BlockingClient([]), type="email", block_ms=60_000)
    worker.start(lambda _job: None)
    assert entered.wait(1)

    with pytest.raises(TimeoutError, match="close timed out"):
        worker.close(timeout=0.01)

    release.set()
    worker.close(timeout=1)
    assert worker.is_running is False


def test_flow_worker_close_waits_for_caller_managed_run_thread():
    entered = threading.Event()
    release = threading.Event()

    class BlockingClient(FakeFlowClient):
        def claim_flows(self, type, **kwargs):
            self.claim_calls.append((type, kwargs))
            entered.set()
            release.wait()
            return []

    client = BlockingClient([])
    worker = QueueFlowWorker(
        client,
        type="email",
        block_ms=60_000,
    )
    worker._owns_client = True
    run_thread = threading.Thread(target=worker.run_forever, args=(lambda _job: None,))
    run_thread.start()
    assert entered.wait(1)

    try:
        with pytest.raises(TimeoutError, match="close timed out"):
            worker.close(timeout=0.01)
        assert getattr(client, "closed", False) is False
        assert run_thread.is_alive()
    finally:
        release.set()
        run_thread.join(1)
        if not getattr(client, "closed", False):
            worker.close(timeout=1)

    assert getattr(client, "closed", False) is True
    assert worker.is_running is False


def test_flow_worker_close_deadline_includes_pending_completions():
    worker = QueueFlowWorker(FakeFlowClient([]), type="email")
    pending: Future[QueueFlowWorkerResult] = Future()
    worker._pending_completions.append(pending)
    release = threading.Timer(
        0.15,
        lambda: pending.set_result(QueueFlowWorkerResult(completed=1)),
    )
    release.start()
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="close timed out"):
            worker.close(timeout=0.01)
        assert time.monotonic() - started < 0.1
    finally:
        if not pending.done():
            pending.set_result(QueueFlowWorkerResult(completed=1))
        release.cancel()
        worker.close(timeout=1)


def test_flow_worker_close_does_not_confuse_completion_timeout_with_deadline():
    worker = QueueFlowWorker(FakeFlowClient([]), type="email")
    failed: Future[QueueFlowWorkerResult] = Future()
    failed.set_exception(TimeoutError("completion operation timed out"))
    worker._pending_completions.append(failed)

    with pytest.raises(TimeoutError, match="completion operation timed out"):
        worker.close(timeout=1)

    assert not worker._pending_completions
    worker.close(timeout=1)


def test_flow_worker_close_deadline_includes_standalone_run_once_executor_work():
    entered = threading.Event()
    release = threading.Event()
    job = ClaimedFlow("f1", b"lease-1", 1, partition_key="p1")
    worker = QueueFlowWorker(
        FakeFlowClient([[job]]),
        type="email",
        concurrency=2,
        batch_size=1,
    )

    def handler(_job):
        entered.set()
        release.wait()
        return b"done"

    run_thread = threading.Thread(target=worker.run_once, args=(handler,))
    run_thread.start()
    assert entered.wait(1)
    delayed_release = threading.Timer(0.2, release.set)
    delayed_release.start()
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="close timed out"):
            worker.close(timeout=0.01)
        assert time.monotonic() - started < 0.1
    finally:
        release.set()
        delayed_release.cancel()
        run_thread.join(1)

    worker.close(timeout=1)
    assert run_thread.is_alive() is False


def test_flow_worker_fuses_complete_and_next_claim_on_sync_hot_path():
    first = ClaimedFlow("f1", b"lease-1", 1, partition_key="p1")
    second = ClaimedFlow("f2", b"lease-2", 2, partition_key="p1")
    client = FusedFakeFlowClient([[first], [second]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        worker="w1",
        batch_size=1,
        claim_drain_batches=2,
        partition_key="p1",
        fuse_complete_claim=True,
    )

    result = worker.run_batch_once(lambda jobs: f"done-{jobs[0].id}")
    worker.close()

    assert result == QueueFlowWorkerResult(claimed=2, completed=2, claim_calls=2)
    assert len(client.claim_calls) == 1
    assert len(client.complete_claim_calls) == 1
    assert len(client.completed) == 1
    assert [job.id for job in client.complete_claim_calls[0][0]] == ["f1"]
    assert client.complete_claim_calls[0][1]["result"] == "done-f1"
    assert client.complete_claim_calls[0][1]["type"] == "email"
    assert client.complete_claim_calls[0][1]["state"] == "queued"
    assert client.complete_claim_calls[0][1]["partition_key"] == "p1"
    assert [job.id for job in client.completed[0][0]] == ["f2"]


def test_flow_worker_async_fuses_complete_and_next_claim_without_completion_thread():
    first = ClaimedFlow("f1", b"lease-1", 1, partition_key="p1")
    second = ClaimedFlow("f2", b"lease-2", 2, partition_key="p1")
    client = AsyncFusedFakeFlowClient([[first], [second]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        worker="w1",
        batch_size=1,
        claim_drain_batches=2,
        partition_key="p1",
        complete_async_depth=4,
        fuse_complete_claim=True,
    )

    result = worker.run_batch_once(lambda jobs: f"done-{jobs[0].id}")
    worker.close()

    assert result == QueueFlowWorkerResult(claimed=2, claim_calls=2)
    assert worker.stats.completed == 2
    assert len(client.claim_calls) == 1
    assert len(client.complete_claim_calls) == 1
    assert len(client.completed) == 1
    assert [job.id for job in client.complete_claim_calls[0][0]] == ["f1"]
    assert client.complete_claim_calls[0][1]["result"] == "done-f1"
    assert [job.id for job in client.completed[0][0]] == ["f2"]


def test_flow_worker_async_fusion_can_use_separate_protocol_claim_client():
    first = ClaimedFlow("f1", b"lease-1", 1, partition_key="p1")
    second = ClaimedFlow("f2", b"lease-2", 2, partition_key="p1")
    command_client = FakeFlowClient([])
    claim_client = AsyncFusedFakeFlowClient([[first], [second]])
    worker = QueueFlowWorker(
        command_client,
        claim_client=claim_client,
        type="email",
        state="queued",
        worker="w1",
        batch_size=1,
        claim_drain_batches=2,
        partition_key="p1",
        complete_async_depth=4,
        fuse_complete_claim=True,
    )

    result = worker.run_batch_once(lambda jobs: f"done-{jobs[0].id}")
    worker.close()

    assert result == QueueFlowWorkerResult(claimed=2, claim_calls=2)
    assert len(claim_client.claim_calls) == 1
    assert len(claim_client.complete_claim_calls) == 1
    assert command_client.completed[0][0] == [second]
    assert claim_client.completed == []


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


def test_worker_config_exposes_protocol_claim_prefetch_when_explicit():
    kwargs = WorkerConfig(claim_prefetch=8).to_kwargs()

    assert kwargs["claim_prefetch"] == 8


def test_worker_config_exposes_protocol_wake_hints_when_explicit():
    kwargs = WorkerConfig(protocol_wake_hints=True).to_kwargs()

    assert kwargs["protocol_wake_hints"] is True


def test_flow_worker_rejects_invalid_claim_prefetch():
    with pytest.raises(ValueError, match="claim_prefetch"):
        QueueFlowWorker(FakeFlowClient([]), type="email", claim_prefetch=-1)


def test_flow_worker_protocol_wake_hints_subscribes_to_claim_filter():
    client = FakeWakeFlowClient([], [{"event": "FLOW_WAKE"}])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        partition_keys=["bucket-0", "bucket-1"],
        priority=0,
        batch_size=500,
        protocol_wake_hints=True,
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
    assert worker._wait_for_protocol_wake_hint(0.01) is True
    assert client.wait_timeouts == [0.01]


def test_flow_worker_owns_an_isolated_wake_subscription_session():
    dedicated_clients: list[FakeWakeFlowClient] = []

    class DedicatedWakeClient(FakeWakeFlowClient):
        def __init__(self) -> None:
            super().__init__([], [{"event": "FLOW_WAKE"}])
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class SharedClaimClient(FakeWakeFlowClient):
        def __init__(self) -> None:
            super().__init__([], [])

        def _acquire_subscription_client(self):
            dedicated = DedicatedWakeClient()
            dedicated_clients.append(dedicated)
            return dedicated, True

    shared = SharedClaimClient()
    first = QueueFlowWorker(shared, type="email", protocol_wake_hints=True)
    second = QueueFlowWorker(shared, type="sms", protocol_wake_hints=True)
    try:
        assert shared.wake_subscriptions == []
        assert len(dedicated_clients) == 2
        assert [client.wake_subscriptions[0][0] for client in dedicated_clients] == [
            "email",
            "sms",
        ]
        assert first._wait_for_protocol_wake_hint(0.01) is True
    finally:
        first.close()
        second.close()

    assert all(client.closed for client in dedicated_clients)


def test_flow_worker_prefetches_blocking_claims_with_protocol_future_client():
    job = ClaimedFlow("flow-1", b"lease-1", 1, partition_key="bucket-0")
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


def test_flow_worker_close_preserves_and_finishes_prefetched_claim_ownership():
    first_job = ClaimedFlow("flow-1", b"lease-1", 1, partition_key="bucket-0")
    second_job = ClaimedFlow("flow-2", b"lease-2", 2, partition_key="bucket-0")
    first: Future[list[ClaimedFlow]] = Future()
    first.set_result([first_job])
    second: Future[list[ClaimedFlow]] = Future()
    third: Future[list[ClaimedFlow]] = Future()
    client = FakeFutureClaimFlowClient([first, second, third])
    handled: list[str] = []
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=1,
        block_ms=5_000,
        claim_prefetch=2,
        idle_sleep_s=0,
    )

    worker.run_once(lambda job: handled.append(job.id) or "ok")

    try:
        with pytest.raises(TimeoutError, match="close timed out"):
            worker.close(timeout=0.01)
        assert second.cancelled() is False
        assert third.cancelled() is False
    finally:
        if not second.done():
            second.set_result([second_job])
        if not third.done():
            third.set_result([])
        worker.close(timeout=1)

    assert handled == ["flow-1", "flow-2"]
    assert [jobs for jobs, _kwargs in client.completed] == [[first_job], [second_job]]


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


def test_flow_worker_does_not_claim_when_async_completion_slots_are_full():
    blocked = Future()
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        batch_size=10,
        complete_async_depth=1,
    )
    worker._pending_completions.append(blocked)

    result = worker.run_batch_once_for_partition_keys(
        lambda claimed: "ok",
        ["bucket-1"],
        claim_credit=10,
        block_ms=None,
    )

    assert result.claimed == 0
    assert result.claim_calls == 0
    assert client.claim_calls == []

    blocked.set_result(QueueFlowWorkerResult(completed=1))
    worker.close()


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


def test_flow_worker_begin_run_is_atomic_across_concurrent_callers():
    worker = QueueFlowWorker(FakeFlowClient([]), type="email")
    barrier = threading.Barrier(3)
    original_reset = worker._terminal_state.reset
    successes: list[None] = []
    errors: list[BaseException] = []

    def synchronized_reset() -> None:
        time.sleep(0.05)
        original_reset()

    worker._terminal_state.reset = synchronized_reset  # type: ignore[method-assign]

    def begin() -> None:
        barrier.wait(timeout=1)
        try:
            worker._begin_run()
            successes.append(None)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=begin) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=1)
    for thread in threads:
        thread.join(1)

    worker.stop()
    worker.close()
    assert len(successes) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert str(errors[0]) == "worker already running"


def test_flow_worker_close_cannot_finish_before_inflight_start_transition():
    worker = QueueFlowWorker(FakeFlowClient([]), type="email")
    reset_entered = threading.Event()
    release_reset = threading.Event()
    close_done = threading.Event()
    original_reset = worker._terminal_state.reset

    def blocked_reset() -> None:
        reset_entered.set()
        assert release_reset.wait(1)
        original_reset()

    worker._terminal_state.reset = blocked_reset  # type: ignore[method-assign]
    begin_thread = threading.Thread(target=worker._begin_run)
    begin_thread.start()
    assert reset_entered.wait(1)

    close_thread = threading.Thread(target=lambda: (worker.close(), close_done.set()))
    close_thread.start()
    close_finished_during_start = close_done.wait(0.05)
    release_reset.set()
    begin_thread.join(1)
    close_thread.join(1)

    assert close_finished_during_start is False
    assert close_done.is_set()
    assert worker.is_running is False


def test_flow_worker_close_timeout_bounds_owned_blocking_client_cleanup():
    class SlowCloseClient(FakeFlowClient):
        def __init__(self) -> None:
            super().__init__([])
            self.close_entered = threading.Event()
            self.close_finished = threading.Event()
            self.release_close = threading.Event()
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            self.close_entered.set()
            self.release_close.wait(1)
            self.close_finished.set()

    client = SlowCloseClient()
    worker = QueueFlowWorker(client, type="email")
    worker._owns_client = True
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError, match="close timed out"):
            worker.close(timeout=0.01)
        elapsed = time.monotonic() - started
        assert elapsed < 0.1
        assert client.close_entered.wait(1)
    finally:
        client.release_close.set()

    assert client.close_finished.wait(1)
    worker.close(timeout=1)
    assert client.close_calls == 1
    assert worker._owns_client is False


def test_flow_worker_pending_completion_queue_has_constant_time_head_drains():
    worker = QueueFlowWorker(FakeFlowClient([]), type="email")

    assert isinstance(worker._pending_completions, deque)

    worker.close()


def test_flow_worker_join_propagates_background_failure():
    client = FakeFlowClient([[object()]])
    worker = QueueFlowWorker(
        client,
        type="email",
        exception_policy=ExceptionPolicy.RAISE,
        idle_sleep_s=0,
    )
    worker.start(lambda _job: (_ for _ in ()).throw(RuntimeError("background boom")))

    with pytest.raises(RuntimeError, match="background boom"):
        worker.join(timeout=1)


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
        ClaimedFlow("f1", b"lease-1", 1, partition_key="p1"),
        ClaimedFlow("f2", b"lease-2", 2, partition_key="p1"),
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


def test_flow_worker_handler_fanout_keeps_only_a_bounded_pending_window():
    class TrackedFuture(Future):
        def __init__(self, executor, value):
            super().__init__()
            self.executor = executor
            self.consumed = False
            self.set_result(value)

        def result(self, timeout=None):
            if not self.consumed:
                self.consumed = True
                self.executor.pending -= 1
            return super().result(timeout)

    class TrackingExecutor:
        def __init__(self):
            self.pending = 0
            self.max_pending = 0

        def submit(self, operation, item):
            self.pending += 1
            self.max_pending = max(self.max_pending, self.pending)
            return TrackedFuture(self, operation(item))

    jobs = [ClaimedFlow(f"f{index}", b"lease", index, partition_key="p1") for index in range(100)]
    worker = QueueFlowWorker(FakeFlowClient([]), type="email", concurrency=4)
    assert worker._executor is not None
    worker._executor.shutdown(wait=True)
    executor = TrackingExecutor()
    worker._executor = executor

    handled = worker._run_handlers(jobs, lambda job: job.id)

    worker._executor = None
    assert handled.failures == []
    assert handled.jobs == jobs
    assert executor.max_pending <= 4


def test_flow_worker_pipelines_distinct_failures_as_one_mutation_batch():
    jobs = [
        ClaimedFlow("f1", b"lease-1", 1, partition_key="p1"),
        ClaimedFlow("f2", b"lease-2", 2, partition_key="p1"),
    ]

    class MutationClient(FakeFlowClient):
        def __init__(self) -> None:
            super().__init__([jobs])
            self.mutation_batches = []

        def apply_job_mutations(self, mutations):
            self.mutation_batches.append(list(mutations))
            return [b"OK"] * len(mutations)

    client = MutationClient()
    worker = QueueFlowWorker(client, type="email", batch_size=2, on_error="retry")

    result = worker.run_once(lambda job: (_ for _ in ()).throw(RuntimeError(f"boom-{job.id}")))

    assert result.retried == 2
    assert len(client.mutation_batches) == 1
    assert [mutation.kind.value for mutation in client.mutation_batches[0]] == [
        "retry",
        "retry",
    ]
    assert [mutation.options["error"] for mutation in client.mutation_batches[0]] == [
        "boom-f1",
        "boom-f2",
    ]


def test_flow_worker_does_not_conflate_bool_and_int_completion_results():
    jobs = [
        ClaimedFlow("bool", b"lease-1", 1, partition_key="p1"),
        ClaimedFlow("int", b"lease-2", 2, partition_key="p1"),
    ]

    class TypedResultClient(FakeFlowClient):
        def __init__(self) -> None:
            super().__init__([jobs])
            self.result_batches = []

        def complete_job_results(self, items):
            self.result_batches.append(list(items))
            return [b"OK"] * len(items)

    client = TypedResultClient()
    worker = QueueFlowWorker(client, type="typed", batch_size=2)

    result = worker.run_once(lambda job: job.id == "bool" if job.id == "bool" else 1)

    assert result.completed == 2
    assert client.completed == []
    assert [value for _job, value in client.result_batches[0]] == [True, 1]


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

    assert client.policies[0] == (
        "email",
        {"retry": queue_policy, "replace": False, "expected_generation": None},
    )
    assert client.policies[1] == (
        "sms",
        {
            "retry": default_policy,
            "states": None,
            "replace": False,
            "expected_generation": None,
        },
    )


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
        "ferric://example:6388",
        worker_config=WorkerConfig(workers=3, claim_connections=2),
    )

    worker = queue_client.queue(type="email").worker()

    assert calls == [
        ("ferric://example:6388", {"max_connections": 1}),
        ("ferric://example:6388", {"max_connections": 2}),
    ]
    assert worker.client is queue_client.flow
    assert worker.claim_client is queue_client.claim_flow
    assert queue_client.claim_flow is not queue_client.flow


def test_queue_client_from_protocol_url_separates_command_and_claim_clients(monkeypatch):
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

    assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 3]
    assert queue_client.claim_flow is not queue_client.flow
    assert worker.client is queue_client.flow
    assert worker.claim_client is queue_client.claim_flow


def test_flow_worker_from_protocol_url_creates_bounded_claim_client(monkeypatch):
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
        claim_connections=3,
    )

    assert [(url, kwargs) for url, kwargs, _client in calls] == [
        ("ferric://example:6388", {"max_connections": 1}),
        ("ferric://example:6388", {"max_connections": 3}),
    ]
    assert worker.claim_client is not worker.client


def test_queue_worker_config_at_queue_time_resizes_claim_pool(monkeypatch):
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

    assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 1, 16]
    assert worker.client is queue_client.flow
    assert worker.claim_client is calls[-1][2]


def test_queue_worker_config_reuses_matching_claim_pool(monkeypatch):
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
    second_worker = queue_client.queue(
        type="sms",
        worker_config=WorkerConfig(workers=16),
    ).worker()

    assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 1, 16]
    assert worker.client is queue_client.flow
    assert worker.claim_client is calls[-1][2]
    assert second_worker.claim_client is worker.claim_client


def test_protocol_queue_client_respects_explicit_command_connections(monkeypatch):
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
        worker_config=WorkerConfig(workers=16, command_connections=3),
    )

    assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [3, 16]
    assert queue_client.claim_flow is not queue_client.flow


def test_queue_client_close_does_not_close_externally_owned_clients():
    flow = FakeFlowClient([])
    claim_flow = FakeFlowClient([])

    queue_client = QueueClient(flow, claim_client=claim_flow)
    queue_client.close()

    assert not hasattr(flow, "closed")
    assert not hasattr(claim_flow, "closed")


def test_queue_client_close_attempts_every_owned_resource_after_failure():
    closed = []

    class CloseClient(FakeFlowClient):
        def __init__(self, name, *, fail_once=False):
            super().__init__([])
            self.name = name
            self.fail_once = fail_once
            self.close_calls = 0

        def close(self):
            self.close_calls += 1
            closed.append(self.name)
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError(f"{self.name} close failed")

    flow = CloseClient("flow")
    claim = CloseClient("claim")
    extra = CloseClient("extra", fail_once=True)
    client = QueueClient(flow, claim_client=claim)
    client._owns_flow = True
    client._owns_claim_flow = True
    client._owned_extra_claim_flows.append(extra)
    client._claim_flows_by_size[99] = extra

    with pytest.raises(RuntimeError, match="extra close failed"):
        client.close()

    assert closed == ["extra", "claim", "flow"]
    assert client._owned_extra_claim_flows == [extra]
    assert client._claim_flows_by_size == {}
    assert client._owns_claim_flow is False
    assert client._owns_flow is False

    client.close()

    assert closed == ["extra", "claim", "flow", "extra"]
    assert client._owned_extra_claim_flows == []


def test_queue_client_close_prevents_new_owned_claim_pools(monkeypatch):
    opened: list[FakeFlowClient] = []

    def from_url(_url, **_kwargs):
        client = FakeFlowClient([])
        opened.append(client)
        return client

    monkeypatch.setattr(worker_module.FlowClient, "from_url", staticmethod(from_url))
    client = QueueClient.from_url("ferric://seed.local:6388")
    client.close()

    with pytest.raises(RuntimeError, match="closed"):
        client.queue(type="email", worker_config=WorkerConfig(workers=4))

    assert len(opened) == 2


def test_queue_client_close_waits_for_inflight_owned_claim_pool_creation(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    close_done = threading.Event()
    opened: list[FakeFlowClient] = []
    errors: list[BaseException] = []

    def from_url(_url, **_kwargs):
        entered.set()
        if not release.wait(timeout=2):
            raise TimeoutError("claim-pool test release timed out")
        client = FakeFlowClient([])
        client.closed = False
        opened.append(client)
        return client

    monkeypatch.setattr(worker_module.FlowClient, "from_url", staticmethod(from_url))
    flow = FakeFlowClient([])
    claim_flow = FakeFlowClient([])
    client = QueueClient(flow, claim_client=claim_flow)
    client._url = "ferric://seed.local:6388"
    client._claim_client_explicit = False
    client._owns_flow = True
    client._owns_claim_flow = True

    def create_queue() -> None:
        try:
            client.queue(type="email", worker_config=WorkerConfig(workers=4))
        except BaseException as exc:
            errors.append(exc)

    def close_client() -> None:
        try:
            client.close()
        except BaseException as exc:
            errors.append(exc)
        finally:
            close_done.set()

    create_thread = threading.Thread(target=create_queue)
    close_thread = threading.Thread(target=close_client)
    create_thread.start()
    assert entered.wait(timeout=1)
    close_thread.start()

    close_waited = not close_done.wait(timeout=0.05)
    release.set()
    create_thread.join(timeout=1)
    close_thread.join(timeout=1)

    assert close_waited is True
    assert not create_thread.is_alive()
    assert not close_thread.is_alive()
    assert errors == []
    assert len(opened) == 1
    assert opened[0].closed is True
    assert client._owned_extra_claim_flows == []


def test_worker_connection_counts_reject_zero_limits():
    with pytest.raises(ValueError, match="command_connections"):
        resolve_worker_connection_counts(workers=4, command_connections=0)

    with pytest.raises(ValueError, match="claim_connections"):
        resolve_worker_connection_counts(workers=4, claim_connections=0)
