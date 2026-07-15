import asyncio
import threading
from typing import Any

import pytest

import ferricstore.async_queue_api as async_queue_api_module
import ferricstore.async_queue_runtime as async_queue_runtime_module
import ferricstore.async_worker as async_worker_module
import ferricstore.async_workflow_runtime as async_workflow_runtime_module
import ferricstore.workflow as workflow_module
from ferricstore import (
    AsyncFlowClient,
    AsyncQueueClient,
    AsyncQueueFlow,
    AsyncQueueFlowWorker,
    AsyncWorkflow,
    AsyncWorkflowClient,
    AsyncWorkflowContext,
    AsyncWorkflowEffect,
    BudgetPolicy,
    ChildSpec,
    ExceptionPolicy,
    FerricStoreError,
    RetryPolicy,
    ValueConfig,
    WorkerConfig,
    complete,
    fail,
    retry,
    transition,
)
from ferricstore.async_wake import AsyncFlowWakeCoordinator
from ferricstore.types import ClaimedFlow


class FakeExecutor:
    def __init__(self):
        self.calls = []
        self.closed = False

    async def execute_command(self, *args):
        self.calls.append(args)
        if args[0] in {"FLOW.EFFECT.RESERVE", "FLOW.EFFECT.CONFIRM", "FLOW.EFFECT.FAIL"}:
            status = {
                "FLOW.EFFECT.RESERVE": b"reserved",
                "FLOW.EFFECT.CONFIRM": b"confirmed",
                "FLOW.EFFECT.FAIL": b"failed",
            }[args[0]]
            effect_key = args[args.index("EFFECT_KEY") + 1]
            effect_type = (
                args[args.index("EFFECT_TYPE") + 1] if "EFFECT_TYPE" in args else b"external"
            )
            return {
                b"id": b"f1:effect",
                b"flow_id": args[1].encode() if isinstance(args[1], str) else args[1],
                b"effect_key": effect_key.encode() if isinstance(effect_key, str) else effect_key,
                b"effect_type": effect_type.encode()
                if isinstance(effect_type, str)
                else effect_type,
                b"status": status,
                b"decision": b"allowed",
            }
        if args[0] in {"FLOW.BUDGET.RESERVE", "FLOW.BUDGET.COMMIT", "FLOW.BUDGET.RELEASE"}:
            status = {
                "FLOW.BUDGET.RESERVE": b"reserved",
                "FLOW.BUDGET.COMMIT": b"committed",
                "FLOW.BUDGET.RELEASE": b"released",
            }[args[0]]
            actual_amount = (
                args[args.index("ACTUAL_AMOUNT") + 1] if "ACTUAL_AMOUNT" in args else None
            )
            return {
                b"scope": args[1].encode() if isinstance(args[1], str) else args[1],
                b"limit": 100,
                b"window_ms": 60_000,
                b"window_start_ms": 1_000,
                b"used": actual_amount if actual_amount is not None else 10,
                b"remaining": 90,
                b"over_budget": False,
                b"reservations_count": 1,
                b"reservation_id": b"budget-res-1",
                b"reserved_amount": 10,
                b"actual_amount": actual_amount,
                b"status": status,
                b"overage_amount": 0,
            }
        if args[0] == "FLOW.CLAIM_DUE":
            return [[b"f1", b"p1", b"lease", 7]]
        if args[0] in {"FLOW.COMPLETE_MANY", "FLOW.TRANSITION_MANY"}:
            return [b"OK"]
        return b"OK"

    async def close(self):
        self.closed = True


class ValueClaimExecutor(FakeExecutor):
    async def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "FLOW.CLAIM_DUE":
            return [
                {
                    b"id": b"f1",
                    b"type": b"order",
                    b"state": b"running",
                    b"run_state": b"queued",
                    b"partition_key": b"p1",
                    b"lease_token": b"lease",
                    b"fencing_token": 7,
                    b"values": {b"cached": b"one"},
                    b"value_refs": {b"remote": {b"ref": b"ref-remote"}},
                }
            ]
        if args[0] == "FLOW.VALUE.MGET":
            return [b"two"]
        if args[0] == "FLOW.COMPLETE_MANY":
            return [b"OK"]
        return b"OK"


@pytest.mark.parametrize(
    ("module", "constructor"),
    [
        (async_queue_api_module, "queue_flow"),
        (async_queue_api_module, "queue_client"),
        (async_queue_api_module, "queue_client_from_url"),
        (async_queue_runtime_module, "queue_worker"),
        (async_workflow_runtime_module, "workflow"),
        (async_workflow_runtime_module, "workflow_client"),
        (async_workflow_runtime_module, "workflow_client_from_url"),
    ],
)
def test_async_owned_construction_rolls_back_first_client(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    constructor: str,
) -> None:
    class OwnedClient:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    opened: list[OwnedClient] = []

    def from_url(_url: str, **_kwargs: Any) -> OwnedClient:
        if opened:
            raise OSError("claim connection failed")
        client = OwnedClient()
        opened.append(client)
        return client

    monkeypatch.setattr(module.AsyncFlowClient, "from_url", staticmethod(from_url))

    with pytest.raises(OSError, match="claim connection failed"):
        if constructor == "queue_flow":
            AsyncQueueFlow("ferric://seed.local:6388", type="order")
        elif constructor == "queue_client":
            AsyncQueueClient("ferric://seed.local:6388")
        elif constructor == "queue_client_from_url":
            AsyncQueueClient.from_url("ferric://seed.local:6388")
        elif constructor == "queue_worker":
            AsyncQueueFlowWorker("ferric://seed.local:6388", type="order")
        elif constructor == "workflow":
            AsyncWorkflow("ferric://seed.local:6388", type="order")
        elif constructor == "workflow_client":
            AsyncWorkflowClient("ferric://seed.local:6388")
        else:
            AsyncWorkflowClient.from_url("ferric://seed.local:6388")

    assert len(opened) == 1
    assert opened[0].closed is True


@pytest.mark.parametrize(
    ("module", "constructor"),
    [
        (async_queue_api_module, "queue_flow"),
        (async_queue_runtime_module, "queue_worker"),
    ],
)
def test_async_owned_construction_rolls_back_after_late_initialization_failure(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    constructor: str,
) -> None:
    class OwnedClient:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    opened: list[OwnedClient] = []

    def from_url(_url: str, **_kwargs: Any) -> OwnedClient:
        client = OwnedClient()
        opened.append(client)
        return client

    def fail_wake_coordinator(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("wake initialization failed")

    monkeypatch.setattr(module.AsyncFlowClient, "from_url", staticmethod(from_url))
    monkeypatch.setattr(module, "AsyncFlowWakeCoordinator", fail_wake_coordinator)

    with pytest.raises(OSError, match="wake initialization failed"):
        if constructor == "queue_flow":
            AsyncQueueFlow(
                "ferric://seed.local:6388",
                type="order",
                protocol_wake_hints=True,
            )
        else:
            AsyncQueueFlowWorker(
                "ferric://seed.local:6388",
                type="order",
                protocol_wake_hints=True,
            )

    assert len(opened) == 2
    assert all(client.closed for client in opened)


@pytest.mark.parametrize(
    ("module", "constructor"),
    [
        (async_queue_api_module, "queue_client"),
        (async_workflow_runtime_module, "workflow"),
        (async_workflow_runtime_module, "workflow_client"),
    ],
)
def test_async_owned_construction_validates_values_before_opening_clients(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    constructor: str,
) -> None:
    opened: list[object] = []

    def from_url(_url: str, **_kwargs: Any) -> object:
        client = object()
        opened.append(client)
        return client

    def fail_value_config() -> None:
        raise ValueError("value configuration failed")

    monkeypatch.setattr(module.AsyncFlowClient, "from_url", staticmethod(from_url))
    monkeypatch.setattr(module, "ValueConfig", fail_value_config)

    with pytest.raises(ValueError, match="value configuration failed"):
        if constructor == "queue_client":
            AsyncQueueClient("ferric://seed.local:6388")
        elif constructor == "workflow":
            AsyncWorkflow("ferric://seed.local:6388", type="order")
        else:
            AsyncWorkflowClient("ferric://seed.local:6388")

    assert opened == []


def test_async_owned_rollback_finishes_inside_active_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OwnedClient:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    opened: list[OwnedClient] = []

    def from_url(_url: str, **_kwargs: Any) -> OwnedClient:
        if opened:
            raise OSError("claim connection failed")
        client = OwnedClient()
        opened.append(client)
        return client

    monkeypatch.setattr(
        async_queue_api_module.AsyncFlowClient,
        "from_url",
        staticmethod(from_url),
    )

    async def run() -> None:
        with pytest.raises(OSError, match="claim connection failed"):
            AsyncQueueClient("ferric://seed.local:6388")
        await asyncio.sleep(0)

    asyncio.run(run())

    assert len(opened) == 1
    assert opened[0].closed is True


@pytest.mark.parametrize("workers", [1, 16, 57, 128, 256])
def test_auto_partition_ownership_is_complete_and_non_empty(workers):
    assignments = [
        async_worker_module._owned_auto_partition_keys(
            worker_index=worker_index,
            workers=workers,
            server_shards=16,
        )
        for worker_index in range(workers)
    ]

    assert all(assignments)
    flattened = [key for assignment in assignments for key in assignment]
    assert len(flattened) == async_worker_module.AUTO_PARTITION_BUCKETS
    assert len(set(flattened)) == async_worker_module.AUTO_PARTITION_BUCKETS


def test_auto_partition_ownership_keeps_each_worker_shard_local():
    workers = 128
    assignments = [
        async_worker_module._owned_auto_partition_keys(
            worker_index=worker_index,
            workers=workers,
            server_shards=16,
        )
        for worker_index in range(workers)
    ]

    for assignment in assignments:
        shards = {
            async_worker_module._auto_partition_server_shard(
                int(key.removeprefix(async_worker_module.AUTO_PARTITION_PREFIX)),
                16,
            )
            for key in assignment
        }
        assert len(shards) == 1


def test_auto_partition_assignment_plan_is_cached():
    async_worker_module._auto_partition_assignments.cache_clear()

    first = async_worker_module._auto_partition_assignments(57, 16)
    second = async_worker_module._auto_partition_assignments(57, 16)

    assert first is second
    assert async_worker_module._auto_partition_assignments.cache_info().hits == 1


@pytest.mark.parametrize("invalid", [True, 1.0])
def test_auto_partition_cache_does_not_alias_invalid_shard_counts(invalid: Any) -> None:
    async_worker_module._auto_partition_assignments.cache_clear()
    async_worker_module._auto_partition_assignments(1, 1)

    with pytest.raises(ValueError, match="server_shards"):
        async_worker_module._auto_partition_assignments(1, invalid)


def test_auto_partition_workers_cannot_exceed_bucket_count():
    with pytest.raises(ValueError, match="cannot exceed"):
        async_worker_module._owned_auto_partition_keys(
            worker_index=0,
            workers=async_worker_module.AUTO_PARTITION_BUCKETS + 1,
            server_shards=16,
        )


def test_async_queue_worker_rejects_empty_auto_partition_ownership(monkeypatch):
    monkeypatch.setattr(
        "ferricstore.async_queue_runtime._owned_auto_partition_keys",
        lambda **_: [],
    )

    with pytest.raises(ValueError, match="no auto partitions"):
        AsyncQueueFlowWorker(
            FakeExecutor(),
            type="order",
            auto_partitions=True,
        )


@pytest.mark.parametrize("runtime", [AsyncQueueFlow, AsyncWorkflow])
def test_async_multiworker_runtimes_reject_more_workers_than_auto_partitions(runtime):
    with pytest.raises(ValueError, match="cannot exceed"):
        runtime(
            FakeExecutor(),
            type="order",
            workers=async_worker_module.AUTO_PARTITION_BUCKETS + 1,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"concurrency": 0}, "concurrency must be positive"),
        ({"batch_size": 0}, "batch_size must be positive"),
        ({"claim_partition_batch_size": 0}, "claim_partition_batch_size must be positive"),
        ({"idle_sleep_s": -0.001}, "idle_sleep_s must be non-negative"),
        ({"max_idle_sleep_s": -0.001}, "max_idle_sleep_s must be non-negative"),
    ],
)
def test_async_queue_rejects_invalid_runtime_configuration_before_creating_clients(
    monkeypatch,
    kwargs,
    message,
):
    created = []

    def from_url(*args, **options):
        created.append((args, options))
        return FakeExecutor()

    monkeypatch.setattr(
        "ferricstore.async_queue_runtime.AsyncFlowClient.from_url",
        staticmethod(from_url),
    )

    with pytest.raises(ValueError, match=message):
        AsyncQueueFlow("ferric://127.0.0.1:6388", type="order", **kwargs)

    assert created == []


@pytest.mark.parametrize(
    ("field", "invalid", "message"),
    [
        ("concurrency", 1.5, "concurrency must be a positive integer"),
        ("concurrency", True, "concurrency must be a positive integer"),
        ("batch_size", float("nan"), "batch_size must be a positive integer"),
        ("workers", "2", "workers must be a positive integer"),
        (
            "claim_partition_batch_size",
            1.5,
            "claim_partition_batch_size must be a positive integer",
        ),
        ("block_ms", True, "block_ms must be a non-negative integer"),
    ],
)
def test_async_queue_rejects_non_integer_runtime_configuration(
    field: str,
    invalid: Any,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        AsyncQueueFlow(FakeExecutor(), type="order", **{field: invalid})


@pytest.mark.parametrize(
    ("runtime", "field", "value"),
    [
        ("queue", "claim_values", "payload"),
        ("queue_worker", "states", "queued"),
        ("queue_worker", "partition_keys", "tenant"),
        ("workflow", "states", "queued"),
        ("workflow", "partition_by", "tenant"),
        ("workflow", "claim_values", "payload"),
    ],
)
def test_async_runtimes_reject_scalar_strings_for_sequence_configuration(
    runtime: str,
    field: str,
    value: str,
) -> None:
    kwargs: dict[str, Any] = {field: value}

    with pytest.raises(ValueError, match=field):
        if runtime == "queue":
            AsyncQueueFlow(FakeExecutor(), type="order", **kwargs)
        elif runtime == "queue_worker":
            AsyncQueueFlowWorker(FakeExecutor(), type="order", **kwargs)
        else:
            AsyncWorkflow(FakeExecutor(), type="order", **kwargs)


@pytest.mark.parametrize("invalid", [0, -1, True, 1.5, 1025])
@pytest.mark.parametrize("runtime", ["queue", "queue_worker", "workflow"])
def test_async_runtimes_reject_invalid_server_shard_topology(
    runtime: str,
    invalid: Any,
) -> None:
    with pytest.raises(ValueError, match="server_shards"):
        if runtime == "queue":
            AsyncQueueFlow(FakeExecutor(), type="order", server_shards=invalid)
        elif runtime == "queue_worker":
            AsyncQueueFlowWorker(FakeExecutor(), type="order", server_shards=invalid)
        else:
            AsyncWorkflow(FakeExecutor(), type="order", server_shards=invalid)


def test_async_queue_builds_wake_partition_keys_only_when_hints_are_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[int] = []

    def partition_key(index: int) -> str:
        built.append(index)
        return f"partition:{index}"

    monkeypatch.setattr(async_queue_api_module, "_auto_partition_key", partition_key)

    AsyncQueueFlow(FakeExecutor(), type="order")
    assert built == []

    enabled = AsyncQueueFlow(FakeExecutor(), type="order", protocol_wake_hints=True)
    assert built == list(range(async_queue_runtime_module.AUTO_PARTITION_BUCKETS))
    assert enabled._wake_coordinator is not None


@pytest.mark.parametrize(
    ("runtime", "kwargs"),
    [
        ("queue", {"claim_values": [""]}),
        ("queue_worker", {"states": [1]}),
        ("workflow", {"partition_by": [b"tenant"]}),
    ],
)
def test_async_runtimes_reject_invalid_sequence_items(
    runtime: str,
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(ValueError, match="non-empty strings"):
        if runtime == "queue":
            AsyncQueueFlow(FakeExecutor(), type="order", **kwargs)
        elif runtime == "queue_worker":
            AsyncQueueFlowWorker(FakeExecutor(), type="order", **kwargs)
        else:
            AsyncWorkflow(FakeExecutor(), type="order", **kwargs)


def test_async_workflow_rejects_negative_idle_sleep_before_creating_clients(monkeypatch):
    created = []

    def from_url(*args, **options):
        created.append((args, options))
        return FakeExecutor()

    monkeypatch.setattr(
        "ferricstore.async_workflow_runtime.AsyncFlowClient.from_url",
        staticmethod(from_url),
    )

    with pytest.raises(ValueError, match="idle_sleep_s must be non-negative"):
        AsyncWorkflow(
            "ferric://127.0.0.1:6388",
            type="order",
            idle_sleep_s=-0.001,
        )

    assert created == []


def test_async_queue_worker_standalone_run_once_does_not_prefetch_leased_jobs():
    class FusedClient:
        def __init__(self) -> None:
            self.claim_calls = 0
            self.fused_calls = 0
            self.complete_calls = 0

        async def claim_flows(self, *_args, **_kwargs):
            self.claim_calls += 1
            return [ClaimedFlow("first", b"lease-1", 1, partition_key="p1")]

        async def complete_flows_and_claim_flows(self, jobs, **_kwargs):
            self.fused_calls += 1
            assert [job.id for job in jobs] == ["first"]
            return [ClaimedFlow("second", b"lease-2", 2, partition_key="p1")]

        async def complete_jobs(self, *_args, **_kwargs):
            self.complete_calls += 1

    async def run() -> None:
        client = FusedClient()
        worker = AsyncQueueFlowWorker(
            client,
            type="order",
            partition_key="p1",
            fuse_complete_claim=True,
        )

        result = await worker.run_once(lambda _job: b"done")

        assert result.claimed == 1
        assert result.completed == 1
        assert client.claim_calls == 1
        assert client.fused_calls == 0
        assert client.complete_calls == 1
        assert worker._prefetched_jobs == []

    asyncio.run(run())


def test_async_queue_worker_handler_fanout_creates_only_bounded_workers(monkeypatch):
    original_gather = asyncio.gather
    gather_widths = []

    async def tracking_gather(*awaitables, **kwargs):
        gather_widths.append(len(awaitables))
        return await original_gather(*awaitables, **kwargs)

    monkeypatch.setattr(async_worker_module.asyncio, "gather", tracking_gather)

    async def run():
        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(FakeExecutor()),
            type="email",
            concurrency=4,
        )
        jobs = [
            ClaimedFlow(f"f{index}", b"lease", index, partition_key="p1") for index in range(100)
        ]

        handled = await worker._run_handlers(jobs, lambda job: job.id)

        assert handled.failures == []
        assert handled.jobs == jobs

    asyncio.run(run())
    assert gather_widths
    assert max(gather_widths) <= 4


def test_async_queue_worker_fused_claim_advances_partition_cursor():
    class FusedClient:
        def __init__(self) -> None:
            self.claim_targets = []
            self.fused_targets = []
            self.completed = []

        async def claim_flows(self, *_args, **kwargs):
            self.claim_targets.append(kwargs.get("partition_key"))
            return [ClaimedFlow("first", b"lease-1", 1, partition_key="p1")]

        async def complete_flows_and_claim_flows(self, _jobs, **kwargs):
            self.fused_targets.append(kwargs.get("partition_key"))
            return [ClaimedFlow("second", b"lease-2", 2, partition_key="p2")]

        async def complete_jobs(self, jobs, **_kwargs):
            self.completed.extend(job.id for job in jobs)
            return [b"OK"] * len(jobs)

    async def run() -> None:
        client = FusedClient()
        worker = AsyncQueueFlowWorker(
            client,
            type="order",
            partition_keys=["p1", "p2"],
            claim_partition_batch_size=1,
            fuse_complete_claim=True,
        )

        seen: list[str] = []

        async def handler(job: ClaimedFlow) -> bytes:
            seen.append(job.id)
            if job.id == "second":
                worker.stop()
            return b"done"

        await asyncio.wait_for(worker.run_forever(handler), timeout=1)

        assert client.claim_targets == ["p1"]
        assert client.fused_targets == ["p2"]
        assert client.completed == ["second"]
        assert seen == ["first", "second"]

    asyncio.run(run())


def test_async_queue_worker_subscribes_to_protocol_wake_hints():
    class WakeClient:
        def __init__(self) -> None:
            self.subscriptions = []

        async def claim_flows(self, *_args, **_kwargs):
            return []

        async def subscribe_flow_wake(self, *args, **kwargs):
            self.subscriptions.append((args, kwargs))
            return b"OK"

        async def wait_event(self, timeout=None):
            return None

    async def run() -> None:
        client = WakeClient()
        worker = AsyncQueueFlowWorker(
            client,
            type="order",
            state="queued",
            protocol_wake_hints=True,
        )

        await worker._subscribe_protocol_wake_hints()

        assert worker._protocol_wake_hints_enabled
        assert client.subscriptions == [
            (
                ("order",),
                {
                    "state": "queued",
                    "states": None,
                    "partition_key": None,
                    "partition_keys": None,
                    "priority": 0,
                    "limit": 10,
                },
            )
        ]

    asyncio.run(run())


def test_async_queue_worker_retries_failed_wake_subscription():
    class WakeClient:
        def __init__(self) -> None:
            self.attempts = 0

        async def subscribe_flow_wake(self, *_args, **_kwargs):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("temporary subscription failure")
            return b"OK"

        async def wait_event(self, timeout=None):
            return None

    async def run() -> None:
        client = WakeClient()
        worker = AsyncQueueFlowWorker(
            client,
            type="order",
            protocol_wake_hints=True,
        )

        with pytest.raises(RuntimeError, match="temporary"):
            await worker._subscribe_protocol_wake_hints()
        assert not worker._protocol_wake_hints_subscribed

        await worker._subscribe_protocol_wake_hints()

        assert client.attempts == 2
        assert worker._protocol_wake_hints_enabled

    asyncio.run(run())


def test_async_queue_workers_share_one_union_wake_subscription():
    class WakeClient:
        def __init__(self) -> None:
            self.subscriptions = []

        async def subscribe_flow_wake(self, *args, **kwargs):
            self.subscriptions.append((args, kwargs))
            return b"OK"

        async def wait_event(self, timeout=None):
            await asyncio.Future()

    async def run() -> None:
        client = WakeClient()
        queue = AsyncQueueFlow(
            client,
            type="order",
            workers=3,
            protocol_wake_hints=True,
        )
        workers = [queue._build_worker(index) for index in range(queue.workers)]
        queue._workers = workers
        try:
            await asyncio.gather(*(worker._subscribe_protocol_wake_hints() for worker in workers))

            assert len(client.subscriptions) == 1
            args, kwargs = client.subscriptions[0]
            assert args == ("order",)
            assert kwargs["partition_key"] is None
            assert set(kwargs["partition_keys"]) == {
                async_worker_module._auto_partition_key(index)
                for index in range(async_worker_module.AUTO_PARTITION_BUCKETS)
            }
        finally:
            await queue.close()

    asyncio.run(run())


def test_async_queue_wake_event_is_broadcast_to_all_waiting_workers():
    class WakeClient:
        def __init__(self) -> None:
            self.events = asyncio.Queue()
            self.active_waits = 0
            self.max_active_waits = 0

        async def subscribe_flow_wake(self, *_args, **_kwargs):
            return b"OK"

        async def wait_event(self, timeout=None):
            self.active_waits += 1
            self.max_active_waits = max(self.max_active_waits, self.active_waits)
            try:
                if timeout is None:
                    return await self.events.get()
                try:
                    return await asyncio.wait_for(self.events.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    return None
            finally:
                self.active_waits -= 1

    async def run() -> None:
        client = WakeClient()
        queue = AsyncQueueFlow(
            client,
            type="order",
            workers=2,
            protocol_wake_hints=True,
        )
        workers = [queue._build_worker(index) for index in range(queue.workers)]
        queue._workers = workers
        try:
            await asyncio.gather(*(worker._subscribe_protocol_wake_hints() for worker in workers))
            waits = [
                asyncio.create_task(worker._wait_for_protocol_wake_hint(0.2)) for worker in workers
            ]
            await asyncio.sleep(0)
            await client.events.put({"event": "FLOW_WAKE"})

            assert await asyncio.gather(*waits) == [True, True]
            assert client.max_active_waits == 1
        finally:
            await queue.close()

    asyncio.run(run())


def test_async_wake_coordinator_recovers_after_transient_event_failure():
    class WakeClient:
        def __init__(self) -> None:
            self.subscriptions = 0
            self.waits = 0
            self.events: asyncio.Queue[object] = asyncio.Queue()
            self.resubscribed = asyncio.Event()

        async def subscribe_flow_wake(self, *_args, **_kwargs):
            self.subscriptions += 1
            if self.subscriptions >= 2:
                self.resubscribed.set()
            return b"OK"

        async def wait_event(self, timeout=None):
            del timeout
            self.waits += 1
            if self.waits == 1:
                raise ConnectionError("transient disconnect")
            return await self.events.get()

    async def run() -> None:
        client = WakeClient()
        coordinator = AsyncFlowWakeCoordinator(
            client,
            type="order",
            state=None,
            states=None,
            partition_key=None,
            partition_keys=None,
            priority=0,
            limit=10,
            enabled=True,
        )
        try:
            assert await coordinator.subscribe()
            await asyncio.wait_for(client.resubscribed.wait(), timeout=1)

            assert client.subscriptions >= 2
            assert coordinator.enabled
            await client.events.put({"event": "FLOW_WAKE"})
            woke, generation = await coordinator.wait(0, 0.5)
            assert woke
            assert generation == 1
        finally:
            await coordinator.close()

    asyncio.run(run())


def test_async_wake_coordinator_owns_an_isolated_subscription_session():
    class DedicatedWakeClient:
        def __init__(self) -> None:
            self.subscriptions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
            self.events: asyncio.Queue[object] = asyncio.Queue()
            self.closed = False

        async def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> bytes:
            self.subscriptions.append((args, kwargs))
            return b"OK"

        async def wait_event(self, timeout: float | None = None) -> object:
            del timeout
            return await self.events.get()

        async def close(self) -> None:
            self.closed = True

    class SharedClaimClient:
        def __init__(self) -> None:
            self.subscriptions = 0
            self.dedicated = DedicatedWakeClient()

        async def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> bytes:
            self.subscriptions += 1
            return b"OK"

        async def wait_event(self, timeout: float | None = None) -> None:
            del timeout
            return None

        async def _acquire_subscription_client(self):
            return self.dedicated, True

    async def run() -> None:
        shared = SharedClaimClient()
        coordinator = AsyncFlowWakeCoordinator(
            shared,
            type="order",
            state=None,
            states=None,
            partition_key=None,
            partition_keys=None,
            priority=0,
            limit=10,
            enabled=True,
        )
        assert await coordinator.subscribe()
        assert shared.subscriptions == 0
        assert shared.dedicated.subscriptions

        await coordinator.close()

        assert shared.dedicated.closed

    asyncio.run(run())


def test_async_wake_coordinator_retries_owned_subscription_close():
    class DedicatedWakeClient:
        def __init__(self) -> None:
            self.close_attempts = 0

        async def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> bytes:
            return b"OK"

        async def wait_event(self, timeout: float | None = None) -> None:
            del timeout
            await asyncio.Future()

        async def close(self) -> None:
            self.close_attempts += 1
            if self.close_attempts == 1:
                raise RuntimeError("temporary close failure")

    class SharedClaimClient:
        def __init__(self) -> None:
            self.dedicated = DedicatedWakeClient()

        async def _acquire_subscription_client(self):
            return self.dedicated, True

    async def run() -> None:
        shared = SharedClaimClient()
        coordinator = AsyncFlowWakeCoordinator(
            shared,
            type="order",
            state=None,
            states=None,
            partition_key=None,
            partition_keys=None,
            priority=0,
            limit=10,
            enabled=True,
        )
        assert await coordinator.subscribe()

        with pytest.raises(RuntimeError, match="temporary close failure"):
            await coordinator.close()
        await coordinator.close()

        assert shared.dedicated.close_attempts == 2

    asyncio.run(run())


def test_async_wake_close_cancels_a_hung_resubscription():
    class WakeClient:
        def __init__(self) -> None:
            self.subscriptions = 0
            self.reconnect_entered = asyncio.Event()

        async def subscribe_flow_wake(self, *_args, **_kwargs):
            self.subscriptions += 1
            if self.subscriptions > 1:
                self.reconnect_entered.set()
                await asyncio.Event().wait()
            return b"OK"

        async def wait_event(self, timeout=None):
            del timeout
            raise ConnectionError("stream disconnected")

    async def run() -> None:
        client = WakeClient()
        coordinator = AsyncFlowWakeCoordinator(
            client,
            type="order",
            state=None,
            states=None,
            partition_key=None,
            partition_keys=None,
            priority=0,
            limit=10,
            enabled=True,
        )
        assert await coordinator.subscribe()
        await asyncio.wait_for(client.reconnect_entered.wait(), timeout=1)

        await asyncio.wait_for(coordinator.close(), timeout=0.1)

        assert not coordinator.enabled

    asyncio.run(run())


def test_async_wake_recovery_backoff_survives_successful_resubscriptions():
    class WakeClient:
        def __init__(self) -> None:
            self.subscription_times: list[float] = []
            self.enough_attempts = asyncio.Event()

        async def subscribe_flow_wake(self, *_args, **_kwargs):
            self.subscription_times.append(asyncio.get_running_loop().time())
            if len(self.subscription_times) >= 4:
                self.enough_attempts.set()
            return b"OK"

        async def wait_event(self, timeout=None):
            del timeout
            raise ConnectionError("stream disconnected")

    async def run() -> None:
        client = WakeClient()
        coordinator = AsyncFlowWakeCoordinator(
            client,
            type="order",
            state=None,
            states=None,
            partition_key=None,
            partition_keys=None,
            priority=0,
            limit=10,
            enabled=True,
        )
        coordinator._MIN_RECOVERY_DELAY_S = 0.01
        coordinator._MAX_RECOVERY_DELAY_S = 0.04
        try:
            assert await coordinator.subscribe()
            await asyncio.wait_for(client.enough_attempts.wait(), timeout=1)
        finally:
            await coordinator.close()

        intervals = [
            later - earlier
            for earlier, later in zip(
                client.subscription_times[:-1],
                client.subscription_times[1:],
                strict=True,
            )
        ]
        assert intervals[0] >= 0.008
        assert intervals[1] >= 0.018
        assert intervals[2] >= 0.035

    asyncio.run(run())


def test_async_worker_config_exposes_shared_scheduler_fast_paths():
    assert {
        "max_idle_sleep_s",
        "protocol_wake_hints",
        "fuse_complete_claim",
    } <= async_worker_module.ASYNC_QUEUE_WORKER_CONFIG_KEYS


def test_async_flow_client_uses_async_executor_commands():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)

        result = await client.create(
            "f1",
            type="order",
            payload=b"payload",
            now_ms=100,
            return_record=False,
        )
        assert result == b"OK"
        assert executor.calls[0][:4] == ("FLOW.CREATE", "f1", "TYPE", "order")

        assert await client.command("SET", "k", "v") == b"OK"
        assert executor.calls[-1] == ("SET", "k", "v")

    asyncio.run(run())


def test_async_queue_client_creates_queue_and_delegates_flow_commands():
    async def run():
        executor = FakeExecutor()
        client = AsyncQueueClient(AsyncFlowClient(executor))
        queue = client.queue(type="email")

        await queue.enqueue("e1", payload=b"body")
        assert executor.calls[0][:4] == ("FLOW.CREATE", "e1", "TYPE", "email")
        assert "STATE" in executor.calls[0]
        assert "queued" in executor.calls[0]

        assert await client.command("PING") == b"OK"
        assert executor.calls[-1] == ("PING",)
        assert queue.worker(workers=1).__class__.__name__ == "AsyncQueueFlow"

    asyncio.run(run())


def test_async_queue_client_retry_policy_is_inherited_and_can_be_overridden():
    async def run():
        executor = FakeExecutor()
        default_policy = RetryPolicy(max_retries=5)
        queue_policy = RetryPolicy(max_retries=2)
        client = AsyncQueueClient(AsyncFlowClient(executor), retry_policy=default_policy)
        queue = client.queue(type="email", retry_policy=queue_policy)

        await queue.install_policy()
        await client.install_policy("sms")

        assert executor.calls[0][:2] == ("FLOW.POLICY.SET", "email")
        assert queue_policy.max_retries in executor.calls[0]
        assert executor.calls[1][:2] == ("FLOW.POLICY.SET", "sms")
        assert default_policy.max_retries in executor.calls[1]

    asyncio.run(run())


def test_async_queue_client_worker_config_is_inherited_and_overridable():
    async def run():
        client = AsyncQueueClient(
            AsyncFlowClient(FakeExecutor()),
            worker_config=WorkerConfig(workers=3, concurrency=50, batch_size=100),
        )
        queue = client.queue(type="email")

        worker = queue.worker(batch_size=10)

        assert worker.workers == 3
        assert worker.concurrency == 50
        assert worker.batch_size == 10

    asyncio.run(run())


def test_async_queue_client_from_url_creates_bounded_command_and_claim_pools(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        calls.append((url, kwargs))
        return AsyncFlowClient(FakeExecutor())

    monkeypatch.setattr("ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url))

    client = AsyncQueueClient.from_url(
        "ferric://example:6388",
        worker_config=WorkerConfig(workers=4, claim_connections=2),
    )
    worker = client.queue(type="email").worker()._build_worker(0)

    assert calls == [
        ("ferric://example:6388", {"max_connections": 1}),
        ("ferric://example:6388", {"max_connections": 2}),
    ]
    assert worker.client is client.flow
    assert worker.claim_client is client.claim_flow
    assert client.claim_flow is not client.flow


def test_async_queue_client_from_protocol_url_separates_command_and_claim_clients(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = AsyncFlowClient(FakeExecutor())
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url))

    client = AsyncQueueClient.from_url(
        "ferric://example:6388",
        worker_config=WorkerConfig(workers=4),
    )
    worker = client.queue(type="email").worker()._build_worker(0)

    assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 4]
    assert client.claim_flow is not client.flow
    assert worker.client is client.flow
    assert worker.claim_client is client.claim_flow


def test_async_queue_worker_config_at_queue_time_resizes_claim_client(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = AsyncFlowClient(FakeExecutor())
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url))

    client = AsyncQueueClient.from_url("ferric://example:6388")
    worker = (
        client.queue(
            type="email",
            worker_config=WorkerConfig(workers=64),
        )
        .worker()
        ._build_worker(0)
    )

    assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 1, 64]
    assert worker.client is client.flow
    assert worker.claim_client is calls[-1][2]


def test_async_queue_worker_config_reuses_matching_claim_pool(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = AsyncFlowClient(FakeExecutor())
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url))

    client = AsyncQueueClient.from_url("ferric://example:6388")
    worker = (
        client.queue(
            type="email",
            worker_config=WorkerConfig(workers=64),
        )
        .worker()
        ._build_worker(0)
    )
    second_worker = (
        client.queue(
            type="sms",
            worker_config=WorkerConfig(workers=64),
        )
        .worker()
        ._build_worker(0)
    )

    assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 1, 64]
    assert worker.client is client.flow
    assert worker.claim_client is calls[-1][2]
    assert second_worker.claim_client is worker.claim_client


def test_async_queue_client_close_does_not_close_externally_owned_clients():
    async def run():
        flow_executor = FakeExecutor()
        claim_executor = FakeExecutor()
        client = AsyncQueueClient(
            AsyncFlowClient(flow_executor),
            claim_client=AsyncFlowClient(claim_executor),
        )

        await client.close()

        assert flow_executor.closed is False
        assert claim_executor.closed is False

    asyncio.run(run())


def test_async_queue_client_close_attempts_every_owned_resource_after_failure():
    class CloseExecutor(FakeExecutor):
        def __init__(self, name, closed, *, fail_once=False):
            super().__init__()
            self.name = name
            self.closed_names = closed
            self.fail_once = fail_once
            self.close_calls = 0

        async def close(self):
            self.close_calls += 1
            self.closed_names.append(self.name)
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError(f"{self.name} close failed")
            self.closed = True

    async def run():
        closed = []
        flow_executor = CloseExecutor("flow", closed)
        claim_executor = CloseExecutor("claim", closed)
        extra_executor = CloseExecutor("extra", closed, fail_once=True)
        flow = AsyncFlowClient(flow_executor)
        claim = AsyncFlowClient(claim_executor)
        extra = AsyncFlowClient(extra_executor)
        client = AsyncQueueClient(flow, claim_client=claim)
        client._owns_flow = True
        client._owns_claim_flow = True
        client._owned_extra_claim_flows.append(extra)
        client._claim_flows_by_size[99] = extra

        with pytest.raises(RuntimeError, match="extra close failed"):
            await client.close()

        assert set(closed) == {"extra", "claim", "flow"}
        assert client._owned_extra_claim_flows == [extra]
        assert client._claim_flows_by_size == {}
        assert client._owns_claim_flow is False
        assert client._owns_flow is False

        await client.close()

        assert extra_executor.close_calls == 2
        assert claim_executor.close_calls == 1
        assert flow_executor.close_calls == 1
        assert client._owned_extra_claim_flows == []

    asyncio.run(run())


def test_async_queue_client_cancelled_close_can_be_rejoined():
    class BlockingCloseExecutor(FakeExecutor):
        def __init__(self) -> None:
            super().__init__()
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def close(self):
            self.entered.set()
            await self.release.wait()
            self.closed = True

    async def run():
        executor = BlockingCloseExecutor()
        client = AsyncQueueClient(AsyncFlowClient(executor))
        client._owns_flow = True

        first = asyncio.create_task(client.close())
        await executor.entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        second = asyncio.create_task(client.close())
        try:
            await asyncio.sleep(0)
            assert second.done() is False
        finally:
            executor.release.set()
            await second

        assert executor.closed is True

    asyncio.run(run())


def test_async_queue_client_close_prevents_new_owned_claim_pools(monkeypatch):
    async def run():
        opened: list[AsyncFlowClient] = []

        def from_url(_url, **_kwargs):
            client = AsyncFlowClient(FakeExecutor())
            opened.append(client)
            return client

        monkeypatch.setattr(
            async_worker_module.AsyncFlowClient,
            "from_url",
            staticmethod(from_url),
        )
        client = AsyncQueueClient.from_url("ferric://seed.local:6388")
        await client.close()

        with pytest.raises(RuntimeError, match="closed"):
            client.queue(type="email", worker_config=WorkerConfig(workers=4))

        assert len(opened) == 2

    asyncio.run(run())


def test_async_queue_client_value_config_is_passed_to_worker():
    async def run():
        client = AsyncQueueClient(
            AsyncFlowClient(FakeExecutor()),
            worker_config=WorkerConfig(claim_values=["cached"]),
            value_config=ValueConfig(value_max_bytes=64_000),
        )
        queue = client.queue(type="email")

        worker = queue.worker(workers=1)

        assert worker.claim_values == ["cached"]
        assert worker.value_max_bytes == 64_000

    asyncio.run(run())


def test_async_workflow_client_creates_workflow_and_delegates_flow_commands():
    async def run():
        executor = FakeExecutor()
        client = AsyncWorkflowClient(AsyncFlowClient(executor))
        workflow = client.workflow(type="order", states=["queued"], initial_state="queued")

        await workflow.start_flow("o1", payload=b"payload")
        assert executor.calls[0][:4] == ("FLOW.CREATE", "o1", "TYPE", "order")
        assert "STATE" in executor.calls[0]
        assert "queued" in executor.calls[0]

        assert await client.command("PING") == b"OK"
        assert executor.calls[-1] == ("PING",)

    asyncio.run(run())


def test_async_workflow_rejects_duplicate_state_handlers():
    workflow = AsyncWorkflow(FakeExecutor(), type="order", states=["queued"])

    @workflow.on("queued")
    async def first(_ctx):
        return complete()

    with pytest.raises(ValueError, match="duplicate workflow state"):

        @workflow.on("queued")
        async def duplicate(_ctx):
            return complete()


def test_async_workflow_has_explicit_worker_start_and_deprecates_ambiguous_start():
    class EmptyClient:
        async def claim_flows(self, *_args, **_kwargs):
            return []

    async def run() -> None:
        workflow = AsyncWorkflow(EmptyClient(), type="order", states=["queued"])

        @workflow.on("queued")
        async def queued(_ctx):
            return complete()

        tasks = workflow.start_workers()
        assert isinstance(tasks, list)
        workflow.stop()
        await workflow.close()

        flow_workflow = AsyncWorkflow(FakeExecutor(), type="order", states=["queued"])
        with pytest.warns(DeprecationWarning, match="start_flow"):
            assert await flow_workflow.start("o1", payload=b"payload") == b"OK"
        await flow_workflow.close()

        legacy_workers = AsyncWorkflow(EmptyClient(), type="order", states=["queued"])

        @legacy_workers.on("queued")
        async def legacy_queued(_ctx):
            return complete()

        with pytest.warns(DeprecationWarning, match="start_workers"):
            legacy_tasks = legacy_workers.start()
        assert isinstance(legacy_tasks, list)
        legacy_workers.stop()
        await legacy_workers.close()

    asyncio.run(run())


def test_async_workflow_client_retry_policy_is_inherited_and_state_can_override():
    async def run():
        executor = FakeExecutor()
        default_policy = RetryPolicy(max_retries=5)
        state_policy = RetryPolicy(max_retries=2)
        client = AsyncWorkflowClient(AsyncFlowClient(executor), retry_policy=default_policy)
        workflow = client.workflow(type="order", states=["created"], initial_state="created")

        @workflow.state("created", retry_policy=state_policy)
        async def created(job):
            return transition("done")

        await workflow.install_policy()

        call = executor.calls[-1]
        assert call[:2] == ("FLOW.POLICY.SET", "order")
        assert "STATE" in call
        assert "created" in call
        assert default_policy.max_retries in call
        assert state_policy.max_retries in call

    asyncio.run(run())


def test_async_workflow_client_worker_and_value_config_are_inherited_and_overridable():
    async def run():
        client = AsyncWorkflowClient(
            AsyncFlowClient(FakeExecutor()),
            worker_config=WorkerConfig(workers=3, concurrency=50, batch_size=100),
            value_config=ValueConfig(value_max_bytes=64_000, local_cache=True),
        )

        workflow = client.workflow(
            type="order",
            states=["created"],
            initial_state="created",
            batch_size=10,
        )

        assert workflow.workers == 3
        assert workflow.concurrency == 50
        assert workflow.batch_size == 10
        assert workflow.value_max_bytes == 64_000
        assert workflow.value_config.local_cache is True

    asyncio.run(run())


def test_async_workflow_budget_policy_reserves_commits_and_stamps_attributes():
    async def run():
        executor = FakeExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            initial_state="queued",
            batch_size=1,
        )

        @workflow.state("queued", budget=BudgetPolicy(scope="tenant-a", amount=10, limit=100))
        async def queued(_job):
            return complete(result=b"ok")

        result = await workflow.run_once()

        assert result.applied == 1
        assert executor.calls[1][:4] == ("FLOW.BUDGET.RESERVE", "tenant-a", "AMOUNT", 10)
        assert executor.calls[2][:6] == (
            "FLOW.BUDGET.COMMIT",
            "tenant-a",
            "RESERVATION_ID",
            "budget-res-1",
            "ACTUAL_AMOUNT",
            10,
        )
        complete_call = executor.calls[3]
        assert complete_call[0] == "FLOW.COMPLETE_MANY"
        assert "governance_budget_scope" in complete_call
        assert "governance_budget_status" in complete_call

    asyncio.run(run())


def test_async_workflow_budget_reserve_failure_preserves_policy_and_skips_release():
    class ReserveFailExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.RESERVE":
                self.calls.append(args)
                raise RuntimeError("budget reserve failed")
            return await super().execute_command(*args)

    async def run():
        executor = ReserveFailExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            batch_size=1,
        )

        @workflow.state("queued", budget=BudgetPolicy(scope="tenant-a", amount=10))
        async def queued(_ctx):
            return complete()

        result = await workflow.run_once()

        assert result.applied == 1
        assert [call[0] for call in executor.calls] == [
            "FLOW.CLAIM_DUE",
            "FLOW.BUDGET.RESERVE",
            "FLOW.RETRY_MANY",
        ]
        retry_call = executor.calls[-1]
        assert retry_call[retry_call.index("ERROR") + 1] == b"budget reserve failed"

    asyncio.run(run())


def test_async_workflow_budget_commit_failure_releases_and_uses_error_policy():
    class CommitFailExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.COMMIT":
                self.calls.append(args)
                raise RuntimeError("budget commit failed")
            return await super().execute_command(*args)

    async def run():
        executor = CommitFailExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            batch_size=1,
        )

        @workflow.state("queued", budget=BudgetPolicy(scope="tenant-a", amount=10))
        async def queued(_ctx):
            return complete()

        result = await workflow.run_once()

        assert result.applied == 1
        assert [call[0] for call in executor.calls] == [
            "FLOW.CLAIM_DUE",
            "FLOW.BUDGET.RESERVE",
            "FLOW.BUDGET.COMMIT",
            "FLOW.BUDGET.RELEASE",
            "FLOW.RETRY_MANY",
        ]
        retry_call = executor.calls[-1]
        assert retry_call[retry_call.index("ERROR") + 1] == b"budget commit failed"

    asyncio.run(run())


def test_async_workflow_budget_commit_error_remains_primary_when_release_fails():
    class SettlementFailExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] in {"FLOW.BUDGET.COMMIT", "FLOW.BUDGET.RELEASE"}:
                self.calls.append(args)
                action = "commit" if args[0].endswith("COMMIT") else "release"
                raise RuntimeError(f"budget {action} failed")
            return await super().execute_command(*args)

    async def run():
        executor = SettlementFailExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            batch_size=1,
            exception_policy=ExceptionPolicy.RAISE,
        )

        @workflow.state("queued", budget=BudgetPolicy(scope="tenant-a", amount=10))
        async def queued(_ctx):
            return complete()

        with pytest.raises(RuntimeError, match="budget commit failed") as raised:
            await workflow.run_once()

        assert isinstance(raised.value.__cause__, RuntimeError)
        assert str(raised.value.__cause__) == "budget release failed"

    asyncio.run(run())


def test_async_workflow_budget_context_releases_after_clean_body_commit_failure():
    class CommitFailExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.COMMIT":
                self.calls.append(args)
                raise RuntimeError("budget commit failed")
            return await super().execute_command(*args)

    async def run():
        executor = CommitFailExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            exception_policy=ExceptionPolicy.RAISE,
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow("f1", b"lease", 1, partition_key="p1"),
            "queued",
        )

        with pytest.raises(RuntimeError, match="budget commit failed"):
            async with ctx.budget("tenant-a", 10):
                pass

        assert [call[0] for call in executor.calls] == [
            "FLOW.BUDGET.RESERVE",
            "FLOW.BUDGET.COMMIT",
            "FLOW.BUDGET.RELEASE",
        ]

    asyncio.run(run())


def test_async_workflow_budget_release_waits_for_inflight_commit():
    class BlockingCommitExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.commit_started = asyncio.Event()
            self.commit_allowed = asyncio.Event()
            self.commit_finished = False
            self.release_before_commit = False

        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.COMMIT":
                result = await super().execute_command(*args)
                self.commit_started.set()
                await self.commit_allowed.wait()
                self.commit_finished = True
                return result
            if args[0] == "FLOW.BUDGET.RELEASE":
                self.release_before_commit = not self.commit_finished
            return await super().execute_command(*args)

    async def run():
        executor = BlockingCommitExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow("f1", b"lease", 1, partition_key="p1"),
            "queued",
        )
        budget = ctx.budget("tenant-a", 10)
        await budget.__aenter__()

        commit_caller = asyncio.create_task(budget.commit())
        await executor.commit_started.wait()
        commit_caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await commit_caller

        release_caller = asyncio.create_task(budget.release())
        await asyncio.sleep(0)
        assert executor.release_before_commit is False

        executor.commit_allowed.set()
        await release_caller
        assert executor.release_before_commit is False
        assert [call[0] for call in executor.calls] == [
            "FLOW.BUDGET.RESERVE",
            "FLOW.BUDGET.COMMIT",
        ]

    asyncio.run(run())


def test_async_workflow_budget_releases_when_commit_operation_is_cancelled():
    class CancelledCommitExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.COMMIT":
                self.calls.append(args)
                raise asyncio.CancelledError
            return await super().execute_command(*args)

    async def run():
        executor = CancelledCommitExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            batch_size=1,
        )

        @workflow.state("queued", budget=BudgetPolicy(scope="tenant-a", amount=10))
        async def queued(_ctx):
            return complete()

        with pytest.raises(asyncio.CancelledError):
            await workflow.run_once()

        assert [call[0] for call in executor.calls] == [
            "FLOW.CLAIM_DUE",
            "FLOW.BUDGET.RESERVE",
            "FLOW.BUDGET.COMMIT",
            "FLOW.BUDGET.RELEASE",
        ]

    asyncio.run(run())


def test_async_workflow_budget_release_retries_after_operation_cancellation():
    class CancelFirstReleaseExecutor(FakeExecutor):
        def __init__(self) -> None:
            super().__init__()
            self.release_calls = 0

        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.RELEASE":
                self.release_calls += 1
                if self.release_calls == 1:
                    self.calls.append(args)
                    raise asyncio.CancelledError
            return await super().execute_command(*args)

    async def run():
        executor = CancelFirstReleaseExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow("f1", b"lease", 1, partition_key="p1"),
            "queued",
        )
        budget = ctx.budget("tenant-a", 10)
        await budget.__aenter__()

        with pytest.raises(asyncio.CancelledError):
            await budget.release()
        result = await budget.release()

        assert result.status == "released"
        assert executor.release_calls == 2

    asyncio.run(run())


def test_async_workflow_budget_release_failure_does_not_mask_handler_failure():
    class ReleaseFailExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.RELEASE":
                self.calls.append(args)
                raise RuntimeError("budget release failed")
            return await super().execute_command(*args)

    async def run():
        executor = ReleaseFailExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            batch_size=1,
            exception_policy=ExceptionPolicy.RAISE,
        )

        @workflow.state("queued", budget=BudgetPolicy(scope="tenant-a", amount=10))
        async def queued(_ctx):
            raise ValueError("handler failed")

        with pytest.raises(ValueError, match="handler failed") as raised:
            await workflow.run_once()

        assert isinstance(raised.value.__cause__, RuntimeError)
        assert str(raised.value.__cause__) == "budget release failed"

    asyncio.run(run())


def test_async_workflow_budget_context_cleanup_does_not_mask_body_failure():
    class ReleaseFailExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.RELEASE":
                self.calls.append(args)
                raise RuntimeError("budget release failed")
            return await super().execute_command(*args)

    async def run():
        workflow = AsyncWorkflow(
            AsyncFlowClient(ReleaseFailExecutor()),
            type="order",
            states=["queued"],
            exception_policy=ExceptionPolicy.RAISE,
        )

        @workflow.on("queued")
        async def queued(ctx):
            async with ctx.budget("tenant-a", 10):
                raise ValueError("budget body failed")

        with pytest.raises(ValueError, match="budget body failed") as raised:
            await workflow.run_once()

        assert isinstance(raised.value.__cause__, RuntimeError)
        assert str(raised.value.__cause__) == "budget release failed"

    asyncio.run(run())


def test_async_workflow_effect_cleanup_does_not_mask_body_failure():
    class FailReportExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.EFFECT.FAIL":
                self.calls.append(args)
                raise RuntimeError("effect fail reporting failed")
            return await super().execute_command(*args)

    async def run():
        workflow = AsyncWorkflow(
            AsyncFlowClient(FailReportExecutor()),
            type="order",
            states=["queued"],
            exception_policy=ExceptionPolicy.RAISE,
        )

        @workflow.on("queued")
        async def queued(ctx):
            async with ctx.effect("charge", "payment.charge"):
                raise ValueError("effect body failed")

        with pytest.raises(ValueError, match="effect body failed") as raised:
            await workflow.run_once()

        assert isinstance(raised.value.__cause__, RuntimeError)
        assert str(raised.value.__cause__) == "effect fail reporting failed"

    asyncio.run(run())


def test_async_workflow_budget_release_completes_after_caller_cancellation():
    class BlockingReleaseExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.release_started = asyncio.Event()
            self.release_allowed = asyncio.Event()

        async def execute_command(self, *args):
            if args[0] == "FLOW.BUDGET.RELEASE":
                self.release_started.set()
                await self.release_allowed.wait()
            return await super().execute_command(*args)

    async def run():
        executor = BlockingReleaseExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow("f1", b"lease", 1, partition_key="p1"),
            "queued",
        )
        budget = ctx.budget("tenant-a", 10)
        await budget.__aenter__()

        release_task = asyncio.create_task(budget.release())
        await executor.release_started.wait()
        release_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release_task

        executor.release_allowed.set()
        for _ in range(10):
            if budget._closed:
                break
            await asyncio.sleep(0)
        assert budget._closed is True

    asyncio.run(run())


def test_async_workflow_context_budget_allows_explicit_actual_usage():
    async def run():
        executor = FakeExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            initial_state="queued",
            batch_size=1,
        )

        @workflow.state("queued")
        async def queued(ctx):
            async with ctx.budget("tenant-a", 10, limit=100) as budget:
                await budget.commit(7, usage={"tokens": 7})
            return transition("next")

        result = await workflow.run_once()

        assert result.applied == 1
        assert executor.calls[1][:4] == ("FLOW.BUDGET.RESERVE", "tenant-a", "AMOUNT", 10)
        assert executor.calls[2] == (
            "FLOW.BUDGET.COMMIT",
            "tenant-a",
            "RESERVATION_ID",
            "budget-res-1",
            "ACTUAL_AMOUNT",
            7,
            "USAGE",
            {"tokens": 7},
        )
        transition_call = executor.calls[3]
        assert transition_call[0] == "FLOW.TRANSITION_MANY"
        assert "governance_budget_actual_amount" in transition_call
        assert 7 in transition_call

    asyncio.run(run())


def test_async_workflow_context_effect_decorator_reserves_and_confirms():
    async def run():
        executor = FakeExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            initial_state="queued",
            batch_size=1,
        )

        @workflow.state("queued")
        async def queued(ctx):
            effect = ctx.effect(
                "charge",
                "payment.charge",
                operation_digest="charge:v1",
                external_id=lambda result: result["id"],
            )
            assert isinstance(effect, AsyncWorkflowEffect)

            @effect
            async def charge():
                return {"id": "ch_1"}

            await charge()
            return complete(result=b"ok")

        result = await workflow.run_once()

        assert result.applied == 1
        reserve_call = executor.calls[1]
        assert reserve_call[:4] == ("FLOW.EFFECT.RESERVE", "f1", "EFFECT_KEY", "charge")
        assert reserve_call[reserve_call.index("EFFECT_TYPE") + 1] == "payment.charge"
        assert reserve_call[reserve_call.index("OPERATION_DIGEST") + 1] == "charge:v1"
        assert reserve_call[reserve_call.index("LEASE_TOKEN") + 1] == b"lease"
        assert reserve_call[reserve_call.index("FENCING") + 1] == 7

        confirm_call = executor.calls[2]
        assert confirm_call[:4] == ("FLOW.EFFECT.CONFIRM", "f1", "EFFECT_KEY", "charge")
        assert confirm_call[confirm_call.index("EXTERNAL_ID") + 1] == "ch_1"
        assert isinstance(confirm_call[confirm_call.index("LATENCY_MS") + 1], int)

    asyncio.run(run())


def test_async_workflow_effect_auto_latency_starts_after_reserve(monkeypatch):
    class FakeLoop:
        def __init__(self) -> None:
            self.ticks = iter([10.0, 10.25])

        def time(self) -> float:
            return next(self.ticks)

    async def run():
        executor = FakeExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            initial_state="queued",
            batch_size=1,
        )
        job = ClaimedFlow(
            id="f1",
            type="order",
            state="queued",
            partition_key="tenant-a",
            lease_token=b"lease",
            fencing_token=7,
        )
        ctx = async_worker_module.AsyncWorkflowContext(workflow, job, "queued")
        effect = ctx.effect("charge", "payment.charge", operation_digest="charge:v1")

        fake_loop = FakeLoop()
        monkeypatch.setattr(async_worker_module.asyncio, "get_running_loop", lambda: fake_loop)

        await effect.reserve()
        await effect.confirm(external_id="ch_1")

        confirm_call = executor.calls[1]
        assert confirm_call[confirm_call.index("LATENCY_MS") + 1] == 250

    asyncio.run(run())


def test_async_workflow_context_effect_decorator_fails_on_exception():
    async def run():
        executor = FakeExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            initial_state="queued",
            batch_size=1,
        )

        @workflow.state("queued")
        async def queued(ctx):
            @ctx.effect("charge", "payment.charge", operation_digest="charge:v1")
            async def boom():
                raise RuntimeError("stripe down")

            await boom()
            return complete(result=b"ok")

        result = await workflow.run_once()

        assert result.applied == 1
        assert executor.calls[1][0] == "FLOW.EFFECT.RESERVE"
        fail_call = executor.calls[2]
        assert fail_call[:4] == ("FLOW.EFFECT.FAIL", "f1", "EFFECT_KEY", "charge")
        assert fail_call[fail_call.index("ERROR") + 1] == "stripe down"
        assert fail_call[fail_call.index("REASON") + 1] == "RuntimeError"
        assert isinstance(fail_call[fail_call.index("LATENCY_MS") + 1], int)

    asyncio.run(run())


def test_async_workflow_effect_fail_waits_for_cancelled_confirm_caller():
    class BlockingConfirmExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.confirm_started = asyncio.Event()
            self.confirm_allowed = asyncio.Event()

        async def execute_command(self, *args):
            if args[0] == "FLOW.EFFECT.CONFIRM":
                self.confirm_started.set()
                await self.confirm_allowed.wait()
            return await super().execute_command(*args)

    async def run():
        executor = BlockingConfirmExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow("f1", b"lease", 1, partition_key="p1"),
            "queued",
        )
        effect = ctx.effect("charge", "payment.charge")
        await effect.reserve()

        confirm_caller = asyncio.create_task(effect.confirm(external_id="ch_1"))
        await executor.confirm_started.wait()
        confirm_caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await confirm_caller

        fail_caller = asyncio.create_task(effect.fail(error="cancelled"))
        await asyncio.sleep(0)
        fail_completed_before_confirm = fail_caller.done()
        executor.confirm_allowed.set()
        result = await fail_caller

        assert fail_completed_before_confirm is False
        assert result.status == "confirmed"
        assert [call[0] for call in executor.calls] == [
            "FLOW.EFFECT.RESERVE",
            "FLOW.EFFECT.CONFIRM",
        ]

    asyncio.run(run())


def test_async_workflow_effect_fails_after_confirm_operation_failure():
    class ConfirmFailExecutor(FakeExecutor):
        async def execute_command(self, *args):
            if args[0] == "FLOW.EFFECT.CONFIRM":
                self.calls.append(args)
                raise RuntimeError("confirm unavailable")
            return await super().execute_command(*args)

    async def run():
        executor = ConfirmFailExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow("f1", b"lease", 1, partition_key="p1"),
            "queued",
        )
        effect = ctx.effect("charge", "payment.charge")

        with pytest.raises(RuntimeError, match="confirm unavailable"):
            await effect.confirm()
        result = await effect.fail(error="confirmation failed")

        assert result.status == "failed"
        assert [call[0] for call in executor.calls] == [
            "FLOW.EFFECT.RESERVE",
            "FLOW.EFFECT.CONFIRM",
            "FLOW.EFFECT.FAIL",
        ]

    asyncio.run(run())


def test_async_workflow_effect_shares_reservation_and_terminal_operation():
    class BlockingReserveExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.reserve_started = asyncio.Event()
            self.reserve_allowed = asyncio.Event()

        async def execute_command(self, *args):
            if args[0] == "FLOW.EFFECT.RESERVE":
                self.reserve_started.set()
                await self.reserve_allowed.wait()
            return await super().execute_command(*args)

    async def run():
        executor = BlockingReserveExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow("f1", b"lease", 1, partition_key="p1"),
            "queued",
        )
        effect = ctx.effect("charge", "payment.charge")

        confirm = asyncio.create_task(effect.confirm())
        await executor.reserve_started.wait()
        report_failure = asyncio.create_task(effect.fail(error="concurrent failure"))
        await asyncio.sleep(0)
        executor.reserve_allowed.set()
        confirmed, reported = await asyncio.gather(confirm, report_failure)

        assert confirmed.status == reported.status
        operations = [call[0] for call in executor.calls]
        assert operations.count("FLOW.EFFECT.RESERVE") == 1
        assert (
            sum(
                operations.count(operation)
                for operation in ("FLOW.EFFECT.CONFIRM", "FLOW.EFFECT.FAIL")
            )
            == 1
        )

    asyncio.run(run())


def test_async_worker_caches_reference_fingerprint_while_coalescing_results():
    class CountingDict(dict[str, int]):
        def __init__(self) -> None:
            super().__init__((str(index), index) for index in range(1_000))
            self.visited = 0

        def items(self):
            self.visited += len(self)
            return super().items()

    jobs = [ClaimedFlow(f"f{index}", b"lease", index, partition_key="p1") for index in range(32)]
    values = [CountingDict() for _ in jobs]

    handled = AsyncQueueFlowWorker._handled_from_results(
        [(job, True, value) for job, value in zip(jobs, values, strict=True)]
    )

    assert handled.mixed_results is None
    assert values[0].visited == len(values[0])


def test_async_workflow_close_is_terminal_and_idempotent_before_start():
    class OwnedClient:
        def __init__(self) -> None:
            self.close_calls = 0

        async def claim_flows(self, *_args, **_kwargs):
            return []

        async def close(self):
            self.close_calls += 1

    async def run() -> None:
        client = OwnedClient()
        workflow = AsyncWorkflow(client, type="order", states=["queued"])
        workflow._owns_client = True

        @workflow.on("queued")
        async def queued(_ctx):
            return complete()

        await workflow.close()
        await workflow.close()

        with pytest.raises(RuntimeError, match="closed"):
            workflow.start_workers()
        assert client.close_calls == 1

    asyncio.run(run())


def test_async_workflow_close_continues_after_caller_cancellation():
    class BlockingOwnedClient:
        def __init__(self) -> None:
            self.close_calls = 0
            self.close_started = asyncio.Event()
            self.close_allowed = asyncio.Event()

        async def claim_flows(self, *_args, **_kwargs):
            return []

        async def close(self):
            self.close_calls += 1
            self.close_started.set()
            await self.close_allowed.wait()

    async def run() -> None:
        client = BlockingOwnedClient()
        workflow = AsyncWorkflow(client, type="order", states=["queued"])
        workflow._owns_client = True

        caller = asyncio.create_task(workflow.close(timeout=None))
        await client.close_started.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller

        retry = asyncio.create_task(workflow.close(timeout=None))
        await asyncio.sleep(0)
        client.close_allowed.set()
        await retry

        assert client.close_calls == 1
        with pytest.raises(RuntimeError, match="closed"):
            workflow.start_workers()

    asyncio.run(run())


def test_async_workflow_close_retries_only_resources_that_failed():
    class RetryCloseClient:
        def __init__(self) -> None:
            self.close_calls = 0

        async def claim_flows(self, *_args, **_kwargs):
            return []

        async def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("transient close failure")

    async def run() -> None:
        client = RetryCloseClient()
        workflow = AsyncWorkflow(client, type="order", states=["queued"])
        workflow._owns_client = True

        with pytest.raises(RuntimeError, match="transient close failure"):
            await workflow.close()
        assert workflow._owns_client is True

        await workflow.close()
        await workflow.close()

        assert client.close_calls == 2
        assert workflow._owns_client is False

    asyncio.run(run())


def test_async_workflow_rejects_invalid_close_timeout_without_closing():
    class EmptyClient:
        async def claim_flows(self, *_args, **_kwargs):
            return []

    async def run() -> None:
        workflow = AsyncWorkflow(EmptyClient(), type="order", states=["queued"])

        @workflow.on("queued")
        async def queued(_ctx):
            return complete()

        with pytest.raises(ValueError, match="non-negative"):
            await workflow.close(timeout=-1)

        tasks = workflow.start_workers()
        assert isinstance(tasks, list)
        workflow.stop()
        await workflow.close()

    asyncio.run(run())


def test_async_workflow_partition_by_applies_to_single_and_batch_producers():
    class RecordingClient:
        def __init__(self) -> None:
            self.calls = []

        async def enqueue(self, id, **kwargs):
            self.calls.append(("enqueue", id, kwargs))
            return b"OK"

        async def enqueue_many(self, items, **kwargs):
            self.calls.append(("enqueue_many", items, kwargs))
            return [b"OK"] * len(items)

        async def run_steps_many(self, items, **kwargs):
            self.calls.append(("run_steps_many", items, kwargs))
            return b"OK"

    async def run() -> None:
        client = RecordingClient()
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            partition_by=("tenant_id", "order_id"),
        )

        await workflow.enqueue("one", tenant_id="tenant-a", order_id=7)
        await workflow.enqueue_many(
            ["two", "three"],
            tenant_id="tenant-a",
            order_id=7,
        )
        await workflow.run_steps_many(
            ["four"],
            states=["queued"],
            worker="inline",
            tenant_id="tenant-a",
            order_id=7,
        )

        assert [call[2]["partition_key"] for call in client.calls] == [
            "tenant-a:7",
            "tenant-a:7",
            "tenant-a:7",
        ]
        assert all(
            "tenant_id" not in call[2] and "order_id" not in call[2] for call in client.calls
        )

    asyncio.run(run())


def test_async_workflow_partition_by_honors_explicit_partition_without_attributes():
    class RecordingClient:
        def __init__(self) -> None:
            self.kwargs = None

        async def enqueue(self, _id, **kwargs):
            self.kwargs = kwargs
            return b"OK"

    async def run() -> None:
        client = RecordingClient()
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            partition_by=("tenant_id", "order_id"),
        )

        await workflow.enqueue("one", partition_key="manual")

        assert client.kwargs is not None
        assert client.kwargs["partition_key"] == "manual"

    asyncio.run(run())


def test_async_workflow_client_exposes_partition_by_configuration():
    client = AsyncWorkflowClient(AsyncFlowClient(FakeExecutor()))

    workflow = client.workflow(
        type="order",
        partition_by=("tenant_id", "order_id"),
    )

    assert workflow.partition_by == ("tenant_id", "order_id")


def test_async_workflow_client_from_url_creates_bounded_command_and_claim_pools(monkeypatch):
    async def run():
        calls = []

        def from_url(url, **kwargs):
            client = AsyncFlowClient(FakeExecutor())
            client.url = url
            client.kwargs = kwargs
            calls.append((url, kwargs, client))
            return client

        monkeypatch.setattr(
            "ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url)
        )

        client = AsyncWorkflowClient.from_url(
            "ferric://example:6388",
            worker_config=WorkerConfig(workers=4, claim_connections=2),
        )
        workflow = client.workflow(type="order", states=["queued"], initial_state="queued")

        assert [(url, kwargs) for url, kwargs, _client in calls] == [
            ("ferric://example:6388", {"max_connections": 1}),
            ("ferric://example:6388", {"max_connections": 2}),
        ]
        assert workflow.client is client.flow
        assert workflow.claim_client is client.claim_flow
        assert client.claim_flow is not client.flow

    asyncio.run(run())


def test_async_workflow_client_from_protocol_url_separates_command_and_claim_clients(monkeypatch):
    async def run():
        calls = []

        def from_url(url, **kwargs):
            client = AsyncFlowClient(FakeExecutor())
            client.url = url
            client.kwargs = kwargs
            calls.append((url, kwargs, client))
            return client

        monkeypatch.setattr(
            "ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url)
        )

        client = AsyncWorkflowClient.from_url(
            "ferric://example:6388",
            worker_config=WorkerConfig(workers=4),
        )
        workflow = client.workflow(type="order", states=["queued"], initial_state="queued")

        assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 4]
        assert workflow.client is client.flow
        assert workflow.claim_client is client.claim_flow
        assert client.claim_flow is not client.flow

    asyncio.run(run())


def test_async_workflow_worker_config_at_workflow_time_resizes_claim_pool(monkeypatch):
    async def run():
        calls = []

        def from_url(url, **kwargs):
            client = AsyncFlowClient(FakeExecutor())
            client.url = url
            client.kwargs = kwargs
            calls.append((url, kwargs, client))
            return client

        monkeypatch.setattr(
            "ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url)
        )

        client = AsyncWorkflowClient.from_url("ferric://example:6388")
        workflow = client.workflow(
            type="order",
            states=["queued"],
            initial_state="queued",
            worker_config=WorkerConfig(workers=64),
        )

        assert [kwargs["max_connections"] for _url, kwargs, _client in calls] == [1, 1, 64]
        assert workflow.client is client.flow
        assert workflow.claim_client is calls[-1][2]

    asyncio.run(run())


def test_async_workflow_client_close_does_not_close_externally_owned_clients():
    async def run():
        flow_executor = FakeExecutor()
        claim_executor = FakeExecutor()
        client = AsyncWorkflowClient(
            AsyncFlowClient(flow_executor),
            claim_client=AsyncFlowClient(claim_executor),
        )

        await client.close()

        assert flow_executor.closed is False
        assert claim_executor.closed is False

    asyncio.run(run())


def test_async_workflow_client_close_retries_only_failed_owned_clients():
    class CloseExecutor(FakeExecutor):
        def __init__(self, *, fail_once: bool = False) -> None:
            super().__init__()
            self.close_calls = 0
            self.fail_once = fail_once

        async def close(self):
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("transient close failure")
            self.closed = True

    async def run():
        flow_executor = CloseExecutor(fail_once=True)
        claim_executor = CloseExecutor()
        client = AsyncWorkflowClient(
            AsyncFlowClient(flow_executor),
            claim_client=AsyncFlowClient(claim_executor),
        )
        client._owns_flow = True
        client._owns_claim_flow = True

        with pytest.raises(RuntimeError, match="transient close failure"):
            await client.close()
        await client.close()

        assert flow_executor.close_calls == 2
        assert claim_executor.close_calls == 1
        assert flow_executor.closed is True
        assert claim_executor.closed is True

    asyncio.run(run())


def test_async_queue_worker_claims_and_completes():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        worker = AsyncQueueFlowWorker(client, type="email", state="queued", batch_size=10)
        seen = []

        async def handler(job: ClaimedFlow):
            seen.append(job.id)
            return b"done"

        result = await worker.run_once(handler)

        assert seen == ["f1"]
        assert result.claimed == 1
        assert result.completed == 1
        assert executor.calls[0][0] == "FLOW.CLAIM_DUE"
        assert executor.calls[1][0] == "FLOW.COMPLETE_MANY"

    asyncio.run(run())


def test_async_queue_worker_start_stop_join_tracks_stats():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        worker = AsyncQueueFlowWorker(client, type="email", state="queued", idle_sleep_s=0.001)

        async def handler(_job: ClaimedFlow):
            worker.stop()

        worker.start(handler)
        stats = await worker.join()

        assert stats.claimed == 1
        assert stats.completed == 1
        assert worker.is_running is False

    asyncio.run(run())


def test_async_queue_worker_start_then_immediate_close_cannot_resurrect_loop():
    class EmptyClient:
        async def claim_flows(self, *_args, **_kwargs):
            return []

    async def run():
        worker = AsyncQueueFlowWorker(
            EmptyClient(),
            type="email",
            idle_sleep_s=0.001,
        )
        worker.start(lambda _job: b"done")

        await asyncio.wait_for(worker.close(), timeout=0.2)

        assert worker.is_running is False

    asyncio.run(run())


def test_async_queue_worker_close_preserves_in_flight_claim_until_response():
    class BlockingClient:
        def __init__(self) -> None:
            self.claim_started = asyncio.Event()
            self.release = asyncio.Event()
            self.cancelled = False

        async def claim_flows(self, *_args, **_kwargs):
            self.claim_started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            return []

    async def run():
        client = BlockingClient()
        worker = AsyncQueueFlowWorker(client, type="email", block_ms=60_000)
        worker.start(lambda _job: b"done")
        await client.claim_started.wait()

        with pytest.raises(TimeoutError, match="close timed out"):
            await worker.close(timeout=0.01)

        assert client.cancelled is False
        client.release.set()
        await worker.close(timeout=0.2)

        assert worker.is_running is False

    asyncio.run(run())


def test_async_queue_worker_close_waits_for_caller_managed_run_task():
    class BlockingClient:
        def __init__(self) -> None:
            self.claim_started = asyncio.Event()
            self.release = asyncio.Event()
            self.closed = False

        async def claim_flows(self, *_args, **_kwargs):
            self.claim_started.set()
            await self.release.wait()
            return []

        async def close(self) -> None:
            self.closed = True

    async def run():
        client = BlockingClient()
        worker = AsyncQueueFlowWorker(
            client,
            type="email",
            block_ms=60_000,
            close_client=True,
        )
        run_task = asyncio.create_task(worker.run_forever(lambda _job: b"done"))
        await client.claim_started.wait()

        try:
            with pytest.raises(TimeoutError, match="close timed out"):
                await worker.close(timeout=0.01)
            assert client.closed is False
            assert run_task.done() is False
        finally:
            client.release.set()
            await asyncio.wait_for(run_task, timeout=0.2)
            if not client.closed:
                await worker.close(timeout=0.2)

        assert client.closed is True
        assert worker.is_running is False

    asyncio.run(run())


def test_async_queue_worker_close_cleans_owned_client_before_task_error():
    class OwnedClient:
        def __init__(self) -> None:
            self.closed = False

        async def claim_flows(self, *_args, **_kwargs):
            return [ClaimedFlow("f1", b"lease", 1, partition_key="p1")]

        async def close(self):
            self.closed = True

    async def run():
        client = OwnedClient()
        worker = AsyncQueueFlowWorker(
            client,
            type="email",
            exception_policy=ExceptionPolicy.RAISE,
            close_client=True,
        )

        async def fail(_job):
            raise RuntimeError("handler failed")

        task = worker.start(fail)
        await asyncio.wait({task})

        with pytest.raises(RuntimeError, match="handler failed"):
            await worker.close()
        assert client.closed is True

    asyncio.run(run())


def test_async_queue_worker_rejects_partial_independent_completion_result():
    jobs = [
        ClaimedFlow("ok", b"lease-1", 1, partition_key="p1"),
        ClaimedFlow("stale", b"lease-2", 2, partition_key="p1"),
    ]

    class PartialClient:
        async def claim_flows(self, *_args, **_kwargs):
            return jobs

        async def complete_jobs(self, *_args, **_kwargs):
            return [b"OK", FerricStoreError("fencing token mismatch")]

    async def run():
        worker = AsyncQueueFlowWorker(
            PartialClient(), type="email", partition_key="p1", batch_size=2
        )
        with pytest.raises(FerricStoreError, match="fencing token mismatch"):
            await worker.run_once(lambda _job: b"done")

    asyncio.run(run())


def test_async_queue_worker_batches_distinct_completion_results():
    jobs = [ClaimedFlow(str(index), b"lease", index, partition_key="p1") for index in range(100)]

    class Client:
        def __init__(self) -> None:
            self.single_calls = 0
            self.batch_calls = []

        async def claim_flows(self, *_args, **_kwargs):
            return jobs

        async def complete(self, *_args, **_kwargs):
            self.single_calls += 1
            return b"OK"

        async def complete_job_results(self, items):
            self.batch_calls.append(items)
            return [b"OK"] * len(items)

    async def run():
        client = Client()
        worker = AsyncQueueFlowWorker(client, type="email", partition_key="p1", batch_size=100)

        result = await worker.run_once(lambda job: job.id)

        assert result.completed == 100
        assert client.single_calls == 0
        assert len(client.batch_calls) == 1

    asyncio.run(run())


def test_async_queue_worker_does_not_conflate_bool_and_int_results():
    jobs = [
        ClaimedFlow("bool", b"lease-1", 1, partition_key="p1"),
        ClaimedFlow("int", b"lease-2", 2, partition_key="p1"),
    ]

    class Client:
        def __init__(self) -> None:
            self.uniform_calls = []
            self.result_batches = []

        async def claim_flows(self, *_args, **_kwargs):
            return jobs

        async def complete_jobs(self, claimed, **kwargs):
            self.uniform_calls.append((claimed, kwargs))
            return [b"OK"] * len(claimed)

        async def complete_job_results(self, items):
            self.result_batches.append(list(items))
            return [b"OK"] * len(items)

    async def run():
        client = Client()
        worker = AsyncQueueFlowWorker(client, type="typed", batch_size=2)

        result = await worker.run_once(lambda job: True if job.id == "bool" else 1)

        assert result.completed == 2
        assert client.uniform_calls == []
        assert [value for _job, value in client.result_batches[0]] == [True, 1]

    asyncio.run(run())


def test_async_queue_worker_fusion_uses_claim_client_and_stops_before_extra_claim():
    first = ClaimedFlow("first", b"lease-1", 1, partition_key="p1")
    second = ClaimedFlow("second", b"lease-2", 2, partition_key="p1")

    class CommandClient:
        def __init__(self) -> None:
            self.completed = []

        async def complete_jobs(self, jobs, **_kwargs):
            self.completed.extend(job.id for job in jobs)
            return [b"OK"] * len(jobs)

    class ClaimClient:
        def __init__(self) -> None:
            self.claim_calls = 0
            self.fused_calls = 0

        async def claim_flows(self, *_args, **_kwargs):
            self.claim_calls += 1
            return [first]

        async def complete_flows_and_claim_flows(self, jobs, **_kwargs):
            self.fused_calls += 1
            assert jobs == [first]
            return [second]

    async def run():
        command = CommandClient()
        claim = ClaimClient()
        worker = AsyncQueueFlowWorker(
            command,
            claim_client=claim,
            type="email",
            partition_key="p1",
            fuse_complete_claim=True,
        )

        async def handler(job):
            if job.id == "second":
                worker.stop()
            return b"done"

        await asyncio.wait_for(worker.run_forever(handler), timeout=1)

        assert claim.claim_calls == 1
        assert claim.fused_calls == 1
        assert command.completed == ["second"]

    asyncio.run(run())


def test_async_queue_worker_without_partition_claims_all_partitions_by_default():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        worker = AsyncQueueFlowWorker(
            client,
            type="email",
            state="queued",
            batch_size=10,
            claim_partition_batch_size=3,
        )

        await worker.run_once(lambda _job: b"done")

        claim = executor.calls[0]
        assert claim[0] == "FLOW.CLAIM_DUE"
        assert "PARTITION" not in claim
        assert "PARTITIONS" not in claim

    asyncio.run(run())


def test_async_queue_flow_simple_api_enqueues_and_claims_owned_buckets():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        queue = AsyncQueueFlow(
            client,
            type="email",
            workers=1,
            batch_size=10,
            claim_partition_batch_size=4,
            producer_loop_thread=False,
        )

        await queue.enqueue_many([("f1", b"a"), ("f2", b"b")])
        result = await queue.run_once(lambda _job: b"done")

        assert result.claimed == 1
        commands = [call[0] for call in executor.calls]
        assert commands.count("FLOW.CREATE_MANY") >= 1
        assert "FLOW.CLAIM_DUE" in commands
        assert "FLOW.COMPLETE_MANY" in commands
        assert commands.index("FLOW.CLAIM_DUE") < commands.index("FLOW.COMPLETE_MANY")

    asyncio.run(run())


class _ThreadAwareUrlClient:
    def __init__(self) -> None:
        self.enqueue_ids: list[str] = []
        self.enqueue_threads: list[int] = []
        self.closed = 0

    async def enqueue(self, id: str, **_kwargs: Any) -> bytes:
        self.enqueue_ids.append(id)
        self.enqueue_threads.append(threading.get_ident())
        return b"OK"

    async def enqueue_many(self, items: Any, **_kwargs: Any) -> list[bytes]:
        return [await self.enqueue(item.id, **_kwargs) for item in items]

    async def claim_flows(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def close(self) -> None:
        self.closed += 1


def test_direct_producer_loop_thread_reuses_one_background_client(monkeypatch: Any) -> None:
    created: list[tuple[int, dict[str, Any], _ThreadAwareUrlClient]] = []

    def from_url(_url: str, **kwargs: Any) -> _ThreadAwareUrlClient:
        client = _ThreadAwareUrlClient()
        created.append((threading.get_ident(), kwargs, client))
        return client

    monkeypatch.setattr(AsyncFlowClient, "from_url", staticmethod(from_url))

    async def run() -> None:
        main_thread = threading.get_ident()
        queue = AsyncQueueFlow(
            "ferric://localhost:6388",
            type="email",
            producer_loop_thread=True,
        )
        await queue.enqueue("one")
        await queue.enqueue("two")
        await queue.close()

        background = [entry for entry in created if entry[0] != main_thread]
        assert len(background) == 1
        assert background[0][2].enqueue_ids == ["one", "two"]
        assert background[0][2].closed == 1

    asyncio.run(run())


@pytest.mark.parametrize("runtime", ["queue", "workflow"])
def test_high_level_producer_loop_preserves_url_kwargs_and_reuses_client(
    monkeypatch: Any,
    runtime: str,
) -> None:
    created: list[tuple[int, dict[str, Any], _ThreadAwareUrlClient]] = []

    def from_url(_url: str, **kwargs: Any) -> _ThreadAwareUrlClient:
        client = _ThreadAwareUrlClient()
        created.append((threading.get_ident(), kwargs, client))
        return client

    monkeypatch.setattr(AsyncFlowClient, "from_url", staticmethod(from_url))

    async def run() -> None:
        main_thread = threading.get_ident()
        config = WorkerConfig(producer_loop_thread=True)
        if runtime == "queue":
            sdk: AsyncQueueClient | AsyncWorkflowClient = AsyncQueueClient.from_url(
                "ferric://localhost:6388",
                worker_config=config,
                timeout=7.0,
            )
            owner: AsyncQueueFlow | AsyncWorkflow = sdk.queue(type="email").worker()
        else:
            sdk = AsyncWorkflowClient.from_url(
                "ferric://localhost:6388",
                worker_config=config,
                timeout=7.0,
            )
            owner = sdk.workflow(type="orders", states=["queued"])

        await owner.enqueue("one")
        await owner.enqueue("two")
        await owner.close()
        await sdk.close()

        background = [entry for entry in created if entry[0] != main_thread]
        assert len(background) == 1
        assert background[0][1] == {"timeout": 7.0}
        assert background[0][2].enqueue_ids == ["one", "two"]
        assert background[0][2].closed == 1

    asyncio.run(run())


def test_async_queue_flow_defaults_to_nonblocking_claims():
    queue = AsyncQueueFlow(
        AsyncFlowClient(FakeExecutor()),
        type="email",
        workers=1,
        producer_loop_thread=False,
    )

    worker = queue._build_worker(0)

    assert worker.block_ms is None


def test_async_queue_worker_defaults_match_easy_startup_profile():
    worker = AsyncQueueFlowWorker(
        AsyncFlowClient(FakeExecutor()),
        type="email",
    )

    assert worker.batch_size == 10
    assert worker.block_ms is None
    assert worker.claim_partition_batch_size == 1


def test_async_queue_worker_uses_separate_claim_client_when_provided():
    async def run():
        command_executor = FakeExecutor()
        claim_executor = FakeExecutor()
        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(command_executor),
            claim_client=AsyncFlowClient(claim_executor),
            type="email",
            workers=1,
            batch_size=10,
        )

        result = await worker.run_once(lambda _job: b"done")

        assert result.claimed == 1
        assert [call[0] for call in claim_executor.calls] == ["FLOW.CLAIM_DUE"]
        assert [call[0] for call in command_executor.calls] == ["FLOW.COMPLETE_MANY"]

    asyncio.run(run())


def test_async_sdk_rejects_old_owner_wakeup_options():
    with pytest.raises(TypeError):
        AsyncQueueFlow(
            AsyncFlowClient(FakeExecutor()),
            type="email",
            workers=1,
            owner_wakeup=True,
        )

    with pytest.raises(TypeError):
        AsyncWorkflow(
            AsyncFlowClient(FakeExecutor()),
            type="order",
            states=["queued"],
            workers=1,
            owner_wakeup=True,
        )

    with pytest.raises(TypeError):
        WorkerConfig(owner_wakeup=True)


def test_async_queue_flow_on_error_fail_is_passed_to_worker():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        queue = AsyncQueueFlow(
            client,
            type="email",
            workers=1,
            batch_size=10,
            producer_loop_thread=False,
            on_error="fail",
        )

        async def handler(_job: ClaimedFlow):
            raise RuntimeError("boom")

        result = await queue.run_once(handler)

        assert result.failed == 1
        assert executor.calls[1][0] == "FLOW.FAIL_MANY"
        assert executor.calls[1][executor.calls[1].index("ERROR") + 1] == b"boom"

    asyncio.run(run())


def test_async_queue_flow_rejects_invalid_on_error():
    executor = FakeExecutor()
    client = AsyncFlowClient(executor)

    with pytest.raises(ValueError, match="on_error"):
        AsyncQueueFlow(client, type="email", on_error="explode")


def test_async_queue_flow_join_surfaces_later_worker_error():
    async def run():
        class BlockingWorker:
            stopped = False
            cancelled = False
            _event: asyncio.Event

            def __init__(self):
                self._event = asyncio.Event()

            async def join(self):
                try:
                    await self._event.wait()
                except asyncio.CancelledError:
                    self.cancelled = True
                    raise

            def stop(self):
                self.stopped = True
                self._event.set()

        class FailingWorker:
            async def join(self):
                raise RuntimeError("boom")

            def stop(self):
                pass

        queue = AsyncQueueFlow(AsyncFlowClient(FakeExecutor()), type="email", workers=2)
        blocking = BlockingWorker()
        queue._workers = [blocking, FailingWorker()]

        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(queue.join(), timeout=0.2)

        assert blocking.stopped is True
        assert blocking.cancelled is False

    asyncio.run(run())


def test_async_queue_flow_close_attempts_owned_clients_after_worker_failure():
    class OwnedClient:
        def __init__(self, name, closed):
            self.name = name
            self.closed = closed

        async def close(self):
            self.closed.append(self.name)

    class FailingWorker:
        def stop(self):
            pass

        async def close(self):
            raise RuntimeError("worker close failed")

    async def run():
        closed = []
        client = OwnedClient("client", closed)
        claim_client = OwnedClient("claim", closed)
        queue = AsyncQueueFlow(
            client,
            claim_client=claim_client,
            type="email",
            workers=1,
        )
        queue._owns_client = True
        queue._owns_claim_client = True
        queue._workers = [FailingWorker()]

        with pytest.raises(RuntimeError, match="worker close failed"):
            await queue.close()

        assert set(closed) == {"client", "claim"}
        assert queue._workers == []
        assert queue._owns_client is False
        assert queue._owns_claim_client is False

    asyncio.run(run())


def test_async_queue_flow_closes_workers_before_owned_clients():
    class OwnedClient:
        def __init__(self, name, events):
            self.name = name
            self.events = events

        async def close(self):
            self.events.append(self.name)

    class BlockingWorker:
        def __init__(self, started, allowed, events):
            self.started = started
            self.allowed = allowed
            self.events = events

        def stop(self):
            pass

        async def close(self):
            self.started.set()
            await self.allowed.wait()
            self.events.append("worker")

    async def run():
        events = []
        started = asyncio.Event()
        allowed = asyncio.Event()
        client = OwnedClient("client", events)
        claim_client = OwnedClient("claim", events)
        queue = AsyncQueueFlow(client, claim_client=claim_client, type="email")
        queue._owns_client = True
        queue._owns_claim_client = True
        queue._workers = [BlockingWorker(started, allowed, events)]

        close_task = asyncio.create_task(queue.close())
        await started.wait()
        await asyncio.sleep(0)
        assert events == []

        allowed.set()
        await close_task
        assert events[0] == "worker"
        assert set(events[1:]) == {"client", "claim"}

    asyncio.run(run())


def test_async_queue_flow_close_continues_in_order_after_caller_cancellation():
    class OwnedClient:
        def __init__(self, name, events):
            self.name = name
            self.events = events

        async def close(self):
            self.events.append(self.name)

    class BlockingWorker:
        def __init__(self, started, allowed, events):
            self.started = started
            self.allowed = allowed
            self.events = events

        def stop(self):
            pass

        async def close(self):
            self.started.set()
            await self.allowed.wait()
            self.events.append("worker")

    async def run():
        events = []
        started = asyncio.Event()
        allowed = asyncio.Event()
        client = OwnedClient("client", events)
        claim_client = OwnedClient("claim", events)
        queue = AsyncQueueFlow(client, claim_client=claim_client, type="email")
        queue._owns_client = True
        queue._owns_claim_client = True
        queue._workers = [BlockingWorker(started, allowed, events)]

        caller = asyncio.create_task(queue.close())
        await started.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert events == []

        allowed.set()
        await queue.close()
        assert events[0] == "worker"
        assert set(events[1:]) == {"client", "claim"}

    asyncio.run(run())


def test_async_queue_flow_close_timeout_is_retryable_without_closing_clients():
    class OwnedClient:
        def __init__(self, name, closed):
            self.name = name
            self.closed = closed

        async def close(self):
            self.closed.append(self.name)

    class TimeoutOnceWorker:
        def __init__(self):
            self.close_calls = 0

        def stop(self):
            pass

        async def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise TimeoutError("worker close timed out")

    async def run():
        closed = []
        client = OwnedClient("client", closed)
        claim_client = OwnedClient("claim", closed)
        worker = TimeoutOnceWorker()
        queue = AsyncQueueFlow(client, claim_client=claim_client, type="email")
        queue._owns_client = True
        queue._owns_claim_client = True
        queue._workers = [worker]

        with pytest.raises(TimeoutError, match="timed out"):
            await queue.close()

        assert closed == []
        assert queue._workers == [worker]
        assert queue._owns_client is True
        assert queue._owns_claim_client is True

        await queue.close()
        assert set(closed) == {"client", "claim"}
        assert worker.close_calls == 2

    asyncio.run(run())


def test_async_queue_flow_cannot_restart_after_close():
    async def run():
        queue = AsyncQueueFlow(AsyncFlowClient(FakeExecutor()), type="email")
        await queue.close()

        with pytest.raises(RuntimeError, match="closed"):
            queue.start(lambda _job: b"done")

    asyncio.run(run())


def test_async_queue_worker_preserves_distinct_failure_messages():
    async def run():
        class TwoJobExecutor(FakeExecutor):
            def __init__(self):
                super().__init__()
                self.batches = []

            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "FLOW.CLAIM_DUE":
                    return [[b"f1", b"p1", b"lease-1", 1], [b"f2", b"p1", b"lease-2", 2]]
                return b"OK"

            async def execute_batch(self, commands):
                self.batches.append(list(commands))
                return [b"OK"] * len(commands)

        executor = TwoJobExecutor()
        client = AsyncFlowClient(executor)
        worker = AsyncQueueFlowWorker(
            client,
            type="email",
            state="queued",
            batch_size=10,
            auto_partitions=False,
        )

        async def handler(job: ClaimedFlow):
            raise RuntimeError(f"boom-{job.id}")

        result = await worker.run_once(handler)

        assert result.retried == 2
        assert len(executor.batches) == 1
        assert [call[0] for call in executor.batches[0]] == ["FLOW.RETRY", "FLOW.RETRY"]
        assert [call[call.index("ERROR") + 1] for call in executor.batches[0]] == [
            b"boom-f1",
            b"boom-f2",
        ]

    asyncio.run(run())


def test_async_workflow_pipelines_heterogeneous_outcomes_as_one_mutation_batch():
    class MutationClient:
        def __init__(self) -> None:
            self.mutation_batches = []

        async def claim_flows(self, *_args, **_kwargs):
            return [
                ClaimedFlow("transition", b"lease-1", 1, partition_key="p1"),
                ClaimedFlow("retry", b"lease-2", 2, partition_key="p1"),
                ClaimedFlow("fail", b"lease-3", 3, partition_key="p1"),
            ]

        async def apply_job_mutations(self, mutations):
            self.mutation_batches.append(list(mutations))
            return [b"OK"] * len(mutations)

    async def run():
        client = MutationClient()
        workflow = AsyncWorkflow(client, type="order", states=["queued"], batch_size=3)

        @workflow.on("queued")
        async def queued(ctx):
            if ctx.id == "transition":
                return transition("next")
            if ctx.id == "retry":
                return retry(error="later")
            return fail(error="terminal")

        result = await workflow.run_once(state="queued")

        assert result.applied == 3
        assert len(client.mutation_batches) == 1
        assert [mutation.kind.value for mutation in client.mutation_batches[0]] == [
            "transition",
            "retry",
            "fail",
        ]

    asyncio.run(run())


def test_async_workflow_rejects_initial_state_not_in_states():
    with pytest.raises(ValueError, match="initial_state"):
        AsyncWorkflow(
            AsyncFlowClient(FakeExecutor()),
            type="order",
            states=["step1"],
            initial_state="queued",
        )


def test_async_workflow_simple_api_batches_transition_and_complete():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued", "done"],
            workers=1,
            batch_size=10,
            claim_partition_batch_size=2,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedFlow):
            return transition("done")

        @workflow.on("done")
        async def done(_job: ClaimedFlow):
            return complete(result=b"ok")

        await workflow.enqueue("f1")
        first = await workflow.run_once(state="queued")
        second = await workflow.run_once(state="done")

        assert first.claimed == 1
        assert second.claimed == 1
        commands = [call[0] for call in executor.calls]
        assert commands == [
            "FLOW.CREATE",
            "FLOW.CLAIM_DUE",
            "FLOW.TRANSITION_MANY",
            "FLOW.CLAIM_DUE",
            "FLOW.COMPLETE_MANY",
        ]

    asyncio.run(run())


def test_async_workflow_raise_waits_for_active_siblings_and_leaves_no_orphans():
    async def run():
        workflow = AsyncWorkflow(
            AsyncFlowClient(FakeExecutor()),
            type="order",
            states=["queued"],
            concurrency=2,
            exception_policy=ExceptionPolicy.RAISE,
        )
        sibling_started = asyncio.Event()
        failure_raised = asyncio.Event()
        release_sibling = asyncio.Event()
        sibling_finished = False

        @workflow.on("queued")
        async def queued(ctx):
            nonlocal sibling_finished
            if ctx.id == "failing":
                await sibling_started.wait()
                failure_raised.set()
                raise RuntimeError("primary handler failure")
            sibling_started.set()
            await release_sibling.wait()
            sibling_finished = True
            return complete()

        jobs = [
            ClaimedFlow("failing", b"lease-1", 1, partition_key="p1"),
            ClaimedFlow("sibling", b"lease-2", 2, partition_key="p1"),
        ]
        task = asyncio.create_task(workflow._handle_claimed_batch("queued", jobs))
        caught = None
        try:
            await sibling_started.wait()
            await failure_raised.wait()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.02)
        finally:
            release_sibling.set()
            [caught] = await asyncio.gather(task, return_exceptions=True)

        assert isinstance(caught, RuntimeError)
        assert str(caught) == "primary handler failure"
        assert sibling_finished is True

    asyncio.run(run())


def test_async_workflow_close_preserves_in_flight_claim_until_response():
    class BlockingClient:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.cancelled = False

        async def claim_flows(self, *_args, **_kwargs):
            self.started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            return []

    async def run() -> None:
        client = BlockingClient()
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            block_ms=60_000,
        )

        @workflow.on("queued")
        async def handle(_ctx):
            return complete()

        workflow.start_workers()
        await client.started.wait()
        with pytest.raises(TimeoutError, match="close timed out"):
            await workflow.close(timeout=0.01)
        assert client.cancelled is False
        client.release.set()
        await workflow.close(timeout=0.2)

    asyncio.run(run())


def test_async_workflow_close_cleans_owned_client_before_task_error():
    class OwnedClient:
        def __init__(self) -> None:
            self.closed = False

        async def claim_flows(self, *_args, **_kwargs):
            return [ClaimedFlow("f1", b"lease", 1, partition_key="p1")]

        async def close(self):
            self.closed = True

    async def run() -> None:
        client = OwnedClient()
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            exception_policy=ExceptionPolicy.RAISE,
        )
        workflow._owns_client = True

        @workflow.on("queued")
        async def fail(_ctx):
            raise RuntimeError("handler failed")

        tasks = workflow.start_workers()
        assert isinstance(tasks, list)
        await asyncio.wait(tasks)

        with pytest.raises(RuntimeError, match="handler failed"):
            await workflow.close()
        assert client.closed is True

    asyncio.run(run())


def test_async_workflow_rejects_partial_independent_mutation_result():
    class PartialExecutor(FakeExecutor):
        async def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "FLOW.CLAIM_DUE":
                return [
                    [b"f1", b"p1", b"lease-1", 1],
                    [b"f2", b"p1", b"lease-2", 2],
                ]
            if args[0] == "FLOW.COMPLETE_MANY":
                return [b"OK", FerricStoreError("stale lease")]
            return b"OK"

    async def run():
        workflow = AsyncWorkflow(
            AsyncFlowClient(PartialExecutor()),
            type="order",
            states=["queued"],
            batch_size=2,
        )

        @workflow.on("queued")
        async def queued(_job):
            return complete(result=b"done")

        with pytest.raises(FerricStoreError, match="stale lease"):
            await workflow.run_once(state="queued")

    asyncio.run(run())


def test_async_workflow_pipelines_distinct_completion_results_in_one_batch():
    class DistinctClient:
        def __init__(self) -> None:
            self.mutation_batches = []

        async def claim_flows(self, *_args, **_kwargs):
            return [
                ClaimedFlow(
                    f"f{index}",
                    b"lease",
                    index,
                    partition_key="tenant:order",
                )
                for index in range(100)
            ]

        async def complete_job_mutations(self, items):
            self.mutation_batches.append(list(items))
            return [b"OK"] * len(items)

    async def run():
        client = DistinctClient()
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            batch_size=100,
        )

        @workflow.on("queued")
        async def queued(job):
            return complete(result=job.id)

        result = await workflow.run_once(state="queued")

        assert result.applied == 100
        assert len(client.mutation_batches) == 1
        assert [options["result"] for _job, options in client.mutation_batches[0]] == [
            f"f{index}" for index in range(100)
        ]

    asyncio.run(run())


def test_async_workflow_blocking_claims_all_states_with_compact_state_return():
    async def run():
        class ClaimAnyExecutor(FakeExecutor):
            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "FLOW.CLAIM_DUE":
                    return [
                        [b"f1", b"p1", b"lease-1", 1, b"queued"],
                        [b"f2", b"p1", b"lease-2", 2, b"done"],
                    ]
                if args[0] in {"FLOW.COMPLETE_MANY", "FLOW.TRANSITION_MANY"}:
                    return [b"OK"]
                return b"OK"

        executor = ClaimAnyExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued", "done"],
            workers=1,
            batch_size=10,
            block_ms=5000,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedFlow):
            return transition("done")

        @workflow.on("done")
        async def done(_job: ClaimedFlow):
            return complete(result=b"ok")

        result = await workflow.run_once()

        assert result.claimed == 2
        assert result.applied == 2
        claim = executor.calls[0]
        assert "STATE" not in claim
        assert claim[claim.index("RETURN") : claim.index("RETURN") + 2] == (
            "RETURN",
            "JOBS_COMPACT_STATE_ATTRS",
        )
        assert [call[0] for call in executor.calls[1:]] == [
            "FLOW.TRANSITION_MANY",
            "FLOW.COMPLETE_MANY",
        ]

    asyncio.run(run())


def test_async_workflow_claims_all_priorities_by_default():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedFlow):
            return transition("queued", priority=2)

        await workflow.run_once(state="queued")

        assert executor.calls[0][0] == "FLOW.CLAIM_DUE"
        assert "PRIORITY" not in executor.calls[0]

    asyncio.run(run())


def test_async_workflow_signal_is_first_class():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["waiting_payment", "verify_payment"],
            workers=1,
            batch_size=10,
            claim_partition_batch_size=2,
        )

        await workflow.signal(
            "f1",
            signal="payment_received",
            partition_key="__flow_auto__:0",
            if_state="waiting_payment",
            transition_to="verify_payment",
            values={"payment_event": b"payment-bytes"},
            now_ms=1100,
        )

        assert executor.calls[0][0] == "FLOW.SIGNAL"
        assert "TRANSITION_TO" in executor.calls[0]
        assert "VALUE" in executor.calls[0]

    asyncio.run(run())


def test_async_workflow_defaults_to_nonblocking_claims():
    workflow = AsyncWorkflow(
        AsyncFlowClient(FakeExecutor()),
        type="order",
        states=["queued"],
        workers=1,
    )

    assert workflow.block_ms is None


def test_async_workflow_uses_separate_claim_client_when_provided():
    async def run():
        command_executor = FakeExecutor()
        claim_executor = FakeExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(command_executor),
            claim_client=AsyncFlowClient(claim_executor),
            type="order",
            states=["queued"],
            workers=1,
        )

        @workflow.state("queued")
        async def queued(_ctx):
            return b"done"

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert [call[0] for call in claim_executor.calls] == ["FLOW.CLAIM_DUE"]
        assert [call[0] for call in command_executor.calls] == ["FLOW.COMPLETE_MANY"]

    asyncio.run(run())


def test_async_workflow_missing_handler_does_not_claim_flows():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            claim_partition_batch_size=4,
        )

        with pytest.raises(ValueError, match="no handler"):
            await workflow.run_once(state="queued")

        assert executor.calls == []

    asyncio.run(run())


def test_async_workflow_can_claim_and_fetch_value_refs():
    async def run():
        executor = ValueClaimExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            claim_values=["cached"],
        )
        seen = {}

        @workflow.on("queued")
        async def queued(ctx):
            seen.update(await ctx.value_many(["cached", "remote"], local_cache=True))
            return complete()

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert seen == {"cached": b"one", "remote": b"two"}
        assert "VALUE" in executor.calls[0]
        assert "cached" in executor.calls[0]
        assert executor.calls[1] == ("FLOW.VALUE.MGET", "ref-remote")
        assert executor.calls[2][0] == "FLOW.COMPLETE_MANY"

    asyncio.run(run())


def test_async_workflow_context_flow_helper_defaults_to_current_job():
    async def run():
        class FlowCommandExecutor(FakeExecutor):
            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "FLOW.CLAIM_DUE":
                    return [
                        {
                            b"id": b"f1",
                            b"type": b"order",
                            b"state": b"running",
                            b"run_state": b"queued",
                            b"partition_key": b"p1",
                            b"lease_token": b"lease",
                            b"fencing_token": 7,
                        }
                    ]
                if args[0] == "FLOW.GET":
                    return {
                        b"id": b"f1",
                        b"type": b"order",
                        b"state": b"running",
                        b"partition_key": b"p1",
                    }
                if args[0] == "FLOW.HISTORY":
                    return []
                if args[0] == "FLOW.COMPLETE_MANY":
                    return [b"OK"]
                return b"OK"

        executor = FlowCommandExecutor()
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor),
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            exception_policy=ExceptionPolicy.RAISE,
        )

        @workflow.on("queued")
        async def queued(ctx):
            await ctx.flow.get()
            await ctx.flow.history(count=1)
            return complete()

        result = await workflow.run_once(state="queued")

        assert result.applied == 1
        assert ("FLOW.GET", "f1", "PARTITION", "p1") in executor.calls
        assert ("FLOW.HISTORY", "f1", "COUNT", 1, "PARTITION", "p1") in executor.calls

    asyncio.run(run())


def test_async_workflow_flow_commands_match_sync_public_api():
    def public_methods(cls):
        return {
            name
            for name, value in vars(cls).items()
            if not name.startswith("_") and callable(value)
        }

    assert public_methods(async_worker_module.AsyncWorkflowFlowCommands) == public_methods(
        workflow_module.WorkflowFlowCommands
    )


def test_async_workflow_flow_commands_apply_context_defaults():
    class RecordingClient:
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            async def call(*args, **kwargs):
                self.calls.append((name, args, kwargs))
                return name

            return call

    async def run():
        client = RecordingClient()
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            initial_state="queued",
        )
        ctx = AsyncWorkflowContext(
            workflow,
            ClaimedFlow(
                "parent-1",
                b"lease-1",
                7,
                partition_key="tenant-a",
                type="order",
                state="running",
                run_state="queued",
            ),
            "queued",
        )

        assert ctx.flow.client is client
        with pytest.raises(AttributeError):
            object.__getattribute__(ctx.flow, "__dict__")
        await ctx.flow.create("child-1", payload=b"payload")
        await ctx.flow.step_continue("done", lease_ms=1_000)
        await ctx.flow.retry(error="later")
        await ctx.flow.fail(error="broken")
        await ctx.flow.cancel()
        await ctx.flow.spawn_children(
            [ChildSpec(id="child-2", type="child")],
            wait_state="children_done",
        )
        await ctx.flow.value_put(b"value")
        await ctx.flow.policy_get()

        assert client.calls == [
            (
                "create",
                ("child-1",),
                {
                    "type": "order",
                    "state": "queued",
                    "payload": b"payload",
                    "partition_key": "tenant-a",
                    "return_record": False,
                },
            ),
            (
                "step_continue",
                ("parent-1",),
                {
                    "lease_token": b"lease-1",
                    "from_state": "running",
                    "to_state": "done",
                    "fencing_token": 7,
                    "partition_key": "tenant-a",
                    "lease_ms": 1_000,
                },
            ),
            (
                "retry",
                ("parent-1",),
                {
                    "lease_token": b"lease-1",
                    "fencing_token": 7,
                    "partition_key": "tenant-a",
                    "return_record": False,
                    "error": "later",
                },
            ),
            (
                "fail",
                ("parent-1",),
                {
                    "lease_token": b"lease-1",
                    "fencing_token": 7,
                    "partition_key": "tenant-a",
                    "return_record": False,
                    "error": "broken",
                },
            ),
            (
                "cancel",
                ("parent-1",),
                {
                    "fencing_token": 7,
                    "lease_token": b"lease-1",
                    "partition_key": "tenant-a",
                    "return_record": False,
                },
            ),
            (
                "spawn_children",
                ("parent-1", [ChildSpec(id="child-2", type="child")]),
                {
                    "partition_key": "tenant-a",
                    "lease_token": b"lease-1",
                    "fencing_token": 7,
                    "wait_state": "children_done",
                },
            ),
            (
                "value_put",
                (b"value",),
                {"partition_key": "tenant-a", "owner_flow_id": "parent-1"},
            ),
            ("policy_get", ("order",), {}),
        ]

    asyncio.run(run())


def test_async_workflow_on_error_fail_uses_fail_many():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            exception_policy=ExceptionPolicy.FAIL,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedFlow):
            raise RuntimeError("boom")

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert executor.calls[1][0] == "FLOW.FAIL_MANY"
        assert executor.calls[1][executor.calls[1].index("ERROR") + 1] == b"boom"

    asyncio.run(run())


def test_async_workflow_state_alias_registers_handler_with_exception_policy():
    async def run():
        workflow = AsyncWorkflow(
            AsyncFlowClient(FakeExecutor()),
            type="order",
            states=["queued"],
            workers=1,
            exception_policy=ExceptionPolicy.RETRY,
        )

        @workflow.state("queued", exception_policy=ExceptionPolicy.FAIL)
        async def queued(_job: ClaimedFlow):
            return complete()

        assert "queued" in workflow.handlers
        assert workflow.error_modes["queued"] == "fail"

    asyncio.run(run())


def test_async_workflow_loop_runs_one_claim_per_iteration_when_handler_stops():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            idle_sleep_s=0,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedFlow):
            workflow.stop()
            return complete(result=b"ok")

        workflow.start_workers()
        stats = await workflow.join()

        assert stats.claimed == 1
        assert [call[0] for call in executor.calls].count("FLOW.CLAIM_DUE") == 1

    asyncio.run(run())


def test_async_workflow_polls_with_blocking_claim_when_no_jobs_are_cached_locally():
    async def run():
        executor = FakeExecutor()
        client = AsyncFlowClient(executor)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            claim_partition_batch_size=4,
            idle_sleep_s=0,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedFlow):
            return complete(result=b"ok")

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert result.applied == 1
        assert executor.calls[0][0] == "FLOW.CLAIM_DUE"
        assert executor.calls[1][0] == "FLOW.COMPLETE_MANY"

    asyncio.run(run())
