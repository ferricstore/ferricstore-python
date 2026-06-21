import asyncio

import pytest

import ferricstore.async_worker as async_worker_module
from ferricstore import (
    AsyncFlowClient,
    AsyncQueueClient,
    AsyncQueueFlow,
    AsyncQueueFlowWorker,
    AsyncWorkflow,
    AsyncWorkflowClient,
    AsyncWorkflowEffect,
    BudgetPolicy,
    ExceptionPolicy,
    RetryPolicy,
    ValueConfig,
    WorkerConfig,
    complete,
    transition,
)
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
        worker_config=WorkerConfig(workers=4),
    )
    worker = client.queue(type="email").worker()._build_worker(0)

    assert calls == [("ferric://example:6388", {"max_connections": 1})]
    assert worker.client is client.flow
    assert worker.claim_client is client.claim_flow


def test_async_queue_client_from_protocol_url_reuses_multiplexed_claim_client(monkeypatch):
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

    assert len(calls) == 1
    assert calls[0][1] == {"max_connections": 1}
    assert client.claim_flow is client.flow
    assert worker.client is client.flow
    assert worker.claim_client is client.flow


def test_async_queue_worker_config_at_queue_time_keeps_native_claim_client(monkeypatch):
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

    assert calls[0][1]["max_connections"] == 1
    assert worker.client is client.flow
    assert worker.claim_client is client.flow


def test_async_queue_worker_config_does_not_resize_protocol_claim_pool(monkeypatch):
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

    assert len(calls) == 1
    assert calls[0][1] == {"max_connections": 1}
    assert worker.client is client.flow
    assert worker.claim_client is client.flow


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

        await workflow.start("o1", payload=b"payload")
        assert executor.calls[0][:4] == ("FLOW.CREATE", "o1", "TYPE", "order")
        assert "STATE" in executor.calls[0]
        assert "queued" in executor.calls[0]

        assert await client.command("PING") == b"OK"
        assert executor.calls[-1] == ("PING",)

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
            worker_config=WorkerConfig(workers=4),
        )
        workflow = client.workflow(type="order", states=["queued"], initial_state="queued")

        assert [(url, kwargs) for url, kwargs, _client in calls] == [
            ("ferric://example:6388", {"max_connections": 1}),
        ]
        assert workflow.client is client.flow
        assert workflow.claim_client is client.claim_flow

    asyncio.run(run())


def test_async_workflow_client_from_protocol_url_reuses_multiplexed_claim_client(monkeypatch):
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

        assert len(calls) == 1
        assert calls[0][1] == {"max_connections": 1}
        assert workflow.client is client.flow
        assert workflow.claim_client is client.flow

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

        assert calls[0][1]["max_connections"] == 1
        assert workflow.client is client.flow
        assert workflow.claim_client is client.flow

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


def test_async_queue_worker_preserves_distinct_failure_messages():
    async def run():
        class TwoJobExecutor(FakeExecutor):
            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "FLOW.CLAIM_DUE":
                    return [[b"f1", b"p1", b"lease-1", 1], [b"f2", b"p1", b"lease-2", 2]]
                if args[0] == "FLOW.RETRY_MANY":
                    return [b"OK"]
                return b"OK"

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

        retry_calls = [call for call in executor.calls if call[0] == "FLOW.RETRY_MANY"]
        assert result.retried == 2
        assert [call[call.index("ERROR") + 1] for call in retry_calls] == [b"boom-f1", b"boom-f2"]

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
            return transition("queued", priority=5)

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

        workflow.start()
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
