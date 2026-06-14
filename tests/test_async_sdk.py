import asyncio

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncQueueClient,
    AsyncQueueFlow,
    AsyncQueueFlowWorker,
    AsyncWorkflow,
    AsyncWorkflowClient,
    ExceptionPolicy,
    RetryPolicy,
    ValueConfig,
    WorkerConfig,
    complete,
    transition,
)
from ferricstore.types import ClaimedItem


class FakeRedis:
    def __init__(self):
        self.calls = []
        self.closed = False

    async def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "FLOW.CLAIM_DUE":
            return [[b"f1", b"p1", b"lease", 7]]
        if args[0] in {"FLOW.COMPLETE_MANY", "FLOW.TRANSITION_MANY"}:
            return [b"OK"]
        return b"OK"

    async def close(self):
        self.closed = True


class ValueClaimRedis(FakeRedis):
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
        redis = FakeRedis()
        client = AsyncFlowClient(redis)

        result = await client.create(
            "f1",
            type="order",
            payload=b"payload",
            now_ms=100,
            return_record=False,
        )
        assert result == b"OK"
        assert redis.calls[0][:4] == ("FLOW.CREATE", "f1", "TYPE", "order")

        assert await client.command("SET", "k", "v") == b"OK"
        assert redis.calls[-1] == ("SET", "k", "v")

    asyncio.run(run())


def test_async_queue_client_creates_queue_and_delegates_flow_commands():
    async def run():
        redis = FakeRedis()
        client = AsyncQueueClient(AsyncFlowClient(redis))
        queue = client.queue(type="email")

        await queue.enqueue("e1", payload=b"body")
        assert redis.calls[0][:4] == ("FLOW.CREATE", "e1", "TYPE", "email")
        assert "STATE" in redis.calls[0]
        assert "queued" in redis.calls[0]

        assert await client.command("PING") == b"OK"
        assert redis.calls[-1] == ("PING",)
        assert queue.worker(workers=1).__class__.__name__ == "AsyncQueueFlow"

    asyncio.run(run())


def test_async_queue_client_retry_policy_is_inherited_and_can_be_overridden():
    async def run():
        redis = FakeRedis()
        default_policy = RetryPolicy(max_retries=5)
        queue_policy = RetryPolicy(max_retries=2)
        client = AsyncQueueClient(AsyncFlowClient(redis), retry_policy=default_policy)
        queue = client.queue(type="email", retry_policy=queue_policy)

        await queue.install_policy()
        await client.install_policy("sms")

        assert redis.calls[0][:2] == ("FLOW.POLICY.SET", "email")
        assert queue_policy.max_retries in redis.calls[0]
        assert redis.calls[1][:2] == ("FLOW.POLICY.SET", "sms")
        assert default_policy.max_retries in redis.calls[1]

    asyncio.run(run())


def test_async_queue_client_worker_config_is_inherited_and_overridable():
    async def run():
        client = AsyncQueueClient(
            AsyncFlowClient(FakeRedis()),
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
        return AsyncFlowClient(FakeRedis())

    monkeypatch.setattr("ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url))

    client = AsyncQueueClient.from_url(
        "redis://example/0",
        worker_config=WorkerConfig(workers=4),
    )
    worker = client.queue(type="email").worker()._build_worker(0)

    assert calls == [
        ("redis://example/0", {"max_connections": 4}),
        ("redis://example/0", {"max_connections": 4}),
    ]
    assert worker.client is client.flow
    assert worker.claim_client is client.claim_flow


def test_async_queue_client_from_protocol_url_reuses_multiplexed_claim_client(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = AsyncFlowClient(FakeRedis())
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
    assert calls[0][1] == {"max_connections": 4}
    assert client.claim_flow is client.flow
    assert worker.client is client.flow
    assert worker.claim_client is client.flow


def test_async_queue_worker_config_at_queue_time_resizes_claim_pool(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = AsyncFlowClient(FakeRedis())
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url))

    client = AsyncQueueClient.from_url("redis://example/0")
    worker = (
        client.queue(
            type="email",
            worker_config=WorkerConfig(workers=64),
        )
        .worker()
        ._build_worker(0)
    )

    assert calls[0][1]["max_connections"] == 2
    assert calls[1][1]["max_connections"] == 1
    assert calls[2][1]["max_connections"] == 64
    assert worker.client is client.flow
    assert worker.claim_client is calls[2][2]


def test_async_queue_worker_config_does_not_resize_protocol_claim_pool(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = AsyncFlowClient(FakeRedis())
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
    assert worker.client is client.flow
    assert worker.claim_client is client.flow


def test_async_queue_client_close_does_not_close_externally_owned_clients():
    async def run():
        flow_redis = FakeRedis()
        claim_redis = FakeRedis()
        client = AsyncQueueClient(
            AsyncFlowClient(flow_redis),
            claim_client=AsyncFlowClient(claim_redis),
        )

        await client.close()

        assert flow_redis.closed is False
        assert claim_redis.closed is False

    asyncio.run(run())


def test_async_queue_client_value_config_is_passed_to_worker():
    async def run():
        client = AsyncQueueClient(
            AsyncFlowClient(FakeRedis()),
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
        redis = FakeRedis()
        client = AsyncWorkflowClient(AsyncFlowClient(redis))
        workflow = client.workflow(type="order", states=["queued"], initial_state="queued")

        await workflow.start("o1", payload=b"payload")
        assert redis.calls[0][:4] == ("FLOW.CREATE", "o1", "TYPE", "order")
        assert "STATE" in redis.calls[0]
        assert "queued" in redis.calls[0]

        assert await client.command("PING") == b"OK"
        assert redis.calls[-1] == ("PING",)

    asyncio.run(run())


def test_async_workflow_client_retry_policy_is_inherited_and_state_can_override():
    async def run():
        redis = FakeRedis()
        default_policy = RetryPolicy(max_retries=5)
        state_policy = RetryPolicy(max_retries=2)
        client = AsyncWorkflowClient(AsyncFlowClient(redis), retry_policy=default_policy)
        workflow = client.workflow(type="order", states=["created"], initial_state="created")

        @workflow.state("created", retry_policy=state_policy)
        async def created(job):
            return transition("done")

        await workflow.install_policy()

        call = redis.calls[-1]
        assert call[:2] == ("FLOW.POLICY.SET", "order")
        assert "STATE" in call
        assert "created" in call
        assert default_policy.max_retries in call
        assert state_policy.max_retries in call

    asyncio.run(run())


def test_async_workflow_client_worker_and_value_config_are_inherited_and_overridable():
    async def run():
        client = AsyncWorkflowClient(
            AsyncFlowClient(FakeRedis()),
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


def test_async_workflow_client_from_url_creates_bounded_command_and_claim_pools(monkeypatch):
    async def run():
        calls = []

        def from_url(url, **kwargs):
            client = AsyncFlowClient(FakeRedis())
            client.url = url
            client.kwargs = kwargs
            calls.append((url, kwargs, client))
            return client

        monkeypatch.setattr(
            "ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url)
        )

        client = AsyncWorkflowClient.from_url(
            "redis://example/0",
            worker_config=WorkerConfig(workers=4),
        )
        workflow = client.workflow(type="order", states=["queued"], initial_state="queued")

        assert [(url, kwargs) for url, kwargs, _client in calls] == [
            ("redis://example/0", {"max_connections": 4}),
            ("redis://example/0", {"max_connections": 4}),
        ]
        assert workflow.client is client.flow
        assert workflow.claim_client is client.claim_flow

    asyncio.run(run())


def test_async_workflow_worker_config_at_workflow_time_resizes_claim_pool(monkeypatch):
    async def run():
        calls = []

        def from_url(url, **kwargs):
            client = AsyncFlowClient(FakeRedis())
            client.url = url
            client.kwargs = kwargs
            calls.append((url, kwargs, client))
            return client

        monkeypatch.setattr(
            "ferricstore.async_worker.AsyncFlowClient.from_url", staticmethod(from_url)
        )

        client = AsyncWorkflowClient.from_url("redis://example/0")
        workflow = client.workflow(
            type="order",
            states=["queued"],
            initial_state="queued",
            worker_config=WorkerConfig(workers=64),
        )

        assert calls[0][1]["max_connections"] == 2
        assert calls[1][1]["max_connections"] == 1
        assert calls[2][1]["max_connections"] == 64
        assert workflow.client is client.flow
        assert workflow.claim_client is calls[2][2]

    asyncio.run(run())


def test_async_workflow_client_close_does_not_close_externally_owned_clients():
    async def run():
        flow_redis = FakeRedis()
        claim_redis = FakeRedis()
        client = AsyncWorkflowClient(
            AsyncFlowClient(flow_redis),
            claim_client=AsyncFlowClient(claim_redis),
        )

        await client.close()

        assert flow_redis.closed is False
        assert claim_redis.closed is False

    asyncio.run(run())


def test_async_queue_worker_claims_and_completes():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        worker = AsyncQueueFlowWorker(client, type="email", state="queued", batch_size=10)
        seen = []

        async def handler(job: ClaimedItem):
            seen.append(job.id)
            return b"done"

        result = await worker.run_once(handler)

        assert seen == ["f1"]
        assert result.claimed == 1
        assert result.completed == 1
        assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
        assert redis.calls[1][0] == "FLOW.COMPLETE_MANY"

    asyncio.run(run())


def test_async_queue_worker_start_stop_join_tracks_stats():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        worker = AsyncQueueFlowWorker(client, type="email", state="queued", idle_sleep_s=0.001)

        async def handler(_job: ClaimedItem):
            worker.stop()

        worker.start(handler)
        stats = await worker.join()

        assert stats.claimed == 1
        assert stats.completed == 1
        assert worker.is_running is False

    asyncio.run(run())


def test_async_queue_worker_without_partition_claims_all_partitions_by_default():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        worker = AsyncQueueFlowWorker(
            client,
            type="email",
            state="queued",
            batch_size=10,
            claim_partition_batch_size=3,
        )

        await worker.run_once(lambda _job: b"done")

        claim = redis.calls[0]
        assert claim[0] == "FLOW.CLAIM_DUE"
        assert "PARTITION" not in claim
        assert "PARTITIONS" not in claim

    asyncio.run(run())


def test_async_queue_flow_simple_api_enqueues_and_claims_owned_buckets():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
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
        commands = [call[0] for call in redis.calls]
        assert commands.count("FLOW.CREATE_MANY") >= 1
        assert "FLOW.CLAIM_DUE" in commands
        assert "FLOW.COMPLETE_MANY" in commands
        assert commands.index("FLOW.CLAIM_DUE") < commands.index("FLOW.COMPLETE_MANY")

    asyncio.run(run())


def test_async_queue_flow_defaults_to_nonblocking_claims():
    queue = AsyncQueueFlow(
        AsyncFlowClient(FakeRedis()),
        type="email",
        workers=1,
        producer_loop_thread=False,
    )

    worker = queue._build_worker(0)

    assert worker.block_ms is None


def test_async_queue_worker_defaults_match_easy_startup_profile():
    worker = AsyncQueueFlowWorker(
        AsyncFlowClient(FakeRedis()),
        type="email",
    )

    assert worker.batch_size == 10
    assert worker.block_ms is None
    assert worker.claim_partition_batch_size == 1


def test_async_queue_worker_uses_separate_claim_client_when_provided():
    async def run():
        command_redis = FakeRedis()
        claim_redis = FakeRedis()
        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(command_redis),
            claim_client=AsyncFlowClient(claim_redis),
            type="email",
            workers=1,
            batch_size=10,
        )

        result = await worker.run_once(lambda _job: b"done")

        assert result.claimed == 1
        assert [call[0] for call in claim_redis.calls] == ["FLOW.CLAIM_DUE"]
        assert [call[0] for call in command_redis.calls] == ["FLOW.COMPLETE_MANY"]

    asyncio.run(run())


def test_async_sdk_rejects_old_owner_wakeup_options():
    with pytest.raises(TypeError):
        AsyncQueueFlow(
            AsyncFlowClient(FakeRedis()),
            type="email",
            workers=1,
            owner_wakeup=True,
        )

    with pytest.raises(TypeError):
        AsyncWorkflow(
            AsyncFlowClient(FakeRedis()),
            type="order",
            states=["queued"],
            workers=1,
            owner_wakeup=True,
        )

    with pytest.raises(TypeError):
        WorkerConfig(owner_wakeup=True)


def test_async_queue_flow_on_error_fail_is_passed_to_worker():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        queue = AsyncQueueFlow(
            client,
            type="email",
            workers=1,
            batch_size=10,
            producer_loop_thread=False,
            on_error="fail",
        )

        async def handler(_job: ClaimedItem):
            raise RuntimeError("boom")

        result = await queue.run_once(handler)

        assert result.failed == 1
        assert redis.calls[1][0] == "FLOW.FAIL_MANY"
        assert redis.calls[1][redis.calls[1].index("ERROR") + 1] == b"boom"

    asyncio.run(run())


def test_async_queue_flow_rejects_invalid_on_error():
    redis = FakeRedis()
    client = AsyncFlowClient(redis)

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

        queue = AsyncQueueFlow(AsyncFlowClient(FakeRedis()), type="email", workers=2)
        blocking = BlockingWorker()
        queue._workers = [blocking, FailingWorker()]

        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(queue.join(), timeout=0.2)

        assert blocking.stopped is True
        assert blocking.cancelled is False

    asyncio.run(run())


def test_async_queue_worker_preserves_distinct_failure_messages():
    async def run():
        class TwoJobRedis(FakeRedis):
            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "FLOW.CLAIM_DUE":
                    return [[b"f1", b"p1", b"lease-1", 1], [b"f2", b"p1", b"lease-2", 2]]
                if args[0] == "FLOW.RETRY_MANY":
                    return [b"OK"]
                return b"OK"

        redis = TwoJobRedis()
        client = AsyncFlowClient(redis)
        worker = AsyncQueueFlowWorker(
            client,
            type="email",
            state="queued",
            batch_size=10,
            auto_partitions=False,
        )

        async def handler(job: ClaimedItem):
            raise RuntimeError(f"boom-{job.id}")

        result = await worker.run_once(handler)

        retry_calls = [call for call in redis.calls if call[0] == "FLOW.RETRY_MANY"]
        assert result.retried == 2
        assert [call[call.index("ERROR") + 1] for call in retry_calls] == [b"boom-f1", b"boom-f2"]

    asyncio.run(run())


def test_async_workflow_rejects_initial_state_not_in_states():
    with pytest.raises(ValueError, match="initial_state"):
        AsyncWorkflow(
            AsyncFlowClient(FakeRedis()),
            type="order",
            states=["step1"],
            initial_state="queued",
        )


def test_async_workflow_simple_api_batches_transition_and_complete():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued", "done"],
            workers=1,
            batch_size=10,
            claim_partition_batch_size=2,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            return transition("done")

        @workflow.on("done")
        async def done(_job: ClaimedItem):
            return complete(result=b"ok")

        await workflow.enqueue("f1")
        first = await workflow.run_once(state="queued")
        second = await workflow.run_once(state="done")

        assert first.claimed == 1
        assert second.claimed == 1
        commands = [call[0] for call in redis.calls]
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
        class ClaimAnyRedis(FakeRedis):
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

        redis = ClaimAnyRedis()
        workflow = AsyncWorkflow(
            AsyncFlowClient(redis),
            type="order",
            states=["queued", "done"],
            workers=1,
            batch_size=10,
            block_ms=5000,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            return transition("done")

        @workflow.on("done")
        async def done(_job: ClaimedItem):
            return complete(result=b"ok")

        result = await workflow.run_once()

        assert result.claimed == 2
        assert result.applied == 2
        claim = redis.calls[0]
        assert "STATE" not in claim
        assert claim[claim.index("RETURN") : claim.index("RETURN") + 2] == (
            "RETURN",
            "JOBS_COMPACT_STATE",
        )
        assert [call[0] for call in redis.calls[1:]] == [
            "FLOW.TRANSITION_MANY",
            "FLOW.COMPLETE_MANY",
        ]

    asyncio.run(run())


def test_async_workflow_claims_all_priorities_by_default():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            return transition("queued", priority=5)

        await workflow.run_once(state="queued")

        assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
        assert "PRIORITY" not in redis.calls[0]

    asyncio.run(run())


def test_async_workflow_signal_is_first_class():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
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

        assert redis.calls[0][0] == "FLOW.SIGNAL"
        assert "TRANSITION_TO" in redis.calls[0]
        assert "VALUE" in redis.calls[0]

    asyncio.run(run())


def test_async_workflow_defaults_to_nonblocking_claims():
    workflow = AsyncWorkflow(
        AsyncFlowClient(FakeRedis()),
        type="order",
        states=["queued"],
        workers=1,
    )

    assert workflow.block_ms is None


def test_async_workflow_uses_separate_claim_client_when_provided():
    async def run():
        command_redis = FakeRedis()
        claim_redis = FakeRedis()
        workflow = AsyncWorkflow(
            AsyncFlowClient(command_redis),
            claim_client=AsyncFlowClient(claim_redis),
            type="order",
            states=["queued"],
            workers=1,
        )

        @workflow.state("queued")
        async def queued(_ctx):
            return b"done"

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert [call[0] for call in claim_redis.calls] == ["FLOW.CLAIM_DUE"]
        assert [call[0] for call in command_redis.calls] == ["FLOW.COMPLETE_MANY"]

    asyncio.run(run())


def test_async_workflow_missing_handler_does_not_claim_jobs():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
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

        assert redis.calls == []

    asyncio.run(run())


def test_async_workflow_can_claim_and_fetch_value_refs():
    async def run():
        redis = ValueClaimRedis()
        client = AsyncFlowClient(redis)
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
        assert "VALUE" in redis.calls[0]
        assert "cached" in redis.calls[0]
        assert redis.calls[1] == ("FLOW.VALUE.MGET", "ref-remote")
        assert redis.calls[2][0] == "FLOW.COMPLETE_MANY"

    asyncio.run(run())


def test_async_workflow_context_flow_helper_defaults_to_current_job():
    async def run():
        class FlowCommandRedis(FakeRedis):
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

        redis = FlowCommandRedis()
        workflow = AsyncWorkflow(
            AsyncFlowClient(redis),
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
        assert ("FLOW.GET", "f1", "PARTITION", "p1") in redis.calls
        assert ("FLOW.HISTORY", "f1", "COUNT", 1, "PARTITION", "p1") in redis.calls

    asyncio.run(run())


def test_async_workflow_on_error_fail_uses_fail_many():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            exception_policy=ExceptionPolicy.FAIL,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            raise RuntimeError("boom")

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert redis.calls[1][0] == "FLOW.FAIL_MANY"
        assert redis.calls[1][redis.calls[1].index("ERROR") + 1] == b"boom"

    asyncio.run(run())


def test_async_workflow_state_alias_registers_handler_with_exception_policy():
    async def run():
        workflow = AsyncWorkflow(
            AsyncFlowClient(FakeRedis()),
            type="order",
            states=["queued"],
            workers=1,
            exception_policy=ExceptionPolicy.RETRY,
        )

        @workflow.state("queued", exception_policy=ExceptionPolicy.FAIL)
        async def queued(_job: ClaimedItem):
            return complete()

        assert "queued" in workflow.handlers
        assert workflow.error_modes["queued"] == "fail"

    asyncio.run(run())


def test_async_workflow_loop_runs_one_claim_per_iteration_when_handler_stops():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        workflow = AsyncWorkflow(
            client,
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            idle_sleep_s=0,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            workflow.stop()
            return complete(result=b"ok")

        workflow.start()
        stats = await workflow.join()

        assert stats.claimed == 1
        assert [call[0] for call in redis.calls].count("FLOW.CLAIM_DUE") == 1

    asyncio.run(run())


def test_async_workflow_polls_with_blocking_claim_when_no_jobs_are_cached_locally():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
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
        async def queued(_job: ClaimedItem):
            return complete(result=b"ok")

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert result.applied == 1
        assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
        assert redis.calls[1][0] == "FLOW.COMPLETE_MANY"

    asyncio.run(run())
