import asyncio

from ferricstore import AsyncFlowClient, AsyncQueueFlow, AsyncQueueFlowWorker, AsyncWorkflow, complete, transition
from ferricstore.types import ClaimedItem


class FakeRedis:
    def __init__(self):
        self.calls = []

    async def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "FLOW.CLAIM_DUE":
            return [[b"f1", b"p1", b"lease", 7]]
        if args[0] in {"FLOW.COMPLETE_MANY", "FLOW.TRANSITION_MANY"}:
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


def test_async_queue_worker_defaults_to_auto_bucket_multi_partition_claim():
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
        assert "PARTITIONS" in claim
        offset = claim.index("PARTITIONS")
        assert claim[offset + 1] == 3
        assert all(str(key).startswith("__flow_auto__:") for key in claim[offset + 2 : offset + 5])

    asyncio.run(run())


def test_async_queue_flow_simple_api_enqueues_notifies_and_claims_owned_buckets():
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
        assert queue.wake_source is not None
        assert queue.wake_source.notified_jobs == 2
        commands = [call[0] for call in redis.calls]
        assert commands.count("FLOW.CREATE_MANY") >= 1
        assert "FLOW.CLAIM_DUE" in commands
        assert "FLOW.COMPLETE_MANY" in commands
        assert commands.index("FLOW.CLAIM_DUE") < commands.index("FLOW.COMPLETE_MANY")

    asyncio.run(run())


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


def test_async_workflow_signal_is_first_class_and_wakes_transition_target():
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

        assert workflow.wake_source is not None
        assert workflow.wake_source.notified_jobs == 1
        assert redis.calls[0][0] == "FLOW.SIGNAL"
        assert "TRANSITION_TO" in redis.calls[0]
        assert "VALUE" in redis.calls[0]

    asyncio.run(run())
