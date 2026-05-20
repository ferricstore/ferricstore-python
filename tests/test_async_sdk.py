import asyncio

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncPartitionWakeCoordinator,
    AsyncQueueFlow,
    AsyncQueueFlowWorker,
    AsyncStateWakeCoordinator,
    AsyncWorkflow,
    complete,
    transition,
)
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


def test_async_wake_coordinators_are_top_level_exports():
    assert AsyncPartitionWakeCoordinator.__name__ == "AsyncPartitionWakeCoordinator"
    assert AsyncStateWakeCoordinator.__name__ == "AsyncStateWakeCoordinator"


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


def test_async_queue_worker_wake_index_defaults_to_worker_index():
    async def run():
        class Wake:
            def __init__(self):
                self.worker_indexes = []

            async def next_partitions(self, worker_index, **_kwargs):
                self.worker_indexes.append(worker_index)
                return [], 0

        wake = Wake()
        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(FakeRedis()),
            type="email",
            state="queued",
            worker_index=3,
            workers=4,
            auto_partitions=True,
            wake_source=wake,
            idle_sleep_s=0,
        )

        await worker.run_once(lambda _job: b"done")

        assert wake.worker_indexes == [3]

    asyncio.run(run())


def test_async_queue_worker_explicit_partition_keeps_wake_source():
    async def run():
        class Wake:
            def __init__(self):
                self.calls = 0

            async def next_partitions(self, *args, **kwargs):
                self.calls += 1
                return [0], 1

        wake = Wake()
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        worker = AsyncQueueFlowWorker(
            client,
            type="email",
            state="queued",
            partition_key="__flow_auto__:0",
            wake_source=wake,
        )

        await worker.run_once(lambda _job: b"done")

        assert wake.calls == 1

    asyncio.run(run())


def test_async_queue_worker_returns_filtered_wake_credit():
    async def run():
        wake = AsyncPartitionWakeCoordinator(1)
        await wake.notify_partition("__flow_auto__:2", 1)

        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(FakeRedis()),
            type="email",
            state="queued",
            partition_key="__flow_auto__:1",
            wake_source=wake,
            idle_sleep_s=0,
        )

        await worker.run_once(lambda _job: b"done")

        assert await wake.total_credit() == 1

    asyncio.run(run())


def test_async_queue_worker_returns_exact_filtered_wake_credit():
    async def run():
        wake = AsyncPartitionWakeCoordinator(1)
        await wake.notify_partition("__flow_auto__:1", 999)
        await wake.notify_partition("__flow_auto__:2", 1)
        redis = FakeRedis()

        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(redis),
            type="email",
            state="queued",
            partition_key="__flow_auto__:1",
            wake_source=wake,
            batch_size=1000,
            claim_partition_batch_size=2,
            idle_sleep_s=0,
        )

        await worker.run_once(lambda _job: b"done")

        claim = redis.calls[0]
        assert claim[claim.index("LIMIT") + 1] == 999
        assert await wake.total_credit() == 1

    asyncio.run(run())


def test_async_queue_worker_throttles_broad_poll_without_wake_credit():
    async def run():
        class Wake:
            async def next_partition_credits(self, *_args, **_kwargs):
                return [], 0

        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(FakeRedis()),
            type="email",
            state="queued",
            partition_keys=["bucket-1"],
            wake_source=Wake(),
            wake_worker_index=0,
            batch_size=10,
            idle_sleep_s=0,
            allow_wake_partitions_outside_claim_set=True,
            wake_broad_poll_interval_s=60.0,
        )

        assert await worker._next_wake_or_claim_plan() == (None, None, 10)
        assert await worker._next_wake_or_claim_plan() == ("bucket-1", None, 10)

    asyncio.run(run())


def test_async_queue_worker_retry_does_not_require_notify_partition_method():
    async def run():
        class Wake:
            async def next_partitions(self, *_args, **_kwargs):
                return [0], 1

        worker = AsyncQueueFlowWorker(
            AsyncFlowClient(FakeRedis()),
            type="email",
            state="queued",
            partition_keys=["__flow_auto__:0"],
            wake_source=Wake(),
            wake_worker_index=0,
            on_error="retry",
            idle_sleep_s=0,
        )

        result = await worker.run_once(lambda _job: (_ for _ in ()).throw(RuntimeError("boom")))

        assert result.retried == 1

    asyncio.run(run())


def test_async_queue_flow_claims_custom_partition_wake_tokens():
    async def run():
        redis = FakeRedis()
        queue = AsyncQueueFlow(
            AsyncFlowClient(redis),
            type="email",
            workers=1,
            batch_size=10,
            producer_loop_thread=False,
        )

        await queue.enqueue("f-custom", partition_key="tenant:1")
        await queue.run_once(lambda _job: b"done")

        claim = next(call for call in redis.calls if call[0] == "FLOW.CLAIM_DUE")
        assert claim[claim.index("PARTITION") + 1] == "tenant:1"

    asyncio.run(run())


def test_async_queue_flow_does_not_notify_for_other_state_enqueue():
    async def run():
        queue = AsyncQueueFlow(
            AsyncFlowClient(FakeRedis()),
            type="email",
            state="queued",
            workers=1,
            producer_loop_thread=False,
        )

        await queue.enqueue("f1", state="scheduled")
        await queue.enqueue_many(["f2"], state="scheduled")

        assert queue.wake_source is not None
        assert queue.wake_source.notified_jobs == 0

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
            owner_wakeup=False,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            return transition("queued", priority=5)

        await workflow.run_once(state="queued")

        assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
        assert "PRIORITY" not in redis.calls[0]

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


def test_async_queue_flow_falls_back_to_polling_when_wake_has_no_credit():
    async def run():
        redis = FakeRedis()
        client = AsyncFlowClient(redis)
        queue = AsyncQueueFlow(
            client,
            type="email",
            workers=1,
            batch_size=10,
            claim_partition_batch_size=4,
            idle_sleep_s=0,
            producer_loop_thread=False,
        )

        result = await queue.run_once(lambda _job: b"done")

        assert result.claimed == 1
        assert result.completed == 1
        assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
        assert "PARTITION" not in redis.calls[0]
        assert "PARTITIONS" not in redis.calls[0]
        assert redis.calls[1][0] == "FLOW.COMPLETE_MANY"

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


def test_async_workflow_falls_back_to_broad_claim_when_wake_has_no_credit():
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
            return complete()

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
        assert "PARTITION" not in redis.calls[0]
        assert "PARTITIONS" not in redis.calls[0]

    asyncio.run(run())


def test_async_workflow_throttles_broad_poll_without_wake_credit():
    async def run():
        redis = FakeRedis()
        workflow = AsyncWorkflow(
            AsyncFlowClient(redis),
            type="order",
            states=["queued"],
            workers=1,
            batch_size=10,
            claim_partition_batch_size=4,
            idle_sleep_s=0,
            wake_broad_poll_interval_s=60.0,
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            return complete(result=b"ok")

        await workflow.run_once(state="queued")
        await workflow.run_once(state="queued")

        claims = [call for call in redis.calls if call[0] == "FLOW.CLAIM_DUE"]
        assert "PARTITION" not in claims[0]
        assert "PARTITIONS" not in claims[0]
        assert "PARTITION" in claims[1] or "PARTITIONS" in claims[1]

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
            owner_wakeup=False,
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
            owner_wakeup=False,
            on_error="raise",
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
            on_error="fail",
        )

        @workflow.on("queued")
        async def queued(_job: ClaimedItem):
            raise RuntimeError("boom")

        result = await workflow.run_once(state="queued")

        assert result.claimed == 1
        assert redis.calls[1][0] == "FLOW.FAIL_MANY"
        assert redis.calls[1][redis.calls[1].index("ERROR") + 1] == b"boom"

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


def test_async_workflow_falls_back_to_polling_when_wake_has_no_credit():
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
