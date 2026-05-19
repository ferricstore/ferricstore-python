import asyncio

import pytest

from ferricstore import AsyncFlowClient, FlowClient, JsonCodec
from ferricstore.types import ClaimedItem, CreateItem


class FakeAsyncRedis:
    def __init__(self):
        self.calls = []
        self.closed = False

    async def execute_command(self, *args):
        self.calls.append(args)
        command = args[0]
        if command == "FLOW.CLAIM_DUE":
            return [[b"f1", b"tenant:1", b"lease", 7]]
        if command in {"FLOW.CREATE_MANY", "FLOW.COMPLETE_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY"}:
            return [b"OK"]
        if command == "FLOW.VALUE.MGET":
            return [b'{"ok": true}']
        if command == "RATELIMIT.ADD":
            return [b"allowed", 1, 9, 100]
        return b"OK"

    async def close(self):
        self.closed = True


def run(coro):
    return asyncio.run(coro)


def test_async_create_uses_real_async_executor_without_thread_fallback(monkeypatch):
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        def forbidden_to_thread(*args, **kwargs):
            raise AssertionError("async client must not use asyncio.to_thread")

        monkeypatch.setattr(asyncio, "to_thread", forbidden_to_thread)

        result = await client.create(
            "f1",
            type="order",
            state="queued",
            partition_key="tenant:1",
            payload=b"hello",
            now_ms=100,
            return_record=False,
        )

        assert result == b"OK"
        assert redis.calls == [
            (
                "FLOW.CREATE",
                "f1",
                "TYPE",
                "order",
                "STATE",
                "queued",
                "NOW",
                100,
                "PARTITION",
                "tenant:1",
                "PAYLOAD",
                b"hello",
                "RUN_AT",
                100,
            )
        ]

    run(main())


def test_async_claim_jobs_and_complete_jobs_use_hot_compact_paths():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        jobs = await client.claim_jobs(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            limit=100,
            now_ms=100,
        )
        result = await client.complete_jobs(jobs, result=b"done", now_ms=200)

        assert jobs == [
            ClaimedItem(
                id="f1",
                partition_key="tenant:1",
                lease_token=b"lease",
                fencing_token=7,
            )
        ]
        assert result == [b"OK"]
        assert redis.calls[0] == (
            "FLOW.CLAIM_DUE",
            "order",
            "STATE",
            "queued",
            "WORKER",
            "worker-1",
            "LEASE_MS",
            30000,
            "LIMIT",
            100,
            "NOW",
            100,
            "PARTITION",
            "tenant:1",
            "PRIORITY",
            0,
            "RETURN",
            "JOBS_COMPACT",
            "RECLAIM_EXPIRED",
            "false",
        )
        assert redis.calls[1] == (
            "FLOW.COMPLETE_MANY",
            "tenant:1",
            "RESULT",
            b"done",
            "NOW",
            200,
            "INDEPENDENT",
            "true",
            "ITEMS",
            "f1",
            b"lease",
            7,
        )

    run(main())


def test_async_enqueue_many_keeps_auto_bucket_grouping():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        result = await client.enqueue_many(
            [CreateItem("flow-1", b"a"), CreateItem("flow-2", b"b")],
            type="order",
            now_ms=100,
        )

        assert result == [b"OK", b"OK"]
        assert all(call[0] == "FLOW.CREATE_MANY" for call in redis.calls)
        assert all(str(call[1]).startswith("__flow_auto__:") for call in redis.calls)

    run(main())


def test_async_signal_builds_flow_signal_command():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        result = await client.signal(
            "f1",
            signal="payment_received",
            partition_key="tenant:1",
            idempotency_key="stripe_evt_1",
            if_state=["waiting_payment", "manual_review"],
            transition_to="verify_payment",
            values={"payment_event": b"payment-bytes"},
            run_at_ms=1250,
            now_ms=1100,
        )

        assert result == b"OK"
        assert redis.calls[0] == (
            "FLOW.SIGNAL",
            "f1",
            "SIGNAL",
            "payment_received",
            "PARTITION",
            "tenant:1",
            "IDEMPOTENCY",
            "stripe_evt_1",
            "IF_STATE",
            "waiting_payment",
            "IF_STATE",
            "manual_review",
            "TRANSITION_TO",
            "verify_payment",
            "RUN_AT",
            1250,
            "NOW",
            1100,
            "VALUE",
            "payment_event",
            b"payment-bytes",
        )

    run(main())


def test_async_value_mget_decodes_with_codec_and_close_awaits_executor():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis, codec=JsonCodec())

        values = await client.value_mget(["ref-1"])
        await client.close()

        assert values == [{"ok": True}]
        assert redis.closed is True

    run(main())


def test_async_client_rejects_sync_flow_client():
    with pytest.raises(TypeError, match="requires an async executor"):
        AsyncFlowClient(FlowClient(FakeAsyncRedis()))
