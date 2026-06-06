import asyncio
import zlib

import pytest

from ferricstore import (
    AsyncFlowClient,
    BackpressurePolicy,
    FlowAlreadyExistsError,
    FlowClient,
    JsonCodec,
    OverloadedError,
    StaleLeaseError,
)
from ferricstore.types import ChildSpec, ClaimedItem, CreateItem, FencedItem


class FakeAsyncRedis:
    def __init__(self):
        self.calls = []
        self.closed = False
        self.responses = []

    async def execute_command(self, *args):
        self.calls.append(args)
        if self.responses:
            return self.responses.pop(0)
        command = args[0]
        record = {
            b"id": b"f1",
            b"type": b"order",
            b"state": b"created",
            b"partition_key": b"tenant:1",
            b"version": 1,
            b"lease_token": b"lease",
            b"fencing_token": 7,
            b"payload": b'{"ok":true}',
        }
        if command in {"FLOW.CLAIM_DUE", "FLOW.RECLAIM"}:
            return [[b"f1", b"tenant:1", b"lease", 7]]
        if command in {
            "FLOW.LIST",
            "FLOW.TERMINALS",
            "FLOW.FAILURES",
            "FLOW.BY_PARENT",
            "FLOW.BY_ROOT",
            "FLOW.BY_CORRELATION",
            "FLOW.STUCK",
        }:
            return [record]
        if command in {"FLOW.INFO", "FLOW.POLICY.GET", "FLOW.RETENTION_CLEANUP"}:
            return {b"ok": 1}
        if command == "FLOW.HISTORY":
            return [[b"event-1", {b"event": b"created"}]]
        if command in {
            "FLOW.CREATE_MANY",
            "FLOW.COMPLETE_MANY",
            "FLOW.RETRY_MANY",
            "FLOW.FAIL_MANY",
        }:
            return [b"OK"]
        if command == "FLOW.VALUE.MGET":
            return [b'{"ok": true}']
        if command == "RATELIMIT.ADD":
            return [b"allowed", 1, 9, 100]
        return b"OK"

    async def close(self):
        self.closed = True


class OverloadThenAckAsyncRedis(FakeAsyncRedis):
    def __init__(self, overloads: int):
        super().__init__()
        self.overloads = overloads

    async def execute_command(self, *args):
        self.calls.append(args)
        if self.overloads > 0:
            self.overloads -= 1
            raise OverloadedError("ERR overloaded")
        return b"OK"


class CreateAckThenGetAsyncRedis(FakeAsyncRedis):
    async def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "FLOW.CREATE":
            return b"OK"
        if args[0] == "FLOW.GET":
            return {
                b"id": args[1].encode() if isinstance(args[1], str) else args[1],
                b"type": b"order",
                b"state": b"queued",
                b"partition_key": args[args.index("PARTITION") + 1].encode(),
                b"version": 1,
            }
        return await super().execute_command(*args)


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


def test_async_create_honors_zero_now_ms():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        await client.create("f-zero", type="order", now_ms=0, return_record=False)

        call = redis.calls[0]
        assert call[call.index("NOW") + 1] == 0
        assert call[call.index("RUN_AT") + 1] == 0

    run(main())


def test_async_command_pipeline_is_top_level_exported():
    from ferricstore import AsyncCommandPipeline

    assert AsyncCommandPipeline.__name__ == "AsyncCommandPipeline"


def test_async_command_pipeline_maps_native_redis_pipeline_errors():
    async def main():
        class ErrorPipe:
            def execute_command(self, *args):
                return self

            async def execute(self):
                raise RuntimeError("ERR flow already exists")

        class NativeRedis:
            def pipeline(self, transaction=False):
                return ErrorPipe()

        class RedisExecutor:
            client = NativeRedis()

            async def execute_command(self, *args):
                return b"OK"

        client = AsyncFlowClient(RedisExecutor())

        with pytest.raises(FlowAlreadyExistsError):
            await client.pipeline().command("FLOW.CREATE", "f1").execute()

    run(main())


def test_async_direct_command_errors_are_typed():
    async def main():
        class ErrorRedis:
            async def execute_command(self, *args):
                if args[0] == "FLOW.CREATE":
                    raise RuntimeError("ERR flow already exists")
                raise RuntimeError("ERR stale flow lease")

        client = AsyncFlowClient(ErrorRedis())

        with pytest.raises(FlowAlreadyExistsError):
            await client.command("FLOW.CREATE", "f1")

        with pytest.raises(StaleLeaseError):
            await client.command("FLOW.COMPLETE", "f1")

    run(main())


def test_async_direct_many_methods_noop_on_empty_inputs():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        assert await client.create_many("tenant:1", [], type="order") == []
        assert await client.complete_many("tenant:1", []) == []
        assert (
            await client.transition_many(
                "tenant:1",
                from_state="running",
                to_state="next",
                items=[],
            )
            == []
        )
        assert await client.retry_many("tenant:1", []) == []
        assert await client.fail_many("tenant:1", []) == []
        assert await client.cancel_many("tenant:1", []) == []
        assert redis.calls == []

    run(main())


def test_async_create_ack_followup_get_uses_auto_partition_when_partition_omitted():
    async def main():
        redis = CreateAckThenGetAsyncRedis()
        client = AsyncFlowClient(redis)
        expected_partition = f"__flow_auto__:{zlib.crc32(b'f-auto') % 256}"

        record = await client.create(
            "f-auto",
            type="order",
            payload=b"hello",
            now_ms=100,
            return_record=True,
        )

        assert record.id == "f-auto"
        assert redis.calls[1][:2] == ("FLOW.GET", "f-auto")
        assert redis.calls[1][redis.calls[1].index("PARTITION") + 1] == expected_partition

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


def test_async_claim_jobs_can_request_compact_state_items():
    async def main():
        redis = FakeAsyncRedis()
        redis.responses.append([[b"f1", b"tenant:1", b"lease", 7, b"ready"]])
        client = AsyncFlowClient(redis)

        jobs = await client.claim_jobs(
            "order",
            worker="worker-1",
            partition_key="tenant:1",
            limit=10,
            block_ms=5000,
            include_state=True,
        )

        assert jobs == [
            ClaimedItem(
                id="f1",
                partition_key="tenant:1",
                lease_token=b"lease",
                fencing_token=7,
                run_state="ready",
            )
        ]
        assert redis.calls[0] == (
            "FLOW.CLAIM_DUE",
            "order",
            "WORKER",
            "worker-1",
            "LEASE_MS",
            30000,
            "LIMIT",
            10,
            "PARTITION",
            "tenant:1",
            "PRIORITY",
            0,
            "RETURN",
            "JOBS_COMPACT_STATE",
            "BLOCK",
            5000,
        )

    run(main())


def test_async_claim_due_omits_now_when_not_supplied():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        await client.claim_jobs(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            limit=10,
        )

        assert "NOW" not in redis.calls[0]

    run(main())


def test_async_claim_due_sends_block_when_supplied():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        await client.claim_jobs(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            limit=10,
            block_ms=5000,
        )

        assert "BLOCK" in redis.calls[0]
        assert redis.calls[0][redis.calls[0].index("BLOCK") + 1] == 5000

    run(main())


def test_async_claim_jobs_only_sends_reclaim_expired_when_explicit():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        await client.claim_jobs(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            reclaim_expired=False,
        )

        call = redis.calls[0]
        assert call[call.index("RECLAIM_EXPIRED") : call.index("RECLAIM_EXPIRED") + 2] == (
            "RECLAIM_EXPIRED",
            "false",
        )

    run(main())


def test_async_reclaim_exposes_claim_due_response_options_and_partitions():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        result = await client.reclaim(
            "order",
            worker="worker-1",
            partition_keys=["p1", "p2"],
            priority=5,
            limit=10,
            now_ms=100,
            job_only=True,
            payload=False,
            values=["order"],
            value_max_bytes=128,
        )

        assert isinstance(result[0], ClaimedItem)
        assert redis.calls[0] == (
            "FLOW.RECLAIM",
            "order",
            "WORKER",
            "worker-1",
            "LEASE_MS",
            30000,
            "LIMIT",
            10,
            "NOW",
            100,
            "PARTITIONS",
            2,
            "p1",
            "p2",
            "PRIORITY",
            5,
            "RETURN",
            "JOBS_COMPACT",
            "NOPAYLOAD",
            "VALUE",
            "order",
            "VALUE_MAX_BYTES",
            128,
        )

    run(main())


def test_async_reclaim_rejects_non_running_state_alias():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        with pytest.raises(ValueError, match=r"FLOW\.RECLAIM only supports running"):
            await client.reclaim("order", state="queued", worker="worker-1")

        assert redis.calls == []

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


def test_async_enqueue_passes_retention_ttl_ms():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        result = await client.enqueue(
            "f1",
            type="order",
            payload=b"hello",
            now_ms=100,
            retention_ttl_ms=300_000,
        )

        assert result == b"OK"
        assert "RETENTION_TTL_MS" in redis.calls[0]
        assert redis.calls[0][redis.calls[0].index("RETENTION_TTL_MS") + 1] == 300_000

    run(main())


def test_async_enqueue_retries_server_overload_with_backpressure():
    async def main():
        redis = OverloadThenAckAsyncRedis(overloads=2)
        client = AsyncFlowClient(
            redis,
            backpressure=BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0),
        )

        result = await client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

        assert result == b"OK"
        assert len(redis.calls) == 3

    run(main())


def test_async_enqueue_default_backpressure_retries_until_server_recovers():
    async def main():
        redis = OverloadThenAckAsyncRedis(overloads=12)
        client = AsyncFlowClient(
            redis,
            backpressure=BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0),
        )

        result = await client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

        assert result == b"OK"
        assert len(redis.calls) == 13

    run(main())


def test_async_enqueue_stops_after_backpressure_retry_budget():
    async def main():
        redis = OverloadThenAckAsyncRedis(overloads=2)
        client = AsyncFlowClient(
            redis,
            backpressure=BackpressurePolicy(
                max_retries=1,
                base_delay_ms=0,
                max_delay_ms=0,
                jitter=0,
            ),
        )

        with pytest.raises(OverloadedError):
            await client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

        assert len(redis.calls) == 2

    run(main())


def test_async_enqueue_many_passes_retention_ttl_ms_to_auto_bucket_batches():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        result = await client.enqueue_many(
            [CreateItem("flow-1", b"a"), CreateItem("flow-2", b"b")],
            type="order",
            now_ms=100,
            retention_ttl_ms=300_000,
        )

        assert result == [b"OK", b"OK"]
        assert redis.calls
        for call in redis.calls:
            assert "RETENTION_TTL_MS" in call
            assert call[call.index("RETENTION_TTL_MS") + 1] == 300_000

    run(main())


def test_async_create_many_mixed_allows_auto_partition_items():
    async def main():
        redis = FakeAsyncRedis()
        redis.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(redis)

        result = await client.create_many(
            None,
            [
                CreateItem("f1", b"p1"),
                CreateItem("f2", b"p2", partition_key="tenant:2"),
            ],
            type="order",
            state="queued",
            now_ms=100,
        )

        assert result == [b"OK", b"OK"]
        assert redis.calls[0] == (
            "FLOW.CREATE_MANY",
            "MIXED",
            "TYPE",
            "order",
            "STATE",
            "queued",
            "NOW",
            100,
            "RUN_AT",
            100,
            "ITEMS_EXT",
            2,
            "f1",
            "-",
            b"p1",
            0,
            0,
            "f2",
            "tenant:2",
            b"p2",
            0,
            0,
        )

    run(main())


def test_async_spawn_children_exposes_parent_guards_and_child_policies():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        await client.spawn_children(
            "parent-1",
            [ChildSpec("c1", "email", b"p1")],
            partition_key="parent-p",
            group_id="g1",
            from_state="running",
            wait_state="waiting_children",
            on_child_failed="ignore",
            on_parent_closed="abandon_children",
            success="done",
            failure="failed",
        )

        call = redis.calls[0]
        assert call[call.index("FROM_STATE") + 1] == "running"
        assert call[call.index("WAIT_STATE") + 1] == "waiting_children"
        assert call[call.index("ON_CHILD_FAILED") + 1] == "ignore"
        assert call[call.index("ON_PARENT_CLOSED") + 1] == "abandon_children"
        assert call[call.index("SUCCESS") + 1] == "done"
        assert call[call.index("FAILURE") + 1] == "failed"

    run(main())


def test_async_many_commands_reject_items_from_different_explicit_partition():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        with pytest.raises(ValueError, match="partition_key"):
            await client.create_many(
                "p1", [CreateItem("f1", b"p", partition_key="p2")], type="order"
            )

        with pytest.raises(ValueError, match="partition_key"):
            await client.complete_many("p1", [ClaimedItem("f1", b"lease", 3, partition_key="p2")])

        with pytest.raises(ValueError, match="partition_key"):
            await client.cancel_many("p1", [FencedItem("f1", 3, partition_key="p2")])

        assert redis.calls == []

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


def test_async_value_mget_normalizes_omission_metadata_recursively():
    async def main():
        redis = FakeAsyncRedis()
        redis.responses = [
            [{b"ref": b"ref-a", b"omitted": True, b"size": 123, b"nested": {b"k": b"v"}}]
        ]
        client = AsyncFlowClient(redis)

        values = await client.value_mget(["ref-a"], max_bytes=10)

        assert values == [{"ref": "ref-a", "omitted": True, "size": 123, "nested": {"k": "v"}}]
        assert redis.calls[-1] == ("FLOW.VALUE.MGET", "ref-a", "MAX_BYTES", 10)

    run(main())


def test_async_query_policy_and_cleanup_commands():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        assert (await client.list("order", state="queued", count=10))[0].id == "f1"
        assert redis.calls[-1] == ("FLOW.LIST", "order", "STATE", "queued", "COUNT", 10)

        assert (await client.terminals("order", state="completed", rev=True, count=5))[0].id == "f1"
        assert redis.calls[-1] == (
            "FLOW.TERMINALS",
            "order",
            "COUNT",
            5,
            "REV",
            "true",
            "STATE",
            "completed",
        )

        assert (await client.failures("order", from_ms=10, to_ms=20))[0].id == "f1"
        assert redis.calls[-1] == ("FLOW.FAILURES", "order", "FROM_MS", 10, "TO_MS", 20)

        assert (await client.by_parent("p", count=1, terminal_only=True))[0].id == "f1"
        assert redis.calls[-1] == (
            "FLOW.BY_PARENT",
            "p",
            "COUNT",
            1,
            "TERMINAL_ONLY",
            "true",
        )

        assert (await client.by_root("root", count=1))[0].id == "f1"
        assert redis.calls[-1] == ("FLOW.BY_ROOT", "root", "COUNT", 1)

        assert (await client.by_correlation("checkout-1", include_cold=True))[0].id == "f1"
        assert redis.calls[-1] == ("FLOW.BY_CORRELATION", "checkout-1", "INCLUDE_COLD", "true")

        assert await client.info("order") == {b"ok": 1}
        assert (await client.stuck("order", older_than_ms=100, now_ms=200))[0].id == "f1"
        assert await client.history("f1", count=10, from_version=2, values=True)
        assert await client.policy_get("order", state="queued") == {b"ok": 1}
        assert await client.retention_cleanup(limit=100, now_ms=123) == {b"ok": 1}

    run(main())


def test_async_client_rejects_sync_flow_client():
    with pytest.raises(TypeError, match="requires an async executor"):
        AsyncFlowClient(FlowClient(FakeAsyncRedis()))
