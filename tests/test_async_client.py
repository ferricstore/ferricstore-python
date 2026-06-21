import asyncio
import inspect
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
from ferricstore.types import (
    ApprovalResult,
    ChildSpec,
    CircuitBreakerStatus,
    ClaimedFlow,
    CreateItem,
    EffectResult,
    FencedItem,
    GovernanceOverview,
    ScheduleResult,
)
from ferricstore.redis_commands import RedisCommandsMixin


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
            if "RETURN" in args and str(args[args.index("RETURN") + 1]).endswith("_ATTRS"):
                return [[b"f1", b"tenant:1", b"lease", 7, {b"tenant": b"acme"}]]
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
        if command == "FLOW.ATTRIBUTES":
            return [{b"name": b"tenant", b"count": 3}]
        if command == "FLOW.ATTRIBUTE_VALUES":
            return [{b"value": b"acme", b"count": 2}]
        if command in {
            "FLOW.SCHEDULE.LIST",
            "FLOW.APPROVAL.LIST",
            "FLOW.GOVERNANCE.LEDGER",
            "FLOW.LIMIT.LIST",
        }:
            return [{b"id": b"item-1", b"scope": b"tenant-a", b"status": b"active"}]
        if command == "FLOW.BUDGET.LIST":
            return [
                {
                    b"scope": b"tenant-a",
                    b"limit": 100,
                    b"window_ms": 60_000,
                    b"window_start_ms": 1_000,
                    b"used": 10,
                    b"remaining": 90,
                    b"over_budget": False,
                    b"reservations_count": 1,
                }
            ]
        if command in {
            "FLOW.BUDGET.RESERVE",
            "FLOW.BUDGET.COMMIT",
            "FLOW.BUDGET.RELEASE",
            "FLOW.BUDGET.GET",
        }:
            return {
                b"scope": b"tenant-a",
                b"limit": 100,
                b"window_ms": 60_000,
                b"window_start_ms": 1_000,
                b"used": 7,
                b"remaining": 93,
                b"over_budget": False,
                b"reservations_count": 1,
                b"reservation_id": b"budget-res-1",
                b"reserved_amount": 10,
                b"actual_amount": 7,
                b"status": b"committed",
                b"usage": {b"tokens": 7},
                b"overage_amount": 0,
                b"reserved_at_ms": 1_000,
                b"settled_at_ms": 2_000,
            }
        if command in {
            "FLOW.SCHEDULE.CREATE",
            "FLOW.SCHEDULE.GET",
            "FLOW.SCHEDULE.FIRE",
            "FLOW.SCHEDULE.PAUSE",
            "FLOW.SCHEDULE.RESUME",
            "FLOW.SCHEDULE.DELETE",
            "FLOW.SCHEDULE.FIRE_DUE",
            "FLOW.EFFECT.RESERVE",
            "FLOW.EFFECT.CONFIRM",
            "FLOW.EFFECT.FAIL",
            "FLOW.EFFECT.COMPENSATE",
            "FLOW.EFFECT.GET",
            "FLOW.APPROVAL.REQUEST",
            "FLOW.APPROVAL.APPROVE",
            "FLOW.APPROVAL.REJECT",
            "FLOW.APPROVAL.GET",
            "FLOW.GOVERNANCE.OVERVIEW",
            "FLOW.CIRCUIT.OPEN",
            "FLOW.CIRCUIT.CLOSE",
            "FLOW.CIRCUIT.GET",
            "FLOW.LIMIT.LEASE",
            "FLOW.LIMIT.SPEND",
            "FLOW.LIMIT.RELEASE",
            "FLOW.LIMIT.GET",
        }:
            return {b"id": b"item-1", b"scope": b"tenant-a", b"status": b"active"}
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


def test_async_start_and_step_continue_send_protocol_step_commands():
    async def main():
        redis = FakeAsyncRedis()
        redis.responses.extend(
            [
                {
                    b"id": b"f1",
                    b"type": b"order",
                    b"state": b"running",
                    b"run_state": b"reserve_inventory",
                    b"partition_key": b"tenant:1",
                    b"lease_token": b"lease-1",
                    b"fencing_token": 1,
                },
                {
                    b"id": b"f1",
                    b"type": b"order",
                    b"state": b"running",
                    b"run_state": b"charge_card",
                    b"partition_key": b"tenant:1",
                    b"lease_token": b"lease-2",
                    b"fencing_token": 2,
                },
            ]
        )
        client = AsyncFlowClient(redis)

        started = await client.start_and_claim(
            "f1",
            type="order",
            initial_state="reserve_inventory",
            worker="worker-1",
            partition_key="tenant:1",
            lease_ms=30_000,
            now_ms=100,
        )
        continued = await client.step_continue(
            "f1",
            lease_token=started.lease_token,
            from_state="reserve_inventory",
            to_state="charge_card",
            fencing_token=started.fencing_token,
            partition_key="tenant:1",
            lease_ms=45_000,
            now_ms=101,
        )

        assert started.run_state == "reserve_inventory"
        assert continued.run_state == "charge_card"
        assert redis.calls[0][:2] == ("FLOW.START_AND_CLAIM", "f1")
        assert redis.calls[1][:5] == (
            "FLOW.STEP_CONTINUE",
            "f1",
            b"lease-1",
            "reserve_inventory",
            "charge_card",
        )

    run(main())


def test_async_command_pipeline_is_top_level_exported():
    from ferricstore import AsyncCommandPipeline

    assert AsyncCommandPipeline.__name__ == "AsyncCommandPipeline"


def test_async_command_pipeline_maps_protocol_redis_pipeline_errors():
    async def main():
        class ErrorPipe:
            def execute_command(self, *args):
                return self

            async def execute(self):
                raise RuntimeError("ERR flow already exists")

        class ProtocolRedis:
            def pipeline(self, transaction=False):
                return ErrorPipe()

        class RedisExecutor:
            client = ProtocolRedis()

            async def execute_command(self, *args):
                return b"OK"

        client = AsyncFlowClient(RedisExecutor())

        with pytest.raises(FlowAlreadyExistsError):
            await client.pipeline().command("FLOW.CREATE", "f1").execute()

    run(main())


def test_async_session_and_blocking_helpers_are_named_commands():
    async def main():
        class RedisExecutor:
            def __init__(self):
                self.calls = []
                self.responses = [
                    [["subscribe", "jobs", 1]],
                    "OK",
                    "QUEUED",
                    ["OK"],
                    [b"queue", b"job"],
                ]

            async def execute_command(self, *args):
                self.calls.append(args)
                return self.responses.pop(0)

        redis = RedisExecutor()
        client = AsyncFlowClient(redis)

        assert await client.subscribe("jobs") == [["subscribe", "jobs", 1]]
        assert redis.calls[-1] == ("SUBSCRIBE", "jobs")
        assert await client.multi() == "OK"
        assert redis.calls[-1] == ("MULTI",)
        assert await client.set("k", "v") == "QUEUED"
        assert redis.calls[-1] == ("COMMAND_EXEC", "SET", "k", b"v")
        assert await client.transaction_exec() == ["OK"]
        assert redis.calls[-1] == ("EXEC",)
        assert await client.blpop("queue", timeout=1) == [b"queue", b"job"]
        assert redis.calls[-1] == ("BLPOP", "queue", 1)

    run(main())


def test_async_redis_command_helper_parity_with_sync_mixin():
    sync_methods = {
        name
        for name, value in RedisCommandsMixin.__dict__.items()
        if callable(value) and not name.startswith("_")
    }
    missing = sorted(name for name in sync_methods if not hasattr(AsyncFlowClient, name))

    assert missing == []
    for name in sync_methods - {"command"}:
        assert inspect.iscoroutinefunction(getattr(AsyncFlowClient, name)), name


def test_async_redis_command_helpers_are_codec_aware_and_easy_to_use():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis, codec=JsonCodec())
        redis.responses.extend(["OK", b'{"answer":42}', [b'{"a":1}', None]])

        assert await client.kv_set("kv", {"answer": 42}, px=100, nx=True) == "OK"
        assert redis.calls[-1] == ("SET", "kv", b'{"answer":42}', "PX", 100, "NX")
        assert await client.kv_get("kv") == {"answer": 42}
        assert redis.calls[-1] == ("GET", "kv")
        assert await client.kv_mget("a", "missing") == [{"a": 1}, None]
        assert redis.calls[-1] == ("MGET", "a", "missing")

    run(main())


def test_async_pubsub_session_decodes_native_events_without_raw_command_usage():
    async def main():
        class EventRedis(FakeAsyncRedis):
            def __init__(self):
                super().__init__()
                self.events = [
                    {
                        b"kind": b"message",
                        b"channel": b"jobs",
                        b"message": b'{"job":1}',
                    }
                ]

            async def wait_event(self, timeout=None):
                return self.events.pop(0) if self.events else None

        client = AsyncFlowClient(EventRedis(), codec=JsonCodec())
        pubsub = client.pubsub_session()

        message = await pubsub.get_message(timeout=0.01)

        assert message is not None
        assert message.kind == "message"
        assert message.channel == "jobs"
        assert message.message == {"job": 1}

    run(main())


def test_async_transaction_context_uses_named_helpers_inside_multi():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)
        redis.responses.extend(["OK", "QUEUED", ["OK"]])

        async with client.transaction() as tx:
            assert await tx.set("k", "v") == "QUEUED"

        assert redis.calls == [
            ("MULTI",),
            ("COMMAND_EXEC", "SET", "k", b"v"),
            ("EXEC",),
        ]

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


def test_async_claim_flows_and_complete_jobs_use_hot_compact_paths():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        jobs = await client.claim_flows(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            limit=100,
            now_ms=100,
        )
        result = await client.complete_jobs(jobs, result=b"done", now_ms=200)

        assert jobs == [
            ClaimedFlow(
                id="f1",
                partition_key="tenant:1",
                lease_token=b"lease",
                fencing_token=7,
                attributes={"tenant": "acme"},
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
            "JOBS_COMPACT_ATTRS",
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


def test_async_claim_due_accepts_legacy_job_only_alias():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        jobs = await client.claim_due(
            "order",
            state="queued",
            worker="worker-1",
            limit=10,
            priority=0,
            now_ms=100,
            job_only=True,
        )

        assert isinstance(jobs[0], ClaimedFlow)
        assert redis.calls[0][-2:] == ("RETURN", "JOBS_COMPACT_ATTRS")

    run(main())


def test_async_claim_due_rejects_conflicting_include_record_and_job_only():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        with pytest.raises(ValueError, match="include_record and job_only"):
            await client.claim_due(
                "order",
                state="queued",
                worker="worker-1",
                include_record=False,
                job_only=False,
            )

        assert redis.calls == []

    run(main())


def test_async_claim_flows_can_request_compact_state_items():
    async def main():
        redis = FakeAsyncRedis()
        redis.responses.append([[b"f1", b"tenant:1", b"lease", 7, b"ready", {b"tenant": b"acme"}]])
        client = AsyncFlowClient(redis)

        jobs = await client.claim_flows(
            "order",
            worker="worker-1",
            partition_key="tenant:1",
            limit=10,
            block_ms=5000,
            include_state=True,
        )

        assert jobs == [
            ClaimedFlow(
                id="f1",
                partition_key="tenant:1",
                lease_token=b"lease",
                fencing_token=7,
                run_state="ready",
                attributes={"tenant": "acme"},
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
            "JOBS_COMPACT_STATE_ATTRS",
            "BLOCK",
            5000,
        )

    run(main())


def test_async_claim_due_omits_now_when_not_supplied():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        await client.claim_flows(
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

        await client.claim_flows(
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


def test_async_claim_flows_only_sends_reclaim_expired_when_explicit():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        await client.claim_flows(
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
            include_record=False,
            payload=False,
            values=["order"],
            value_max_bytes=128,
        )

        assert isinstance(result[0], ClaimedFlow)
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
            "JOBS_COMPACT_ATTRS",
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


def test_async_create_many_can_attach_shared_attributes():
    async def main():
        redis = FakeAsyncRedis()
        redis.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(redis)

        result = await client.create_many(
            "tenant:1",
            [CreateItem("f1", b"p1"), CreateItem("f2", b"p2")],
            type="order",
            state="queued",
            now_ms=100,
            attributes={"tenant": "acme"},
        )

        assert result == [b"OK", b"OK"]
        assert redis.calls[0] == (
            "FLOW.CREATE_MANY",
            "tenant:1",
            "TYPE",
            "order",
            "STATE",
            "queued",
            "NOW",
            100,
            "RUN_AT",
            100,
            "ATTRIBUTE",
            "tenant",
            "acme",
            "ITEMS",
            "f1",
            b"p1",
            "f2",
            b"p2",
        )

    run(main())


def test_async_create_many_reuses_identical_item_attributes_as_shared_attributes():
    async def main():
        redis = FakeAsyncRedis()
        redis.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(redis)

        result = await client.create_many(
            "tenant:1",
            [
                CreateItem("f1", b"p1", attributes={"tenant": "acme"}),
                CreateItem("f2", b"p2", attributes={"tenant": "acme"}),
            ],
            type="order",
            now_ms=100,
        )

        assert result == [b"OK", b"OK"]
        call = redis.calls[0]
        assert call[call.index("ATTRIBUTE") : call.index("ATTRIBUTE") + 3] == (
            "ATTRIBUTE",
            "tenant",
            "acme",
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
            await client.complete_many("p1", [ClaimedFlow("f1", b"lease", 3, partition_key="p2")])

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


def test_async_admin_flow_wrappers_build_readable_commands_and_normalize_responses():
    async def main():
        redis = FakeAsyncRedis()
        client = AsyncFlowClient(redis)

        assert await client.attributes("order", state="queued", count=10) == [
            {"name": "tenant", "count": 3}
        ]
        assert redis.calls[-1] == ("FLOW.ATTRIBUTES", "order", "STATE", "queued", "COUNT", 10)

        assert await client.attribute_values("order", "tenant", state="queued") == [
            {"value": "acme", "count": 2}
        ]
        assert redis.calls[-1] == (
            "FLOW.ATTRIBUTE_VALUES",
            "order",
            "tenant",
            "STATE",
            "queued",
        )

        schedule = await client.schedule_create(
            "daily-report",
            target={"id": "flow-1", "type": "report", "state": "queued"},
            timezone="Asia/Jerusalem",
            overwrite=True,
            now_ms=100,
        )
        assert isinstance(schedule, ScheduleResult)
        assert schedule.status == "active"
        assert schedule["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.SCHEDULE.CREATE",
            "daily-report",
            "TIMEZONE",
            "Asia/Jerusalem",
            "TARGET",
            {"id": "flow-1", "type": "report", "state": "queued"},
            "OVERWRITE",
            "true",
            "NOW",
            100,
        )

        due = await client.schedule_fire_due(block_ms=1000, limit=50)
        assert isinstance(due, ScheduleResult)
        assert due["status"] == "active"
        assert redis.calls[-1] == ("FLOW.SCHEDULE.FIRE_DUE", "BLOCK", 1000, "LIMIT", 50)
        schedules = await client.schedule_list(target_type="flow")
        assert isinstance(schedules[0], ScheduleResult)
        assert schedules[0]["status"] == "active"
        assert redis.calls[-1] == ("FLOW.SCHEDULE.LIST", "TARGET_TYPE", "flow")

        effect = await client.effect_reserve(
            "flow-1",
            "send-email",
            "email.send",
            partition_key="tenant-a",
            lease_token=b"lease",
            fencing_token=7,
            operation_digest="digest",
            governance_scope="email",
            now_ms=101,
        )
        assert isinstance(effect, EffectResult)
        assert effect.status == "active"
        assert effect["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.EFFECT.RESERVE",
            "flow-1",
            "EFFECT_KEY",
            "send-email",
            "EFFECT_TYPE",
            "email.send",
            "PARTITION",
            "tenant-a",
            "LEASE_TOKEN",
            b"lease",
            "FENCING",
            7,
            "OPERATION_DIGEST",
            "digest",
            "GOVERNANCE_SCOPE",
            "email",
            "NOW",
            101,
        )

        effect = await client.effect_confirm(
            "flow-1",
            "send-email",
            external_id="mail-1",
            latency_ms=42,
        )
        assert isinstance(effect, EffectResult)
        assert effect["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.EFFECT.CONFIRM",
            "flow-1",
            "EFFECT_KEY",
            "send-email",
            "EXTERNAL_ID",
            "mail-1",
            "LATENCY_MS",
            42,
        )

        effect = await client.effect_fail(
            "flow-1",
            "send-email",
            error="smtp down",
            reason="provider_unavailable",
            latency_ms=84,
        )
        assert isinstance(effect, EffectResult)
        assert effect["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.EFFECT.FAIL",
            "flow-1",
            "EFFECT_KEY",
            "send-email",
            "ERROR",
            "smtp down",
            "REASON",
            "provider_unavailable",
            "LATENCY_MS",
            84,
        )

        effect = await client.effect_compensate(
            "flow-1",
            "send-email",
            lease_token=b"lease",
            fencing_token=7,
            external_id="mail-comp-1",
            reason="rollback",
        )
        assert isinstance(effect, EffectResult)
        assert effect["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.EFFECT.COMPENSATE",
            "flow-1",
            "EFFECT_KEY",
            "send-email",
            "LEASE_TOKEN",
            b"lease",
            "FENCING",
            7,
            "EXTERNAL_ID",
            "mail-comp-1",
            "REASON",
            "rollback",
        )

        approval = await client.approval_request(
            "approval-1",
            flow_id="flow-1",
            scope="tenant-a",
            assignees=["ops"],
            policy_hash="hash",
            policy_version=2,
            timeout_ms=30_000,
        )
        assert isinstance(approval, ApprovalResult)
        assert approval.status == "active"
        assert approval["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.APPROVAL.REQUEST",
            "approval-1",
            "FLOW_ID",
            "flow-1",
            "SCOPE",
            "tenant-a",
            "ASSIGNEES",
            ["ops"],
            "POLICY_HASH",
            "hash",
            "POLICY_VERSION",
            2,
            "TIMEOUT_MS",
            30_000,
        )

        approval = await client.approval_approve("approval-1", approver="admin", reason="ok")
        assert isinstance(approval, ApprovalResult)
        assert approval["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.APPROVAL.APPROVE",
            "approval-1",
            "APPROVER",
            "admin",
            "REASON",
            "ok",
        )

        ledger = await client.governance_ledger(
            "flow-1", partition_key="tenant-a", rev=True, limit=5
        )
        assert ledger[0]["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.GOVERNANCE.LEDGER",
            "flow-1",
            "PARTITION",
            "tenant-a",
            "LIMIT",
            5,
            "REV",
            "true",
        )

        circuit = await client.circuit_open("email", open_ms=1000, failure_threshold=3)
        assert isinstance(circuit, CircuitBreakerStatus)
        assert circuit.status == "active"
        assert circuit["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.CIRCUIT.OPEN",
            "email",
            "OPEN_MS",
            1000,
            "FAILURE_THRESHOLD",
            3,
        )

        budget = await client.budget_reserve(
            "tenant-a", 10, limit=100, window_ms=60_000, reservation_id="budget-res-1"
        )
        assert budget.scope == "tenant-a"
        assert budget.reservation_id == "budget-res-1"
        assert budget["remaining"] == 93
        assert redis.calls[-1] == (
            "FLOW.BUDGET.RESERVE",
            "tenant-a",
            "AMOUNT",
            10,
            "LIMIT",
            100,
            "WINDOW_MS",
            60_000,
            "RESERVATION_ID",
            "budget-res-1",
        )

        overview = await client.governance_overview(
            scope="tenant-a",
            status="pending",
            flow_id="flow-1",
        )
        assert isinstance(overview, GovernanceOverview)
        assert overview["status"] == "active"
        assert redis.calls[-1] == (
            "FLOW.GOVERNANCE.OVERVIEW",
            "SCOPE",
            "tenant-a",
            "STATUS",
            "pending",
            "FLOW_ID",
            "flow-1",
        )

        committed = await client.budget_commit("tenant-a", "budget-res-1", 7, usage={"tokens": 7})
        assert committed.status == "committed"
        assert committed.usage == {"tokens": 7}
        assert redis.calls[-1] == (
            "FLOW.BUDGET.COMMIT",
            "tenant-a",
            "RESERVATION_ID",
            "budget-res-1",
            "ACTUAL_AMOUNT",
            7,
            "USAGE",
            {"tokens": 7},
        )

        released = await client.budget_release("tenant-a", "budget-res-unused")
        assert released.get("reserved_amount") == 10
        assert redis.calls[-1] == (
            "FLOW.BUDGET.RELEASE",
            "tenant-a",
            "RESERVATION_ID",
            "budget-res-unused",
        )

        await client.limit_lease("tenant-a", shard_id=1, amount=5, ttl_ms=1000, limit=10)
        assert redis.calls[-1] == (
            "FLOW.LIMIT.LEASE",
            "tenant-a",
            "SHARD_ID",
            1,
            "AMOUNT",
            5,
            "LIMIT",
            10,
            "TTL_MS",
            1000,
        )

    run(main())
