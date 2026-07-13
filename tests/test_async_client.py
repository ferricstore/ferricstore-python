import asyncio
import inspect
import json
import zlib

import pytest

import ferricstore.async_client as async_client_module
from ferricstore import (
    AsyncFlowClient,
    BackpressurePolicy,
    FerricStoreError,
    FlowAlreadyExistsError,
    FlowClient,
    FlowStateMode,
    FlowStatePolicy,
    JsonCodec,
    OverloadedError,
    StaleLeaseError,
)
from ferricstore.backpressure import BackpressureController
from ferricstore.commands import DataCommandsMixin
from ferricstore.errors import InvalidCommandError
from ferricstore.protocol import AsyncProtocolAdapterPool
from ferricstore.types import (
    ApprovalResult,
    ChildSpec,
    CircuitBreakerStatus,
    ClaimedFlow,
    CreateItem,
    EffectResult,
    FencedItem,
    GovernanceOverview,
    RetryPolicy,
    ScheduleResult,
)


class FakeAsyncExecutor:
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
            "FLOW.SEARCH",
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


def test_async_transaction_suspends_heartbeat_until_exec():
    class TransactionExecutor:
        def __init__(self) -> None:
            self.paused = 0
            self.calls: list[tuple[object, ...]] = []

        async def pause_heartbeat(self) -> None:
            self.paused += 1

        async def resume_heartbeat(self) -> None:
            self.paused -= 1

        async def execute_command(self, *args):
            self.calls.append(args)
            return b"OK"

    async def run() -> None:
        executor = TransactionExecutor()
        client = AsyncFlowClient(executor)

        await client.multi()
        assert executor.paused == 1
        await client.command("SET", "key", "value")
        assert executor.paused == 1
        await client.transaction_exec()

        assert executor.paused == 0
        assert executor.calls == [
            ("MULTI",),
            ("COMMAND_EXEC", "SET", "key", "value"),
            ("EXEC",),
        ]

    asyncio.run(run())


def test_async_transaction_state_machine_accepts_byte_command_names():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses.extend([b"OK", b"QUEUED", [b"OK"]])
        client = AsyncFlowClient(executor)

        await client.command(b"MULTI")
        await client.command(b"SET", b"key", b"value")
        await client.command(b"EXEC")

        assert executor.calls == [
            (b"MULTI",),
            ("COMMAND_EXEC", b"SET", b"key", b"value"),
            (b"EXEC",),
        ]

    run(main())


def test_async_pubsub_cleanup_failure_unsubscribes_patterns_and_invalidates_session():
    class SessionExecutor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []
            self.invalidated = False
            self.closed = False

        async def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "UNSUBSCRIBE":
                raise FerricStoreError("unsubscribe failed")
            return b"OK"

        async def invalidate(self) -> None:
            self.invalidated = True

        async def close(self) -> None:
            self.closed = True

    class RootExecutor:
        def __init__(self, session: SessionExecutor) -> None:
            self.session = session

        async def execute_command(self, *_args):
            return b"OK"

        async def acquire_session(self) -> SessionExecutor:
            return self.session

    async def run() -> None:
        session = SessionExecutor()
        pubsub = AsyncFlowClient(RootExecutor(session)).pubsub_session()
        await pubsub.subscribe("jobs")

        with pytest.raises(FerricStoreError, match="unsubscribe failed"):
            await pubsub.close()

        assert ("UNSUBSCRIBE",) in session.calls
        assert ("PUNSUBSCRIBE",) in session.calls
        assert session.invalidated
        assert session.closed

    asyncio.run(run())


def test_async_complete_and_claim_uses_single_batch_round_trip():
    class BatchExecutor:
        def __init__(self) -> None:
            self.batches: list[list[tuple[object, ...]]] = []

        async def execute_command(self, *_args):
            return b"OK"

        async def execute_batch(self, commands):
            self.batches.append(commands)
            return [b"OK", [[b"next", b"p1", b"next-lease", 8]]]

    async def run() -> None:
        executor = BatchExecutor()
        client = AsyncFlowClient(executor)
        jobs = [ClaimedFlow("done", b"lease", 7, partition_key="p1")]

        claimed = await client.complete_flows_and_claim_flows(
            jobs,
            result=b"result",
            type="order",
            state="queued",
            worker="worker-1",
            partition_key="p1",
        )

        assert len(executor.batches) == 1
        assert [command[0] for command in executor.batches[0]] == [
            "FLOW.COMPLETE_MANY",
            "FLOW.CLAIM_DUE",
        ]
        assert claimed == [ClaimedFlow("next", b"next-lease", 8, partition_key="p1")]

    asyncio.run(run())


def test_async_subscribe_flow_wake_delegates_to_protocol_executor():
    class WakeExecutor:
        def __init__(self) -> None:
            self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

        async def execute_command(self, *_args):
            return b"OK"

        async def subscribe_flow_wake(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return b"subscribed"

    async def run() -> None:
        executor = WakeExecutor()
        client = AsyncFlowClient(executor)

        result = await client.subscribe_flow_wake(
            "order",
            state="queued",
            partition_key="p1",
            limit=10,
        )

        assert result == b"subscribed"
        assert executor.calls == [
            (
                ("order",),
                {
                    "state": "queued",
                    "states": None,
                    "partition_key": "p1",
                    "partition_keys": None,
                    "priority": 0,
                    "limit": 10,
                },
            )
        ]

    asyncio.run(run())


class OverloadThenAckAsyncExecutor(FakeAsyncExecutor):
    def __init__(self, overloads: int):
        super().__init__()
        self.overloads = overloads

    async def execute_command(self, *args):
        self.calls.append(args)
        if self.overloads > 0:
            self.overloads -= 1
            raise OverloadedError("ERR overloaded")
        return b"OK"


class CreateAckThenGetAsyncExecutor(FakeAsyncExecutor):
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
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

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
        assert executor.calls == [
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


def test_async_transaction_cleans_watch_if_multi_fails():
    class Session:
        def __init__(self) -> None:
            self.calls = []
            self.invalidated = False

        async def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "MULTI":
                raise FerricStoreError("MULTI failed")
            return b"OK"

        async def close(self):
            pass

        async def invalidate(self):
            self.invalidated = True

    class Executor:
        def __init__(self) -> None:
            self.session = Session()

        async def execute_command(self, *_args):
            return b"OK"

        async def acquire_session_for_key(self, _key):
            return self.session

    async def main():
        executor = Executor()
        client = AsyncFlowClient(executor)

        with pytest.raises(FerricStoreError, match="MULTI failed"):
            async with client.transaction(watch=["tenant:{42}"]):
                pass

        assert executor.session.calls == [
            ("WATCH", "tenant:{42}"),
            ("MULTI",),
            ("UNWATCH",),
        ]
        assert executor.session.invalidated

    run(main())


def test_async_claim_retries_legacy_compact_return_mode():
    class Executor:
        def __init__(self) -> None:
            self.calls = []

        async def execute_command(self, *args):
            self.calls.append(args)
            if len(self.calls) == 1:
                raise FerricStoreError("FLOW CLAIM return must be records, jobs, or jobs_compact")
            return [[b"f1", b"p1", b"lease", 7]]

    async def main():
        executor = Executor()
        client = AsyncFlowClient(executor)

        jobs = await client.claim_flows("jobs", worker="w1")

        assert [job.id for job in jobs] == ["f1"]
        assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == ("JOBS_COMPACT_ATTRS")
        assert executor.calls[1][executor.calls[1].index("RETURN") + 1] == "JOBS_COMPACT"

    run(main())


def test_async_manual_transaction_commands_fail_fast_on_rotating_pool():
    class Adapter:
        async def execute_command(self, *_args):
            return b"OK"

        async def close(self):
            pass

    async def main():
        client = AsyncFlowClient(AsyncProtocolAdapterPool([Adapter(), Adapter()]))

        with pytest.raises(InvalidCommandError, match=r"transaction\(\)"):
            await client.multi()
        with pytest.raises(InvalidCommandError, match=r"transaction\(\)"):
            await client.watch("key")

    run(main())


def test_async_transaction_key_acquires_routed_affine_session():
    class Session:
        def __init__(self) -> None:
            self.calls = []

        async def execute_command(self, *args):
            self.calls.append(args)
            return b"OK"

        async def close(self):
            pass

    class Executor:
        def __init__(self) -> None:
            self.key = None
            self.session = Session()

        async def execute_command(self, *_args):
            raise AssertionError("transaction should use its affine session")

        async def acquire_session_for_key(self, key):
            self.key = key
            return self.session

    async def main():
        executor = Executor()
        client = AsyncFlowClient(executor)

        async with client.transaction(key="tenant:{42}") as transaction:
            await transaction.command("SET", "tenant:{42}", b"value")

        assert executor.key == "tenant:{42}"
        assert executor.session.calls == [
            ("MULTI",),
            ("COMMAND_EXEC", "SET", "tenant:{42}", b"value"),
            ("EXEC",),
        ]

    run(main())


def test_async_transaction_validates_key_and_all_watches_before_acquiring_session():
    class Executor:
        def __init__(self) -> None:
            self.session_keys = None
            self.calls = []

        async def execute_command(self, *args):
            self.calls.append(args)
            return b"OK"

        async def acquire_session_for_keys(self, keys):
            self.session_keys = keys
            raise InvalidCommandError("transaction keys must hash to the same slot")

    async def main():
        executor = Executor()
        client = AsyncFlowClient(executor)

        with pytest.raises(InvalidCommandError, match="same slot"):
            async with client.transaction(key="a", watch=["b"]):
                pass

        assert executor.session_keys == ("a", "b")
        assert executor.calls == []

    run(main())


def test_async_complete_and_claim_rejects_partial_completion_result():
    class Executor:
        async def execute_command(self, *_args):
            return b"OK"

        async def execute_batch(self, _commands):
            return [[b"OK", FerricStoreError("stale lease")], []]

    async def main():
        client = AsyncFlowClient(Executor())
        jobs = [
            ClaimedFlow("ok", b"lease-1", 1, partition_key="p1"),
            ClaimedFlow("stale", b"lease-2", 2, partition_key="p1"),
        ]

        with pytest.raises(FerricStoreError, match="stale lease"):
            await client.complete_flows_and_claim_flows(
                jobs,
                type="jobs",
                worker="w1",
                partition_key="p1",
            )

    run(main())


def test_async_create_honors_zero_now_ms():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        await client.create("f-zero", type="order", now_ms=0, return_record=False)

        call = executor.calls[0]
        assert call[call.index("NOW") + 1] == 0
        assert call[call.index("RUN_AT") + 1] == 0

    run(main())


def test_async_start_and_step_continue_send_protocol_step_commands():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses.extend(
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
        client = AsyncFlowClient(executor)

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
        assert executor.calls[0][:2] == ("FLOW.START_AND_CLAIM", "f1")
        assert executor.calls[1][:5] == (
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


def test_async_command_pipeline_maps_native_execute_batch_errors():
    async def main():
        class BatchExecutor:
            async def execute_command(self, *args):
                return b"OK"

            async def execute_batch(self, commands):
                raise RuntimeError("ERR flow already exists")

        client = AsyncFlowClient(BatchExecutor())

        with pytest.raises(FlowAlreadyExistsError):
            await client.pipeline().command("FLOW.CREATE", "f1").execute()

    run(main())


def test_async_command_pipeline_rejects_wrong_batch_cardinality():
    async def main():
        class BatchExecutor:
            async def execute_command(self, *args):
                return b"OK"

            async def execute_batch(self, commands):
                return []

        client = AsyncFlowClient(BatchExecutor())

        with pytest.raises(FerricStoreError, match="returned 0 items; expected 1"):
            await client.pipeline().command("GET", "key").execute()

    run(main())


def test_async_session_and_blocking_helpers_are_named_commands():
    async def main():
        class ExecutorExecutor:
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

        executor = ExecutorExecutor()
        client = AsyncFlowClient(executor)

        assert await client.subscribe("jobs") == [["subscribe", "jobs", 1]]
        assert executor.calls[-1] == ("SUBSCRIBE", "jobs")
        assert await client.multi() == "OK"
        assert executor.calls[-1] == ("MULTI",)
        assert await client.set("k", "v") == "QUEUED"
        assert executor.calls[-1] == ("COMMAND_EXEC", "SET", "k", b"v")
        assert await client.transaction_exec() == ["OK"]
        assert executor.calls[-1] == ("EXEC",)
        assert await client.blpop("queue", timeout=1) == [b"queue", b"job"]
        assert executor.calls[-1] == ("BLPOP", "queue", 1)

    run(main())


def test_async_data_command_helper_parity_with_sync_mixin():
    sync_methods = {
        name
        for name, value in DataCommandsMixin.__dict__.items()
        if callable(value) and not name.startswith("_")
    }
    missing = sorted(name for name in sync_methods if not hasattr(AsyncFlowClient, name))

    assert missing == []
    for name in sync_methods - {"command"}:
        assert inspect.iscoroutinefunction(getattr(AsyncFlowClient, name)), name


def test_async_data_command_helpers_are_codec_aware_and_easy_to_use():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor, codec=JsonCodec())
        executor.responses.extend(["OK", b'{"answer":42}', [b'{"a":1}', None]])

        assert await client.kv_set("kv", {"answer": 42}, px=100, nx=True) == "OK"
        assert executor.calls[-1] == ("SET", "kv", b'{"answer":42}', "PX", 100, "NX")
        assert await client.kv_get("kv") == {"answer": 42}
        assert executor.calls[-1] == ("GET", "kv")
        assert await client.kv_mget("a", "missing") == [{"a": 1}, None]
        assert executor.calls[-1] == ("MGET", "a", "missing")

    run(main())


def test_async_shared_backpressure_wait_observes_extensions_while_sleeping(monkeypatch):
    now = [100.0]
    sleeps: list[float] = []
    controller = BackpressureController(BackpressurePolicy(jitter=0, shared=False))
    controller._state.blocked_until = 101.0

    monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: now[0])

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        now[0] += delay
        if len(sleeps) == 1:
            controller._state.blocked_until = now[0] + 2.0

    monkeypatch.setattr("ferricstore.backpressure.asyncio.sleep", sleep)

    async def main() -> None:
        assert await controller.before_request_async(elapsed_s=0.0)

    run(main())
    assert sleeps == [1.0, 2.0]


def test_async_pubsub_session_decodes_native_events_without_raw_command_usage():
    async def main():
        class EventExecutor(FakeAsyncExecutor):
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

        client = AsyncFlowClient(EventExecutor(), codec=JsonCodec())
        pubsub = client.pubsub_session()

        message = await pubsub.get_message(timeout=0.01)

        assert message is not None
        assert message.kind == "message"
        assert message.channel == "jobs"
        assert message.message == {"job": 1}

    run(main())


def test_async_transaction_context_uses_named_helpers_inside_multi():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)
        executor.responses.extend(["OK", "QUEUED", ["OK"]])

        async with client.transaction() as tx:
            assert await tx.set("k", "v") == "QUEUED"

        assert executor.calls == [
            ("MULTI",),
            ("COMMAND_EXEC", "SET", "k", b"v"),
            ("EXEC",),
        ]

    run(main())


def test_async_transaction_context_uses_connection_affine_executor_session():
    async def main():
        class SessionExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.closed = False

            async def close(self):
                self.closed = True

        class PoolExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.session = SessionExecutor()

            async def acquire_session(self):
                return self.session

        executor = PoolExecutor()
        executor.session.responses.extend(["OK", "QUEUED", ["OK"]])
        client = AsyncFlowClient(executor)

        async with client.transaction() as tx:
            assert await tx.set("k", "v") == "QUEUED"

        assert executor.calls == []
        assert executor.session.calls == [
            ("MULTI",),
            ("COMMAND_EXEC", "SET", "k", b"v"),
            ("EXEC",),
        ]
        assert executor.session.closed

    run(main())


def test_async_transaction_exec_failure_discards_before_releasing_affine_session():
    async def main():
        class SessionExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.closed = False
                self.invalidated = False

            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "EXEC":
                    raise FerricStoreError("exec failed")
                return b"OK"

            async def close(self):
                self.closed = True

            async def invalidate(self):
                self.invalidated = True

        class PoolExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.session = SessionExecutor()

            async def acquire_session(self):
                return self.session

        executor = PoolExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(FerricStoreError, match="exec failed"):
            async with client.transaction():
                pass

        assert executor.session.calls == [("MULTI",), ("EXEC",), ("DISCARD",)]
        assert executor.session.invalidated
        assert executor.session.closed

    run(main())


def test_async_transaction_preserves_exec_failure_when_session_close_also_fails():
    async def main():
        class SessionExecutor(FakeAsyncExecutor):
            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "EXEC":
                    raise FerricStoreError("primary async exec failure")
                return b"OK"

            async def close(self):
                raise RuntimeError("secondary async close failure")

            async def invalidate(self):
                pass

        class PoolExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.session = SessionExecutor()

            async def acquire_session(self):
                return self.session

        transaction = AsyncFlowClient(PoolExecutor()).transaction()
        with pytest.raises(FerricStoreError, match="primary async exec failure") as raised:
            async with transaction:
                pass

        assert isinstance(raised.value.__cause__, RuntimeError)
        assert "secondary async close failure" in str(raised.value.__cause__)
        assert transaction._active_client is None
        assert not transaction._owns_client

    run(main())


def test_async_transaction_preserves_body_failure_when_discard_also_fails():
    async def main():
        class SessionExecutor(FakeAsyncExecutor):
            async def execute_command(self, *args):
                self.calls.append(args)
                if args[0] == "DISCARD":
                    raise FerricStoreError("secondary async discard failure")
                return b"OK"

            async def close(self):
                pass

            async def invalidate(self):
                pass

        class PoolExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.session = SessionExecutor()

            async def acquire_session(self):
                return self.session

        transaction = AsyncFlowClient(PoolExecutor()).transaction()
        with pytest.raises(ValueError, match="primary async body failure") as raised:
            async with transaction:
                raise ValueError("primary async body failure")

        assert isinstance(raised.value.__cause__, FerricStoreError)
        assert "secondary async discard failure" in str(raised.value.__cause__)
        assert transaction._active_client is None
        assert not transaction._owns_client

    run(main())


def test_async_pubsub_uses_connection_affine_executor_session():
    async def main():
        class SessionExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.closed = False
                self.events = [{b"kind": b"message", b"channel": b"jobs", b"message": b"one"}]

            async def wait_event(self, timeout=None):
                return self.events.pop(0) if self.events else None

            async def close(self):
                self.closed = True

        class PoolExecutor(FakeAsyncExecutor):
            def __init__(self):
                super().__init__()
                self.session = SessionExecutor()

            async def acquire_session(self):
                return self.session

        executor = PoolExecutor()
        client = AsyncFlowClient(executor)
        pubsub = client.pubsub_session()

        await pubsub.subscribe("jobs")
        message = await pubsub.get_message(timeout=0)
        await pubsub.close()

        assert message is not None
        assert message.channel == "jobs"
        assert executor.calls == []
        assert executor.session.calls[0] == ("SUBSCRIBE", "jobs")
        assert executor.session.closed

    run(main())


def test_async_direct_command_errors_are_typed():
    async def main():
        class ErrorExecutor:
            async def execute_command(self, *args):
                if args[0] == "FLOW.CREATE":
                    raise RuntimeError("ERR flow already exists")
                raise RuntimeError("ERR stale flow lease")

        client = AsyncFlowClient(ErrorExecutor())

        with pytest.raises(FlowAlreadyExistsError):
            await client.command("FLOW.CREATE", "f1")

        with pytest.raises(StaleLeaseError):
            await client.command("FLOW.COMPLETE", "f1")

    run(main())


def test_async_direct_many_methods_noop_on_empty_inputs():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

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
        assert executor.calls == []

    run(main())


def test_async_create_ack_followup_get_uses_auto_partition_when_partition_omitted():
    async def main():
        executor = CreateAckThenGetAsyncExecutor()
        client = AsyncFlowClient(executor)
        expected_partition = f"__flow_auto__:{zlib.crc32(b'f-auto') % 256}"

        record = await client.create(
            "f-auto",
            type="order",
            payload=b"hello",
            now_ms=100,
            return_record=True,
        )

        assert record.id == "f-auto"
        assert executor.calls[1][:2] == ("FLOW.GET", "f-auto")
        assert executor.calls[1][executor.calls[1].index("PARTITION") + 1] == expected_partition

    run(main())


def test_async_claim_flows_and_complete_jobs_use_hot_compact_paths():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

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
        assert executor.calls[0] == (
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
        assert executor.calls[1] == (
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


def test_async_run_steps_many_matches_sync_wire_shape_and_preserves_zero_now():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses.append(b"OK")
        client = AsyncFlowClient(executor)

        result = await client.run_steps_many(
            ["f1", CreateItem("f2", partition_key="tenant:2")],
            type="order",
            states=["reserve", "charge"],
            worker="worker-1",
            now_ms=0,
            partition_key="tenant:1",
        )

        assert result == b"OK"
        assert executor.calls[0] == (
            "FLOW.RUN_STEPS_MANY",
            "TYPE",
            "order",
            "STATES",
            ["reserve", "charge"],
            "WORKER",
            "worker-1",
            "LEASE_MS",
            30_000,
            "NOW",
            0,
            "ITEMS",
            [
                {"id": "f1", "partition_key": "tenant:1"},
                {"id": "f2", "partition_key": "tenant:2"},
            ],
        )

    run(main())


def test_async_step_continue_can_return_compact_claimed_job():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses.append([b"f1", b"tenant:1", b"lease-2", 8])
        client = AsyncFlowClient(executor)

        job = await client.step_continue(
            "f1",
            lease_token=b"lease-1",
            from_state="queued",
            to_state="running",
            fencing_token=7,
            partition_key="tenant:1",
            now_ms=100,
            return_job=True,
        )

        assert isinstance(job, ClaimedFlow)
        assert job.lease_token == b"lease-2"
        assert executor.calls[0][-2:] == ("RETURN", "JOBS_COMPACT")

    run(main())


def test_async_complete_many_and_jobs_support_ok_on_success():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)
        jobs = [ClaimedFlow("f1", b"lease", 7, partition_key="tenant:1")]

        await client.complete_many(
            "tenant:1",
            jobs,
            now_ms=100,
            return_ok_on_success=True,
        )
        await client.complete_jobs(jobs, now_ms=101, return_ok_on_success=True)

        assert all("OK_ON_SUCCESS" in call for call in executor.calls)

    run(main())


def test_async_flow_api_signatures_match_shared_sync_operations():
    for name in ("run_steps_many", "step_continue", "complete_many", "complete_jobs"):
        sync_parameters = list(inspect.signature(getattr(FlowClient, name)).parameters.values())[1:]
        async_parameters = list(
            inspect.signature(getattr(AsyncFlowClient, name)).parameters.values()
        )[1:]
        assert [
            (parameter.name, parameter.kind, parameter.default) for parameter in async_parameters
        ] == [(parameter.name, parameter.kind, parameter.default) for parameter in sync_parameters]


def test_async_claim_due_accepts_legacy_job_only_alias():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

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
        assert executor.calls[0][-2:] == ("RETURN", "JOBS_COMPACT_ATTRS")

    run(main())


def test_async_claim_due_rejects_conflicting_include_record_and_job_only():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match="include_record and job_only"):
            await client.claim_due(
                "order",
                state="queued",
                worker="worker-1",
                include_record=False,
                job_only=False,
            )

        assert executor.calls == []

    run(main())


def test_async_claim_flows_can_request_compact_state_items():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses.append(
            [[b"f1", b"tenant:1", b"lease", 7, b"ready", {b"tenant": b"acme"}]]
        )
        client = AsyncFlowClient(executor)

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
        assert executor.calls[0] == (
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
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        await client.claim_flows(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            limit=10,
        )

        assert "NOW" not in executor.calls[0]

    run(main())


def test_async_claim_due_sends_block_when_supplied():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        await client.claim_flows(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            limit=10,
            block_ms=5000,
        )

        assert "BLOCK" in executor.calls[0]
        assert executor.calls[0][executor.calls[0].index("BLOCK") + 1] == 5000

    run(main())


def test_async_claim_flows_only_sends_reclaim_expired_when_explicit():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        await client.claim_flows(
            "order",
            state="queued",
            worker="worker-1",
            partition_key="tenant:1",
            reclaim_expired=False,
        )

        call = executor.calls[0]
        assert call[call.index("RECLAIM_EXPIRED") : call.index("RECLAIM_EXPIRED") + 2] == (
            "RECLAIM_EXPIRED",
            "false",
        )

    run(main())


def test_async_reclaim_exposes_claim_due_response_options_and_partitions():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

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
        assert executor.calls[0] == (
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
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match=r"FLOW\.RECLAIM only supports running"):
            await client.reclaim("order", state="queued", worker="worker-1")

        assert executor.calls == []

    run(main())


def test_async_enqueue_many_keeps_auto_bucket_grouping():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        result = await client.enqueue_many(
            [CreateItem("flow-1", b"a"), CreateItem("flow-2", b"b")],
            type="order",
            now_ms=100,
        )

        assert result == [b"OK", b"OK"]
        assert all(call[0] == "FLOW.CREATE_MANY" for call in executor.calls)
        assert all(str(call[1]).startswith("__flow_auto__:") for call in executor.calls)

    run(main())


def test_async_enqueue_many_groups_explicit_partitions_and_pins_one_now(monkeypatch):
    class PartitionExecutor:
        def __init__(self):
            self.calls = []

        async def execute_command(self, *args):
            self.calls.append(args)
            item_offset = args.index("ITEMS") + 1
            return [str(item_id).encode() for item_id in args[item_offset::2]]

    async def main():
        executor = PartitionExecutor()
        client = AsyncFlowClient(executor)
        items = [
            CreateItem("flow-a", b"a", partition_key="tenant:b"),
            CreateItem("flow-b", b"b", partition_key="tenant:a"),
            CreateItem("flow-c", b"c", partition_key="tenant:b"),
            CreateItem("flow-d", b"d"),
            CreateItem("flow-e", b"e", partition_key=""),
        ]

        assert await client.enqueue_many(items, type="order") == [
            item.id.encode() for item in items
        ]
        assert all(call[0] == "FLOW.CREATE_MANY" for call in executor.calls)
        assert all(call[1] != "MIXED" for call in executor.calls)
        assert {call[1] for call in executor.calls} == {
            "tenant:a",
            "tenant:b",
            "",
            async_client_module._auto_partition_key_for_id("flow-d"),
        }
        assert {call[call.index("NOW") + 1] for call in executor.calls} == {123_456}

    monkeypatch.setattr(async_client_module, "_now_ms", lambda: 123_456)
    run(main())


def test_async_enqueue_many_uses_bounded_concurrent_fanout_for_safe_executors():
    class ConcurrentExecutor:
        supports_concurrent_fanout = True

        def __init__(self) -> None:
            self.active = 0
            self.max_active = 0

        async def execute_command(self, *args):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                await asyncio.sleep(0.01)
                item_args = args[args.index("ITEMS") + 1 :]
                return [id.encode() for id in item_args[0::2]]
            finally:
                self.active -= 1

    async def main():
        executor = ConcurrentExecutor()
        client = AsyncFlowClient(executor)
        items = [CreateItem(f"fanout-{idx}", b"payload") for idx in range(64)]

        assert await client.enqueue_many(items, type="order", now_ms=100) == [
            item.id.encode() for item in items
        ]
        assert 1 < executor.max_active <= 16

    run(main())


def test_async_enqueue_passes_retention_ttl_ms():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        result = await client.enqueue(
            "f1",
            type="order",
            payload=b"hello",
            now_ms=100,
            retention_ttl_ms=300_000,
        )

        assert result == b"OK"
        assert "RETENTION_TTL_MS" in executor.calls[0]
        assert executor.calls[0][executor.calls[0].index("RETENTION_TTL_MS") + 1] == 300_000

    run(main())


def test_async_enqueue_retries_server_overload_with_backpressure():
    async def main():
        executor = OverloadThenAckAsyncExecutor(overloads=2)
        client = AsyncFlowClient(
            executor,
            backpressure=BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0),
        )

        result = await client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

        assert result == b"OK"
        assert len(executor.calls) == 3

    run(main())


def test_async_enqueue_default_backpressure_retries_until_server_recovers():
    async def main():
        executor = OverloadThenAckAsyncExecutor(overloads=12)
        client = AsyncFlowClient(
            executor,
            backpressure=BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0),
        )

        result = await client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

        assert result == b"OK"
        assert len(executor.calls) == 13

    run(main())


def test_async_enqueue_stops_after_backpressure_retry_budget():
    async def main():
        executor = OverloadThenAckAsyncExecutor(overloads=2)
        client = AsyncFlowClient(
            executor,
            backpressure=BackpressurePolicy(
                max_retries=1,
                base_delay_ms=0,
                max_delay_ms=0,
                jitter=0,
            ),
        )

        with pytest.raises(OverloadedError):
            await client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

        assert len(executor.calls) == 2

    run(main())


def test_async_backpressure_retry_budget_rejects_wait_that_would_overrun(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("ferricstore.backpressure.asyncio.sleep", fake_sleep)

    async def main():
        controller = BackpressureController(
            BackpressurePolicy(
                max_retries=None,
                max_elapsed_ms=10,
                base_delay_ms=0,
                max_delay_ms=0,
                jitter=0,
                shared=False,
            )
        )

        allowed = await controller.record_overload_async(
            0,
            retry_after_ms=20,
            elapsed_s=0.009,
        )

        assert allowed is False
        assert sleeps == []

    run(main())


def test_async_enqueue_many_passes_retention_ttl_ms_to_auto_bucket_batches():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        result = await client.enqueue_many(
            [CreateItem("flow-1", b"a"), CreateItem("flow-2", b"b")],
            type="order",
            now_ms=100,
            retention_ttl_ms=300_000,
        )

        assert result == [b"OK", b"OK"]
        assert executor.calls
        for call in executor.calls:
            assert "RETENTION_TTL_MS" in call
            assert call[call.index("RETENTION_TTL_MS") + 1] == 300_000

    run(main())


def test_async_create_many_mixed_allows_auto_partition_items():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(executor)

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
        assert executor.calls[0] == (
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


def test_async_extended_many_items_preserve_explicit_empty_partition_keys():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(executor)

        await client.create_many(
            None,
            [
                CreateItem("f1", b"p1", partition_key="", values={"value": b"one"}),
                CreateItem("f2", b"p2", partition_key="tenant:2"),
            ],
            type="order",
            now_ms=100,
        )
        create_call = executor.calls[-1]
        create_items = create_call.index("ITEMS_EXT") + 2
        assert create_call[create_items : create_items + 3] == ("f1", "", b"p1")

        await client.spawn_children(
            "parent-1",
            [
                ChildSpec("c1", "email", b"p1", partition_key="", values={"value": b"one"}),
                ChildSpec("c2", "audit", b"p2", partition_key="tenant:2"),
            ],
            now_ms=100,
        )
        spawn_call = executor.calls[-1]
        spawn_items = spawn_call.index("ITEMS_EXT") + 2
        assert spawn_call[spawn_items : spawn_items + 4] == ("c1", "", "email", b"p1")

    run(main())


def test_async_create_many_can_attach_shared_attributes():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(executor)

        result = await client.create_many(
            "tenant:1",
            [CreateItem("f1", b"p1"), CreateItem("f2", b"p2")],
            type="order",
            state="queued",
            now_ms=100,
            attributes={"tenant": "acme"},
        )

        assert result == [b"OK", b"OK"]
        assert executor.calls[0] == (
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


def test_async_flow_mutation_commands_can_attach_state_meta():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)
        claimed = [ClaimedFlow("f1", b"lease", 7, partition_key="tenant:1")]
        fenced = [FencedItem("f1", 7, lease_token=b"lease", partition_key="tenant:1")]

        def assert_state_meta(call, value):
            index = call.index("STATE_META")
            assert call[index : index + 3] == ("STATE_META", "version", value)

        await client.create("f1", type="order", state_meta={"version": 1}, now_ms=100)
        assert_state_meta(executor.calls[-1], 1)

        executor.responses.append(
            {
                b"id": b"f2",
                b"type": b"order",
                b"state": b"running",
                b"partition_key": b"tenant:1",
                b"version": 1,
                b"lease_token": b"lease",
                b"fencing_token": 7,
            }
        )
        await client.start_and_claim(
            "f2",
            type="order",
            initial_state="accept",
            worker="worker-1",
            state_meta={"version": 2},
            now_ms=101,
        )
        assert_state_meta(executor.calls[-1], 2)

        await client.transition(
            "f1",
            from_state="queued",
            to_state="charged",
            lease_token=b"lease",
            fencing_token=7,
            state_meta={"version": 3},
            now_ms=102,
        )
        assert_state_meta(executor.calls[-1], 3)

        executor.responses.append(
            {
                b"id": b"f1",
                b"type": b"order",
                b"state": b"running",
                b"partition_key": b"tenant:1",
                b"version": 2,
                b"lease_token": b"lease",
                b"fencing_token": 8,
            }
        )
        await client.step_continue(
            "f1",
            lease_token=b"lease",
            from_state="charged",
            to_state="settled",
            fencing_token=7,
            state_meta={"version": 4},
            now_ms=103,
        )
        assert_state_meta(executor.calls[-1], 4)

        await client.complete(
            "f1",
            lease_token=b"lease",
            fencing_token=7,
            state_meta={"version": 5},
        )
        assert_state_meta(executor.calls[-1], 5)

        await client.retry(
            "f1",
            lease_token=b"lease",
            fencing_token=7,
            state_meta={"version": 6},
        )
        assert_state_meta(executor.calls[-1], 6)

        await client.fail(
            "f1",
            lease_token=b"lease",
            fencing_token=7,
            state_meta={"version": 7},
        )
        assert_state_meta(executor.calls[-1], 7)

        await client.cancel(
            "f1",
            fencing_token=7,
            lease_token=b"lease",
            state_meta={"version": 8},
        )
        assert_state_meta(executor.calls[-1], 8)

        await client.complete_many("tenant:1", claimed, state_meta={"version": 9})
        assert_state_meta(executor.calls[-1], 9)

        await client.transition_many(
            "tenant:1",
            from_state="queued",
            to_state="charged",
            items=fenced,
            state_meta={"version": 10},
        )
        assert_state_meta(executor.calls[-1], 10)

        await client.retry_many("tenant:1", claimed, state_meta={"version": 11})
        assert_state_meta(executor.calls[-1], 11)

        await client.fail_many("tenant:1", claimed, state_meta={"version": 12})
        assert_state_meta(executor.calls[-1], 12)

        await client.cancel_many("tenant:1", fenced, state_meta={"version": 13})
        assert_state_meta(executor.calls[-1], 13)

    run(main())


def test_async_create_many_can_attach_shared_state_meta():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(executor)

        result = await client.create_many(
            "tenant:1",
            [CreateItem("f1", b"p1"), CreateItem("f2", b"p2")],
            type="order",
            state="queued",
            now_ms=100,
            state_meta={"version": 1},
        )

        assert result == [b"OK", b"OK"]
        call = executor.calls[0]
        assert call[call.index("STATE_META") : call.index("STATE_META") + 3] == (
            "STATE_META",
            "version",
            1,
        )

    run(main())


def test_async_create_many_reuses_identical_item_attributes_as_shared_attributes():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses = [[b"OK", b"OK"]]
        client = AsyncFlowClient(executor)

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
        call = executor.calls[0]
        assert call[call.index("ATTRIBUTE") : call.index("ATTRIBUTE") + 3] == (
            "ATTRIBUTE",
            "tenant",
            "acme",
        )

    run(main())


def test_async_spawn_children_exposes_parent_guards_and_child_policies():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

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

        call = executor.calls[0]
        assert call[call.index("FROM_STATE") + 1] == "running"
        assert call[call.index("WAIT_STATE") + 1] == "waiting_children"
        assert call[call.index("ON_CHILD_FAILED") + 1] == "ignore"
        assert call[call.index("ON_PARENT_CLOSED") + 1] == "abandon_children"
        assert call[call.index("SUCCESS") + 1] == "done"
        assert call[call.index("FAILURE") + 1] == "failed"

    run(main())


def test_async_many_commands_reject_items_from_different_explicit_partition():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match="partition_key"):
            await client.create_many(
                "p1", [CreateItem("f1", b"p", partition_key="p2")], type="order"
            )

        with pytest.raises(ValueError, match="partition_key"):
            await client.complete_many("p1", [ClaimedFlow("f1", b"lease", 3, partition_key="p2")])

        with pytest.raises(ValueError, match="partition_key"):
            await client.cancel_many("p1", [FencedItem("f1", 3, partition_key="p2")])

        assert executor.calls == []

    run(main())


def test_async_signal_builds_flow_signal_command():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

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
        assert executor.calls[0] == (
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
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor, codec=JsonCodec())

        values = await client.value_mget(["ref-1"])
        await client.close()

        assert values == [{"ok": True}]
        assert executor.closed is True

    run(main())


def test_async_value_mget_normalizes_omission_metadata_recursively():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses = [
            [{b"ref": b"ref-a", b"omitted": True, b"size": 123, b"nested": {b"k": b"v"}}]
        ]
        client = AsyncFlowClient(executor)

        values = await client.value_mget(["ref-a"], max_bytes=10)

        assert values == [{"ref": "ref-a", "omitted": True, "size": 123, "nested": {"k": "v"}}]
        assert executor.calls[-1] == ("FLOW.VALUE.MGET", "ref-a", "MAX_BYTES", 10)

    run(main())


def test_async_value_mget_rejects_partial_responses():
    async def main():
        executor = FakeAsyncExecutor()
        executor.responses = [[]]
        client = AsyncFlowClient(executor)

        with pytest.raises(FerricStoreError, match="expected 1"):
            await client.value_mget(["ref-a"])

    run(main())


def test_async_flow_client_preserves_falsey_custom_codec():
    class FalseyCodec:
        def __bool__(self) -> bool:
            return False

        def encode(self, value):
            return str(value).encode()

        def decode(self, value):
            return value

    codec = FalseyCodec()

    assert AsyncFlowClient(FakeAsyncExecutor(), codec=codec).codec is codec


def test_async_query_policy_and_cleanup_commands():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        assert (await client.list("order", state="queued", count=10))[0].id == "f1"
        assert executor.calls[-1] == ("FLOW.LIST", "order", "STATE", "queued", "COUNT", 10)

        assert (await client.terminals("order", state="completed", rev=True, count=5))[0].id == "f1"
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == ("FLOW.FAILURES", "order", "FROM_MS", 10, "TO_MS", 20)

        assert (await client.by_parent("p", count=1, terminal_only=True))[0].id == "f1"
        assert executor.calls[-1] == (
            "FLOW.BY_PARENT",
            "p",
            "COUNT",
            1,
            "TERMINAL_ONLY",
            "true",
        )

        assert (await client.by_root("root", count=1))[0].id == "f1"
        assert executor.calls[-1] == ("FLOW.BY_ROOT", "root", "COUNT", 1)

        assert (await client.by_correlation("checkout-1", include_cold=True))[0].id == "f1"
        assert executor.calls[-1] == ("FLOW.BY_CORRELATION", "checkout-1", "INCLUDE_COLD", "true")

        assert await client.info("order") == {b"ok": 1}
        assert (await client.stuck("order", older_than_ms=100, now_ms=200))[0].id == "f1"
        assert await client.history("f1", count=10, from_version=2, values=True)
        assert await client.policy_get("order", state="queued") == {b"ok": 1}
        assert await client.retention_cleanup(limit=100, now_ms=123) == {b"ok": 1}

    run(main())


def test_async_client_rejects_sync_flow_client():
    with pytest.raises(TypeError, match="requires an async executor"):
        AsyncFlowClient(FlowClient(FakeAsyncExecutor()))


def test_async_install_policy_can_set_indexed_state_meta():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        await client.install_policy(
            "order",
            indexed_state_meta="version",
            retry=RetryPolicy(max_retries=2),
            states={"queued": RetryPolicy(max_retries=5)},
        )

        call = executor.calls[-1]
        assert call[:4] == ("FLOW.POLICY.SET", "order", "INDEXED_STATE_META", "version")
        assert call[4:6] == ("MAX_RETRIES", 2)
        state_index = call.index("STATE")
        assert call[state_index : state_index + 4] == ("STATE", "queued", "MAX_RETRIES", 5)

    run(main())


def test_async_install_policy_can_set_fifo_state_mode():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        await client.install_policy(
            "order",
            states={
                "queued": FlowStatePolicy(
                    mode=FlowStateMode.FIFO,
                    retry=RetryPolicy(max_retries=5),
                )
            },
        )

        call = executor.calls[-1]
        state_index = call.index("STATE")
        assert call[state_index : state_index + 5] == (
            "STATE",
            "queued",
            "MODE",
            "FIFO",
            "MAX_RETRIES",
        )

    run(main())


def test_async_management_wrappers_build_control_plane_commands_and_normalize_responses():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)
        executor.responses.extend(
            [
                {b"sdk": True, b"flow_observability": True},
                b"OK",
                {b"user": b"platform"},
                [b"default", b"platform"],
                b"OK",
                b"OK",
                {b"prefix": b"tenant:"},
                {b"prefix": b"tenant:"},
                [{b"prefix": b"tenant:"}],
                b"OK",
                {b"keys": 100, b"bytes": 1024},
                {b"keys": 100, b"bytes": 1024},
                {b"keys": 2, b"bytes": 256},
                {b"cluster": b"ok"},
                {b"keys": 2},
                [{b"id": b"f1", b"type": b"order"}],
                [{b"event": b"created"}],
            ]
        )

        assert await client.capabilities() == {"sdk": True, "flow_observability": True}
        assert await client.acl_set_user("platform", ["on", "+PING", "~tenant:*"]) == "OK"
        assert await client.acl_get_user("platform") == {"user": "platform"}
        assert await client.acl_list_users() == ["default", "platform"]
        assert await client.acl_del_user("platform") == "OK"
        assert await client.acl_save() == "OK"
        assert await client.ensure_namespace(
            "tenant:", {"owner": "platform"}, durability="memory"
        ) == {"prefix": "tenant:"}
        assert await client.get_namespace("tenant:") == {"prefix": "tenant:"}
        assert await client.list_namespaces() == [{"prefix": "tenant:"}]
        assert await client.delete_namespace("tenant:") == "OK"
        assert await client.set_quota("tenant:", {"keys": 100}, bytes=1024) == {
            "keys": 100,
            "bytes": 1024,
        }
        assert await client.get_quota("tenant:") == {"keys": 100, "bytes": 1024}
        assert await client.quota_usage("tenant:") == {"keys": 2, "bytes": 256}
        assert await client.cluster_info() == {"cluster": "ok"}
        assert await client.namespace_usage("tenant:") == {"keys": 2}
        assert await client.flow_query({"type": "order"}, state="queued") == [
            {"id": "f1", "type": "order"}
        ]
        assert await client.flow_history("f1", {"include": "metadata"}) == [{"event": "created"}]

        assert executor.calls == [
            ("FERRICSTORE.CAPABILITIES",),
            ("ACL", "SETUSER", "platform", "on", "+PING", "~tenant:*"),
            ("ACL", "GETUSER", "platform"),
            ("ACL", "LIST"),
            ("ACL", "DELUSER", "platform"),
            ("ACL", "SAVE"),
            (
                "FERRICSTORE.NAMESPACE",
                "ENSURE",
                "tenant:",
                "OWNER",
                "platform",
                "DURABILITY",
                "memory",
            ),
            ("FERRICSTORE.NAMESPACE", "GET", "tenant:"),
            ("FERRICSTORE.NAMESPACE", "LIST"),
            ("FERRICSTORE.NAMESPACE", "DELETE", "tenant:"),
            ("FERRICSTORE.QUOTA", "SET", "tenant:", "KEYS", 100, "BYTES", 1024),
            ("FERRICSTORE.QUOTA", "GET", "tenant:"),
            ("FERRICSTORE.QUOTA", "USAGE", "tenant:"),
            ("FERRICSTORE.TELEMETRY", "CLUSTER_INFO"),
            ("FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", "tenant:"),
            ("FERRICSTORE.TELEMETRY", "FLOW_QUERY", "TYPE", "order", "STATE", "queued"),
            ("FERRICSTORE.TELEMETRY", "FLOW_HISTORY", "f1", "INCLUDE", "metadata"),
        ]

    run(main())


def test_async_invocation_helpers_build_narrow_commands_and_request_context():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)
        executor.responses.extend(
            [
                {b"name": b"send-email"},
                {b"name": b"send-email"},
                [{b"name": b"send-email"}],
                {b"invocation_id": b"inv-1"},
                {b"id": b"inv-1"},
                [{b"scope": b"tenant:acme"}],
            ]
        )

        assert await client.invocation_definition_put(
            {"name": "send-email", "acl": {"scope_required": True}}
        ) == {"name": "send-email"}
        assert await client.invocation_definition_get("send-email") == {"name": "send-email"}
        assert await client.invocation_definition_list() == [{"name": "send-email"}]
        assert await client.invocation_create(
            "send-email",
            {"tenant": "acme"},
            context={"subject": "user-1"},
            idempotency_key="idem-1",
            request_context={
                "subject": "proxy",
                "tenant": "acme",
                "scopes": ["invocation:create:*"],
            },
        ) == {"invocation_id": "inv-1"}
        assert await client.invocation_get("inv-1") == {"id": "inv-1"}
        assert await client.invocation_partition_list("send-email", scope="tenant:acme") == [
            {"scope": "tenant:acme"}
        ]

        definition = json.loads(executor.calls[0][1])
        assert definition == {"acl": {"scope_required": True}, "name": "send-email"}

        create_call = executor.calls[3]
        assert create_call[:2] == ("INVOCATION.CREATE", "send-email")
        assert json.loads(create_call[2]) == {
            "attrs": {"tenant": "acme"},
            "context": {"subject": "user-1"},
            "idempotency_key": "idem-1",
        }
        assert create_call[3:] == (
            "REQUEST_CONTEXT",
            {
                "subject": "proxy",
                "tenant": "acme",
                "scopes": ["invocation:create:*"],
            },
        )
        assert executor.calls[5] == (
            "INVOCATION.PARTITION.LIST",
            "send-email",
            "SCOPE",
            "tenant:acme",
        )

    run(main())


def test_async_admin_flow_wrappers_build_readable_commands_and_normalize_responses():
    async def main():
        executor = FakeAsyncExecutor()
        client = AsyncFlowClient(executor)

        search_results = await client.search(
            "order",
            state="queued",
            count=10,
            attributes={"tenant": "acme"},
            state_meta={"version": 1},
            terminal_only=True,
            consistent_projection=True,
        )
        assert search_results[0].id == "f1"
        assert executor.calls[-1] == (
            "FLOW.SEARCH",
            "order",
            "COUNT",
            10,
            "STATE",
            "queued",
            "TERMINAL_ONLY",
            "true",
            "CONSISTENT_PROJECTION",
            "true",
            "ATTRIBUTE",
            "tenant",
            "acme",
            "STATE_META",
            "queued",
            {"version": 1},
        )

        assert await client.attributes("order", state="queued", count=10) == [
            {"name": "tenant", "count": 3}
        ]
        assert executor.calls[-1] == ("FLOW.ATTRIBUTES", "order", "STATE", "queued", "COUNT", 10)

        assert await client.attribute_values("order", "tenant", state="queued") == [
            {"value": "acme", "count": 2}
        ]
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == ("FLOW.SCHEDULE.FIRE_DUE", "BLOCK", 1000, "LIMIT", 50)
        schedules = await client.schedule_list(target_type="flow")
        assert isinstance(schedules[0], ScheduleResult)
        assert schedules[0]["status"] == "active"
        assert executor.calls[-1] == ("FLOW.SCHEDULE.LIST", "TARGET_TYPE", "flow")

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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
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
        assert executor.calls[-1] == (
            "FLOW.BUDGET.RELEASE",
            "tenant-a",
            "RESERVATION_ID",
            "budget-res-unused",
        )

        await client.limit_lease("tenant-a", shard_id=1, amount=5, ttl_ms=1000, limit=10)
        assert executor.calls[-1] == (
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
