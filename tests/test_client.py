import threading
import zlib
from concurrent.futures import Future

import pytest

from ferricstore import (
    BackpressurePolicy,
    FlowAlreadyExistsError,
    FlowClient,
    JsonCodec,
    OverloadedError,
    QueueFlowWorker,
    StaleLeaseError,
)
from ferricstore.backpressure import BackpressureController
from ferricstore.errors import FerricStoreError, classify_server_error, map_exception
from ferricstore.types import (
    ApprovalResult,
    ChildSpec,
    CircuitBreakerStatus,
    ClaimedFlow,
    CreateItem,
    EffectResult,
    FencedItem,
    FlowRecord,
    GovernanceOverview,
    KeyInfo,
    PubSubMessage,
    RateLimitResult,
    RetryPolicy,
    ScheduleResult,
)


class FakeExecutor:
    def __init__(self):
        self.calls = []
        self.responses = []

    def execute_command(self, *args):
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
            b"attributes": {b"tenant": b"acme"},
        }
        if "VALUE" in args:
            record[b"values"] = {b"order": b"order-bytes"}
            record[b"value_refs"] = {b"order": {b"ref": b"ref-order"}}
        if command == "FLOW.VALUE.MGET":
            return list(args[1:])
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
        if command in {
            "FLOW.CLAIM_DUE",
            "FLOW.RECLAIM",
            "FLOW.CREATE_MANY",
            "FLOW.COMPLETE_MANY",
            "FLOW.TRANSITION_MANY",
            "FLOW.RETRY_MANY",
            "FLOW.FAIL_MANY",
            "FLOW.CANCEL_MANY",
            "FLOW.LIST",
            "FLOW.TERMINALS",
            "FLOW.FAILURES",
            "FLOW.BY_PARENT",
            "FLOW.BY_ROOT",
            "FLOW.BY_CORRELATION",
            "FLOW.STUCK",
        }:
            return [
                {
                    **record,
                    b"id": b"f1",
                    b"type": b"order",
                    b"state": b"created",
                    b"partition_key": b"tenant:1",
                    b"lease_token": b"lease",
                    b"fencing_token": 7,
                    b"payload": b'{"ok":true}',
                }
            ]
        if command in {"FLOW.INFO", "FLOW.POLICY.GET", "FLOW.RETENTION_CLEANUP"}:
            return {b"ok": 1}
        if command == "FLOW.HISTORY":
            return [[b"event-1", {b"event": b"created"}]]
        if command == "FLOW.VALUE.PUT":
            return {b"ref": b"v1"}
        if command == "FLOW.STATS":
            return {b"count": 1, b"type": b"order"}
        return record

    def submit_command(self, *args):
        future = Future()
        try:
            future.set_result(self.execute_command(*args))
        except Exception as exc:
            future.set_exception(exc)
        return future


class RejectRichClaimReturnExecutor(FakeExecutor):
    def execute_command(self, *args):
        if (
            args[:1] == ("FLOW.CLAIM_DUE",)
            and "RETURN" in args
            and args[args.index("RETURN") + 1]
            in {"JOBS_COMPACT_ATTRS", "JOBS_COMPACT_STATE", "JOBS_COMPACT_STATE_ATTRS"}
        ):
            self.calls.append(args)
            raise FerricStoreError("flow claim return must be records, jobs, or jobs_compact")
        return super().execute_command(*args)


class FakeBatchExecutor(FakeExecutor):
    def __init__(self):
        super().__init__()
        self.batches = []

    def execute_batch(self, commands):
        self.batches.append(list(commands))
        return [
            [b"OK"],
            [[b"f2", b"tenant:1", b"lease-2", 8]],
        ]


class FakeSubmitCommandsExecutor(FakeExecutor):
    def __init__(self):
        super().__init__()
        self.submitted = []

    def submit_commands(self, commands):
        self.submitted.append(list(commands))
        complete = Future()
        claim = Future()
        complete.set_result([b"OK"])
        claim.set_result([[b"f2", b"tenant:1", b"lease-2", 8]])
        return [complete, claim]


class CloseExecutor(FakeExecutor):
    def __init__(self):
        super().__init__()
        self.closed = False

    def close(self):
        self.closed = True


class AckExecutor(FakeExecutor):
    def execute_command(self, *args):
        self.calls.append(args)
        return b"OK"


class OverloadThenAckExecutor(FakeExecutor):
    def __init__(self, overloads: int):
        super().__init__()
        self.overloads = overloads

    def execute_command(self, *args):
        self.calls.append(args)
        if self.overloads > 0:
            self.overloads -= 1
            raise OverloadedError("ERR overloaded")
        return b"OK"


class PerItemAckExecutor(FakeExecutor):
    def execute_command(self, *args):
        self.calls.append(args)
        command = args[0]
        if command == "FLOW.CREATE_MANY":
            if "ITEMS_EXT" in args:
                return [b"OK"] * int(args[args.index("ITEMS_EXT") + 1])
            width = 3 if args[1] == "MIXED" else 2
            count = (len(args) - args.index("ITEMS") - 1) // width
            return [b"OK"] * count
        if command in {"FLOW.COMPLETE_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY"}:
            width = 4 if args[1] == "MIXED" else 3
            count = (len(args) - args.index("ITEMS") - 1) // width
            return [b"OK"] * count
        if command == "FLOW.TRANSITION_MANY":
            width = 4 if args[1] == "MIXED" else 3
            count = (len(args) - args.index("ITEMS") - 1) // width
            return [b"OK"] * count
        if command == "FLOW.CANCEL_MANY":
            width = 3 if args[1] == "MIXED" else 2
            count = (len(args) - args.index("ITEMS") - 1) // width
            return [b"OK"] * count
        return b"OK"


class BlockingExecutor(PerItemAckExecutor):
    def __init__(self):
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "FLOW.CREATE_MANY":
            self.entered.set()
            self.release.wait()
            if "ITEMS_EXT" in args:
                return [b"OK"] * int(args[args.index("ITEMS_EXT") + 1])
            width = 3 if args[1] == "MIXED" else 2
            count = (len(args) - args.index("ITEMS") - 1) // width
            return [b"OK"] * count
        return b"OK"


class ClaimThenAckExecutor(FakeExecutor):
    def execute_command(self, *args):
        self.calls.append(args)
        command = args[0]
        if command == "FLOW.CLAIM_DUE":
            if "RETURN" in args:
                return_mode = args[args.index("RETURN") + 1]
                if str(return_mode).startswith("JOBS_COMPACT"):
                    if str(return_mode).endswith("_ATTRS"):
                        return [[b"f1", b"tenant:1", b"lease", 7, {b"tenant": b"acme"}]]
                    return [[b"f1", b"tenant:1", b"lease", 7]]

            return [
                {
                    b"id": b"f1",
                    b"type": b"order",
                    b"state": b"queued",
                    b"partition_key": b"tenant:1",
                    b"lease_token": b"lease",
                    b"fencing_token": 7,
                    **(
                        {
                            b"values": {b"order": b"order-bytes"},
                            b"value_refs": {b"order": {b"ref": b"ref-order"}},
                        }
                        if "VALUE" in args
                        else {}
                    ),
                }
            ]
        if command == "FLOW.COMPLETE_MANY":
            width = 4 if args[1] == "MIXED" else 3
            count = (len(args) - args.index("ITEMS") - 1) // width
            return [b"OK"] * count
        return b"OK"


class CreateAckThenGetExecutor(FakeExecutor):
    def execute_command(self, *args):
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
        return super().execute_command(*args)


def test_claimed_item_top_level_alias_remains_available():
    import ferricstore

    assert ferricstore.ClaimedItem is ferricstore.ClaimedFlow


def test_create_builds_flow_create_command():
    executor = FakeExecutor()
    client = FlowClient(executor)

    record = client.create(
        "f1",
        type="order",
        state="created",
        partition_key="tenant:1",
        payload=b"hello",
        now_ms=100,
        return_record=True,
    )

    assert record.id == "f1"
    assert executor.calls[0] == (
        "FLOW.CREATE",
        "f1",
        "TYPE",
        "order",
        "STATE",
        "created",
        "NOW",
        100,
        "PARTITION",
        "tenant:1",
        "PAYLOAD",
        b"hello",
        "RUN_AT",
        100,
    )


def test_create_can_return_ack_without_followup_get():
    executor = AckExecutor()
    client = FlowClient(executor)

    result = client.create(
        "f1",
        type="order",
        state="created",
        partition_key="tenant:1",
        payload=b"hello",
        now_ms=100,
    )

    assert result == b"OK"
    assert len(executor.calls) == 1


def test_create_honors_zero_now_ms():
    executor = AckExecutor()
    client = FlowClient(executor)

    client.create("f-zero", type="order", now_ms=0, return_record=False)

    call = executor.calls[0]
    assert call[call.index("NOW") + 1] == 0
    assert call[call.index("RUN_AT") + 1] == 0


def test_flow_client_close_forwards_to_executor():
    executor = CloseExecutor()
    client = FlowClient(executor)

    client.close()

    assert executor.closed is True


def test_create_ack_followup_get_uses_auto_partition_when_partition_omitted():
    executor = CreateAckThenGetExecutor()
    client = FlowClient(executor)
    expected_partition = f"__flow_auto__:{zlib.crc32(b'f-auto') % 256}"

    record = client.create("f-auto", type="order", payload=b"hello", now_ms=100, return_record=True)

    assert record.id == "f-auto"
    assert executor.calls[1][:2] == ("FLOW.GET", "f-auto")
    assert executor.calls[1][executor.calls[1].index("PARTITION") + 1] == expected_partition


def test_create_can_attach_named_values_and_refs():
    executor = AckExecutor()
    client = FlowClient(executor)

    result = client.create(
        "f1",
        type="order",
        partition_key="tenant:1",
        values={"order": b"order-bytes"},
        value_refs={"profile": "profile-ref"},
        now_ms=100,
        return_record=False,
    )

    assert result == b"OK"
    assert executor.calls[0] == (
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
        "RUN_AT",
        100,
        "VALUE",
        "order",
        b"order-bytes",
        "VALUE_REF",
        "profile",
        "profile-ref",
    )


def test_create_and_transition_can_attach_attributes():
    executor = AckExecutor()
    client = FlowClient(executor)

    assert client.create("f1", type="order", attributes={"tenant": "acme"}, now_ms=100) == b"OK"
    assert executor.calls[-1] == (
        "FLOW.CREATE",
        "f1",
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
    )

    assert (
        client.transition(
            "f1",
            from_state="queued",
            to_state="charged",
            lease_token=b"lease",
            fencing_token=7,
            attributes_merge={"phase": "charge"},
            attributes_delete=["tenant"],
            now_ms=101,
        )
        == b"OK"
    )
    assert executor.calls[-1] == (
        "FLOW.TRANSITION",
        "f1",
        "queued",
        "charged",
        "LEASE_TOKEN",
        b"lease",
        "FENCING",
        7,
        "NOW",
        101,
        "RUN_AT",
        101,
        "ATTRIBUTE_MERGE",
        "phase",
        "charge",
        "ATTRIBUTE_DELETE",
        "tenant",
    )


def test_flow_mutation_commands_can_attach_state_meta():
    executor = FakeExecutor()
    client = FlowClient(executor)
    claimed = [ClaimedFlow("f1", b"lease", 7, partition_key="tenant:1")]
    fenced = [FencedItem("f1", 7, lease_token=b"lease", partition_key="tenant:1")]

    def assert_state_meta(call, value):
        index = call.index("STATE_META")
        assert call[index : index + 3] == ("STATE_META", "version", value)

    client.create("f1", type="order", state_meta={"version": 1}, now_ms=100)
    assert_state_meta(executor.calls[-1], 1)

    client.start_and_claim(
        "f2",
        type="order",
        initial_state="accept",
        worker="worker-1",
        state_meta={"version": 2},
        now_ms=101,
    )
    assert_state_meta(executor.calls[-1], 2)

    client.transition(
        "f1",
        from_state="queued",
        to_state="charged",
        lease_token=b"lease",
        fencing_token=7,
        state_meta={"version": 3},
        now_ms=102,
    )
    assert_state_meta(executor.calls[-1], 3)

    client.step_continue(
        "f1",
        lease_token=b"lease",
        from_state="charged",
        to_state="settled",
        fencing_token=7,
        state_meta={"version": 4},
        now_ms=103,
    )
    assert_state_meta(executor.calls[-1], 4)

    client.complete("f1", lease_token=b"lease", fencing_token=7, state_meta={"version": 5})
    assert_state_meta(executor.calls[-1], 5)

    client.retry("f1", lease_token=b"lease", fencing_token=7, state_meta={"version": 6})
    assert_state_meta(executor.calls[-1], 6)

    client.fail("f1", lease_token=b"lease", fencing_token=7, state_meta={"version": 7})
    assert_state_meta(executor.calls[-1], 7)

    client.cancel("f1", fencing_token=7, lease_token=b"lease", state_meta={"version": 8})
    assert_state_meta(executor.calls[-1], 8)

    client.complete_many("tenant:1", claimed, state_meta={"version": 9})
    assert_state_meta(executor.calls[-1], 9)

    client.transition_many(
        "tenant:1",
        from_state="queued",
        to_state="charged",
        items=fenced,
        state_meta={"version": 10},
    )
    assert_state_meta(executor.calls[-1], 10)

    client.retry_many("tenant:1", claimed, state_meta={"version": 11})
    assert_state_meta(executor.calls[-1], 11)

    client.fail_many("tenant:1", claimed, state_meta={"version": 12})
    assert_state_meta(executor.calls[-1], 12)

    client.cancel_many("tenant:1", fenced, state_meta={"version": 13})
    assert_state_meta(executor.calls[-1], 13)


def test_data_command_helpers_are_codec_aware_and_easy_to_use():
    executor = FakeExecutor()
    client = FlowClient(executor, codec=JsonCodec())

    executor.responses.append("OK")
    assert client.kv_set("kv", {"answer": 42}, px=100, nx=True) == "OK"
    assert executor.calls[-1] == ("SET", "kv", b'{"answer":42}', "PX", 100, "NX")

    executor.responses.append(b'{"answer":42}')
    assert client.kv_get("kv") == {"answer": 42}
    assert executor.calls[-1] == ("GET", "kv")

    executor.responses.append([b'{"a":1}', None])
    assert client.kv_mget("a", "missing") == [{"a": 1}, None]
    assert executor.calls[-1] == ("MGET", "a", "missing")

    executor.responses.append("OK")
    assert client.kv_mset({"a": {"n": 1}, "b": {"n": 2}}) == "OK"
    assert executor.calls[-1] == ("MSET", "a", b'{"n":1}', "b", b'{"n":2}')


def test_data_command_helpers_cover_non_flow_command_families():
    executor = FakeExecutor()
    client = FlowClient(executor)

    executor.responses.extend(
        [
            1,
            1,
            10,
            2,
            b"stream-id",
            1,
            "OK",
            1,
            1,
            1,
            1,
            b"OK",
        ]
    )

    assert client.incr("counter") == 1
    assert executor.calls[-1] == ("INCR", "counter")
    assert client.expire("counter", 60, nx=True) == 1
    assert executor.calls[-1] == ("EXPIRE", "counter", 60, "NX")
    assert client.hset("hash", {"field": "value"}) == 10
    assert executor.calls[-1] == ("HSET", "hash", "field", b"value")
    assert client.sadd("set", "a", "b") == 2
    assert executor.calls[-1] == ("SADD", "set", b"a", b"b")
    assert client.xadd("stream", {"field": "value"}) == b"stream-id"
    assert executor.calls[-1] == ("XADD", "stream", "*", "field", b"value")
    assert client.xlen("stream") == 1
    assert executor.calls[-1] == ("XLEN", "stream")
    assert client.bf_reserve("bf", 0.01, 100) == "OK"
    assert executor.calls[-1] == ("BF.RESERVE", "bf", 0.01, 100)
    assert client.bf_add("bf", "member") == 1
    assert executor.calls[-1] == ("BF.ADD", "bf", "member")
    assert client.pfadd("hll", "a", "b") == 1
    assert executor.calls[-1] == ("PFADD", "hll", "a", "b")
    assert client.publish("channel", "message") == 1
    assert executor.calls[-1] == ("PUBLISH", "channel", b"message")
    assert client.dbsize() == 1
    assert executor.calls[-1] == ("DBSIZE",)


def test_data_command_helpers_cover_native_session_and_blocking_commands():
    executor = FakeExecutor()
    client = FlowClient(executor)

    executor.responses.extend(
        [
            [["subscribe", "jobs", 1]],
            [["unsubscribe", "jobs", 0]],
            "OK",
            "QUEUED",
            ["OK"],
            [b"queue", b"job"],
            [b"dst-job"],
            [b"queue", [b"a", b"b"]],
        ]
    )

    assert client.subscribe("jobs") == [["subscribe", "jobs", 1]]
    assert executor.calls[-1] == ("SUBSCRIBE", "jobs")
    assert client.unsubscribe("jobs") == [["unsubscribe", "jobs", 0]]
    assert executor.calls[-1] == ("UNSUBSCRIBE", "jobs")
    assert client.multi() == "OK"
    assert executor.calls[-1] == ("MULTI",)
    assert client.set("k", "v") == "QUEUED"
    assert executor.calls[-1] == ("COMMAND_EXEC", "SET", "k", b"v")
    assert client.transaction_exec() == ["OK"]
    assert executor.calls[-1] == ("EXEC",)
    assert client.blpop("queue", timeout=1) == [b"queue", b"job"]
    assert executor.calls[-1] == ("BLPOP", "queue", 1)
    assert client.blmove("queue", "dst", "LEFT", "RIGHT", timeout=2) == [b"dst-job"]
    assert executor.calls[-1] == ("BLMOVE", "queue", "dst", "LEFT", "RIGHT", 2)
    assert client.blmpop(3, ["queue"], "LEFT", count=2) == [b"queue", [b"a", b"b"]]
    assert executor.calls[-1] == ("BLMPOP", 3, 1, "queue", "LEFT", "COUNT", 2)


def test_pubsub_session_decodes_native_events_without_raw_command_usage():
    class EventExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.events = [
                {
                    b"kind": b"message",
                    b"channel": b"jobs",
                    b"message": b'{"job":1}',
                }
            ]

        def wait_event(self, timeout=None):
            return self.events.pop(0) if self.events else None

    client = FlowClient(EventExecutor(), codec=JsonCodec())
    pubsub = client.pubsub_session()

    message = pubsub.get_message(timeout=0.01)

    assert message is not None
    assert message.kind == "message"
    assert message.channel == "jobs"
    assert message.message == {"job": 1}


def test_pubsub_session_decodes_nested_native_pubsub_event():
    class EventExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.events = [
                {
                    b"event": b"PUBSUB_MESSAGE",
                    b"payload": {
                        b"kind": b"message",
                        b"channel": b"jobs",
                        b"message": b'{"job":1}',
                    },
                    b"at_ms": 123,
                }
            ]

        def wait_event(self, timeout=None):
            return self.events.pop(0) if self.events else None

    client = FlowClient(EventExecutor(), codec=JsonCodec())

    message = client.pubsub_session().get_message(timeout=0.01)

    assert message is not None
    assert message.kind == "message"
    assert message.channel == "jobs"
    assert message.message == {"job": 1}


def test_pubsub_session_decodes_push_array_event_shapes():
    message = PubSubMessage.from_event(
        [b"pmessage", b"jobs:*", b"jobs:1", b'{"job":1}'],
        decode=JsonCodec().decode,
    )

    assert message.kind == "pmessage"
    assert message.pattern == "jobs:*"
    assert message.channel == "jobs:1"
    assert message.message == {"job": 1}


def test_transaction_context_uses_named_helpers_inside_multi():
    executor = FakeExecutor()
    client = FlowClient(executor)
    executor.responses.extend(["OK", "QUEUED", ["OK"]])

    with client.transaction() as tx:
        assert tx.kv_set("k", "v") == "QUEUED"

    assert executor.calls == [
        ("MULTI",),
        ("COMMAND_EXEC", "SET", "k", b"v"),
        ("EXEC",),
    ]


def test_signal_builds_flow_signal_command_with_guards_and_values():
    executor = AckExecutor()
    client = FlowClient(executor)

    result = client.signal(
        "f1",
        signal="payment_received",
        partition_key="tenant:1",
        idempotency_key="stripe_evt_1",
        if_state=["manual_review", "waiting_payment"],
        transition_to="verify_payment",
        values={"payment_event": b"payment-bytes"},
        value_refs={"profile": "profile-ref"},
        drop_values=["old_event"],
        override_values=["payment_event"],
        run_at_ms=1250,
        now_ms=1100,
        priority=5,
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
        "manual_review",
        "IF_STATE",
        "waiting_payment",
        "TRANSITION_TO",
        "verify_payment",
        "RUN_AT",
        1250,
        "NOW",
        1100,
        "PRIORITY",
        5,
        "VALUE",
        "payment_event",
        b"payment-bytes",
        "VALUE_REF",
        "profile",
        "profile-ref",
        "DROP_VALUE",
        "old_event",
        "OVERRIDE_VALUE",
        "payment_event",
    )


def test_enqueue_uses_ack_only_create_by_default():
    executor = AckExecutor()
    client = FlowClient(executor)

    result = client.enqueue(
        "f1",
        type="order",
        payload=b"hello",
        partition_key="tenant:1",
        now_ms=100,
    )

    assert result == b"OK"
    assert executor.calls[0] == (
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
        "PRIORITY",
        0,
    )


def test_enqueue_passes_retention_ttl_ms():
    executor = AckExecutor()
    client = FlowClient(executor)

    result = client.enqueue(
        "f1",
        type="order",
        payload=b"hello",
        now_ms=100,
        retention_ttl_ms=300_000,
    )

    assert result == b"OK"
    assert "RETENTION_TTL_MS" in executor.calls[0]
    assert executor.calls[0][executor.calls[0].index("RETENTION_TTL_MS") + 1] == 300_000


def test_enqueue_retries_server_overload_with_backpressure():
    executor = OverloadThenAckExecutor(overloads=2)
    client = FlowClient(
        executor,
        backpressure=BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0),
    )

    result = client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

    assert result == b"OK"
    assert len(executor.calls) == 3


def test_enqueue_default_backpressure_retries_until_server_recovers():
    executor = OverloadThenAckExecutor(overloads=12)
    client = FlowClient(
        executor,
        backpressure=BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0),
    )

    result = client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

    assert result == b"OK"
    assert len(executor.calls) == 13


def test_backpressure_delay_saturates_without_overflow():
    controller = BackpressureController(
        BackpressurePolicy(base_delay_ms=5, max_delay_ms=500, jitter=0, shared=False)
    )

    assert controller._delay_for_attempt(10_000) == 0.5


def test_overload_error_parses_retry_after_hint():
    error = classify_server_error(
        "BUSY FerricStore overloaded: new Flow creates paused; "
        "retry_after_ms=2000 reason=rss_pressure"
    )

    assert isinstance(error, OverloadedError)
    assert error.retry_after_ms == 2000
    assert error.reason == "rss_pressure"


@pytest.mark.parametrize(
    ("message", "code"),
    [
        ("ERR flow already exists", "flow_already_exists"),
        ("ERR wrong state: queued", "flow_wrong_state"),
        ("ERR stale flow lease", "stale_lease"),
        ("ERR flow does not exist", "flow_not_found"),
        ("ERR lock is held", "lock_held"),
        ("ERR caller is not the lock owner", "lock_not_owned"),
        ("ERR wrong number of arguments", "invalid_command"),
        ("ERR unknown failure", "ferricstore_error"),
    ],
)
def test_server_error_classification_codes(message, code):
    raw = RuntimeError(message)

    error = classify_server_error(message, raw=raw)

    assert error.code == code
    assert error.raw is raw


def test_map_exception_preserves_known_errors_and_maps_server_errors():
    known = FerricStoreError("known")
    assert map_exception(known) is known

    class ResponseError(Exception):
        pass

    mapped = map_exception(ResponseError("ERR syntax error"))
    assert mapped.code == "invalid_command"

    wrong_type = map_exception(RuntimeError("WRONGTYPE key has wrong type"))
    assert wrong_type.code == "ferricstore_error"

    local = ValueError("local failure")
    assert map_exception(local) is local


def test_backpressure_uses_retry_after_hint_and_shares_block():
    policy = BackpressurePolicy(
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=True,
    )
    producer_a = BackpressureController(policy)
    producer_b = BackpressureController(policy)

    assert producer_a._record_overload_delay(0, retry_after_ms=1) == 0.001
    assert producer_b._wait_delay() > 0


def test_backpressure_disabled_never_waits_or_retries():
    controller = BackpressureController(
        BackpressurePolicy(enabled=False, base_delay_ms=1, max_delay_ms=1, shared=False)
    )

    assert controller._wait_delay() == 0
    assert controller.can_retry(0) is False
    assert controller._record_overload_delay(0, retry_after_ms=1000) == 0

    controller.record_overload(0, retry_after_ms=1000)
    controller.record_success()


def test_backpressure_success_decays_consecutive_overload_count():
    controller = BackpressureController(
        BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0, shared=False)
    )

    controller.record_overload(0)
    assert controller._state.consecutive_overloads == 1

    controller.record_success()
    assert controller._state.consecutive_overloads == 0

    controller.record_success()
    assert controller._state.consecutive_overloads == 0


def test_enqueue_stops_after_backpressure_retry_budget():
    executor = OverloadThenAckExecutor(overloads=2)
    client = FlowClient(
        executor,
        backpressure=BackpressurePolicy(max_retries=1, base_delay_ms=0, max_delay_ms=0, jitter=0),
    )

    with pytest.raises(OverloadedError):
        client.enqueue("f1", type="order", payload=b"hello", now_ms=100)

    assert len(executor.calls) == 2


def test_enqueue_many_groups_no_partition_items_by_auto_bucket():
    executor = PerItemAckExecutor()
    client = FlowClient(executor)
    items = [CreateItem(f"flow-{idx}", b"payload") for idx in range(64)]

    results = client.enqueue_many(items, type="order", now_ms=100)

    assert results == [b"OK"] * len(items)
    assert len(executor.calls) > 1

    for call in executor.calls:
        bucket = call[1]
        assert isinstance(bucket, str)
        assert bucket.startswith("__flow_auto__:")
        assert "RETENTION_TTL_MS" not in call
        item_args = call[call.index("ITEMS") + 1 :]
        ids = item_args[0::2]
        for id in ids:
            assert bucket == f"__flow_auto__:{zlib.crc32(id.encode()) % 256}"


def test_enqueue_many_groups_per_item_partition_keys_without_mixed_frame():
    executor = PerItemAckExecutor()
    client = FlowClient(executor)
    items = [
        CreateItem("flow-0", b"payload", partition_key="tenant:a"),
        CreateItem("flow-1", b"payload", partition_key="tenant:b"),
        CreateItem("flow-2", b"payload", partition_key="tenant:a"),
        CreateItem("flow-3", b"payload"),
    ]

    results = client.enqueue_many(items, type="order", now_ms=100)

    assert results == [b"OK"] * len(items)
    assert len(executor.calls) == 3
    partitions = [call[1] for call in executor.calls]
    assert "MIXED" not in partitions
    assert "tenant:a" in partitions
    assert "tenant:b" in partitions
    assert f"__flow_auto__:{zlib.crc32(b'flow-3') % 256}" in partitions

    by_partition = {call[1]: call for call in executor.calls}
    tenant_a_items = by_partition["tenant:a"][by_partition["tenant:a"].index("ITEMS") + 1 :]
    assert tenant_a_items[0::2] == ("flow-0", "flow-2")


def test_submit_create_many_uses_executor_submit_command():
    class SubmitExecutor:
        def __init__(self):
            self.calls = []

        def submit_command(self, *args):
            self.calls.append(args)
            future = Future()
            future.set_result(b"OK")
            return future

    executor = SubmitExecutor()
    client = FlowClient(executor)
    future = client.submit_create_many(
        "__flow_auto__:1",
        [CreateItem("flow-1", b"payload")],
        type="order",
        now_ms=100,
        return_ok_on_success=True,
        independent=True,
    )

    assert future.result() == b"OK"
    assert executor.calls == [
        (
            "FLOW.CREATE_MANY",
            "__flow_auto__:1",
            "TYPE",
            "order",
            "STATE",
            "queued",
            "NOW",
            100,
            "RUN_AT",
            100,
            "INDEPENDENT",
            "true",
            "RETURN",
            "OK_ON_SUCCESS",
            "ITEMS",
            "flow-1",
            b"payload",
        )
    ]


def test_enqueue_many_passes_retention_ttl_ms_to_auto_bucket_batches():
    executor = PerItemAckExecutor()
    client = FlowClient(executor)

    results = client.enqueue_many(
        [CreateItem(f"flow-{idx}", b"payload") for idx in range(16)],
        type="order",
        now_ms=100,
        retention_ttl_ms=300_000,
    )

    assert results == [b"OK"] * 16
    assert executor.calls
    for call in executor.calls:
        assert "RETENTION_TTL_MS" in call
        assert call[call.index("RETENTION_TTL_MS") + 1] == 300_000


def test_direct_many_methods_noop_on_empty_inputs():
    executor = PerItemAckExecutor()
    client = FlowClient(executor)

    assert client.create_many("tenant:1", [], type="order") == []
    assert client.complete_many("tenant:1", []) == []
    assert (
        client.transition_many(
            "tenant:1",
            from_state="running",
            to_state="next",
            items=[],
        )
        == []
    )
    assert client.retry_many("tenant:1", []) == []
    assert client.fail_many("tenant:1", []) == []
    assert client.cancel_many("tenant:1", []) == []
    assert executor.calls == []


def test_autobatch_groups_no_partition_creates_by_auto_bucket():
    executor = PerItemAckExecutor()
    client = FlowClient(executor).autobatch(max_batch=64, max_delay_ms=0)

    futures = [
        client.create_async(
            f"flow-{idx}",
            type="order",
            payload=b"payload",
            return_record=False,
            now_ms=100,
        )
        for idx in range(64)
    ]
    client.flush()

    assert [future.result() for future in futures] == [b"OK"] * len(futures)
    for call in executor.calls:
        if call[0] != "FLOW.CREATE_MANY":
            continue
        bucket = call[1]
        assert isinstance(bucket, str)
        assert bucket.startswith("__flow_auto__:")
        item_args = call[call.index("ITEMS") + 1 :]
        ids = item_args[0::2]
        for id in ids:
            assert bucket == f"__flow_auto__:{zlib.crc32(id.encode()) % 256}"
    client.close()


def test_claim_due_decodes_resp3_maps_and_payload():
    executor = FakeExecutor()
    client = FlowClient(executor, codec=JsonCodec())

    records = client.claim_due(
        "order",
        state="created",
        worker="w1",
        partition_key="tenant:1",
        now_ms=100,
    )

    assert records[0].id == "f1"
    assert records[0].lease_token == b"lease"
    assert records[0].fencing_token == 7
    assert records[0].payload == {"ok": True}


def test_claim_due_can_target_priority():
    executor = FakeExecutor()
    client = FlowClient(executor)

    jobs = client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        priority=0,
        now_ms=100,
        reclaim_expired=False,
    )

    assert jobs[0].id == "f1"
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
        10,
        "NOW",
        100,
        "PARTITION",
        "tenant:1",
        "PRIORITY",
        0,
        "RECLAIM_EXPIRED",
        "false",
    )


def test_claim_due_omits_now_when_not_supplied():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
    )

    assert "NOW" not in executor.calls[0]


def test_claim_due_sends_block_when_supplied():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_flows(
        "order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        block_ms=5000,
    )

    assert "BLOCK" in executor.calls[0]
    assert executor.calls[0][executor.calls[0].index("BLOCK") + 1] == 5000


def test_claim_due_omits_state_when_none():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_due(
        "order",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        priority=0,
        now_ms=100,
    )

    assert executor.calls[0] == (
        "FLOW.CLAIM_DUE",
        "order",
        "WORKER",
        "worker-1",
        "LEASE_MS",
        30000,
        "LIMIT",
        10,
        "NOW",
        100,
        "PARTITION",
        "tenant:1",
        "PRIORITY",
        0,
    )


def test_claim_due_can_target_multiple_states():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_due(
        "order",
        states=["queued", "retry"],
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        priority=0,
        now_ms=100,
    )

    assert executor.calls[0] == (
        "FLOW.CLAIM_DUE",
        "order",
        "STATE",
        "queued",
        "STATE",
        "retry",
        "WORKER",
        "worker-1",
        "LEASE_MS",
        30000,
        "LIMIT",
        10,
        "NOW",
        100,
        "PARTITION",
        "tenant:1",
        "PRIORITY",
        0,
    )


def test_claim_due_can_request_selected_named_values():
    executor = FakeExecutor()
    client = FlowClient(executor)

    records = client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        values=["order"],
        value_max_bytes=1024,
        now_ms=100,
    )

    assert records[0].values == {"order": b"order-bytes"}
    assert records[0].value_refs == {"order": {"ref": "ref-order"}}
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
        1,
        "NOW",
        100,
        "PARTITION",
        "tenant:1",
        "VALUE",
        "order",
        "VALUE_MAX_BYTES",
        1024,
    )


def test_complete_many_can_attach_named_values_and_refs():
    executor = PerItemAckExecutor()
    client = FlowClient(executor)

    result = client.complete_many(
        "tenant:1",
        [ClaimedFlow(id="f1", lease_token=b"lease", fencing_token=7)],
        values={"receipt": b"receipt-bytes"},
        value_refs={"profile": "profile-ref"},
        drop_values=["old"],
        override_values=["receipt"],
        now_ms=100,
        independent=True,
    )

    call = executor.calls[0]
    assert result == [b"OK"]
    assert call[0] == "FLOW.COMPLETE_MANY"
    assert call[call.index("VALUE") : call.index("VALUE") + 3] == (
        "VALUE",
        "receipt",
        b"receipt-bytes",
    )
    assert call[call.index("VALUE_REF") : call.index("VALUE_REF") + 3] == (
        "VALUE_REF",
        "profile",
        "profile-ref",
    )
    assert call[call.index("DROP_VALUE") : call.index("DROP_VALUE") + 2] == (
        "DROP_VALUE",
        "old",
    )
    assert call[call.index("OVERRIDE_VALUE") : call.index("OVERRIDE_VALUE") + 2] == (
        "OVERRIDE_VALUE",
        "receipt",
    )
    assert call.index("VALUE") < call.index("ITEMS")


def test_complete_many_can_return_ok_on_success():
    executor = PerItemAckExecutor()
    client = FlowClient(executor)

    result = client.complete_many(
        "tenant:1",
        [ClaimedFlow(id="f1", lease_token=b"lease", fencing_token=7)],
        now_ms=100,
        independent=True,
        return_ok_on_success=True,
    )

    assert result == [b"OK"]
    assert "RETURN" in executor.calls[0]
    assert "OK_ON_SUCCESS" in executor.calls[0]


def test_claim_due_rejects_state_and_states_together():
    executor = FakeExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="state and states are mutually exclusive"):
        client.claim_due("order", state="queued", states=["retry"], worker="worker-1")


def test_value_put_named_options_and_value_mget_decode_values():
    executor = FakeExecutor()
    client = FlowClient(executor)

    response = client.value_put(
        b"order-v1",
        partition_key="tenant:1",
        owner_flow_id="f1",
        name="order",
        override=True,
        now_ms=100,
    )
    values = client.value_mget(["ref-a", "ref-b"])

    assert response == {b"ref": b"v1"}
    assert values == ["ref-a", "ref-b"]
    assert executor.calls[0] == (
        "FLOW.VALUE.PUT",
        b"order-v1",
        "NOW",
        100,
        "PARTITION",
        "tenant:1",
        "OWNER_FLOW_ID",
        "f1",
        "NAME",
        "order",
        "OVERRIDE",
        "true",
    )
    assert executor.calls[1] == ("FLOW.VALUE.MGET", "ref-a", "ref-b")


def test_value_mget_normalizes_omission_metadata_recursively():
    executor = FakeExecutor()
    client = FlowClient(executor)
    executor.responses = [
        [{b"ref": b"ref-a", b"omitted": True, b"size": 123, b"nested": {b"k": b"v"}}]
    ]

    values = client.value_mget(["ref-a"], max_bytes=10)

    assert values == [{"ref": "ref-a", "omitted": True, "size": 123, "nested": {"k": "v"}}]
    assert executor.calls[-1] == ("FLOW.VALUE.MGET", "ref-a", "MAX_BYTES", 10)


def test_get_can_request_selected_named_values():
    executor = FakeExecutor()
    client = FlowClient(executor)

    record = client.get("f1", partition_key="tenant:1", values=["order"], value_max_bytes=1024)

    assert record is not None
    assert record.values == {"order": b"order-bytes"}
    assert executor.calls[0] == (
        "FLOW.GET",
        "f1",
        "PARTITION",
        "tenant:1",
        "VALUE",
        "order",
        "VALUE_MAX_BYTES",
        1024,
    )


def test_transition_and_terminal_commands_can_mutate_named_values():
    executor = AckExecutor()
    client = FlowClient(executor)

    transition_result = client.transition(
        "f1",
        from_state="running",
        to_state="waiting",
        lease_token=b"lease",
        fencing_token=7,
        partition_key="tenant:1",
        values={"payment": b"payment-v1"},
        drop_values=["order"],
        override_values=["payment"],
        now_ms=100,
        return_record=False,
    )
    complete_result = client.complete(
        "f1",
        lease_token=b"lease-2",
        fencing_token=8,
        partition_key="tenant:1",
        values={"receipt": b"receipt-v1"},
        now_ms=200,
        return_record=False,
    )

    assert transition_result == b"OK"
    assert complete_result == b"OK"
    assert executor.calls[0] == (
        "FLOW.TRANSITION",
        "f1",
        "running",
        "waiting",
        "LEASE_TOKEN",
        b"lease",
        "FENCING",
        7,
        "NOW",
        100,
        "PARTITION",
        "tenant:1",
        "RUN_AT",
        100,
        "VALUE",
        "payment",
        b"payment-v1",
        "DROP_VALUE",
        "order",
        "OVERRIDE_VALUE",
        "payment",
    )
    assert executor.calls[1] == (
        "FLOW.COMPLETE",
        "f1",
        b"lease-2",
        "FENCING",
        8,
        "NOW",
        200,
        "PARTITION",
        "tenant:1",
        "VALUE",
        "receipt",
        b"receipt-v1",
    )


def test_run_steps_many_sends_fused_deterministic_step_command():
    executor = AckExecutor()
    client = FlowClient(executor)

    result = client.run_steps_many(
        [
            "f1",
            {"id": "f2", "partition_key": "tenant:2"},
            CreateItem("f3", partition_key="tenant:3"),
        ],
        type="order",
        states=["reserve", "charge", "email"],
        worker="worker-1",
        lease_ms=45_000,
        now_ms=123,
        payload=b"payload",
        result=b"ok",
        partition_key="tenant:1",
        retention_ttl_ms=60_000,
    )

    assert result == b"OK"
    assert executor.calls[0] == (
        "FLOW.RUN_STEPS_MANY",
        "TYPE",
        "order",
        "STATES",
        ["reserve", "charge", "email"],
        "WORKER",
        "worker-1",
        "LEASE_MS",
        45_000,
        "NOW",
        123,
        "PAYLOAD",
        b"payload",
        "RESULT",
        b"ok",
        "RETENTION_TTL_MS",
        60_000,
        "ITEMS",
        [
            {"id": "f1", "partition_key": "tenant:1"},
            {"id": "f2", "partition_key": "tenant:2"},
            {"id": "f3", "partition_key": "tenant:3"},
        ],
    )


def test_run_steps_many_can_use_step_count_and_rejects_ambiguous_state_shape():
    executor = AckExecutor()
    client = FlowClient(executor)

    assert client.run_steps_many(["f1"], type="order", steps=3, worker="worker-1", now_ms=123)
    assert executor.calls[0] == (
        "FLOW.RUN_STEPS_MANY",
        "TYPE",
        "order",
        "STEPS",
        3,
        "WORKER",
        "worker-1",
        "LEASE_MS",
        30_000,
        "NOW",
        123,
        "ITEMS",
        [{"id": "f1"}],
    )

    with pytest.raises(ValueError, match="exactly one"):
        client.run_steps_many(["f1"], type="order", worker="worker-1")

    with pytest.raises(ValueError, match="exactly one"):
        client.run_steps_many(["f1"], type="order", states=["reserve"], steps=1, worker="worker-1")


def test_claim_due_can_return_claimed_flow_items():
    executor = FakeExecutor()
    client = FlowClient(executor)

    jobs = client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        limit=10,
        priority=0,
        now_ms=100,
        include_record=False,
    )

    assert isinstance(jobs[0], ClaimedFlow)
    assert jobs[0].id == "f1"
    assert jobs[0].lease_token == b"lease"
    assert jobs[0].fencing_token == 7
    assert jobs[0].attributes == {"tenant": "acme"}
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
        10,
        "NOW",
        100,
        "PRIORITY",
        0,
        "RETURN",
        "JOBS_COMPACT_ATTRS",
    )


def test_claim_due_accepts_legacy_job_only_alias():
    executor = FakeExecutor()
    client = FlowClient(executor)

    jobs = client.claim_due(
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


def test_claim_due_falls_back_when_server_rejects_attribute_return_mode():
    executor = RejectRichClaimReturnExecutor()
    client = FlowClient(executor)

    jobs = client.claim_flows("order", state="queued", worker="worker-1")

    assert isinstance(jobs[0], ClaimedFlow)
    assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == "JOBS_COMPACT_ATTRS"
    assert executor.calls[1][executor.calls[1].index("RETURN") + 1] == "JOBS_COMPACT"


def test_claim_due_falls_back_when_server_rejects_state_attribute_return_mode():
    executor = RejectRichClaimReturnExecutor()
    client = FlowClient(executor)

    jobs = client.claim_flows(
        "order",
        state="queued",
        worker="worker-1",
        include_state=True,
    )

    assert isinstance(jobs[0], ClaimedFlow)
    assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == "JOBS_COMPACT_STATE_ATTRS"
    assert executor.calls[1][executor.calls[1].index("RETURN") + 1] == "JOBS_COMPACT"


def test_claim_due_rejects_conflicting_include_record_and_job_only():
    executor = FakeExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="include_record and job_only"):
        client.claim_due(
            "order",
            state="queued",
            worker="worker-1",
            include_record=False,
            job_only=False,
        )

    assert executor.calls == []


def test_claim_due_claimed_flow_can_omit_attributes():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        limit=10,
        priority=0,
        now_ms=100,
        include_record=False,
        include_attributes=False,
    )

    assert executor.calls[0][-2:] == ("RETURN", "JOBS_COMPACT")


def test_claim_flows_can_request_compact_state_items():
    executor = FakeExecutor()
    executor.responses.append([[b"f1", b"tenant:1", b"lease", 7, b"ready", {b"tenant": b"acme"}]])
    client = FlowClient(executor)

    jobs = client.claim_flows(
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


def test_claim_flows_future_uses_protocol_submit_and_decodes_items():
    executor = FakeExecutor()
    executor.responses.append([[b"f1", b"tenant:1", b"lease", 7, {b"tenant": b"acme"}]])
    client = FlowClient(executor)

    future = client.claim_flows_future(
        "order",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        block_ms=5000,
    )

    assert future.result(timeout=1.0) == [
        ClaimedFlow(
            id="f1",
            partition_key="tenant:1",
            lease_token=b"lease",
            fencing_token=7,
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
        "JOBS_COMPACT_ATTRS",
        "BLOCK",
        5000,
    )


def test_claim_due_can_scan_multiple_partitions():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        partition_keys=["p1", "p2"],
        limit=10,
        now_ms=100,
        include_record=False,
    )

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
        10,
        "NOW",
        100,
        "PARTITIONS",
        2,
        "p1",
        "p2",
        "RETURN",
        "JOBS_COMPACT_ATTRS",
    )


def test_claim_flows_can_scan_multiple_partitions():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_flows(
        "order",
        state="queued",
        worker="worker-1",
        partition_keys=["p1", "p2"],
        limit=10,
        now_ms=100,
    )

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
        10,
        "NOW",
        100,
        "PARTITIONS",
        2,
        "p1",
        "p2",
        "PRIORITY",
        0,
        "RETURN",
        "JOBS_COMPACT_ATTRS",
    )


def test_claim_flows_only_sends_reclaim_expired_when_explicit():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.claim_flows(
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


def test_claim_flows_and_complete_jobs_hide_hot_path_options():
    executor = ClaimThenAckExecutor()
    client = FlowClient(executor)

    jobs = client.claim_flows(
        "order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        now_ms=100,
    )
    result = client.complete_jobs(jobs, now_ms=200, return_ok_on_success=True)

    assert isinstance(jobs[0], ClaimedFlow)
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
        10,
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
        "NOW",
        200,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "f1",
        b"lease",
        7,
    )


def test_complete_flows_and_claim_flows_batches_ack_then_next_claim():
    executor = FakeBatchExecutor()
    client = FlowClient(executor)

    next_jobs = client.complete_flows_and_claim_flows(
        [ClaimedFlow("f1", b"lease-1", 7, partition_key="tenant:1")],
        result=b"ok",
        now_ms=200,
        type="order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        lease_ms=10_000,
        limit=50,
        priority=0,
        block_ms=5,
    )

    assert next_jobs == [
        ClaimedFlow(
            id="f2",
            partition_key="tenant:1",
            lease_token=b"lease-2",
            fencing_token=8,
        )
    ]
    assert executor.calls == []
    assert executor.batches == [
        [
            (
                "FLOW.COMPLETE_MANY",
                "tenant:1",
                "RESULT",
                b"ok",
                "NOW",
                200,
                "INDEPENDENT",
                "true",
                "TERMINAL_LOCAL_ONLY",
                "true",
                "ITEMS",
                "f1",
                b"lease-1",
                7,
            ),
            (
                "FLOW.CLAIM_DUE",
                "order",
                "STATE",
                "queued",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                10000,
                "LIMIT",
                50,
                "PARTITION",
                "tenant:1",
                "PRIORITY",
                0,
                "RETURN",
                "JOBS_COMPACT_ATTRS",
                "BLOCK",
                5,
            ),
        ]
    ]


def test_submit_complete_flows_and_claim_flows_returns_independent_futures():
    executor = FakeSubmitCommandsExecutor()
    client = FlowClient(executor)

    submitted = client.submit_complete_flows_and_claim_flows(
        [ClaimedFlow("f1", b"lease-1", 7, partition_key="tenant:1")],
        result=b"ok",
        now_ms=200,
        type="order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        lease_ms=10_000,
        limit=50,
        priority=0,
    )

    assert submitted is not None
    complete_future, claim_future = submitted
    assert complete_future.result(timeout=1.0) == 1
    assert claim_future.result(timeout=1.0) == [
        ClaimedFlow(
            id="f2",
            partition_key="tenant:1",
            lease_token=b"lease-2",
            fencing_token=8,
        )
    ]
    assert executor.submitted[0][0][0] == "FLOW.COMPLETE_MANY"
    assert executor.submitted[0][1][0] == "FLOW.CLAIM_DUE"


def test_reclaim_exposes_claim_due_response_options_and_partitions():
    executor = FakeExecutor()
    client = FlowClient(executor)

    result = client.reclaim(
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


def test_reclaim_rejects_non_running_state_alias():
    executor = FakeExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match=r"FLOW\.RECLAIM only supports running"):
        client.reclaim("order", state="queued", worker="worker-1")

    assert executor.calls == []


def test_flow_worker_runs_hot_path_with_minimal_developer_code():
    executor = ClaimThenAckExecutor()
    client = FlowClient(executor)
    seen = []
    worker = QueueFlowWorker(
        client,
        type="order",
        state="queued",
        worker="worker-1",
        batch_size=10,
    )

    result = worker.run_once(lambda job: seen.append(job.id))
    worker.close()

    assert seen == ["f1"]
    assert result.claimed == 1
    assert result.completed == 1
    assert executor.calls[0][:3] == ("FLOW.CLAIM_DUE", "order", "STATE")
    assert "RETURN" in executor.calls[0]
    assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == "JOBS_COMPACT_ATTRS"
    assert executor.calls[1][0] == "FLOW.COMPLETE_MANY"


def test_flow_worker_omitted_state_means_any_state():
    executor = ClaimThenAckExecutor()
    client = FlowClient(executor)
    worker = QueueFlowWorker(client, type="order", worker="worker-1")

    worker.run_once(lambda _job: None)
    worker.close()

    assert "STATE" not in executor.calls[0]
    assert executor.calls[0][0] == "FLOW.CLAIM_DUE"


def test_flow_worker_supports_multi_state_claims():
    executor = ClaimThenAckExecutor()
    client = FlowClient(executor)
    worker = QueueFlowWorker(
        client,
        type="order",
        states=["queued", "retry"],
        worker="worker-1",
    )

    worker.run_once(lambda _job: None)
    worker.close()

    assert executor.calls[0][:6] == (
        "FLOW.CLAIM_DUE",
        "order",
        "STATE",
        "queued",
        "STATE",
        "retry",
    )


def test_flow_worker_can_claim_named_values_without_compact_return():
    executor = ClaimThenAckExecutor()
    client = FlowClient(executor)
    seen = []
    worker = QueueFlowWorker(
        client,
        type="order",
        state="queued",
        worker="worker-1",
        claim_values=["order"],
        value_max_bytes=1024,
    )

    result = worker.run_once(lambda job: seen.append(job.values["order"]))
    worker.close()

    assert seen == [b"order-bytes"]
    assert result.claimed == 1
    assert result.completed == 1
    claim = executor.calls[0]
    assert "RETURN" not in claim
    assert claim[claim.index("VALUE") : claim.index("VALUE") + 2] == ("VALUE", "order")
    assert claim[claim.index("VALUE_MAX_BYTES") : claim.index("VALUE_MAX_BYTES") + 2] == (
        "VALUE_MAX_BYTES",
        1024,
    )


def test_json_codec_omits_none_optional_values_on_singular_writes():
    executor = AckExecutor()
    client = FlowClient(executor, codec=JsonCodec())

    client.create("create-none", type="order", now_ms=100, return_record=False)
    client.transition(
        "transition-none",
        from_state="queued",
        to_state="next",
        lease_token=b"lease",
        fencing_token=1,
        now_ms=101,
        return_record=False,
    )
    client.complete(
        "complete-none",
        lease_token=b"lease",
        fencing_token=2,
        now_ms=102,
        return_record=False,
    )
    client.retry(
        "retry-none",
        lease_token=b"lease",
        fencing_token=3,
        now_ms=103,
        return_record=False,
    )
    client.fail(
        "fail-none",
        lease_token=b"lease",
        fencing_token=4,
        now_ms=104,
        return_record=False,
    )

    for call in executor.calls:
        assert "PAYLOAD" not in call
        assert "RESULT" not in call
        assert "ERROR" not in call


def test_spawn_children_mixed_uses_one_mixed_marker():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.spawn_children(
        "parent-1",
        [
            ChildSpec("c1", "email", b"p1", partition_key="p1"),
            ChildSpec("c2", "audit", b"p2", partition_key="p2"),
        ],
        partition_key="parent-p",
        group_id="g1",
    )

    assert executor.calls[0].count("MIXED") == 1
    items_idx = executor.calls[0].index("ITEMS")
    assert executor.calls[0][items_idx:] == (
        "ITEMS",
        "MIXED",
        "c1",
        "p1",
        "email",
        b"p1",
        "c2",
        "p2",
        "audit",
        b"p2",
    )


def test_create_many_mixed_builds_items():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        None,
        [
            CreateItem("f1", b"p1", partition_key="p1"),
            CreateItem("f2", b"p2", partition_key="p2"),
        ],
        type="order",
        state="queued",
        now_ms=100,
    )

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
        "ITEMS",
        "f1",
        "p1",
        b"p1",
        "f2",
        "p2",
        b"p2",
    )


def test_create_many_mixed_allows_auto_partition_items():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        None,
        [
            CreateItem("f1", b"p1"),
            CreateItem("f2", b"p2", partition_key="tenant:2"),
        ],
        type="order",
        state="queued",
        now_ms=100,
    )

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


def test_create_many_without_partition_uses_auto_wire_shape():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        None,
        [
            CreateItem("f1", b"p1"),
            CreateItem("f2", b"p2"),
        ],
        type="order",
        state="queued",
        now_ms=100,
        independent=True,
    )

    assert executor.calls[0] == (
        "FLOW.CREATE_MANY",
        "AUTO",
        "TYPE",
        "order",
        "STATE",
        "queued",
        "NOW",
        100,
        "RUN_AT",
        100,
        "INDEPENDENT",
        "true",
        "ITEMS",
        "f1",
        b"p1",
        "f2",
        b"p2",
    )


def test_create_many_can_attach_shared_attributes():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        "tenant:1",
        [CreateItem("f1", b"p1"), CreateItem("f2", b"p2")],
        type="order",
        state="queued",
        now_ms=100,
        attributes={"tenant": "acme", "campaign": "spring"},
    )

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
        "ATTRIBUTE",
        "campaign",
        "spring",
        "ITEMS",
        "f1",
        b"p1",
        "f2",
        b"p2",
    )


def test_create_many_can_attach_shared_state_meta():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        "tenant:1",
        [CreateItem("f1", b"p1"), CreateItem("f2", b"p2")],
        type="order",
        state="queued",
        now_ms=100,
        state_meta={"version": 1, "owner": "risk"},
    )

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
        "STATE_META",
        "version",
        1,
        "STATE_META",
        "owner",
        "risk",
        "ITEMS",
        "f1",
        b"p1",
        "f2",
        b"p2",
    )


def test_create_many_reuses_identical_item_state_meta_as_shared_state_meta():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        "tenant:1",
        [
            CreateItem("f1", b"p1", state_meta={"version": 1}),
            CreateItem("f2", b"p2", state_meta={"version": 1}),
        ],
        type="order",
        now_ms=100,
    )

    call = executor.calls[0]
    assert call[call.index("STATE_META") : call.index("STATE_META") + 3] == (
        "STATE_META",
        "version",
        1,
    )


def test_create_many_rejects_mixed_item_state_meta_instead_of_dropping_it():
    executor = FakeExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="shared state_meta"):
        client.create_many(
            "tenant:1",
            [
                CreateItem("f1", b"p1", state_meta={"version": 1}),
                CreateItem("f2", b"p2", state_meta={"version": 2}),
            ],
            type="order",
            now_ms=100,
        )

    assert executor.calls == []


def test_create_many_reuses_identical_item_attributes_as_shared_attributes():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        "tenant:1",
        [
            CreateItem("f1", b"p1", attributes={"tenant": "acme"}),
            CreateItem("f2", b"p2", attributes={"tenant": "acme"}),
        ],
        type="order",
        now_ms=100,
    )

    call = executor.calls[0]
    assert call[call.index("ATTRIBUTE") : call.index("ATTRIBUTE") + 3] == (
        "ATTRIBUTE",
        "tenant",
        "acme",
    )


def test_create_many_rejects_mixed_item_attributes_instead_of_dropping_them():
    executor = FakeExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="shared attributes"):
        client.create_many(
            "tenant:1",
            [
                CreateItem("f1", b"p1", attributes={"tenant": "acme"}),
                CreateItem("f2", b"p2", attributes={"tenant": "other"}),
            ],
            type="order",
            now_ms=100,
        )

    assert executor.calls == []


def test_create_many_uses_extended_items_for_per_item_named_values():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.create_many(
        None,
        [
            CreateItem("f1", b"p1", partition_key="p1", values={"order": b"o1"}),
            CreateItem("f2", b"p2", partition_key="p2", value_refs={"profile": "profile-ref"}),
        ],
        type="order",
        state="queued",
        now_ms=100,
        values={"shared": b"shared-bytes"},
    )

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
        "p1",
        b"p1",
        2,
        "shared",
        b"shared-bytes",
        "order",
        b"o1",
        0,
        "f2",
        "p2",
        b"p2",
        1,
        "shared",
        b"shared-bytes",
        1,
        "profile",
        "profile-ref",
    )


def test_spawn_children_uses_extended_items_for_per_child_named_values():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.spawn_children(
        "parent-1",
        [
            ChildSpec("c1", "email", b"p1", partition_key="p1", values={"order": b"o1"}),
            ChildSpec(
                "c2", "audit", b"p2", partition_key="p2", value_refs={"profile": "profile-ref"}
            ),
        ],
        partition_key="parent-p",
        group_id="g1",
        values={"shared": b"shared-bytes"},
        now_ms=100,
    )

    assert executor.calls[0] == (
        "FLOW.SPAWN_CHILDREN",
        "parent-1",
        "GROUP",
        "g1",
        "WAIT",
        "all",
        "NOW",
        100,
        "PARTITION",
        "parent-p",
        "ITEMS_EXT",
        2,
        "c1",
        "p1",
        "email",
        b"p1",
        2,
        "shared",
        b"shared-bytes",
        "order",
        b"o1",
        0,
        "c2",
        "p2",
        "audit",
        b"p2",
        1,
        "shared",
        b"shared-bytes",
        1,
        "profile",
        "profile-ref",
    )


def test_spawn_children_exposes_parent_guards_and_child_policies():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.spawn_children(
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


def test_create_many_allows_ok_response():
    executor = AckExecutor()
    client = FlowClient(executor)

    result = client.create_many(
        None,
        [CreateItem("f1", b"p1", partition_key="p1")],
        type="order",
        state="queued",
        now_ms=100,
    )

    assert result == b"OK"


def test_many_commands_reject_items_from_different_explicit_partition():
    executor = FakeExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="partition_key"):
        client.create_many("p1", [CreateItem("f1", b"p", partition_key="p2")], type="order")

    with pytest.raises(ValueError, match="partition_key"):
        client.complete_many("p1", [ClaimedFlow("f1", b"lease", 3, partition_key="p2")])

    with pytest.raises(ValueError, match="partition_key"):
        client.cancel_many("p1", [FencedItem("f1", 3, partition_key="p2")])

    assert executor.calls == []


def test_many_commands_support_independent_option():
    executor = FakeExecutor()
    client = FlowClient(executor)
    claimed = ClaimedFlow("f1", b"lease", 3, partition_key="p1")
    fenced = FencedItem("f1", 4, b"lease", partition_key="p1")

    client.create_many(
        None,
        [CreateItem("f1", b"p1", partition_key="p1")],
        type="order",
        now_ms=100,
        independent=True,
    )
    assert "INDEPENDENT" in executor.calls[-1]
    assert executor.calls[-1][executor.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.complete_many(None, [claimed], result=b"ok", now_ms=101, independent=True)
    assert "INDEPENDENT" in executor.calls[-1]
    assert executor.calls[-1][executor.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.transition_many(
        None,
        from_state="queued",
        to_state="ready",
        items=[fenced],
        now_ms=102,
        independent=True,
    )
    assert "INDEPENDENT" in executor.calls[-1]
    assert executor.calls[-1][executor.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.retry_many(None, [claimed], error=b"err", now_ms=103, independent=True)
    assert "INDEPENDENT" in executor.calls[-1]
    assert executor.calls[-1][executor.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.fail_many(None, [claimed], error=b"err", now_ms=104, independent=True)
    assert "INDEPENDENT" in executor.calls[-1]
    assert executor.calls[-1][executor.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.cancel_many(None, [fenced], reason=b"stop", now_ms=105, independent=True)
    assert "INDEPENDENT" in executor.calls[-1]
    assert executor.calls[-1][executor.calls[-1].index("INDEPENDENT") + 1] == "true"


def test_autobatch_create_uses_create_many_independent_for_ack_calls():
    executor = PerItemAckExecutor()
    client = FlowClient(executor).autobatch(max_batch=10, max_delay_ms=5)
    results = [None, None, None]

    threads = [
        threading.Thread(
            target=lambda index=index: results.__setitem__(
                index,
                client.create(
                    f"f{index}",
                    type="order",
                    partition_key=f"p{index}",
                    return_record=False,
                ),
            )
        )
        for index in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    client.close()

    assert results == [b"OK", b"OK", b"OK"]
    assert executor.calls[0][0] == "FLOW.CREATE_MANY"
    assert executor.calls[0][1] == "MIXED"
    assert "INDEPENDENT" in executor.calls[0]


def test_autobatch_state_meta_uses_direct_mutation_commands():
    executor = FakeExecutor()
    client = FlowClient(executor).autobatch(max_batch=10, max_delay_ms=5)

    client.create(
        "f1",
        type="order",
        partition_key="tenant:1",
        state_meta={"version": "1"},
    )
    client.complete(
        "f1",
        lease_token=b"lease",
        fencing_token=7,
        partition_key="tenant:1",
        state_meta={"version": "2"},
    )
    client.transition(
        "f1",
        from_state="queued",
        to_state="charged",
        lease_token=b"lease",
        fencing_token=7,
        partition_key="tenant:1",
        state_meta={"version": "3"},
    )
    client.retry(
        "f1",
        lease_token=b"lease",
        fencing_token=7,
        partition_key="tenant:1",
        state_meta={"version": "4"},
    )
    client.fail(
        "f1",
        lease_token=b"lease",
        fencing_token=7,
        partition_key="tenant:1",
        state_meta={"version": "5"},
    )
    client.cancel(
        "f1",
        lease_token=b"lease",
        fencing_token=7,
        partition_key="tenant:1",
        state_meta={"version": "6"},
    )
    client.close()

    assert [call[0] for call in executor.calls] == [
        "FLOW.CREATE",
        "FLOW.COMPLETE",
        "FLOW.TRANSITION",
        "FLOW.RETRY",
        "FLOW.FAIL",
        "FLOW.CANCEL",
    ]
    assert all("STATE_META" in call for call in executor.calls)


def test_autobatch_close_timeout_does_not_hang():
    executor = BlockingExecutor()
    client = FlowClient(executor).autobatch(max_batch=10, max_delay_ms=0)

    future = client.create_async(
        "f1",
        type="order",
        payload=b"p",
        partition_key="p1",
        return_record=False,
    )
    assert executor.entered.wait(1)

    with pytest.raises(TimeoutError):
        client.close(timeout=0.01)

    executor.release.set()
    assert future.result(timeout=1) == b"OK"
    client.close(timeout=1)


def test_autobatch_complete_uses_complete_many_independent_for_ack_calls():
    executor = PerItemAckExecutor()
    client = FlowClient(executor).autobatch(max_batch=10, max_delay_ms=5)
    results = [None, None, None]

    threads = [
        threading.Thread(
            target=lambda index=index: results.__setitem__(
                index,
                client.complete(
                    f"f{index}",
                    lease_token=b"lease",
                    fencing_token=index,
                    partition_key=f"p{index}",
                    result=b"ok",
                    return_record=False,
                ),
            )
        )
        for index in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    client.close()

    assert results == [b"OK", b"OK", b"OK"]
    assert executor.calls[0][0] == "FLOW.COMPLETE_MANY"
    assert executor.calls[0][1] == "MIXED"
    assert "INDEPENDENT" in executor.calls[0]


def test_autobatch_create_preserves_per_item_named_values():
    executor = PerItemAckExecutor()
    client = FlowClient(executor).autobatch(max_batch=10, max_delay_ms=5)
    results = [None, None]

    threads = [
        threading.Thread(
            target=lambda index=index: results.__setitem__(
                index,
                client.create(
                    f"f{index}",
                    type="order",
                    partition_key=f"p{index}",
                    values={"order": f"o{index}".encode()},
                    value_refs={"profile": f"profile-{index}"},
                    return_record=False,
                ),
            )
        )
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    client.close()

    assert results == [b"OK", b"OK"]
    call = executor.calls[0]
    assert call[0] == "FLOW.CREATE_MANY"
    assert "ITEMS_EXT" in call
    assert b"o0" in call
    assert b"o1" in call
    assert "profile-0" in call
    assert "profile-1" in call


def test_autobatch_terminal_mutations_preserve_named_values():
    executor = PerItemAckExecutor()
    client = FlowClient(executor).autobatch(max_batch=10, max_delay_ms=5)
    results = [None, None]

    threads = [
        threading.Thread(
            target=lambda index=index: results.__setitem__(
                index,
                client.complete(
                    f"f{index}",
                    lease_token=b"lease",
                    fencing_token=index,
                    partition_key=f"p{index}",
                    result=b"ok",
                    values={"receipt": b"receipt"},
                    value_refs={"profile": "profile-ref"},
                    drop_values=["old"],
                    override_values=["receipt"],
                    return_record=False,
                ),
            )
        )
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    client.close()

    assert results == [b"OK", b"OK"]
    call = executor.calls[0]
    assert call[0] == "FLOW.COMPLETE_MANY"
    assert call[call.index("VALUE") : call.index("VALUE") + 3] == (
        "VALUE",
        "receipt",
        b"receipt",
    )
    assert call[call.index("VALUE_REF") : call.index("VALUE_REF") + 3] == (
        "VALUE_REF",
        "profile",
        "profile-ref",
    )
    assert call[call.index("DROP_VALUE") : call.index("DROP_VALUE") + 2] == (
        "DROP_VALUE",
        "old",
    )
    assert call[call.index("OVERRIDE_VALUE") : call.index("OVERRIDE_VALUE") + 2] == (
        "OVERRIDE_VALUE",
        "receipt",
    )


def test_autobatch_falls_back_only_when_record_response_is_required():
    executor = FakeExecutor()
    client = FlowClient(executor).autobatch(max_batch=10, max_delay_ms=5)

    record = client.create("f1", type="order", return_record=True)
    ack = client.create("f2", type="order", return_record=False)
    client.close()

    assert record.id == "f1"
    assert isinstance(ack, FlowRecord)
    assert executor.calls[0][0] == "FLOW.CREATE"
    assert executor.calls[1][0] == "FLOW.CREATE_MANY"
    assert executor.calls[1][1].startswith("__flow_auto__:")


def test_complete_many_allows_ok_response():
    executor = AckExecutor()
    client = FlowClient(executor)
    item = ClaimedFlow("f1", b"lease", 3, partition_key="p1")

    result = client.complete_many(None, [item], result=b"ok", now_ms=100)

    assert result == b"OK"


def test_many_mutations_put_options_before_items():
    executor = FakeExecutor()
    client = FlowClient(executor)
    item = ClaimedFlow("f1", b"lease", 3, partition_key="p1")

    client.complete_many(None, [item], result=b"ok", now_ms=100)
    assert executor.calls[-1] == (
        "FLOW.COMPLETE_MANY",
        "MIXED",
        "RESULT",
        b"ok",
        "NOW",
        100,
        "ITEMS",
        "f1",
        "p1",
        b"lease",
        3,
    )

    client.retry_many("p1", [ClaimedFlow("f1", b"lease", 3)], error=b"err", now_ms=101)
    assert executor.calls[-1] == (
        "FLOW.RETRY_MANY",
        "p1",
        "ERROR",
        b"err",
        "NOW",
        101,
        "ITEMS",
        "f1",
        b"lease",
        3,
    )

    client.fail_many("p1", [ClaimedFlow("f1", b"lease", 3)], error=b"err", now_ms=102)
    assert executor.calls[-1] == (
        "FLOW.FAIL_MANY",
        "p1",
        "ERROR",
        b"err",
        "NOW",
        102,
        "ITEMS",
        "f1",
        b"lease",
        3,
    )


def test_transition_and_cancel_many_wire_shapes():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.transition_many(
        None,
        from_state="a",
        to_state="b",
        items=[FencedItem("f1", 4, b"lease", partition_key="p1")],
        now_ms=100,
    )
    assert executor.calls[-1] == (
        "FLOW.TRANSITION_MANY",
        "MIXED",
        "a",
        "b",
        "NOW",
        100,
        "ITEMS",
        "f1",
        "p1",
        4,
        b"lease",
    )

    client.cancel_many("p1", [FencedItem("f1", 4)], reason=b"stop", now_ms=101)
    assert executor.calls[-1] == (
        "FLOW.CANCEL_MANY",
        "p1",
        "REASON",
        b"stop",
        "NOW",
        101,
        "ITEMS",
        "f1",
        4,
    )


def test_single_extra_mutation_commands():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.extend_lease(
        "f1", b"lease", fencing_token=3, lease_ms=10_000, partition_key="p1", now_ms=100
    )
    assert executor.calls[-1][0] == "FLOW.EXTEND_LEASE"

    client.cancel(
        "f1", fencing_token=3, lease_token=b"lease", partition_key="p1", reason=b"stop", now_ms=101
    )
    assert executor.calls[-1][0] == "FLOW.CANCEL"

    client.rewind("f1", to_event="e1", partition_key="p1", expect_state="failed", now_ms=102)
    assert executor.calls[-1] == (
        "FLOW.REWIND",
        "f1",
        "TO_EVENT",
        "e1",
        "NOW",
        102,
        "PARTITION",
        "p1",
        "EXPECT_STATE",
        "failed",
    )

    client.value_put(b"value", partition_key="p1", owner_flow_id="f1", ttl_ms=1_000, now_ms=103)
    assert executor.calls[-1][0] == "FLOW.VALUE.PUT"


def test_query_policy_and_cleanup_commands():
    executor = FakeExecutor()
    client = FlowClient(executor)

    assert client.list("order", state="queued", count=10)[0].id == "f1"
    assert executor.calls[-1] == ("FLOW.LIST", "order", "STATE", "queued", "COUNT", 10)

    assert client.terminals("order", state="completed", rev=True, count=5)[0].id == "f1"
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

    assert client.failures("order", from_ms=10, to_ms=20)[0].id == "f1"
    assert executor.calls[-1] == ("FLOW.FAILURES", "order", "FROM_MS", 10, "TO_MS", 20)

    assert client.stats("order", state="queued", attributes={"tenant": "acme"})["count"] == 1
    assert executor.calls[-1] == (
        "FLOW.STATS",
        "order",
        "STATE",
        "queued",
        "ATTRIBUTE",
        "tenant",
        "acme",
    )

    assert client.by_parent("p", count=1, terminal_only=True)[0].id == "f1"
    assert executor.calls[-1] == (
        "FLOW.BY_PARENT",
        "p",
        "COUNT",
        1,
        "TERMINAL_ONLY",
        "true",
    )

    assert client.info("order") == {b"ok": 1}
    assert client.stuck("order", older_than_ms=100, now_ms=200)[0].id == "f1"
    assert client.history("f1", count=10, from_version=2, values=True)
    assert client.policy_get("order", state="queued") == {b"ok": 1}
    assert client.retention_cleanup(limit=100, now_ms=123) == {b"ok": 1}


def test_protocol_ferricstore_commands_are_first_class():
    class ProtocolExecutor(FakeExecutor):
        def execute_command(self, *args):
            self.calls.append(args)
            command = args[0]
            if command == "CAS":
                return 1
            if command in {
                "LOCK",
                "FETCH_OR_COMPUTE_RESULT",
                "FETCH_OR_COMPUTE_ERROR",
                "CLUSTER.JOIN",
                "CLUSTER.LEAVE",
                "CLUSTER.FAILOVER",
                "CLUSTER.PROMOTE",
                "CLUSTER.DEMOTE",
            }:
                return b"OK"
            if command in {"UNLOCK", "EXTEND"}:
                return 1
            if command == "RATELIMIT.ADD":
                return [b"allowed", 3, 7, 99]
            if command == "FERRICSTORE.KEY_INFO":
                return [
                    b"type",
                    b"string",
                    b"value_size",
                    b"12",
                    b"ttl_ms",
                    b"-1",
                    b"hot_cache_status",
                    b"hot",
                    b"last_write_shard",
                    b"2",
                ]
            if command == "FETCH_OR_COMPUTE":
                return [b"hit", b"value"]
            if command == "CLUSTER.KEYSLOT":
                return 42
            if command == "CLUSTER.HEALTH":
                return "shard_0:\r\n  role: leader\r\n  keys: 12\r\n"
            if command == "CLUSTER.STATS":
                return "total_keys: 12\r\ntotal_memory_bytes: 100\r\n"
            if command == "CLUSTER.STATUS":
                return "cluster_state: ok\r\npromotion_epoch: 3\r\n"
            if command == "FERRICSTORE.METRICS":
                return [b"ops", b"10", b"latency_ms", b"2"]
            return b"OK"

    executor = ProtocolExecutor()
    client = FlowClient(executor)

    assert client.cas("k", b"old", b"new", ex=10) is True
    assert executor.calls[-1] == ("CAS", "k", b"old", b"new", "EX", 10)

    assert client.lock("lock:k", "owner", 1_000) is True
    assert executor.calls[-1] == ("LOCK", "lock:k", "owner", 1_000)
    assert client.unlock("lock:k", "owner") == 1
    assert client.extend_lock("lock:k", "owner", 2_000) == 1

    rate = client.ratelimit_add("rl:k", window_ms=1_000, max=10, count=3)
    assert isinstance(rate, RateLimitResult)
    assert rate.allowed is True
    assert rate.count == 3
    assert executor.calls[-1] == ("RATELIMIT.ADD", "rl:k", 1000, 10, 3)

    info = client.key_info("k")
    assert isinstance(info, KeyInfo)
    assert info.type == "string"
    assert info.value_size == 12
    assert info.last_write_shard == 2

    computed = client.fetch_or_compute("foc:k", ttl_ms=5_000, hint="expensive")
    assert computed.hit is True
    assert computed.value == b"value"
    assert executor.calls[-1] == ("FETCH_OR_COMPUTE", "foc:k", 5000, "expensive")

    assert client.fetch_or_compute_result("foc:k", b"value", ttl_ms=5_000) is True
    assert executor.calls[-1] == ("FETCH_OR_COMPUTE_RESULT", "foc:k", b"value", 5000)
    assert client.fetch_or_compute_error("foc:k", "failed") is True

    assert client.cluster_keyslot("k") == 42
    assert client.cluster_health()["shard_0"]["keys"] == 12
    assert client.cluster_stats()["total_keys"] == 12
    assert client.cluster_status()["promotion_epoch"] == 3
    assert client.cluster_join("node@127.0.0.1", replace=True) is True
    assert executor.calls[-1] == ("CLUSTER.JOIN", "node@127.0.0.1", "REPLACE")

    client.ferricstore_config("GET", "max_memory")
    assert executor.calls[-1] == ("FERRICSTORE.CONFIG", "GET", "max_memory")
    assert client.ferricstore_metrics()["ops"] == b"10"


def test_command_passes_through_data_structure_commands():
    executor = FakeExecutor()
    client = FlowClient(executor)

    assert client.command("SET", "k", "v")[b"id"] == b"f1"
    assert executor.calls[-1] == ("SET", "k", "v")


def test_command_pipeline_batches_mixed_commands_with_sequential_fallback():
    executor = FakeExecutor()
    client = FlowClient(executor)

    pipe = client.pipeline()
    result = pipe.command("SET", "k", "v").command("FLOW.CREATE", "f1", "TYPE", "order").execute()

    assert len(result) == 2
    assert executor.calls[0] == ("SET", "k", "v")
    assert executor.calls[1] == ("FLOW.CREATE", "f1", "TYPE", "order")


def test_command_pipeline_context_executes_on_success():
    executor = FakeExecutor()
    client = FlowClient(executor)

    with client.pipeline() as pipe:
        pipe.command("GET", "k")
        pipe.command("HSET", "h", "f", "v")

    assert pipe.results is not None
    assert executor.calls[-2:] == [("GET", "k"), ("HSET", "h", "f", "v")]


def test_command_pipeline_maps_native_execute_batch_errors():
    class BatchExecutor:
        def execute_command(self, *args):
            return b"OK"

        def execute_batch(self, commands):
            raise RuntimeError("ERR flow already exists")

    client = FlowClient(BatchExecutor())

    with pytest.raises(FlowAlreadyExistsError):
        client.pipeline().command("FLOW.CREATE", "f1").execute()


def test_server_errors_are_typed():
    class ErrorExecutor:
        def execute_command(self, *args):
            raise RuntimeError("ERR flow already exists")

    client = FlowClient(ErrorExecutor())

    with pytest.raises(FlowAlreadyExistsError) as exc:
        client.command("FLOW.CREATE", "f1")

    assert exc.value.code == "flow_already_exists"


def test_stale_lease_errors_are_typed():
    class ErrorExecutor:
        def execute_command(self, *args):
            raise RuntimeError("ERR stale flow lease")

    client = FlowClient(ErrorExecutor())

    with pytest.raises(StaleLeaseError):
        client.complete("f1", lease_token=b"old", fencing_token=1, return_record=False)


def test_claimed_item_decodes_compact_rows_without_resp_dict():
    rows = [
        [b"flow-1", b"tenant-1", b"lease-1", 7],
        [b"flow-2", None, b"lease-2", 8, b"running:step"],
    ]

    items = ClaimedFlow.from_compact_rows(rows)

    assert items == [
        ClaimedFlow("flow-1", b"lease-1", 7, partition_key="tenant-1"),
        ClaimedFlow("flow-2", b"lease-2", 8, run_state="running:step"),
    ]


def test_claimed_item_compact_rows_fallback_to_resp_maps():
    items = ClaimedFlow.from_compact_rows(
        [
            {
                b"id": b"flow-1",
                b"lease_token": b"lease-1",
                b"fencing_token": 7,
                b"partition_key": b"tenant-1",
            }
        ]
    )

    assert items == [ClaimedFlow("flow-1", b"lease-1", 7, partition_key="tenant-1")]


def test_install_policy_still_builds_state_policy():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.install_policy(
        "order",
        states={
            "queued": RetryPolicy(
                max_retries=5,
                backoff="exponential",
                base_ms=100,
                max_ms=1_000,
                jitter_pct=10,
                exhausted_to="failed",
            )
        },
    )

    assert executor.calls[-1][:4] == ("FLOW.POLICY.SET", "order", "STATE", "queued")


def test_install_policy_can_set_indexed_state_meta():
    executor = FakeExecutor()
    client = FlowClient(executor)

    client.install_policy(
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


def test_flow_record_decodes_state_meta_and_indexed_state_meta():
    record = FlowRecord.from_resp(
        {
            b"id": b"f1",
            b"type": b"order",
            b"state": b"completed",
            b"partition_key": b"tenant:1",
            b"version": 3,
            b"state_meta": {
                b"accept": {b"version": 1},
                b"completed": {b"version": 3},
            },
            b"indexed_state_meta": b"version",
        }
    )

    assert record.state_meta == {
        "accept": {"version": 1},
        "completed": {"version": 3},
    }
    assert record.indexed_state_meta == "version"


def test_admin_flow_wrappers_build_readable_commands_and_normalize_responses():
    executor = FakeExecutor()
    client = FlowClient(executor)

    assert client.attributes("order", state="queued", count=10) == [{"name": "tenant", "count": 3}]
    assert executor.calls[-1] == ("FLOW.ATTRIBUTES", "order", "STATE", "queued", "COUNT", 10)

    assert client.attribute_values("order", "tenant", state="queued") == [
        {"value": "acme", "count": 2}
    ]
    assert executor.calls[-1] == (
        "FLOW.ATTRIBUTE_VALUES",
        "order",
        "tenant",
        "STATE",
        "queued",
    )

    schedule = client.schedule_create(
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

    due = client.schedule_fire_due(block_ms=1000, limit=50)
    assert isinstance(due, ScheduleResult)
    assert due["status"] == "active"
    assert executor.calls[-1] == ("FLOW.SCHEDULE.FIRE_DUE", "BLOCK", 1000, "LIMIT", 50)
    schedules = client.schedule_list(target_type="flow")
    assert isinstance(schedules[0], ScheduleResult)
    assert schedules[0]["status"] == "active"
    assert executor.calls[-1] == ("FLOW.SCHEDULE.LIST", "TARGET_TYPE", "flow")

    executor.responses.append("OK")
    deleted = client.schedule_delete("daily-report", now_ms=200)
    assert isinstance(deleted, ScheduleResult)
    assert deleted.id == "daily-report"
    assert deleted.status == "deleted"
    assert executor.calls[-1] == ("FLOW.SCHEDULE.DELETE", "daily-report", "NOW", 200)

    effect = client.effect_reserve(
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

    effect = client.effect_confirm(
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

    effect = client.effect_fail(
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

    effect = client.effect_compensate(
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

    approval = client.approval_request(
        "approval-1",
        flow_id="flow-1",
        scope="tenant-a",
        reason="manual review",
        requested_by="worker-1",
        assignees=["ops"],
        policy_hash="hash",
        policy_version=2,
        timeout_ms=30_000,
        expires_at_ms=130_000,
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
        "REASON",
        "manual review",
        "REQUESTED_BY",
        "worker-1",
        "ASSIGNEES",
        ["ops"],
        "POLICY_HASH",
        "hash",
        "POLICY_VERSION",
        2,
        "TIMEOUT_MS",
        30_000,
        "EXPIRES_AT_MS",
        130_000,
    )

    approval = client.approval_approve("approval-1", approver="admin", reason="ok")
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

    ledger = client.governance_ledger("flow-1", partition_key="tenant-a", rev=True, limit=5)
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

    circuit = client.circuit_open("email", open_ms=1000, failure_threshold=3)
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

    budget = client.budget_reserve(
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

    overview = client.governance_overview(scope="tenant-a", status="pending", flow_id="flow-1")
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

    committed = client.budget_commit("tenant-a", "budget-res-1", 7, usage={"tokens": 7})
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

    released = client.budget_release("tenant-a", "budget-res-unused")
    assert released.get("reserved_amount") == 10
    assert executor.calls[-1] == (
        "FLOW.BUDGET.RELEASE",
        "tenant-a",
        "RESERVATION_ID",
        "budget-res-unused",
    )

    client.limit_lease("tenant-a", shard_id=1, amount=5, ttl_ms=1000, limit=10)
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
