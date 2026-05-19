import threading
import zlib

import pytest

from ferricstore import FlowAlreadyExistsError, FlowClient, QueueFlowWorker, JsonCodec, StaleLeaseError
from ferricstore.types import (
    ChildSpec,
    ClaimedItem,
    CreateItem,
    FencedItem,
    FlowRecord,
    KeyInfo,
    RateLimitResult,
    RetryPolicy,
)


class FakeRedis:
    def __init__(self):
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        command = args[0]
        record = {
            b"id": b"f1",
            b"type": b"order",
            b"state": b"created",
            b"partition_key": b"tenant:1",
            b"version": 1,
        }
        if "VALUE" in args:
            record[b"values"] = {b"order": b"order-bytes"}
            record[b"value_refs"] = {b"order": {b"ref": b"ref-order"}}
        if command == "FLOW.VALUE.MGET":
            return list(args[1:])
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
        return record


class AckRedis(FakeRedis):
    def execute_command(self, *args):
        self.calls.append(args)
        return b"OK"


class PerItemAckRedis(FakeRedis):
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


class ClaimThenAckRedis(FakeRedis):
    def execute_command(self, *args):
        self.calls.append(args)
        command = args[0]
        if command == "FLOW.CLAIM_DUE":
            if "RETURN" in args and args[args.index("RETURN") + 1] == "JOBS_COMPACT":
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


def test_create_builds_flow_create_command():
    redis = FakeRedis()
    client = FlowClient(redis)

    record = client.create(
        "f1",
        type="order",
        state="created",
        partition_key="tenant:1",
        payload=b"hello",
        now_ms=100,
    )

    assert record.id == "f1"
    assert redis.calls[0] == (
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
    redis = AckRedis()
    client = FlowClient(redis)

    result = client.create(
        "f1",
        type="order",
        state="created",
        partition_key="tenant:1",
        payload=b"hello",
        now_ms=100,
        return_record=False,
    )

    assert result == b"OK"
    assert len(redis.calls) == 1


def test_create_can_attach_named_values_and_refs():
    redis = AckRedis()
    client = FlowClient(redis)

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
    assert redis.calls[0] == (
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


def test_signal_builds_flow_signal_command_with_guards_and_values():
    redis = AckRedis()
    client = FlowClient(redis)

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
    redis = AckRedis()
    client = FlowClient(redis)

    result = client.enqueue(
        "f1",
        type="order",
        payload=b"hello",
        partition_key="tenant:1",
        now_ms=100,
    )

    assert result == b"OK"
    assert redis.calls[0] == (
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


def test_enqueue_many_groups_no_partition_items_by_auto_bucket():
    redis = PerItemAckRedis()
    client = FlowClient(redis)
    items = [CreateItem(f"flow-{idx}", b"payload") for idx in range(64)]

    results = client.enqueue_many(items, type="order", now_ms=100)

    assert results == [b"OK"] * len(items)
    assert len(redis.calls) > 1

    for call in redis.calls:
        bucket = call[1]
        assert isinstance(bucket, str)
        assert bucket.startswith("__flow_auto__:")
        item_args = call[call.index("ITEMS") + 1 :]
        ids = item_args[0::2]
        for id in ids:
            assert bucket == f"__flow_auto__:{zlib.crc32(id.encode()) % 256}"


def test_autobatch_groups_no_partition_creates_by_auto_bucket():
    redis = PerItemAckRedis()
    client = FlowClient(redis).autobatch(max_batch=64, max_delay_ms=0)

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
    for call in redis.calls:
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
    redis = FakeRedis()
    client = FlowClient(redis, codec=JsonCodec())

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
    redis = FakeRedis()
    client = FlowClient(redis)

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


def test_claim_due_omits_state_when_none():
    redis = FakeRedis()
    client = FlowClient(redis)

    client.claim_due(
        "order",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        priority=0,
        now_ms=100,
    )

    assert redis.calls[0] == (
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
    redis = FakeRedis()
    client = FlowClient(redis)

    client.claim_due(
        "order",
        states=["queued", "retry"],
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        priority=0,
        now_ms=100,
    )

    assert redis.calls[0] == (
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
    redis = FakeRedis()
    client = FlowClient(redis)

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
    redis = PerItemAckRedis()
    client = FlowClient(redis)

    result = client.complete_many(
        "tenant:1",
        [ClaimedItem(id="f1", lease_token=b"lease", fencing_token=7)],
        values={"receipt": b"receipt-bytes"},
        value_refs={"profile": "profile-ref"},
        drop_values=["old"],
        override_values=["receipt"],
        now_ms=100,
        independent=True,
    )

    call = redis.calls[0]
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


def test_claim_due_rejects_state_and_states_together():
    redis = FakeRedis()
    client = FlowClient(redis)

    with pytest.raises(ValueError, match="state and states are mutually exclusive"):
        client.claim_due("order", state="queued", states=["retry"], worker="worker-1")


def test_value_put_named_options_and_value_mget_decode_values():
    redis = FakeRedis()
    client = FlowClient(redis)

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
    assert redis.calls[0] == (
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
    assert redis.calls[1] == ("FLOW.VALUE.MGET", "ref-a", "ref-b")


def test_get_can_request_selected_named_values():
    redis = FakeRedis()
    client = FlowClient(redis)

    record = client.get("f1", partition_key="tenant:1", values=["order"], value_max_bytes=1024)

    assert record is not None
    assert record.values == {"order": b"order-bytes"}
    assert redis.calls[0] == (
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
    redis = AckRedis()
    client = FlowClient(redis)

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
    assert redis.calls[0] == (
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
    assert redis.calls[1] == (
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


def test_claim_due_can_return_job_only_items():
    redis = FakeRedis()
    client = FlowClient(redis)

    jobs = client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        limit=10,
        priority=0,
        now_ms=100,
        job_only=True,
    )

    assert isinstance(jobs[0], ClaimedItem)
    assert jobs[0].id == "f1"
    assert jobs[0].lease_token == b"lease"
    assert jobs[0].fencing_token == 7
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
        10,
        "NOW",
        100,
        "PRIORITY",
        0,
        "RETURN",
        "JOBS_COMPACT",
    )


def test_claim_due_can_scan_multiple_partitions():
    redis = FakeRedis()
    client = FlowClient(redis)

    client.claim_due(
        "order",
        state="queued",
        worker="worker-1",
        partition_keys=["p1", "p2"],
        limit=10,
        now_ms=100,
        job_only=True,
    )

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
        10,
        "NOW",
        100,
        "PARTITIONS",
        2,
        "p1",
        "p2",
        "RETURN",
        "JOBS_COMPACT",
    )


def test_claim_jobs_can_scan_multiple_partitions():
    redis = FakeRedis()
    client = FlowClient(redis)

    client.claim_jobs(
        "order",
        state="queued",
        worker="worker-1",
        partition_keys=["p1", "p2"],
        limit=10,
        now_ms=100,
    )

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
        "JOBS_COMPACT",
        "RECLAIM_EXPIRED",
        "false",
    )


def test_claim_jobs_and_complete_jobs_hide_hot_path_options():
    redis = ClaimThenAckRedis()
    client = FlowClient(redis)

    jobs = client.claim_jobs(
        "order",
        state="queued",
        worker="worker-1",
        partition_key="tenant:1",
        limit=10,
        now_ms=100,
    )
    result = client.complete_jobs(jobs, now_ms=200)

    assert isinstance(jobs[0], ClaimedItem)
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
        10,
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
        "NOW",
        200,
        "INDEPENDENT",
        "true",
        "ITEMS",
        "f1",
        b"lease",
        7,
    )


def test_flow_worker_runs_hot_path_with_minimal_developer_code():
    redis = ClaimThenAckRedis()
    client = FlowClient(redis)
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
    assert redis.calls[0][:3] == ("FLOW.CLAIM_DUE", "order", "STATE")
    assert "RETURN" in redis.calls[0]
    assert redis.calls[0][redis.calls[0].index("RETURN") + 1] == "JOBS_COMPACT"
    assert redis.calls[1][0] == "FLOW.COMPLETE_MANY"


def test_flow_worker_omitted_state_means_any_state():
    redis = ClaimThenAckRedis()
    client = FlowClient(redis)
    worker = QueueFlowWorker(client, type="order", worker="worker-1")

    worker.run_once(lambda _job: None)
    worker.close()

    assert "STATE" not in redis.calls[0]
    assert redis.calls[0][0] == "FLOW.CLAIM_DUE"


def test_flow_worker_supports_multi_state_claims():
    redis = ClaimThenAckRedis()
    client = FlowClient(redis)
    worker = QueueFlowWorker(
        client,
        type="order",
        states=["queued", "retry"],
        worker="worker-1",
    )

    worker.run_once(lambda _job: None)
    worker.close()

    assert redis.calls[0][:6] == (
        "FLOW.CLAIM_DUE",
        "order",
        "STATE",
        "queued",
        "STATE",
        "retry",
    )


def test_flow_worker_can_claim_named_values_without_compact_return():
    redis = ClaimThenAckRedis()
    client = FlowClient(redis)
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
    claim = redis.calls[0]
    assert "RETURN" not in claim
    assert claim[claim.index("VALUE") : claim.index("VALUE") + 2] == ("VALUE", "order")
    assert claim[claim.index("VALUE_MAX_BYTES") : claim.index("VALUE_MAX_BYTES") + 2] == (
        "VALUE_MAX_BYTES",
        1024,
    )


def test_json_codec_omits_none_optional_values_on_singular_writes():
    redis = AckRedis()
    client = FlowClient(redis, codec=JsonCodec())

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

    for call in redis.calls:
        assert "PAYLOAD" not in call
        assert "RESULT" not in call
        assert "ERROR" not in call


def test_spawn_children_mixed_uses_one_mixed_marker():
    redis = FakeRedis()
    client = FlowClient(redis)

    client.spawn_children(
        "parent-1",
        [
            ChildSpec("c1", "email", b"p1", partition_key="p1"),
            ChildSpec("c2", "audit", b"p2", partition_key="p2"),
        ],
        partition_key="parent-p",
        group_id="g1",
    )

    assert redis.calls[0].count("MIXED") == 1
    items_idx = redis.calls[0].index("ITEMS")
    assert redis.calls[0][items_idx:] == (
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
    redis = FakeRedis()
    client = FlowClient(redis)

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
        "ITEMS",
        "f1",
        "p1",
        b"p1",
        "f2",
        "p2",
        b"p2",
    )


def test_create_many_without_partition_uses_auto_wire_shape():
    redis = FakeRedis()
    client = FlowClient(redis)

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

    assert redis.calls[0] == (
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


def test_create_many_uses_extended_items_for_per_item_named_values():
    redis = FakeRedis()
    client = FlowClient(redis)

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
    redis = FakeRedis()
    client = FlowClient(redis)

    client.spawn_children(
        "parent-1",
        [
            ChildSpec("c1", "email", b"p1", partition_key="p1", values={"order": b"o1"}),
            ChildSpec("c2", "audit", b"p2", partition_key="p2", value_refs={"profile": "profile-ref"}),
        ],
        partition_key="parent-p",
        group_id="g1",
        values={"shared": b"shared-bytes"},
        now_ms=100,
    )

    assert redis.calls[0] == (
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


def test_create_many_allows_ok_response():
    redis = AckRedis()
    client = FlowClient(redis)

    result = client.create_many(
        None,
        [CreateItem("f1", b"p1", partition_key="p1")],
        type="order",
        state="queued",
        now_ms=100,
    )

    assert result == b"OK"


def test_many_commands_support_independent_option():
    redis = FakeRedis()
    client = FlowClient(redis)
    claimed = ClaimedItem("f1", b"lease", 3, partition_key="p1")
    fenced = FencedItem("f1", 4, b"lease", partition_key="p1")

    client.create_many(
        None,
        [CreateItem("f1", b"p1", partition_key="p1")],
        type="order",
        now_ms=100,
        independent=True,
    )
    assert "INDEPENDENT" in redis.calls[-1]
    assert redis.calls[-1][redis.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.complete_many(None, [claimed], result=b"ok", now_ms=101, independent=True)
    assert "INDEPENDENT" in redis.calls[-1]
    assert redis.calls[-1][redis.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.transition_many(
        None,
        from_state="queued",
        to_state="ready",
        items=[fenced],
        now_ms=102,
        independent=True,
    )
    assert "INDEPENDENT" in redis.calls[-1]
    assert redis.calls[-1][redis.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.retry_many(None, [claimed], error=b"err", now_ms=103, independent=True)
    assert "INDEPENDENT" in redis.calls[-1]
    assert redis.calls[-1][redis.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.fail_many(None, [claimed], error=b"err", now_ms=104, independent=True)
    assert "INDEPENDENT" in redis.calls[-1]
    assert redis.calls[-1][redis.calls[-1].index("INDEPENDENT") + 1] == "true"

    client.cancel_many(None, [fenced], reason=b"stop", now_ms=105, independent=True)
    assert "INDEPENDENT" in redis.calls[-1]
    assert redis.calls[-1][redis.calls[-1].index("INDEPENDENT") + 1] == "true"


def test_autobatch_create_uses_create_many_independent_for_ack_calls():
    redis = PerItemAckRedis()
    client = FlowClient(redis).autobatch(max_batch=10, max_delay_ms=5)
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
    assert redis.calls[0][0] == "FLOW.CREATE_MANY"
    assert redis.calls[0][1] == "MIXED"
    assert "INDEPENDENT" in redis.calls[0]


def test_autobatch_complete_uses_complete_many_independent_for_ack_calls():
    redis = PerItemAckRedis()
    client = FlowClient(redis).autobatch(max_batch=10, max_delay_ms=5)
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
    assert redis.calls[0][0] == "FLOW.COMPLETE_MANY"
    assert redis.calls[0][1] == "MIXED"
    assert "INDEPENDENT" in redis.calls[0]


def test_autobatch_create_preserves_per_item_named_values():
    redis = PerItemAckRedis()
    client = FlowClient(redis).autobatch(max_batch=10, max_delay_ms=5)
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
    call = redis.calls[0]
    assert call[0] == "FLOW.CREATE_MANY"
    assert "ITEMS_EXT" in call
    assert b"o0" in call
    assert b"o1" in call
    assert "profile-0" in call
    assert "profile-1" in call


def test_autobatch_terminal_mutations_preserve_named_values():
    redis = PerItemAckRedis()
    client = FlowClient(redis).autobatch(max_batch=10, max_delay_ms=5)
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
    call = redis.calls[0]
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
    redis = FakeRedis()
    client = FlowClient(redis).autobatch(max_batch=10, max_delay_ms=5)

    record = client.create("f1", type="order", return_record=True)
    ack = client.create("f2", type="order", return_record=False)
    client.close()

    assert record.id == "f1"
    assert isinstance(ack, FlowRecord)
    assert redis.calls[0][0] == "FLOW.CREATE"
    assert redis.calls[1][0] == "FLOW.CREATE_MANY"
    assert redis.calls[1][1].startswith("__flow_auto__:")


def test_complete_many_allows_ok_response():
    redis = AckRedis()
    client = FlowClient(redis)
    item = ClaimedItem("f1", b"lease", 3, partition_key="p1")

    result = client.complete_many(None, [item], result=b"ok", now_ms=100)

    assert result == b"OK"


def test_many_mutations_put_options_before_items():
    redis = FakeRedis()
    client = FlowClient(redis)
    item = ClaimedItem("f1", b"lease", 3, partition_key="p1")

    client.complete_many(None, [item], result=b"ok", now_ms=100)
    assert redis.calls[-1] == (
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

    client.retry_many("p1", [ClaimedItem("f1", b"lease", 3)], error=b"err", now_ms=101)
    assert redis.calls[-1] == (
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

    client.fail_many("p1", [ClaimedItem("f1", b"lease", 3)], error=b"err", now_ms=102)
    assert redis.calls[-1] == (
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
    redis = FakeRedis()
    client = FlowClient(redis)

    client.transition_many(
        None,
        from_state="a",
        to_state="b",
        items=[FencedItem("f1", 4, b"lease", partition_key="p1")],
        now_ms=100,
    )
    assert redis.calls[-1] == (
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
    assert redis.calls[-1] == (
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
    redis = FakeRedis()
    client = FlowClient(redis)

    client.extend_lease("f1", b"lease", fencing_token=3, lease_ms=10_000, partition_key="p1", now_ms=100)
    assert redis.calls[-1][0] == "FLOW.EXTEND_LEASE"

    client.cancel("f1", fencing_token=3, lease_token=b"lease", partition_key="p1", reason=b"stop", now_ms=101)
    assert redis.calls[-1][0] == "FLOW.CANCEL"

    client.rewind("f1", to_event="e1", partition_key="p1", expect_state="failed", now_ms=102)
    assert redis.calls[-1] == (
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
    assert redis.calls[-1][0] == "FLOW.VALUE.PUT"


def test_query_policy_and_cleanup_commands():
    redis = FakeRedis()
    client = FlowClient(redis)

    assert client.list("order", state="queued", count=10)[0].id == "f1"
    assert redis.calls[-1] == ("FLOW.LIST", "order", "STATE", "queued", "COUNT", 10)

    assert client.terminals("order", state="completed", rev=True, count=5)[0].id == "f1"
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

    assert client.failures("order", from_ms=10, to_ms=20)[0].id == "f1"
    assert redis.calls[-1] == ("FLOW.FAILURES", "order", "FROM_MS", 10, "TO_MS", 20)

    assert client.by_parent("p", count=1, terminal_only=True)[0].id == "f1"
    assert redis.calls[-1] == (
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


def test_native_ferricstore_commands_are_first_class():
    class NativeRedis(FakeRedis):
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

    redis = NativeRedis()
    client = FlowClient(redis)

    assert client.cas("k", b"old", b"new", ex=10) is True
    assert redis.calls[-1] == ("CAS", "k", b"old", b"new", "EX", 10)

    assert client.lock("lock:k", "owner", 1_000) is True
    assert redis.calls[-1] == ("LOCK", "lock:k", "owner", 1_000)
    assert client.unlock("lock:k", "owner") == 1
    assert client.extend_lock("lock:k", "owner", 2_000) == 1

    rate = client.ratelimit_add("rl:k", window_ms=1_000, max=10, count=3)
    assert isinstance(rate, RateLimitResult)
    assert rate.allowed is True
    assert rate.count == 3
    assert redis.calls[-1] == ("RATELIMIT.ADD", "rl:k", 1000, 10, 3)

    info = client.key_info("k")
    assert isinstance(info, KeyInfo)
    assert info.type == "string"
    assert info.value_size == 12
    assert info.last_write_shard == 2

    computed = client.fetch_or_compute("foc:k", ttl_ms=5_000, hint="expensive")
    assert computed.hit is True
    assert computed.value == b"value"
    assert redis.calls[-1] == ("FETCH_OR_COMPUTE", "foc:k", 5000, "expensive")

    assert client.fetch_or_compute_result("foc:k", b"value", ttl_ms=5_000) is True
    assert redis.calls[-1] == ("FETCH_OR_COMPUTE_RESULT", "foc:k", b"value", 5000)
    assert client.fetch_or_compute_error("foc:k", "failed") is True

    assert client.cluster_keyslot("k") == 42
    assert client.cluster_health()["shard_0"]["keys"] == 12
    assert client.cluster_stats()["total_keys"] == 12
    assert client.cluster_status()["promotion_epoch"] == 3
    assert client.cluster_join("node@127.0.0.1", replace=True) is True
    assert redis.calls[-1] == ("CLUSTER.JOIN", "node@127.0.0.1", "REPLACE")

    client.ferricstore_config("GET", "max_memory")
    assert redis.calls[-1] == ("FERRICSTORE.CONFIG", "GET", "max_memory")
    assert client.ferricstore_metrics()["ops"] == b"10"


def test_command_passes_through_normal_redis_commands():
    redis = FakeRedis()
    client = FlowClient(redis)

    assert client.command("SET", "k", "v")[b"id"] == b"f1"
    assert redis.calls[-1] == ("SET", "k", "v")


def test_command_pipeline_batches_mixed_commands_with_sequential_fallback():
    redis = FakeRedis()
    client = FlowClient(redis)

    pipe = client.pipeline()
    result = (
        pipe.command("SET", "k", "v")
        .command("FLOW.CREATE", "f1", "TYPE", "order")
        .execute()
    )

    assert len(result) == 2
    assert redis.calls[0] == ("SET", "k", "v")
    assert redis.calls[1] == ("FLOW.CREATE", "f1", "TYPE", "order")


def test_command_pipeline_context_executes_on_success():
    redis = FakeRedis()
    client = FlowClient(redis)

    with client.pipeline() as pipe:
        pipe.command("GET", "k")
        pipe.command("HSET", "h", "f", "v")

    assert pipe.results is not None
    assert redis.calls[-2:] == [("GET", "k"), ("HSET", "h", "f", "v")]


def test_server_errors_are_typed():
    class ErrorRedis:
        def execute_command(self, *args):
            raise RuntimeError("ERR flow already exists")

    client = FlowClient(ErrorRedis())

    with pytest.raises(FlowAlreadyExistsError) as exc:
        client.command("FLOW.CREATE", "f1")

    assert exc.value.code == "flow_already_exists"


def test_stale_lease_errors_are_typed():
    class ErrorRedis:
        def execute_command(self, *args):
            raise RuntimeError("ERR stale flow lease")

    client = FlowClient(ErrorRedis())

    with pytest.raises(StaleLeaseError):
        client.complete("f1", lease_token=b"old", fencing_token=1, return_record=False)


def test_install_policy_still_builds_state_policy():
    redis = FakeRedis()
    client = FlowClient(redis)

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

    assert redis.calls[-1][:4] == ("FLOW.POLICY.SET", "order", "STATE", "queued")
