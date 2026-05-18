import threading
import zlib

from ferricstore import FlowClient, JsonCodec
from ferricstore.types import ChildSpec, ClaimedItem, CreateItem, FencedItem, FlowRecord, RetryPolicy


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
            return [
                {
                    b"id": b"f1",
                    b"type": b"order",
                    b"state": b"queued",
                    b"partition_key": b"tenant:1",
                    b"lease_token": b"lease",
                    b"fencing_token": 7,
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
        "JOBS",
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
        "JOBS",
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
