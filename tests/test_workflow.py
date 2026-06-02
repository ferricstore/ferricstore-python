import pytest

from ferricstore import (
    ChildSpec,
    Complete,
    ExceptionPolicy,
    FlowClient,
    FlowWorkflow,
    RetryPolicy,
    ValueConfig,
    Worker,
    WorkerConfig,
    Workflow,
    WorkflowClient,
    WorkflowContext,
    WorkflowWorker,
    complete,
    fail,
    state,
    transition,
)
from ferricstore.types import FlowRecord


class FakeRedis:
    def __init__(self):
        self.calls = []
        self.claim_state = b"created"
        self.claim_run_state = None
        self.claim_ids = [b"f1"]
        self.closed = False

    def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "FLOW.CLAIM_DUE":
            records = []
            for idx, claim_id in enumerate(self.claim_ids, start=1):
                record = {
                    b"id": claim_id,
                    b"type": b"order",
                    b"state": self.claim_state,
                    b"partition_key": b"tenant:order",
                    b"lease_token": f"lease-{idx}".encode(),
                    b"fencing_token": idx,
                }
                if self.claim_run_state is not None:
                    record[b"run_state"] = self.claim_run_state
                if "VALUE" in args:
                    record[b"values"] = {b"order": b"order-bytes"}
                    record[b"value_refs"] = {b"order": {b"ref": b"ref-order"}}
                records.append(record)
            return records
        return {
            b"id": b"f1",
            b"type": b"order",
            b"state": b"next",
            b"partition_key": b"tenant:order",
        }

    def close(self):
        self.closed = True


class OrderWorkflow(Workflow):
    type = "order"
    initial_state = "created"
    partition_by = ("tenant_id", "order_id")

    @state("created")
    def created(self, job):
        return transition("next", payload=b"ok")

    @state("done")
    def done(self, job) -> Complete:
        return complete(result=b"done")


class LeanWorkflow(Workflow):
    type = "lean"
    initial_state = "created"

    @state("created", claim_payload=False, return_record=False)
    def created(self, job):
        return transition("next")


class DoneWorkflow(Workflow):
    type = "order"
    initial_state = "done"

    @state("done", claim_payload=False, return_record=False)
    def done(self, job) -> Complete:
        return complete(result=b"done")


class CompactWorkflow(Workflow):
    type = "lean"
    initial_state = "created"

    @state("created", claim_payload=False, claim_record=False, return_record=False)
    def created(self, job):
        return transition("next")


class ContextWorkflow(Workflow):
    type = "order"
    initial_state = "created"

    def __init__(self, client):
        super().__init__(client)
        self.seen_contexts = []

    @state("created", return_record=False)
    def created(self, ctx: WorkflowContext):
        self.seen_contexts.append(
            {
                "id": ctx.id,
                "type": ctx.type,
                "state": ctx.state,
                "logical_state": ctx.logical_state,
                "partition_key": ctx.partition_key,
                "lease_token": ctx.lease_token,
                "fencing_token": ctx.fencing_token,
            }
        )
        ctx.flow.enqueue("child-1", type="child", payload=b"payload")
        return complete(result=b"ok")


class ContextLookupWorkflow(Workflow):
    type = "order"
    initial_state = "created"

    @state("created", return_record=False)
    def created(self, ctx: WorkflowContext):
        ctx.flow.get()
        ctx.flow.history(count=5)
        return complete(result=b"ok")


class ContextChildrenWorkflow(Workflow):
    type = "order"
    initial_state = "created"

    @state("created", return_record=False)
    def created(self, ctx: WorkflowContext):
        ctx.flow.spawn_children(
            [ChildSpec(id="child-1", type="child", payload=b"payload")], wait_state="done"
        )
        return transition("waiting")


class ValueWorkflow(Workflow):
    type = "value-order"
    initial_state = "created"

    def __init__(self, client):
        super().__init__(client)
        self.seen_values = []

    @state(
        "created",
        claim_payload=False,
        claim_values=["order"],
        value_max_bytes=1024,
        return_record=False,
    )
    def created(self, ctx: WorkflowContext):
        self.seen_values.append(ctx.value("order", local_cache=True))
        self.seen_values.append(ctx.value("order", local_cache=True))
        return transition(
            "next",
            values={"receipt": b"receipt"},
            value_refs={"profile": "profile-ref"},
            drop_values=["old"],
            override_values=["receipt"],
        )


class BatchValueWorkflow(Workflow):
    type = "batch-value"
    initial_state = "created"

    @state("created", claim_payload=False, return_record=False)
    def created(self, ctx: WorkflowContext):
        return complete(values={"receipt": b"receipt"}, override_values=["receipt"])


class PlainReturnWorkflow(Workflow):
    type = "plain-return"
    initial_state = "created"

    @state("created", claim_payload=False, return_record=False)
    def created(self, _job):
        return b"plain-result"


def test_workflow_create_uses_partition_by():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))

    workflow.create("f1", tenant_id="tenant", order_id="order", payload=b"p", now_ms=100)

    assert "tenant:order" in redis.calls[0]


def test_workflow_create_allows_custom_initial_state():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))

    workflow.create(
        "f1", tenant_id="tenant", order_id="order", state="review", payload=b"p", now_ms=100
    )

    assert redis.calls[0][redis.calls[0].index("STATE") + 1] == "review"


def test_workflow_enqueue_uses_ack_only_create_with_partition_by():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))

    workflow.enqueue("f1", tenant_id="tenant", order_id="order", payload=b"p", now_ms=100)

    assert "tenant:order" in redis.calls[0]
    assert len(redis.calls) == 1


def test_run_once_claims_and_applies_transition():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))

    results = workflow.run_once("created", worker="w1", partition_key="tenant:order")

    assert len(results) == 1
    assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
    assert redis.calls[1][0] == "FLOW.TRANSITION"
    assert redis.calls[1][1:4] == ("f1", "created", "next")


def test_run_once_dispatches_claimed_running_record_by_run_state():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = b"created"
    workflow = OrderWorkflow(FlowClient(redis))

    results = workflow.run_once("created", worker="w1", partition_key="tenant:order")

    assert len(results) == 1
    assert redis.calls[1][0] == "FLOW.TRANSITION"
    assert redis.calls[1][1:4] == ("f1", "running", "next")


def test_run_batch_once_uses_many_command_for_uniform_transitions():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = b"created"
    redis.claim_ids = [b"f1", b"f2"]
    workflow = LeanWorkflow(FlowClient(redis))

    results = workflow.run_batch_once("created", worker="w1", partition_key="tenant:order", limit=2)

    assert len(results) == 2
    assert redis.calls[1][0] == "FLOW.TRANSITION_MANY"
    assert redis.calls[1][2:4] == ("running", "next")
    assert "INDEPENDENT" in redis.calls[1]


def test_run_batch_once_uses_many_command_for_uniform_completions():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = b"done"
    redis.claim_ids = [b"f1", b"f2"]
    workflow = DoneWorkflow(FlowClient(redis))

    results = workflow.run_batch_once("done", worker="w1", partition_key="tenant:order", limit=2)

    assert len(results) == 2
    assert redis.calls[1][0] == "FLOW.COMPLETE_MANY"
    assert "INDEPENDENT" in redis.calls[1]


def test_workflow_claim_due_accepts_partition_keys():
    redis = FakeRedis()
    workflow = LeanWorkflow(FlowClient(redis))

    workflow.run_batch_once("created", worker="w1", partition_keys=["p1", "p2"], limit=2)

    assert "PARTITIONS" in redis.calls[0]
    partitions_idx = redis.calls[0].index("PARTITIONS")
    assert redis.calls[0][partitions_idx : partitions_idx + 4] == ("PARTITIONS", 2, "p1", "p2")


def test_workflow_can_claim_compact_metadata_without_full_record():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = None
    redis.claim_ids = [b"f1", b"f2"]
    workflow = CompactWorkflow(FlowClient(redis))

    results = workflow.run_batch_once("created", worker="w1", partition_key="tenant:order", limit=2)

    assert len(results) == 2
    assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
    assert redis.calls[0][-2:] == ("RETURN", "JOBS_COMPACT")
    assert redis.calls[1][0] == "FLOW.TRANSITION_MANY"
    assert redis.calls[1][2:4] == ("running", "next")


def test_blocking_workflow_worker_claims_all_states_with_compact_state_return():
    class ClaimAnyRedis(FakeRedis):
        def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "FLOW.CLAIM_DUE":
                return [
                    [b"f1", b"tenant:order", b"lease-1", 1, b"created"],
                    [b"f2", b"tenant:order", b"lease-2", 2, b"done"],
                ]
            if args[0] in {"FLOW.TRANSITION_MANY", "FLOW.COMPLETE_MANY"}:
                return [b"OK"]
            return super().execute_command(*args)

    class AnyStateWorkflow(Workflow):
        type = "order"
        initial_state = "created"

        @state("created", claim_payload=False, claim_record=False, return_record=False)
        def created(self, _job):
            return transition("done")

        @state("done", claim_payload=False, claim_record=False, return_record=False)
        def done(self, _job):
            return complete(result=b"ok")

    redis = ClaimAnyRedis()
    workflow = AnyStateWorkflow(FlowClient(redis))
    worker = WorkflowWorker(workflow, batch_size=2, block_ms=5000, apply_async_depth=0)

    result = worker.run_once()

    assert result.claimed == 2
    assert result.applied == 2
    claim = redis.calls[0]
    assert "STATE" not in claim
    assert claim[claim.index("RETURN") : claim.index("RETURN") + 2] == (
        "RETURN",
        "JOBS_COMPACT_STATE",
    )
    assert redis.calls[1][0] == "FLOW.TRANSITION_MANY"
    assert redis.calls[2][0] == "FLOW.COMPLETE_MANY"


def test_workflow_context_value_many_preserves_present_none_values():
    workflow = OrderWorkflow(FlowClient(FakeRedis()))
    job = FlowRecord(
        id="f1",
        type="order",
        state="created",
        partition_key="tenant:order",
        lease_token=b"lease",
        fencing_token=1,
        values={"optional": None},
    )
    ctx = workflow.context(job, "created")

    assert ctx.value_many(["optional", "missing"]) == {"optional": None}


def test_workflow_context_value_many_batches_value_refs():
    class ValueRefRedis(FakeRedis):
        def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "FLOW.VALUE.MGET":
                return [b"one", b"two"]
            return super().execute_command(*args)

    redis = ValueRefRedis()
    workflow = ValueWorkflow(FlowClient(redis))
    job = FlowRecord(
        id="f1",
        type="value-order",
        state="created",
        partition_key="tenant:order",
        value_refs={"a": {"ref": "ref-a"}, "b": {"ref": "ref-b"}},
    )
    ctx = WorkflowContext(workflow, job, "created")

    assert ctx.value_many(["a", "b"], local_cache=True) == {"a": b"one", "b": b"two"}

    mget_calls = [call for call in redis.calls if call[0] == "FLOW.VALUE.MGET"]
    assert mget_calls == [("FLOW.VALUE.MGET", "ref-a", "ref-b", "MAX_BYTES", 1024)]


def test_workflow_outcomes_propagate_priority_and_terminal_ttl_options():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))
    job = FlowRecord(
        id="f1",
        type="order",
        state="created",
        partition_key="tenant:order",
        lease_token=b"lease",
        fencing_token=1,
    )

    workflow.apply(job, transition("next", priority=7), state_name="created")
    workflow.apply(job, complete(result=b"done", ttl_ms=123), state_name="done")
    workflow.apply(job, fail(error=b"bad", ttl_ms=456), state_name="done")

    assert redis.calls[0][0] == "FLOW.TRANSITION"
    assert redis.calls[0][redis.calls[0].index("PRIORITY") + 1] == 7
    assert redis.calls[1][0] == "FLOW.COMPLETE"
    assert redis.calls[1][redis.calls[1].index("TTL") + 1] == 123
    assert redis.calls[2][0] == "FLOW.FAIL"
    assert redis.calls[2][redis.calls[2].index("TTL") + 1] == 456


def test_handle_claimed_batch_count_uses_many_without_materializing_results():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = None
    redis.claim_ids = [b"f1", b"f2"]
    workflow = CompactWorkflow(FlowClient(redis))

    jobs = workflow.claim_due("created", worker="w1", partition_key="tenant:order", limit=2)
    count = workflow.handle_claimed_batch_count("created", jobs)

    assert count == 2
    assert redis.calls[1][0] == "FLOW.TRANSITION_MANY"
    assert redis.calls[1][2:4] == ("running", "next")


def test_handle_claimed_batch_count_chunks_many_commands_at_server_limit():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = None
    redis.claim_ids = [f"f{idx}".encode() for idx in range(1001)]
    workflow = CompactWorkflow(FlowClient(redis))

    jobs = workflow.claim_due("created", worker="w1", partition_key="tenant:order", limit=1001)
    count = workflow.handle_claimed_batch_count("created", jobs)

    transition_calls = [call for call in redis.calls if call[0] == "FLOW.TRANSITION_MANY"]
    assert count == 1001
    assert len(transition_calls) == 2


def test_workflow_worker_runs_compact_batch_without_materializing_records():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_ids = [b"f1", b"f2"]
    workflow = CompactWorkflow(FlowClient(redis))
    worker = WorkflowWorker(
        workflow,
        state="created",
        worker="w1",
        partition_key="tenant:order",
        batch_size=2,
        apply_async_depth=0,
    )

    result = worker.run_once()

    assert result.claimed == 2
    assert result.applied == 2
    assert result.claim_calls == 1
    return_idx = redis.calls[0].index("RETURN")
    assert redis.calls[0][return_idx : return_idx + 2] == ("RETURN", "JOBS_COMPACT")
    assert redis.calls[1][0] == "FLOW.TRANSITION_MANY"


def test_workflow_worker_can_apply_batches_async_and_flush():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_ids = [b"f1", b"f2"]
    workflow = CompactWorkflow(FlowClient(redis))
    worker = workflow.worker(
        state="created",
        worker="w1",
        partition_key="tenant:order",
        batch_size=2,
        apply_async_depth=1,
    )

    result = worker.run_once()
    flushed = worker.close()

    assert result.claimed == 2
    assert result.applied == 0
    assert flushed.applied == 2
    assert redis.calls[1][0] == "FLOW.TRANSITION_MANY"


def test_workflow_worker_cycles_configured_states():
    redis = FakeRedis()
    redis.claim_ids = []
    workflow = OrderWorkflow(FlowClient(redis))
    worker = WorkflowWorker(
        workflow,
        states=["created", "done"],
        worker="w1",
        partition_key="tenant:order",
        apply_async_depth=0,
    )

    first = worker.run_once()
    second = worker.run_once()

    assert first.empty_claims == 1
    assert second.empty_claims == 1
    first_state_idx = redis.calls[0].index("STATE")
    second_state_idx = redis.calls[1].index("STATE")
    assert redis.calls[0][first_state_idx + 1] == "created"
    assert redis.calls[1][second_state_idx + 1] == "done"


def test_polling_worker_rejects_invalid_limit_and_empty_states():
    workflow = OrderWorkflow(FlowClient(FakeRedis()))

    with pytest.raises(ValueError, match="limit"):
        Worker(workflow, worker="w1", limit=0)

    with pytest.raises(ValueError, match="states"):
        Worker(workflow, worker="w1", states=[])


def test_workflow_worker_rotates_partition_keys():
    redis = FakeRedis()
    redis.claim_ids = []
    workflow = CompactWorkflow(FlowClient(redis))
    worker = WorkflowWorker(
        workflow,
        state="created",
        worker="w1",
        partition_keys=["p1", "p2"],
        claim_partition_batch_size=1,
        apply_async_depth=0,
    )

    worker.run_once()
    worker.run_once()

    first_partition_idx = redis.calls[0].index("PARTITION")
    second_partition_idx = redis.calls[1].index("PARTITION")
    assert redis.calls[0][first_partition_idx + 1] == "p1"
    assert redis.calls[1][second_partition_idx + 1] == "p2"


def test_workflow_worker_batches_partition_keys_by_default():
    redis = FakeRedis()
    redis.claim_ids = []
    workflow = CompactWorkflow(FlowClient(redis))
    worker = WorkflowWorker(
        workflow,
        state="created",
        worker="w1",
        partition_keys=["p1", "p2", "p3"],
        apply_async_depth=0,
    )

    worker.run_once()

    assert "PARTITION" not in redis.calls[0]
    partitions_idx = redis.calls[0].index("PARTITIONS")
    assert redis.calls[0][partitions_idx : partitions_idx + 5] == (
        "PARTITIONS",
        3,
        "p1",
        "p2",
        "p3",
    )


def test_workflow_worker_cycles_all_state_partition_pairs():
    redis = FakeRedis()
    redis.claim_ids = []
    workflow = OrderWorkflow(FlowClient(redis))
    worker = WorkflowWorker(
        workflow,
        states=["created", "done"],
        worker="w1",
        partition_keys=["p1", "p2"],
        claim_partition_batch_size=1,
        apply_async_depth=0,
    )

    for _ in range(4):
        worker.run_once()

    pairs = []
    for call in redis.calls:
        state_idx = call.index("STATE")
        partition_idx = call.index("PARTITION")
        pairs.append((call[state_idx + 1], call[partition_idx + 1]))

    assert pairs == [
        ("created", "p1"),
        ("created", "p2"),
        ("done", "p1"),
        ("done", "p2"),
    ]


def test_state_config_controls_claim_payload_and_mutation_return():
    redis = FakeRedis()
    workflow = LeanWorkflow(FlowClient(redis))

    results = workflow.run_once("created", worker="w1", partition_key="tenant:order", priority=0)

    assert len(results) == 1
    claim = redis.calls[0]
    assert claim[:10] == (
        "FLOW.CLAIM_DUE",
        "lean",
        "STATE",
        "created",
        "WORKER",
        "w1",
        "LEASE_MS",
        30000,
        "LIMIT",
        1,
    )
    assert "NOW" not in claim
    assert claim[10:] == ("PARTITION", "tenant:order", "PRIORITY", 0, "PAYLOAD", "false")
    assert redis.calls[1][0] == "FLOW.TRANSITION"
    assert "PAYLOAD" not in redis.calls[1]
    assert len(redis.calls) == 2


def test_workflow_handler_receives_context_and_can_enqueue_child_flow():
    redis = FakeRedis()
    workflow = ContextWorkflow(FlowClient(redis))

    results = workflow.run_once("created", worker="w1", partition_key="tenant:order")

    assert len(results) == 1
    assert workflow.seen_contexts == [
        {
            "id": "f1",
            "type": "order",
            "state": "created",
            "logical_state": "created",
            "partition_key": "tenant:order",
            "lease_token": b"lease-1",
            "fencing_token": 1,
        }
    ]
    assert redis.calls[1][0] == "FLOW.CREATE"
    assert redis.calls[1][1] == "child-1"
    assert redis.calls[1][redis.calls[1].index("TYPE") + 1] == "child"
    assert redis.calls[1][redis.calls[1].index("STATE") + 1] == "queued"
    assert "PARTITION" in redis.calls[1]
    assert redis.calls[1][redis.calls[1].index("PARTITION") + 1] == "tenant:order"
    assert redis.calls[2][0] == "FLOW.COMPLETE"


def test_workflow_context_flow_lookup_defaults_to_current_flow_and_partition():
    redis = FakeRedis()
    workflow = ContextLookupWorkflow(FlowClient(redis))

    workflow.run_once("created", worker="w1", partition_key="tenant:order")

    assert redis.calls[1][0] == "FLOW.GET"
    assert redis.calls[1][1] == "f1"
    assert redis.calls[1][redis.calls[1].index("PARTITION") + 1] == "tenant:order"
    assert redis.calls[2][0] == "FLOW.HISTORY"
    assert redis.calls[2][1] == "f1"
    assert redis.calls[2][redis.calls[2].index("PARTITION") + 1] == "tenant:order"


def test_workflow_context_flow_spawn_children_inherits_current_claim_tokens():
    redis = FakeRedis()
    workflow = ContextChildrenWorkflow(FlowClient(redis))

    workflow.run_once("created", worker="w1", partition_key="tenant:order")

    assert redis.calls[1][0] == "FLOW.SPAWN_CHILDREN"
    assert redis.calls[1][1] == "f1"
    assert "PARTITION" in redis.calls[1]
    assert redis.calls[1][redis.calls[1].index("PARTITION") + 1] == "tenant:order"
    assert redis.calls[1][redis.calls[1].index("LEASE_TOKEN") + 1] == b"lease-1"
    assert redis.calls[1][redis.calls[1].index("FENCING") + 1] == 1


def test_workflow_claims_named_values_and_applies_named_outcome_mutations():
    redis = FakeRedis()
    workflow = ValueWorkflow(FlowClient(redis))

    workflow.run_once("created", worker="w1", partition_key="tenant:order")

    claim = redis.calls[0]
    assert claim[claim.index("VALUE") : claim.index("VALUE") + 2] == ("VALUE", "order")
    assert claim[claim.index("VALUE_MAX_BYTES") : claim.index("VALUE_MAX_BYTES") + 2] == (
        "VALUE_MAX_BYTES",
        1024,
    )
    assert workflow.seen_values == [b"order-bytes", b"order-bytes"]

    mutation = redis.calls[1]
    assert mutation[0] == "FLOW.TRANSITION"
    assert mutation[mutation.index("VALUE") : mutation.index("VALUE") + 3] == (
        "VALUE",
        "receipt",
        b"receipt",
    )
    assert mutation[mutation.index("VALUE_REF") : mutation.index("VALUE_REF") + 3] == (
        "VALUE_REF",
        "profile",
        "profile-ref",
    )
    assert mutation[mutation.index("DROP_VALUE") : mutation.index("DROP_VALUE") + 2] == (
        "DROP_VALUE",
        "old",
    )
    assert mutation[mutation.index("OVERRIDE_VALUE") : mutation.index("OVERRIDE_VALUE") + 2] == (
        "OVERRIDE_VALUE",
        "receipt",
    )


def test_workflow_batch_outcomes_forward_named_values_to_many_command():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = b"created"
    redis.claim_ids = [b"f1", b"f2"]
    workflow = BatchValueWorkflow(FlowClient(redis))

    workflow.run_batch_once("created", worker="w1", partition_key="tenant:order", limit=2)

    mutation = redis.calls[1]
    assert mutation[0] == "FLOW.COMPLETE_MANY"
    assert mutation[mutation.index("VALUE") : mutation.index("VALUE") + 3] == (
        "VALUE",
        "receipt",
        b"receipt",
    )
    assert mutation[mutation.index("OVERRIDE_VALUE") : mutation.index("OVERRIDE_VALUE") + 2] == (
        "OVERRIDE_VALUE",
        "receipt",
    )


def test_workflow_batch_plain_return_completes_with_result():
    redis = FakeRedis()
    redis.claim_state = b"running"
    redis.claim_run_state = b"created"
    redis.claim_ids = [b"f1", b"f2"]
    workflow = PlainReturnWorkflow(FlowClient(redis))

    results = workflow.run_batch_once("created", worker="w1", partition_key="tenant:order", limit=2)

    assert len(results) == 2
    complete_call = redis.calls[1]
    assert complete_call[0] == "FLOW.COMPLETE_MANY"
    assert complete_call[complete_call.index("RESULT") + 1] == b"plain-result"


def test_workflow_worker_start_stop_join_tracks_stats():
    worker_ref = {}

    class StoppingWorkflow(Workflow):
        type = "order"
        initial_state = "created"

        @state("created", claim_payload=False, return_record=False)
        def created(self, job):
            worker_ref["worker"].stop()
            return complete(result=b"done")

    redis = FakeRedis()
    workflow = StoppingWorkflow(FlowClient(redis))
    worker = WorkflowWorker(
        workflow,
        state="created",
        batch_size=1,
        idle_sleep_s=0.001,
        apply_async_depth=0,
    )
    worker_ref["worker"] = worker

    worker.start()
    stats = worker.join(timeout=1)

    assert stats.claimed == 1
    assert stats.applied == 1
    assert worker.is_running is False


def test_workflow_on_error_raise_propagates():
    class RaisingWorkflow(Workflow):
        type = "order"
        initial_state = "created"

        @state("created", on_error="raise", claim_payload=False, return_record=False)
        def created(self, _job):
            raise RuntimeError("boom")

    redis = FakeRedis()
    workflow = RaisingWorkflow(FlowClient(redis))

    with pytest.raises(RuntimeError, match="boom"):
        workflow.run_once("created", worker="w1", partition_key="tenant:order")

    assert len(redis.calls) == 1


def test_workflow_worker_resets_running_after_loop_exception():
    class RaisingWorkflow(Workflow):
        type = "order"
        initial_state = "created"

        @state("created", on_error="raise", claim_payload=False, return_record=False)
        def created(self, _job):
            raise RuntimeError("boom")

    worker = WorkflowWorker(
        RaisingWorkflow(FlowClient(FakeRedis())),
        state="created",
        worker="w1",
        partition_key="tenant:order",
        idle_sleep_s=0,
        apply_async_depth=0,
    )

    with pytest.raises(RuntimeError, match="boom"):
        worker._run_loop()

    assert worker.is_running is False


def test_state_defaults_to_ack_only_and_retry_exception_policy():
    class DefaultWorkflow(Workflow):
        type = "default"
        initial_state = "created"

        @state("created")
        def created(self, job):
            return transition("done")

    workflow = DefaultWorkflow(FlowClient(FakeRedis()))
    config = workflow._states["created"]

    assert config.return_record is False
    assert config.on_error == "retry"


def test_state_accepts_exception_policy_enum():
    class EnumPolicyWorkflow(Workflow):
        type = "default"
        initial_state = "created"

        @state("created", exception_policy=ExceptionPolicy.RAISE)
        def created(self, job):
            return transition("done")

    workflow = EnumPolicyWorkflow(FlowClient(FakeRedis()))

    assert workflow._states["created"].on_error == "raise"


def test_state_rejects_exception_policy_and_on_error_together():
    with pytest.raises(ValueError, match="mutually exclusive"):

        class BadWorkflow(Workflow):
            type = "bad"
            initial_state = "created"

            @state("created", exception_policy=ExceptionPolicy.RETRY, on_error="fail")
            def created(self, job):
                return transition("done")


def test_flow_workflow_constructor_registers_state_handlers_and_partition_by():
    redis = FakeRedis()
    workflow = FlowWorkflow(
        FlowClient(redis),
        type="order",
        initial_state="created",
        partition_by=("tenant_id", "order_id"),
    )

    @workflow.state("created", exception_policy=ExceptionPolicy.FAIL)
    def created(job):
        return transition("done")

    workflow.create("f1", tenant_id="tenant-a", order_id="order-1", payload=b"p", now_ms=100)

    assert workflow._states["created"].on_error == "fail"
    assert redis.calls[0][redis.calls[0].index("PARTITION") + 1] == "tenant-a:order-1"


def test_flow_workflow_on_alias_registers_state_handler():
    workflow = FlowWorkflow(FlowClient(FakeRedis()), type="order", initial_state="created")

    @workflow.on("created")
    def created(job):
        return transition("done")

    assert "created" in workflow._states
    assert "created" in workflow._handlers


def test_workflow_client_creates_workflow_and_delegates_flow_commands():
    redis = FakeRedis()
    client = WorkflowClient(FlowClient(redis))
    workflow = client.workflow(
        type="order",
        initial_state="created",
        partition_by=("tenant_id", "order_id"),
    )

    @workflow.state("created")
    def created(job):
        return transition("done")

    workflow.start("f1", tenant_id="tenant-a", order_id="order-1", payload=b"p", now_ms=100)
    client.command("PING")

    assert redis.calls[0][0] == "FLOW.CREATE"
    assert redis.calls[0][redis.calls[0].index("PARTITION") + 1] == "tenant-a:order-1"
    assert redis.calls[1] == ("PING",)


def test_workflow_client_retry_policy_is_inherited_and_state_can_override():
    redis = FakeRedis()
    default_policy = RetryPolicy(max_retries=5, backoff="exponential", base_ms=200)
    state_policy = RetryPolicy(max_retries=2, backoff="fixed", base_ms=50)
    client = WorkflowClient(FlowClient(redis), retry_policy=default_policy)
    workflow = client.workflow(type="order", initial_state="created")

    @workflow.state("created", retry_policy=state_policy)
    def created(job):
        return transition("done")

    workflow.install_policy()

    call = redis.calls[-1]
    assert call[:2] == ("FLOW.POLICY.SET", "order")
    assert "STATE" in call
    assert "created" in call
    assert default_policy.max_retries in call
    assert state_policy.max_retries in call


def test_state_rejects_retry_policy_and_retry_alias_together():
    with pytest.raises(ValueError, match="mutually exclusive"):
        state(
            "created",
            retry_policy=RetryPolicy(max_retries=1),
            retry=RetryPolicy(max_retries=2),
        )(lambda job: transition("done"))


def test_workflow_client_worker_and_value_config_are_inherited_and_overridable():
    client = WorkflowClient(
        FlowClient(FakeRedis()),
        worker_config=WorkerConfig(
            batch_size=50,
            idle_sleep_s=0.01,
            apply_async_depth=0,
            exception_policy=ExceptionPolicy.FAIL,
        ),
        value_config=ValueConfig(value_max_bytes=64_000, local_cache=True),
    )
    workflow = client.workflow(type="order", initial_state="created")

    @workflow.state("created", claim_values=["order"])
    def created(job):
        return transition("done")

    worker = workflow.worker(batch_size=10)

    assert worker.batch_size == 10
    assert worker.idle_sleep_s == 0.01
    assert worker.apply_async_depth == 0
    assert workflow._states["created"].on_error == "fail"
    assert workflow._states["created"].value_max_bytes == 64_000
    assert workflow.value_config.local_cache is True


def test_workflow_client_from_url_creates_separate_claim_pool(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = FlowClient(FakeRedis())
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.workflow.FlowClient.from_url", staticmethod(from_url))

    client = WorkflowClient.from_url(
        "redis://example/0",
        worker_config=WorkerConfig(workers=4),
    )
    workflow = client.workflow(type="order", initial_state="created")

    @workflow.state("created")
    def created(job):
        return transition("done")

    jobs = workflow.claim_due("created", worker="w1", block_ms=5000)

    assert [(url, kwargs) for url, kwargs, _client in calls] == [
        ("redis://example/0", {"max_connections": 4}),
        ("redis://example/0", {"max_connections": 4}),
    ]
    assert jobs
    assert workflow.client is client.flow
    assert workflow.claim_client is client.claim_flow
    assert client.flow.executor._executor.calls == []
    assert client.claim_flow.executor._executor.calls[0][0] == "FLOW.CLAIM_DUE"


def test_workflow_worker_config_at_workflow_time_resizes_claim_pool(monkeypatch):
    calls = []

    def from_url(url, **kwargs):
        client = FlowClient(FakeRedis())
        client.url = url
        client.kwargs = kwargs
        calls.append((url, kwargs, client))
        return client

    monkeypatch.setattr("ferricstore.workflow.FlowClient.from_url", staticmethod(from_url))

    client = WorkflowClient.from_url("redis://example/0")
    workflow = client.workflow(
        type="order",
        initial_state="created",
        worker_config=WorkerConfig(workers=16),
    )

    assert calls[0][1]["max_connections"] == 2
    assert calls[1][1]["max_connections"] == 1
    assert calls[2][1]["max_connections"] == 16
    assert workflow.client is client.flow
    assert workflow.claim_client is calls[2][2]


def test_workflow_client_close_does_not_close_externally_owned_clients():
    flow_redis = FakeRedis()
    claim_redis = FakeRedis()
    client = WorkflowClient(
        FlowClient(flow_redis),
        claim_client=FlowClient(claim_redis),
    )

    client.close()

    assert flow_redis.closed is False
    assert claim_redis.closed is False


def test_class_workflow_worker_config_exception_policy_is_inherited():
    class ConfiguredWorkflow(Workflow):
        type = "configured"
        initial_state = "created"

        @state("created")
        def created(self, job):
            return transition("done")

    workflow = ConfiguredWorkflow(
        FlowClient(FakeRedis()),
        worker_config=WorkerConfig(exception_policy=ExceptionPolicy.FAIL),
    )

    assert workflow._states["created"].on_error == "fail"
