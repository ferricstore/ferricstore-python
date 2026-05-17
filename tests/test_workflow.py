from ferricstore import Complete, FlowClient, Workflow, complete, state, transition


class FakeRedis:
    def __init__(self):
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        if args[0] == "FLOW.CLAIM_DUE":
            return [
                {
                    b"id": b"f1",
                    b"type": b"order",
                    b"state": b"created",
                    b"partition_key": b"tenant:order",
                    b"lease_token": b"lease",
                    b"fencing_token": 1,
                }
            ]
        return {
            b"id": b"f1",
            b"type": b"order",
            b"state": b"next",
            b"partition_key": b"tenant:order",
        }


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


def test_workflow_create_uses_partition_by():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))

    workflow.create("f1", tenant_id="tenant", order_id="order", payload=b"p", now_ms=100)

    assert "tenant:order" in redis.calls[0]


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


def test_state_config_controls_claim_payload_and_mutation_return():
    redis = FakeRedis()
    workflow = LeanWorkflow(FlowClient(redis))

    results = workflow.run_once("created", worker="w1", partition_key="tenant:order", priority=0)

    assert len(results) == 1
    claim = redis.calls[0]
    assert claim[:11] == (
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
        "NOW",
    )
    assert claim[12:] == ("PARTITION", "tenant:order", "PRIORITY", 0, "PAYLOAD", "false")
    assert redis.calls[1][0] == "FLOW.TRANSITION"
    assert "PAYLOAD" not in redis.calls[1]
    assert len(redis.calls) == 2
