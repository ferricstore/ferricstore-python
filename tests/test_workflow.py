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


def test_workflow_create_uses_partition_by():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))

    workflow.create("f1", tenant_id="tenant", order_id="order", payload=b"p", now_ms=100)

    assert "tenant:order" in redis.calls[0]


def test_run_once_claims_and_applies_transition():
    redis = FakeRedis()
    workflow = OrderWorkflow(FlowClient(redis))

    results = workflow.run_once("created", worker="w1", partition_key="tenant:order")

    assert len(results) == 1
    assert redis.calls[0][0] == "FLOW.CLAIM_DUE"
    assert redis.calls[1][0] == "FLOW.TRANSITION"
    assert redis.calls[1][1:4] == ("f1", "created", "next")

