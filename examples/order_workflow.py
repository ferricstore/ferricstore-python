from ferricstore import FlowClient, Workflow, complete, state, transition


class OrderWorkflow(Workflow):
    type = "order"
    initial_state = "created"
    partition_by = ("tenant_id", "order_id")

    @state("created", lease_ms=30_000, claim_payload=True, on_error="fail")
    def created(self, job):
        return transition("charged", payload=job.payload)

    @state("charged", lease_ms=30_000, claim_payload=True, on_error="fail")
    def charged(self, job):
        return complete(result=b"ok")


if __name__ == "__main__":
    client = FlowClient.from_url("redis://127.0.0.1:6379/0")
    workflow = OrderWorkflow(client)
    record = workflow.create(
        "order-1",
        tenant_id="tenant-a",
        order_id="order-1",
        payload=b"order payload",
    )
    workflow.run_once("created", worker="worker-1", partition_key=record.partition_key)
    workflow.run_once("charged", worker="worker-1", partition_key=record.partition_key)
    print(client.get("order-1", partition_key=record.partition_key))

