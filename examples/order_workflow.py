from ferricstore import WorkflowClient, complete, transition

client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
order = client.workflow(
    type="order",
    initial_state="created",
    partition_by=("tenant_id", "order_id"),
)


@order.state("created", lease_ms=30_000, claim_payload=True)
def created(job):
    return transition("charged", payload=job.payload)


@order.state("charged", lease_ms=30_000, claim_payload=True)
def charged(job):
    return complete(result=b"ok")


if __name__ == "__main__":
    partition_key = "tenant-a:order-1"
    order.start(
        "order-1",
        tenant_id="tenant-a",
        order_id="order-1",
        payload=b"order payload",
    )
    order.run_once("created", worker="worker-1", partition_key=partition_key)
    order.run_once("charged", worker="worker-1", partition_key=partition_key)
    print(client.get("order-1", partition_key=partition_key))
