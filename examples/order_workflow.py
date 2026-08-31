from ferricstore import WorkflowClient, complete, transition

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
order = client.workflow(
    type="order",
    initial_state="created",
    partition_by=("tenant_id", "order_id"),
)


def charge_card(payload: bytes, *, idempotency_key: str) -> bytes:
    print(f"charge {payload!r} with idempotency key {idempotency_key}")
    return b"charge-accepted"


@order.state("created", lease_ms=30_000, claim_payload=True)
def created(ctx):
    charge = ctx.step(
        name="charge-customer:v1",
        run=lambda: charge_card(
            ctx.payload,
            idempotency_key=f"{ctx.id}:charge-customer:v1",
        ),
        to_state="charge_recorded",
    )
    return transition("receipt_pending", payload=charge)


@order.state("receipt_pending", lease_ms=30_000, claim_payload=True)
def receipt_pending(job):
    return complete(result=b"ok")


if __name__ == "__main__":
    started = order.start(
        "order-1",
        tenant_id="tenant-a",
        order_id="order-1",
        payload=b"order payload",
    )
    partition_key = started.partition_key
    order.run_once("created", worker="worker-1", partition_key=partition_key)
    order.run_once("receipt_pending", worker="worker-1", partition_key=partition_key)
    print(client.get("order-1", partition_key=partition_key))
