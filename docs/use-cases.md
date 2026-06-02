# Use Case Examples

This page shows production-style ways to use FerricFlow from Python.

Examples use `WorkflowClient` for state machines and `QueueClient` for queue
workloads. Both clients expose lower-level helpers like `value_put`, `signal`,
`history`, and `command(...)` when a handler or endpoint needs them.

The examples intentionally use explicit states, idempotency keys, named values,
and value refs. That is the shape you want in real systems: small hot state,
large data loaded only when needed, and durable boundaries that are easy to
debug.

## Value-ref best practices

Use these rules across all examples:

| Rule | Why |
| --- | --- |
| Keep `payload` small | It is the current state payload and is often on the hot path. |
| Put large data in named values | Different states can hydrate only what they need. |
| Use `value_put` for reusable bytes | Avoid storing the same MB-scale data repeatedly. |
| Use stable `owner_flow_id` and `name` | Makes value writes idempotent per flow. |
| Keep `override=False` by default | Prevents accidental replacement on retries. |
| Use `override=True` only for intentional replacement | For example, regenerating a draft/report. |

Example:

```python
from ferricstore import WorkflowClient


client = WorkflowClient.from_url(url)

meta = client.value_put(
    b"...large invoice PDF...",
    owner_flow_id="order-1",
    name="invoice_pdf",
    partition_key="tenant-a:order-1",
    override=False,
)

client.transition(
    job.id,
    from_state=job.state,
    to_state="email_invoice",
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    value_refs={"invoice_pdf": meta["ref"]},
)
```

For transition/complete/retry/fail value writes, do not pass
`override_values=[...]` for normal first-write step outputs. Let unexpected
duplicate writes surface during testing. Use `override_values` only when the
state intentionally regenerates that named value.

Fetch later:

```python
record = client.get(
    "order-1",
    partition_key="tenant-a:order-1",
    values=["invoice_pdf"],
)
```

In a workflow handler:

```python
invoice = job.value("invoice_pdf")
```

Advanced: `job.value("invoice_pdf", local_cache=True)` caches the value only for
the current handler invocation. Use it when one handler reads the same value more
than once and the value is safe to keep in memory briefly. The default is
`False`.

## Saga: order payment, inventory, shipment

Use a workflow saga when you need visible durable steps and compensation.

```python
from ferricstore import WorkflowClient, complete, fail, retry, transition


client = WorkflowClient.from_url(url)
order_saga = client.workflow(
    type="order_saga",
    initial_state="reserve_inventory",
    partition_by=("tenant_id", "order_id"),
)

@order_saga.state("reserve_inventory", claim_values=["order"])
def reserve_inventory(job):
    order = decode_order(job.value("order"))
    reservation_id = inventory.reserve(order, idempotency_key=f"{job.id}:reserve")

    return transition(
        "charge_payment",
        values={"reservation": reservation_id.encode()},
    )

@order_saga.state("charge_payment", claim_values=["order", "reservation"])
def charge_payment(job):
    order = decode_order(job.value("order"))
    charge_id = payments.charge(order, idempotency_key=f"{job.id}:charge")

    return transition(
        "ship_order",
        values={"charge": charge_id.encode()},
    )

@order_saga.state("ship_order", claim_values=["order", "reservation", "charge"])
def ship_order(job):
    order = decode_order(job.value("order"))
    shipment_id = shipping.create_label(order, idempotency_key=f"{job.id}:ship")

    return complete(result=shipment_id.encode(), ttl_ms=30 * 86_400_000)

@order_saga.state("compensate_payment", claim_values=["charge"])
def compensate_payment(job):
    charge_id = job.value("charge")
    if charge_id:
        payments.refund(charge_id.decode(), idempotency_key=f"{job.id}:refund")
    return transition("compensate_inventory")

@order_saga.state("compensate_inventory", claim_values=["reservation"])
def compensate_inventory(job):
    reservation_id = job.value("reservation")
    if reservation_id:
        inventory.release(reservation_id.decode(), idempotency_key=f"{job.id}:release")
    return fail(error=b"order compensated", ttl_ms=30 * 86_400_000)
```

Producer:

```python
order_saga.start(
    "order-123",
    tenant_id="tenant-a",
    order_id="order-123",
    payload=b"small routing payload",
    values={"order": encode_order(order)},
)
```

Best practices:

- Use `partition_by` for order-level ordering.
- Use external idempotency keys for every side effect.
- Store step outputs as named values: `reservation`, `charge`, `shipment`.
- Do not override saga step outputs by default; duplicate writes should reveal a
  retry/idempotency bug unless the value is intentionally regenerated.
- Use compensation states instead of hiding compensation inside a catch block.

## IoT fanout: command many devices

Use FerricFlow for durable orchestration and an MQTT/AWS IoT layer for the device
network. FerricFlow tracks command state, retries, timeouts, and audit history.

Parent workflow:

```python
from ferricstore import ChildSpec, WorkflowClient, complete, transition


client = WorkflowClient.from_url(url)
rollout = client.workflow(
    type="firmware_rollout",
    initial_state="fanout",
    partition_by=("tenant_id", "rollout_id"),
)

@rollout.state("fanout", claim_values=["manifest"])
def fanout(job):
    manifest = job.value("manifest")
    devices = load_target_devices(job.id)

    manifest_ref = job.value_refs["manifest"]["ref"]
    children = [
        ChildSpec(
            id=f"{job.id}:device:{device.id}",
            type="device_command",
            partition_key=f"device:{device.id}",
            payload=device.id.encode(),
            value_refs={"manifest": manifest_ref},
        )
        for device in devices
    ]

    job.flow.spawn_children(
        children,
        wait="all",
        wait_state="waiting_devices",
        success="completed",
        failure="device_failed",
        from_state=job.state,
    )

    return transition("waiting_devices")

@rollout.state("completed")
def completed(job):
    return complete(result=b"rollout complete")
```

Device-command workflow:

```python
device_command = client.workflow(type="device_command", initial_state="send")

@device_command.state("send", claim_values=["manifest"])
def send(job):
    device_id = job.payload.decode()
    manifest = job.value("manifest")

    mqtt.publish(
        topic=f"devices/{device_id}/commands",
        payload=build_command(manifest),
        qos=1,
        message_id=job.id,
    )

    return transition("waiting_ack", run_at_ms=now_ms() + 60_000)

@device_command.state("waiting_ack")
def waiting_ack(job):
    return retry(error=b"device ack timeout", run_at_ms=now_ms() + 60_000)
```

ACK webhook:

```python
client.signal(
    f"{rollout_id}:device:{device_id}",
    partition_key=f"device:{device_id}",
    signal="device_ack",
    if_state="waiting_ack",
    transition_to="acked",
    values={"ack": ack_payload},
)
```

Best practices:

- Use device id as partition key for per-device ordering.
- Put large firmware/manifest bytes in one named value or value ref.
- Child flows reference the same manifest instead of copying it.
- Use device ACKs as `signal` calls.
- Keep the actual network transport outside FerricFlow.
- Use timeout states for devices that never ACK.

## AI orchestration: prompt, tools, review, final answer

Use FerricFlow for durable AI pipelines where prompts, tool outputs, and human
review need auditability.

```python
from ferricstore import WorkflowClient, complete, retry, transition

client = WorkflowClient.from_url(url)
ai_run = client.workflow(
    type="ai_run",
    initial_state="plan",
    partition_by=("tenant_id", "run_id"),
)

@ai_run.state("plan", claim_values=["input"])
def plan(job):
    user_input = job.value("input")
    plan = llm.plan(user_input, idempotency_key=f"{job.id}:plan")

    return transition(
        "run_tools",
        values={"plan": plan},
    )

@ai_run.state("run_tools", claim_values=["plan"])
def run_tools(job):
    plan = decode_plan(job.value("plan"))
    tool_outputs = execute_tools(plan, idempotency_key=f"{job.id}:tools")

    # This state intentionally regenerates tool output when retried after
    # tool-version changes. Keep override=False if retries must preserve the
    # first tool output exactly.
    tool_ref = job.flow.value_put(
        encode_tool_outputs(tool_outputs),
        owner_flow_id=job.id,
        name="tool_outputs",
        partition_key=job.partition_key,
        override=True,
    )

    return transition(
        "draft_answer",
        value_refs={"tool_outputs": tool_ref["ref"]},
    )

@ai_run.state("draft_answer", claim_values=["input", "plan", "tool_outputs"])
def draft_answer(job):
    draft = llm.answer(
        input=job.value("input"),
        plan=job.value("plan"),
        tools=job.value("tool_outputs"),
        idempotency_key=f"{job.id}:draft",
    )

    return transition("human_review", values={"draft": draft})

@ai_run.state("human_review")
def human_review(job):
    return retry(error=b"waiting for human review", run_at_ms=now_ms() + 300_000)

@ai_run.state("approved", claim_values=["draft"])
def approved(job):
    return complete(result=job.value("draft"))
```

Human review callback:

```python
client.signal(
    "run-123",
    partition_key="tenant-a:run-123",
    signal="approved",
    if_state="human_review",
    transition_to="approved",
    values={"reviewer": b"alice"},
)
```

Best practices:

- Store large tool outputs as value refs.
- Store final answer/result separately from intermediate tool data.
- Use idempotency keys for LLM/tool calls if the provider supports them.
- Use `override=True` / `override_values` only for values the workflow
  intentionally regenerates, such as a draft after model/tool changes.
- Make human review a durable state, not a local blocking wait.
- Use `history` and named values for audit/debug.

## Batch document processing

Use one parent flow for the batch and one child per document.

```python
from ferricstore import ChildSpec, WorkflowClient, complete, transition

client = WorkflowClient.from_url(url)
document_batch = client.workflow(type="document_batch", initial_state="split")

@document_batch.state("split", claim_values=["archive"])
def split(job):
    archive = job.value("archive")
    docs = extract_documents(archive)

    children = []
    for doc in docs:
        ref = job.flow.value_put(
            doc.bytes,
            owner_flow_id=job.id,
            name=f"doc:{doc.id}",
            partition_key=job.partition_key,
            override=False,
        )
        children.append(
            ChildSpec(
                id=f"{job.id}:doc:{doc.id}",
                type="document_task",
                partition_key=f"doc:{doc.id}",
                value_refs={"document": ref["ref"]},
            )
        )

    job.flow.spawn_children(children, wait="all", wait_state="waiting", success="merge")
    return transition("waiting")

@document_batch.state("merge")
def merge(job):
    return complete(result=b"batch done")
```

Best practices:

- Keep each document as a named value/ref.
- Partition child work by document id.
- Use the parent history to audit split/merge.
- Do not put the whole archive in every child payload.

## Human approval workflow

```python
from ferricstore import WorkflowClient, complete, fail, retry

client = WorkflowClient.from_url(url)
approval = client.workflow(type="approval", initial_state="waiting")

@approval.state("waiting")
def waiting(job):
    notify_approver(job.id)
    return retry(error=b"waiting approval", run_at_ms=now_ms() + 86_400_000)

@approval.state("approved")
def approved(job):
    return complete(result=b"approved")

@approval.state("rejected")
def rejected(job):
    return fail(error=b"rejected")
```

Approval endpoint:

```python
def approve(flow_id, partition_key, reviewer):
    client.signal(
        flow_id,
        partition_key=partition_key,
        signal="approved",
        if_state="waiting",
        transition_to="approved",
        values={"reviewer": reviewer.encode()},
    )
```

Best practices:

- Use `signal` for callbacks.
- Store reviewer/comment values as named values.
- Keep long waits durable, not in Python memory.

## Webhook ingestion

Use flow ids as idempotency keys:

```python
from ferricstore import QueueClient

queue_client = QueueClient.from_url(url)

def handle_webhook(event):
    flow_id = f"webhook:{event.provider}:{event.id}"
    queue_client.queue(type="webhook").enqueue(
        flow_id,
        payload=event.summary_bytes,
        values={"raw_event": event.raw_bytes},
        idempotent=True,
    )
```

Worker:

```python
webhooks = queue_client.queue(type="webhook")
worker = webhooks.worker(claim_values=["raw_event"])
```

Best practices:

- Deterministic flow id prevents duplicate webhook ingestion.
- Raw event is a named value, not hot payload.
- Handler can safely retry parsing and downstream delivery.

## Periodic jobs

Use deterministic flow ids per period:

```python
from ferricstore import QueueClient

client = QueueClient.from_url(url)
reports = client.queue(type="daily_report")

flow_id = f"daily-report:{tenant_id}:{date}"
reports.enqueue(
    flow_id,
    partition_key=f"tenant:{tenant_id}",
    payload=date.encode(),
    idempotent=True,
    run_at_ms=scheduled_ms,
)
```

Best practices:

- deterministic ids make scheduling idempotent
- partition by tenant for tenant-level ordering
- use `run_at_ms` for future scheduling
- use terminal TTL/retention policy for cleanup

## Choosing between payload, values, and value refs

| Data shape | Store as |
| --- | --- |
| Small current-state data | `payload` |
| Large data needed by one or more future states | named `values` |
| Reusable data shared across children | `value_put` plus `value_refs` |
| External pointer | `value_refs` to your own URI/ref if appropriate |
| Final terminal output | `result` or named value plus terminal metadata |

Rule: hot flow state should answer "what should run next?", not carry every byte
the workflow might ever need.
