# Workflow and Queue APIs

The SDK has two high-level execution styles:

| Style | Use when |
| --- | --- |
| `QueueFlowWorker` | You want DBOS-style durable queued work: create, claim, process, complete. |
| `Workflow` | You want an explicit durable state machine with named states. |

Both use FerricFlow underneath. Neither replays Python code. Each claim/handler
execution ends in one durable Flow command.

## QueueFlowWorker

Use `QueueFlowWorker` for queue workloads.

```python
from ferricstore import FlowClient, QueueFlowWorker

client = FlowClient.from_url("redis://127.0.0.1:6379/0")

client.enqueue("email-1", type="email", payload=b"welcome")

worker = QueueFlowWorker(
    client,
    type="email",
    state="queued",
    concurrency=100,
    batch_size=100,
    lease_ms=30_000,
)


def handle_email(job):
    send_email(job.id)


worker.run(handle_email)
```

The worker handles:

| Step | Command |
| --- | --- |
| Claim | `FLOW.CLAIM_DUE` |
| Success | `FLOW.COMPLETE_MANY` when batching is possible |
| Handler error with `on_error="retry"` | `FLOW.RETRY_MANY` |
| Handler error with `on_error="fail"` | `FLOW.FAIL_MANY` |

Use `claim_values` when the handler needs named values:

```python
worker = QueueFlowWorker(
    client,
    type="email",
    state="queued",
    concurrency=500,
    batch_size=100,
    claim_values=["template", "profile"],
    value_max_bytes=64 * 1024,
)
```

Use `states=[...]` when a worker can process more than one state. Omit `state`
only when you intentionally want any state supported by the server configuration.

## Queue producers

Single job:

```python
client.enqueue(
    "email-1",
    type="email",
    payload=b"small payload",
    values={"template": b"welcome-template"},
)
```

Batch jobs:

```python
from ferricstore import CreateItem

client.enqueue_many(
    [CreateItem(f"email-{i}", b"payload") for i in range(10_000)],
    type="email",
)
```

For hot queues, prefer `enqueue`/`enqueue_many` over `create`/`create_many` when
the producer does not need the created record immediately.

## Workflow DSL

Use `Workflow` for explicit state-machine logic.

```python
from ferricstore import FlowClient, Workflow, complete, fail, retry, state, transition


class PaymentWorkflow(Workflow):
    type = "payment"
    initial_state = "created"
    partition_by = ("tenant_id", "payment_id")

    @state("created", lease_ms=30_000, on_error="retry", return_record=False)
    def created(self, job):
        charge_card(job.payload)
        return transition("charged", payload=b"charge result")

    @state("charged", lease_ms=30_000, claim_values=["receipt"], on_error="fail")
    def charged(self, job):
        send_receipt(job.values.get("receipt"))
        return complete(result=b"ok")
```

Create or enqueue:

```python
client = FlowClient.from_url("redis://127.0.0.1:6379/0")
workflow = PaymentWorkflow(client)

record = workflow.create(
    "payment-1",
    tenant_id="tenant-a",
    payment_id="payment-1",
    payload=b"raw payment request",
    values={"receipt": b"receipt bytes"},
)

workflow.enqueue(
    "payment-2",
    tenant_id="tenant-a",
    payment_id="payment-2",
    payload=b"raw payment request",
)
```

`partition_by` builds a partition key from arguments:

```text
tenant-a:payment-1
```

Override it when needed:

```python
workflow.create("payment-1", partition_key="custom-key", payload=b"...")
```

## Run one state

```python
workflow.run_once("created", worker="worker-1", partition_key=record.partition_key)
```

This performs:

```text
FLOW.CLAIM_DUE payment STATE created
handler(job)
FLOW.TRANSITION / FLOW.COMPLETE / FLOW.RETRY / FLOW.FAIL
```

## Handler outcomes

```python
return transition("next_state", payload=b"new payload")
return complete(result=b"ok")
return retry(error=b"temporary", run_at_ms=next_attempt_ms)
return fail(error=b"permanent")
```

Outcome helpers also support named value mutations:

```python
return transition(
    "charged",
    values={"charge": b"charge response"},
    drop_values=["temporary_input"],
    override_values=["charge"],
)
```

## Calling other commands inside a workflow

The handler context proxies FlowClient methods through `job.flow`, so workflow
code can create children, query history, use native FerricStore commands, or call
normal Redis commands.

```python
@state("created", return_record=False)
def created(self, job):
    job.flow.create(
        "child-1",
        type="payment-child",
        payload=b"child payload",
        return_record=False,
    )

    job.flow.command("SET", "payment:last", job.id)
    return transition("child_created")
```

Use this carefully. External side effects and extra commands should be idempotent
because a state can be retried after lease expiry.

## Exceptions

If handler raises:

| `on_error` | Behavior |
| --- | --- |
| `retry` | SDK sends `FLOW.RETRY`. |
| `fail` | SDK sends `FLOW.FAIL`. |

Default is retry.

## Performance switches

| Switch | Why use it |
| --- | --- |
| `return_record=False` | Avoid post-mutation `FLOW.GET`. |
| `claim_payload=False` | Avoid payload hydration when handler does not need payload. |
| `claim_values=[...]` | Hydrate only selected named values. |
| `value_max_bytes=N` | Bound named value hydration. |
| `workflow.enqueue(...)` | Producer only needs ack. |

## What this is not

This DSL is not deterministic replay. It does not resume inside a Python function.
It makes durable states explicit:

```text
created -> charged -> completed
```

That keeps retries, partitioning, querying, and debugging visible.
