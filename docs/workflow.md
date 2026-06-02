# Workflow and Queue APIs

The SDK has two high-level execution styles:

| Style | Use when |
| --- | --- |
| `QueueClient` | You want DBOS-style durable queued work: create, claim, process, complete. |
| `WorkflowClient` | You want an explicit durable state machine with named states. |

Both use FerricFlow underneath. Neither replays Python code. Each claim/handler
execution ends in one durable Flow command.

For production deployment concerns such as lease sizing, idempotency, graceful
shutdown, connection pools, and metrics, read [Production Readiness](production.md).
For complete examples such as sagas, IoT fanout, AI orchestration, human
approval, and batch fanout, read [Use Case Examples](use-cases.md).

## QueueClient

Use `QueueClient` for queue workloads.

```python
from ferricstore import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome")

def handle_email(job):
    send_email(job.id)

emails.worker(concurrency=100, batch_size=1000, lease_ms=30_000).run(handle_email)
```

The worker handles:

| Step | Command |
| --- | --- |
| Claim | `FLOW.CLAIM_DUE` |
| Success | `FLOW.COMPLETE_MANY` when batching is possible |
| Handler exception with `exception_policy=ExceptionPolicy.RETRY` | `FLOW.RETRY_MANY` |
| Handler exception with `exception_policy=ExceptionPolicy.FAIL` | `FLOW.FAIL_MANY` |

Use `claim_values` when the handler needs named values:

```python
emails.worker(
    concurrency=500,
    batch_size=1000,
    claim_values=["template", "profile"],
).run(handle_email)
```

Use `states=[...]` when a worker can process more than one state. Omit `state`
only when you intentionally want any state supported by the server configuration.

## Queue producers

Single job:

```python
emails.enqueue(
    "email-1",
    payload=b"small payload",
    values={"template": b"welcome-template"},
)
```

Batch jobs:

```python
from ferricstore import CreateItem

emails.enqueue_many([CreateItem(f"email-{i}", b"payload") for i in range(10_000)])
```

For larger examples, see [Use Case Examples](use-cases.md).

## WorkflowClient

Use `WorkflowClient` for explicit state-machine logic.

```python
from ferricstore import WorkflowClient, complete, fail, retry, transition

client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
payment = client.workflow(
    type="payment",
    initial_state="created",
    partition_by=("tenant_id", "payment_id"),
)

@payment.state("created", lease_ms=30_000)
def created(job):
    charge_card(job.payload)
    return transition("charged", payload=b"charge result")

@payment.state("charged", lease_ms=30_000, claim_values=["receipt"])
def charged(job):
    send_receipt(job.values.get("receipt"))
    return complete(result=b"ok")
```

Start a workflow:

```python
payment.start(
    "payment-1",
    tenant_id="tenant-a",
    payment_id="payment-1",
    payload=b"raw payment request",
    values={"receipt": b"receipt bytes"},
)
```

`Workflow` subclasses remain supported for framework-style codebases, but
`WorkflowClient.workflow(...)` is the primary SDK style.

## Async APIs

Use async APIs when your application already runs on `asyncio`.

```python
from ferricstore import AsyncQueueClient

client = AsyncQueueClient.from_url("redis://127.0.0.1:6379/0")
emails = client.queue(type="email")

worker = emails.worker(
    concurrency=500,
    batch_size=1000,
)
```

For state machines, use `AsyncWorkflowClient`:

```python
from ferricstore import AsyncWorkflowClient, complete, transition

client = AsyncWorkflowClient.from_url("redis://127.0.0.1:6379/0")
workflow = client.workflow(
    type="order",
    states=["created", "charged"],
)


@workflow.state("created")
async def created(job):
    await charge_async(job.id)
    return transition("charged")


@workflow.state("charged")
async def charged(job):
    return complete(result=b"ok")
```

Use one async client per event loop as the simple production default. Bound
downstream concurrency with semaphores when handlers call external services.
