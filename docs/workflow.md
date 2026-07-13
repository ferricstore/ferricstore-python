# Workflow and Queue APIs

The SDK has two high-level execution styles:

| Style | Use when |
| --- | --- |
| `QueueClient` | You want DBOS-style durable queued work: create, claim, process, complete. |
| `WorkflowClient` | You want an explicit durable state machine with named states. |

Both use FerricFlow underneath. Neither replays Python code. Each claim/handler
execution ends in one durable Flow command that is accepted through quorum and
written to disk before success is returned.

For production deployment concerns such as lease sizing, idempotency, graceful
shutdown, connection pools, and metrics, read [Production Readiness](production.md).
For complete examples such as sagas, IoT fanout, AI orchestration, human
approval, and batch fanout, read [Use Case Examples](use-cases.md).

## QueueClient

Use `QueueClient` for queue workloads.

```python
from ferricstore import QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
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

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
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

## Governance budgets in workflows

Use `BudgetPolicy` when a state should reserve capacity before it runs and
settle the reservation automatically when the handler succeeds.

```python
from ferricstore import BudgetPolicy, WorkflowClient, complete

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
workflow = client.workflow(type="agent", initial_state="call_model")

@workflow.state(
    "call_model",
    budget=BudgetPolicy(scope=lambda ctx: f"tenant:{ctx.partition_key}", amount=10_000),
)
def call_model(ctx):
    result = call_llm(ctx.payload)
    return complete(result=result)
```

When the state completes, the SDK stamps budget attributes such as
`governance_budget_scope`, `governance_budget_status`, and
`governance_budget_overage_amount` onto the Flow mutation. They appear in Flow
history and attribute search after normal projection catch-up.

Use `ctx.budget(...)` when actual usage is only known after the operation:

```python
@workflow.state("call_model")
def call_model(ctx):
    with ctx.budget("tenant:acme", 10_000, limit=1_000_000) as budget:
        result = call_llm(ctx.payload)
        budget.commit(result.tokens, usage={"tokens": result.tokens})
    return complete(result=result.text)
```

If the handler raises, the context manager releases the unused reservation.
For async workflows, use `async with ctx.budget(...)`. The first successful
`commit()` or `release()` is terminal and repeated settlement calls return that
same result without issuing a contradictory command. If an async release
operation itself is cancelled, a later `release()` can retry it.

Use `ctx.effect(...)` when a state performs an external side effect and you
want effect fencing plus circuit-breaker accounting around that call:

```python
@workflow.state("charge")
def charge(ctx):
    @ctx.effect(
        "stripe-charge",
        "payment.charge",
        operation_digest=f"stripe:{ctx.id}:v1",
        external_id=lambda result: result["charge_id"],
    )
    def call_stripe():
        return stripe.charge(ctx.value("order"))

    result = call_stripe()
    return complete(result=result)
```

The SDK reserves before `call_stripe`, confirms on success, and fails the
effect on exception. Circuit policies attached to the effect type can deny the
reservation before the external call happens.

For effect fencing, human approvals, distributed limits, circuits, telemetry,
and denial error shapes, see [Governance](governance.md).

## Async APIs

Use async APIs when your application already runs on `asyncio`.

```python
from ferricstore import AsyncQueueClient

client = AsyncQueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")

worker = emails.worker(
    concurrency=500,
    batch_size=1000,
)
```

For state machines, use `AsyncWorkflowClient`:

```python
from ferricstore import AsyncWorkflowClient, complete, transition

client = AsyncWorkflowClient.from_url("ferric://127.0.0.1:6388")
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
