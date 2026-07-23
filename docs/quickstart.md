# Quickstart

This guide gets from zero to a working queue and a working state machine.

## Install

```bash
pip install ferricstore
```

For local development from this repo:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## Workflow in 5 minutes

Use a workflow when one durable flow moves through named states. Successful
workflow commands mean durable state progress, not just a notification that work
was seen.

```python
from ferricstore import WorkflowClient, complete, transition

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
order = client.workflow(
    type="order",
    initial_state="created",
    partition_by=("tenant_id", "order_id"),
)


@order.state("created")
def created(job):
    charge_card(job.payload)
    return transition("charged")


@order.state("charged")
def charged(job):
    send_receipt(job.id)
    return complete(result=b"ok")


order.start(
    "order-1",
    tenant_id="tenant-a",
    order_id="order-1",
    payload=b"order payload",
)
```

Production workers normally run forever through `WorkflowWorker` or your own
scheduler. `run_once` is useful for tests and examples.

## Queue in 5 minutes

Use a queue when each item is one unit of durable work.

```python
from ferricstore import QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome:user-1")
emails.worker().run(send_email)
```

If the handler raises, the default worker behavior is retry.

## Queue with named values

Use named values when different handlers need different pieces of data.

```python
emails.enqueue(
    "email-2",
    payload=b"small routing bytes",
    values={
        "template": b"welcome template bytes",
        "profile": b"user profile snapshot",
    },
)

emails.worker(
    claim_values=["template"],
).run(send_email)
```

Only `template` is hydrated for this handler.

## Escape hatch

High-level clients still expose lower-level commands when needed:

```python
client.command("SET", "last-started", "order-1")
client.value_put(b"large bytes", owner_flow_id="order-1", name="invoice")
```

## Async queue

```python
import asyncio

from ferricstore import AsyncQueueClient


async def main():
    client = AsyncQueueClient.from_url("ferric://127.0.0.1:6388")
    emails = client.queue(type="email")

    worker = emails.worker()

    async def handler(job):
        await send_email_async(job.id)

    await worker.run(handler)


asyncio.run(main())
```

Use async APIs for async applications. Do not wrap sync clients in event-loop
thread executors unless you have a specific reason.

## Which API should I use?

| Workload | Use |
| --- | --- |
| Durable queue | `QueueClient` / `AsyncQueueClient` |
| Explicit state machine | `WorkflowClient` / `AsyncWorkflowClient` |
| Custom command control | `FlowClient` / `AsyncFlowClient` |
| Data-structure command | `client.command(...)` |
| Fanout/children | `spawn_children` or workflow `job.flow.spawn_children(...)` |
| Large optional data | named values and `value_mget` |

## Next steps

- Read [Production Readiness](production.md) before deploying.
- Read [Configuration](configuration.md) for `RetryPolicy`, `WorkerConfig`, and `ValueConfig`.
- Read [Data in Workflows](data.md) before storing large payloads.
- See [Web server and worker split](production.md#web-server-and-worker-split) for FastAPI/serverless producer examples.
- Read [Async APIs](async.md) for asyncio services.
- Read [Patterns and Recipes](patterns.md) for copy-paste service patterns.
- Read [Troubleshooting](troubleshooting.md) when claims, leases, or payloads behave unexpectedly.
