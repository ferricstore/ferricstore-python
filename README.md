# FerricStore Python SDK

Python SDK for FerricStore and FerricFlow.

Status: public alpha `0.1.0`. APIs may change before `1.0`.

## What you use

- `QueueClient` / `AsyncQueueClient` for durable queues.
- `WorkflowClient` / `AsyncWorkflowClient` for explicit durable state machines.
- `FlowClient` / `AsyncFlowClient` for advanced command-level control.
- `RetryPolicy`, `WorkerConfig`, `ValueConfig`, and `ExceptionPolicy` for runtime defaults.
- `RawCodec` by default, `JsonCodec` when you want JSON payloads.
- `client.command(...)` as the Redis/FerricStore escape hatch.

FerricFlow is not a hidden deterministic replay engine. It is an explicit durable
state pipeline:

```text
create -> claim -> handler -> transition/complete/retry/fail
```

Handlers should be idempotent because work can be retried after lease expiry,
worker crash, or explicit retry.

## Install

```bash
pip install ferricstore
```

Local development:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## Queue quickstart

```python
from ferricstore import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome:user-1", idempotent=True)
emails.worker(concurrency=100, batch_size=500).run(send_email)
```

If the handler raises, the default worker policy is retry.

## Workflow quickstart

```python
from ferricstore import WorkflowClient, complete, transition

client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
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
    idempotent=True,
)
```

## Named values

Use named values when different states need different pieces of data:

```python
emails.enqueue(
    "email-2",
    payload=b"small routing bytes",
    values={
        "template": b"welcome template bytes",
        "profile": b"user profile snapshot",
    },
)

emails.worker(claim_values=["template"]).run(send_email)
```

Only requested values are hydrated for the handler. Use `ValueConfig` or
`value_max_bytes` in production to cap large value reads.

## Async

```python
import asyncio

from ferricstore import AsyncQueueClient


async def main():
    client = AsyncQueueClient.from_url("redis://127.0.0.1:6379/0")
    emails = client.queue(type="email")

    async def handler(job):
        await send_email_async(job.payload)

    await emails.worker(concurrency=100, batch_size=500).run(handler)


asyncio.run(main())
```

## Production shape

Use one process/service to create work and a separate long-lived worker service
to claim and complete work.

```text
web/serverless producer -> FerricStore -> worker service
```

Before production, configure timeouts, connection pools, lease duration,
backpressure behavior, graceful shutdown, and value hydration caps.

## Docs

- [Documentation index](docs/index.md)
- [Quickstart](docs/quickstart.md)
- [SDK guide](docs/sdk.md)
- [Configuration](docs/configuration.md)
- [Production readiness](docs/production.md)
- [Data in workflows](docs/data.md)
- [Worker runtime](docs/worker.md)
- [Async APIs](docs/async.md)
- [Use cases](docs/use-cases.md)
- [Testing](docs/testing.md)
- [Troubleshooting](docs/troubleshooting.md)

## Examples

- `examples/order_workflow.py`: two-state workflow.
- `examples/queue_worker.py`: queue producer and worker.
- `examples/async_queue_worker.py`: async queue producer and worker.
- `examples/state_machine_workflow.py`: explicit workflow runner.
- `examples/native_commands.py`: Redis/FerricStore command helpers.
- `examples/dbos_style_benchmark.py`: DBOS-style throughput benchmark.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md), [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md),
[SECURITY.md](SECURITY.md), and [RELEASE.md](RELEASE.md).
