# FerricStore Python SDK

Python SDK for FerricStore and FerricFlow.

Status: public alpha `0.1.1`. APIs may change before `1.0`, but the SDK is
tested against command construction, queue/workflow handlers, leases, retries,
history, indexed attributes, named values, idempotent create, worker loops,
async flows, and local FerricStore integration scenarios.

FerricFlow keeps each workflow or job's state and history in one durable place. It
is an explicit durable state pipeline, not a hidden deterministic replay engine:

```text
create -> claim -> handler -> transition/complete/retry/fail
```

Handlers should be idempotent because work can be retried after lease expiry,
worker crash, or explicit retry.

Durability is the default contract. A workflow command returns success only
after the state change is accepted through FerricStore's quorum path and written
to disk.

## First 10 minutes

### 1. Install

```bash
pip install ferricstore
```

For local development from this repo:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Start FerricStore

Use a local FerricStore server with the FerricStore protocol listener enabled:

```bash
ferricstore start
```

If you are running from the FerricStore source repo, use that repo's documented
server command. The SDK examples assume:

```text
ferric://127.0.0.1:6388
```

### 3. Create a durable queue item

```python
from ferricstore import FlowClient, QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome:user-1", idempotent=True)
```

Use `attributes` for small indexed metadata you want to filter/count later:

```python
emails.enqueue(
    "email-2",
    payload=b"welcome:user-2",
    attributes={"tenant": "acme", "campaign": "summer"},
    idempotent=True,
)

flow = FlowClient.from_url("ferric://127.0.0.1:6388")
records = flow.list("email", attributes={"tenant": "acme"})
stats = flow.stats("email", attributes={"tenant": "acme"})
```

Attributes are not payload bytes. Use named values/value refs for large data.

### 4. Run a queue worker

```python
from ferricstore import QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")


def send_email(job):
    print(f"send {job.id}: {job.payload!r}")
    return b"sent"


emails.worker(concurrency=10, batch_size=100).run(send_email)
```

If the handler raises, the default worker policy is retry.

### 5. Create a workflow/state machine

Use workflows when one durable flow moves through named states.

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
    idempotent=True,
)

order.worker(states=["created", "charged"], concurrency=10, batch_size=100).run()
```

### 6. Store and fetch named values

Use named values when different states need different pieces of data. Values are
stored as FerricFlow value refs and are only hydrated when requested.

```python
emails.enqueue(
    "email-2",
    payload=b"small routing bytes",
    values={
        "template": b"welcome template bytes",
        "profile": b"user profile snapshot",
    },
    idempotent=True,
)

emails.worker(claim_values=["template"]).run(send_email)
```

Fetch one or many values directly when needed:

```python
profile = client.value_get(owner_flow_id="email-2", name="profile")
values = client.value_mget(
    owner_flow_id="email-2",
    names=["template", "profile"],
)
```

Use `ValueConfig` or `value_max_bytes` in production to cap large value reads.

### 7. Inspect history

```python
record = emails.get("email-1")
history = emails.history("email-1")

print(record)
for event in history:
    print(event)
```

History is for debugging and audit. Handlers should use claimed job data and
requested values, not history replay.

### 8. Common errors

| Error | Meaning | Usual fix |
| --- | --- | --- |
| `FlowAlreadyExistsError` | The flow id already exists. | Use `idempotent=True` for safe producer retries or generate a new id. |
| `FlowNotFoundError` | The flow does not exist or was retained/expired. | Check id, partition inputs, and retention policy. |
| `FlowWrongStateError` | The command expected a different current state. | Check worker state filters and handler transitions. |
| `StaleLeaseError` | A worker tried to complete with an old lease. | Keep handlers under `lease_ms` or renew/retry safely. |
| `OverloadedError` | Server backpressure rejected the write. | Let the SDK retry/back off; reduce producer rate under sustained pressure. |

## What you use

- `QueueClient` / `AsyncQueueClient` for durable queues.
- `WorkflowClient` / `AsyncWorkflowClient` for explicit durable state machines.
- `FlowClient` / `AsyncFlowClient` for advanced command-level control.
- `ScheduleResult`, `EffectResult`, `ApprovalResult`, `CircuitBreakerStatus`,
  `BudgetResult`, and `GovernanceOverview` for typed admin/governance responses
  with dict fallback.
- `RetryPolicy`, `WorkerConfig`, `ValueConfig`, and `ExceptionPolicy` for runtime defaults.
- `RawCodec` by default, `JsonCodec` when you want JSON payloads.
- `client.command(...)` as the Redis/FerricStore escape hatch.

## Async quickstart

```python
import asyncio

from ferricstore import AsyncQueueClient


async def main():
    client = AsyncQueueClient.from_url("ferric://127.0.0.1:6388")
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

Before production, configure timeouts, lease duration, backpressure behavior,
graceful shutdown, and value hydration caps. The `ferric://` transport defaults
to one multiplexed connection with 8 request lanes; only raise connection or
lane counts after profiling shows client-side saturation.

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
- `examples/protocol_commands.py`: Redis/FerricStore command helpers.
- `examples/protocol_kv_benchmark.py`: protocol SET/GET benchmark.
- `examples/protocol_dbos_benchmark.py`: protocol DBOS-style queued workflow benchmark.
- `examples/dbos_style_benchmark.py`: DBOS-style throughput benchmark.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md), [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md),
[SECURITY.md](SECURITY.md), and [RELEASE.md](RELEASE.md).
