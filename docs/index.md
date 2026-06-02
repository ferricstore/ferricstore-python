# FerricStore Python SDK Documentation

This is the documentation hub for the Python SDK.

FerricStore is Redis-compatible storage plus FerricFlow durable execution. The
Python SDK gives typed helpers, queue workers, workflow/state-machine helpers,
async APIs, and access to normal Redis-compatible commands.

## Start here

| If you want to... | Read |
| --- | --- |
| Understand project maturity | [Project Status](status.md) |
| Run locally from a fresh checkout | [Local Development](local-development.md) |
| Build your first queue or workflow | [Quickstart](quickstart.md) |
| Choose between queue, workflow, and low-level commands | [SDK Guide](sdk.md) |
| Deploy a real service | [Production Readiness](production.md) |
| Configure auth/TLS/security-sensitive settings | [Security](security.md) |
| See complete production-style examples | [Use Case Examples](use-cases.md) |
| Configure retries, workers, values, and pools | [Configuration](configuration.md) |
| Store payloads, named values, and value refs correctly | [Data in Workflows](data.md) |
| Use FastAPI, serverless, or a separate worker service | [Web and Serverless Usage](web.md) |
| Copy production patterns | [Patterns and Recipes](patterns.md) |
| Debug common problems | [Troubleshooting](troubleshooting.md) |
| Understand Flow concepts | [Concepts](concepts.md) |

## API docs

| Area | Docs |
| --- | --- |
| Low-level Flow commands | [Client API](client.md) |
| Queue and workflow APIs | [Workflow and Queue APIs](workflow.md) |
| Worker loops | [Worker](worker.md) |
| Payload codecs | [Payload Codecs](codecs.md) |
| Redis adapters and pools | [Redis Adapters](adapters.md) |
| Async APIs | [Async APIs](async.md) |
| Children and fanout | [Children and Fanout](children.md) |
| Retry and errors | [Retry and Errors](retry.md) |
| Celery, Temporal, DBOS comparisons | [If You Know Celery, Temporal, or DBOS](compare.md) |
| Sagas, IoT, AI orchestration, human approval | [Use Case Examples](use-cases.md) |
| Testing | [Testing](testing.md) |
| Benchmarks | [Benchmark Example](benchmark.md) |
| Project status and release posture | [Project Status](status.md) |
| Local development setup | [Local Development](local-development.md) |
| Security | [Security](security.md) |

## Recommended first production shape

For queue-like work:

```python
from ferricstore import QueueClient

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    socket_connect_timeout=2,
    socket_timeout=10,
    health_check_interval=30,
)

emails = client.queue(type="email")
emails.worker(lease_ms=60_000).run(lambda job: send_email(job.id))
```

For explicit state machines:

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
    charge(job.payload)
    return transition("charged")

@order.state("charged")
def charged(job):
    return complete(result=b"ok")

order.start("order-1", tenant_id="tenant-a", order_id="order-1", payload=b"...")
```

## Mental model

FerricFlow is not deterministic replay. It is an explicit durable state pipeline:

```text
create -> claim -> handler -> transition/complete/retry/fail
```

That means:

- every durable boundary is visible
- workers can be scaled horizontally
- handlers must be idempotent
- payload hydration is opt-in
- normal Redis/FerricStore commands remain available

## Production minimum

Before using the SDK in production:

- Set Redis connect and command timeouts.
- Size connection pools for producers and workers.
- Use explicit worker `state` or `states`.
- Make handlers idempotent.
- Set `lease_ms` above handler p99 with margin.
- Cap large value hydration with `value_max_bytes`.
- Wire graceful shutdown.
- Export claimed/completed/retried/failed/empty-claim metrics.
- Test crash plus reclaim behavior.

See [Production Readiness](production.md) for the full checklist.
