# Production Readiness

This page is the production checklist for the Python SDK. It focuses on the
application-side choices that decide whether FerricFlow behaves like a reliable
queue/workflow runtime in real services.

FerricStore remains the durable source of truth. The SDK is a client/runtime
layer: it builds commands, manages worker loops, batches hot paths, maps errors,
and gives queue/workflow ergonomics. It does not remove the need for idempotent
handlers, capacity planning, timeouts, or operational monitoring.

## Production default stack

Use this shape for normal services:

| Need | Recommendation |
| --- | --- |
| Sync apps | `WorkflowClient.from_url(...)` or `QueueClient.from_url(...)`. |
| Async apps | `AsyncWorkflowClient.from_url(...)` or `AsyncQueueClient.from_url(...)`. |
| Queue workloads | `QueueClient.queue(...)`. |
| State machines | `WorkflowClient.workflow(...)` / `AsyncWorkflowClient.workflow(...)` with explicit states. |
| Hot producers | `queue.enqueue_many(...)` or `queue.enqueue(...)`, both ack-only by default. |
| Hot workers | `claim_due(..., include_record=False)` / worker APIs with compact claim responses. |
| Payload-heavy flows | Named values and `value_max_bytes` caps. |
| Recovery | Keep expired-lease reclaim enabled unless you deliberately run a separate recovery path. |

## Client construction

Use the FerricStore protocol transport for new services:

```python
from ferricstore import ExceptionPolicy, RetryPolicy, ValueConfig, WorkerConfig, WorkflowClient

client = WorkflowClient.from_url(
    "ferric://ferricstore.service:6388",
    timeout=10,
    retry_policy=RetryPolicy(max_retries=10, backoff="exponential", base_ms=500, max_ms=60_000),
    worker_config=WorkerConfig(
        batch_size=1000,
        idle_sleep_s=0.001,
        exception_policy=ExceptionPolicy.RETRY,
    ),
    value_config=ValueConfig(value_max_bytes=64 * 1024),
)
```

For async services:

```python
from ferricstore import AsyncWorkflowClient

client = AsyncWorkflowClient.from_url(
    "ferric://ferricstore.service:6388",
    timeout=10,
)
```

Guidance:

| Setting | Production rule |
| --- | --- |
| Protocol transport | Prefer `ferric://` / `ferrics://` for new SDK services. |
| Protocol connections | Default is one multiplexed connection. Keep it unless profiling shows socket/client saturation. |
| Protocol lanes | Default is 8 request lanes. Raise only for measured throughput workloads. |
| Timeouts | Set connect and command timeouts explicitly. |
| Async client | One async client per event loop is the simple safe default. |
| Sync client | One client per process/service component is usually enough; use multiple clients for isolated pools. |

RESP/Redis URLs are still supported for compatibility. If you use
`redis://` / `rediss://`, keep RESP3 and raw byte responses:

```python
redis.Redis.from_url(url, protocol=3, decode_responses=False)
```

For RESP URLs, pass normal `redis-py` TLS, auth, ACL, Sentinel/proxy, and pool
options through `from_url(...)`.

Connection sizing defaults:

| Runtime | Starting point |
| --- | --- |
| Protocol queue/workflow workers | Leave `command_connections` / `claim_connections` unset. Claim traffic reuses the same multiplexed connection. |
| Protocol high-throughput services | Try `max_connections=2` or higher `lanes` only after measuring. |
| RESP queue workers | Size Redis pools for concurrent producers, workers, and async completion depth. |
| Web/API producers | Bound local concurrency and retries; do not create a client per request. |
| Dedicated benchmark clients | Use a larger pool only when the benchmark is intentionally driving many in-flight batches. |

## Web server and worker split

Use the same deployment shape as Temporal-style systems:

```text
web server / serverless function = client that starts work
worker service / pod / VM = long-running worker that claims and completes work
both agree on Flow type/state
```

Do not run a normal worker loop inside a request handler. Request handlers should
enqueue/start work and return. Workers should run as a separate long-lived
process: Kubernetes Deployment, ECS service, VM/systemd, Nomad job, Docker
Compose service, or Cloud Run service with enough minimum instances.

`flow_config.py`:

```python
from ferricstore import ExceptionPolicy, JsonCodec, QueueClient, RetryPolicy, ValueConfig, WorkerConfig


def queue_client() -> QueueClient:
    return QueueClient.from_url(
        "ferric://ferricstore.service:6388",
        codec=JsonCodec(),
        timeout=10,
        retry_policy=RetryPolicy(
            max_retries=10,
            backoff="exponential",
            base_ms=500,
            max_ms=60_000,
            jitter_pct=20,
        ),
        worker_config=WorkerConfig(
            batch_size=1000,
            concurrency=200,
            lease_ms=30_000,
            idle_sleep_s=0.001,
            exception_policy=ExceptionPolicy.RETRY,
        ),
        value_config=ValueConfig(value_max_bytes=64 * 1024),
    )
```

`api.py`:

```python
from fastapi import FastAPI

from flow_config import queue_client


app = FastAPI()
emails = queue_client().queue(type="email")


@app.post("/emails")
def create_email(req: dict):
    flow_id = f"email:{req['id']}"
    emails.enqueue(
        flow_id,
        payload={
            "to": req["to"],
            "template": req["template"],
        },
        idempotent=True,
    )
    return {"id": flow_id, "status": "queued"}
```

`worker.py`:

```python
from flow_config import queue_client


emails = queue_client().queue(type="email")


def send_email(job):
    payload = job.payload
    send_email_provider(payload["to"], payload["template"])
    return b"sent"


if __name__ == "__main__":
    emails.worker().run(send_email)
```

Serverless producer:

```python
from flow_config import queue_client


emails = queue_client().queue(type="email")


def handler(event, context):
    flow_id = f"email:{event['id']}"
    emails.enqueue(flow_id, payload=event, idempotent=True)
    return {"id": flow_id, "status": "queued"}
```

## Worker configuration

Start with explicit state workers:

```python
from ferricstore import QueueClient


client = QueueClient.from_url("ferric://ferricstore.service:6388")
emails = client.queue(type="email")
worker = emails.worker(
    state="queued",
    concurrency=200,
    batch_size=1000,
    lease_ms=60_000,
    complete_async_depth=4,
)
```

Production rules:

| Option | Rule |
| --- | --- |
| `state` / `states` | Prefer explicit states. Omit state only when the worker truly handles any state. |
| `lease_ms` | Set above handler p99 plus network/server margin. |
| `batch_size` | Set near handler capacity. Larger is not always faster. |
| `concurrency` | Bound it to downstream capacity, not just CPU. |
| `block_ms` | Use `1000-5000` for production workers so idle queues block server-side instead of polling. Leave `None` for tests/custom schedulers that must return immediately. |
| `claim_partition_batch_size` | Start at `16`; increase for many-partition queues with low per-partition volume. |
| `claim_drain_batches` | Start at `1`; increase only when claims are full and handlers are cheap. |
| `retry_policy` | Put the normal retry policy on `WorkflowClient` / `QueueClient`; override per workflow, queue, or state only when needed. |
| `worker_config` | Put normal worker defaults on the client; explicit worker arguments still win. |
| `value_config` | Put normal named-value caps/cache defaults on the client; request large values explicitly. |
| `exception_policy` | Use `ExceptionPolicy.RETRY` for transient handler exceptions, `ExceptionPolicy.FAIL` for invalid-data exceptions, `ExceptionPolicy.RAISE` mainly for tests/supervisors. |
| `complete_async_depth` | Use small values like `2-8`; too deep can increase contention. |
| `claim_values` | Fetch only named values the handler needs. |
| `value_max_bytes` | Always cap hydrated values in production. |

For long-running handlers, either set a longer lease or call `extend_lease`
before the current lease can expire. Do not rely on handler runtime being stable
under load.

Long timers:

- Use `run_at_ms` / retries for delayed work; do not keep sleeping workers or app timers.
- FerricStore may hibernate far-future due flows out of hot RAM and indexes.
- Normal `claim_due`, queue workers, and workflow workers still claim hibernated flows when due.
- Prefer explicit `state` / `states` workers for hot paths; omit state only for workers that truly handle any state.

## Idempotency and side effects

Every handler must be idempotent.

A worker can:

- claim a job
- call an external service
- crash before `FLOW.COMPLETE` or `FLOW.TRANSITION`
- let another worker reclaim the expired lease

Use idempotency keys based on stable Flow data:

```text
<flow_id>:<logical_state>:<fencing_token>
```

or, for external APIs that need a stable retry key:

```text
<flow_id>:<logical_state>
```

Use `fencing_token` to reject stale local writes. The SDK passes
`lease_token` and `fencing_token` into Flow mutation helpers, but external
systems only become safe if you include equivalent idempotency/fencing in those
systems too.

## Reclaim and crash recovery

Lean worker claims and workers leave `reclaim_expired` unset by default, so the server
default applies. That is the right production default because expired running
leases should become claimable again.

Disable reclaim only for controlled benchmarks or when you run a separate,
intentional recovery process:

```python
emails = client.queue(type="email")
worker = emails.worker(
    state="queued",
    reclaim_expired=False,  # benchmark/specialized path only
)
```

If you run a dedicated recovery process, use `client.reclaim(...)` with clear
limits and observability:

```python
expired = client.reclaim("email", worker="reaper-1", limit=100, include_record=False)
```

## Partitioning

Partition keys decide locality, ordering, and shard parallelism.

| Workload | Partition strategy |
| --- | --- |
| Need per-entity order | Use stable explicit partition keys, e.g. `tenant:order`. |
| Need maximum queue throughput | Let `enqueue_many` use no explicit partition key; the SDK/server auto-bucket by flow id. |
| Need tenant isolation | Put tenant id in the partition key and/or flow type. |
| Need fanout | Use child flow ids/partitions that spread across shards unless order requires one partition. |

Do not put all jobs under one partition unless strict ordering is more important
than throughput.

## Payload and named values

Keep hot state small.

Use payload for small state-local data. Use named values for large or optional
data that only some states need:

```python
orders = client.queue(type="order")
orders.enqueue(
    "order-1",
    payload=b"small routing bytes",
    values={
        "order": b"...large order document...",
        "customer": b"...customer snapshot...",
    },
)
```

Claim only what the handler needs:

```python
orders = client.queue(type="order")
worker = orders.worker(
    state="charge",
    claim_values=["order"],
    value_max_bytes=64 * 1024,
)
```

Production rules:

- Do not hydrate payloads or named values by default.
- Always use `value_max_bytes` for untrusted or large values.
- Split large multi-stage data into named values, for example `order`,
  `fraud_report`, `invoice`, `shipment_label`.
- Use `value_put(..., owner_flow_id=..., name=..., override=False)` for
  idempotent named value creation.

## Graceful shutdown

For sync workers:

```python
worker.stop()
stats = worker.join(timeout=30)
worker.close()
```

For async workers:

```python
worker.stop()
stats = await worker.join()
await worker.close()
```

Shutdown goal:

1. stop accepting new claims
2. finish or fail in-flight handlers
3. flush async completions
4. close client pools if the worker owns them

If your service manager sends `SIGTERM`, wire it to `stop()` and give the worker
enough termination grace time for normal handler p99.

## Observability

At minimum, log or export:

| Metric | Why |
| --- | --- |
| claimed jobs/sec | Worker throughput. |
| completed/retried/failed counts | Outcome health. |
| empty claims | Polling inefficiency or missing wake signals. |
| handler latency p50/p95/p99 | Lease sizing and capacity. |
| claim latency p50/p95/p99 | Server/index health. |
| mutation latency p50/p95/p99 | WAL/storage pressure. |
| retry count by state/error | Downstream failures. |
| value/payload omitted counts | Payload caps hit. |
| worker shutdown drain time | Deployment safety. |

The SDK returns worker result objects from `run_once`, `join`, and batch helpers.
Use those for local counters. Wrap handlers and Redis adapters for latency
histograms.

## Backpressure

Backpressure should happen before FerricStore becomes the bottleneck:

- cap `concurrency`
- cap producer batch sizes
- cap `max_connections`
- use downstream semaphores for external APIs
- use rate-limit commands for tenant/request limits
- avoid unbounded local queues around `enqueue_many`

For async apps, avoid spawning unlimited tasks that all call the SDK. Use bounded
producer/consumer queues or semaphores.

## Testing before production

Use three layers:

| Layer | Purpose |
| --- | --- |
| Unit tests with fake adapters | Verify SDK command shape and handler outcomes. |
| Integration tests with real FerricStore | Verify create, claim, transition, complete, history, value refs, and reclaim. |
| Failure tests | Crash/timeout/reclaim/idempotency behavior. |

Minimum failure tests:

- handler raises and sends retry/fail according to policy
- worker crashes before completion, lease expires, job is reclaimed
- stale lease/fencing mutation is rejected
- payload/value cap omits large values safely
- duplicate producer create is handled idempotently when required
- graceful shutdown flushes pending completions

## Security and ACLs

Use Redis/FerricStore auth and TLS according to your deployment. Keep admin
helpers out of request handlers:

- `cluster_*`
- `ferricstore_config`
- `ferricstore_blobgc`
- retention cleanup jobs

Give normal application workers only the command categories they need.

## Production readiness checklist

Before calling a Python SDK service production-ready:

- `ferric://` or `ferrics://` protocol connection is configured for new services.
- RESP3/raw bytes is configured only when using Redis-compatible `redis://` URLs.
- Connect and command timeouts are set.
- Connection pool size matches worker/producers.
- Handler p99 is below `lease_ms` with margin, or leases are extended.
- Handlers are idempotent across retries/reclaims.
- External side effects use stable idempotency keys.
- Explicit `state`/`states` are used for workers.
- Payload/named value hydration is capped.
- Graceful shutdown is wired to service manager signals.
- Retry/fail policy is intentional per state.
- Reclaim behavior is intentional and tested.
- Partition strategy is documented.
- Metrics cover claims, empty claims, outcomes, latency, retries, and shutdown.
- Integration and failure tests run in CI.

## Known SDK boundaries

The SDK does not:

- provide deterministic replay
- make external side effects idempotent automatically
- auto-size leases from handler latency
- auto-instrument OpenTelemetry
- replace server-side backups, replication, or retention policy

Those are application/operations responsibilities. The SDK gives the workflow
state APIs and safe defaults needed to implement them.
