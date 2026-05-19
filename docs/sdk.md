# FerricStore Python SDK Guide

This guide covers the public Python SDK surface and when to use each layer.

FerricStore speaks Redis-compatible RESP. The SDK gives typed helpers for
FerricFlow and FerricStore-native commands, while still letting you call any
normal Redis command through one passthrough method.

## What the SDK includes

| Area | API |
| --- | --- |
| Low-level Flow commands | `FlowClient` |
| Queue-style workers | `QueueFlowWorker` |
| Explicit state-machine workflows | `Workflow`, `@state`, `transition`, `complete`, `retry`, `fail` |
| Queue/workflow support types | `CreateItem`, `ClaimedItem`, `FencedItem`, `ChildSpec`, `RetryPolicy`, `FlowRecord` |
| Native FerricStore commands | `cas`, `lock`, `ratelimit_add`, `fetch_or_compute`, `key_info`, cluster/admin helpers |
| Normal Redis commands | `client.command(...)` |
| Payload codecs | `RawCodec`, `JsonCodec` |
| Transport adapter | `RedisAdapter`, `RedisCommandExecutor` |

## Install and connect

```bash
pip install ferricstore
```

Development install from this repo:

```bash
cd /Users/yoavgea/repos/ferricstore-python
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

Create a client:

```python
from ferricstore import FlowClient

client = FlowClient.from_url("redis://127.0.0.1:6379/0")
```

`from_url` uses `redis-py` with RESP3 and raw byte responses:

```python
redis.Redis.from_url(url, protocol=3, decode_responses=False)
```

Use JSON payloads when you want language-neutral structured values:

```python
from ferricstore import FlowClient, JsonCodec

client = FlowClient.from_url("redis://127.0.0.1:6379/0", codec=JsonCodec())
```

The default `RawCodec` accepts `bytes`, `bytearray`, `str`, or `None` and returns
raw bytes on decode.

## Which API should I use?

| Use case | Recommended API |
| --- | --- |
| DBOS-style queue workload | `client.enqueue(...)` plus `QueueFlowWorker` |
| Explicit durable state machine | `Workflow` plus `@state` handlers |
| Advanced batching, fanout, custom routing | `FlowClient` directly |
| Normal Redis data structures | `client.command("SET", ...)`, `client.command("HSET", ...)` |
| Locks, CAS, rate limits, fetch stampede protection | First-class native helpers on `FlowClient` |
| Cluster/admin operations | Cluster/admin helpers or `client.command(...)` |

Rule of thumb: start with `QueueFlowWorker` for queues, start with `Workflow` for
state machines, and drop to `FlowClient` when you need exact command control.

## Normal Redis commands

Use `command` as the explicit escape hatch for every Redis-compatible command.

```python
client.command("SET", "k", "v")
value = client.command("GET", "k")
client.command("HSET", "user:1", "name", "Ada")
client.command("ZADD", "scores", 10, "a")
```

The SDK does not try to wrap every Redis command. FerricStore-specific behavior
gets typed helpers; standard Redis commands stay reachable through `command`.

## FerricStore-native commands

FerricStore adds commands that are not part of vanilla Redis. The SDK exposes the
main ones directly.

### CAS

```python
updated = client.cas("account:1", b"old", b"new", ex=60)
if updated:
    print("changed")
```

Maps to:

```text
CAS key expected new [EX seconds]
```

### Distributed lock

```python
if client.lock("lock:invoice:1", "worker-1", ttl_ms=30_000):
    try:
        do_work()
        client.extend_lock("lock:invoice:1", "worker-1", ttl_ms=30_000)
    finally:
        client.unlock("lock:invoice:1", "worker-1")
```

Maps to:

```text
LOCK key owner ttl_ms
EXTEND key owner ttl_ms
UNLOCK key owner
```

### Rate limit

```python
limit = client.ratelimit_add("rl:user:42", window_ms=1_000, max=10, count=1)

if limit.allowed:
    handle_request()
else:
    reject_request(retry_after_ms=limit.reset_ms)
```

`ratelimit_add` returns `RateLimitResult`:

```python
RateLimitResult(status="allowed", count=1, remaining=9, reset_ms=997)
```

### Fetch or compute

Use this for stampede protection around expensive cached work.

```python
result = client.fetch_or_compute("report:42", ttl_ms=60_000, hint="report-build")

if result.hit:
    report = result.value
elif result.should_compute:
    try:
        report = build_report()
        client.fetch_or_compute_result("report:42", report, ttl_ms=60_000)
    except Exception as exc:
        client.fetch_or_compute_error("report:42", str(exc))
        raise
```

Only one caller computes. Other callers wait and then receive the result or
error.

### Key diagnostics

```python
info = client.key_info("order:1")
print(info.type, info.value_size, info.ttl_ms, info.hot_cache_status)
```

`key_info` returns `KeyInfo` with parsed fields plus the raw response.

### Cluster/admin helpers

```python
client.cluster_health()
client.cluster_stats()
slot = client.cluster_keyslot("order:1")
client.cluster_slots()
client.cluster_status()
client.cluster_role()

client.cluster_join("node@127.0.0.1", replace=True)
client.cluster_leave()
client.cluster_failover(0, "node@127.0.0.1")
client.cluster_promote("node@127.0.0.1")
client.cluster_demote("node@127.0.0.1")

client.ferricstore_config("GET", "max_memory")
client.ferricstore_hotness()
client.ferricstore_metrics()
client.ferricstore_blobgc("RUN")
```

Admin commands depend on the server configuration and ACLs. Use them from
operator code, not normal request handlers.

## FlowClient basics

`FlowClient` is the low-level typed wrapper around FerricFlow commands.

### Create one flow

```python
record = client.create(
    "order-1",
    type="order",
    state="created",
    partition_key="tenant-a:order-1",
    payload=b"raw order payload",
    correlation_id="checkout-123",
)
```

Important create options:

| Option | Meaning |
| --- | --- |
| `type` | Workflow type. Required. |
| `state` | Initial state. Default is `queued`. |
| `payload` | User value encoded by the codec. |
| `partition_key` | Routing key for shard locality and ordering. |
| `parent_flow_id`, `root_flow_id`, `correlation_id` | Lineage/query metadata. |
| `run_at_ms` | Due time for claiming. |
| `priority` | Priority used by due indexes. |
| `idempotent` | Let duplicate creates resolve safely when server supports it. |
| `values`, `value_refs` | Named per-flow values. |
| `return_record` | If `False`, return raw ack and skip follow-up `FLOW.GET`. |

Use `enqueue` when you only need acknowledgement:

```python
client.enqueue(
    "order-1",
    type="order",
    payload=b"raw order payload",
    partition_key="tenant-a:order-1",
)
```

### Create many flows

```python
from ferricstore import CreateItem

records = client.create_many(
    "tenant-a",
    [
        CreateItem("order-1", b"payload-1"),
        CreateItem("order-2", b"payload-2"),
    ],
    type="order",
    state="queued",
)
```

For partition-free producer code, use `partition_key=None` and include per-item
partitions, or use `enqueue_many` and let the SDK auto-bucket by flow id.

```python
client.enqueue_many(
    [
        CreateItem("order-1", b"payload-1"),
        CreateItem("order-2", b"payload-2"),
    ],
    type="order",
)
```

`enqueue_many` is the preferred hot producer path for queue workloads. It returns
acks and lets the SDK group work efficiently.

### Claim work

```python
jobs = client.claim_due(
    "order",
    state="queued",
    worker="worker-1",
    partition_key="tenant-a:order-1",
    lease_ms=30_000,
    limit=100,
)
```

`claim_due` returns `FlowRecord` values. Each claimed record carries:

| Field | Use |
| --- | --- |
| `id` | Flow id to mutate. |
| `partition_key` | Route future commands to the same shard. |
| `lease_token` | Lease ownership token. |
| `fencing_token` | Monotonic stale-worker fence. |
| `payload` | Hydrated only when requested by claim options. |
| `values` | Named values hydrated only when requested. |

Use `claim_jobs` for the compact queue hot path when the worker only needs job
ids and tokens:

```python
jobs = client.claim_jobs("order", state="queued", worker="worker-1", limit=100)
```

### Complete, retry, fail, cancel

```python
job = jobs[0]

client.complete(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    result=b"ok",
    return_record=False,
)
```

```python
client.retry(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    error=b"temporary failure",
    run_at_ms=next_attempt_ms,
    return_record=False,
)
```

```python
client.fail(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    error=b"permanent failure",
    return_record=False,
)
```

```python
client.cancel(
    job.id,
    fencing_token=job.fencing_token,
    lease_token=job.lease_token,
    partition_key=job.partition_key,
    reason=b"user cancelled",
    return_record=False,
)
```

### Transition between states

```python
client.transition(
    job.id,
    from_state=job.state,
    to_state="charged",
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    payload=b"charged payload",
    return_record=False,
)
```

### Batch mutations

Use `ClaimedItem` for lease-token commands:

```python
from ferricstore import ClaimedItem

items = [
    ClaimedItem(job.id, job.lease_token, job.fencing_token, partition_key=job.partition_key)
    for job in jobs
]

client.complete_many(None, items, result=b"ok", independent=True)
client.retry_many(None, items, error=b"temporary", independent=True)
client.fail_many(None, items, error=b"permanent", independent=True)
```

Use `FencedItem` for transition and cancel batches:

```python
from ferricstore import FencedItem

items = [
    FencedItem(job.id, job.fencing_token, job.lease_token, partition_key=job.partition_key)
    for job in jobs
]

client.transition_many(None, "queued", "running", items, independent=True)
client.cancel_many(None, items, reason=b"stop", independent=True)
```

`independent=True` means per-item success/failure. Without it, same-shard groups
are atomic and fail as a group.

### Lease extension and reclaim

```python
client.extend_lease(
    job.id,
    job.lease_token,
    fencing_token=job.fencing_token,
    lease_ms=60_000,
    partition_key=job.partition_key,
)

expired = client.reclaim("order", worker="reaper-1", limit=100)
```

### Queries

```python
record = client.get("order-1", partition_key="tenant-a:order-1")
history = client.history("order-1", partition_key="tenant-a:order-1", count=100)
queued = client.list("order", state="queued", count=100)
completed = client.terminals("order", state="completed", count=100)
failed = client.failures("order", count=100)
children = client.by_parent("parent-flow-id", count=100)
root = client.by_root("root-flow-id", count=100)
correlated = client.by_correlation("checkout-123", count=100)
info = client.info("order")
stuck = client.stuck("order", older_than_ms=300_000, count=100)
```

### Policy and cleanup

```python
from ferricstore import RetryPolicy

client.install_policy(
    "order",
    states={
        "queued": RetryPolicy(max_retries=5, backoff="exponential", base_ms=100, max_ms=5_000),
    },
)

policy = client.policy_get("order", state="queued")
client.retention_cleanup(limit=10_000)
```

## Named values and value refs

Named values let a flow keep multiple payload-like values without putting all
bytes into the hot state record. This is useful when different states need
different pieces of data.

Create with named values:

```python
client.create(
    "order-1",
    type="order",
    payload=b"small routing payload",
    values={
        "order": b"large order document",
        "customer": b"customer snapshot",
    },
)
```

Mutate named values during transitions or terminal commands:

```python
client.transition(
    job.id,
    from_state="queued",
    to_state="charged",
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    values={"charge": b"charge response"},
    drop_values=["customer"],
    override_values=["charge"],
    return_record=False,
)
```

Fetch only the values needed by this read or claim:

```python
record = client.get("order-1", values=["order"], value_max_bytes=64 * 1024)

jobs = client.claim_due(
    "order",
    state="queued",
    worker="worker-1",
    values=["order", "customer"],
    value_max_bytes=64 * 1024,
)
```

Store and reuse a value ref:

```python
meta = client.value_put(b"shared bytes", owner_flow_id="order-1", name="invoice")
ref = meta[b"ref"]

client.create("order-2", type="order", value_refs={"invoice": ref})
values = client.value_mget([ref])
```

Queue and workflow APIs expose `claim_values` so handlers can receive named
values on claim without an extra `get`.

## Queue API

Use `QueueFlowWorker` when the flow is acting as a durable queue: create jobs,
claim jobs, run a handler, complete/retry/fail behind the scenes.

```python
from ferricstore import FlowClient, QueueFlowWorker

client = FlowClient.from_url("redis://127.0.0.1:6379/0")

worker = QueueFlowWorker(
    client,
    type="email",
    state="queued",
    concurrency=500,
    batch_size=100,
    lease_ms=30_000,
    claim_values=["template"],
    value_max_bytes=64 * 1024,
)


def send_email(job):
    template = None
    if getattr(job, "values", None):
        template = job.values.get("template")
    deliver(job.id, template)


worker.run(send_email)
```

Common queue producer:

```python
client.enqueue(
    "email-1",
    type="email",
    payload=b"small routing payload",
    values={"template": b"welcome"},
)
```

High-throughput producer:

```python
from ferricstore import CreateItem

client.enqueue_many(
    [CreateItem(f"email-{i}", b"payload") for i in range(10_000)],
    type="email",
)
```

Important `QueueFlowWorker` options:

| Option | Meaning |
| --- | --- |
| `type` | Flow type to claim. |
| `state` | One state to claim. Omit only when you intentionally want any state. |
| `states` | Multiple states to claim. Mutually exclusive with `state`. |
| `concurrency` | Handler concurrency. |
| `batch_size` | Max jobs per claim. |
| `lease_ms` | Lease duration for claimed jobs. |
| `claim_values` | Named values to hydrate with each claim. |
| `value_max_bytes` | Per-value hydration cap. |
| `on_error` | `retry`, `fail`, or `raise`. |
| `partition_key`, `partition_keys` | Restrict claims to specific partitions. |
| `claim_partition_batch_size` | Claim from multiple partitions per loop when supplied. |
| `complete_async_depth` | Async completion batching depth. |
| `wake_source` | Optional ready-signal coordinator/source. |

Error behavior:

| `on_error` | Behavior |
| --- | --- |
| `retry` | Failed handler sends `FLOW.RETRY`. |
| `fail` | Failed handler sends `FLOW.FAIL`. |
| `raise` | Exception escapes to caller. |

`QueueFlowWorker` is the API to sell for queue workloads. It hides the optimized
claim/complete batching while keeping handler code simple.

## Workflow/state-machine API

Use `Workflow` when the app is an explicit durable state machine.

```python
from ferricstore import FlowClient, Workflow, complete, fail, retry, state, transition


class OrderWorkflow(Workflow):
    type = "order"
    initial_state = "created"
    partition_by = ("tenant_id", "order_id")

    @state("created", lease_ms=30_000, claim_payload=True, on_error="retry", return_record=False)
    def created(self, job):
        charge_card(job.payload)
        return transition("charged", payload=b"charge result")

    @state("charged", lease_ms=30_000, claim_values=["invoice"], on_error="fail", return_record=False)
    def charged(self, job):
        send_receipt(job.values.get("invoice"))
        return complete(result=b"ok")


client = FlowClient.from_url("redis://127.0.0.1:6379/0")
workflow = OrderWorkflow(client)

record = workflow.create(
    "order-1",
    tenant_id="tenant-a",
    order_id="order-1",
    payload=b"order payload",
    values={"invoice": b"invoice payload"},
)

workflow.run_once("created", worker="worker-1", partition_key=record.partition_key)
workflow.run_once("charged", worker="worker-1", partition_key=record.partition_key)
```

What a state handler does:

```text
FLOW.CLAIM_DUE type STATE state
handler(job)
FLOW.TRANSITION, FLOW.COMPLETE, FLOW.RETRY, or FLOW.FAIL
```

Handler outcomes:

```python
return transition("next_state", payload=b"new payload")
return complete(result=b"ok")
return retry(error=b"temporary", run_at_ms=next_attempt_ms)
return fail(error=b"permanent")
```

A handler can also call other Flow or Redis commands through the context/client
helpers exposed by the workflow layer:

```python
@state("created", return_record=False)
def created(self, job):
    job.flow.create(
        "child-1",
        type="child",
        payload=b"child payload",
        return_record=False,
    )

    job.flow.command("SET", "last-order", job.id)
    return transition("child_created")
```

Use `return_record=False` when the handler does not need the post-mutation record.
Use `claim_payload=False` when the handler does not need the current payload. Use
`claim_values=[...]` when it needs only selected named values.

This workflow DSL does not replay Python code. Each handler is one durable state
boundary. That is the design: explicit states, explicit retries, explicit side
updates.

## Autobatching

`FlowClient.autobatch()` wraps common single-flow write calls and flushes them as
server batch commands when possible.

```python
auto = client.autobatch(max_batch=100, max_delay_ms=1.0)

future = auto.create_async("order-1", type="order", payload=b"payload", return_record=False)
result = future.result()
```

Autobatch is useful when app code naturally issues many independent single-flow
calls but you still want server-side batching. The queue and benchmark helpers use
more specialized batching for hot paths.

## Performance guidelines

Use these defaults for production hot paths:

| Workload | Recommendation |
| --- | --- |
| Queue producer | `enqueue_many` or `enqueue(..., return_record=False)` |
| Queue worker | `QueueFlowWorker` with `batch_size` near handler capacity |
| State machine | `Workflow` with `return_record=False` on hot states |
| Payload-heavy flows | Use named values and request only needed values |
| Normal Redis operations | Use `client.command(...)` |
| Reused expensive values | Use `value_put`, `value_refs`, and `value_mget` |
| Expensive cached computation | Use `fetch_or_compute` |

Avoid hydrating payloads or named values unless the handler needs them. Avoid
post-mutation reads in hot paths unless the next line of user code actually uses
the new record.

## Correctness guidelines

Handlers must be idempotent. A worker can crash after doing a side effect but
before completing the flow. When the lease expires, another worker may reclaim the
same job.

Use these tools:

| Risk | Tool |
| --- | --- |
| Duplicate external side effect | External idempotency key based on flow id/state/version. |
| Stale worker writes | `lease_token` and `fencing_token`. The SDK passes them into mutations. |
| Large payload copied too often | Named values or value refs. |
| Need same-order updates | Use a stable `partition_key`. |
| Need high throughput | Let SDK auto-bucket no-partition queue creates. |

## Minimal examples

### Queue workload

```python
from ferricstore import FlowClient, QueueFlowWorker

client = FlowClient.from_url("redis://127.0.0.1:6379/0")
client.enqueue("email-1", type="email", payload=b"welcome")

worker = QueueFlowWorker(client, type="email", state="queued", concurrency=100, batch_size=100)
worker.run(lambda job: send_email(job.id))
```

### State machine workload

```python
from ferricstore import FlowClient, Workflow, complete, state, transition


class Signup(Workflow):
    type = "signup"
    initial_state = "created"

    @state("created", return_record=False)
    def created(self, job):
        return transition("email_sent")

    @state("email_sent", return_record=False)
    def email_sent(self, job):
        return complete(result=b"ok")


client = FlowClient.from_url("redis://127.0.0.1:6379/0")
workflow = Signup(client)
workflow.enqueue("signup-1", payload=b"user")
workflow.run_once("created", worker="worker-1")
workflow.run_once("email_sent", worker="worker-1")
```

### Native command plus Redis passthrough

```python
if client.ratelimit_add("rl:user:1", window_ms=1_000, max=10).allowed:
    client.command("INCR", "accepted:user:1")
```
