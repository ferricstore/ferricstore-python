# FerricStore Python SDK Guide

This guide covers the public Python SDK surface and when to use each layer.

FerricStore Python SDK uses the native `ferric://` / `ferrics://` protocol. It defaults to one multiplexed connection with 8 request lanes. The SDK gives typed helpers for FerricFlow and FerricStore commands, while still letting you call lower-level data-structure commands through one passthrough method.

If you are new, start with [Quickstart](quickstart.md). If you need
production-style examples for sagas, IoT fanout, AI orchestration, human
approval, webhooks, or batch fanout, read [Use Case Examples](use-cases.md).

## What the SDK includes

| Area | API |
| --- | --- |
| High-level workflows | `WorkflowClient`, `@workflow.state`, `transition`, `complete`, `retry`, `fail` |
| High-level queues | `QueueClient` |
| Async high-level workflows | `AsyncWorkflowClient` |
| Async high-level queues | `AsyncQueueClient` |
| Low-level Flow commands | `FlowClient` |
| Queue/workflow support types | `CreateItem`, `ClaimedFlow`, `FencedItem`, `ChildSpec`, `RetryPolicy`, `WorkerConfig`, `ValueConfig`, `ExceptionPolicy`, `FlowRecord` |
| Protocol FerricStore commands | `cas`, `lock`, `ratelimit_add`, `fetch_or_compute`, `key_info`, cluster/admin helpers |
| Data-structure commands | typed helpers such as `kv_set`, `hash_set`, `list_push`, or `client.command(...)` |
| Payload codecs | `RawCodec`, `JsonCodec` |
| Transport adapter | native protocol adapter, or a custom `CommandExecutor` for tests/advanced embedding |

For production deployment defaults, worker sizing, lease/reclaim policy,
observability, graceful shutdown, and security guidance, read
[Production Readiness](production.md).

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
from ferricstore import WorkflowClient

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
```

`from_url` uses the native FerricStore protocol adapter. URLs must use `ferric://` or `ferrics://`.

Use JSON payloads when you want language-neutral structured values:

```python
from ferricstore import JsonCodec, WorkflowClient

client = WorkflowClient.from_url("ferric://127.0.0.1:6388", codec=JsonCodec())
```

The default `RawCodec` accepts `bytes`, `bytearray`, `str`, or `None` and returns
raw bytes on decode.

## Which API should I use?

| Use case | Recommended API |
| --- | --- |
| DBOS-style queue workload | `QueueClient` |
| Explicit durable state machine | `WorkflowClient` plus `@workflow.state` handlers |
| Async queue or workflow service | `AsyncQueueClient` / `AsyncWorkflowClient` |
| Advanced batching, fanout, custom routing | `FlowClient` directly |
| Data-structure commands | `client.kv_set(...)`, `client.hash_set(...)`, `client.list_push(...)`, or `client.command(...)` |
| Locks, CAS, rate limits, fetch stampede protection | First-class protocol helpers on `FlowClient` |
| Cluster/admin operations | Cluster/admin and management helpers, or `client.command(...)` |

Rule of thumb: start with `QueueClient` for queues, start with `WorkflowClient`
for state machines, use the async equivalents in `asyncio` services, and drop to
`FlowClient` only when you need exact command control.

## Client-level defaults

Put normal production defaults on the high-level client. Queues and workflows
inherit them, and explicit `.queue(...)`, `.workflow(...)`, `.worker(...)`, or
`@workflow.state(...)` arguments override them.

```python
from ferricstore import ExceptionPolicy, RetryPolicy, ValueConfig, WorkerConfig, WorkflowClient

client = WorkflowClient.from_url(
    "ferric://127.0.0.1:6388",
    retry_policy=RetryPolicy(max_retries=10, backoff="exponential"),
    worker_config=WorkerConfig(
        batch_size=100,
        idle_sleep_s=0.01,
        exception_policy=ExceptionPolicy.RETRY,
    ),
    value_config=ValueConfig(value_max_bytes=64 * 1024),
)
```

These are service defaults, not benchmark flags. Throughput benchmarks use
explicit profiles in `examples/protocol_kv_benchmark.py`,
`examples/protocol_dbos_benchmark.py`, and
`examples/protocol_restate_latency_benchmark.py`.

## Data-structure commands

Use typed helpers for common data-structure commands, and `command` as the explicit escape hatch for commands that do not yet have a typed helper.

```python
client.command("SET", "k", "v")
value = client.command("GET", "k")
client.command("HSET", "user:1", "name", "Ada")
client.command("ZADD", "scores", 10, "a")
```

The SDK does not try to wrap every low-level command. FerricStore-specific behavior gets typed helpers; low-level command access stays reachable through `command`.

## FerricStore protocol commands

FerricStore adds commands that are not part of the basic data-structure command set. The SDK exposes the
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

### Management/control-plane helpers

`FlowClient` and `AsyncFlowClient` expose narrow management operations for
control planes. They map to stable FerricStore commands instead of requiring
arbitrary command execution.

```python
caps = client.capabilities()
client.acl_set_user("platform_worker", ["on", "+PING", "+@read", "~tenant:acme:*"])
client.ensure_namespace("tenant:acme:", {"owner": "platform"})
client.set_quota("tenant:acme:", {"keys": 100_000, "bytes": 1_000_000_000})
usage = client.namespace_usage("tenant:acme:")
flows = client.flow_query({"type": "order", "state": "failed"})
```

## FlowClient basics

`FlowClient` is the low-level typed wrapper around FerricFlow commands.

### Create one flow

```python
client.create(
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
| `return_record` | Default `False`. Set `True` only when the caller needs the updated `FlowRecord`. |

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

client.create_many(
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

Use `include_record=False` for the compact queue hot path when the worker only
needs claim metadata:

```python
claims = client.claim_due(
    "order",
    state="queued",
    worker="worker-1",
    limit=100,
    include_record=False,
)
```

Compact claims include indexed `attributes` by default so workers can route or
branch without an extra `get`. Use `include_attributes=False` when a very hot
path only needs ids, lease tokens, and fencing tokens.

Long timers use the same claim path. FerricStore may hibernate far-future
`run_at_ms` flows to keep hot RAM/indexes small; `claim_due` and the worker APIs
still promote and claim those flows normally once they are due.

### Complete, retry, fail, cancel

```python
job = jobs[0]

client.complete(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    result=b"ok",
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
)
```

```python
client.fail(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    error=b"permanent failure",
)
```

```python
client.cancel(
    job.id,
    fencing_token=job.fencing_token,
    lease_token=job.lease_token,
    partition_key=job.partition_key,
    reason=b"user cancelled",
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
)
```

### Batch mutations

Use `ClaimedFlow` for lease-token commands:

```python
from ferricstore import ClaimedFlow

items = [
    ClaimedFlow(claim.id, claim.lease_token, claim.fencing_token, partition_key=claim.partition_key)
    for claim in claims
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
from ferricstore import RetryPolicy, WorkflowClient

client = WorkflowClient.from_url(
    "ferric://127.0.0.1:6388",
    retry_policy=RetryPolicy(max_retries=5, backoff="exponential", base_ms=100, max_ms=5_000),
)
workflow = client.workflow(type="order", initial_state="queued")
workflow.install_policy()

policy = client.policy_get("order")
client.retention_cleanup(limit=10_000)
```

## Indexed attributes

Use attributes for small metadata that should be queryable without reading
payload or named values:

```python
client.create(
    "order-1",
    type="order",
    payload=b"small routing bytes",
    attributes={"tenant": "acme", "region": "us"},
)

client.transition(
    job.id,
    from_state="created",
    to_state="charged",
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    attributes_merge={"phase": "charge"},
)

records = client.list("order", attributes={"tenant": "acme"})
stats = client.stats("order", attributes={"tenant": "acme"})
```

Keep attributes small and stable: tenant, region, campaign, device group, model,
or customer-visible status. Use named values/value refs for large bytes.
Attribute queries use FerricFlow's async projection; use
`consistent_projection=True` for admin/debug reads that must wait for catch-up.

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
)
```

Use `override_values=[...]` only when the state intentionally replaces a named
value. For normal step outputs, prefer first-write semantics so duplicate
side effects or unexpected retries are visible.

Fetch only the values needed by this read or claim:

```python
record = client.get("order-1", values=["order"])

jobs = client.claim_due(
    "order",
    state="queued",
    worker="worker-1",
    values=["order", "customer"],
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

Use `QueueClient` when each item is one durable unit of work.

```python
from ferricstore import QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome:user-1")

def send_email(job):
    deliver(job.id)

emails.worker(concurrency=500, batch_size=1000).run(send_email)
```

Queue workers hide claim/complete batching while keeping handler code simple.

## Workflow/state-machine API

Use `WorkflowClient` when the app is an explicit durable state machine.

```python
from ferricstore import WorkflowClient, complete, fail, retry, transition

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
order = client.workflow(
    type="order",
    initial_state="created",
    partition_by=("tenant_id", "order_id"),
)

@order.state("created", lease_ms=30_000, claim_payload=True)
def created(job):
    charge_card(job.payload)
    return transition("charged", payload=b"charge result")

@order.state("charged", lease_ms=30_000, claim_values=["invoice"])
def charged(job):
    send_receipt(job.values.get("invoice"))
    return complete(result=b"ok")

order.start(
    "order-1",
    tenant_id="tenant-a",
    order_id="order-1",
    payload=b"order payload",
    values={"invoice": b"invoice payload"},
)
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

A handler can also call other Flow or FerricStore commands through the context/client
helpers exposed by the workflow layer:

```python
@order.state("created")
def created(job):
    job.flow.create(
        "child-1",
        type="child",
        payload=b"child payload",
    )

    job.flow.command("SET", "last-order", job.id)
    return transition("child_created")
```

Workflow state decorators default to ack-only mutation responses. Set
`return_record=True` only when the handler needs the post-mutation record.
Use `claim_payload=False` when the handler does not need the current payload. Use
`claim_values=[...]` when it needs only selected named values.

Advanced: `job.value("name", local_cache=True)` caches a named value only inside
the current handler invocation. The default is `False`; enable it only when the
same handler reads the same value repeatedly and the value is safe to hold in
memory briefly.

This workflow DSL does not replay Python code. Each handler is one durable state
boundary. That is the design: explicit states, explicit retries, explicit side
updates.

## Autobatching

`FlowClient.autobatch()` wraps common single-flow write calls and flushes them as
server batch commands when possible.

```python
auto = client.autobatch(max_batch=100, max_delay_ms=1.0)

future = auto.create_async("order-1", type="order", payload=b"payload")
result = future.result()
```

Autobatch is useful when app code naturally issues many independent single-flow
calls but you still want server-side batching. The queue and benchmark helpers use
more specialized batching for hot paths.

## Performance guidelines

Use these defaults for production hot paths:

| Workload | Recommendation |
| --- | --- |
| Queue workload | `QueueClient.queue(...).enqueue(...)` plus `.worker(...)` |
| State machine | `WorkflowClient.workflow(...)` plus `@workflow.state(...)` |
| Payload-heavy flows | Use named values and request only needed values |
| Data-structure operations | Use typed helpers or `client.command(...)` |
| Reused expensive values | Use `value_put`, `value_refs`, and `value_mget` |
| Expensive cached computation | Use `fetch_or_compute` |

Avoid hydrating payloads or named values unless the handler needs them. Avoid
post-mutation reads in hot paths unless the next line of user code actually uses
the new record.

For production services, also set explicit FerricStore timeouts, size connection pools,
wire graceful shutdown, cap value hydration with `value_max_bytes`, and verify
crash/reclaim behavior in integration tests. See [Production Readiness](production.md).

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
from ferricstore import QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"welcome")
emails.worker(concurrency=100, batch_size=1000).run(lambda job: send_email(job.id))
```

### State machine workload

```python
from ferricstore import WorkflowClient, complete, transition

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
signup = client.workflow(type="signup", initial_state="created")

@signup.state("created")
def created(job):
    return transition("email_sent")

@signup.state("email_sent")
def email_sent(job):
    return complete(result=b"ok")

signup.start("signup-1", payload=b"user")
```

### Protocol command plus low-level passthrough

```python
if client.ratelimit_add("rl:user:1", window_ms=1_000, max=10).allowed:
    client.command("INCR", "accepted:user:1")
```
