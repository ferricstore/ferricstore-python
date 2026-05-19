# Client API

`FlowClient` is the low-level typed wrapper around FerricStore Flow commands.

```python
from ferricstore import FlowClient

client = FlowClient.from_url("redis://127.0.0.1:6379/0")
```

## Parity

`FlowClient` covers the Flow command set:

| Area | Methods |
| --- | --- |
| Create | `create`, `create_many` |
| Value refs | `value_put` |
| Claim/lease | `claim_due`, `reclaim`, `extend_lease` |
| Mutate | `transition`, `transition_many`, `complete`, `complete_many`, `retry`, `retry_many`, `fail`, `fail_many`, `cancel`, `cancel_many`, `rewind` |
| Children | `spawn_children` |
| Query | `get`, `history`, `list`, `terminals`, `failures`, `by_parent`, `by_root`, `by_correlation`, `info`, `stuck` |
| Policy/cleanup | `install_policy`, `policy_get`, `retention_cleanup` |

`from_url` uses `redis-py` with RESP3:

```python
redis.Redis.from_url(url, protocol=3, decode_responses=False)
```

## `create`

Creates one flow.

```python
record = client.create(
    "flow-1",
    type="order",
    state="created",
    partition_key="tenant-a:order-1",
    payload=b"payload",
    correlation_id="checkout-123",
)
```

Maps to:

```text
FLOW.CREATE flow-1 TYPE order STATE created PARTITION tenant-a:order-1 PAYLOAD ...
```

Important options:

* `type`: required workflow type.
* `state`: initial state, default `queued`.
* `payload`: encoded through codec.
* `partition_key`: shard/order key.
* `parent_flow_id`, `root_flow_id`, `correlation_id`: query lineage.
* `run_at_ms`: due time.
* `priority`: lower/higher depends on server ordering policy.
* `idempotent`: let duplicate create by id return existing result when supported.
* `return_record`: default `True`. When `False`, return the raw server response
  and skip the SDK follow-up `FLOW.GET` used when the server returns `OK`.

## `create_many`

Creates a batch.

```python
from ferricstore import CreateItem

records = client.create_many(
    "tenant-a",
    [
        CreateItem("flow-1", b"payload-1"),
        CreateItem("flow-2", b"payload-2"),
    ],
    type="order",
    state="created",
)
```

Use `partition_key=None` for mixed partitions:

```python
records = client.create_many(
    None,
    [
        CreateItem("flow-1", b"payload-1", partition_key="tenant-a:1"),
        CreateItem("flow-2", b"payload-2", partition_key="tenant-a:2"),
    ],
    type="order",
)
```

Same-partition batches are atomic as one shard group. Mixed batches are grouped by
shard; each shard group is atomic.

Batch commands may return either RESP3 maps or `OK`, depending on the server
command. The SDK decodes list-of-map responses into `FlowRecord` values and
passes `OK` through unchanged.

## `value_put`

Stores a reusable value and returns server metadata/reference.

```python
ref = client.value_put(b"large payload", partition_key="tenant-a", owner_flow_id="flow-1")
```

## `claim_due`

Claims due work for a type/state.

```python
jobs = client.claim_due(
    "order",
    state="created",
    worker="worker-1",
    partition_key="tenant-a:order-1",
    lease_ms=30_000,
    limit=10,
)
```

Returns `list[FlowRecord]`. Each record includes `lease_token` and
`fencing_token`; pass both into mutation commands.

## `reclaim`

Claims expired leases, typically for recovery workers.

```python
jobs = client.reclaim("order", worker="reaper-1", limit=100)
```

## `extend_lease`

Extends a running lease.

```python
client.extend_lease(
    job.id,
    job.lease_token,
    fencing_token=job.fencing_token,
    lease_ms=60_000,
    partition_key=job.partition_key,
)
```

## `transition`

Moves a claimed job to another state.

```python
client.transition(
    job.id,
    from_state=job.state,
    to_state="charged",
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    payload=b"next payload",
)
```

Set `return_record=False` when the caller only needs acknowledgement and not the
new record. This removes one `FLOW.GET` from the hot path.

## `complete`

Closes a flow as completed.

```python
client.complete(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    result=b"ok",
)
```

`complete`, `retry`, `fail`, and `cancel` support the same `return_record=False`
ack-only option.

## Batch Mutations

Use `ClaimedItem` for commands that require lease token:

```python
from ferricstore import ClaimedItem

items = [
    ClaimedItem(job.id, job.lease_token, job.fencing_token, partition_key=job.partition_key)
]

client.complete_many(None, items, result=b"ok")
client.retry_many(None, items, error=b"temporary")
client.fail_many(None, items, error=b"permanent")
```

Use `FencedItem` for transition/cancel batches:

```python
from ferricstore import FencedItem

client.transition_many(
    None,
    from_state="created",
    to_state="charged",
    items=[FencedItem(job.id, job.fencing_token, job.lease_token, job.partition_key)],
)

client.cancel_many(
    None,
    items=[FencedItem(job.id, job.fencing_token, partition_key=job.partition_key)],
    reason=b"user cancelled",
)
```

## `cancel`

Cancels a flow.

```python
client.cancel(
    job.id,
    fencing_token=job.fencing_token,
    lease_token=job.lease_token,
    partition_key=job.partition_key,
    reason=b"user cancelled",
)
```

## `rewind`

Rewinds to a prior history event.

```python
client.rewind(
    "flow-1",
    to_event="event-id",
    partition_key="tenant-a:order-1",
    expect_state="failed",
)
```

## `retry`

Schedules retry for claimed job.

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

## `fail`

Closes a flow as failed.

```python
client.fail(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    error=b"permanent failure",
)
```

## `get`

Reads current flow record.

```python
record = client.get("flow-1", partition_key="tenant-a:order-1")
```

Returns `FlowRecord | None`.

## `history`

Reads recent history.

```python
events = client.history("flow-1", partition_key="tenant-a:order-1", count=100)
```

Supports filters:

```python
events = client.history(
    "flow-1",
    partition_key="tenant-a:order-1",
    from_ms=1_000,
    to_ms=2_000,
    from_version=2,
    rev=True,
    event="transition",
    values=True,
    payload_max_bytes=64_000,
)
```

## Query Commands

```python
client.list("order", state="queued", count=100)
client.terminals("order", state="completed", rev=True, count=100)
client.failures("order", from_ms=0, to_ms=now_ms)
client.by_parent("parent-flow-id", terminal_only=True)
client.by_root("root-flow-id", state="failed")
client.by_correlation("checkout-123", include_cold=True)
client.info("order", include_cold=True)
client.stuck("order", older_than_ms=60_000)
```

## `install_policy`

Installs retry policy globally or per state.

```python
from ferricstore import RetryPolicy

client.install_policy(
    "order",
    retry=RetryPolicy(max_retries=3, backoff="fixed", base_ms=100, max_ms=1_000),
    states={
        "charge": RetryPolicy(max_retries=10, backoff="exponential", base_ms=1_000, max_ms=86_400_000),
    },
)
```

## `policy_get`

```python
policy = client.policy_get("order", state="charge")
```

## `retention_cleanup`

```python
summary = client.retention_cleanup(limit=1_000)
```

## FerricStore-native commands

`FlowClient` also exposes FerricStore commands that are not part of vanilla
Redis.

```python
client.cas("k", b"old", b"new", ex=60)

if client.lock("lock:k", "owner", ttl_ms=30_000):
    try:
        do_work()
    finally:
        client.unlock("lock:k", "owner")

client.extend_lock("lock:k", "owner", ttl_ms=30_000)

limit = client.ratelimit_add("rl:user:42", window_ms=1_000, max=10)
if limit.allowed:
    handle_request()

info = client.key_info("k")
```

Fetch-or-compute gives stampede protection:

```python
result = client.fetch_or_compute("report:42", ttl_ms=60_000)

if result.hit:
    report = result.value
else:
    try:
        report = build_report()
        client.fetch_or_compute_result("report:42", report, ttl_ms=60_000)
    except Exception as exc:
        client.fetch_or_compute_error("report:42", str(exc))
        raise
```

Cluster/admin helpers:

```python
client.cluster_health()
client.cluster_stats()
client.cluster_keyslot("k")
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

## Normal Redis command passthrough

Use `command` for any Redis-compatible command that does not need a typed SDK
wrapper.

```python
client.command("SET", "k", "v")
client.command("GET", "k")
client.command("HSET", "user:1", "name", "Ada")
client.command("ZADD", "scores", 10, "a")
```

This is the intended escape hatch. FerricStore-specific APIs are typed; generic
Redis remains available without turning the SDK into a full Redis client.
