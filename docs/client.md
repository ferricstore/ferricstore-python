# Client API

`FlowClient` is the low-level typed wrapper around FerricStore Flow commands.
Most applications should start with `WorkflowClient` or `QueueClient`; use this
page when you need exact command-level control.

```python
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
```

The SDK only opens FerricStore native protocol connections from URLs. Use `ferric://` for plaintext development and `ferrics://` for TLS deployments.

## Parity

`FlowClient` covers the Flow command set:

| Area | Methods |
| --- | --- |
| Create | `create`, `create_many` |
| Value refs | `value_put`, `value_mget` |
| Claim/lease | `claim_due`, `reclaim`, `extend_lease` |
| Mutate | `transition`, `transition_many`, `complete`, `complete_many`, `retry`, `retry_many`, `fail`, `fail_many`, `cancel`, `cancel_many`, `rewind` |
| Children | `spawn_children` |
| Query | `get`, `history`, `list`, `search`, `terminals`, `failures`, `by_parent`, `by_root`, `by_correlation`, `info`, `stuck` |
| Attribute discovery | `attributes`, `attribute_values` |
| Schedules | `schedule_create`, `schedule_get`, `schedule_fire`, `schedule_pause`, `schedule_resume`, `schedule_delete`, `schedule_fire_due`, `schedule_list` |
| Governance | `effect_reserve`, `effect_confirm`, `effect_fail`, `effect_compensate`, `effect_get`, `governance_ledger`, `approval_request`, `approval_approve`, `approval_reject`, `approval_get`, `approval_list`, `governance_overview`, `circuit_open`, `circuit_close`, `circuit_get`, `budget_reserve`, `budget_commit`, `budget_release`, `budget_get`, `budget_list`, `limit_lease`, `limit_spend`, `limit_release`, `limit_get`, `limit_list` |
| Management reads/writes | `capabilities`, `acl_*`, `ensure_namespace`, `get_namespace`, `list_namespaces`, `delete_namespace`, `set_quota`, `get_quota`, `quota_usage`, `cluster_info`, `namespace_usage`, `flow_query`, `flow_history`, `invocation_*` |
| Policy/cleanup | `install_policy`, `policy_get`, `retention_cleanup` |

`from_url` uses the native FerricStore protocol adapter.

## `create`

Creates one flow.

```python
ack = client.create(
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
* `return_record`: default `False`. When `True`, fetch and return the updated
  `FlowRecord` after the mutation.

## `create_many`

Creates a batch.

```python
from ferricstore import CreateItem

client.create_many(
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
client.create_many(
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

Batch commands normally return `OK`. With `independent=True`, the server returns
per-item outcomes such as `OK` or an error for each item; the SDK preserves that
shape so callers can see which items succeeded.

`enqueue_many` groups auto-partitioned items before writing. Native sync and
async transports send independent groups with an ordered, bounded fanout of at
most 16 calls. Injected custom executors remain sequential unless they explicitly
set `supports_concurrent_fanout = True`, which should only be done when concurrent
`execute_command` calls are safe.

Async `pipeline()` calls preserve command order within each routed destination,
including compact Flow mutation groups that cannot share one wire frame. The
lower-level `execute_batch()` transport hook retains bounded concurrency because
its groups are treated as independent; custom adapters can expose
`execute_batch_ordered()` when they support the stronger pipeline contract.

## `value_put`

Stores a reusable value and returns server metadata/reference.

```python
ref = client.value_put(b"large payload", partition_key="tenant-a", owner_flow_id="flow-1")
```

Named values are unique per owner flow and name. Keep `override=False` for normal
first-write values. Use `override=True` only when the caller intentionally
replaces an existing value, such as regenerating a draft/report.

```python
ref = client.value_put(
    b"fraud-report",
    partition_key="tenant-a",
    owner_flow_id="flow-1",
    name="fraud_report",
    override=False,
)

values = client.value_mget([ref["ref"]], max_bytes=64 * 1024)
```

## `claim_due`

Claims due work for a type/state.

```python
records = client.claim_due(
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

For a lean worker claim response, set `include_record=False`. This returns
`ClaimedFlow` values with `attributes` included by default:

```python
claims = client.claim_due(
    "order",
    state="created",
    worker="worker-1",
    include_record=False,
)
```

`client.claim_flows(...)` is a convenience wrapper for the same lean shape.

Pass `include_attributes=False` only when a hot path needs just id, lease token,
fencing token, and partition key.

Default behavior:

| Concern | Default |
| --- | --- |
| Long timers | Delayed flows may be hibernated in FerricStore cold-due storage, but `claim_due` claims them normally once due. |
| Payload | Payload is not hydrated unless `payload=True` or named `values=[...]` are requested. |
| Blocking | `block_ms=None` means one immediate claim attempt. Set `block_ms` to wait server-side. |
| Lease recovery | `reclaim_expired=True` by default for `claim_due`; set it explicitly on lean worker paths when desired. |

## `reclaim`

Claims expired leases, typically for recovery workers.

```python
records = client.reclaim("order", worker="reaper-1", limit=100)
```

`reclaim` supports the same partition and response controls as `claim_due`:
`partition_key`, `partition_keys`, `priority`, `include_record`, `payload`,
`payload_max_bytes`, `values`, and `value_max_bytes`.

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
    priority=10,
)
```

Mutators are ack-only by default. Set `return_record=True` only when the caller
needs the new record immediately.

## Governance budgets

Budget commands return `BudgetResult`, a typed object that is also compatible
with dict-style access.

```python
reservation = client.budget_reserve(
    "tenant:acme:llm",
    10_000,
    limit=1_000_000,
    window_ms=60_000,
)

committed = False
try:
    used_tokens = call_model()
    result = client.budget_commit(
        "tenant:acme:llm",
        reservation.reservation_id,
        used_tokens,
        usage={"tokens": used_tokens},
    )
    committed = result.status == "committed"
finally:
    if not committed:
        client.budget_release("tenant:acme:llm", reservation.reservation_id)
```

For normal workflow handlers, prefer `BudgetPolicy` or `ctx.budget(...)` from
[Workflow and Queue APIs](workflow.md). The low-level commands are useful for
admin tools and custom runtimes.

## `complete`

Closes a flow as completed.

```python
client.complete(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    result=b"ok",
    ttl_ms=86_400_000,
)
```

`complete`, `retry`, `fail`, and `cancel` support the same `return_record=True`
option when the caller needs the post-mutation record.

## Batch Mutations

Use `ClaimedFlow` for commands that require lease token:

```python
from ferricstore import ClaimedFlow

items = [
    ClaimedFlow(claim.id, claim.lease_token, claim.fencing_token, partition_key=claim.partition_key)
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

## `spawn_children`

Creates child flows and optionally moves the parent into a wait state.

```python
from ferricstore import ChildSpec

client.spawn_children(
    parent.id,
    [
        ChildSpec("child-1", type="order.child", payload=b"one"),
        ChildSpec("child-2", type="order.child", payload=b"two"),
    ],
    partition_key=parent.partition_key,
    lease_token=parent.lease_token,
    fencing_token=parent.fencing_token,
    wait="all",
    wait_state="waiting_children",
    success="children_done",
    failure="child_failed",
    from_state=parent.state,
    on_child_failed="fail_parent",
    on_parent_closed="cancel_children",
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
client.search(
    "order",
    state="completed",
    attributes={"tenant": "acme"},
    state_meta={"version": 3},
    terminal_only=True,
    count=100,
)
client.terminals("order", state="completed", rev=True, count=100)
client.failures("order", from_ms=0, to_ms=now_ms)
client.by_parent("parent-flow-id", terminal_only=True)
client.by_root("root-flow-id", state="failed")
client.by_correlation("checkout-123", include_cold=True)
client.info("order", include_cold=True)
client.stuck("order", older_than_ms=60_000)
```

Broad `search(...)` filters use server-side policy indexes. Attribute filters
require `indexed_attributes`; state metadata filters require `indexed_state_meta`.

## Attribute Discovery

Use attributes for workflow search/debug filters. Attribute indexes are projected
asynchronously by FerricStore; payload bytes are not indexed.

```python
keys = client.attributes("order", state="queued", partition_key="tenant-a")
values = client.attribute_values("order", "tenant", state="queued", count=20)
```

`attributes(...)` returns key/count rows. `attribute_values(...)` returns
value/count rows for one attribute key. Use these for dashboard filter pickers
and saved views.

## Schedules

Schedules create Flow work at durable times. The schedule only starts work; the
Flow state machine handles the rest.

```python
client.schedule_create(
    "daily-report",
    target={
        "id": "report-2026-06-17",
        "type": "report",
        "state": "queued",
        "partition_key": "tenant-a",
        "payload": b"{}",
    },
    cron="0 7 * * *",
    timezone="Asia/Jerusalem",
    overlap_policy="skip",
)

due = client.schedule_fire_due(limit=100, block_ms=1000)
client.schedule_pause("daily-report")
client.schedule_resume("daily-report")
client.schedule_fire("daily-report")  # manual/admin fire
client.schedule_delete("daily-report")
```

Use `overwrite=True` only when replacing an existing schedule definition.

## Governance

Governance APIs are explicit Flow-side controls for effects, approvals,
circuits, budgets, and distributed limits. They are useful when a workflow must
explain why it paused, retried, or did not call an external system.

For concepts, error shapes, telemetry events, and production patterns, read the
[Governance guide](governance.md).

In workflow handlers, prefer `ctx.effect(...)` so reserve/confirm/fail happens
around the actual external call. Use the raw client methods below when you are
building your own worker runtime or admin tooling.

```python
import time

effect = client.effect_reserve(
    job.id,
    "stripe-charge",
    "payment.charge",
    partition_key=job.partition_key,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    operation_digest="sha256:...",
)

started = time.perf_counter()

try:
    external_id = charge_card()
    latency_ms = int((time.perf_counter() - started) * 1000)
    client.effect_confirm(
        job.id,
        "stripe-charge",
        external_id=external_id,
        latency_ms=latency_ms,
    )
except Exception as exc:
    latency_ms = int((time.perf_counter() - started) * 1000)
    client.effect_fail(
        job.id,
        "stripe-charge",
        error=str(exc),
        reason=type(exc).__name__,
        latency_ms=latency_ms,
    )
    raise
```

`ctx.effect(...)` in the workflow API measures `latency_ms` automatically. Pass
it manually only when using raw client methods.

Approvals:

```python
approval = client.approval_request(
    "approval:order-1",
    flow_id="order-1",
    scope="tenant-a",
    reason="manual fraud review",
    requested_by="fraud-worker",
    assignees=["ops"],
)

client.approval_approve(approval["id"], approver="ops-user")
```

Budgets and limits:

```python
budget = client.budget_reserve(
    "tenant-a:ai:tokens:daily",
    8_000,
    limit=2_000_000,
    window_ms=86_400_000,
)

try:
    result = call_llm(max_tokens=budget["reserved_amount"])
    client.budget_commit(
        "tenant-a:ai:tokens:daily",
        budget["reservation_id"],
        result.usage.total_tokens,
        usage={
            "model": result.model,
            "input_tokens": result.usage.input_tokens,
            "output_tokens": result.usage.output_tokens,
        },
    )
except Exception:
    client.budget_release("tenant-a:ai:tokens:daily", budget["reservation_id"])
    raise
```

`budget_commit(...)` records actual usage. If actual usage is lower than the
reservation, unused budget is refunded. If actual usage is higher, the overage
is recorded so later reservations are denied until the window resets.

Distributed limits:

```python
lease = client.limit_lease("tenant-a:email", shard_id=0, amount=10, ttl_ms=30_000)
client.limit_release("tenant-a:email", shard_id=0, amount=10)
```

Circuits:

```python
client.circuit_open("payment-api", open_ms=30_000, failure_threshold=5)
client.circuit_close("payment-api")
```

Circuit policies can also use `window_ms`, `min_calls`, `failure_rate_pct`,
`latency_threshold_ms`, `error_classes`, `half_open_max_probes`, and
`half_open_success_threshold`. See [Governance](governance.md) for the full
policy shape.

Read APIs:

```python
client.governance_ledger("order-1", rev=True, limit=100)
client.approval_list(scope="tenant-a", limit=100)
client.governance_overview(scope="tenant-a")
client.budget_list(scope="tenant-a")
client.limit_list(scope="tenant-a")
```

Schedule, effect, approval, circuit, budget, and overview calls return typed result
objects for autocomplete:

```python
schedule.status
effect.external_id
approval.flow_id
circuit.status
budget.remaining
overview.counts
```

They remain dict-compatible for escape-hatch fields:

```python
effect["status"]
approval.get("reason")
```

The public result classes are `ScheduleResult`, `EffectResult`,
`ApprovalResult`, `CircuitBreakerStatus`, `BudgetResult`, and
`GovernanceOverview`.

## `install_policy`

Installs retry policy globally or per state. State mode policy is also
supported; FIFO is opt-in per state and requires explicit `partition_key` when
records enter that state.

```python
from ferricstore import FlowStatePolicy, RetryPolicy

client.install_policy(
    "order",
    retry=RetryPolicy(max_retries=3, backoff="fixed", base_ms=100, max_ms=1_000),
    states={
        "charge": RetryPolicy(max_retries=10, backoff="exponential", base_ms=1_000, max_ms=86_400_000),
        "dispatch": FlowStatePolicy.fifo(),
    },
)
```

## Enterprise invocation helpers

Invocation helpers build narrow public native commands instead of exposing an
arbitrary command surface:

```python
client.invocation_definition_put(
    {
        "name": "send-email",
        "acl": {"scope_required": True},
        "partition": {"key": "tenant:{tenant}:invocation:send-email"},
    }
)

created = client.invocation_create(
    "send-email",
    {"tenant": "acme"},
    context={"subject": "user-1"},
    request_context={"subject": "platform", "tenant": "acme"},
)
client.invocation_get(created["invocation_id"])
client.invocation_partition_list("send-email", scope="tenant:acme")
```

## `policy_get`

```python
policy = client.policy_get("order", state="charge")
```

## `retention_cleanup`

```python
summary = client.retention_cleanup(limit=1_000)
```

## FerricStore protocol commands

`FlowClient` also exposes FerricStore commands that are FerricStore-specific higher-level operations.

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

## Command passthrough

Use `command` for any FerricStore data-structure command that does not need a typed SDK
wrapper.

```python
client.command("SET", "k", "v")
client.command("GET", "k")
client.command("HSET", "user:1", "name", "Ada")
client.command("ZADD", "scores", 10, "a")
```

This is the intended escape hatch. FerricStore-specific APIs are typed; generic
Low-level commands remain available without turning the SDK into a large wrapper surface.
