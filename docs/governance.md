# Governance

FerricFlow governance is for workflows that must enforce and explain operational
limits: AI token budgets, external side effects, human approvals, distributed
capacity, and circuit breakers.

Governance state is stored in FerricStore. Workflow attribute search and the
dashboard use FerricStore projections; do not mirror workflow governance data
into your application database.

## What governance gives you

| Area | Use it for |
| --- | --- |
| Budgets | Reserve capacity before work, then commit actual usage or release unused capacity. |
| Limits | Coordinate distributed concurrency or quota across workers and shards. |
| Effects | Fence external calls, record whether they were reserved, confirmed, failed, or compensated. |
| Approvals | Pause a workflow until a human or control plane approves or rejects it. |
| Circuits | Stop known-bad work early and record why it did not run. |
| Ledger | Debug exactly why a flow paused, retried, skipped, or called an external system. |

## Budget lifecycle

Use budgets when a handler needs a bounded resource such as LLM tokens.

```python
reservation = client.budget_reserve(
    "tenant:acme:llm:daily",
    10_000,
    limit=1_000_000,
    window_ms=86_400_000,
)

committed = False
try:
    result = call_model(max_tokens=reservation.reserved_amount)
    client.budget_commit(
        reservation.scope,
        reservation.reservation_id,
        result.usage.total_tokens,
        usage={
            "model": result.model,
            "input_tokens": result.usage.input_tokens,
            "output_tokens": result.usage.output_tokens,
        },
    )
    committed = True
finally:
    if not committed:
        client.budget_release(reservation.scope, reservation.reservation_id)
```

`budget_commit(...)` records actual usage. If actual usage is lower than the
reservation, the unused amount is released. If actual usage is higher, the
overage is recorded and later reservations can be denied until the window resets.

For workflow handlers, prefer the high-level context manager:

```python
@workflow.state("call_model")
def call_model(ctx):
    with ctx.budget("tenant:acme:llm:daily", 10_000, limit=1_000_000) as budget:
        result = call_llm(ctx.payload)
        budget.commit(result.tokens, usage={"tokens": result.tokens})

    return complete(result=result.text)
```

## Effects

Use effects when a workflow calls an external system and you need a durable audit
record plus lease/fencing validation.

```python
effect = client.effect_reserve(
    job.id,
    "stripe-charge",
    "payment.charge",
    partition_key=job.partition_key,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    operation_digest="sha256:...",
)

try:
    external_id = charge_card()
    client.effect_confirm(job.id, "stripe-charge", external_id=external_id)
except Exception as exc:
    client.effect_fail(job.id, "stripe-charge", error=str(exc))
    raise
```

If you use raw effect calls and circuit latency rules, pass `latency_ms` to
`effect_confirm(...)` or `effect_fail(...)`. The workflow `ctx.effect(...)`
decorator measures and sends it automatically.

Effect, approval, schedule, circuit, budget, and overview calls return typed result
objects for autocomplete:

```python
effect.status
approval.flow_id
circuit.status
budget.remaining
overview.counts
```

They also support dict-style access for escape-hatch fields:

```python
effect["status"]
approval.get("reason")
```

The typed governance result classes are `EffectResult`, `ApprovalResult`,
`CircuitBreakerStatus`, `BudgetResult`, and `GovernanceOverview`.

## Approvals

Use approvals when a workflow must wait for a human decision.

```python
approval = client.approval_request(
    "approval:order-1",
    flow_id="order-1",
    scope="tenant:acme",
    reason="manual fraud review",
    requested_by="fraud-worker",
    assignees=["ops"],
    timeout_ms=30 * 60 * 1000,
)

client.approval_approve(approval["id"], approver="ops-user", reason="verified")
```

Rejected or expired approvals should transition the flow to an explicit state
such as `approval_rejected`, `manual_review_timeout`, or `failed`.

## Distributed limits

Use limits when multiple workers share a bounded resource.

```python
lease = client.limit_lease(
    "tenant:acme:email",
    shard_id=0,
    amount=50,
    limit=500,
    ttl_ms=30_000,
)

spent_amount = 10
spent = None
try:
    spent = client.limit_spend("tenant:acme:email", shard_id=0, amount=spent_amount)
    send_batch(spent_amount)
finally:
    if spent is not None:
        client.limit_release(
            "tenant:acme:email",
            shard_id=0,
            reservation_ids=spent["reservation_ids"],
        )
```

Release must use the exact reservation IDs returned by `limit_spend`; amount-only
release is not safe and is not part of the FerricStore 0.9.1 contract. The worker
fast path can use cached credits. Durable `in_use` can remain visible until
credits are reused, released, or reclaimed after expiry; this is expected and
protects correctness under worker death.

## Circuits

Use circuits to stop known-bad work before it burns retries.

In workflow handlers, prefer `ctx.effect(...)`. It reserves the effect before
the external call, confirms it on success, and fails it on exception. Circuit
policy is evaluated during reservation.

```python
@workflow.state("charge")
def charge(ctx):
    @ctx.effect(
        "stripe-charge",
        "payment.charge",
        operation_digest=f"stripe:{ctx.id}:v1",
        governance_scope=f"tenant:{ctx.partition_key}:stripe",
        external_id=lambda result: result["charge_id"],
    )
    def call_stripe():
        return stripe.charge(ctx.value("order"))

    result = call_stripe()
    return transition("email_receipt", values={"charge": result})
```

Async workflows use the same shape:

```python
@workflow.state("charge")
async def charge(ctx):
    @ctx.effect("stripe-charge", "payment.charge", operation_digest=f"stripe:{ctx.id}:v1")
    async def call_stripe():
        return await stripe.charge(await ctx.value("order"))

    await call_stripe()
    return transition("email_receipt")
```

Operators can also force a circuit open or closed:

```python
status = client.circuit_open("payment-api", open_ms=30_000, failure_threshold=5)
assert status.status == "open"

client.circuit_close("payment-api")
```

When a Flow policy includes a circuit for an effect type, failed effects
increment that circuit automatically. Once the failure threshold is reached,
new effect reservations fail with `GOVERNANCE_CIRCUIT_OPEN` until the retry
window passes. After the window, bounded half-open probe reservations are
allowed; enough confirmed probes close the circuit and reset failures.

The default circuit mode is failure-driven for the hot path. Clean successful
effects do not write circuit state while the circuit is closed. A success writes
only when it resets prior failures, completes a half-open probe, or when exact
failure-rate tracking is explicitly enabled.

Circuit policy can also use sliding-window failure-rate and latency rules:

```python
client.policy_set(
    "order",
    governance={
        "effects": {"allowed": ["payment.charge"]},
        "circuits": {
            "payment.charge": {
                "scope": "tenant:acme:stripe",
                "open_ms": 30_000,
                "failure_threshold": 100,
                "window_ms": 60_000,
                "min_calls": 20,
                "failure_rate_pct": 50,
                "latency_threshold_ms": 2_000,
                "error_classes": ["TimeoutError", "ConnectionError"],
                "half_open_max_probes": 2,
                "half_open_success_threshold": 2,
            }
        },
    },
)
```

`ctx.effect(...)` measures handler call latency automatically. A confirmed
effect above `latency_threshold_ms` is counted as a slow-call failure for the
circuit, but still leaves the effect itself confirmed. Failed effects can pass
an error class through the SDK exception wrapper; only classes listed in
`error_classes` count toward the circuit when the list is present.

`failure_rate_pct` is the advanced exact-rate mode. It tracks successes and
failures in the configured window, so it costs more than the default
failure-driven breaker. Use it only when the policy needs a true
failures-per-total-calls ratio.

After `open_ms`, the circuit enters half-open by allowing a bounded number of
probe reservations. `half_open_max_probes` controls concurrent probes and
`half_open_success_threshold` controls how many successful probes close the
circuit. A failed or slow probe opens it again.

Manual `circuit_open` and `circuit_close` are still available for operator
intervention. Circuit decisions are visible in the governance ledger and
dashboard. The dashboard governance page shows the current circuit table, a
small status graph, and a recent circuit timeline with open, close, failure,
slow-call, ignored-failure, and half-open probe events.

## Errors

Governance denials include both a stable public `code` and a shorter `reason`.

| Code | Reason |
| --- | --- |
| `GOVERNANCE_LIMIT_EXCEEDED` | `limit_exhausted` |
| `GOVERNANCE_BUDGET_EXHAUSTED` | `budget_exhausted` |
| `GOVERNANCE_APPROVAL_REQUIRED` | `approval_required` |
| `GOVERNANCE_EFFECT_DENIED` | `effect_denied` |
| `GOVERNANCE_CIRCUIT_OPEN` | `circuit_open` |
| `GOVERNANCE_UNAVAILABLE` | `governance_unavailable` |
| `GOVERNANCE_CONFLICT` | `governance_conflict` |

## Read and debug

```python
client.governance_ledger("order-1", rev=True, limit=100)
client.approval_list(scope="tenant:acme", status="pending", limit=100)
client.governance_overview(scope="tenant:acme")
client.budget_list(scope="tenant:acme")
client.limit_list(scope="tenant:acme")
```

The dashboard governance page shows approvals, budgets, limits, counts, and
circuits. Operators can filter by approval status and circuit status, manually
open/close circuit scopes, view a small per-scope circuit status graph, and
inspect the recent circuit event timeline.

## Telemetry

FerricStore emits governance telemetry events under:

```text
[:ferricstore, :flow, :governance, action]
```

Common actions include:

```text
budget_reserve
budget_commit
budget_release
limit_lease
limit_spend
limit_release
limit_reclaim
limit_cache_hit
limit_cache_miss
approval_request
approval_approve
approval_reject
effect_reserve
effect_confirmed
effect_failed
effect_compensated
circuit_open
circuit_close
circuit_failure
circuit_success
```

Each event includes `status`; errors include `code` when available. Circuit
events include the circuit `scope`, `circuit_status`, current `failure_count`,
thresholds, window settings, latency threshold, and retry timing when available.
