# FerricStore Python SDK

Python SDK for FerricStore and FerricFlow.

The SDK gives Python apps a typed wrapper around FerricStore Flow commands and a
small workflow DSL. It is intentionally explicit: workflows are state pipelines,
not hidden replay engines. That keeps handlers easy to reason about, easy to test,
and close to the command semantics FerricStore actually stores.

## What It Provides

* `FlowClient`: low-level typed client for Flow commands.
* `Workflow`: class-based state workflow helper.
* `@state`: decorator for state handlers.
* `Worker`: polling worker for one workflow.
* `RedisAdapter`: default adapter for `redis-py`.
* `RedisCommandExecutor`: Protocol for other Redis clients.
* `RawCodec`: default raw bytes payload codec.
* `JsonCodec`: optional JSON payload codec.

## Flow Command Coverage

`FlowClient` supports the full FerricStore Flow command surface exposed over RESP3:

* create: `create`, `create_many`
* value refs: `value_put`
* claiming/leases: `claim_due`, `reclaim`, `extend_lease`
* mutations: `transition`, `transition_many`, `complete`, `complete_many`,
  `retry`, `retry_many`, `fail`, `fail_many`, `cancel`, `cancel_many`, `rewind`
* children: `spawn_children`
* reads/queries: `get`, `history`, `list`, `terminals`, `failures`,
  `by_parent`, `by_root`, `by_correlation`, `info`, `stuck`
* policy/retention: `install_policy`, `policy_get`, `retention_cleanup`

The workflow DSL wraps common create/claim/handle/query paths. Advanced batch and
admin operations remain available through `workflow.client`.

## Install

Development install:

```bash
cd /Users/yoavgea/repos/ferricstore-python
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

Runtime install, once published:

```bash
pip install ferricstore
```

## Quick Start

```python
from ferricstore import FlowClient

client = FlowClient.from_url("redis://127.0.0.1:6379/0")

flow = client.create(
    "order-1",
    type="order",
    state="created",
    partition_key="tenant-a:order-1",
    payload=b"raw bytes",
)

jobs = client.claim_due(
    "order",
    state="created",
    worker="worker-1",
    partition_key=flow.partition_key,
    limit=1,
)

job = jobs[0]

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

## Workflow DSL

```python
from ferricstore import FlowClient, Workflow, complete, state, transition


class OrderWorkflow(Workflow):
    type = "order"
    initial_state = "created"
    partition_by = ("tenant_id", "order_id")

    @state("created", lease_ms=30_000, claim_payload=True, on_error="fail")
    def created(self, job):
        return transition("charged", payload=job.payload)

    @state("charged", lease_ms=30_000, claim_payload=True, on_error="fail")
    def charged(self, job):
        return complete(result=b"ok")


client = FlowClient.from_url("redis://127.0.0.1:6379/0")
workflow = OrderWorkflow(client)

record = workflow.create(
    "order-1",
    tenant_id="tenant-a",
    order_id="order-1",
    payload=b"order payload",
)

workflow.run_once("created", worker="worker-1", partition_key=record.partition_key)
workflow.run_once("charged", worker="worker-1", partition_key=record.partition_key)
```

## Docs

* [Concepts](docs/concepts.md)
* [Client API](docs/client.md)
* [Workflow DSL](docs/workflow.md)
* [Worker](docs/worker.md)
* [Payload Codecs](docs/codecs.md)
* [Redis Adapters](docs/adapters.md)
* [Children and Fanout](docs/children.md)
* [Retry and Errors](docs/retry.md)
* [Benchmark Example](docs/benchmark.md)
* [Testing](docs/testing.md)

## Examples

* `examples/order_workflow.py`: simple two-state workflow.
* `examples/dbos_style_benchmark.py`: DBOS-style sequential-step benchmark.

## Current Scope

This repo is an SDK layer. FerricStore remains source of truth. The SDK does not
run a separate workflow database and does not implement deterministic replay.
Handlers should be idempotent because claimed work may be retried after lease
expiry or worker crash.
