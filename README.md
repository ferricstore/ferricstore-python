# FerricStore Python SDK

Python SDK for FerricStore and FerricFlow.

Design goals:

* default transport: `redis-py`, the common Python Redis client
* adapter Protocol so other Redis clients can be plugged in
* RESP3 maps normalized into typed Python dataclasses
* raw payloads by default; optional JSON codec
* workflow ergonomics inspired by Temporal/DBOS, without hidden replay magic
* explicit state pipeline: claim, handle, transition/complete/retry/fail

## Install

```bash
pip install -e ".[dev]"
```

## Client

```python
from ferricstore import FlowClient

client = FlowClient.from_url("redis://127.0.0.1:6379/0")

flow = client.create(
    "order-1",
    type="order",
    state="created",
    partition_key="tenant-a",
    payload=b"raw bytes",
)

jobs = client.claim_due(
    "order",
    state="created",
    worker="worker-1",
    partition_key="tenant-a",
    limit=10,
)

job = jobs[0]
client.transition(
    job.id,
    from_state=job.state,
    to_state="charged",
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
)
```

## Workflow DSL

```python
from ferricstore import FlowClient, Workflow, state, transition, complete


class OrderWorkflow(Workflow):
    type = "order"
    initial_state = "created"
    partition_by = ("tenant_id", "order_id")

    @state("created", lease_ms=30_000, claim_payload=True, on_error="failed")
    def created(self, job):
        return transition("charged", payload=job.payload)

    @state("charged", lease_ms=30_000, claim_payload=True, on_error="failed")
    def charged(self, job):
        return complete(result=b"ok")


client = FlowClient.from_url("redis://127.0.0.1:6379/0")
wf = OrderWorkflow(client)

wf.create(
    "order-1",
    tenant_id="tenant-a",
    order_id="order-1",
    payload=b"raw bytes",
)

wf.run_once("created", worker="worker-1", partition_key="tenant-a:order-1")
wf.run_once("charged", worker="worker-1", partition_key="tenant-a:order-1")
```

## Adapter

The SDK only requires an object with `execute_command(*args)`. `RedisAdapter`
wraps `redis-py`; tests or alternate clients can implement `RedisCommandExecutor`.

```python
from ferricstore import FlowClient, RedisCommandExecutor


class MyAdapter:
    def execute_command(self, *args):
        ...


client = FlowClient(MyAdapter())
```

## DBOS-Style Benchmark

See `examples/dbos_style_benchmark.py` for a Python benchmark shape equivalent to
the public DBOS workflow benchmark:

* HTTP request starts one workflow execution
* each step claims work, increments a counter, then transitions
* final step completes

