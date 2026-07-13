# Async APIs

Use async clients when the application already runs on `asyncio`.

| Workload | Client |
| --- | --- |
| Durable queue | `AsyncQueueClient` |
| Explicit state machine | `AsyncWorkflowClient` |
| Low-level command control | `AsyncFlowClient` |

Do not wrap sync clients in event-loop thread executors unless you have a
specific reason.

## Async queue

```python
import asyncio

from ferricstore import AsyncQueueClient


async def main():
    client = AsyncQueueClient.from_url("ferric://127.0.0.1:6388")
    emails = client.queue(type="email")

    async def send_email(job):
        await provider_send_email(job.payload)

    await emails.worker().run(send_email)


asyncio.run(main())
```

For hot protocol-backed queues, the async worker supports the same scheduling
fast paths as the sync worker:

```python
worker = emails.worker(
    max_idle_sleep_s=0.05,
    protocol_wake_hints=True,
    fuse_complete_claim=True,
)
await worker.run(send_email)
```

Wake hints replace idle polling when supported. Fused completion-and-claim sends
the completed batch and the next claim in one protocol round trip. Both options
are explicit so deployments can enable them after validating their server
version and workload.

Fusion is active only inside `run()`/`run_forever()`. A standalone `run_once()`
always completes its claimed jobs without prefetching another leased batch, so
the caller never inherits hidden outstanding leases. Wake subscriptions are
replayed when a protocol adapter reconnects or a topology pool adds a leader,
including every connection in a pooled leader endpoint.

Async worker shutdown uses one deadline across the running task and owned
clients:

```python
worker.stop()
await worker.close(timeout=30)
```

Once a blocking claim has been sent, shutdown waits for its response instead of
cancelling it: the server may already have committed leases in that response.
If the deadline expires, `close()` raises `TimeoutError` without cancelling the
claim; call it again after the claim returns.

`AsyncQueueFlow.close()` follows the same retryable rule for a composite queue:
it closes every worker before closing owned command/claim clients. Cancelling
the caller does not cancel that ordered shutdown. A worker timeout leaves the
clients and ownership intact so a later `close()` can finish safely; a queue
that has begun closing cannot be restarted.

`AsyncQueueClient.close()` and `AsyncWorkflowClient.close()` also share an
in-flight close operation across callers. Cancelling one caller does not detach
later callers from cleanup, and a failed owned resource remains available for a
subsequent close retry while resources already closed are not closed twice.

Native async socket and topology-pool close calls follow the same ownership
rule. Cancelling one close waiter does not cancel transport cleanup, and a later
waiter rejoins it. If one topology adapter fails to close, only that adapter is
retained for the next retry. Topology wake-subscription broadcasts share a
global concurrency limit of 16, including connections nested inside endpoint
pools.

## Async workflow

```python
import asyncio

from ferricstore import AsyncWorkflowClient, complete, transition


async def main():
    client = AsyncWorkflowClient.from_url("ferric://127.0.0.1:6388")
    order = client.workflow(
        type="order",
        states=["created", "charged"],
        initial_state="created",
        partition_by=("tenant_id", "order_id"),
    )

    @order.state("created")
    async def created(job):
        await charge(job.payload)
        return transition("charged")

    @order.state("charged")
    async def charged(job):
        await send_receipt(job.id)
        return complete(result=b"ok")

    await order.start_flow(
        "order-1",
        payload=b"...",
        tenant_id="tenant-a",
        order_id="order-1",
    )
    await order.run()


asyncio.run(main())
```

`start_flow(id, ...)` creates a Flow, while `start_workers()` starts the local
consumer tasks. `run()` is the convenience form that starts consumers and joins
them. The older overloaded `start(...)` spelling remains for compatibility but
is deprecated because its return type depended on whether an ID was supplied.
Registering two handlers for the same state raises `ValueError` instead of
silently replacing the first handler.

`partition_by` has the same routing behavior in sync and async workflows. An
explicit `partition_key=...` takes precedence; otherwise the configured values
are joined in order and removed from the command options. The same rule applies
to `enqueue_many()` and `run_steps_many()`.

`AsyncWorkflow.close()` is terminal and cancellation-safe. It waits for worker
tasks before owned transports, retains failed resource ownership for a retry,
and rejects new starts once closing begins.

## Async producers

For async web apps, create one client per event loop. With `ferric://`, the
default is one multiplexed protocol connection with 8 request lanes.

```python
from ferricstore import AsyncQueueClient


client = AsyncQueueClient.from_url("ferric://ferricstore.service:6388", timeout=10)
emails = client.queue(type="email")


async def create_email(req: dict):
    flow_id = f"email:{req['id']}"
    await emails.enqueue(flow_id, payload=req, idempotent=True)
    return {"id": flow_id, "status": "queued"}
```

## Common mistakes

| Mistake | Fix |
| --- | --- |
| Using sync client in async route | Use `AsyncQueueClient` / `AsyncWorkflowClient`. |
| Protocol client saturation | Increase `lanes` first; use `max_connections` only after measuring. |
| Native pool saturation | Increase `lanes` first; use `max_connections` only after measuring. |
| Unbounded handler concurrency | Set `WorkerConfig(concurrency=...)`. |
| Claiming values the handler does not need | Use explicit `claim_values`. |
| Running worker in serverless handler | Enqueue in serverless, run worker elsewhere. |
