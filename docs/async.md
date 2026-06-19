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

## Async workflow

```python
import asyncio

from ferricstore import AsyncWorkflowClient, complete, transition


async def main():
    client = AsyncWorkflowClient.from_url("ferric://127.0.0.1:6388")
    order = client.workflow(type="order", states=["created", "charged"], initial_state="created")

    @order.state("created")
    async def created(job):
        await charge(job.payload)
        return transition("charged")

    @order.state("charged")
    async def charged(job):
        await send_receipt(job.id)
        return complete(result=b"ok")

    await order.start_flow("order-1", payload=b"...")
    await order.run()


asyncio.run(main())
```

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
| RESP pool saturation | Increase `max_connections`. |
| Unbounded handler concurrency | Set `WorkerConfig(concurrency=...)`. |
| Claiming values the handler does not need | Use explicit `claim_values`. |
| Running worker in serverless handler | Enqueue in serverless, run worker elsewhere. |
