# Web and Serverless Usage

The production shape is:

```text
web server / serverless function = client that starts work
worker service / pod / VM = long-running worker that claims and completes work
both agree on Flow type/state
```

This is the same operational split used by durable execution systems such as
Temporal: request handlers start work; workers poll task queues.

## Shared configuration

Put shared SDK configuration in one module.

`flow_config.py`:

```python
from ferricstore import ExceptionPolicy, JsonCodec, QueueClient, RetryPolicy, ValueConfig, WorkerConfig


def queue_client() -> QueueClient:
    return QueueClient.from_url(
        "redis://ferricstore.service:6379/0",
        codec=JsonCodec(),
        socket_connect_timeout=2,
        socket_timeout=10,
        health_check_interval=30,
        max_connections=32,
        retry_policy=RetryPolicy(max_retries=10, backoff="exponential", base_ms=500),
        worker_config=WorkerConfig(
            concurrency=20,
            batch_size=100,
            lease_ms=30_000,
            idle_sleep_s=0.01,
            exception_policy=ExceptionPolicy.RETRY,
        ),
        value_config=ValueConfig(value_max_bytes=64 * 1024),
    )
```

## FastAPI producer

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

## Worker service

`worker.py`:

```python
from flow_config import queue_client


emails = queue_client().queue(type="email")


def send_email(job):
    payload = job.payload
    provider_send_email(payload["to"], payload["template"])
    return b"sent"


if __name__ == "__main__":
    emails.worker().run(send_email)
```

Run it as a separate service:

```text
Kubernetes Deployment
ECS service
VM/systemd
Nomad job
Docker Compose service
Cloud Run service with enough minimum instances
```

## Serverless producer

Serverless functions should enqueue/start work and return. Do not run a normal
worker loop inside a Lambda-style request handler.

```python
from flow_config import queue_client


emails = queue_client().queue(type="email")


def handler(event, context):
    flow_id = f"email:{event['id']}"
    emails.enqueue(flow_id, payload=event, idempotent=True)
    return {"id": flow_id, "status": "queued"}
```

## Workflow web API

Use `WorkflowClient` when one business object moves through named states.

`order_flow.py`:

```python
from ferricstore import JsonCodec, WorkflowClient, complete, transition


client = WorkflowClient.from_url("redis://ferricstore.service:6379/0", codec=JsonCodec())
orders = client.workflow(type="order", initial_state="created", partition_by=("tenant_id", "order_id"))


def register_handlers():
    @orders.state("created")
    def created(job):
        charge(job.payload)
        return transition("charged")

    @orders.state("charged")
    def charged(job):
        send_receipt(job.id)
        return complete(result={"status": "sent"})
```

`api.py`:

```python
from fastapi import FastAPI

from order_flow import orders


app = FastAPI()


@app.post("/orders")
def create_order(req: dict):
    flow_id = f"order:{req['tenant_id']}:{req['order_id']}"
    orders.start(
        flow_id,
        tenant_id=req["tenant_id"],
        order_id=req["order_id"],
        payload=req,
        idempotent=True,
    )
    return {"id": flow_id, "status": "created"}
```

`worker.py`:

```python
from order_flow import orders, register_handlers


if __name__ == "__main__":
    register_handlers()
    orders.worker(batch_size=1000, idle_sleep_s=0.001).run()
```

## Operational rules

| Rule | Why |
| --- | --- |
| Use deterministic flow ids | Makes request retries idempotent. |
| Keep request handler fast | Durable work happens in workers. |
| Run workers separately | Workers need long-running claim loops. |
| Share client config | Producers and workers use same type/state/policy defaults. |
| Size connection pools | Account for web concurrency and worker completion depth. |
