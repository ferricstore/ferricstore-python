# If You Know Celery, Temporal, or DBOS

This page maps common durable-work concepts to FerricFlow.

## Short version

| If you know | FerricFlow equivalent |
| --- | --- |
| Celery task | `QueueClient.queue(...).enqueue(...)` |
| Celery worker | `queue.worker(...).run(handler)` |
| Temporal client | `WorkflowClient` / `QueueClient` producer |
| Temporal task queue | Flow `type` plus `state` selector |
| Temporal worker | long-running FerricFlow worker |
| DBOS queued workflow | `QueueClient` |
| DBOS workflow with steps | `WorkflowClient.workflow(...)` with explicit states |

## Compared with Celery

Celery is a task queue. FerricFlow can be used as a durable task queue, but it
also stores state, history, leases, retry policy, named values, and lineage.

```python
client = QueueClient.from_url(url)
emails = client.queue(type="email")

emails.enqueue("email-1", payload=b"...")
emails.worker().run(send_email)
```

Use FerricFlow when you want durable claim/lease/fencing semantics and built-in
state/history around each unit of work.

## Compared with Temporal

Temporal usually has:

```text
client starts workflow on task queue
worker polls task queue and runs workflow/activity code
```

FerricFlow has the same deployment split:

```text
web/serverless producer starts flow
worker claims by type/state and completes/transitions flow
```

The important difference: FerricFlow is an explicit state pipeline, not
deterministic replay.

```python
orders = client.workflow(type="order", initial_state="created")


@orders.state("created")
def created(job):
    return transition("charged")


@orders.state("charged")
def charged(job):
    return complete(result=b"ok")
```

Benefits of explicit states:

| Benefit | Why |
| --- | --- |
| Visible durable boundaries | Every state transition is observable. |
| Easier debugging | Current state and history are direct records. |
| Language-neutral commands | A small command/API surface can be used from any client. |
| Payload control | Large values are opt-in through named values/refs. |

## Compared with DBOS

DBOS makes application code look close to normal functions with durable
execution underneath. FerricFlow favors explicit workflow state that workers
mutate through durable commands:

```python
workflow = client.workflow(type="order", initial_state="reserve")


@workflow.state("reserve")
def reserve(job):
    return transition("charge")


@workflow.state("charge")
def charge(job):
    return complete(result=b"ok")
```

For DBOS-style queued throughput, use `QueueClient`:

```python
queue = client.queue(type="email")
queue.enqueue("email-1", payload=b"...")
queue.worker(batch_size=1000, concurrency=200).run(send_email)
```

## Choosing the API

| Need | Use |
| --- | --- |
| One durable work item | `QueueClient` |
| Explicit business state machine | `WorkflowClient` |
| Async service | `AsyncQueueClient` / `AsyncWorkflowClient` |
| Exact command control | `FlowClient` |
| Large per-flow data | Named values and value refs |
| Fanout/children | `spawn_children` or child workflows |
