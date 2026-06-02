# Testing

Test handlers as normal Python functions first. Use FerricStore integration tests
only for command semantics, leases, retries, and history.

## Unit-test queue handlers

Queue handlers receive a claimed job object. You can build a small fake:

```python
from dataclasses import dataclass


@dataclass
class FakeJob:
    id: str
    payload: dict
    partition_key: str | None = None
    lease_token: bytes = b"lease"
    fencing_token: int = 1


def test_send_email_handler():
    job = FakeJob("email-1", {"to": "a@example.com", "template": "welcome"})

    result = send_email(job)

    assert result == b"sent"
```

## Unit-test workflow handlers

Workflow handlers return outcomes. Assert the returned outcome, not server
state.

```python
from ferricstore import Transition


def test_created_handler_moves_to_charged():
    job = FakeJob("order-1", {"amount": 10})

    result = created(job)

    assert isinstance(result, Transition)
    assert result.to_state == "charged"
```

## Test command construction

Use a fake Redis executor when you want to assert command shape.

```python
from ferricstore import FlowClient, WorkflowClient


class FakeRedis:
    def __init__(self):
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        return b"OK"


def test_workflow_start_command():
    redis = FakeRedis()
    client = WorkflowClient(FlowClient(redis))
    orders = client.workflow(type="order", initial_state="created")

    orders.start("order-1", payload=b"p")

    assert redis.calls[0][:4] == (
        "FLOW.CREATE",
        "order-1",
        "TYPE",
        "order",
    )
```

## Run one worker iteration

Use `run_once` to test worker behavior without an infinite loop.

```python
def test_worker_completes_one_job():
    result = emails.worker(batch_size=1).run_once(send_email)

    assert result.completed == 1
```

For workflow workers:

```python
result = workflow.worker(batch_size=1).run_once()
assert result.applied == 1
```

## Integration tests

Use a real local FerricStore when testing:

- create/start
- claim due
- transition
- complete/fail/retry
- lease reclaim
- history
- named value hydration
- idempotent create

Example:

```python
from ferricstore import WorkflowClient, complete, transition


def test_order_workflow_integration():
    client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
    order = client.workflow(type="test_order", initial_state="created")

    @order.state("created")
    def created(job):
        return transition("charged")

    @order.state("charged")
    def charged(job):
        return complete(result=b"ok")

    order.start("test-order-1", payload=b"p", idempotent=True)
    assert order.worker(state="created", batch_size=1).run_once().applied == 1
    assert order.worker(state="charged", batch_size=1).run_once().applied == 1
```

## Failure and retry tests

Use `ExceptionPolicy.RAISE` when a test should fail fast:

```python
from ferricstore import ExceptionPolicy

worker = emails.worker(exception_policy=ExceptionPolicy.RAISE)
```

Use `ExceptionPolicy.RETRY` when testing retry mutation:

```python
worker = emails.worker(exception_policy=ExceptionPolicy.RETRY)
result = worker.run_once(handler_that_raises)
assert result.retried == 1
```

## Performance tests

Keep performance tests separate from unit tests.

Use:

- fixed payload size
- fixed shard count
- clear warmup
- enough flows to amortize startup
- p50/p95/p99 latency
- server-side and client-side timing
- separate producer throughput and processing throughput

Avoid:

- sharing dirty data directories between runs
- changing client concurrency between comparisons
- comparing sync and async without matching pool size
- benchmarking with a handler that does real external I/O unless that is the goal
