# Redis Adapters

The SDK depends on a tiny protocol:

```python
class RedisCommandExecutor(Protocol):
    def execute_command(self, *args): ...
```

Any client can be adapted if it can send raw Redis commands.

## Default: redis-py

`redis-py` is the standard Python Redis client. The SDK uses it by default:

```python
from ferricstore import WorkflowClient

client = WorkflowClient.from_url("redis://127.0.0.1:6379/0")
```

This sets:

```python
protocol=3
decode_responses=False
```

RESP3 is important because FerricStore returns maps for Flow records.

Production clients should pass normal `redis-py` connection options through
`from_url`:

```python
client = WorkflowClient.from_url(
    "redis://ferricstore.service:6379/0",
    socket_connect_timeout=2,
    socket_timeout=10,
    health_check_interval=30,
    max_connections=128,
)
```

Keep `protocol=3` and `decode_responses=False`. Size `max_connections` for the
number of producer threads, worker threads, and async completion clients in the
process.

For asyncio services, use `AsyncWorkflowClient.from_url(...)` or
`AsyncQueueClient.from_url(...)` instead of wrapping the sync client in thread
executors. Use `AsyncFlowClient` directly only for low-level command control.

## Custom Adapter

```python
from ferricstore import FlowClient


class MyRedisAdapter:
    def execute_command(self, *args):
        return my_client.execute_command(*args)


client = FlowClient(MyRedisAdapter())
```

## Test Adapter

Unit tests should use fake adapters:

```python
class FakeRedis:
    def __init__(self):
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        return {b"id": b"f1", b"type": b"order", b"state": b"created"}
```

This makes workflow code testable without starting FerricStore.
