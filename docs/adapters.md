# Command Executors

The SDK is native-protocol first. `FlowClient.from_url(...)`, `QueueClient.from_url(...)`, and `WorkflowClient.from_url(...)` open `ferric://` / `ferrics://` connections.

Internally the high-level clients depend on a tiny executor protocol:

```python
class CommandExecutor(Protocol):
    def execute_command(self, *args): ...
```

Any advanced embedding or test double can be used if it implements `execute_command`. The executor sends FerricStore command frames.

## Default native adapter

```python
from ferricstore import WorkflowClient

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
```

Useful options:

```python
client = WorkflowClient.from_url(
    "ferrics://app_user:secret@ferricstore.service:6389",
    timeout=10,
    max_connections=1,
    lanes=8,
)
```

## Custom executor

```python
from ferricstore import FlowClient


class MyExecutor:
    def execute_command(self, *args):
        return my_transport.send_command(*args)


client = FlowClient(MyExecutor())
```

This minimal executor receives only actual FerricStore command arguments.
Typed query routing hints are local SDK metadata and are not serialized or
passed to `execute_command`.

To support `deadline_ms` on `query`, `explain`, and `explain_analyze`, a custom
executor must also implement the optional query capability:

```python
class MyExecutor:
    def execute_command(self, *args):
        return my_transport.send_command(*args)

    def execute_flow_query_command(self, *args, deadline_ms=None, routing_key=None):
        return my_transport.send_flow_query(
            *args,
            deadline_ms=deadline_ms,
            routing_key=routing_key,
        )
```

The capability must encode `deadline_ms` into the native FLOW.QUERY payload.
`routing_key` is only an endpoint-selection hint and must never be placed in the
wire payload. Without this capability, queries still execute through
`execute_command`; requesting a deadline raises `TypeError` instead of silently
dropping it.

Async clients use the corresponding awaitable capability:

```python
class MyAsyncExecutor:
    async def execute_command(self, *args):
        return await my_transport.send_command(*args)

    async def execute_flow_query_command(self, *args, deadline_ms=None, routing_key=None):
        return await my_transport.send_flow_query(
            *args,
            deadline_ms=deadline_ms,
            routing_key=routing_key,
        )
```

Both async executor methods must return an awaitable. The same deadline and
routing rules apply; a synchronous return is rejected instead of being accepted
as an already-completed query.

## Test executor

Unit tests should use fake executors:

```python
class FakeExecutor:
    def __init__(self):
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        return {b"id": b"f1", b"type": b"order", b"state": b"created"}
```

This makes workflow code testable without starting FerricStore.
