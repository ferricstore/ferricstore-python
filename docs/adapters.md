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
