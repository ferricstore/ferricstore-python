# Command Executors

`FlowClient.from_url(...)`, `QueueClient.from_url(...)`, and
`WorkflowClient.from_url(...)` select a network adapter by URL scheme while
keeping the command layer unchanged.

Internally the high-level clients depend on a tiny executor protocol:

```python
class CommandExecutor(Protocol):
    def execute_command(self, *args): ...
```

Any advanced embedding or test double can be used if it implements `execute_command`. The executor sends FerricStore command frames.

## Built-in adapters

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

Through a FerricStore HTTP endpoint:

```python
client = WorkflowClient.from_url(
    "https://ferricstore.example.com",
    bearer_token="http-token",
    max_connections=8,
)
```

One `command(...)` call becomes one `POST /v1/commands` request. One SDK
`pipeline()` becomes one HTTP command batch. The endpoint may be the in-process
`ferricstore-http` server or a compatible gateway. The HTTP adapter keeps a bounded pool of persistent
HTTP/TLS connections and closes it with the SDK client. Its timeout is one
operation deadline, including local capacity waits and redirects. Async clients
use their own bounded worker executor rather than consuming the event loop's
default executor while waiting for HTTP capacity.

Optional HTTP/2 keeps the same command behavior while allowing concurrent
requests to share fewer physical TLS connections:

```bash
pip install 'ferricstore[http2]'
```

```python
client = WorkflowClient.from_url(
    "https://ferricstore.example.com",
    http2=True,
    max_connections=1,
    max_concurrent_requests=100,
)
```

The two limits are intentionally separate: `max_connections` is socket capacity,
while `max_concurrent_requests` is application backpressure.

For measured burst workloads, `coalesce_window_ms` and `coalesce_max_items` can
combine concurrent single-command calls into ordered HTTP batches. The feature
is opt-in so latency-sensitive calls do not wait for a batching window. Per-call
results and errors remain isolated even when the wire request is shared.

The optional `ferricstore[compact]` extra enables `compact=True`, which replaces
the JSON command envelope with MessagePack and carries arbitrary bytes natively.
It changes serialization only; command ordering and response/error behavior are
the same.

### HTTP transport scope

HTTP is intentionally a bounded request/response transport, not a native
session tunnel.

- Ordinary stateless commands and ordered pipelines are supported through the
  same high-level clients and command methods used by the native transport.
- Pub/Sub subscriptions are not supported because they require a live pushed
  connection. `WATCH`/`MULTI` transactions and direct `AUTH` are also
  native-only because their state belongs to one persistent native connection
  across multiple calls. The HTTP endpoint authenticates before dispatching
  commands instead.
- Topology discovery and routing belong to the HTTP endpoint, not to the SDK
  caller. Native query deadline metadata is not currently exposed
  by the HTTP command endpoint.
- The command envelope is JSON with a versioned binary-safe layer. The SDK
  requests that encoding for every command because even a text-only `GET` can
  return arbitrary bytes. It tags bytes with strict Base64 and represents maps
  as encoded key/value pairs; the endpoint decodes them before command dispatch
  and tags response values the same way. Arbitrary bytes and binary map keys
  therefore round-trip without changing the command API. Plain non-SDK JSON
  clients remain supported by the endpoint's legacy envelope.
- Standard HTTP redirects remain enabled. A deployment that adds redirects or
  gateways is responsible for ensuring that authorization headers are sent
  only to destinations it trusts.

Blocking commands are native-only in the current stateless HTTP gateway. Their
wait state requires a persistent native session and must not occupy an HTTP
request or shared execution batch.

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
