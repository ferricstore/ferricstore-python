# Configuration

This page explains the SDK configuration model.

The high-level clients are the normal place to put production defaults:

```python
from ferricstore import ExceptionPolicy, QueueClient, RetryPolicy, ValueConfig, WorkerConfig

client = QueueClient.from_url(
    "ferric://ferricstore.service:6388",
    timeout=10,
    retry_policy=RetryPolicy(
        max_retries=10,
        backoff="exponential",
        base_ms=500,
        max_ms=60_000,
        jitter_pct=20,
    ),
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

For `ferric://`, the SDK defaults are latency-first:

```text
one TCP connection
8 multiplexed request lanes
worker facades use a separate one-connection claim pool
```

The separate claim pool keeps blocking claims from consuming command capacity.
Only override `max_connections`, `lanes`, `command_connections`, or
`claim_connections` after profiling shows the client socket or Python process is
the bottleneck.

## Inheritance

Configuration is inherited in this order:

```text
client defaults
-> queue/workflow override
-> worker/state explicit arguments
```

Example:

```python
client = QueueClient.from_url(
    url,
    worker_config=WorkerConfig(batch_size=100, concurrency=20),
)

emails = client.queue(type="email")

# Inherits concurrency=20, overrides batch_size.
worker = emails.worker(batch_size=25)
```

## RetryPolicy

`RetryPolicy` controls durable backoff after `FLOW.RETRY`.

```python
from ferricstore import RetryPolicy, WorkflowClient

client = WorkflowClient.from_url(
    url,
    retry_policy=RetryPolicy(
        max_retries=10,
        backoff="exponential",
        base_ms=500,
        max_ms=60_000,
        jitter_pct=20,
        exhausted_to="failed",
    ),
)

order = client.workflow(type="order", initial_state="charge")
order.install_policy()
```

Override per workflow or state:

```python
order = client.workflow(
    type="order",
    retry_policy=RetryPolicy(max_retries=5),
)

@order.state("charge", retry_policy=RetryPolicy(max_retries=3))
def charge(job):
    ...
```

## BackpressurePolicy

`BackpressurePolicy` controls client-side retries for server-declared overloads on
safe producer writes. It does not retry timeouts or disconnects because their
commit outcome can be unknown.

```python
from ferricstore import BackpressurePolicy, QueueClient

client = QueueClient.from_url(
    url,
    backpressure=BackpressurePolicy(
        max_retries=None,
        max_elapsed_ms=30_000,
        base_delay_ms=5,
        max_delay_ms=500,
    ),
)
```

The default elapsed retry budget is 30 seconds. Shared pressure is coordinated
only within the same transport endpoint or pool, so an overloaded cluster does
not delay clients connected to an unrelated cluster. Set `shared=False` only
when a producer needs an independent retry window.

## ExceptionPolicy

`ExceptionPolicy` controls what the SDK does when Python handler code raises.

```python
from ferricstore import ExceptionPolicy, WorkerConfig

worker_config = WorkerConfig(exception_policy=ExceptionPolicy.RETRY)
```

| Policy | Behavior |
| --- | --- |
| `ExceptionPolicy.RETRY` | Catch handler exception and send `FLOW.RETRY`. |
| `ExceptionPolicy.FAIL` | Catch handler exception and send `FLOW.FAIL`. |
| `ExceptionPolicy.RAISE` | Let exception escape. Useful for tests/supervisors. |

Do not confuse this with `RetryPolicy`:

```text
ExceptionPolicy = what to do when handler raises
RetryPolicy = how FerricStore schedules durable retry
```

## WorkerConfig

`WorkerConfig` stores common worker defaults. Only options supported by the
specific runtime are applied.

Support matrix:

| Option | Sync queue | Sync workflow | Async queue | Async workflow |
| --- | --- | --- | --- | --- |
| `workers` | No | No | Yes | Yes |
| `concurrency` | Yes | No | Yes | Yes |
| `command_connections` | Advanced | Advanced | Advanced | Advanced |
| `claim_connections` | Advanced native claim pool | Advanced native claim pool | Advanced native claim pool | Advanced native claim pool |
| `batch_size` | Yes | Yes | Yes | Yes |
| `lease_ms` | Yes | State-level for workflow handlers | No | No |
| `priority` | Yes | Yes | No | Yes |
| `claim_values` | Yes | State-level for workflow handlers | Yes | Yes |
| `value_max_bytes` | Yes | State-level / `ValueConfig` | Yes | Yes |
| `exception_policy` | Yes | Yes | Yes | Yes |
| `complete_async_depth` | Yes | No | No | No |
| `apply_async_depth` | No | Yes | No | No |
| `max_idle_sleep_s` | Yes | Yes | Yes | No |
| `protocol_wake_hints` | Yes | No | Yes | No |
| `fuse_complete_claim` | Yes | No | Yes | No |
| `server_shards` | No | No | Yes | Yes |
| `producer_loop_thread` | No | No | Yes | Yes |

Unsupported fields are ignored by that facade.

Common sync queue options:

```python
WorkerConfig(
    concurrency=200,
    batch_size=1000,
    lease_ms=30_000,
    claim_values=["template"],
    value_max_bytes=64 * 1024,
    complete_async_depth=4,
    idle_sleep_s=0.001,
)
```

Common workflow options:

```python
WorkerConfig(
    batch_size=1000,
    idle_sleep_s=0.001,
    apply_async_depth=4,
    exception_policy=ExceptionPolicy.RETRY,
)
```

Common async options:

```python
WorkerConfig(
    workers=16,
    concurrency=500,
    batch_size=1000,
    claim_partition_batch_size=16,
    server_shards=16,
    idle_sleep_s=0.001,
    max_idle_sleep_s=0.05,
    protocol_wake_hints=True,
    fuse_complete_claim=True,
)
```

Explicit worker kwargs always win:

```python
emails.worker(batch_size=25)
```

## ValueConfig

`ValueConfig` controls named-value hydration defaults.

```python
from ferricstore import ValueConfig

value_config = ValueConfig(
    value_max_bytes=64 * 1024,
    local_cache=False,
)
```

Rules:

| Setting | Rule |
| --- | --- |
| `value_max_bytes` | Default cap for hydrated named values. |
| `local_cache` | Default per-job in-memory cache for `job.value(...)`. Keep `False` unless repeated reads matter. |

Per-call overrides still work:

```python
invoice = job.value("invoice_pdf", local_cache=True)
```

## Connection options

For `ferric://` / `ferrics://`, high-level `from_url(...)` methods pass protocol
options to the FerricStore protocol adapter:

```python
client = QueueClient.from_url(
    "ferrics://app_user:secret@ferricstore.service:6389",
    timeout=10,
    max_connections=1,
    lanes=8,
    max_inflight_requests=4_096,
    max_pending_request_bytes=64 * 1024 * 1024,
    max_response_chunks=1_024,
    max_event_queue_size=10_000,
    max_decoded_collection_items=100_000,
)
```

Unknown `from_url(...)` keyword arguments are passed to the native protocol
adapter. Keep production services explicit about `timeout`, `max_connections`,
and `lanes`. Protocol push events use a bounded deque; the default
`max_event_queue_size=10_000` fails the connection on overflow instead of
silently losing an event. Set it to `None` only when another layer provides a
strict memory bound.

Each native connection admits at most `max_inflight_requests=4_096` requests
and 64 MiB of cumulative encoded request data by default. Capacity is reserved
before a frame is written and released when its response, timeout, cancellation,
or transport failure removes the request. Exceeding either limit fails locally
without writing the rejected frame. Both options accept `None` to disable that
specific guard, but production services should keep finite bounds. Low-level
protocol adapters expose `pending_request_count` and `pending_request_bytes` for
observability. Sync futures returned by `submit_*` also expire at the adapter's
configured `timeout`; use `timeout=None` only when lifecycle ownership supplies
another bound.

Chunked responses are limited to `max_response_chunks=1_024` by default in
addition to the encoded and decompressed byte limits. This bounds continuation
header processing even when a peer sends many zero-length chunks. Response
bodies are assembled incrementally instead of retaining a second list of all
chunks. Set the limit to `None` only for a trusted peer with a separate
wire-frame bound.

Generic and compact protocol collection counts are charged to one cumulative
budget before Python lists or maps are allocated. The default
`max_decoded_collection_items=100_000` applies across every nesting level and
is independent of the encoded response-byte limits, protecting small frames
from expanding into unbounded object graphs. Exact-cardinality commands such
as pipelines, `MGET`, and Flow many operations are additionally checked against
their request count. Increase the limit only for an application that
intentionally returns larger nested collections; `None` disables this defense.

All protocol byte, event, collection, chunk, and async write-drain limits require
actual integers; booleans, strings, fractional values, and negative values are
rejected. Byte/event/collection limits and `write_drain_bytes` may be zero.
`max_response_chunks` must be positive. `None` is accepted only by optional
limits and explicitly disables that guard.

`max_connections` and `lanes` must be positive. In SHARDS/topology mode,
`max_connections` applies to every discovered endpoint. For example, a
three-endpoint topology with `max_connections=2` can open up to six routed
connections, plus any short-lived affine session connections. Size it with the
per-endpoint meaning in mind.

## Recommended production default

```python
from ferricstore import ExceptionPolicy, QueueClient, RetryPolicy, ValueConfig, WorkerConfig

client = QueueClient.from_url(
    url,
    retry_policy=RetryPolicy(max_retries=10, backoff="exponential", base_ms=500, max_ms=60_000),
    worker_config=WorkerConfig(
        concurrency=200,
        batch_size=1000,
        lease_ms=30_000,
        idle_sleep_s=0.001,
        exception_policy=ExceptionPolicy.RETRY,
        complete_async_depth=4,
    ),
    value_config=ValueConfig(value_max_bytes=64 * 1024),
)
```
