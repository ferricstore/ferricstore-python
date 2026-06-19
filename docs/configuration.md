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
claim traffic reuses the same protocol connection
```

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
| `claim_connections` | RESP claim pool | RESP claim pool | RESP claim pool | RESP claim pool |
| `batch_size` | Yes | Yes | Yes | Yes |
| `lease_ms` | Yes | State-level for workflow handlers | No | No |
| `priority` | Yes | Yes | No | Yes |
| `claim_values` | Yes | State-level for workflow handlers | Yes | Yes |
| `value_max_bytes` | Yes | State-level / `ValueConfig` | Yes | Yes |
| `exception_policy` | Yes | Yes | Yes | Yes |
| `complete_async_depth` | Yes | No | No | No |
| `apply_async_depth` | No | Yes | No | No |
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
)
```

For `redis://` / `rediss://`, high-level `from_url(...)` methods pass unknown
keyword arguments to `redis-py`:

```python
client = QueueClient.from_url(
    "rediss://app_user:secret@ferricstore.service:6380/0",
    socket_connect_timeout=2,
    socket_timeout=10,
    health_check_interval=30,
    max_connections=128,
)
```

This supports normal Redis URL auth, ACL username/password, TLS, Sentinel/proxy
options, and pool settings supported by `redis-py`.

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
