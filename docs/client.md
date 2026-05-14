# Client API

`FlowClient` is the low-level typed wrapper around FerricStore Flow commands.

```python
from ferricstore import FlowClient

client = FlowClient.from_url("redis://127.0.0.1:6379/0")
```

`from_url` uses `redis-py` with RESP3:

```python
redis.Redis.from_url(url, protocol=3, decode_responses=False)
```

## `create`

Creates one flow.

```python
record = client.create(
    "flow-1",
    type="order",
    state="created",
    partition_key="tenant-a:order-1",
    payload=b"payload",
    correlation_id="checkout-123",
)
```

Maps to:

```text
FLOW.CREATE flow-1 TYPE order STATE created PARTITION tenant-a:order-1 PAYLOAD ...
```

Important options:

* `type`: required workflow type.
* `state`: initial state, default `queued`.
* `payload`: encoded through codec.
* `partition_key`: shard/order key.
* `parent_flow_id`, `root_flow_id`, `correlation_id`: query lineage.
* `run_at_ms`: due time.
* `priority`: lower/higher depends on server ordering policy.
* `idempotent`: let duplicate create by id return existing result when supported.

## `claim_due`

Claims due work for a type/state.

```python
jobs = client.claim_due(
    "order",
    state="created",
    worker="worker-1",
    partition_key="tenant-a:order-1",
    lease_ms=30_000,
    limit=10,
)
```

Returns `list[FlowRecord]`. Each record includes `lease_token` and
`fencing_token`; pass both into mutation commands.

## `transition`

Moves a claimed job to another state.

```python
client.transition(
    job.id,
    from_state=job.state,
    to_state="charged",
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    payload=b"next payload",
)
```

## `complete`

Closes a flow as completed.

```python
client.complete(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    result=b"ok",
)
```

## `retry`

Schedules retry for claimed job.

```python
client.retry(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    error=b"temporary failure",
    run_at_ms=next_attempt_ms,
)
```

## `fail`

Closes a flow as failed.

```python
client.fail(
    job.id,
    lease_token=job.lease_token,
    fencing_token=job.fencing_token,
    partition_key=job.partition_key,
    error=b"permanent failure",
)
```

## `get`

Reads current flow record.

```python
record = client.get("flow-1", partition_key="tenant-a:order-1")
```

Returns `FlowRecord | None`.

## `history`

Reads recent history.

```python
events = client.history("flow-1", partition_key="tenant-a:order-1", count=100)
```

## `install_policy`

Installs retry policy globally or per state.

```python
from ferricstore import RetryPolicy

client.install_policy(
    "order",
    retry=RetryPolicy(max_retries=3, backoff="fixed", base_ms=100, max_ms=1_000),
    states={
        "charge": RetryPolicy(max_retries=10, backoff="exponential", base_ms=1_000, max_ms=86_400_000),
    },
)
```

