# Concepts

## FerricFlow Model

FerricFlow stores durable workflow records inside FerricStore. A flow record has:

* `id`: unique flow id.
* `type`: workflow type, for example `order`.
* `state`: current state, for example `created`.
* `partition_key`: routing key. Same partition keeps ordering and shard locality.
* `payload`: raw user bytes or codec-decoded value.
* `lease_token`: token returned by `claim_due`.
* `fencing_token`: monotonic token used to reject stale workers.
* lineage fields: `parent_flow_id`, `root_flow_id`, `correlation_id`.

## Command Flow

Typical state transition:

1. `FLOW.CREATE` creates a flow in initial state.
2. Worker calls `FLOW.CLAIM_DUE` for a type/state.
3. FerricStore returns leased jobs.
4. Handler performs work.
5. Handler returns one outcome:
   * transition to next state
   * complete
   * retry
   * fail
6. SDK sends `FLOW.TRANSITION`, `FLOW.COMPLETE`, `FLOW.RETRY`, or `FLOW.FAIL`.

## Why Explicit State Pipeline

Temporal and DBOS let user code look like one sequential function. FerricFlow
chooses explicit states. This makes each durable boundary visible:

```text
created -> charged -> shipped -> completed
```

That is better for:

* audit/debug
* AI/codegen readability
* explicit retries per state
* partition-aware work distribution
* simple Redis/RESP usage from any language

## Idempotency

Handlers must be idempotent. A worker can crash after doing side effects but before
completing a state. When lease expires, another worker may reclaim the job.

Use external idempotency keys, flow ids, or fencing tokens for side effects.

## Partition Keys

Partition key controls shard choice. Use one partition key when order matters:

```python
partition_key = "tenant-a:order-1"
```

Use different partition keys when parallelism matters:

```python
partition_key = "tenant-a:device-123"
```

## Payloads

Payloads are raw bytes by default. The SDK does not JSON encode unless `JsonCodec`
is used. This keeps large payload handling predictable and avoids accidental copies.

