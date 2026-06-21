# Troubleshooting

This page covers common SDK issues.

## No jobs are claimed

Check:

- flow `type` matches worker `type`
- flow state matches worker `state` or `states`
- `run_at_ms` is due
- partition key matches the worker claim partition
- lease has expired if the flow is already running
- priority selector matches the flow priority

Useful commands:

```python
record = client.get(flow_id, partition_key=partition_key)
history = client.history(flow_id, partition_key=partition_key, count=50)
info = client.info(record.type)
```

## Job processed twice

This can happen after worker crash plus lease reclaim. Fix the handler, not the
queue:

- use external idempotency keys
- set `lease_ms` above handler p99
- call `extend_lease` for long work
- keep side effects tied to `flow_id` and logical state

## Stale lease or fencing error

The worker no longer owns the job.

Fix:

- do not reuse old `ClaimedFlow` values
- always pass `job.partition_key`
- increase or extend leases
- investigate slow handlers

## Large value omitted

The SDK/server avoided hydrating a value above the requested cap.

Fix:

```python
record = client.get(
    flow_id,
    partition_key=partition_key,
    values=["order"],
    value_max_bytes=512 * 1024,
)
```

Use named values and request only what the handler needs.

## High empty-claim rate

Likely causes:

- workers polling wrong partitions
- state selector too broad
- too many workers for current backlog
- producers not using the same flow type/state

Fix:

- use explicit `state` or `states`
- use `QueueClient.queue(...).worker(...)`
- increase idle sleep for low-volume queues
- opt into `FLOW.CLAIM_DUE ... BLOCK` only for low-volume queues; hot workers
  should prefer non-blocking claims plus short idle backoff

## Async app is slow

Check:

- use `AsyncQueueClient` / `AsyncWorkflowClient`, not sync clients
- set enough `max_connections`
- use `enqueue_many` for bursts
- bound downstream concurrency with semaphores

## Sync app is slow

Check:

- keep hot mutators ack-only; use `return_record=True` only when needed
- use `claim_due(..., include_record=False)` when full records are not needed
- avoid payload hydration in hot handlers
- use `enqueue_many` for producer bursts
- increase workers only until server/downstream latency rises

## Connection errors

Set timeouts and pool size:

```python
from ferricstore import WorkflowClient

client = WorkflowClient.from_url(
    url,
    socket_connect_timeout=2,
    socket_timeout=10,
    health_check_interval=30,
    max_connections=128,
)
```

If errors persist, reduce worker concurrency or add server capacity.

## History looks stale

Current Flow state is source of truth. Some projections can be asynchronous.

Use:

```python
record = client.get(flow_id, partition_key=partition_key)
events = client.history(flow_id, partition_key=partition_key, count=50)
```

For investigations, collect current state, history, lease deadline, next run
time, priority, attempts, and recent errors.
