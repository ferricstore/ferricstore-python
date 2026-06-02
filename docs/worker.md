# Worker

`Worker` is a small polling loop around one workflow.

```python
from ferricstore import Worker

worker = Worker(
    workflow,
    worker="worker-1",
    states=["created", "charged"],
    partition_key=None,
    limit=10,
    idle_sleep_s=0.1,
)

worker.run_forever()
```

## `run_once`

For tests, cron jobs, or custom schedulers:

```python
processed = worker.run_once()
```

Returns number of jobs handled.

## Polling Strategy

FerricFlow uses pull-based workers:

* no required PubSub
* no hidden background lease recovery
* workers decide polling cadence
* `claim_due(reclaim_expired=True)` can reclaim expired leases when desired

Use `block_ms` for production workers so idle queues wait server-side instead of
polling:

```python
worker = queue.worker(
    state="queued",
    concurrency=200,
    batch_size=1000,
    block_ms=5_000,
)
```

Long timers do not require a different worker. FerricStore may hibernate
far-future `run_at_ms` flows out of hot memory; normal workers promote and claim
them when due.

## Production Notes

Use multiple worker processes for parallelism. Partition keys decide ordering:

* same partition key: ordered on same shard
* different partition keys: parallel work

Keep handlers idempotent. Worker crash can cause a later reclaim.

Production checklist:

| Concern | Recommendation |
| --- | --- |
| Lease duration | Set `lease_ms` above handler p99 plus margin. |
| Long handlers | Call `extend_lease` or split work into states. |
| Handler exceptions | Use `exception_policy=ExceptionPolicy.RETRY` for transient failures and `exception_policy=ExceptionPolicy.FAIL` for permanent failures. |
| Hot queues | Prefer `QueueClient.queue(...).worker(...)` over manual claim/complete loops. |
| Idle queues | Set `block_ms=1000-5000` instead of aggressive local polling. |
| Many partitions | Start with `claim_partition_batch_size=16`; increase if claim batches are underfilled. |
| Payloads | Use `claim_values` and `value_max_bytes` instead of hydrating everything. |
| Shutdown | Call `stop()`, `join()`, then `close()` to flush completions. |
| Metrics | Count claimed, completed, retried, failed, empty claims, and handler latency. |

For the full production checklist, see [Production Readiness](production.md).
