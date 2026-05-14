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

## Production Notes

Use multiple worker processes for parallelism. Partition keys decide ordering:

* same partition key: ordered on same shard
* different partition keys: parallel work

Keep handlers idempotent. Worker crash can cause a later reclaim.

