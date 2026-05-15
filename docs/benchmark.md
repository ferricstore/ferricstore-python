# DBOS-Style Benchmark

`examples/dbos_style_benchmark.py` has two modes:

* `queued`: throughput benchmark, closer to DBOS queued workflow numbers.
* `serial-latency`: old single-workflow latency benchmark.

The DBOS published benchmark reports throughput, not one serial workflow:

* point writes/sec
* direct workflows/sec
* queued workflows/sec
* queued workflows/sec with multiple queues/partitions

FerricFlow queued mapping:

```text
FLOW.CREATE pipelined, or FLOW.CREATE_MANY
workers:
  FLOW.CLAIM_DUE limit=N
  FLOW.COMPLETE pipelined, or FLOW.COMPLETE_MANY
```

Default run:

```bash
python examples/dbos_style_benchmark.py \
  --mode queued \
  --flows 10000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 100 \
  --transport pipeline
```

Output:

```text
{
  "mode": "queued",
  "flows": 10000,
  "workers": 16,
  "partitions": 16,
  "process_flows_per_sec": ...,
  "end_to_end_flows_per_sec": ...
}
```

Important options:

* `--claim-batch-size`: `FLOW.CLAIM_DUE LIMIT`, default `100`.
* `--create-batch-size`: `FLOW.CREATE_MANY` size, default `100`.
* `--transport pipeline`: use redis-py pipeline for `FLOW.CREATE` and
  `FLOW.COMPLETE`. This matches common Redis-client usage.
* `--transport many`: use FerricFlow batch commands `FLOW.CREATE_MANY` and
  `FLOW.COMPLETE_MANY`.
* `--workers`: concurrent claim/complete workers.
* `--producers`: concurrent create workers.
* `--partitions`: partition keys used for shard parallelism.
* `--claim-any`: claim globally instead of partition polling. Without this,
  workers poll assigned partitions. If workers are fewer than partitions, they
  round-robin remaining partitions.
* `--no-complete-batch`: use single `FLOW.COMPLETE` instead of `FLOW.COMPLETE_MANY`.
* `--payload-bytes`: raw payload bytes per flow.
* `--work-command incr`: add one `INCR` per claimed flow.

Serial latency mode:

```text
FLOW.CREATE
for step in 1..N:
  FLOW.CLAIM_DUE
  INCR counter
  FLOW.TRANSITION
last step:
  FLOW.COMPLETE
```

For `N = 10`, one workflow execution sends:

* 1 `FLOW.CREATE`
* 10 `FLOW.CLAIM_DUE`
* 10 `INCR`
* 9 `FLOW.TRANSITION`
* 1 `FLOW.COMPLETE`

Total: 31 FerricStore commands.

The benchmark uses ack-only mutators (`return_record=False`) for create,
transition, and complete. That matches the command count above and avoids
measuring extra post-mutation `FLOW.GET` calls.

```bash
python examples/dbos_style_benchmark.py --mode serial-latency --steps 10 --iterations 100
```

## Why `INCR`

`serial-latency` always uses `INCR`, matching the old step benchmark.
`queued` defaults to no-op handlers for closer queued-workflow throughput. Use
`--work-command incr` when you want one extra durable mutation per flow.
