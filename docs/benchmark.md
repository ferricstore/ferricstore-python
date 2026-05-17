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
producers and workers run concurrently
producers:
  FLOW.CREATE pipelined, or FLOW.CREATE_MANY
workers:
  owner-wakeup mode assigns partitions to workers
  producer wake for partition P goes only to owner(P)
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

Optimized FerricFlow queued run with explicit partitions:

```bash
python examples/dbos_style_benchmark.py \
  --mode queued \
  --queued-shape live \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 16 \
  --partitions 16 \
  --partition-mode explicit \
  --worker-mode owner-wakeup \
  --claim-batch-size 500 \
  --create-batch-size 500 \
  --no-reclaim-expired \
  --claim-priority 0 \
  --complete-async-depth 4 \
  --claim-job-only
```

Optimized FerricFlow queued run without user partition keys:

```bash
python examples/dbos_style_benchmark.py \
  --mode queued \
  --queued-shape live \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 32 \
  --partitions 16 \
  --partition-mode auto \
  --worker-mode owner-wakeup \
  --claim-batch-size 500 \
  --create-batch-size 500 \
  --no-reclaim-expired \
  --claim-priority 0 \
  --complete-async-depth 4 \
  --claim-job-only
```

Output:

```text
{
  "mode": "queued",
  "queued_shape": "live",
  "flows": 10000,
  "workers": 16,
  "partitions": 16,
  "process_flows_per_sec": ...,
  "end_to_end_flows_per_sec": ...
}
```

Important options:

* `--claim-batch-size`: `FLOW.CLAIM_DUE LIMIT`, default `100`.
  For explicit partition owner-wakeup, `500` currently gives the best local
  throughput. `1000` was slower because it increased empty/partial claim work.
* `--create-batch-size`: create flush size. With `--transport pipeline`, this is Redis pipeline depth for singular `FLOW.CREATE`. With `--transport many`, this is `FLOW.CREATE_MANY` size.
* `--queued-shape live`: producers and workers run concurrently. This is the
  DBOS-style queued benchmark shape.
* `--queued-shape preloaded`: producers finish first, then workers run. This
  isolates `FLOW.CLAIM_DUE` + terminal command throughput.
* `--transport pipeline`: use normal SDK calls over a buffered Redis executor.
  Create uses singular `FLOW.CREATE` in Redis pipeline flushes. Workers call
  `FLOW.CLAIM_DUE` immediately, then flush singular `FLOW.COMPLETE` commands in
  a Redis pipeline. This is the optimized developer-facing path.
* `--transport many`: use FerricFlow batch commands `FLOW.CREATE_MANY` and
  `FLOW.COMPLETE_MANY` with `INDEPENDENT true`, preserving per-item success/failure
  while using the optimized batch path.
* `--workers`: concurrent claim/complete workers.
* `--producers`: concurrent create workers.
* `--partitions`: partition keys used for shard parallelism.
* `--claim-any`: claim globally instead of partition polling. Without this,
  workers poll assigned partitions. If workers are fewer than partitions, they
  round-robin remaining partitions.
* `--partition-mode auto`: create flows without an explicit partition key and
  let FerricFlow spread them across hidden auto buckets. The benchmark and SDK
  group no-partition creates by hidden bucket before flushing `FLOW.CREATE_MANY`,
  so the server receives shard-local batches while application code stays
  partition-free.
* `--partition-mode explicit --worker-mode owner-wakeup`: fastest current
  DBOS-style throughput path. It avoids broad `claim_any` polling and lets each
  partition owner form large claim/complete batches.
* `--no-complete-batch`: use single `FLOW.COMPLETE` instead of `FLOW.COMPLETE_MANY`.
* `--no-reclaim-expired`: disables the extra expired-running reclaim pass before
  normal queued claims. This benchmark defaults to disabled because DBOS-style
  queued throughput has no expired running leases in the hot path.
* `--reclaim-expired --reclaim-ratio N`: enable expired-lease reclaim during
  claim polling when measuring recovery behavior.
* `--claim-priority 0`: target the default priority queue directly. Use `-1`
  to omit priority and scan all priorities.
* `--claim-job-only`: asks `FLOW.CLAIM_DUE RETURN JOBS` for only the fields
  needed to complete the job: id, partition, lease token, and fencing token.
  This is the optimized worker path. Use the default full-record claim only
  when the worker needs hydrated payload/state data from the claim response.
* `--complete-async-depth 4`: overlaps terminal writes with the next claim loop.
  In local runs, depth `4` was best; deeper queues increased contention and
  lowered throughput.
* `--payload-bytes`: raw payload bytes per flow.
* `--work-command incr`: add one `INCR` per claimed flow.
* `--worker-mode owner-wakeup`: default live worker policy. Producers notify the owner of each partition after a durable create flush. Only that worker drains the partition, avoiding broadcast/herd polling.
* `--worker-mode polling`: old blind polling mode. Useful as a stress/diagnostic path, but it can destroy batching.
* `--wake-coalesce-ms`: small delay after a partition wake before claiming, default `5ms`, to let live producers form useful claim batches.
* `--idle-sleep-ms` / `--max-idle-sleep-ms`: fallback worker wait/backoff. Defaults to `10ms` with exponential backoff up to `50ms`.

Each queued benchmark run uses a unique Flow type suffix, so dirty servers with
old benchmark data do not contaminate claim results.

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
