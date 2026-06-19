# DBOS-Style Benchmark

Benchmarks are workload-specific and should be treated as reproducibility notes,
not universal product claims. Always record:

```text
SDK commit/version
FerricStore server commit/version
server instance type
client instance type
storage device and mount options
server shard count
Flow due_any setting
benchmark command
flow count
payload size
worker/producers/connections
```

Use fresh data directories for apples-to-apples comparisons.

## Protocol transport benchmarks

SET/GET over the FerricStore protocol transport:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --preset set-throughput
```

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --preset get-throughput
```

Lower-latency GET/SET shapes:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --preset get-latency
```

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --preset set-latency
```

DBOS-style queued workflow benchmark over the protocol transport:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --flows 1000000 \
  --server-shards 16
```

Restate-style workflow latency benchmark over the protocol transport:

```bash
python examples/protocol_restate_latency_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --profile restate-high-load \
  --steps 3 \
  --workflows 100000 \
  --verify-sample 32
```

`--profile restate-high-load` applies the tuned public-SDK batch/in-flight
shape, chooses the optimized chain submit mode for the selected step count, and
compares against Restate published high-load targets. It does not pace requests
by default; use `--target-rps` only when intentionally testing a fixed-rate load
generator.

Canonical high-throughput local queue run:

```bash
python examples/run_optimized_flow_benchmarks.py \
  --start-server \
  --fresh-server-per-benchmark \
  --which queue \
  --flows 1000000
```

The wrapper starts FerricStore with:

```text
MIX_ENV=prod
FERRICSTORE_SHARD_COUNT=16
FERRICSTORE_LOG_LEVEL=warning
```

The default queue benchmark expands to:

```bash
python examples/dbos_style_benchmark.py \
  --url redis://127.0.0.1:6379/0 \
  --mode queued \
  --queued-shape live \
  --transport many \
  --worker-api lowlevel \
  --worker-mode polling \
  --partition-mode auto \
  --flows 1000000 \
  --workers 16 \
  --producers 32 \
  --partitions 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --server-shards 16 \
  --claim-job-only
```

Local lagged-projection validation on June 5, 2026 after restoring hot-only
non-idempotent create duplicate checks:

```text
flows: 1000000
created: 1000000
completed: 1000000
duplicates: 0
create_flows_per_sec: 91817/s
process_flows_per_sec: 78305/s
end_to_end_flows_per_sec: 78249/s
```

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
  FLOW.CLAIM_DUE LIMIT N BLOCK ms
  server wakes blocked workers when matching work is durable
  FLOW.COMPLETE pipelined, or FLOW.COMPLETE_MANY
```

Simple pipeline smoke run:

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
  --worker-mode blocking \
  --claim-batch-size 500 \
  --create-batch-size 500 \
  --no-reclaim-expired \
  --claim-priority 0 \
  --claim-state queued \
  --complete-async-depth 4 \
  --claim-job-only
```

Optimized FerricFlow queued run without user partition keys:

```bash
python examples/dbos_style_benchmark.py \
  --mode queued \
  --queued-shape live \
  --transport many \
  --flows 1000000 \
  --workers 16 \
  --producers 32 \
  --partitions 16 \
  --partition-mode auto \
  --worker-mode polling \
  --claim-batch-size 500 \
  --claim-partition-batch-size 2 \
  --create-batch-size 500 \
  --no-reclaim-expired \
  --claim-priority 0 \
  --complete-async-depth 4 \
  --server-shards 16 \
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
  Leave `--claim-block-ms` disabled for hot throughput; empty polling is cheaper
  than putting hot workers into server-side long polling.
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
* `--partition-mode explicit --worker-mode blocking`: opt-in partition-aware
  server-side blocked `FLOW.CLAIM_DUE` waiters for low-volume/idle queues.
* `--no-complete-batch`: use single `FLOW.COMPLETE` instead of `FLOW.COMPLETE_MANY`.
* `--no-reclaim-expired`: disables the extra expired-running reclaim pass before
  normal queued claims. This benchmark defaults to disabled because DBOS-style
  queued throughput has no expired running leases in the hot path.
* `--reclaim-expired --reclaim-ratio N`: enable expired-lease reclaim during
  claim polling when measuring recovery behavior.
* `--claim-priority 0`: target the default priority queue directly. Use `-1`
  to omit priority and scan all priorities.
* `--claim-state queued|any|omitted`: choose the state selector sent to
  `FLOW.CLAIM_DUE`. `queued` is the DBOS-style single-queue baseline. `omitted`
  sends no `STATE`, which exercises the SDK default "any state" path.
* `--claim-states queued,retry`: explicit multi-state claim selector. The SDK
  sends repeated `STATE` options, preserving the optimized explicit-state
  server path without using the broad `any` selector.
* `--claim-job-only`: asks `FLOW.CLAIM_DUE RETURN JOBS_COMPACT` for only the fields
  needed to complete the job: id, partition, lease token, and fencing token.
  This is the optimized worker path. Use the default full-record claim only
  when the worker needs hydrated payload/state data from the claim response.
* `--complete-async-depth 4`: overlaps terminal writes with the next claim loop.
  In local runs, depth `4` was best; deeper queues increased contention and
  lowered throughput.
* `--payload-bytes`: raw payload bytes per flow.
* `--work-command incr`: add one `INCR` per claimed flow.
* `--worker-mode blocking`: opt-in server wait policy. Use it for low-volume
  queues where idle polling cost matters more than peak throughput.
* `--worker-mode polling`: default hot path. It avoids long-poll tail effects
  and relies on batching, wake credits, and short idle backoff.
* `--claim-block-ms`: max server-side wait for a blocked claim before returning
  an empty result. Default is disabled.
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

The benchmark uses ack-only mutators for create, transition, and complete. That
matches the command count above and avoids measuring extra post-mutation
`FLOW.GET` calls.

```bash
python examples/dbos_style_benchmark.py --mode serial-latency --steps 10 --iterations 100
```

## Why `INCR`

`serial-latency` always uses `INCR`, matching the old step benchmark.
`queued` defaults to no-op handlers for closer queued-workflow throughput. Use
`--work-command incr` when you want one extra durable mutation per flow.

## Recovered optimized Flow benchmark wrapper

Use this wrapper for repeatable queue + workflow testing instead of hand-running
individual benchmark commands:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/run_optimized_flow_benchmarks.py \
  --start-server \
  --fresh-server-per-benchmark \
  --flows 1000000
```

The wrapper starts FerricStore from:

```text
/Users/yoavgea/repos/ferricstore
```

For each benchmark it creates a fresh prod-mode server with a temporary data dir,
then stops it after the run. This avoids stale Ra/Bitcask/index state and avoids
manual server config drift.

Server env used by the wrapper by default:

```text
MIX_ENV=prod
FERRICSTORE_PORT=6379
FERRICSTORE_DATA_DIR=<temp dir>
FERRICSTORE_LOG_LEVEL=warning
```

The wrapper does not override server resource/Flow defaults unless you pass an
explicit override. Production server defaults are:

```text
FERRICSTORE_SHARD_COUNT=0 -> schedulers online
FERRICSTORE_MAX_MEMORY=auto -> 80% detected memory/cgroup limit
FERRICSTORE_KEYDIR_MAX_RAM=auto -> derived from max memory
Flow LMDB projection is always lagged
FERRICSTORE_FLOW_HIBERNATION_ENABLED=true
FERRICSTORE_PROTECTED_MODE=true
```

Current optimized wrapper defaults:

```text
flows: 1000000
workers: 16
producers: 32
partitions: 16
server_shards: auto-detected local CPU count for client-side shard planning
claim_batch_size: 500
claim_partition_batch_size: 2
queue_create_batch_size: 500
workflow_create_batch_size: 1000
complete_async_depth: 4
workflow_apply_async_depth: 4
```

Queue benchmark command generated by the wrapper:

```bash
python examples/dbos_style_benchmark.py \
  --url redis://127.0.0.1:6379/0 \
  --mode queued \
  --queued-shape live \
  --transport many \
  --worker-api lowlevel \
  --worker-mode polling \
  --partition-mode auto \
  --flows 1000000 \
  --workers 16 \
  --producers 32 \
  --partitions 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --server-shards <auto-detected> \
  --claim-job-only
```

Latest local validation on June 2, 2026 with a fresh prod server and no
server-side resource overrides:

```text
flows: 1000000
created: 1000000
completed: 1000000
duplicates: 0
total_seconds: 13.233
create_flows_per_sec: 90077/s
process_flows_per_sec: 75574/s
end_to_end_flows_per_sec: 75567/s
```

Workflow benchmark command generated by the wrapper:

```bash
python examples/state_machine_workflow_benchmark.py \
  --url redis://127.0.0.1:6379/0 \
  --shape live \
  --flows 1000000 \
  --steps 1 \
  --workers 16 \
  --producers 32 \
  --partitions 16 \
  --partition-mode auto \
  --create-mode many \
  --create-batch-size 1000 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 2 \
  --apply-async-depth 4 \
  --worker-mode blocking \
  --server-shards <auto-detected>
```

Recovered local 1M baselines on May 18, 2026:

```text
queue live, 8 producers:
  create_flows_per_sec:      89013/s
  process_flows_per_sec:     88555/s
  end_to_end_flows_per_sec:  88469/s
  empty_claims:              0
  avg_claim_batch:           976.56

workflow live, 8 producers:
  create_flows_per_sec:             81973/s
  workflow_completions_per_sec:     80340/s
  end_to_end_workflows_per_sec:     80272/s
  empty_claims:                     35
  avg_claim_batch:                  944.29
```

Recovered 4-producer reference numbers:

```text
queue live, 4 producers:
  end_to_end_flows_per_sec: ~70800-72600/s

workflow live, 4 producers:
  end_to_end_workflows_per_sec: ~65000-68000/s
```

Azure 4-vCPU server/client results on May 19, 2026:

```text
Server VM:
  Azure size: Standard_L4as_v4
  CPU: 4 vCPU
  Data disk used: /dev/nvme1n1 mounted at /data
  Data disk model: Microsoft NVMe Direct Disk v2
  Filesystem: ext4
  Mount options: rw,noatime,nodiratime
  Unused local NVMe: /dev/nvme2n1

Client VM:
  Azure size: Standard_D4as_v4
  CPU: 4 vCPU

Server code:
  ferricstore commit: 037f2bbd Add Flow signal and hot path optimizations

Python SDK/bench code:
  ferricstore-python commit: 3133f2a Add optimized async Flow SDK and benchmarks

Server env:
  MIX_ENV=prod
  FERRICSTORE_PORT=6379
  FERRICSTORE_DATA_DIR=/data/ferricstore
  FERRICSTORE_SHARD_COUNT=16
  FERRICSTORE_PROTECTED_MODE=false
  ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128 +P 5000000 +Q 65536 +MHas aoffcbf +MBas aoffcbf"

Flow due_any index:
  disabled by default in server compile config
```

All Azure Flow runs used:

```text
url: redis://10.0.1.5:6379/0
shape: live
flows: 1000000
workers: 16
producers: 8
partitions: 1024
partition_mode: auto
server_shards: 16
claim_batch_size: 1000
claim_partition_batch_size: 16
create_batch_size: 1000
worker_mode: blocking
payload_bytes: 0
result_bytes: 0
```

Completed Azure sync queue run:

```bash
python examples/run_optimized_flow_benchmarks.py \
  --url redis://10.0.1.5:6379/0 \
  --runtime sync \
  --which queue \
  --flows 1000000 \
  --server-shards 16
```

```text
created: 1000000
completed: 1000000
claimed_items: 1000000
claim_calls: 1039
empty_claims: 15
avg_claim_batch: 962.46
max_claim_batch: 1000
create_seconds: 62.935866
process_seconds: 63.046595
total_seconds: 63.073939
create_flows_per_sec: 15889/s
process_flows_per_sec: 15861/s
end_to_end_flows_per_sec: 15854/s
```

Completed Azure sync workflow run:

```bash
python examples/run_optimized_flow_benchmarks.py \
  --url redis://10.0.1.5:6379/0 \
  --runtime sync \
  --which workflow \
  --flows 1000000 \
  --server-shards 16
```

```text
created: 1000000
completed: 1000000
claimed_actions: 1000000
claim_calls: 1196
empty_claims: 146
avg_claim_batch: 836.12
max_claim_batch: 1000
create_seconds: 62.056558
process_seconds: 62.449048
total_seconds: 62.479110
create_flows_per_sec: 16114/s
workflow_completions_per_sec: 16013/s
end_to_end_workflows_per_sec: 16005/s
```

Default Azure async profile result:

```text
queue default async:
  create_inflight: 32
  complete_inflight: 4
  result: failed
  error: write timeout; outcome unknown

workflow default async:
  create_inflight: 32
  apply_inflight: 4
  result: failed
  error: write timeout; outcome unknown
```

This is a saturation/overload result, not a throughput number. During the
monitored overload run, the server BEAM process hit roughly 398% CPU on the
4-vCPU server. Client CPU, client memory, and network utilization were not the
bottleneck. The data disk also showed write-utilization and await spikes during
the overload window.

Completed Azure throttled async queue run:

```bash
python examples/run_optimized_flow_benchmarks.py \
  --url redis://10.0.1.5:6379/0 \
  --runtime async \
  --which queue \
  --flows 1000000 \
  --server-shards 16 \
  --async-create-inflight 8 \
  --complete-async-depth 2
```

```text
created: 1000000
completed: 1000000
claimed_items: 1000000
claim_calls: 1053
empty_claims: 29
empty_claim_ratio: 0.027540
avg_claim_batch: 949.67
max_claim_batch: 1000
create_seconds: 38.558406
process_seconds: 70.396745
total_seconds: 70.396748
create_flows_per_sec: 25935/s
process_flows_per_sec: 14205/s
end_to_end_flows_per_sec: 14205/s
```

Completed Azure throttled async workflow run:

```bash
python examples/run_optimized_flow_benchmarks.py \
  --url redis://10.0.1.5:6379/0 \
  --runtime async \
  --which workflow \
  --flows 1000000 \
  --server-shards 16 \
  --async-create-inflight 8 \
  --workflow-apply-async-depth 2
```

```text
created: 1000000
completed: 1000000
claimed_actions: 1000000
claim_calls: 3376
empty_claims: 2352
empty_claim_ratio: 0.696682
avg_claim_batch: 296.21
max_claim_batch: 1000
create_seconds: 37.750147
process_seconds: 70.350906
total_seconds: 70.350908
create_flows_per_sec: 26490/s
workflow_completions_per_sec: 14214/s
end_to_end_workflows_per_sec: 14214/s
```

Azure interpretation:

```text
The completed sync runs are the valid 1M-flow apple-to-apple numbers for the
default optimized wrapper profile on this 4-vCPU server.

The default async profile can overdrive this 4-vCPU server and fail with
write-timeout backpressure. Use the throttled async numbers only when comparing
the lower-inflight async profile, not as an apple-to-apple default async result.

The Azure service config matches the current Terraform cloud-init server tuning
used for Redis/memtier-style testing: same data dir, shard count, Erlang flags,
Raft path, and NVMe /data mount. It does not match the older README example that
shows FERRICSTORE_SHARD_COUNT=8 and a shorter ERL_FLAGS value.
```

Azure shard-count sweep on the same 4-vCPU server:

```text
3 shards:
  queue e2e:    12797/s
  workflow e2e: 12787/s

4 shards:
  queue e2e:    14320/s
  workflow e2e: 14807/s

16 shards:
  queue e2e:    15854/s
  workflow e2e: 16005/s
```

The best completed 4-vCPU Azure result so far is `16` shards with the sync
wrapper. The default async wrapper was also tested at 16 shards, but it
overdrove the 4-vCPU server and failed with `write timeout; outcome unknown`.
The completed throttled async profile reached about `14205/s` queue e2e and
`14214/s` workflow e2e, with create throughput around `26k/s` but process
throughput capped around `14.2k/s`.

Azure 8-vCPU server results on May 20, 2026:

```text
Server VM:
  Azure size: Standard_L8as_v4
  CPU: 8 vCPU
  Data disk used: /dev/nvme1n1 mounted at /data
  Data disk model: Microsoft NVMe Direct Disk v2
  Filesystem: ext4
  Mount options: rw,noatime,nodiratime
  Other local NVMe devices available but unused: /dev/nvme2n1, /dev/nvme3n1, /dev/nvme4n1

Client VM:
  Azure size: Standard_D2as_v4
  CPU: 2 vCPU
  Note: the original 4-vCPU client plus 8-vCPU server shape requires 12 regional
  cores, but the Azure regional quota was 10. The client was not the bottleneck
  in the 4-vCPU-server runs, so the client was reduced to 2 vCPU to fit quota.

Server env:
  MIX_ENV=prod
  FERRICSTORE_PORT=6379
  FERRICSTORE_DATA_DIR=/data/ferricstore
  FERRICSTORE_SHARD_COUNT=16
  FERRICSTORE_PROTECTED_MODE=false
  ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128 +P 5000000 +Q 65536 +MHas aoffcbf +MBas aoffcbf"
```

Completed Azure 8-vCPU sync queue run:

```text
created: 1000000
completed: 1000000
claimed_items: 1000000
claim_calls: 1024
empty_claims: 0
avg_claim_batch: 976.56
max_claim_batch: 1000
create_seconds: 33.095759
process_seconds: 33.185270
total_seconds: 33.207910
create_flows_per_sec: 30215/s
process_flows_per_sec: 30134/s
end_to_end_flows_per_sec: 30113/s
```

Completed Azure 8-vCPU sync workflow run:

```text
created: 1000000
completed: 1000000
claimed_actions: 1000000
claim_calls: 1077
empty_claims: 53
avg_claim_batch: 928.51
max_claim_batch: 1000
create_seconds: 33.977231
process_seconds: 36.109253
total_seconds: 36.135159
create_flows_per_sec: 29431/s
workflow_completions_per_sec: 27694/s
end_to_end_workflows_per_sec: 27674/s
```

Completed Azure 8-vCPU default async queue run:

```text
create_inflight: 32
complete_inflight: 4
created: 1000000
completed: 1000000
claimed_items: 1000000
claim_calls: 1110
empty_claims: 86
empty_claim_ratio: 0.077477
avg_claim_batch: 900.90
max_claim_batch: 1000
create_seconds: 18.365799
process_seconds: 41.872213
total_seconds: 41.872215
create_flows_per_sec: 54449/s
process_flows_per_sec: 23882/s
end_to_end_flows_per_sec: 23882/s
```

Completed Azure 8-vCPU default async workflow run:

```text
create_inflight: 32
apply_inflight: 4
created: 1000000
completed: 1000000
claimed_actions: 1000000
claim_calls: 2599
empty_claims: 1575
empty_claim_ratio: 0.606002
avg_claim_batch: 384.76
max_claim_batch: 1000
create_seconds: 17.548224
process_seconds: 40.466861
total_seconds: 40.466864
create_flows_per_sec: 56986/s
workflow_completions_per_sec: 24712/s
end_to_end_workflows_per_sec: 24712/s
```

Azure 4-vCPU vs 8-vCPU summary:

```text
4-vCPU server, 16 shards:
  sync queue e2e:       15854/s
  sync workflow e2e:    16005/s
  default async queue:  failed with write timeout
  default async workflow: failed with write timeout

8-vCPU server, 16 shards:
  sync queue e2e:       30113/s
  sync workflow e2e:    27674/s
  async queue e2e:      23882/s
  async workflow e2e:   24712/s
```

On this Azure shape, the sync wrapper gives the best completed end-to-end
numbers. Default async now completes on the 8-vCPU server, but it overproduces
creates and makes the process side the limiting phase.

Azure 16-vCPU server results on May 20, 2026:

```text
Server VM:
  Azure size: Standard_L16as_v4
  CPU: 16 vCPU
  Data disk used: /dev/nvme3n1 mounted at /data
  Data disk model: Microsoft NVMe Direct Disk v2
  Filesystem: ext4
  Mount options: rw,noatime,nodiratime
  Other local NVMe devices available but unused: /dev/nvme1n1, /dev/nvme2n1, /dev/nvme4n1

Client VM:
  Azure size: Standard_D2as_v4
  CPU: 2 vCPU

Server env:
  MIX_ENV=prod
  FERRICSTORE_PORT=6379
  FERRICSTORE_DATA_DIR=/data/ferricstore
  FERRICSTORE_SHARD_COUNT=16
  FERRICSTORE_PROTECTED_MODE=false
  ERL_FLAGS="+sbt db +sbwt very_short +swt very_low +K true +A 128 +P 5000000 +Q 65536 +MHas aoffcbf +MBas aoffcbf"
```

Completed Azure 16-vCPU sync queue run:

```text
created: 1000000
completed: 1000000
claimed_items: 1000000
claim_calls: 1024
empty_claims: 0
avg_claim_batch: 976.56
max_claim_batch: 1000
create_seconds: 21.183025
process_seconds: 21.267518
total_seconds: 21.292836
create_flows_per_sec: 47208/s
process_flows_per_sec: 47020/s
end_to_end_flows_per_sec: 46964/s
```

Completed Azure 16-vCPU sync workflow run:

```text
created: 1000000
completed: 1000000
claimed_actions: 1000000
claim_calls: 1167
empty_claims: 135
avg_claim_batch: 856.90
max_claim_batch: 1000
create_seconds: 21.811884
process_seconds: 22.010885
total_seconds: 22.038564
create_flows_per_sec: 45847/s
workflow_completions_per_sec: 45432/s
end_to_end_workflows_per_sec: 45375/s
```

Completed Azure 16-vCPU default async queue run:

```text
create_inflight: 32
complete_inflight: 4
created: 1000000
completed: 1000000
claimed_items: 1000000
claim_calls: 1054
empty_claims: 30
empty_claim_ratio: 0.028463
avg_claim_batch: 948.77
max_claim_batch: 1000
create_seconds: 11.508486
process_seconds: 24.312494
total_seconds: 24.312496
create_flows_per_sec: 86892/s
process_flows_per_sec: 41131/s
end_to_end_flows_per_sec: 41131/s
```

Completed Azure 16-vCPU default async workflow run:

```text
create_inflight: 32
apply_inflight: 4
created: 1000000
completed: 1000000
claimed_actions: 1000000
claim_calls: 3055
empty_claims: 2031
empty_claim_ratio: 0.664812
avg_claim_batch: 327.33
max_claim_batch: 1000
create_seconds: 11.049294
process_seconds: 24.318434
total_seconds: 24.318437
create_flows_per_sec: 90504/s
workflow_completions_per_sec: 41121/s
end_to_end_workflows_per_sec: 41121/s
```

Azure 4-vCPU vs 8-vCPU vs 16-vCPU summary:

```text
4-vCPU server, 16 shards:
  sync queue e2e:       15854/s
  sync workflow e2e:    16005/s
  default async queue:  failed with write timeout
  default async workflow: failed with write timeout

8-vCPU server, 16 shards:
  sync queue e2e:       30113/s
  sync workflow e2e:    27674/s
  async queue e2e:      23882/s
  async workflow e2e:   24712/s

16-vCPU server, 16 shards:
  sync queue e2e:       46964/s
  sync workflow e2e:    45375/s
  async queue e2e:      41131/s
  async workflow e2e:   41121/s
```

The 16-vCPU server continues to scale the sync wrapper, but not linearly from
8 vCPU. With the current 16-shard setting, the next useful sweep is likely
`24` or `32` server shards on the 16-vCPU server, plus an async process-side
tuning pass to reduce workflow empty claims.

Azure 16-vCPU server shard-count sweep:

```text
16 shards:
  sync queue e2e:       46964/s
  sync workflow e2e:    45375/s

24 shards:
  sync queue e2e:       51644/s
  sync workflow e2e:    51977/s

32 shards:
  sync queue e2e:       53790/s
  sync workflow e2e:    54060/s

48 shards:
  sync queue e2e:       54287/s
  sync workflow e2e:    53736/s
```

On this VM, `32` shards is the best balanced setting observed so far. `48`
shards is slightly better for queue-only throughput, but worse for workflow.
The next useful checks would be repeat runs at `32` and `48` to measure
variance, then an async worker/claim tuning pass if we want async e2e to match
or beat sync.

Azure 16-vCPU server async shard-count sweep:

```text
16 shards:
  async queue e2e:      41131/s
  async workflow e2e:   41121/s
  async queue create:   86892/s
  async workflow create: 90504/s

32 shards:
  async queue e2e:      45608/s
  async workflow e2e:   47888/s
  async queue create:   95896/s
  async workflow create: 97196/s

48 shards:
  async queue e2e:      43997/s
  async workflow e2e:   45137/s
  async queue create:   96219/s
  async workflow create: 95195/s
```

Async also peaks at `32` shards in this sweep. It can create around
`96k-97k/s`, but e2e remains process-side limited. Sync at `32` shards remains
the best observed e2e result:

```text
32 shards sync queue e2e:      53790/s
32 shards async queue e2e:     45608/s

32 shards sync workflow e2e:   54060/s
32 shards async workflow e2e:  47888/s
```

Preloaded reference numbers isolate create and process phases. They are not the
DBOS-style live e2e number because create and process run serially:

```text
queue preloaded, 1M:
  create_flows_per_sec:      99645/s
  process_flows_per_sec:    125161/s
  end_to_end_flows_per_sec:  55478/s

workflow preloaded, 1M:
  create_flows_per_sec:              99892/s
  workflow_completions_per_sec:     101080/s
  end_to_end_workflows_per_sec:      50241/s
```

For quick smoke testing:

```bash
python examples/run_optimized_flow_benchmarks.py \
  --start-server \
  --fresh-server-per-benchmark \
  --flows 100000
```

For one benchmark only:

```bash
python examples/run_optimized_flow_benchmarks.py \
  --start-server \
  --fresh-server-per-benchmark \
  --which queue \
  --flows 1000000

python examples/run_optimized_flow_benchmarks.py \
  --start-server \
  --fresh-server-per-benchmark \
  --which workflow \
  --flows 1000000
```

Important testing rule: compare only runs using the same wrapper settings and a
fresh server. Dirty long-lived local servers, different producer counts, disabled
compact claims, or different wake coalesce values can move the result from ~80k/s
down to ~45k/s without any server regression.
