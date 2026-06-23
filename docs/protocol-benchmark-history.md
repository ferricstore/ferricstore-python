# FerricStore Protocol Benchmark History

Append-only benchmark log for native protocol, RESP comparisons, DBOS-style workflow runs, and protocol command microbenchmarks.

## Logging rule

For every benchmark run, append:

- Date/time
- FerricStore commit/branch if known
- Python SDK commit/branch if known
- Server mode: source/container, clean/dirty data dir, shard count, ports, key env flags
- Benchmark command or script settings
- Result: throughput, latency, errors/rejects, CPU/RSS if available
- Notes: dirty server, concurrent benchmark, profiler attached, non-apples-to-apples caveats

## 2026-06-11 native protocol optimization session

### Server setup used for most local source runs

```bash
cd /Users/yoavgea/repos/ferricstore
MIX_ENV=prod \
FERRICSTORE_NATIVE_ENABLED=true \
FERRICSTORE_SHARD_COUNT=16 \
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 \
FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data \
FERRICSTORE_PORT=16379 \
FERRICSTORE_HEALTH_PORT=16380 \
FERRICSTORE_NATIVE_PORT=16388 \
mix run --no-halt
```

Notes:

- Source server, not Docker.
- Native protocol enabled.
- 16 shards.
- Clean-server results are the only apples-to-apples comparison points.
- Dirty-server results after mixed KV/Flow runs are kept for debugging but not treated as final baseline.

### Focused correctness tests

```bash
cd /Users/yoavgea/repos/ferricstore-python
pytest tests/test_protocol.py tests/test_protocol_kv_benchmark.py tests/test_protocol_dbos_benchmark.py tests/test_protocol_flow_commands_benchmark.py -q
```

Result:

```text
100 passed in 0.20s
```

### Native KV observations

Single-process native `GET many` shape:

```text
~2.08M GET/s
CPU roughly one Python core
```

Four-process native `GET many` shape:

```text
~7.82M GET/s
CPU roughly 384%
```

Conclusion:

```text
GET path is mostly Python client/GIL/decode limited for single-process benchmark, not server limited.
```

Native `SET many`:

```text
single process: ~1.65M-1.82M SET/s
4 processes: ~2.76M SET/s
```

Native `PIPELINE GET`, one Python process, one connection, pipeline=1000, lanes=64, 30s:

```text
before compact builder work: ~1.42M/s
after first builder optimization: ~1.51M/s
after hot-loop command/key optimization: ~1.72M/s clean-ish
binary-key benchmark sample: ~1.81M/s
latest dirty-server sample after Flow/KV mixed runs: ~1.589M/s
```

Native `PIPELINE SET`, latest dirty-server 30s sample:

```text
~1.586M/s
```

Caveat:

```text
Latest 30s GET/SET values were on a dirty server after Flow work. Restart clean server before final baseline comparison.
```

### RESP/memtier historical comparison target

Historical local source RESP/memtier baseline shape:

```text
memtier: --clients=200 --threads=4 --pipeline=50
This is 200 clients per thread = 800 total connections.
```

Historical local source result:

```text
SET: 756,799/s, p50 52.479 ms, p99 70.143 ms
GET: 5,102,710/s, p50 7.743 ms, p99 11.455 ms
```

Interpretation:

```text
RESP GET with 800 total connections remains higher than one-process native protocol GET.
Native protocol needs either multi-process benchmark, more client CPU, or lower-copy client decode path for fair max-throughput comparison.
```

### DBOS-style queued workflow benchmark

Clean native DBOS 100k before latest start-path work:

```text
~76.5k workflows/s e2e
```

Clean native DBOS 1M before latest start-path work:

```text
~64.8k workflows/s sustained
```

Dirty DBOS 100k after many KV/Flow mixed runs:

```text
~42k workflows/s
```

Conclusion:

```text
Dirty/background-contended server can hide real protocol changes. Clean data dir + fresh source server required for DBOS comparisons.
```

Native worker connection observation:

```text
1 native worker connection was best for the DBOS benchmark shape.
2 native worker connections degraded badly in one sample (~23.9k/s), likely due to scheduling/backpressure interaction.
Default benchmark path should stay 1 native worker connection until proven otherwise.
```

### Flow command protocol microbenchmarks

Approximate samples from this session:

```text
FLOW.CREATE_MANY: ~223k items/s
FLOW.COMPLETE_MANY: ~161k-227k items/s depending setup/server dirtiness
FLOW.START_AND_CLAIM before compact submit_batch: ~14k-16k/s
FLOW.START_AND_CLAIM after compact submit_batch: ~27k-28k/s
FLOW.STEP_CONTINUE before compact submit_batch: ~46.7k/s with batch=500
FLOW.STEP_CONTINUE after compact submit_batch/pair unwrap: ~63.3k/s measured phase
```

Notes:

```text
START_AND_CLAIM remains lower because it performs create + lease + full record response decode.
Likely next useful feature: optional compact job return for START_AND_CLAIM, preserving correctness while avoiding full Flow record decode when caller only needs minimal job fields.
```

### Profiling notes

Native KV `PIPELINE GET` profile:

```text
Initial hotspots: _compact_pipeline_keys_payload_from_raw, _command_name, _maybe_bytes, benchmark command tuple construction.
After optimization: remaining dominant costs are Python command tuple creation, key encoding, and response value copies.
```

Native Flow `START_AND_CLAIM` profile:

```text
Before compact submit_batch: generic map PIPELINE, encode_value, build_protocol_command.
After compact submit_batch/raw parser: much less build_protocol_command cost; remaining cost is response decode/full record work and server wait.
```

### Policy from this point

```text
Append every benchmark run to this file immediately after the run.
Mark clean vs dirty server explicitly.
Do not compare dirty-server samples against clean baselines.
For final numbers, restart source server with clean data dir and rerun the exact command.
```


## 2026-06-11 13:34:45 IDT clean native KV SET 30s

```bash
python examples/protocol_kv_benchmark.py --command set --url ferric://127.0.0.1:16388 --test-time 30 --threads 1 --processes 1 --clients 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --key-prefix proto-clean --key-count 1000000 --value-bytes 32 --binary-keys --pretty --no-warmup
```

```json
{
  "batch_latency_avg_ms": 36.26822019437316,
  "batch_latency_max_ms": 96.604375,
  "batch_latency_p50_ms": 35.587792,
  "batch_latency_p95_ms": 38.241417,
  "batch_latency_p99_ms": 56.797208,
  "batch_latency_samples": 52605,
  "benchmark": "protocol_kv",
  "binary_keys": true,
  "client_cpu_percent": 99.22152287764118,
  "client_cpu_seconds": 29.778346,
  "clients_per_thread": 1,
  "command": "set",
  "configured_requests": null,
  "errors": 0,
  "inflight_batches": 64,
  "key_count": 1000000,
  "pipeline": 1000,
  "preset": null,
  "processes": 1,
  "protocol_lanes": 64,
  "read_percent": null,
  "request_mode": "pipeline",
  "requests": 52605000,
  "requests_per_sec": 1752799.9073482167,
  "seconds": 30.011982417083345,
  "test_time": 30.0,
  "threads": 1,
  "total_connections": 1,
  "url": "ferric://127.0.0.1:16388",
  "value_bytes": 32,
  "warmed_keys": 0
}
```

## 2026-06-11 13:35:15 IDT clean native KV GET 30s

```bash
python examples/protocol_kv_benchmark.py --command get --url ferric://127.0.0.1:16388 --test-time 30 --threads 1 --processes 1 --clients 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --key-prefix proto-clean --key-count 1000000 --value-bytes 32 --binary-keys --pretty --no-warmup
```

```json
{
  "batch_latency_avg_ms": 41.101556188446054,
  "batch_latency_max_ms": 44.82225,
  "batch_latency_p50_ms": 41.123875,
  "batch_latency_p95_ms": 41.873833,
  "batch_latency_p99_ms": 42.308625,
  "batch_latency_samples": 46443,
  "benchmark": "protocol_kv",
  "binary_keys": true,
  "client_cpu_percent": 102.40746078517289,
  "client_cpu_seconds": 30.724208,
  "clients_per_thread": 1,
  "command": "get",
  "configured_requests": null,
  "errors": 0,
  "inflight_batches": 64,
  "key_count": 1000000,
  "pipeline": 1000,
  "preset": null,
  "processes": 1,
  "protocol_lanes": 64,
  "read_percent": null,
  "request_mode": "pipeline",
  "requests": 46443000,
  "requests_per_sec": 1548000.7495216099,
  "seconds": 30.001923457952216,
  "test_time": 30.0,
  "threads": 1,
  "total_connections": 1,
  "url": "ferric://127.0.0.1:16388",
  "value_bytes": 32,
  "warmed_keys": 0
}
```

## 2026-06-11 13:36:22 IDT native KV GET profile 8s

```bash
python -m cProfile -s cumulative examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --command get --test-time 8 --threads 1 --processes 1 --clients 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --key-prefix proto-clean --key-count 1000000 --value-bytes 32 --binary-keys --no-warmup
```

```text
{"batch_latency_avg_ms": 139.21606478209367, "batch_latency_max_ms": 152.901625, "batch_latency_p50_ms": 139.0125, "batch_latency_p95_ms": 145.556125, "batch_latency_p99_ms": 150.821667, "batch_latency_samples": 3630, "benchmark": "protocol_kv", "binary_keys": true, "client_cpu_percent": 100.74138693157934, "client_cpu_seconds": 8.062935000000001, "clients_per_thread": 1, "command": "get", "configured_requests": null, "errors": 0, "inflight_batches": 64, "key_count": 1000000, "pipeline": 1000, "preset": null, "processes": 1, "protocol_lanes": 64, "read_percent": null, "request_mode": "pipeline", "requests": 3630000, "requests_per_sec": 453546.05309559475, "seconds": 8.003597375005484, "test_time": 8.0, "threads": 1, "total_connections": 1, "url": "ferric://127.0.0.1:16388", "value_bytes": 32, "warmed_keys": 0}
         51408191 function calls (51398572 primitive calls) in 8.062 seconds

   Ordered by: cumulative time

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
     3632    0.020    0.000    9.066    0.002 protocol.py:1447(_recv_response)
     7263    0.007    0.000    8.059    0.001 protocol.py:1521(_recv_exact)
    119/1    0.007    0.000    8.004    8.004 {built-in method builtins.exec}
      2/1    0.000    0.000    8.004    8.004 protocol_kv_benchmark.py:1(<module>)
      2/1    0.000    0.000    8.004    8.004 protocol_kv_benchmark.py:551(main)
      2/1    0.000    0.000    8.004    8.004 protocol_kv_benchmark.py:371(run_benchmark)
      2/1    0.000    0.000    8.003    8.003 protocol_kv_benchmark.py:315(_run_process)
        1    0.000    0.000    8.003    8.003 _base.py:646(__exit__)
        1    0.000    0.000    8.003    8.003 thread.py:220(shutdown)
        1    0.000    0.000    8.003    8.003 threading.py:1059(join)
        1    0.000    0.000    8.003    8.003 {method 'join' of '_thread._ThreadHandle' objects}
        2    0.000    0.000    8.003    4.002 _base.py:199(as_completed)
      8/3    0.000    0.000    8.003    2.668 threading.py:327(wait)
      4/2    0.000    0.000    8.003    4.002 threading.py:641(wait)
    32/13    0.000    0.000    8.001    0.615 {method 'acquire' of '_thread.lock' objects}
        1    0.000    0.000    8.001    8.001 protocol.py:1400(_reader_loop)
        1    0.000    0.000    7.999    7.999 thread.py:54(run)
        1    0.562    0.562    7.999    7.999 protocol_kv_benchmark.py:190(_run_thread)
     3630    0.025    0.000    5.984    0.002 protocol.py:938(submit_batch)
     3630    0.004    0.000    2.848    0.001 protocol.py:389(_compact_pipeline_payload_from_raw)
     3630    1.546    0.000    2.842    0.001 protocol.py:708(_compact_pipeline_keys_payload_from_raw)
     3630    0.337    0.000    1.898    0.001 protocol.py:2617(_compact_flow_many_payloads_from_raw)
  3633630    0.673    0.000    1.557    0.000 protocol.py:4219(_command_name)
  3630000    0.538    0.000    1.438    0.000 protocol_kv_benchmark.py:70(build_command)
     3630    0.002    0.000    1.189    0.000 protocol.py:1076(_submit_pipeline_payload)
     3631    0.024    0.000    1.187    0.000 protocol.py:1375(_submit_request)
     3631    0.003    0.000    0.987    0.000 protocol.py:3237(_try_fast_response_value_at)
     3630    0.779    0.000    0.984    0.000 protocol.py:3349(_try_decode_custom_kv_mget)
  3630000    0.657    0.000    0.900    0.000 protocol_kv_benchmark.py:96(benchmark_key)
  3633630    0.452    0.000    0.647    0.000 protocol.py:4227(_text)
  7260000    0.478    0.000    0.478    0.000 {method 'extend' of 'bytearray' objects}
  7283999    0.396    0.000    0.396    0.000 {built-in method builtins.isinstance}
7331290/7331175    0.344    0.000    0.344    0.000 {built-in method builtins.len}
  3637261    0.281    0.000    0.281    0.000 {method 'pack' of '_struct.Struct' objects}
  3630063    0.244    0.000    0.244    0.000 {method 'encode' of 'str' objects}
  3633630    0.238    0.000    0.238    0.000 {method 'upper' of 'str' objects}
  3637261    0.206    0.000    0.206    0.000 {method 'unpack_from' of '_struct.Struct' objects}
        8    0.000    0.000    0.060    0.008 __init__.py:1(<module>)
    119/9    0.000    0.000    0.058    0.006 <frozen importlib._bootstrap>:1349(_find_and_load)
    119/9    0.000    0.000    0.058    0.006 <frozen importlib._bootstrap>:1304(_find_and_load_unlocked)
   115/11    0.000    0.000    0.057    0.005 <frozen importlib._bootstrap>:911(_load_unlocked)
    91/10    0.000    0.000    0.056    0.006 <frozen importlib._bootstrap_external>:1017(exec_module)
   282/20    0.000    0.000    0.056    0.003 <frozen importlib._bootstrap>:480(_call_with_frames_removed)
     3631    0.007    0.000    0.050    0.000 protocol.py:1326(_send)
     7263    0.049    0.000    0.049    0.000 {method 'recv' of '_socket.socket' objects}
     28/7    0.000    0.000    0.042    0.006 {built-in method builtins.__import__}
7262/3647    0.009    0.000    0.036    0.000 _base.py:537(set_result)
     3631    0.004    0.000    0.030    0.000 protocol.py:3159(_send_frame)
     3631    0.026    0.000    0.026    0.000 {method 'sendall' of '_socket.socket' objects}
7262/3647    0.002    0.000    0.024    0.000 _base.py:337(_invoke_callbacks)
        1    0.000    0.000    0.023    0.023 async_client.py:1(<module>)
     3630    0.005    0.000    0.022    0.000 protocol.py:1059(complete)
   105/50    0.000    0.000    0.022    0.000 <frozen importlib._bootstrap>:1390(_handle_fromlist)
     7262    0.004    0.000    0.021    0.000 _base.py:328(__init__)
23177/22838    0.003    0.000    0.018    0.000 {built-in method builtins.hasattr}
     7269    0.010    0.000    0.017    0.000 threading.py:281(__init__)
        2    0.000    0.000    0.015    0.008 process.py:1(<module>)
        2    0.000    0.000    0.015    0.007 __init__.py:41(__getattr__)
        1    0.000    0.000    0.014    0.014 backpressure.py:1(<module>)
       26    0.000    0.000    0.013    0.001 dataclasses.py:1294(wrap)
       26    0.001    0.000    0.013    0.001 dataclasses.py:929(_process_class)
       91    0.000    0.000    0.013    0.000 <frozen importlib._bootstrap_external>:1090(get_code)
7262/7260    0.006    0.000    0.011    0.000 _base.py:428(result)
     3630    0.002    0.000    0.011    0.000 protocol.py:1050(_complete_batch_future)
        1    0.000    0.000    0.011    0.011 base_events.py:1(<module>)
      115    0.000    0.000    0.010    0.000 <frozen importlib._bootstrap>:806(module_from_spec)
     3630    0.005    0.000    0.009    0.000 _base.py:408(add_done_callback)
       22    0.000    0.000    0.009    0.000 <frozen importlib._bootstrap_external>:1315(create_module)
       22    0.009    0.000    0.009    0.000 {built-in method _imp.create_dynamic}
       91    0.002    0.000    0.009    0.000 <frozen importlib._bootstrap_external>:779(_compile_bytecode)
     3630    0.002    0.000    0.008    0.000 protocol_kv_benchmark.py:141(_wait_futures)
        1    0.000    0.000    0.008    0.008 client.py:1(<module>)
    18168    0.004    0.000    0.007    0.000 threading.py:303(__enter__)
       26    0.000    0.000    0.007    0.000 dataclasses.py:470(add_fns_to_class)
```

## 2026-06-11 13:37:24 IDT clean native KV GET 30s after submit_batch ordering fix

```bash
python examples/protocol_kv_benchmark.py --command get --url ferric://127.0.0.1:16388 --test-time 30 --threads 1 --processes 1 --clients 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --key-prefix proto-clean --key-count 1000000 --value-bytes 32 --binary-keys --pretty --no-warmup
```

```json
{
  "batch_latency_avg_ms": 34.527388082603345,
  "batch_latency_max_ms": 41.862333,
  "batch_latency_p50_ms": 34.547584,
  "batch_latency_p95_ms": 35.340875,
  "batch_latency_p99_ms": 35.86675,
  "batch_latency_samples": 55252,
  "benchmark": "protocol_kv",
  "binary_keys": true,
  "client_cpu_percent": 101.96392166811137,
  "client_cpu_seconds": 30.590522,
  "clients_per_thread": 1,
  "command": "get",
  "configured_requests": null,
  "errors": 0,
  "inflight_batches": 64,
  "key_count": 1000000,
  "pipeline": 1000,
  "preset": null,
  "processes": 1,
  "protocol_lanes": 64,
  "read_percent": null,
  "request_mode": "pipeline",
  "requests": 55252000,
  "requests_per_sec": 1841652.3261703379,
  "seconds": 30.001319583971053,
  "test_time": 30.0,
  "threads": 1,
  "total_connections": 1,
  "url": "ferric://127.0.0.1:16388",
  "value_bytes": 32,
  "warmed_keys": 0
}
```

## 2026-06-11 13:37:55 IDT clean native KV SET 30s after submit_batch ordering fix

```bash
python examples/protocol_kv_benchmark.py --command set --url ferric://127.0.0.1:16388 --test-time 30 --threads 1 --processes 1 --clients 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --key-prefix proto-clean --key-count 1000000 --value-bytes 32 --binary-keys --pretty --no-warmup
```

```json
{
  "batch_latency_avg_ms": 30.412224146385043,
  "batch_latency_max_ms": 49.775959,
  "batch_latency_p50_ms": 30.203833,
  "batch_latency_p95_ms": 31.892542,
  "batch_latency_p99_ms": 37.264625,
  "batch_latency_samples": 62684,
  "benchmark": "protocol_kv",
  "binary_keys": true,
  "client_cpu_percent": 100.64688906126567,
  "client_cpu_seconds": 30.204457,
  "clients_per_thread": 1,
  "command": "set",
  "configured_requests": null,
  "errors": 0,
  "inflight_batches": 64,
  "key_count": 1000000,
  "pipeline": 1000,
  "preset": null,
  "processes": 1,
  "protocol_lanes": 64,
  "read_percent": null,
  "request_mode": "pipeline",
  "requests": 62684000,
  "requests_per_sec": 2088747.8937020379,
  "seconds": 30.01032350002788,
  "test_time": 30.0,
  "threads": 1,
  "total_connections": 1,
  "url": "ferric://127.0.0.1:16388",
  "value_bytes": 32,
  "warmed_keys": 0
}
```

## 2026-06-11 13:38:54 IDT native KV GET profile 8s after submit_batch ordering fix

```text
{"batch_latency_avg_ms": 102.73773353451077, "batch_latency_max_ms": 109.531, "batch_latency_p50_ms": 103.290542, "batch_latency_p95_ms": 105.79025, "batch_latency_p99_ms": 107.713208, "batch_latency_samples": 4926, "benchmark": "protocol_kv", "binary_keys": true, "client_cpu_percent": 100.92384552213875, "client_cpu_seconds": 8.077214999999999, "clients_per_thread": 1, "command": "get", "configured_requests": null, "errors": 0, "inflight_batches": 64, "key_count": 1000000, "pipeline": 1000, "preset": null, "processes": 1, "protocol_lanes": 64, "read_percent": null, "request_mode": "pipeline", "requests": 4926000, "requests_per_sec": 615497.8702957091, "seconds": 8.00327708304394, "test_time": 8.0, "threads": 1, "total_connections": 1, "url": "ferric://127.0.0.1:16388", "value_bytes": 32, "warmed_keys": 0}
         49964295 function calls (49952072 primitive calls) in 8.060 seconds

   Ordered by: cumulative time

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
    119/1    0.007    0.000    8.004    8.004 {built-in method builtins.exec}
      2/1    0.000    0.000    8.004    8.004 protocol_kv_benchmark.py:1(<module>)
      2/1    0.000    0.000    8.004    8.004 protocol_kv_benchmark.py:551(main)
      2/1    0.000    0.000    8.004    8.004 protocol_kv_benchmark.py:371(run_benchmark)
      2/1    0.000    0.000    8.003    8.003 protocol_kv_benchmark.py:315(_run_process)
        1    0.000    0.000    8.003    8.003 _base.py:646(__exit__)
        1    0.000    0.000    8.003    8.003 thread.py:220(shutdown)
        1    0.000    0.000    8.003    8.003 threading.py:1059(join)
        1    0.000    0.000    8.003    8.003 {method 'join' of '_thread._ThreadHandle' objects}
        2    0.000    0.000    8.003    4.001 _base.py:199(as_completed)
      4/2    0.000    0.000    8.003    4.001 threading.py:641(wait)
      7/3    0.000    0.000    8.003    2.668 threading.py:327(wait)
        1    0.000    0.000    8.001    8.001 thread.py:54(run)
        1    0.000    0.000    8.001    8.001 protocol_kv_benchmark.py:190(_run_thread)
        1    0.000    0.000    8.001    8.001 protocol.py:815(close)
        1    0.000    0.000    8.001    8.001 socket.py:501(close)
        1    0.000    0.000    8.001    8.001 socket.py:497(_real_close)
        1    0.000    0.000    8.001    8.001 {function socket.close at 0x1076998a0}
     4926    0.010    0.000    5.348    0.001 protocol.py:938(submit_batch)
     4926    0.005    0.000    3.749    0.001 protocol.py:389(_compact_pipeline_payload_from_raw)
     4926    2.025    0.000    3.738    0.001 protocol.py:708(_compact_pipeline_keys_payload_from_raw)
  4926000    0.697    0.000    1.888    0.000 protocol_kv_benchmark.py:70(build_command)
     4926    0.002    0.000    1.561    0.000 protocol.py:1076(_submit_pipeline_payload)
     4927    0.027    0.000    1.559    0.000 protocol.py:1375(_submit_request)
     4928    0.022    0.000    1.414    0.000 protocol.py:1447(_recv_response)
     4927    0.004    0.000    1.325    0.000 protocol.py:3237(_try_fast_response_value_at)
     4926    1.042    0.000    1.321    0.000 protocol.py:3349(_try_decode_custom_kv_mget)
  4926000    0.871    0.000    1.191    0.000 protocol_kv_benchmark.py:96(benchmark_key)
  9852000    0.629    0.000    0.629    0.000 {method 'extend' of 'bytearray' objects}
9936766/9936651    0.451    0.000    0.451    0.000 {built-in method builtins.len}
  4935853    0.376    0.000    0.376    0.000 {method 'pack' of '_struct.Struct' objects}
  4926063    0.320    0.000    0.320    0.000 {method 'encode' of 'str' objects}
  4935853    0.281    0.000    0.281    0.000 {method 'unpack_from' of '_struct.Struct' objects}
  4956479    0.267    0.000    0.267    0.000 {built-in method builtins.isinstance}
     9855    0.009    0.000    0.068    0.000 protocol.py:1521(_recv_exact)
     4927    0.008    0.000    0.058    0.000 protocol.py:1326(_send)
        8    0.000    0.000    0.057    0.007 __init__.py:1(<module>)
     9855    0.057    0.000    0.057    0.000 {method 'recv' of '_socket.socket' objects}
    119/9    0.000    0.000    0.056    0.006 <frozen importlib._bootstrap>:1349(_find_and_load)
    119/9    0.000    0.000    0.056    0.006 <frozen importlib._bootstrap>:1304(_find_and_load_unlocked)
   115/11    0.000    0.000    0.055    0.005 <frozen importlib._bootstrap>:911(_load_unlocked)
    91/10    0.000    0.000    0.054    0.005 <frozen importlib._bootstrap_external>:1017(exec_module)
   282/20    0.000    0.000    0.054    0.003 <frozen importlib._bootstrap>:480(_call_with_frames_removed)
9854/4935    0.011    0.000    0.045    0.000 _base.py:537(set_result)
     28/7    0.000    0.000    0.041    0.006 {built-in method builtins.__import__}
     4927    0.004    0.000    0.032    0.000 protocol.py:3159(_send_frame)
9854/4935    0.003    0.000    0.030    0.000 _base.py:337(_invoke_callbacks)
     4926    0.006    0.000    0.028    0.000 protocol.py:1059(complete)
     4927    0.028    0.000    0.028    0.000 {method 'sendall' of '_socket.socket' objects}
     9854    0.005    0.000    0.025    0.000 _base.py:328(__init__)
   105/50    0.000    0.000    0.021    0.000 <frozen importlib._bootstrap>:1390(_handle_fromlist)
        1    0.000    0.000    0.021    0.021 async_client.py:1(<module>)
     9861    0.012    0.000    0.020    0.000 threading.py:281(__init__)
30953/30614    0.004    0.000    0.019    0.000 {built-in method builtins.hasattr}
        2    0.000    0.000    0.015    0.007 process.py:1(<module>)
        2    0.000    0.000    0.015    0.007 __init__.py:41(__getattr__)
9854/9853    0.008    0.000    0.014    0.000 _base.py:428(result)
        1    0.000    0.000    0.013    0.013 backpressure.py:1(<module>)
     4926    0.003    0.000    0.013    0.000 protocol.py:1050(_complete_batch_future)
       26    0.000    0.000    0.013    0.000 dataclasses.py:1294(wrap)
       26    0.001    0.000    0.013    0.000 dataclasses.py:929(_process_class)
       91    0.000    0.000    0.012    0.000 <frozen importlib._bootstrap_external>:1090(get_code)
     4926    0.006    0.000    0.010    0.000 _base.py:408(add_done_callback)
     4926    0.002    0.000    0.010    0.000 protocol_kv_benchmark.py:141(_wait_futures)
        1    0.000    0.000    0.010    0.010 base_events.py:1(<module>)
      115    0.000    0.000    0.010    0.000 <frozen importlib._bootstrap>:806(module_from_spec)
    24648    0.005    0.000    0.009    0.000 threading.py:303(__enter__)
       22    0.000    0.000    0.009    0.000 <frozen importlib._bootstrap_external>:1315(create_module)
       22    0.009    0.000    0.009    0.000 {built-in method _imp.create_dynamic}
       91    0.002    0.000    0.008    0.000 <frozen importlib._bootstrap_external>:779(_compile_bytecode)
     9859    0.004    0.000    0.008    0.000 threading.py:428(notify_all)
        1    0.000    0.000    0.007    0.007 client.py:1(<module>)
        1    0.000    0.000    0.007    0.007 connection.py:1(<module>)
       26    0.000    0.000    0.007    0.000 dataclasses.py:470(add_fns_to_class)
```

## 2026-06-11 13:40:01 IDT clean native DBOS queue 100k after submit_batch ordering fix

```bash
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 100000 --workers 16 --producers 4 --partitions 16 --server-shards 16 --claim-batch-size 500 --claim-partition-batch-size 16 --claim-drain-batches 2 --create-batch-size 500 --complete-async-depth 4 --protocol-worker-connections 1 --protocol-lanes 32 --transport many --queued-shape live --worker-api queue --worker-mode polling --partition-mode auto --claim-job-only
```

```text
{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-8b2d7a5eec2c4c39b65eb66f6f9b713b', 'flows': 100000, 'created': 100000, 'completed': 100000, 'claimed_items': 100000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 0.0, 'producer_backpressure_waits': 0, 'producer_backpressure_wait_ms': 0.0, 'producer_backpressure_limited_batches': 0, 'producer_queue_latency_ewma_ms': 60.55175457974327, 'queue_latency_tracked': 1000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 1000, 'queue_latency_avg_ms': 18.329318816, 'queue_latency_p50_ms': 15.762417, 'queue_latency_p95_ms': 32.402208, 'queue_latency_p99_ms': 77.897458, 'queue_latency_max_ms': 136.338792, 'wake_notifications': 256, 'wake_credits': 100000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 270, 'process_empty_claims': 1, 'process_fallback_claims': 14, 'process_avg_claim_batch': 370.3703703703704, 'process_max_claim_batch': 788, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 1.2530313750030473, 'process_seconds': 1.3688835001084954, 'total_seconds': 1.3714501250069588, 'client_cpu_seconds': 0.724657, 'client_cpu_percent': 52.83874249501586, 'create_flows_per_sec': 79806.46135038463, 'process_flows_per_sec': 73052.23563004023, 'end_to_end_flows_per_sec': 72915.5207153396}
```

## 2026-06-11 13:40:51 IDT native Flow command create-many 100k dirty-after-DBOS

```bash
python examples/protocol_flow_commands_benchmark.py --url ferric://127.0.0.1:16388 --operation create-many --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0 --pretty
```

```text
{
  "batch_latency_avg_ms": 93.43287793919444,
  "batch_latency_max_ms": 108.25479193590581,
  "batch_latency_p50_ms": 101.97137494105846,
  "batch_latency_p95_ms": 108.16125001292676,
  "batch_latency_p99_ms": 108.19683293811977,
  "batch_latency_samples": 200,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 19.38493865096924,
  "client_cpu_seconds": 0.065586,
  "completed": 100000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 100000,
  "inflight_batches": 64,
  "items_per_sec": 297473.5202509721,
  "operation": "create-many",
  "partitions": 16,
  "payload_bytes": 0,
  "protocol_lanes": 32,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-f61bbdb601",
  "seconds": 0.3361643749522045,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 0.0,
  "total_seconds": 0.3383348339702934,
  "url": "ferric://127.0.0.1:16388"
}
```

## 2026-06-11 13:40:51 IDT native Flow command complete-many 100k dirty-after-DBOS

```bash
python examples/protocol_flow_commands_benchmark.py --url ferric://127.0.0.1:16388 --operation complete-many --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0 --pretty
```

```text
{
  "batch_latency_avg_ms": 126.46861441549845,
  "batch_latency_max_ms": 199.34220903087407,
  "batch_latency_p50_ms": 122.49737500678748,
  "batch_latency_p95_ms": 195.39541704580188,
  "batch_latency_p99_ms": 198.4599999850616,
  "batch_latency_samples": 200,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 6.670846518453883,
  "client_cpu_seconds": 0.314088,
  "completed": 100000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 100000,
  "inflight_batches": 64,
  "items_per_sec": 206932.74697027524,
  "operation": "complete-many",
  "partitions": 16,
  "payload_bytes": 0,
  "protocol_lanes": 32,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-89f6ae2499",
  "seconds": 0.4832487920066342,
  "setup_batch_size": 500,
  "setup_claim_seconds": 2.1045105409575626,
  "setup_empty_claims": 0,
  "setup_seconds": 2.117558249970898,
  "total_seconds": 4.708367957966402,
  "url": "ferric://127.0.0.1:16388"
}
```

## 2026-06-11 13:40:56 IDT native Flow command start-and-claim 100k dirty-after-DBOS

```bash
python examples/protocol_flow_commands_benchmark.py --url ferric://127.0.0.1:16388 --operation start-and-claim --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0 --pretty
```

```text
{
  "batch_latency_avg_ms": 1300.6392208556645,
  "batch_latency_max_ms": 1757.374083972536,
  "batch_latency_p50_ms": 1412.6435839571059,
  "batch_latency_p95_ms": 1757.2542499983683,
  "batch_latency_p99_ms": 1757.368833059445,
  "batch_latency_samples": 200,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 30.897901878528366,
  "client_cpu_seconds": 1.341808,
  "completed": 100000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 100000,
  "inflight_batches": 64,
  "items_per_sec": 23040.38368680175,
  "operation": "start-and-claim",
  "partitions": 16,
  "payload_bytes": 0,
  "protocol_lanes": 32,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-333a8be81c",
  "seconds": 4.340205500018783,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 0.0,
  "total_seconds": 4.3427155839744955,
  "url": "ferric://127.0.0.1:16388"
}
```

## 2026-06-11 13:41:00 IDT native Flow command step 100k dirty-after-DBOS

```bash
python examples/protocol_flow_commands_benchmark.py --url ferric://127.0.0.1:16388 --operation step --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0 --pretty
```

```text
{
  "batch_latency_avg_ms": 408.17912976955995,
  "batch_latency_max_ms": 508.2712909206748,
  "batch_latency_p50_ms": 414.9299579439685,
  "batch_latency_p95_ms": 489.0218749642372,
  "batch_latency_p99_ms": 503.37387493345886,
  "batch_latency_samples": 200,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 50.283364010700495,
  "client_cpu_seconds": 2.9505559999999997,
  "completed": 100000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 100000,
  "inflight_batches": 64,
  "items_per_sec": 62774.89684319602,
  "operation": "step",
  "partitions": 16,
  "payload_bytes": 0,
  "protocol_lanes": 32,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-f31486fa0b",
  "seconds": 1.5929934580344707,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 4.272132458980195,
  "total_seconds": 5.867857208941132,
  "url": "ferric://127.0.0.1:16388"
}
```

## 2026-06-11 13:41:53 IDT clean native DBOS queue 1M after submit_batch ordering fix

```bash
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 1000000 --workers 16 --producers 4 --partitions 16 --server-shards 16 --claim-batch-size 500 --claim-partition-batch-size 16 --claim-drain-batches 2 --create-batch-size 500 --complete-async-depth 4 --protocol-worker-connections 1 --protocol-lanes 32 --transport many --queued-shape live --worker-api queue --worker-mode polling --partition-mode auto --claim-job-only
```

```text
{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-26568fb903b04dc991f2083790d7a77c', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 0.0, 'producer_backpressure_waits': 0, 'producer_backpressure_wait_ms': 0.0, 'producer_backpressure_limited_batches': 0, 'producer_queue_latency_ewma_ms': 16.951150629443006, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 27.0719701099, 'queue_latency_p50_ms': 22.68675, 'queue_latency_p95_ms': 55.269792, 'queue_latency_p99_ms': 97.736416, 'queue_latency_max_ms': 157.008708, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2052, 'process_empty_claims': 0, 'process_fallback_claims': 4, 'process_avg_claim_batch': 487.32943469785573, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 15.429950750083663, 'process_seconds': 15.488334041903727, 'total_seconds': 15.490738291991875, 'client_cpu_seconds': 7.464541, 'client_cpu_percent': 48.18712226168642, 'create_flows_per_sec': 64809.0208579945, 'process_flows_per_sec': 64564.72318420416, 'end_to_end_flows_per_sec': 64554.702374447974}
```

## Benchmark logging rule

Every protocol benchmark run must append a new entry here before using the number for decisions.

Required fields:

- Timestamp
- Repo/branch context when known
- Clean or dirty server data dir
- Server command/config when relevant
- Client benchmark command
- Result summary: throughput, latency, errors/rejects/empty claims
- Interpretation: accepted baseline, regression, noisy run, or dirty-data diagnostic

Do not compare native protocol, RESP/memtier, queue, workflow, or DBOS-style numbers unless the command shape and server configuration are written in this file.

## 2026-06-11 13:57 Asia/Jerusalem - clean source - protocol FLOW.START_AND_CLAIM job-only compact

Server:

```bash
cd /Users/yoavgea/repos/ferricstore
rm -rf /tmp/ferricstore-protocol-bench
mkdir -p /tmp/ferricstore-protocol-bench/data
MIX_ENV=prod FERRICSTORE_NATIVE_ENABLED=true FERRICSTORE_SHARD_COUNT=16 FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data FERRICSTORE_PORT=16379 FERRICSTORE_HEALTH_PORT=16380 FERRICSTORE_NATIVE_PORT=16388 mix run --no-halt
```

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py --url ferric://127.0.0.1:16388 --operation start-and-claim --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0 --pretty
```

Result:

```text
completed: 100000
items_per_sec: 27716.48/s
total_seconds: 3.617
batch_latency_avg_ms: 1083.88
batch_latency_p50_ms: 1161.18
batch_latency_p95_ms: 1469.10
batch_latency_p99_ms: 1469.26
client_cpu_percent: 11.79
errors: 0
connections: 1
protocol_lanes: 32
```

Interpretation:

Clean-source run after `FLOW.START_AND_CLAIM RETURN JOBS_COMPACT` compact pipeline result shaping. Better than prior dirty sample around 23k/s; still durable-write limited, not socket limited.

## 2026-06-11 13:58 Asia/Jerusalem - clean source - native KV SET 30s

Server: same clean source server as previous entry, 16 shards, native port `16388`, data dir `/tmp/ferricstore-protocol-bench/data`.

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --command set --test-time 30 --clients 1 --threads 1 --processes 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --binary-keys --value-bytes 32 --pretty
```

Result:

```text
requests: 58248000
requests_per_sec: 1940689.64/s
seconds: 30.014
batch_latency_avg_ms: 32.753
batch_latency_p50_ms: 30.591
batch_latency_p95_ms: 41.247
batch_latency_p99_ms: 59.908
batch_latency_max_ms: 115.246
client_cpu_percent: 90.58
errors: 0
total_connections: 1
protocol_lanes: 64
pipeline: 1000
inflight_batches: 64
```

Interpretation:

Clean native SET 30s remains in the same order as the previous protocol compact-pipeline baseline. No correctness errors; client CPU is high, so this shape is at least partly client/encoding bound.

## 2026-06-11 13:59 Asia/Jerusalem - clean source - native KV GET 30s

Server: same clean source server as previous entries, 16 shards, native port `16388`, data dir `/tmp/ferricstore-protocol-bench/data`.

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --command get --test-time 30 --clients 1 --threads 1 --processes 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --binary-keys --value-bytes 32 --pretty
```

Result:

```text
requests: 52797000
requests_per_sec: 1759745.53/s
seconds: 30.003
batch_latency_avg_ms: 36.136
batch_latency_p50_ms: 36.138
batch_latency_p95_ms: 37.576
batch_latency_p99_ms: 38.455
batch_latency_max_ms: 41.488
client_cpu_percent: 102.65
errors: 0
total_connections: 1
protocol_lanes: 64
pipeline: 1000
inflight_batches: 64
warmed_keys: 100000
```

Interpretation:

Clean native GET 30s remains near the previous protocol GET baseline. One Python process is CPU-saturated, so further native KV GET gains probably need SDK/client encode-decode reduction or multi-process benchmark shape, not server Raft work.

## 2026-06-11 14:00 Asia/Jerusalem - dirty source - DBOS-style queue diagnostic

Server: same source server as previous entries, but data dir was dirty after 58M SET and 52M GET protocol KV runs. Not apples-to-apples with clean DBOS queue baselines.

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/dbos_style_benchmark.py --url ferric://127.0.0.1:16388 --mode queued --queued-shape live --transport many --flows 100000 --workers 16 --producers 4 --partitions 16 --claim-batch-size 100 --create-batch-size 500 --worker-api queue --claim-state queued --claim-job-only --complete-batch --independent-many --complete-independent-many --protocol-worker-connections 1 --protocol-lanes 32 --server-shards 16 --protocol-wake-hints
```

Result:

```text
created: 100000
completed: 100000
end_to_end_flows_per_sec: 35666.06/s
create_flows_per_sec: 42321.36/s
process_flows_per_sec: 35696.36/s
total_seconds: 2.804
create_seconds: 2.363
process_seconds: 2.801
process_claim_calls: 1193
process_empty_claims: 37
process_avg_claim_batch: 83.82
queue_latency_avg_ms: 143.48
queue_latency_p50_ms: 83.81
queue_latency_p95_ms: 402.75
queue_latency_p99_ms: 1702.55
producer_backpressure_rate_per_sec: 50000.0
producer_backpressure_limited_batches: 13
client_cpu_percent: 33.72
errors: 0
```

Interpretation:

Dirty diagnostic only. The run was throttled by adaptive producer backpressure and followed heavy KV data-dir load, so it should not be used as clean DBOS regression evidence.

## 2026-06-11 14:02 Asia/Jerusalem - clean source - DBOS-style queue uncapped diagnostic

Server:

```bash
cd /Users/yoavgea/repos/ferricstore
rm -rf /tmp/ferricstore-protocol-bench
mkdir -p /tmp/ferricstore-protocol-bench/data
MIX_ENV=prod FERRICSTORE_NATIVE_ENABLED=true FERRICSTORE_SHARD_COUNT=16 FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data FERRICSTORE_PORT=16379 FERRICSTORE_HEALTH_PORT=16380 FERRICSTORE_NATIVE_PORT=16388 mix run --no-halt
```

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/dbos_style_benchmark.py --url ferric://127.0.0.1:16388 --mode queued --queued-shape live --transport many --flows 100000 --workers 16 --producers 4 --partitions 16 --claim-batch-size 100 --create-batch-size 500 --worker-api queue --claim-state queued --claim-job-only --complete-batch --independent-many --complete-independent-many --protocol-worker-connections 1 --protocol-lanes 32 --server-shards 16 --protocol-wake-hints --producer-max-rate-per-sec 0
```

Result:

```text
created: 100000
completed: 100000
end_to_end_flows_per_sec: 44782.74/s
create_flows_per_sec: 51806.20/s
process_flows_per_sec: 44828.03/s
total_seconds: 2.233
create_seconds: 1.930
process_seconds: 2.231
process_claim_calls: 1156
process_empty_claims: 28
process_avg_claim_batch: 86.51
queue_latency_avg_ms: 131.17
queue_latency_p50_ms: 62.88
queue_latency_p95_ms: 461.36
queue_latency_p99_ms: 1096.99
producer_backpressure_rate_per_sec: 206391.21
producer_backpressure_limited_batches: 4
client_cpu_percent: 41.56
errors: 0
```

Interpretation:

Clean but below prior remembered 70k+ DBOS-style queue result. This suggests the high-throughput command shape differed, or a later SDK/server change altered queue scheduling. Do not use this as accepted peak baseline yet.

## 2026-06-11 14:04 Asia/Jerusalem - clean source - DBOS-style native protocol accepted baseline

Server:

```bash
cd /Users/yoavgea/repos/ferricstore
rm -rf /tmp/ferricstore-protocol-bench
mkdir -p /tmp/ferricstore-protocol-bench/data
MIX_ENV=prod FERRICSTORE_NATIVE_ENABLED=true FERRICSTORE_SHARD_COUNT=16 FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data FERRICSTORE_PORT=16379 FERRICSTORE_HEALTH_PORT=16380 FERRICSTORE_NATIVE_PORT=16388 mix run --no-halt
```

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 100000 --workers 16 --producers 4 --partitions 16 --server-shards 16 --claim-batch-size 500 --claim-partition-batch-size 16 --claim-drain-batches 2 --create-batch-size 500 --complete-async-depth 4 --protocol-worker-connections 1 --protocol-lanes 32 --transport many --queued-shape live --worker-api queue --worker-mode polling --partition-mode auto --claim-job-only
```

Result:

```text
created: 100000
completed: 100000
end_to_end_flows_per_sec: 75823.79/s
create_flows_per_sec: 85313.78/s
process_flows_per_sec: 75962.99/s
total_seconds: 1.319
create_seconds: 1.172
process_seconds: 1.316
process_claim_calls: 266
process_empty_claims: 1
process_avg_claim_batch: 375.94
process_max_claim_batch: 783
queue_latency_avg_ms: 15.90
queue_latency_p50_ms: 13.86
queue_latency_p95_ms: 29.18
queue_latency_p99_ms: 34.44
producer_backpressure_limited_batches: 0
client_cpu_percent: 53.63
errors: 0
```

Interpretation:

Accepted clean native protocol DBOS-style queue baseline. This matches/improves the prior recorded 100k clean result (`~72.9k/s`) when using the exact optimized command shape. Earlier lower runs used different claim batch / completion / wake-hint settings and are not apples-to-apples.

## 2026-06-11 14:05 Asia/Jerusalem - dirty source after 100k DBOS - DBOS-style native protocol 1M diagnostic

Server: same source server as accepted 100k DBOS entry, but data dir already contained the prior 100k DBOS run. Not a clean 1M-only run.

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 1000000 --workers 16 --producers 4 --partitions 16 --server-shards 16 --claim-batch-size 500 --claim-partition-batch-size 16 --claim-drain-batches 2 --create-batch-size 500 --complete-async-depth 4 --protocol-worker-connections 1 --protocol-lanes 32 --transport many --queued-shape live --worker-api queue --worker-mode polling --partition-mode auto --claim-job-only
```

Result:

```text
created: 1000000
completed: 1000000
end_to_end_flows_per_sec: 60733.53/s
create_flows_per_sec: 60927.72/s
process_flows_per_sec: 60743.93/s
total_seconds: 16.465
create_seconds: 16.413
process_seconds: 16.463
process_claim_calls: 2051
process_empty_claims: 0
process_avg_claim_batch: 487.57
process_max_claim_batch: 1000
queue_latency_avg_ms: 29.17
queue_latency_p50_ms: 25.08
queue_latency_p95_ms: 57.41
queue_latency_p99_ms: 88.94
producer_backpressure_limited_batches: 0
client_cpu_percent: 46.18
errors: 0
```

Interpretation:

Dirty diagnostic only. Lower than prior clean 1M protocol DBOS result (`~64.55k/s`); run followed a 100k DBOS load, so clean restart is required before declaring regression.

## 2026-06-11 14:06 Asia/Jerusalem - clean source - DBOS-style native protocol 1M sustained accepted

Server:

```bash
cd /Users/yoavgea/repos/ferricstore
rm -rf /tmp/ferricstore-protocol-bench
mkdir -p /tmp/ferricstore-protocol-bench/data
MIX_ENV=prod FERRICSTORE_NATIVE_ENABLED=true FERRICSTORE_SHARD_COUNT=16 FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data FERRICSTORE_PORT=16379 FERRICSTORE_HEALTH_PORT=16380 FERRICSTORE_NATIVE_PORT=16388 mix run --no-halt
```

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 1000000 --workers 16 --producers 4 --partitions 16 --server-shards 16 --claim-batch-size 500 --claim-partition-batch-size 16 --claim-drain-batches 2 --create-batch-size 500 --complete-async-depth 4 --protocol-worker-connections 1 --protocol-lanes 32 --transport many --queued-shape live --worker-api queue --worker-mode polling --partition-mode auto --claim-job-only
```

Result:

```text
created: 1000000
completed: 1000000
end_to_end_flows_per_sec: 63894.70/s
create_flows_per_sec: 64159.87/s
process_flows_per_sec: 63904.78/s
total_seconds: 15.651
create_seconds: 15.586
process_seconds: 15.648
process_claim_calls: 2054
process_empty_claims: 0
process_avg_claim_batch: 486.85
process_max_claim_batch: 1000
queue_latency_avg_ms: 28.10
queue_latency_p50_ms: 23.84
queue_latency_p95_ms: 56.88
queue_latency_p99_ms: 82.67
producer_backpressure_limited_batches: 0
client_cpu_percent: 48.36
errors: 0
```

Interpretation:

Accepted clean sustained native protocol DBOS-style queue check. Within ~1% of the previous clean 1M record (`~64.55k/s`), so the `START_AND_CLAIM jobs_compact` work did not materially regress the queue path.

## 2026-06-11 14:10 Asia/Jerusalem - clean source - native KV GET preset `get-throughput`

Server:

```bash
cd /Users/yoavgea/repos/ferricstore
rm -rf /tmp/ferricstore-protocol-bench
mkdir -p /tmp/ferricstore-protocol-bench/data
MIX_ENV=prod FERRICSTORE_NATIVE_ENABLED=true FERRICSTORE_SHARD_COUNT=16 FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data FERRICSTORE_PORT=16379 FERRICSTORE_HEALTH_PORT=16380 FERRICSTORE_NATIVE_PORT=16388 mix run --no-halt
```

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset get-throughput --binary-keys --value-bytes 32 --pretty
```

Result:

```text
requests: 67471000
requests_per_sec: 2248900.20/s
seconds: 30.002
request_mode: many
pipeline: 1000
batch_latency_avg_ms: 28.308
batch_latency_p50_ms: 28.261
batch_latency_p95_ms: 29.159
batch_latency_p99_ms: 31.328
batch_latency_max_ms: 33.047
client_cpu_percent: 102.38
errors: 0
total_connections: 1
protocol_lanes: 64
warmed_keys: 100000
```

Interpretation:

Explicit bulk `MGET.COMPACT` native shape is faster than pipeline-of-GET shape (`~1.76M/s`) but still one Python process CPU-bound. Use this as the current one-process native GET throughput ceiling.

## 2026-06-11 14:11 Asia/Jerusalem - clean source - native KV SET preset `set-throughput`

Server: same clean source server as previous KV preset entry.

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset set-throughput --binary-keys --value-bytes 32 --pretty
```

Result:

```text
requests: 62078500
requests_per_sec: 2068622.50/s
seconds: 30.010
request_mode: many
pipeline: 500
batch_latency_avg_ms: 15.377
batch_latency_p50_ms: 14.836
batch_latency_p95_ms: 18.746
batch_latency_p99_ms: 25.828
batch_latency_max_ms: 56.536
client_cpu_percent: 99.24
errors: 0
total_connections: 1
protocol_lanes: 64
```

Interpretation:

Explicit bulk `MSET` native shape has similar throughput to pipeline SET (`~2.09M/s`) with better batch p99 latency. One Python process remains CPU-saturated.

## 2026-06-11 14:14 Asia/Jerusalem - clean source - native KV GET after pack_into encoder experiment rejected

Server: clean source server, 16 shards, native port `16388`, data dir `/tmp/ferricstore-protocol-bench/data`.

Client:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset get-throughput --binary-keys --value-bytes 32 --pretty
```

Result:

```text
requests: 60004000
requests_per_sec: 2000008.19/s
seconds: 30.002
request_mode: many
pipeline: 1000
batch_latency_avg_ms: 31.847
batch_latency_p50_ms: 31.798
batch_latency_p95_ms: 32.747
batch_latency_p99_ms: 35.531
batch_latency_max_ms: 39.897
client_cpu_percent: 102.10
errors: 0
```

Interpretation:

Rejected optimization. Replacing per-length `Struct.pack()` with `pack_into` helper made GET slower than the pre-change clean preset (`~2.25M/s`). Revert this encoder experiment.

## 2026-06-11 - Native KV GET after compact encoder revert validation

Status: accepted validation run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, clean data dir `/tmp/ferricstore-protocol-bench/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: validates that the rejected Python `pack_into` compact encoder experiment was reverted and GET throughput returned to baseline range.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --binary-keys \
  --value-bytes 32 \
  --pretty
```

Result:

```text
requests_per_sec: 2,241,157.93/s
requests: 67,238,000
seconds: 30.001
errors: 0
command: get
preset: get-throughput
request_mode: many
pipeline: 1000
protocol_lanes: 64
inflight_batches: 64
total_connections: 1
processes: 1
threads: 1
client_cpu_percent: 102.38%
client_cpu_seconds: 30.72
batch_latency_avg_ms: 28.406
batch_latency_p50_ms: 28.374
batch_latency_p95_ms: 29.222
batch_latency_p99_ms: 31.376
batch_latency_max_ms: 32.821
key_count: 100,000
value_bytes: 32
binary_keys: true
```

Interpretation:

```text
GET throughput returned to the pre-experiment range (~2.25M/s).
The Python pack_into encoder change is rejected; it regressed GET and was reverted.
Current bottleneck for protocol KV GET is client-side Python CPU/object work, not server durability.
```

## 2026-06-11 - Native Flow CREATE_MANY protocol benchmark

Status: accepted clean source run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, clean data dir `/tmp/ferricstore-protocol-bench/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation create-many \
  --flows 100000 \
  --batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: create-many
items_per_sec: 281,517.13/s
completed: 100,000 / 100,000
total_seconds: 0.366
seconds: 0.355
errors: 0
batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
payload_bytes: 16
batch_latency_avg_ms: 101.668
batch_latency_p50_ms: 105.942
batch_latency_p95_ms: 119.517
batch_latency_p99_ms: 119.543
batch_latency_max_ms: 119.546
client_cpu_percent: 17.98%
run_id: protocol-flow-4a9a1ca463
```

Interpretation:

```text
Native protocol CREATE_MANY remains durable-write/server bound, not client CPU bound.
Throughput is in the same range as prior dirty create-many samples (~297k/s), slightly lower on this clean run.
```

## 2026-06-11 - Native Flow COMPLETE_MANY protocol benchmark

Status: accepted clean source run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir `/tmp/ferricstore-protocol-bench/data` reused from current clean benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation complete-many \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: complete-many
items_per_sec: 180,786.97/s
completed: 100,000 / 100,000
timed_command_seconds: 0.553
total_seconds: 5.133
setup_seconds: 2.014
setup_claim_seconds: 2.564
setup_empty_claims: 0
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
payload_bytes: 16
batch_latency_avg_ms: 152.866
batch_latency_p50_ms: 123.674
batch_latency_p95_ms: 264.859
batch_latency_p99_ms: 267.590
batch_latency_max_ms: 269.789
client_cpu_percent: 6.13%
run_id: protocol-flow-2ce9223f6c
```

Interpretation:

```text
Complete phase is server durable-write bound, not client CPU bound.
Measured complete throughput is lower than CREATE_MANY because terminal apply writes state/history/index cleanup and retention metadata.
```

## 2026-06-11 - Native Flow START_AND_CLAIM protocol benchmark

Status: accepted clean source run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir `/tmp/ferricstore-protocol-bench/data` reused from current clean benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation start-and-claim \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: start-and-claim
items_per_sec: 30,567.90/s
completed: 100,000 / 100,000
total_seconds: 3.274
seconds: 3.271
errors: 0
batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
payload_bytes: 16
batch_latency_avg_ms: 1005.536
batch_latency_p50_ms: 1047.276
batch_latency_p95_ms: 1064.233
batch_latency_p99_ms: 1064.624
batch_latency_max_ms: 1064.673
client_cpu_percent: 12.87%
run_id: protocol-flow-1a979ece5a
```

Interpretation:

```text
Fused start-and-claim path improved versus prior clean job-only sample (~27.7k/s), but remains much slower than create-only because it combines create, immediate lease/claim mutation, and returned job metadata.
The bottleneck is server durable apply/index work, not protocol client CPU.
```

## 2026-06-11 - Native Flow STEP protocol benchmark

Status: accepted clean source run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir `/tmp/ferricstore-protocol-bench/data` reused from current clean benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation step \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: step
items_per_sec: 60,181.30/s
completed: 100,000 / 100,000
timed_step_seconds: 1.662
total_seconds: 6.845
setup_seconds: 5.181
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
payload_bytes: 16
batch_latency_avg_ms: 425.530
batch_latency_p50_ms: 440.685
batch_latency_p95_ms: 476.642
batch_latency_p99_ms: 479.510
batch_latency_max_ms: 480.512
client_cpu_percent: 44.28%
run_id: protocol-flow-808614abe0
```

Interpretation:

```text
STEP is much faster than START_AND_CLAIM because it does not perform claim_due leasing and returned-job hydration.
It is still durable-write/server bound; client CPU is meaningful but not saturated.
```

## 2026-06-11 - Native Flow START_AND_CLAIM deeper-lane diagnostic

Status: rejected diagnostic run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: tests whether START_AND_CLAIM improves with deeper native protocol lanes/inflight.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation start-and-claim \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 128 \
  --connections 1 \
  --protocol-lanes 128 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: start-and-claim
items_per_sec: 20,245.91/s
completed: 100,000 / 100,000
total_seconds: 4.942
seconds: 4.939
errors: 0
batch_size: 500
inflight_batches: 128
connections: 1
protocol_lanes: 128
partitions: 16
batch_latency_avg_ms: 2622.183
batch_latency_p50_ms: 3197.689
batch_latency_p95_ms: 3221.424
batch_latency_p99_ms: 3221.529
batch_latency_max_ms: 3223.529
client_cpu_percent: 8.61%
run_id: protocol-flow-44ff61cb83
```

Interpretation:

```text
Rejected. Deeper native lanes/inflight made START_AND_CLAIM slower than the clean 64-lane run (`30.6k/s`) and greatly increased batch latency.
This points away from client-side concurrency and toward server durable apply/WAL/index contention under excessive in-flight work.
```

## 2026-06-11 - Native Flow START_AND_CLAIM lower-lane diagnostic

Status: rejected diagnostic run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: tests whether START_AND_CLAIM improves with lower native protocol lanes/inflight.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation start-and-claim \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 32 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: start-and-claim
items_per_sec: 19,351.52/s
completed: 100,000 / 100,000
total_seconds: 5.170
seconds: 5.168
errors: 0
batch_size: 500
inflight_batches: 32
connections: 1
protocol_lanes: 32
partitions: 16
batch_latency_avg_ms: 802.423
batch_latency_p50_ms: 788.572
batch_latency_p95_ms: 1006.444
batch_latency_p99_ms: 1006.551
batch_latency_max_ms: 1006.669
client_cpu_percent: 8.49%
run_id: protocol-flow-69eb40164b
```

Interpretation:

```text
Rejected. Lower lanes/inflight did not improve START_AND_CLAIM on the dirty session.
The clean 64-lane accepted sample (`30.6k/s`) remains the best current point; additional diagnostics should use a fresh clean data dir before changing defaults.
```

## 2026-06-11 - Native Flow CLAIM_DUE protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation claim-due \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: claim-due
items_per_sec: 33,389.78/s
completed: 100,000 / 100,000
timed_claim_seconds: 2.995
total_seconds: 5.367
setup_seconds: 2.370
setup_empty_claims: 0
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 14.826
batch_latency_p50_ms: 10.855
batch_latency_p95_ms: 34.782
batch_latency_p99_ms: 56.361
batch_latency_max_ms: 109.730
client_cpu_percent: 4.41%
run_id: protocol-flow-645d304189
```

Interpretation:

```text
Claim_due returns 500-job batches with low per-batch latency, but item throughput is bounded by serialized claim/lease apply and returned job metadata.
Because this ran after several large writes in the same data dir, use as diagnostic only; rerun clean before accepting as baseline.
```

## 2026-06-11 - Native Flow TRANSITION_MANY protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation transition-many \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: transition-many
items_per_sec: 154,846.16/s
completed: 100,000 / 100,000
timed_transition_seconds: 0.646
total_seconds: 2.938
setup_seconds: 2.290
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 188.320
batch_latency_p50_ms: 208.348
batch_latency_p95_ms: 230.486
batch_latency_p99_ms: 230.602
batch_latency_max_ms: 230.672
client_cpu_percent: 7.46%
run_id: protocol-flow-a0648b3f99
```

Interpretation:

```text
TRANSITION_MANY is slower than CREATE_MANY and close to COMPLETE_MANY shape, consistent with state decode/update/encode plus lifecycle index/history work.
Use as diagnostic only because the data dir is dirty.
```

## 2026-06-11 - Native Flow RETRY_MANY protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation retry-many \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: retry-many
items_per_sec: 148,905.35/s
completed: 100,000 / 100,000
timed_retry_seconds: 0.672
total_seconds: 5.430
setup_seconds: 2.142
setup_claim_seconds: 2.614
setup_empty_claims: 0
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 183.729
batch_latency_p50_ms: 162.634
batch_latency_p95_ms: 255.512
batch_latency_p99_ms: 260.140
batch_latency_max_ms: 261.075
client_cpu_percent: 6.02%
run_id: protocol-flow-b5ede2b314
```

Interpretation:

```text
RETRY_MANY is in the same band as TRANSITION_MANY, consistent with requeue lifecycle index mutation and history/state writes.
Use as diagnostic only because the data dir is dirty.
```

## 2026-06-11 - Native Flow FAIL_MANY protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation fail-many \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: fail-many
items_per_sec: 137,568.89/s
completed: 100,000 / 100,000
timed_fail_seconds: 0.727
total_seconds: 7.280
setup_seconds: 2.300
setup_claim_seconds: 4.250
setup_empty_claims: 0
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 204.473
batch_latency_p50_ms: 200.984
batch_latency_p95_ms: 327.881
batch_latency_p99_ms: 330.835
batch_latency_max_ms: 331.961
client_cpu_percent: 4.44%
run_id: protocol-flow-78c623b2ec
```

Interpretation:

```text
FAIL_MANY is slightly slower than RETRY_MANY/TRANSITION_MANY in this dirty session.
Claim setup time grew, likely because the dirty data dir has accumulated many prior flow records/index entries; clean rerun needed before treating this as baseline.
```

## 2026-06-11 - Native Flow CANCEL_MANY protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation cancel-many \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: cancel-many
items_per_sec: 161,318.24/s
completed: 100,000 / 100,000
timed_cancel_seconds: 0.620
total_seconds: 2.897
setup_seconds: 2.274
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 182.529
batch_latency_p50_ms: 193.084
batch_latency_p95_ms: 204.606
batch_latency_p99_ms: 204.952
batch_latency_max_ms: 204.958
client_cpu_percent: 6.66%
run_id: protocol-flow-b2af5974b6
```

Interpretation:

```text
CANCEL_MANY is in the transition/terminal command band but faster than fail/retry in this run, likely because this benchmark cancels created flows without claim/lease setup.
Use as diagnostic only because the data dir is dirty.
```

## 2026-06-11 - Native VALUE.PUT protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation value-put \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --pretty
```

Result:

```text
operation: value-put
items_per_sec: 69,582.86/s
completed: 100,000 / 100,000
total_seconds: 1.440
seconds: 1.437
errors: 0
batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
payload_bytes: 128
batch_latency_avg_ms: 383.046
batch_latency_p50_ms: 351.733
batch_latency_p95_ms: 651.759
batch_latency_p99_ms: 713.185
batch_latency_max_ms: 723.869
client_cpu_percent: 90.77%
run_id: protocol-flow-2b6d53b098
```

Interpretation:

```text
VALUE.PUT with 128-byte values is substantially more client/protocol encode-heavy than terminal Flow commands; client CPU is near saturated.
Further gains likely require reducing Python frame construction/copying or adding a native/compiled client encoder, not just server tuning.
```

## 2026-06-11 - Native VALUE.MGET protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation value-mget \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --pretty
```

Result:

```text
operation: value-mget
items_per_sec: 2,073,423.41/s
completed: 100,000 / 100,000
timed_mget_seconds: 0.048
total_seconds: 1.406
setup_seconds: 1.355
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
payload_bytes: 128
batch_latency_avg_ms: 12.612
batch_latency_p50_ms: 14.895
batch_latency_p95_ms: 15.251
batch_latency_p99_ms: 15.459
batch_latency_max_ms: 15.464
client_cpu_percent: 100.13%
run_id: protocol-flow-230cce2fec
```

Interpretation:

```text
VALUE.MGET read phase is close to native KV GET throughput and is client CPU saturated.
Setup VALUE.PUT dominates total wall time; read path itself is protocol/client decode bound rather than server durable-write bound.
```

## 2026-06-11 - Native FLOW.GET protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-get \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: flow-get
items_per_sec: 72,859.32/s
completed: 100,000 / 100,000
timed_get_seconds: 1.373
total_seconds: 3.585
setup_seconds: 2.210
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 348.414
batch_latency_p50_ms: 386.022
batch_latency_p95_ms: 409.806
batch_latency_p99_ms: 413.267
batch_latency_max_ms: 415.641
client_cpu_percent: 40.78%
run_id: protocol-flow-1708b46a19
```

Interpretation:

```text
FLOW.GET is not client CPU saturated; this suggests server state lookup/decode/response shaping dominates over protocol socket throughput.
Use as diagnostic only because the data dir is dirty.
```

## 2026-06-11 - Native FLOW.HISTORY protocol benchmark

Status: diagnostic dirty-session run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Mode: eventual consistency, hot recent history only.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-history \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: flow-history
items_per_sec: 237,116.78/s
completed: 100,000 / 100,000
timed_history_seconds: 0.422
total_seconds: 2.570
setup_seconds: 2.146
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
flow_read_consistency: eventual
flow_history_include_cold: false
batch_latency_avg_ms: 113.152
batch_latency_p50_ms: 133.832
batch_latency_p95_ms: 135.355
batch_latency_p99_ms: 135.928
batch_latency_max_ms: 136.260
client_cpu_percent: 19.74%
run_id: protocol-flow-0d449a4768
```

Interpretation:

```text
Hot/eventual FLOW.HISTORY is substantially faster than FLOW.GET in this run.
Current history path appears reasonably efficient for hot recent history; cold/consistent mode should be benchmarked separately because it exercises LMDB projection/watermark cost.
```

## 2026-06-11 - Native FLOW.LIST protocol benchmark

Status: diagnostic dirty-session run; performance outlier.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, dirty data dir from current benchmark session.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-list \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: flow-list
items_per_sec: 1,979.37/s
completed: 100,000 / 100,000
timed_list_seconds: 50.521
total_seconds: 52.671
setup_seconds: 2.147
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 15,771.402
batch_latency_p50_ms: 16,530.970
batch_latency_p95_ms: 20,273.686
batch_latency_p99_ms: 20,273.858
batch_latency_max_ms: 20,273.954
client_cpu_percent: 1.80%
run_id: protocol-flow-57902c6e22
```

Interpretation:

```text
FLOW.LIST is the major outlier in the protocol command coverage run.
Client CPU is idle, so bottleneck is server query/list path, likely index scan/range hydration/response construction under dirty data-dir scale.
Investigate this path before treating Flow query performance as production-ready.
```

## 2026-06-11 - Native FLOW.LIST after sparse auto-partition optimization

Status: accepted clean source run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, clean data dir `/tmp/ferricstore-protocol-bench/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: validates server optimization that skips empty global auto partition state indexes before rank-range reads.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-list \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: flow-list
items_per_sec: 117,512.57/s
completed: 100,000 / 100,000
timed_list_seconds: 0.851
total_seconds: 2.823
setup_seconds: 1.963
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 241.837
batch_latency_p50_ms: 189.031
batch_latency_p95_ms: 432.245
batch_latency_p99_ms: 460.421
batch_latency_max_ms: 468.465
client_cpu_percent: 31.28%
run_id: protocol-flow-c979649b9b
```

Comparison:

```text
Before optimization, dirty-session FLOW.LIST outlier: 1,979.37/s, p99 20,273.858ms.
After optimization, clean-source FLOW.LIST: 117,512.57/s, p99 460.421ms.
The main removed cost is issuing rank-range reads against empty hidden auto-partition indexes.
```

Correctness evidence:

```text
mix test apps/ferricstore/test/ferricstore/flow_test.exs --trace
218 tests, 0 failures
```

## 2026-06-11 - DBOS-style protocol queue benchmark after FLOW.LIST optimization

Status: accepted clean source run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, clean data dir `/tmp/ferricstore-protocol-bench/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: validates that the FLOW.LIST read-path optimization did not regress the queue/write hot path.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
flows: 100,000
created: 100,000
completed: 100,000
claimed_items: 100,000
end_to_end_flows_per_sec: 85,193.10/s
create_flows_per_sec: 94,629.54/s
process_flows_per_sec: 85,356.80/s
total_seconds: 1.174
create_seconds: 1.057
process_seconds: 1.172
errors/duplicate_completions: 0
workers: 16
producers: 4
partitions: 16
server_shards: 16
transport: many
worker_api: queue
worker_mode: queue-api
partition_mode: auto
claim_batch_size: 500
claim_partition_batch_size: 16
claim_drain_batches: 2
create_batch_size: 500
complete_async_depth: 4
protocol_worker_connections: 1
protocol_lanes: 32
claim_job_only: true
protocol_wake_hints: false
process_claim_calls: 261
process_empty_claims: 1
process_fallback_claims: 5
process_avg_claim_batch: 383.142
process_max_claim_batch: 798
wake_notifications: 256
wake_credits: 100,000
queue_latency_avg_ms: 15.291
queue_latency_p50_ms: 13.473
queue_latency_p95_ms: 25.255
queue_latency_p99_ms: 44.002
queue_latency_max_ms: 44.278
client_cpu_percent: 59.48%
```

Interpretation:

```text
No DBOS-style queue regression from the FLOW.LIST read-path optimization.
This run is above prior accepted 100k protocol DBOS sample (~75.8k/s), likely helped by current system conditions and/or recent protocol path work.
Queue path remains healthy with one native protocol worker connection.
```

## 2026-06-11 - Native KV SET 30s after FLOW.LIST optimization

Status: accepted diagnostic run; server has dirty Flow data from DBOS validation.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir `/tmp/ferricstore-protocol-bench/data` after DBOS run.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --binary-keys \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: set
requests_per_sec: 2,102,782.03/s
requests: 63,105,000
seconds: 30.010
errors: 0
preset: set-throughput
request_mode: many
pipeline: 500
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
processes: 1
threads: 1
client_cpu_percent: 100.21%
client_cpu_seconds: 30.073
batch_latency_avg_ms: 15.126
batch_latency_p50_ms: 14.703
batch_latency_p95_ms: 16.597
batch_latency_p99_ms: 24.453
batch_latency_max_ms: 73.047
key_count: 100,000
value_bytes: 32
binary_keys: true
```

Interpretation:

```text
KV SET remains in the prior native protocol range (~2.07M/s to ~2.10M/s) and is client CPU saturated.
No evidence that the FLOW.LIST server read-path change affected KV write throughput.
```

## 2026-06-11 - Native KV GET 30s after FLOW.LIST optimization

Status: accepted diagnostic run; server has dirty Flow data from DBOS validation and prior KV SET warmup.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir `/tmp/ferricstore-protocol-bench/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --binary-keys \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: get
requests_per_sec: 2,176,247.02/s
requests: 65,291,000
seconds: 30.002
errors: 0
preset: get-throughput
request_mode: many
pipeline: 1000
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
processes: 1
threads: 1
client_cpu_percent: 102.83%
client_cpu_seconds: 30.850
batch_latency_avg_ms: 29.256
batch_latency_p50_ms: 29.257
batch_latency_p95_ms: 29.964
batch_latency_p99_ms: 30.643
batch_latency_max_ms: 32.641
key_count: 100,000
value_bytes: 32
binary_keys: true
```

Interpretation:

```text
KV GET remains close to the accepted native protocol range (~2.17M/s to ~2.24M/s) and is client CPU saturated.
No evidence that the FLOW.LIST read-path change affected KV read throughput.
```

## 2026-06-11 - Native FLOW.GET after get-only pipeline fast path

Status: accepted clean source run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, clean data dir `/tmp/ferricstore-protocol-bench/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: validates server optimization for get-only/no-payload `pipeline_read_batch`, avoiding generic mixed-read split/merge overhead.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-get \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
operation: flow-get
items_per_sec: 81,079.02/s
completed: 100,000 / 100,000
timed_get_seconds: 1.233
total_seconds: 3.200
setup_seconds: 1.957
errors: 0
batch_size: 500
setup_batch_size: 500
inflight_batches: 64
connections: 1
protocol_lanes: 64
partitions: 16
batch_latency_avg_ms: 323.439
batch_latency_p50_ms: 363.845
batch_latency_p95_ms: 393.162
batch_latency_p99_ms: 394.578
batch_latency_max_ms: 396.719
client_cpu_percent: 40.94%
run_id: protocol-flow-5da441bf2a
```

Comparison:

```text
Before accepted diagnostic FLOW.GET: 72,859.32/s, p99 413.267ms.
After get-only/no-payload pipeline fast path: 81,079.02/s, p99 394.578ms.
Approx gain: +11.3% throughput, modest latency reduction.
```

Correctness evidence:

```text
mix test apps/ferricstore/test/ferricstore/flow/pipeline_read_test.exs apps/ferricstore/test/ferricstore/flow/pipeline_read_command_test.exs --trace
5 tests, 0 failures

mix format --check-formatted apps/ferricstore/lib/ferricstore/flow.ex apps/ferricstore/lib/ferricstore/flow/pipeline_read.ex apps/ferricstore/test/ferricstore/flow/pipeline_read_test.exs
passed
```

## 2026-06-11 - DBOS-style protocol queue benchmark after FLOW.GET fast path - sample 1

Status: low clean source guardrail sample; rerun needed before judging regression.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, clean data dir `/tmp/ferricstore-protocol-bench/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: write-path guardrail after `FLOW.GET` get-only pipeline fast path. This optimization should not affect DBOS queue writes.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
flows: 100,000
created: 100,000
completed: 100,000
claimed_items: 100,000
end_to_end_flows_per_sec: 66,980.03/s
create_flows_per_sec: 71,255.84/s
process_flows_per_sec: 67,085.00/s
total_seconds: 1.493
create_seconds: 1.403
process_seconds: 1.491
errors/duplicate_completions: 0
process_claim_calls: 265
process_empty_claims: 1
process_fallback_claims: 9
process_avg_claim_batch: 377.358
process_max_claim_batch: 794
queue_latency_avg_ms: 20.044
queue_latency_p50_ms: 15.405
queue_latency_p95_ms: 42.478
queue_latency_p99_ms: 122.042
queue_latency_max_ms: 188.243
client_cpu_percent: 47.59%
```

Interpretation:

```text
Low sample versus previous clean guardrail (`85.2k/s`) and prior accepted 100k sample (`75.8k/s`).
Because the code change is read-path only and the server/data dir had just restarted clean, rerun before treating this as a regression.
```

## 2026-06-11 - DBOS-style protocol queue benchmark after FLOW.GET fast path - sample 2

Status: accepted dirty-session guardrail sample.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir reused after sample 1.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: rerun after low sample 1 to check variance.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
flows: 100,000
created: 100,000
completed: 100,000
claimed_items: 100,000
end_to_end_flows_per_sec: 93,966.76/s
create_flows_per_sec: 98,713.98/s
process_flows_per_sec: 94,162.98/s
total_seconds: 1.064
create_seconds: 1.013
process_seconds: 1.062
errors/duplicate_completions: 0
process_claim_calls: 260
process_empty_claims: 0
process_fallback_claims: 4
process_avg_claim_batch: 384.615
process_max_claim_batch: 790
queue_latency_avg_ms: 15.700
queue_latency_p50_ms: 14.552
queue_latency_p95_ms: 26.748
queue_latency_p99_ms: 31.399
queue_latency_max_ms: 38.721
client_cpu_percent: 64.81%
```

Interpretation:

```text
Sample 2 strongly rebounded above prior accepted 100k DBOS protocol samples.
Sample 1 was likely cold-start/system noise rather than a regression from the read-path fast path.
```

## 2026-06-11 - DBOS-style protocol queue benchmark after FLOW.GET fast path - sample 3

Status: low dirty-session guardrail sample.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir reused after samples 1-2.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: third DBOS guardrail sample to understand variance after FLOW.GET fast path.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
flows: 100,000
created: 100,000
completed: 100,000
claimed_items: 100,000
end_to_end_flows_per_sec: 57,785.38/s
create_flows_per_sec: 61,099.90/s
process_flows_per_sec: 57,867.12/s
total_seconds: 1.731
create_seconds: 1.637
process_seconds: 1.728
errors/duplicate_completions: 0
process_claim_calls: 261
process_empty_claims: 0
process_fallback_claims: 5
process_avg_claim_batch: 383.142
process_max_claim_batch: 784
queue_latency_avg_ms: 23.575
queue_latency_p50_ms: 17.332
queue_latency_p95_ms: 61.670
queue_latency_p99_ms: 147.559
queue_latency_max_ms: 155.500
client_cpu_percent: 42.34%
```

Interpretation:

```text
Low sample with high queue p99, despite no errors and normal claim batch fill.
Together with sample 2 (`94.0k/s`) this shows high local variance/background pressure.
Do not claim DBOS improvement from this change; use DBOS only as no-obvious-correctness-regression guardrail here.
```

## 2026-06-11 - Native KV SET 30s after FLOW.GET fast path - dirty low sample

Status: rejected dirty-session guardrail sample.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, data dir reused after three DBOS samples.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: KV guardrail after FLOW.GET read-path optimization, but server was dirty from DBOS write benchmarks.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --binary-keys \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: set
requests_per_sec: 832,173.95/s
requests: 25,105,000
seconds: 30.168
errors: 0
preset: set-throughput
request_mode: many
pipeline: 500
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
processes: 1
threads: 1
client_cpu_percent: 38.84%
batch_latency_avg_ms: 38.349
batch_latency_p50_ms: 35.573
batch_latency_p95_ms: 48.090
batch_latency_p99_ms: 131.128
batch_latency_max_ms: 267.175
```

Interpretation:

```text
Rejected as a KV baseline. Throughput is far below prior SET range (~2.07M/s to ~2.10M/s) and client CPU is not saturated.
Likely server/background persistence pressure from prior DBOS dirty data. Clean restart required before accepted KV guardrail.
```

## 2026-06-11 - Native KV SET 30s after FLOW.GET fast path - clean unique dir

Status: accepted clean source guardrail run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, unique clean data dir `/tmp/ferricstore-protocol-bench-clean-1781178676/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: accepted KV guardrail after rejecting dirty low SET sample.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --binary-keys \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: set
requests_per_sec: 2,041,305.20/s
requests: 61,262,500
seconds: 30.011
errors: 0
preset: set-throughput
request_mode: many
pipeline: 500
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
processes: 1
threads: 1
client_cpu_percent: 97.70%
client_cpu_seconds: 29.322
batch_latency_avg_ms: 15.579
batch_latency_p50_ms: 14.918
batch_latency_p95_ms: 19.610
batch_latency_p99_ms: 27.714
batch_latency_max_ms: 55.603
key_count: 100,000
value_bytes: 32
binary_keys: true
```

Interpretation:

```text
Clean SET guardrail is back in normal native protocol range (~2.04M/s to ~2.10M/s).
The prior 832k/s SET sample was dirty-data/background pressure, not a code regression.
```

## 2026-06-11 - Native KV GET 30s after FLOW.GET fast path - clean unique dir

Status: accepted clean source guardrail run.
Repo/server: `/Users/yoavgea/repos/ferricstore`, source server, 16 shards, unique clean data dir `/tmp/ferricstore-protocol-bench-clean-1781178676/data`.
SDK repo: `/Users/yoavgea/repos/ferricstore-python`.
Protocol: `ferric://127.0.0.1:16388`.
Context: accepted KV GET guardrail after FLOW.GET read-path optimization and after user-interrupted earlier GET run.

Command:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --binary-keys \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: get
requests_per_sec: 2,257,664.88/s
requests: 67,735,000
seconds: 30.002
errors: 0
preset: get-throughput
request_mode: many
pipeline: 1000
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
processes: 1
threads: 1
client_cpu_percent: 102.40%
client_cpu_seconds: 30.723
batch_latency_avg_ms: 28.199
batch_latency_p50_ms: 28.163
batch_latency_p95_ms: 29.025
batch_latency_p99_ms: 30.926
batch_latency_max_ms: 32.548
key_count: 100,000
value_bytes: 32
binary_keys: true
```

Interpretation:

```text
Clean GET guardrail is healthy and slightly above prior accepted range (~2.17M/s to ~2.24M/s).
The FLOW.GET read-path optimization did not regress KV GET.
```

## 2026-06-11 - FLOW.GET after same-partition fast path, sample 1

Server: source server, native port 16388, 16 shards, clean dir `/tmp/ferricstore-protocol-bench-clean-1781179009/data`.
Command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-get \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
items_per_sec: 76,534/s
completed: 100,000
errors: 0
seconds: 1.307s
total_seconds: 3.289s
batch_latency_p50_ms: 382.019
batch_latency_p95_ms: 416.843
batch_latency_p99_ms: 417.204
client_cpu_percent: 42.2%
```

Interpretation: lower than prior get-only fast-path sample (`81,079/s`), slightly outside the 5% gate. Needs more samples before accepting or reverting the same-partition branch.


## 2026-06-11 - FLOW.GET after same-partition fast path, sample 2

Server: source server, native port 16388, 16 shards, clean dir `/tmp/ferricstore-protocol-bench-clean-1781179009/data`.

Result:

```text
items_per_sec: 79,234/s
completed: 100,000
errors: 0
seconds: 1.262s
total_seconds: 3.292s
batch_latency_p50_ms: 387.192
batch_latency_p95_ms: 402.383
batch_latency_p99_ms: 405.284
client_cpu_percent: 40.7%
```


## 2026-06-11 - FLOW.GET after same-partition fast path, sample 3

Server: source server, native port 16388, 16 shards, clean dir `/tmp/ferricstore-protocol-bench-clean-1781179009/data`.

Result:

```text
items_per_sec: 79,262/s
completed: 100,000
errors: 0
seconds: 1.262s
total_seconds: 3.263s
batch_latency_p50_ms: 381.944
batch_latency_p95_ms: 402.380
batch_latency_p99_ms: 405.162
client_cpu_percent: 41.1%
```

## 2026-06-11 - Native KV SET 30s guardrail after FLOW.GET fast path work

Server: source server, native port 16388, 16 shards, warm/dirty from prior FLOW.GET setup data.
Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 1,184,307/s
requests: 35,545,500
errors: 0
seconds: 30.014s
total_connections: 1
request_mode: many
pipeline: 500
protocol_lanes: 64
inflight_batches: 64
batch_latency_p50_ms: 26.858
batch_latency_p95_ms: 34.005
batch_latency_p99_ms: 39.305
client_cpu_percent: 56.6%
```

Interpretation: durable SET protocol guardrail remains healthy on a warm/dirty source server. This is lower than earlier short clean samples above 2M/s, so use as a 30s sustained guardrail, not peak.

## 2026-06-11 - Native KV GET 30s guardrail after FLOW.GET fast path work

Server: source server, native port 16388, 16 shards, warm/dirty from prior FLOW.GET and SET benchmark data.
Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 2,020,565/s
requests: 60,621,000
errors: 0
seconds: 30.002s
total_connections: 1
request_mode: many
pipeline: 1000
protocol_lanes: 64
inflight_batches: 64
batch_latency_p50_ms: 31.556
batch_latency_p95_ms: 32.825
batch_latency_p99_ms: 33.596
client_cpu_percent: 102.6%
warmed_keys: 100,000
```

Interpretation: sustained native GET is stable around 2.0M/s on one client process/socket shape and is client-CPU saturated. Further GET gains need client encode/decode/loop optimization or multi-process client load, not server read-path work first.

## 2026-06-11 - Native KV GET 30s after prebuilt keys + direct bulk submit

Server: source server, native port 16388, 16 shards, same warm/dirty server as previous 30s GET guardrail.
Change: throughput presets now prebuild benchmark keys and use direct `ProtocolAdapter.submit_mget_compact()` instead of constructing `("MGET.COMPACT", ...)` command tuples per batch.
Correctness: `python -m pytest tests/test_protocol.py tests/test_protocol_kv_benchmark.py -q` -> `88 passed`.
Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 2,849,667/s
requests: 85,495,000
errors: 0
seconds: 30.002s
total_connections: 1
request_mode: many
pipeline: 1000
protocol_lanes: 64
inflight_batches: 64
prebuild_keys: true
binary_keys: false
batch_latency_p50_ms: 22.356
batch_latency_p95_ms: 23.969
batch_latency_p99_ms: 24.458
client_cpu_percent: 102.8%
warmed_keys: 100,000
```

Comparison: previous same-server 30s GET guardrail was `2,020,565/s`, p99 `33.596ms`. This is +41.0% throughput and lower latency. Remaining limit is still one Python client process at ~100% CPU.

## 2026-06-11 - Native KV SET 30s after prebuilt keys + direct bulk submit

Server: source server, native port 16388, 16 shards, same warm/dirty server as previous 30s SET guardrail.
Change: throughput presets now prebuild benchmark keys and use direct `ProtocolAdapter.submit_mset_same_value()` instead of constructing `("MSET", ...)` command tuples per batch.
Correctness: `python -m pytest tests/test_protocol.py tests/test_protocol_kv_benchmark.py -q` -> `88 passed`.
Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 1,357,541/s
requests: 40,738,000
errors: 0
seconds: 30.009s
total_connections: 1
request_mode: many
pipeline: 500
protocol_lanes: 64
inflight_batches: 64
prebuild_keys: true
binary_keys: false
batch_latency_p50_ms: 23.266
batch_latency_p95_ms: 29.650
batch_latency_p99_ms: 33.628
client_cpu_percent: 32.9%
```

Comparison: previous same-server 30s SET guardrail was `1,184,307/s`, p99 `39.305ms`. This is +14.6% throughput and lower latency. Client CPU is only 32.9%, so remaining SET limit is server durable write/Raft/Bitcask batching, not Python client construction.

## 2026-06-11 - Native KV SET 30s after server scalar MSET status path

Server: source server, native port 16388, 16 shards, fresh data dir `/tmp/ferricstore-protocol-bench-set-status/data`, `MIX_ENV=prod`.
Change: direct/native scalar `MSET` now uses `Router.batch_quorum_put_status/2`, which submits the same durable Raft/WARaft/Bitcask shard batches but returns `:ok` or the first error without rebuilding the ordered per-key result list.
Correctness:

```bash
mix test apps/ferricstore/test/ferricstore/store/router_batch_put_status_test.exs \
  apps/ferricstore/test/ferricstore/embedded_api_write_errors_test.exs \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace
# 58 tests, 0 failures

mix format --check-formatted apps/ferricstore/lib/ferricstore/impl.ex \
  apps/ferricstore/lib/ferricstore/store/router/part_05.ex \
  apps/ferricstore/test/ferricstore/store/router_batch_put_status_test.exs
# passed
```

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 2,226,801/s
requests: 66,828,000
errors: 0
seconds: 30.011s
total_connections: 1
request_mode: many
pipeline: 500
protocol_lanes: 64
inflight_batches: 64
prebuild_keys: true
binary_keys: false
batch_latency_p50_ms: 13.848
batch_latency_p95_ms: 18.569
batch_latency_p99_ms: 23.087
batch_latency_max_ms: 67.122
client_cpu_percent: 55.9%
```

Comparison: previous same-client 30s SET after client direct bulk submit was `1,357,541/s`, p99 `33.628ms`. This is +64.0% throughput and lower p99 latency. `SET/MSET` remains durable; this change only removes per-key response reconstruction for scalar native MSET replies.

## 2026-06-11 - Native KV GET 30s guardrail after server scalar MSET status path

Server: source server, native port 16388, 16 shards, same server as scalar MSET status benchmark.
Purpose: read-path guardrail after write-path routing/status aggregation change.
Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 2,839,874/s
requests: 85,203,000
errors: 0
seconds: 30.002s
total_connections: 1
request_mode: many
pipeline: 1000
protocol_lanes: 64
inflight_batches: 64
prebuild_keys: true
binary_keys: false
batch_latency_p50_ms: 22.525
batch_latency_p95_ms: 23.300
batch_latency_p99_ms: 23.713
client_cpu_percent: 104.0%
warmed_keys: 100,000
```

Interpretation: no GET regression from the server MSET status optimization. Result is effectively unchanged from previous prebuilt/direct-bulk GET sample (`2,849,667/s`, p99 `24.458ms`) and remains one Python client process CPU-bound.

## 2026-06-11 - Protocol DBOS-style queued 100k guardrail after scalar MSET status path

Server: source server, native port 16388, 16 shards, same server as scalar MSET status benchmark.
Purpose: Flow/protocol guardrail after native KV write-path optimization.
Wrapper command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000
```

Expanded command from dry-run:

```bash
python examples/dbos_style_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --mode queued \
  --queued-shape live \
  --transport many \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --retention-ttl-ms 0 \
  --server-shards 16 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --claim-job-only \
  --no-reclaim-expired
```

Result:

```text
end_to_end_flows_per_sec: 70,831/s
create_flows_per_sec: 75,679/s
process_flows_per_sec: 70,947/s
created/completed: 100,000 / 100,000
claimed_items: 100,000
errors/duplicates: 0 duplicate completions
process_claim_calls: 267
process_empty_claims: 0
process_avg_claim_batch: 374.5
process_max_claim_batch: 786
queue_latency_avg_ms: 18.860
queue_latency_p50_ms: 14.733
queue_latency_p95_ms: 44.217
queue_latency_p99_ms: 74.302
queue_latency_max_ms: 115.394
wake_notifications: 256
wake_credits: 100,000
client_cpu_percent: 51.1%
total_seconds: 1.412s
```

Interpretation: no Flow/DBOS regression from the scalar native MSET status optimization. Flow guardrail remains around the expected 70k/s live queued range on this local source server.

## 2026-06-11 - Native KV SET 30s scalar MSET status path stability sample

Server: source server, native port 16388, 16 shards, same patched server as prior scalar MSET status benchmark.
Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 2,214,876/s
requests: 66,468,500
errors: 0
seconds: 30.010s
total_connections: 1
request_mode: many
pipeline: 500
protocol_lanes: 64
inflight_batches: 64
prebuild_keys: true
binary_keys: false
batch_latency_p50_ms: 13.596
batch_latency_p95_ms: 18.700
batch_latency_p99_ms: 28.347
batch_latency_max_ms: 96.321
client_cpu_percent: 55.7%
```

Interpretation: confirms the scalar native MSET status-path improvement is stable. Two 30s samples are `2,226,801/s` and `2,214,876/s`.

## 2026-06-11 - FLOW.START_AND_CLAIM fresh baseline after KV MSET optimizations

Server: source server, native port 16388, 16 shards, same patched server as scalar MSET status benchmark, dirty from prior KV/DBOS runs.
Command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation start-and-claim \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
items_per_sec: 16,975/s
completed: 100,000
errors: 0
seconds: 5.891s
total_seconds: 5.893s
batch_latency_p50_ms: 1,935.584
batch_latency_p95_ms: 1,974.385
batch_latency_p99_ms: 1,983.369
batch_latency_max_ms: 1,983.583
client_cpu_percent: 7.3%
```

Interpretation: server-side Flow fused start/claim path is the next bottleneck. Client CPU is low, so this is not a Python protocol encode limit.

## 2026-06-11 - FLOW.START_AND_CLAIM after server batch fast path

Command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation start-and-claim \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
items_per_sec: 209,811/s
completed: 100,000 / 100,000
errors: 0
seconds: 0.477s
batch_latency_p50_ms: 135.428
batch_latency_p95_ms: 147.636
batch_latency_p99_ms: 149.812
client_cpu_percent: 89.9%
connections: 1
protocol_lanes: 64
```

Notes:

```text
Server change: homogeneous FLOW.START_AND_CLAIM compact pipeline now routes through a dedicated batch Ra command.
Correctness guard: duplicate/pre-existing item fallback remains per-item and was covered by focused native test.
Previous same-shape baseline: ~16,975/s, p99 batch ~1,983ms.
```

## 2026-06-11 - DBOS guardrail after start-and-claim batch path, default protocol lanes

Command:

```bash
python examples/dbos_style_benchmark.py \
  --mode queued \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 500 \
  --url ferric://127.0.0.1:16388
```

Result:

```text
end_to_end_flows_per_sec: 46,383/s
create_flows_per_sec: 52,732/s
process_flows_per_sec: 46,431/s
created/completed: 100,000 / 100,000
process_claim_calls: 1,154
process_empty_claims: 17
process_avg_claim_batch: 86.66
queue_latency_p50_ms: 63.167
queue_latency_p95_ms: 310.632
queue_latency_p99_ms: 507.630
client_cpu_percent: 42.9%
protocol_lanes: 32
protocol_worker_connections: 1
```

Notes:

```text
Low guardrail sample. Not apples-to-apples with the prior ~70.8k/s protocol guardrail because this run used default protocol_lanes=32.
Next run uses explicit optimized protocol_lanes=64.
```

## 2026-06-11 - DBOS guardrail after start-and-claim batch path, protocol_lanes=64 hung

Command:

```bash
python examples/dbos_style_benchmark.py \
  --mode queued \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 500 \
  --protocol-lanes 64 \
  --url ferric://127.0.0.1:16388
```

Result:

```text
No result after ~90s. Process killed manually.
```

Notes:

```text
This is a regression symptom or benchmark/client scheduling issue under protocol_lanes=64 after the current server changes.
Server printed no obvious errors during the hang.
Need inspect client/server protocol state before accepting the start-and-claim optimization slice.
```

## 2026-06-11 - DBOS guardrail after fresh server restart, protocol_lanes=64

Command:

```bash
python examples/dbos_style_benchmark.py \
  --mode queued \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 500 \
  --protocol-lanes 64 \
  --url ferric://127.0.0.1:16388
```

Result:

```text
end_to_end_flows_per_sec: 50,207/s
create_flows_per_sec: 53,690/s
process_flows_per_sec: 50,267/s
created/completed: 100,000 / 100,000
process_claim_calls: 1,151
process_empty_claims: 2
process_avg_claim_batch: 86.88
queue_latency_p50_ms: 60.505
queue_latency_p95_ms: 360.309
queue_latency_p99_ms: 710.256
client_cpu_percent: 47.1%
protocol_lanes: 64
protocol_worker_connections: 1
```

Notes:

```text
Fresh source server; no hang.
Still below prior ~70.8k/s DBOS guardrail. Need compare exact prior flags before treating this as server regression.
```

## 2026-06-11 - DBOS guardrail apples-to-apples wrapper after start-and-claim batch path, sample 1

Command:

```bash
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 100000
```

Expanded effective shape:

```text
flows: 100000
workers: 16
producers: 4
claim_batch_size: 500
worker_capacity: 500
create_batch_size: 500
protocol_worker_connections: 1
protocol_lanes: 32
transport: many
worker_api: queue
partition_mode: auto
claim_job_only: true
```

Result:

```text
end_to_end_flows_per_sec: 66,942/s
create_flows_per_sec: 70,345/s
process_flows_per_sec: 67,050/s
created/completed: 100,000 / 100,000
process_claim_calls: 260
process_empty_claims: 1
process_avg_claim_batch: 384.62
process_max_claim_batch: 1000
queue_latency_p50_ms: 16.063
queue_latency_p95_ms: 43.852
queue_latency_p99_ms: 111.029
client_cpu_percent: 49.1%
```

Notes:

```text
Corrected config mistake from previous low DBOS runs. This sample is near but below prior ~70.8k/s guardrail; running additional samples before judging regression.
```

## 2026-06-11 - DBOS wrapper samples 2/3 accidentally run concurrently, invalid

Command for each concurrent process:

```bash
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 100000
```

Results:

```text
sample_concurrent_a_end_to_end: 41,376/s
sample_concurrent_b_end_to_end: 38,681/s
```

Notes:

```text
Invalid apples-to-apples samples. They were launched concurrently and competed on the same single source server.
Do not use these for regression decisions.
```

## 2026-06-11 - DBOS guardrail apples-to-apples wrapper after clean restart, serial valid sample

Command:

```bash
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 100000
```

Result:

```text
end_to_end_flows_per_sec: 71,424/s
create_flows_per_sec: 75,948/s
process_flows_per_sec: 71,542/s
created/completed: 100,000 / 100,000
process_claim_calls: 266
process_empty_claims: 1
process_avg_claim_batch: 375.94
process_max_claim_batch: 789
queue_latency_p50_ms: 15.344
queue_latency_p95_ms: 40.368
queue_latency_p99_ms: 103.504
client_cpu_percent: 52.0%
```

Notes:

```text
Valid serial apples-to-apples sample on a fresh source server.
No DBOS regression from the FLOW.START_AND_CLAIM batch path; result is above prior ~70.8k/s guardrail.
```

## 2026-06-11 - Final DBOS guardrail on patched start-and-claim batch code

Command:

```bash
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 100000
```

Result:

```text
end_to_end_flows_per_sec: 73,389/s
create_flows_per_sec: 83,061/s
process_flows_per_sec: 73,521/s
created/completed: 100,000 / 100,000
process_claim_calls: 272
process_empty_claims: 1
process_avg_claim_batch: 367.65
process_max_claim_batch: 1000
queue_latency_p50_ms: 14.210
queue_latency_p95_ms: 28.793
queue_latency_p99_ms: 101.266
client_cpu_percent: 52.1%
```

Notes:

```text
Current patched source server, clean data dir.
No DBOS regression; final sample is above prior ~70.8k/s guardrail.
```

## 2026-06-11 - Final FLOW.START_AND_CLAIM after server batch fast path

Command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation start-and-claim \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --pretty
```

Result:

```text
items_per_sec: 222,539/s
completed: 100,000 / 100,000
errors: 0
seconds: 0.449s
batch_latency_p50_ms: 137.302
batch_latency_p95_ms: 144.925
batch_latency_p99_ms: 146.806
client_cpu_percent: 97.7%
connections: 1
protocol_lanes: 64
```

Notes:

```text
Current patched source server.
Previous same-shape baseline before server batch path: ~16,975/s, p99 batch ~1,983ms.
```

## 2026-06-11 - Final native KV SET 30s after start-and-claim batch path

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 1,590,108/s
requests: 47,728,500
errors: 0
batch_latency_p50_ms: 19.414
batch_latency_p95_ms: 24.904
batch_latency_p99_ms: 39.634
client_cpu_percent: 40.7%
request_mode: many
pipeline: 500
protocol_lanes: 64
prebuild_keys: true
```

Notes:

```text
SET is durable via the normal Ra/WARaft + Bitcask write path.
Current source server already had Flow benchmark data; KV keyspace is independent.
```

## 2026-06-11 - Final native KV GET 30s after start-and-claim batch path

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --test-time 30 \
  --pretty
```

Result:

```text
requests_per_sec: 2,818,909/s
requests: 84,576,000
errors: 0
batch_latency_p50_ms: 22.686
batch_latency_p95_ms: 24.533
batch_latency_p99_ms: 25.936
client_cpu_percent: 104.0%
request_mode: many
pipeline: 1000
protocol_lanes: 64
prebuild_keys: true
warmed_keys: 100000
```

Notes:

```text
GET is served from the current store/read path with native compact MGET.
No KV regression from the Flow start-and-claim server batch change.
```

## 2026-06-11 - Flow command matrix 50k after start-and-claim batch path

Command shape:

```bash
for op in create-many transition-many complete-many retry-many fail-many cancel-many claim-due step signal value-put-owned flow-get flow-history flow-list; do
  python examples/protocol_flow_commands_benchmark.py \
    --url ferric://127.0.0.1:16388 \
    --operation "$op" \
    --flows 50000 \
    --batch-size 500 \
    --setup-batch-size 500 \
    --inflight-batches 64 \
    --connections 1 \
    --protocol-lanes 64 \
    --partitions 16 \
    --payload-bytes 16 \
    --pretty
done
```

Summary:

```text
create-many:      289,999/s, p99 batch 112.420ms, errors 0
transition-many:  236,733/s, p99 batch 137.818ms, errors 0
complete-many:    228,179/s, p99 batch 122.320ms, errors 0
retry-many:       203,809/s, p99 batch 160.281ms, errors 0
fail-many:        209,106/s, p99 batch 123.528ms, errors 0
cancel-many:      302,221/s, p99 batch 116.064ms, errors 0
claim-due:         44,935/s, p99 batch 28.808ms, errors 0
step:              59,442/s, p99 batch 442.296ms, errors 0
signal:           224,486/s, p99 batch 131.612ms, errors 0
value-put-owned:   15,631/s, p99 batch 2234.919ms, errors 0
flow-get:          76,402/s, p99 batch 375.093ms, errors 0
flow-history:     247,465/s, p99 batch 128.879ms, errors 0
flow-list:         52,508/s, p99 batch 727.514ms, errors 0
```

Notes:

```text
Fast writes are healthy after start-and-claim batch path.
Next bottleneck candidate: value-put-owned. It is much slower than other write commands and has high batch latency.
```
### operation=create-many
{
  "batch_latency_avg_ms": 88.04695086204447,
  "batch_latency_max_ms": 112.43974999524653,
  "batch_latency_p50_ms": 88.09804101474583,
  "batch_latency_p95_ms": 112.12008306756616,
  "batch_latency_p99_ms": 112.42029094137251,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 17.969623435632975,
  "client_cpu_seconds": 0.032703,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 289999.1624260763,
  "operation": "create-many",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-aa2871d3fb",
  "seconds": 0.17241429106798023,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 0.0,
  "total_seconds": 0.18199045804794878,
  "url": "ferric://127.0.0.1:16388"
}

### operation=transition-many
{
  "batch_latency_avg_ms": 102.65159999253228,
  "batch_latency_max_ms": 137.85420800559223,
  "batch_latency_p50_ms": 127.91200005449355,
  "batch_latency_p95_ms": 137.7489579608664,
  "batch_latency_p99_ms": 137.81750004272908,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 8.441010499158368,
  "client_cpu_seconds": 0.103266,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 236733.03668530652,
  "operation": "transition-many",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-a72358e899",
  "seconds": 0.2112083750544116,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 1.0099741249578074,
  "total_seconds": 1.2233843330759555,
  "url": "ferric://127.0.0.1:16388"
}

### operation=complete-many
{
  "batch_latency_avg_ms": 95.46699083177373,
  "batch_latency_max_ms": 122.32612504158169,
  "batch_latency_p50_ms": 104.5852079987526,
  "batch_latency_p95_ms": 122.2565839998424,
  "batch_latency_p99_ms": 122.31970799621195,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 6.878435282527781,
  "client_cpu_seconds": 0.15632200000000002,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 228179.48140715144,
  "operation": "complete-many",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-9d77030fe0",
  "seconds": 0.2191257500089705,
  "setup_batch_size": 500,
  "setup_claim_seconds": 1.099162666941993,
  "setup_empty_claims": 0,
  "setup_seconds": 0.9518087910255417,
  "total_seconds": 2.2726389589952305,
  "url": "ferric://127.0.0.1:16388"
}

### operation=retry-many
{
  "batch_latency_avg_ms": 119.04879336012527,
  "batch_latency_max_ms": 160.28112499043345,
  "batch_latency_p50_ms": 116.15258292295039,
  "batch_latency_p95_ms": 159.5139999408275,
  "batch_latency_p99_ms": 160.28074990026653,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 6.664599721560001,
  "client_cpu_seconds": 0.167766,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 203809.436450659,
  "operation": "retry-many",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-eaf948b97b",
  "seconds": 0.24532720795832574,
  "setup_batch_size": 500,
  "setup_claim_seconds": 1.2179943749215454,
  "setup_empty_claims": 0,
  "setup_seconds": 1.0514020000118762,
  "total_seconds": 2.517270458978601,
  "url": "ferric://127.0.0.1:16388"
}

### operation=fail-many
{
  "batch_latency_avg_ms": 99.77399881579913,
  "batch_latency_max_ms": 123.56508302036673,
  "batch_latency_p50_ms": 107.12458298075944,
  "batch_latency_p95_ms": 123.23537503834814,
  "batch_latency_p99_ms": 123.52837494108826,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 6.539228724903888,
  "client_cpu_seconds": 0.16256400000000001,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 209106.44600625726,
  "operation": "fail-many",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-d73a6c6ef4",
  "seconds": 0.2391126670408994,
  "setup_batch_size": 500,
  "setup_claim_seconds": 1.1223063750658184,
  "setup_empty_claims": 0,
  "setup_seconds": 1.1218031670432538,
  "total_seconds": 2.485981250065379,
  "url": "ferric://127.0.0.1:16388"
}

### operation=cancel-many
{
  "batch_latency_avg_ms": 82.29207074386068,
  "batch_latency_max_ms": 116.09616701025516,
  "batch_latency_p50_ms": 99.36649992596358,
  "batch_latency_p95_ms": 115.97591696772724,
  "batch_latency_p99_ms": 116.0635839914903,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 7.661807136851129,
  "client_cpu_seconds": 0.09742699999999999,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 302220.56445689657,
  "operation": "cancel-many",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-0a9cca0e1b",
  "seconds": 0.16544208396226168,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 1.1036090420093387,
  "total_seconds": 1.2715929578989744,
  "url": "ferric://127.0.0.1:16388"
}

### operation=claim-due
{
  "batch_latency_avg_ms": 10.908281515060247,
  "batch_latency_max_ms": 31.143459025770426,
  "batch_latency_p50_ms": 8.83270800113678,
  "batch_latency_p95_ms": 19.819166976958513,
  "batch_latency_p99_ms": 28.808290953747928,
  "batch_latency_samples": 102,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 5.283527834225112,
  "client_cpu_seconds": 0.115626,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 44935.459200235746,
  "operation": "claim-due",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-0cf846dce9",
  "seconds": 1.1127069999929518,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 1.073318999959156,
  "total_seconds": 2.1884241671068594,
  "url": "ferric://127.0.0.1:16388"
}

### operation=step
{
  "batch_latency_avg_ms": 331.63751414977014,
  "batch_latency_max_ms": 442.58770800661296,
  "batch_latency_p50_ms": 339.90020805504173,
  "batch_latency_p95_ms": 438.2578330114484,
  "batch_latency_p99_ms": 442.29599996469915,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 100.17510665755658,
  "client_cpu_seconds": 1.492153,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 59442.07667442694,
  "operation": "step",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-3712f40e82",
  "seconds": 0.8411549999145791,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 0.6456421670736745,
  "total_seconds": 1.4895447080489248,
  "url": "ferric://127.0.0.1:16388"
}

### operation=signal
{
  "batch_latency_avg_ms": 90.7005565718282,
  "batch_latency_max_ms": 131.6667499486357,
  "batch_latency_p50_ms": 102.74345800280571,
  "batch_latency_p95_ms": 131.50016707368195,
  "batch_latency_p99_ms": 131.61170796956867,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 18.478203869368546,
  "client_cpu_seconds": 0.249739,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 224485.87196867357,
  "operation": "signal",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-e2e363b4fe",
  "seconds": 0.22273116593714803,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 1.1263233330100775,
  "total_seconds": 1.3515328749781474,
  "url": "ferric://127.0.0.1:16388"
}

### operation=value-put-owned
{
  "batch_latency_avg_ms": 1510.9982283390127,
  "batch_latency_max_ms": 2237.2652500635013,
  "batch_latency_p50_ms": 1709.8700419301167,
  "batch_latency_p95_ms": 2206.18141698651,
  "batch_latency_p99_ms": 2234.918624977581,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 75.90455108581232,
  "client_cpu_seconds": 3.2427040000000003,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 15630.80537888839,
  "operation": "value-put-owned",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-9d9c556523",
  "seconds": 3.1988114999840036,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 1.0707802079850808,
  "total_seconds": 4.272081125061959,
  "url": "ferric://127.0.0.1:16388"
}

### operation=flow-get
{
  "batch_latency_avg_ms": 269.35284788836725,
  "batch_latency_max_ms": 375.2131670480594,
  "batch_latency_p50_ms": 304.91195898503065,
  "batch_latency_p95_ms": 373.59983404166996,
  "batch_latency_p99_ms": 375.09283295366913,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 42.72916817916539,
  "client_cpu_seconds": 0.693914,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 76401.50932571084,
  "operation": "flow-get",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-137e12fc72",
  "seconds": 0.6544373329961672,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 0.9671630410011858,
  "total_seconds": 1.6239820000482723,
  "url": "ferric://127.0.0.1:16388"
}

### operation=flow-history
{
  "batch_latency_avg_ms": 87.83353499369696,
  "batch_latency_max_ms": 129.01675002649426,
  "batch_latency_p50_ms": 100.06095806602389,
  "batch_latency_p95_ms": 128.8223749725148,
  "batch_latency_p99_ms": 128.87920800130814,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 20.80108634753467,
  "client_cpu_seconds": 0.23942299999999997,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 247464.5193516881,
  "operation": "flow-history",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-5934fdcfa4",
  "seconds": 0.20204916701186448,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 0.9467339160619304,
  "total_seconds": 1.1510120000457391,
  "url": "ferric://127.0.0.1:16388"
}

### operation=flow-list
{
  "batch_latency_avg_ms": 534.5307345839683,
  "batch_latency_max_ms": 727.605541003868,
  "batch_latency_p50_ms": 713.5043749585748,
  "batch_latency_p95_ms": 727.1664999425411,
  "batch_latency_p99_ms": 727.5135829113424,
  "batch_latency_samples": 100,
  "batch_size": 500,
  "benchmark": "protocol_flow_commands",
  "client_cpu_percent": 23.879801933989462,
  "client_cpu_seconds": 0.45350699999999994,
  "completed": 50000,
  "connections": 1,
  "errors": 0,
  "flow_history_include_cold": false,
  "flow_read_consistency": "eventual",
  "flows": 50000,
  "inflight_batches": 64,
  "items_per_sec": 52508.067536322254,
  "operation": "flow-list",
  "partitions": 16,
  "payload_bytes": 16,
  "protocol_lanes": 64,
  "retention_ttl_ms": 0,
  "run_id": "protocol-flow-3c5eaa2b48",
  "seconds": 0.952234625001438,
  "setup_batch_size": 500,
  "setup_claim_seconds": 0.0,
  "setup_empty_claims": 0,
  "setup_seconds": 0.9443936250172555,
  "total_seconds": 1.8991237919544801,
  "url": "ferric://127.0.0.1:16388"
}


## 2026-06-11 - Owned FLOW.VALUE.PUT compact pipeline batch route

Context: after adding a homogeneous owned `FLOW.VALUE.PUT` pipeline batch route on the server. Unique owner-flow route keys coalesce into one per-shard Ra command; duplicate owner-flow route keys still fall back to the generic sequential path.

Server:

```bash
MIX_ENV=prod \
FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench-next/data \
FERRICSTORE_HEALTH_PORT=16380 \
FERRICSTORE_NATIVE_ENABLED=true \
FERRICSTORE_PORT=16379 \
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 \
FERRICSTORE_SHARD_COUNT=16 \
FERRICSTORE_NATIVE_PORT=16388 \
mix run --no-halt
```

Command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation value-put-owned \
  --flows 50000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 16 \
  --pretty
```

Sample 1:

```text
items_per_sec: 16,795/s
batch_latency_p50_ms: 1697.794
batch_latency_p95_ms: 1849.687
batch_latency_p99_ms: 1888.058
errors: 0
```

Sample 2:

```text
items_per_sec: 16,759/s
batch_latency_p50_ms: 1545.006
batch_latency_p95_ms: 2070.994
batch_latency_p99_ms: 2101.685
errors: 0
```

Read: modest improvement over prior ~15.6k/s / p99 ~2235ms. Remaining cost is per-item Flow state/value/history writes, not just protocol/Ra dispatch.

DBOS comparable guardrail:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

```text
created: 100000
completed: 100000
claimed_items: 100000
duplicate_completions: 0
create_flows_per_sec: 75,363/s
process_flows_per_sec: 70,961/s
end_to_end_flows_per_sec: 70,848/s
queue_latency_p99_ms: 75.010
process_empty_claims: 1
```

## 2026-06-11 - Owned FLOW.VALUE.PUT route optimization final retained shape

Context: retained only the server-side homogeneous owned `FLOW.VALUE.PUT` pipeline route. A deeper internal state/history batch apply attempt was tested and removed because it did not improve throughput. Duplicate owner-flow route keys still fall back to the generic sequential path.

Focused validation:

```bash
mix format --check-formatted \
  apps/ferricstore/lib/ferricstore/flow/pipeline_write.ex \
  apps/ferricstore/lib/ferricstore/store/router/part_08.ex \
  apps/ferricstore/lib/ferricstore/raft/state_machine/sections/apply_dispatch.ex \
  apps/ferricstore/lib/ferricstore/raft/state_machine/sections/async_apply.ex \
  apps/ferricstore/lib/ferricstore/raft/state_machine/sections/cross_shard_pending.ex \
  apps/ferricstore/test/ferricstore/flow/pipeline_write_test.exs \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs

mix test \
  apps/ferricstore/test/ferricstore/flow/pipeline_write_test.exs \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs \
  --trace
```

Result:

```text
format: pass
pipeline_write_test: 7 tests, 0 failures
native/commands_test: 48 tests, 0 failures
```

Clean server:

```bash
MIX_ENV=prod \
FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench-next/data \
FERRICSTORE_HEALTH_PORT=16380 \
FERRICSTORE_NATIVE_ENABLED=true \
FERRICSTORE_PORT=16379 \
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 \
FERRICSTORE_SHARD_COUNT=16 \
FERRICSTORE_NATIVE_PORT=16388 \
mix run --no-halt
```

Owned value-put command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation value-put-owned \
  --flows 50000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 16 \
  --pretty
```

Final clean sample:

```text
items_per_sec: 16,414/s
batch_latency_p50_ms: 1552.686
batch_latency_p95_ms: 2121.182
batch_latency_p99_ms: 2221.779
errors: 0
```

Read: modest throughput gain versus the original ~15.6k/s owned value-put baseline, but latency remains high because this command still writes owned value bytes, value link, state record, and history/projection per item.

DBOS guardrail command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Final guardrail:

```text
created: 100000
completed: 100000
claimed_items: 100000
duplicate_completions: 0
create_flows_per_sec: 90,461/s
process_flows_per_sec: 83,339/s
end_to_end_flows_per_sec: 83,167/s
queue_latency_p99_ms: 40.090
process_claim_calls: 265
process_empty_claims: 0
```

## 2026-06-11 - Native protocol guardrails and Flow read-index batching

Environment:
- Server repo: `/Users/yoavgea/repos/ferricstore`
- Python SDK repo: `/Users/yoavgea/repos/ferricstore-python`
- Source server, not Docker
- Data dir: `/tmp/ferricstore-protocol-bench-next` for initial guardrails, `/tmp/ferricstore-protocol-bench-list-opt` for post-patch list check
- Ports: RESP `16379`, native `16388`, health `16380`
- Shards: `FERRICSTORE_SHARD_COUNT=16`
- Native protocol: `ferric://127.0.0.1:16388`

### Native KV 30s guardrails

Command shape:
- `examples/protocol_kv_benchmark.py`
- `--threads 1 --processes 1 --clients 1 --pipeline 1000 --request-mode pipeline --inflight-batches 64 --protocol-lanes 64 --key-count 1000000 --value-bytes 32 --binary-keys --no-warmup`

Results:
- SET: `2,082,384/s`, batch p50 `30.076ms`, p95 `32.528ms`, p99 `44.151ms`, max `54.615ms`, errors `0`, client CPU `99.5%`
- GET: `1,759,916/s`, batch p50 `36.143ms`, p95 `36.944ms`, p99 `37.360ms`, max `39.517ms`, errors `0`, client CPU `102.8%`

Notes:
- SET is durable through the configured WARaft/Bitcask path.
- GET was run against the SET-populated key prefix `proto-refresh-set` to avoid miss-only measurement.

### Native Flow command matrix before direct read-index batching patch

Command shape:
- `examples/protocol_flow_commands_benchmark.py`
- `--flows 50000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 64 --partitions 16 --payload-bytes 16`

Results:
- `create-many`: `247,353/s`, p99 `132.671ms`, errors `0`
- `start-and-claim`: `221,848/s`, p99 `142.700ms`, errors `0`
- `transition-many`: `257,556/s`, p99 `125.521ms`, errors `0`
- `complete-many`: `201,469/s`, p99 `125.638ms`, errors `0`
- `retry-many`: `226,174/s`, p99 `139.583ms`, errors `0`
- `fail-many`: `222,426/s`, p99 `125.812ms`, errors `0`
- `cancel-many`: `255,289/s`, p99 `138.129ms`, errors `0`
- `claim-due`: `45,832/s`, p99 `20.700ms`, errors `0`
- `step`: `57,128/s`, p99 `450.985ms`, errors `0`
- `signal`: `226,836/s`, p99 `133.624ms`, errors `0`
- `value-put-owned`: `15,431/s`, p99 `2295.918ms`, errors `0`
- `flow-get`: `75,745/s`, p99 `400.148ms`, errors `0`
- `flow-history`: `246,467/s`, p99 `130.430ms`, errors `0`
- `flow-list`: `46,845/s`, p99 `782.538ms`, errors `0`

### Optimization retained

Changed direct WARaft Flow index reads so `flow_index_count_all_many/2` and `flow_index_rank_range_many/2` batch by shard instead of doing one direct native-index resource lookup per key/request.

Files:
- `/Users/yoavgea/repos/ferricstore/apps/ferricstore/lib/ferricstore/store/router/part_11.ex`

Correctness validation:
- `mix format --check-formatted apps/ferricstore/lib/ferricstore/store/router/part_11.ex`: pass
- `mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`: `48 tests, 0 failures`
- `mix test apps/ferricstore/test/ferricstore/flow_test.exs apps/ferricstore/test/ferricstore/flow/pipeline_history_read_test.exs --trace`: `226 tests, 0 failures`

Focused post-patch result:
- Operation: `flow-list`
- Command shape: same as matrix above
- Result: `115,070/s`, p50 `275.453ms`, p95 `316.226ms`, p99 `316.605ms`, max `316.697ms`, errors `0`

Interpretation:
- `FLOW.LIST` broad no-partition read improved from `46,845/s` to `115,070/s`.
- p99 improved from `782.538ms` to `316.605ms`.
- Cause: omitted partition means global auto-bucket list; batching direct index count/rank reads removes avoidable per-key native-resource lookup overhead in WARaft/direct mode.

## 2026-06-11 16:45 Asia/Jerusalem - clean source - standard GET/MGET compact wire cleanup

Change under test:

- Removed public `GET.COMPACT` / `MGET.COMPACT` protocol command names.
- Standard `GET` now returns the compact KV GET wire payload internally.
- Standard `MGET` now uses the compact KV MGET request/response wire payload internally.
- SDK benchmark uses `GET` / `MGET`; compact is implementation detail.

Server:

```bash
MIX_ENV=prod FERRICSTORE_NATIVE_ENABLED=true FERRICSTORE_SHARD_COUNT=16 FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data FERRICSTORE_PORT=16379 FERRICSTORE_HEALTH_PORT=16380 FERRICSTORE_NATIVE_PORT=16388 mix run --no-halt
```

SET command:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset set-throughput --binary-keys --value-bytes 32 --pretty
```

SET result:

```json
{
  "requests_per_sec": 2138811.5491775656,
  "requests": 64187500,
  "seconds": 30.010825415956788,
  "batch_latency_p50_ms": 14.715916,
  "batch_latency_p95_ms": 18.785833,
  "batch_latency_p99_ms": 24.063125,
  "errors": 0,
  "request_mode": "many",
  "pipeline": 500,
  "protocol_lanes": 64,
  "total_connections": 1,
  "prebuild_keys": true,
  "preset": "set-throughput"
}
```

GET command:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset get-throughput --binary-keys --value-bytes 32 --pretty
```

GET result:

```json
{
  "requests_per_sec": 3162454.459222327,
  "requests": 94891000,
  "seconds": 30.005491374991834,
  "batch_latency_p50_ms": 20.225083,
  "batch_latency_p95_ms": 21.173584,
  "batch_latency_p99_ms": 21.684208,
  "errors": 0,
  "request_mode": "many",
  "pipeline": 1000,
  "protocol_lanes": 64,
  "total_connections": 1,
  "prebuild_keys": true,
  "preset": "get-throughput",
  "warmed_keys": 100000
}
```

Notes:

- This is standard `GET` / `MGET` API naming with compact native wire internally.
- `GET.COMPACT` / `MGET.COMPACT` should not be used as public protocol commands.
- GET remains one Python process CPU-bound (`client_cpu_percent` ~104.5%).

## 2026-06-11 16:47 Asia/Jerusalem - clean source - native Flow command matrix after standard GET/MGET cleanup

Change context:

- Standard native `GET` / `MGET` now use compact KV wire internally.
- Public `GET.COMPACT` / `MGET.COMPACT` aliases removed from server and SDK.
- This run checks broader Flow command surface after that protocol cleanup.

Server:

```bash
MIX_ENV=prod FERRICSTORE_NATIVE_ENABLED=true FERRICSTORE_SHARD_COUNT=16 FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data FERRICSTORE_PORT=16379 FERRICSTORE_HEALTH_PORT=16380 FERRICSTORE_NATIVE_PORT=16388 mix run --no-halt
```

Command shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation OP \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 0
```

Raw matrix output:

```text
### create-many
{"batch_latency_avg_ms": 106.22697226179298, "batch_latency_max_ms": 129.39562497194856, "batch_latency_p50_ms": 108.9612920768559, "batch_latency_p95_ms": 129.36720892321318, "batch_latency_p99_ms": 129.37691702973098, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 16.781528505944358, "client_cpu_seconds": 0.06395, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 269500.31296362163, "operation": "create-many", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-61b4359326", "seconds": 0.37105708301533014, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 0.0, "total_seconds": 0.381073750089854, "url": "ferric://127.0.0.1:16388"}
### complete-many
{"batch_latency_avg_ms": 105.35002809891012, "batch_latency_max_ms": 196.4300830150023, "batch_latency_p50_ms": 101.65733296889812, "batch_latency_p95_ms": 192.42158299311996, "batch_latency_p99_ms": 195.55941701401025, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 6.226993518934052, "client_cpu_seconds": 0.317428, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 226060.71254624412, "operation": "complete-many", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-33d3cb71ba", "seconds": 0.4423590409569442, "setup_batch_size": 500, "setup_claim_seconds": 2.486219041980803, "setup_empty_claims": 0, "setup_seconds": 2.1666788749862462, "total_seconds": 5.0976125000743195, "url": "ferric://127.0.0.1:16388"}
### start-and-claim
{"batch_latency_avg_ms": 117.76236916950438, "batch_latency_max_ms": 144.86650004982948, "batch_latency_p50_ms": 136.85904210433364, "batch_latency_p95_ms": 142.88200007285923, "batch_latency_p99_ms": 144.29195900447667, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 96.05968160606305, "client_cpu_seconds": 0.43824500000000005, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 220377.6816107474, "operation": "start-and-claim", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-a3934f6e54", "seconds": 0.4537664579693228, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 0.0, "total_seconds": 0.45622158295009285, "url": "ferric://127.0.0.1:16388"}
### transition-many
{"batch_latency_avg_ms": 91.1124533682596, "batch_latency_max_ms": 117.57162492722273, "batch_latency_p50_ms": 98.28937496058643, "batch_latency_p95_ms": 114.99824991915375, "batch_latency_p99_ms": 116.64008407387882, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 8.428374305639457, "client_cpu_seconds": 0.21343199999999998, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 290544.13198378397, "operation": "transition-many", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-9fc17d37b7", "seconds": 0.344181791995652, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.185232416028157, "total_seconds": 2.5323032919550315, "url": "ferric://127.0.0.1:16388"}
### complete-many
{"batch_latency_avg_ms": 114.35027105384506, "batch_latency_max_ms": 183.9612500043586, "batch_latency_p50_ms": 118.43737494200468, "batch_latency_p95_ms": 166.06850002426654, "batch_latency_p99_ms": 183.0812080297619, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 6.144649835410694, "client_cpu_seconds": 0.315442, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 220393.18361755402, "operation": "complete-many", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-b6fc1f439c", "seconds": 0.4537345409626141, "setup_batch_size": 500, "setup_claim_seconds": 2.5058483330067247, "setup_empty_claims": 0, "setup_seconds": 2.171364750014618, "total_seconds": 5.133604167029262, "url": "ferric://127.0.0.1:16388"}
### retry-many
{"batch_latency_avg_ms": 120.62690421415027, "batch_latency_max_ms": 197.47754209674895, "batch_latency_p50_ms": 111.15954094566405, "batch_latency_p95_ms": 181.2172500649467, "batch_latency_p99_ms": 191.78345904219896, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 6.448130200490114, "client_cpu_seconds": 0.31875200000000004, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 223623.95129171255, "operation": "retry-many", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-5b372d47e3", "seconds": 0.4471792910480872, "setup_batch_size": 500, "setup_claim_seconds": 2.363714167033322, "setup_empty_claims": 0, "setup_seconds": 2.130023624980822, "total_seconds": 4.943324500112794, "url": "ferric://127.0.0.1:16388"}
### fail-many
{"batch_latency_avg_ms": 137.57876793155447, "batch_latency_max_ms": 227.14862506836653, "batch_latency_p50_ms": 123.11575002968311, "batch_latency_p95_ms": 216.9869999634102, "batch_latency_p99_ms": 226.37429204769433, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 4.726040409194936, "client_cpu_seconds": 0.315593, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 185764.1787534832, "operation": "fail-many", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-ac3c2c097d", "seconds": 0.5383169170236215, "setup_batch_size": 500, "setup_claim_seconds": 3.9253963340306655, "setup_empty_claims": 0, "setup_seconds": 2.2115053340094164, "total_seconds": 6.677746542030945, "url": "ferric://127.0.0.1:16388"}
### cancel-many
{"batch_latency_avg_ms": 91.24327459954657, "batch_latency_max_ms": 108.44420897774398, "batch_latency_p50_ms": 97.87466702982783, "batch_latency_p95_ms": 105.57391704060137, "batch_latency_p99_ms": 105.6964579038322, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 7.598071693471205, "client_cpu_seconds": 0.18955, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 299913.32470340544, "operation": "cancel-many", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-0a4f0baea0", "seconds": 0.33342966705095023, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.158376041916199, "total_seconds": 2.494711917010136, "url": "ferric://127.0.0.1:16388"}
### claim-due
{"batch_latency_avg_ms": 28.374066333661016, "batch_latency_max_ms": 190.15204207971692, "batch_latency_p50_ms": 10.77091705519706, "batch_latency_p95_ms": 82.57637498900294, "batch_latency_p99_ms": 112.9584169248119, "batch_latency_samples": 460, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 1.7687185423535816, "client_cpu_seconds": 0.269531, "completed": 99724, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 7640.34759203215, "operation": "claim-due", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-c1bf266e30", "seconds": 13.052285749930888, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.1839359999867156, "total_seconds": 15.238772792043164, "url": "ferric://127.0.0.1:16388"}
### step
{"batch_latency_avg_ms": 424.65947895892896, "batch_latency_max_ms": 509.49858396779746, "batch_latency_p50_ms": 438.06737498380244, "batch_latency_p95_ms": 462.84762490540743, "batch_latency_p99_ms": 498.60833398997784, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 99.893641709669, "client_cpu_seconds": 2.983466, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 61413.17235411761, "operation": "step", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-7b4b678802", "seconds": 1.6283151670359075, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 1.3558282499434426, "total_seconds": 2.986642541945912, "url": "ferric://127.0.0.1:16388"}
### signal
{"batch_latency_avg_ms": 115.72900249157101, "batch_latency_max_ms": 134.89779096562415, "batch_latency_p50_ms": 132.36837508156896, "batch_latency_p95_ms": 134.55195899587125, "batch_latency_p99_ms": 134.7679999889806, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 19.666203982972693, "client_cpu_seconds": 0.501994, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 218745.2461094791, "operation": "signal", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-77d224d785", "seconds": 0.45715279201976955, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.0928802919806913, "total_seconds": 2.5525719169527292, "url": "ferric://127.0.0.1:16388"}
### value-put-owned
{"batch_latency_avg_ms": 1833.7425083044218, "batch_latency_max_ms": 2323.8175000296906, "batch_latency_p50_ms": 2029.0976249380037, "batch_latency_p95_ms": 2299.497416941449, "batch_latency_p99_ms": 2319.470208021812, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 74.21311412718403, "client_cpu_seconds": 6.734641, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 15033.538698055616, "operation": "value-put-owned", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-cbe80b0906", "seconds": 6.651793833007105, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.4204407909419388, "total_seconds": 9.074731709086336, "url": "ferric://127.0.0.1:16388"}
### flow-get
{"batch_latency_avg_ms": 350.01427079259884, "batch_latency_max_ms": 428.05454204790294, "batch_latency_p50_ms": 402.28616597596556, "batch_latency_p95_ms": 423.64050005562603, "batch_latency_p99_ms": 427.69650008995086, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 41.60290503973345, "client_cpu_seconds": 1.455825, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 73088.85659393226, "operation": "flow-get", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-70c4a56863", "seconds": 1.3681976249208674, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.1285999580286443, "total_seconds": 3.4993349589640275, "url": "ferric://127.0.0.1:16388"}
### flow-history
{"batch_latency_avg_ms": 112.73680769954808, "batch_latency_max_ms": 137.68654107116163, "batch_latency_p50_ms": 132.35487509518862, "batch_latency_p95_ms": 136.69437495991588, "batch_latency_p99_ms": 137.3550419230014, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 19.933922920845358, "client_cpu_seconds": 0.502033, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 238243.5259517767, "operation": "flow-history", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-c6c3025ebf", "seconds": 0.41973858303390443, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.0962050000671297, "total_seconds": 2.518485708977096, "url": "ferric://127.0.0.1:16388"}
### flow-list
{"batch_latency_avg_ms": 1083.127216436551, "batch_latency_max_ms": 1688.8335419353098, "batch_latency_p50_ms": 1074.1463330341503, "batch_latency_p95_ms": 1687.7179580042139, "batch_latency_p99_ms": 1688.6127500329167, "batch_latency_samples": 200, "batch_size": 500, "benchmark": "protocol_flow_commands", "client_cpu_percent": 16.078428122461258, "client_cpu_seconds": 0.923969, "completed": 100000, "connections": 1, "errors": 0, "flow_history_include_cold": false, "flow_read_consistency": "eventual", "flows": 100000, "inflight_batches": 64, "items_per_sec": 27724.318299297076, "operation": "flow-list", "partitions": 16, "payload_bytes": 0, "protocol_lanes": 32, "retention_ttl_ms": 0, "run_id": "protocol-flow-e8bf320507", "seconds": 3.6069417080143467, "setup_batch_size": 500, "setup_claim_seconds": 0.0, "setup_empty_claims": 0, "setup_seconds": 2.137101374915801, "total_seconds": 5.7466376250376925, "url": "ferric://127.0.0.1:16388"}
```

Summary:

- `create-many`: ~269.5k/s, p99 batch ~129ms.
- `transition-many`: ~290.5k/s, p99 batch ~117ms.
- `cancel-many`: ~299.9k/s, p99 batch ~106ms.
- `complete-many`: ~220k-226k/s, p99 batch ~184-196ms.
- `retry-many`: ~223.6k/s, p99 batch ~192ms.
- `fail-many`: ~185.8k/s, p99 batch ~226ms.
- `signal`: ~218.7k/s, p99 batch ~135ms.
- `flow-history`: ~238.2k/s, p99 batch ~137ms.
- `step`: ~61.4k/s, p99 batch ~499ms.
- `flow-get`: ~73.1k/s, p99 batch ~428ms.
- `claim-due`: ~7.64k/s, p99 batch ~113ms, completed 99,724/100,000 in this micro shape.
- `value-put-owned`: ~15.0k/s, p99 batch ~2319ms.
- `flow-list`: ~27.7k/s, p99 batch ~1689ms.

Next optimization targets from this matrix:

1. `FLOW.LIST` direct index/read response construction.
2. `FLOW.VALUE.PUT` owned value write path and setup overhead.
3. `FLOW.CLAIM_DUE` microbenchmark shape and candidate/drain behavior.
4. `FLOW.GET` response construction/hydration cost.

## 2026-06-11 16:53 Asia/Jerusalem - clean source - FLOW.LIST keyed read coalescing

Change under test:

- `Ferricstore.Flow.PipelineRead` now supports keyed read coalescing for read-only `other` commands.
- `FLOW.LIST` / terminal / lineage query commands return keyed read functions from `PipelineReadCommand`.
- Identical query commands in one native/RESP pipeline execute the index read once and fan the result back to all request slots.
- Output order is preserved; unkeyed read functions remain independent.

Focused tests:

```bash
mix test apps/ferricstore/test/ferricstore/flow/pipeline_read_test.exs --trace
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace
mix compile --warnings-as-errors
```

Result:

```text
PipelineReadTest: 6 tests, 0 failures
Native.CommandsTest: 48 tests, 0 failures
mix compile --warnings-as-errors: pass
```

Benchmark command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-list \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 0 \
  --pretty
```

Before, same matrix shape:

```text
FLOW.LIST: ~27,724/s, p99 batch ~1688.6ms
```

After:

```json
{
  "items_per_sec": 119911.24165962751,
  "batch_latency_p50_ms": 240.9711660584435,
  "batch_latency_p95_ms": 333.8706250069663,
  "batch_latency_p99_ms": 366.0512500209734,
  "completed": 100000,
  "errors": 0,
  "operation": "flow-list",
  "request_shape": "100k commands, batch 500, count 50 list result per command"
}
```

Notes:

- This benchmark repeats identical `FLOW.LIST type STATE queued COUNT 50` commands, so coalescing removes repeated identical index scans/hydration in the same pipeline batch.
- Response encoding still emits one response per command, so this does not hide response cost.
- Next targets remain `FLOW.VALUE.PUT`, `FLOW.CLAIM_DUE`, and `FLOW.GET`.

## 2026-06-11 - FLOW.VALUE.PUT owned success-only return mode

Source server, native protocol, 16 shards, one native connection, 32 lanes, fresh temp data dir.

Change under test: owned `FLOW.VALUE.PUT` uses explicit `RETURN OK_ON_SUCCESS`, compact pipeline mode `14`, and server returns only OK on success while preserving default full-ref behavior for normal calls/errors.

Prior focused result before this optimization: ~15,034 items/s, p99 ~2319 ms, batch size 25.

Results:

| operation | flows | batch | inflight | items/s | p50 batch ms | p95 batch ms | p99 batch ms | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| value-put-owned | 100,000 | 25 | 64 | 95,235 | 15.47 | 25.32 | 40.28 | 0 |
| value-put-owned | 100,000 | 500 | 64 | 142,899 | 188.08 | 213.21 | 214.03 | 0 |

Takeaway: success-only return removes the response-map bottleneck. Batch 25 is better for latency; batch 500 is better for peak throughput.

## 2026-06-11 - Native Flow command matrix and setup-batch benchmark fix

Source server, native protocol, 16 shards, one native connection, 32 lanes, fresh temp data dir on ports 26379/26388.

Benchmark fix: read-style Flow commands now separate command batch size from setup/partition batch size. Previously `FLOW.GET`/`FLOW.HISTORY`/`FLOW.LIST` could create flows using tiny read batch sizes, or target wrong partitions when `--setup-batch-size` was overridden. Default setup batch is now at least 500 while preserving command batch size.

Representative results:

| operation | flows | command batch | setup batch | items/s | p50 batch ms | p95 batch ms | p99 batch ms | notes |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| create-many | 100,000 | 500 | 500 | 263,486 | 115.19 | 124.34 | 125.67 | durable create batch |
| claim-due | 100,000 | 500 | 500 | 40,386 | 11.02 | 17.65 | 22.33 | claim benchmark only |
| complete-many | 100,000 | 500 | 500 | 219,682 | 111.12 | 171.89 | 174.90 | setup claim took 12.43s |
| transition-many | 100,000 | 500 | 500 | 297,331 | 99.21 | 105.27 | 105.48 | success-only return |
| retry-many | 100,000 | 500 | 500 | 208,832 | 118.09 | 192.23 | 194.78 | success-only return |
| fail-many | 100,000 | 500 | 500 | 197,791 | 114.78 | 214.62 | 226.11 | success-only return |
| cancel-many | 100,000 | 500 | 500 | 306,962 | 93.01 | 103.86 | 104.08 | success-only return |
| value-put | 100,000 | 100 | 500 | 59,177 | 101.03 | 137.97 | 139.22 | shared refs returned |
| value-put-owned | 100,000 | 25 | 500 | 95,235 | 15.47 | 25.32 | 40.28 | RETURN OK_ON_SUCCESS |
| value-put-owned | 100,000 | 500 | 500 | 142,899 | 188.08 | 213.21 | 214.03 | throughput shape |
| value-mget | 100,000 | 500 | 500 | 2,064,314 | 15.00 | 15.38 | 15.64 | client CPU-bound |
| flow-get | 100,000 | 5 | 500 | 49,306 | 6.50 | 7.08 | 7.55 | setup fixed |
| flow-history | 100,000 | 5 | 500 | 102,443 | 3.03 | 3.29 | 3.47 | hot history only |
| flow-list | 100,000 | 50 | 500 | 5,400 | 592.44 | 765.41 | 860.31 | exact auto-partition list overfetches |
| flow-list | 100,000 | 500 | 500 | 25,214 | 1191.42 | 1808.25 | 1821.11 | larger page, high tail |
| signal | 100,000 | 500 | 500 | 226,063 | 130.63 | 131.24 | 131.40 | transition signal path |
| step | 100,000 | 10 | 500 | 39,533 | 15.03 | 25.19 | 28.96 | start setup excluded |
| start-and-claim | 100,000 | 10 | 500 | 42,311 | 13.29 | 24.77 | 33.51 | one-step claim UX |

Current outliers:

- `FLOW.LIST` without partition: exact global auto-partition listing scans/merges/hydrates across hidden buckets; benchmark repeats top pages, so it is useful as a stress test but not representative pagination.
- `FLOW.CLAIM_DUE`, `FLOW.STEP_CONTINUE`, and `FLOW.START_AND_CLAIM`: strategic latency paths around 40k/s on one native connection.
- Shared `FLOW.VALUE.PUT`: returns refs, so it cannot use the owned success-only shortcut.

## 2026-06-11 - FLOW.LIST auto-partition exact incremental scan

Source server, native protocol, 16 shards, one native connection, 32 lanes, fresh temp data dir on ports 26379/26388.

Change under test: `FLOW.LIST` without explicit partition no longer fetches `count` records from every populated auto bucket. It now does exact incremental k-way chunk reads and only fetches additional chunks from buckets whose last fetched member can still affect the global cutoff. This avoids write-path cost and preserves exact ordering.

Correctness checks:

- `mix compile --warnings-as-errors`
- `mix test apps/ferricstore/test/ferricstore/flow_test.exs --trace` -> 219 tests, 0 failures before final chunk-cap tuning.
- `mix test apps/ferricstore/test/ferricstore/flow_test.exs --trace --only line:496` -> 3 targeted auto-list tests, 0 failures after chunk-cap tuning.

Results:

| operation | flows | count/batch | setup batch | items/s | p50 batch ms | p95 batch ms | p99 batch ms | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| flow-list before | 100,000 | 50 | 500 | 5,400 | 592.44 | 765.41 | 860.31 | 0 |
| flow-list before | 100,000 | 500 | 500 | 25,214 | 1191.42 | 1808.25 | 1821.11 | 0 |
| flow-list incremental cap=8 | 100,000 | 50 | 500 | 70,155 | 46.50 | 70.22 | 84.18 | 0 |
| flow-list incremental cap=8 | 100,000 | 500 | 500 | 75,679 | 357.79 | 703.89 | 716.58 | 0 |
| flow-list incremental cap=64 | 100,000 | 50 | 500 | 68,087 | 48.83 | 67.89 | 74.04 | 0 |
| flow-list incremental cap=64 | 100,000 | 500 | 500 | 112,864 | 269.59 | 306.73 | 307.76 | 0 |

Takeaway: exact no-partition `FLOW.LIST` is no longer the pathological outlier. Count 50 improved ~12.6x throughput and ~11.6x p99; count 500 improved ~4.5x throughput and ~5.9x p99.

## 2026-06-11 - FLOW.CLAIM_DUE native multiplexed benchmark runner

Source server, native protocol, 16 shards, one native connection, 32 lanes, fresh temp data dir on ports 36379/36388.

Change under test: the benchmark `claim-due` runner now uses native `submit_command` with bounded in-flight claim capacity instead of issuing one blocking `FLOW.CLAIM_DUE` at a time. The old serial helper is retained for comparison/debugging. Server command semantics unchanged.

Correctness checks:

- `pytest tests/test_protocol_flow_commands_benchmark.py -q` -> 20 tests, 0 failures.

Results:

| operation | flows | batch/limit | inflight | items/s | p50 batch ms | p95 batch ms | p99 batch ms | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| claim-due serial | 100,000 | 500 | 1 effective | 42,401 | 10.57 | 17.22 | 23.14 | 0 |
| claim-due multiplexed | 100,000 | 500 | 64 | 375,834 | 76.67 | 103.12 | 104.18 | 0 |

Takeaway: native protocol lanes materially help claim drain throughput. The previous ~40k/s result was mostly benchmark serialization, not a server claim hot-path ceiling. The multiplexed p99 is queued batch latency under 64 in-flight claims, not per-claim service latency.

Follow-up: `examples/protocol_flow_commands_benchmark.py` now exposes `--claim-mode multiplexed|serial`. Default is `multiplexed`; `serial` is available for apples-to-apples blocking pull-loop comparisons.

## 2026-06-11 - STEP / START_AND_CLAIM benchmark tuning

Source server, native protocol, 16 shards, one native connection, 32 lanes, fresh temp data dir on ports 46379/46388.

Changes under test:

- `step` benchmark setup now requests `FLOW.START_AND_CLAIM RETURN JOBS_COMPACT`, then builds `FLOW.STEP_CONTINUE` from compact job tuples instead of full record maps.
- `step` and `start-and-claim` benchmark defaults changed from batch 10 to batch 50. This is a balanced native-protocol shape: materially higher throughput while keeping p99 under roughly 100 ms in local runs.

Correctness checks:

- `pytest tests/test_protocol_flow_commands_benchmark.py -q` -> 24 tests, 0 failures.

Batch sweep before compact job setup:

| operation | batch | items/s | p50 batch ms | p95 batch ms | p99 batch ms | notes |
|---|---:|---:|---:|---:|---:|---|
| step | 10 | 43,166 | 13.96 | 21.37 | 25.55 | full-record setup |
| step | 50 | 59,120 | 52.98 | 62.84 | 68.52 | full-record setup |
| step | 100 | 60,582 | 102.69 | 117.01 | 123.49 | full-record setup |
| start-and-claim | 10 | 53,265 | 10.48 | 24.17 | 30.46 | old default |
| start-and-claim | 50 | 68,040 | 40.00 | 84.83 | 114.23 | best sweep point |
| start-and-claim | 100 | 43,881 | 131.40 | 345.94 | 445.86 | too large locally |

After compact job setup and default batch 50:

| operation | batch | items/s | p50 batch ms | p95 batch ms | p99 batch ms | client CPU % |
|---|---:|---:|---:|---:|---:|---:|
| step | 50 | 66,018 | 47.77 | 65.48 | 75.06 | 86.7 |
| start-and-claim | 50 | 111,986 | 19.07 | 62.34 | 99.42 | 60.4 |

Takeaway: `STEP` is now mostly Python client CPU / command construction at this shape; server path is not the obvious first bottleneck. `START_AND_CLAIM` benefits strongly from the balanced batch default.

Follow-up STEP compact parser optimization:

- `FLOW.STEP_CONTINUE` compact pipeline encoding now uses a raw fast parser instead of the generic `build_protocol_command` path.
- Tests added to assert compact step encoding does not call the generic builder.

Checks:

- `pytest tests/test_protocol.py::test_protocol_compacts_batched_step_continue_as_pipeline_payload tests/test_protocol.py::test_protocol_compacts_batched_step_continue_without_generic_builder tests/test_protocol_flow_commands_benchmark.py -q` -> 26 tests, 0 failures.

Result after raw parser, default batch 50:

| operation | batch | items/s | p50 batch ms | p95 batch ms | p99 batch ms | client CPU % |
|---|---:|---:|---:|---:|---:|---:|
| step | 50 | 72,869 | 41.27 | 66.95 | 93.41 | 78.5 |

Takeaway: step throughput improved from ~43k/s old default to ~72.9k/s with compact jobs, balanced batch size, and raw compact step parsing.

## 2026-06-11 - Native protocol FLOW.VALUE.PUT shared raw compact parser

Change:
- Added raw compact parser for `FLOW.VALUE.PUT` pipeline payload construction.
- Avoids generic `build_protocol_command`/dict construction for compact shared value puts and owned OK-only value puts.
- Server behavior unchanged for shared value put; unsupported shapes fall back to the generic path.

Correctness:
- `pytest tests/test_protocol.py::test_protocol_compacts_batched_value_put_as_pipeline_payloads tests/test_protocol.py::test_protocol_compacts_batched_value_put_without_generic_builder tests/test_protocol_flow_commands_benchmark.py -q`
- Result: 26 passed.

Benchmark command:
```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:56388 \
  --operation value-put \
  --flows 1000000 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 0 \
  --pretty
```

Result:
```text
completed: 1,000,000
seconds: 5.583
throughput: 179,118 value puts/s
batch size: 100
batch p50: 37.29 ms
batch p95: 44.92 ms
batch p99: 51.88 ms
client CPU: 89.34%
errors: 0
```

Comparison:
```text
before raw compact parser: ~59,177/s, p99 ~139 ms, client CPU ~36.8%
after raw compact parser: 179,118/s, p99 ~51.9 ms, client CPU ~89.3%
```

Read:
- This confirms shared `FLOW.VALUE.PUT` was mostly Python protocol command-construction overhead, not server apply throughput.
- The optimized path uses much more client CPU and produces ~3.0x higher throughput with lower p99 batch latency.

## 2026-06-11 - Native protocol FLOW.VALUE.PUT shared success-only compact mode

Change:
- Added compact pipeline mode `15` for shared `FLOW.VALUE.PUT RETURN OK_ON_SUCCESS`.
- Shared ref-return remains mode `7`; owned/named success-only remains mode `14`.
- `Ferricstore.Flow.ValueStore` now honors `return: :ok_on_success` for shared value puts.
- Python protocol encoder now emits mode `15` for shared success-only value-put without using the generic command builder.

Correctness:
- `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
- Result: 88 tests, 0 failures.
- `pytest tests/test_protocol.py::test_protocol_compacts_batched_value_put_as_pipeline_payloads tests/test_protocol.py::test_protocol_compacts_batched_value_put_without_generic_builder tests/test_protocol_flow_commands_benchmark.py -q`
- Result: 27 passed.
- `mix compile --warnings-as-errors`
- Result: passed.
- `python -m compileall -q src/ferricstore examples/protocol_flow_commands_benchmark.py`
- Result: passed.

Benchmark server:
```text
source server
16 shards
1 native connection
32 protocol lanes
fresh data dir: /tmp/ferricstore-protocol-valueput-ok-bench/data
native port: 58388
```

Ref-return benchmark command:
```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:58388 \
  --operation value-put \
  --flows 1000000 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 0 \
  --pretty
```

Ref-return result:
```text
completed: 1,000,000
throughput: 181,028/s
batch size: 100
batch p50: 37.25 ms
batch p95: 45.46 ms
batch p99: 49.49 ms
client CPU: 90.05%
errors: 0
```

Success-only benchmark command:
```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:58388 \
  --operation value-put-ok \
  --flows 5000000 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 0 \
  --pretty
```

Success-only result:
```text
completed: 5,000,000
throughput: 796,785/s
batch size: 500
batch p50: 37.50 ms
batch p95: 47.80 ms
batch p99: 88.68 ms
client CPU: 94.47%
errors: 0
```

Latency shape, success-only batch 100:
```text
completed: 1,000,000
throughput: 281,564/s
batch p50: 22.15 ms
batch p95: 28.69 ms
batch p99: 30.97 ms
client CPU: 41.42%
errors: 0
```

Read:
- Shared value-put ref-return is stable after the change: ~181k/s vs previous ~179k/s.
- Success-only mode reaches SET-like throughput: ~797k/s sustained over 5M writes on one native socket.
- This mode is only for callers that do not need the random shared ref returned. For normal shared value refs, keep ref-return mode or add a future `REF_COMPACT` response type.

## 2026-06-11 - Native protocol KV 30-second gate after value-put compact changes

Server:
```text
source server
16 shards
1 native connection
64 protocol lanes from benchmark preset
fresh data dir: /tmp/ferricstore-protocol-kv-get-bench/data
native port: 59388
```

GET command:
```bash
python examples/protocol_kv_benchmark.py \
  --preset get-throughput \
  --url ferric://127.0.0.1:59388 \
  --pretty
```

GET result:
```text
requests: 84,877,000
seconds: 30.002
throughput: 2,829,010 get/s
batch size: 1000
request mode: many
p50 batch: 22.57 ms
p95 batch: 23.59 ms
p99 batch: 24.00 ms
client CPU: 103.26%
warmed keys: 100,000
errors: 0
```

SET command:
```bash
python examples/protocol_kv_benchmark.py \
  --preset set-throughput \
  --url ferric://127.0.0.1:59388 \
  --pretty
```

SET result:
```text
requests: 65,247,500
seconds: 30.011
throughput: 2,174,109 set/s
batch size: 500
request mode: many
p50 batch: 13.85 ms
p95 batch: 18.54 ms
p99 batch: 32.68 ms
client CPU: 55.84%
errors: 0
```

Read:
- The shared value-put compact change did not hurt native KV throughput.
- Native SET is already far above historical RESP memtier SET throughput on this machine (`~756k/s` with 800 RESP connections).
- Native GET is strong on one socket, but still below historical RESP memtier GET peak (`~5.1M/s` with 800 RESP connections); next KV target is likely client decode/load-generator overhead or native bulk GET response shape.

## 2026-06-11 - Native protocol FLOW.VALUE.PUT compact ref-return response

Change:
- Native compact pipeline responses now encode Flow value-ref maps as a dedicated compact payload tag.
- Existing `FLOW.VALUE.PUT` API behavior is unchanged: shared value puts still return refs by default.
- Python protocol decoder maps the compact value-ref payload back to a small dict with `ref`, optional `partition_key`, and optional `owner_flow_id` keys.
- This optimizes the default shared value-put path without requiring `RETURN OK_ON_SUCCESS`.

Correctness:
- `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
- Result: 89 tests, 0 failures.
- `pytest tests/test_protocol.py::test_protocol_fast_decodes_custom_pipeline_response_from_frame_body_offset tests/test_protocol.py::test_protocol_compacts_batched_value_put_as_pipeline_payloads tests/test_protocol.py::test_protocol_compacts_batched_value_put_without_generic_builder tests/test_protocol_flow_commands_benchmark.py -q`
- Result: 28 passed.
- `mix compile --warnings-as-errors`
- Result: passed.
- `python -m compileall -q src/ferricstore examples/protocol_flow_commands_benchmark.py`
- Result: passed.

Benchmark server:
```text
source server
16 shards
1 native connection
32 protocol lanes
fresh data dir: /tmp/ferricstore-protocol-valueput-refcompact-bench/data
native port: 60388
```

Benchmark command:
```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:60388 \
  --operation value-put \
  --flows 5000000 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 0 \
  --pretty
```

Result:
```text
completed: 5,000,000
throughput: 279,635/s
batch size: 100
batch p50: 22.38 ms
batch p95: 30.14 ms
batch p99: 35.36 ms
client CPU: 62.43%
errors: 0
```

Comparison:
```text
before compact ref response: ~181,028/s, p99 ~49.49 ms
after compact ref response: 279,635/s, p99 ~35.36 ms
```

Read:
- Default shared `FLOW.VALUE.PUT` ref-return is now ~1.5x faster without changing caller semantics.
- `FLOW.VALUE.PUT RETURN OK_ON_SUCCESS` remains the SET-like path for callers that do not need the random ref back (~797k/s in prior 5M run).

## 2026-06-11 - Native protocol GET client-process scaling check

Purpose:
- Determine whether native GET is server/protocol limited or one Python process decode/load-generator limited.
- Prior single-process native GET gate: ~2.83M/s with ~103% client CPU.

Server:
```text
source server
16 shards
fresh data dir: /tmp/ferricstore-protocol-kv-get-scale-bench/data
native port: 61388
```

Command:
```bash
python examples/protocol_kv_benchmark.py \
  --preset get-throughput \
  --url ferric://127.0.0.1:61388 \
  --processes 2 \
  --pretty
```

Result:
```text
requests: 173,906,000
seconds: 30.088
throughput: 5,779,961 get/s
processes: 2
total native connections: 2
batch size: 1000
request mode: many
p50 batch: 22.06 ms
p95 batch: 23.84 ms
p99 batch: 24.95 ms
client CPU: 203.59%
warmed keys: 100,000
errors: 0
```

Read:
- Native GET scales from ~2.83M/s with one Python process to ~5.78M/s with two Python processes.
- This exceeds the historical RESP memtier GET baseline on this machine (~5.10M/s with 800 RESP connections).
- Remaining single-process GET gap is Python decode/load-generator CPU, not a clear server protocol bottleneck.

## 2026-06-11 - Native protocol partitioned FLOW.GET compact pipeline mode

Change:
- Added compact pipeline mode 16 for `FLOW.GET` with optional `partition_key`.
- Kept existing mode 9 for bare-id `FLOW.GET` for wire compatibility.
- Python protocol now emits mode 16 when a batched `FLOW.GET` includes `PARTITION`.
- Server codec/commands decode, validate, and route mode 16 through the same Flow read pipeline fast path as mode 9.

Correctness:
- `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
- Result: 91 tests, 0 failures.
- `PYTHONPATH=src pytest tests/test_protocol.py::test_protocol_execute_batch_requests_compact_pipeline_responses tests/test_protocol.py::test_protocol_execute_batch_compacts_partitioned_flow_get tests/test_protocol.py::test_protocol_compacts_batched_flow_get_as_pipeline_payload tests/test_protocol.py::test_protocol_compacts_partitioned_batched_flow_get_as_pipeline_payload tests/test_protocol_flow_commands_benchmark.py -q`
- Result: 29 passed.

Server:
```text
source server
16 shards
1 native connection
32 protocol lanes
fresh data dir: /tmp/ferricstore-protocol-flow-read-mode16-bench/data
native port: 62388
```

Before, partitioned `FLOW.GET` could not use compact mode 9 because mode 9 only encoded bare IDs. It fell back to the generic pipeline shape.

Before samples:
```text
batch 5:   52,654/s, p99 7.92 ms
batch 50:  77,126/s, p99 47.41 ms
batch 100: 78,313/s, p99 86.51 ms
```

After samples:
```text
batch 5:   64,408/s, p99 5.89 ms, client CPU 49.28%, errors 0
batch 50:  92,043/s, p99 38.31 ms, client CPU 38.78%, errors 0
batch 100: 93,225/s, p99 89.50 ms, client CPU 37.97%, errors 0
```

History guard sample after the change:
```text
FLOW.HISTORY batch 100: 230,928/s, p99 29.99 ms, client CPU 21.16%, errors 0
```

Read:
- Partitioned `FLOW.GET` improved by ~19-22% depending on batch shape.
- Batch 50 is currently the best balanced read shape: ~92k/s with lower p99 than batch 100.
- `FLOW.HISTORY` stayed stable, so the mode 16 change is isolated to `FLOW.GET`.

## 2026-06-11 - Native protocol FLOW.VALUE.MGET sustained read benchmark

Change:
- Added benchmark-only `--read-duration` support for `protocol_flow_commands_benchmark.py`.
- For `value-mget`, the benchmark can now prepare a bounded ref pool once and repeatedly read it for a measured duration.
- This avoids creating tens of millions of value refs just to obtain a 30 second read sample.

Correctness:
- `. .venv/bin/activate && PYTHONPATH=src pytest tests/test_protocol_flow_commands_benchmark.py -q`
- Result: 26 passed.
- `. .venv/bin/activate && python -m compileall -q src/ferricstore examples/protocol_flow_commands_benchmark.py`
- Result: passed.

Server:
```text
source server
16 shards
1 native connection
32 protocol lanes
fresh data dir: /tmp/ferricstore-protocol-flow-sweep/data
native port: 63388
```

FLOW.VALUE.MGET command:
```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:63388 \
  --operation value-mget \
  --flows 100000 \
  --read-duration 30 \
  --batch-size 100 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 16 \
  --pretty
```

FLOW.VALUE.MGET result:
```text
completed: 48,246,200
seconds: 30.0009
throughput: 1,608,161/s
batch size: 100
p50 batch: 3.96 ms
p95 batch: 4.30 ms
p99 batch: 4.44 ms
max batch: 5.79 ms
client CPU: 111.13%
setup refs: 100,000
setup seconds: 0.405
errors: 0
```

Comparable native GET command, same one-connection / pipeline-100 shape:
```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:63388 \
  --command get \
  --test-time 30 \
  --clients 1 \
  --threads 1 \
  --processes 1 \
  --pipeline 100 \
  --request-mode many \
  --inflight-batches 64 \
  --protocol-lanes 32 \
  --key-count 100000 \
  --value-bytes 16 \
  --prebuild-keys \
  --pretty
```

Native GET comparable result:
```text
completed: 53,541,200
seconds: 30.0047
throughput: 1,784,428/s
batch size: 100
p50 batch: 3.59 ms
p95 batch: 4.00 ms
p99 batch: 4.26 ms
max batch: 5.65 ms
client CPU: 111.82%
errors: 0
```

Read:
- `FLOW.VALUE.MGET` is in the same latency area as native `GET` for the same pipeline shape.
- Throughput is ~90% of comparable native `GET` while doing value-ref command parsing and ref resolution.
- Both samples are client/loadgen CPU-bound at ~111% CPU, so server-side optimization is not the next obvious cut for value reads.
- This supports the intended model: `FLOW.GET` is metadata/state, while `FLOW.VALUE.MGET` is the GET-like value path.

## 2026-06-11 - Native protocol FLOW.GET meta response and FLOW.VALUE.MGET guard

Change:
- Added compact pipeline mode 17 for `FLOW.GET ... RETURN META`.
- SDK raw/native pipeline batching now compacts `FLOW.GET` with `RETURN META` into mode 17.
- Benchmark tool added `--operation flow-get-meta`.
- `FLOW.VALUE.MGET` was re-run first because value-ref reads are the more important hot path.

Correctness:
- `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
- Result: 93 tests, 0 failures.
- `mix compile --warnings-as-errors`
- Result: passed.
- `PYTHONPATH=src pytest tests/test_protocol.py::test_protocol_compacts_batched_flow_get_meta_as_pipeline_payload tests/test_protocol.py::test_protocol_execute_batch_compacts_flow_get_meta tests/test_protocol_flow_commands_benchmark.py::test_protocol_flow_read_queries_use_latency_tuned_default_batch_size tests/test_protocol_flow_commands_benchmark.py::test_protocol_flow_get_meta_command_requests_meta_return -q`
- Result: 4 passed.
- `PYTHONPATH=src pytest tests/test_protocol_flow_commands_benchmark.py -q`
- Result: 27 passed.

Server:
```text
source server
MIX_ENV=prod
FERRICSTORE_PROTECTED_MODE=false
FERRICSTORE_SHARD_COUNT=16
FERRICSTORE_NATIVE_ENABLED=true
RESP port: 63379
native port: 63388
fresh data dir: /tmp/ferricstore-protocol-flow-sweep/data
```

Priority guard: FLOW.VALUE.MGET, 30 second sustained read:
```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:63388 \
  --operation value-mget \
  --flows 100000 \
  --read-duration 30 \
  --batch-size 100 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 16 \
  --pretty
```

Result:
```text
completed: 47,811,800
seconds: 30.0008
throughput: 1,593,687/s
batch size: 100
p50 batch: 4.00 ms
p95 batch: 4.32 ms
p99 batch: 4.65 ms
max batch: 6.91 ms
client CPU: 112.03%
setup refs: 100,000
setup seconds: 0.473s
errors: 0
```

FLOW.GET normal, batch 50:
```text
completed: 100,000
throughput: 90,643/s
p50 batch: 35.38 ms
p95 batch: 40.30 ms
p99 batch: 44.41 ms
max batch: 48.55 ms
client CPU: 38.39%
errors: 0
```

FLOW.GET RETURN META, batch 50:
```text
completed: 100,000
throughput: 113,526/s
p50 batch: 28.17 ms
p95 batch: 30.28 ms
p99 batch: 31.75 ms
max batch: 33.65 ms
client CPU: 34.22%
errors: 0
```

Read:
- `FLOW.VALUE.MGET` remains the primary high-throughput value path and stayed in the previous performance band.
- `FLOW.GET RETURN META` improves metadata reads by ~25% throughput and lowers p99 by ~29% versus normal `FLOW.GET` for the same batch-50 native shape.
- This is still not a replacement for `FLOW.VALUE.MGET`; it is a lean state/metadata response for code that does not need values.

## 2026-06-11 - Rejected FLOW.VALUE.MGET batch decode micro-optimization

Attempt:
- Replaced `Enum.zip(refs, values) |> Enum.map(...)` in `Flow.ValueStore.value_mget/3` with a batch value decoder and single recursive capped path.
- Goal was to reduce allocation and per-value decode dispatch.

Correctness while testing the attempt:
- `mix test apps/ferricstore/test/ferricstore/flow_codec_test.exs apps/ferricstore/test/ferricstore/flow_value_payload_test.exs apps/ferricstore/test/ferricstore/flow_named_values_test.exs --trace`
- Result: 34 tests, 0 failures.
- `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
- Result: 93 tests, 0 failures.
- `mix compile --warnings-as-errors`
- Result: passed.

Benchmark shape:
```text
source server
16 shards
1 native connection
32 protocol lanes
FLOW.VALUE.MGET
100k ref pool
read-duration 30s
batch size 100
inflight batches 64
payload 16 bytes
```

Baseline before attempt from same local source shape:
```text
throughput: 1,593,687/s
p50 batch: 4.00 ms
p95 batch: 4.32 ms
p99 batch: 4.65 ms
client CPU: 112.03%
```

Attempt samples:
```text
sample 1: 1,539,837/s, p50 4.15 ms, p95 4.38 ms, p99 4.51 ms, client CPU 110.87%
sample 2: 1,541,548/s, p50 4.15 ms, p95 4.37 ms, p99 4.50 ms, client CPU 110.95%
```

Decision:
- Rejected and reverted.
- Throughput dropped ~3.3% despite slightly lower p99.
- Keep current `FLOW.VALUE.MGET` implementation for now; it is already close to native `GET` under the same one-connection shape.

Post-revert validation:
- `mix test apps/ferricstore/test/ferricstore/flow_codec_test.exs apps/ferricstore/test/ferricstore/flow_value_payload_test.exs apps/ferricstore/test/ferricstore/flow_named_values_test.exs --trace`
- Result: 33 tests, 0 failures.
- `mix compile --warnings-as-errors`
- Result: passed.

## 2026-06-11 - Rejected FLOW.VALUE.MGET missing-scan allocation optimization

Priority: `FLOW.VALUE.MGET` value/data plane remains more important than `FLOW.GET` metadata reads.

Attempt:
- Changed `Flow.ValueStore.flow_value_fill_lmdb_missing/3` to scan refs/values recursively and return the original hot value list when no generated missing refs were found.
- Goal was to avoid `Enum.zip -> Enum.with_index -> Enum.flat_map` allocation on the common hot-present MGET path.
- Added behavior coverage for mixed hot + ordinary missing refs.

Correctness:
```text
mix test apps/ferricstore/test/ferricstore/flow_value_payload_test.exs --trace
14 tests, 0 failures
mix compile --warnings-as-errors
passed
```

Benchmark shape:
```text
source server
16 shards
1 native connection
32 protocol lanes
FLOW.VALUE.MGET
100k ref pool
read-duration 30s
batch size 100
inflight batches 64
payload 16 bytes
```

Previous accepted baseline band:
```text
1,593,687/s, p50 4.00 ms, p95 4.32 ms, p99 4.65 ms
```

Attempt samples:
```text
sample 1: 1,526,884/s, p50 4.18 ms, p95 4.45 ms, p99 4.59 ms
sample 2: 1,533,313/s, p50 4.16 ms, p95 4.41 ms, p99 4.55 ms
```

Decision:
- Rejected and reverted implementation.
- Throughput dropped ~3.8-4.2% with no meaningful p99 improvement.
- Keep the behavior test; it has no hot-path cost.

Post-revert confirm sample:
```text
1,581,857/s, p50 4.02 ms, p95 4.36 ms, p99 4.59 ms
```

Conclusion:
- `FLOW.VALUE.MGET` is not helped by this Elixir missing-scan change.
- Next useful optimizations should target native compact response encode/decode or protocol client batching/CPU, not the missing-ref scan.

## 2026-06-11 - Native direct SET/MSET compact OK response

Change:
- Server direct compact response path now encodes `SET` (`0x0102`) and `MSET` (`0x0105`) OK replies as compact OK-list count=1 when compact response negotiation is active.
- Python native protocol decoder maps direct compact OK-list count=1 back to scalar `b"OK"` for `SET/MSET` so user-visible return shape stays unchanged.
- No new command shape; existing native SET/MSET path only.

Correctness:
```text
mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs --trace
41 tests, 0 failures
mix compile --warnings-as-errors
passed
PYTHONPATH=src pytest tests/test_protocol.py tests/test_protocol_kv_benchmark.py -q
95 passed
python -m compileall -q src/ferricstore examples/protocol_kv_benchmark.py
passed
```

Benchmark server:
```text
source server
16 shards
native port 63388
fresh data dir /tmp/ferricstore-protocol-kv-compact-ok/data
```

SET, 30s, one native connection:
```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:63388 \
  --command set \
  --test-time 30 \
  --clients 1 \
  --threads 1 \
  --processes 1 \
  --pipeline 500 \
  --request-mode many \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 100000 \
  --value-bytes 16 \
  --prebuild-keys \
  --pretty
```

Result:
```text
requests: 67,897,500
throughput: 2,262,386/s
p50 batch: 13.63 ms
p95 batch: 18.28 ms
p99 batch: 24.19 ms
max batch: 45.56 ms
client CPU: 56.15%
errors: 0
```

GET regression check, 30s, one native connection:
```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:63388 \
  --command get \
  --test-time 30 \
  --clients 1 \
  --threads 1 \
  --processes 1 \
  --pipeline 1000 \
  --request-mode many \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 100000 \
  --value-bytes 16 \
  --prebuild-keys \
  --pretty
```

Result:
```text
requests: 83,833,000
throughput: 2,794,125/s
p50 batch: 22.84 ms
p95 batch: 24.06 ms
p99 batch: 24.61 ms
max batch: 26.22 ms
client CPU: 103.90%
errors: 0
```

Read:
- SET improved versus the earlier one-connection native sample around ~2.17M/s.
- GET stayed in the same band as the earlier one-connection native sample around ~2.83M/s.
- This change is worth keeping unless later multi-sample runs contradict it.

## 2026-06-11 - Rejected FLOW.GET RETURN META atom-key compact record field lookup

Attempt:
- Added an atom-key field id lookup table in native protocol Flow compact record encoding.
- Goal was to avoid `Atom.to_string/1` for known Flow record atom keys during `FLOW.GET RETURN META` compact response encoding.

Benchmark shape:
```text
source server
16 shards
1 native connection
64 protocol lanes
FLOW.GET RETURN META
100k flows
setup batch size 500
payload 16 bytes
```

Pre-change samples from same session shape:
```text
batch 50: 116,687/s, p50 27.39 ms, p95 29.64 ms, p99 30.71 ms
batch 100: 119,897/s, p50 53.39 ms, p95 60.46 ms, p99 63.84 ms
```

Attempt samples:
```text
batch 50: 114,333/s, p50 27.97 ms, p95 29.56 ms, p99 31.25 ms
batch 100: 120,938/s, p50 52.90 ms, p95 57.57 ms, p99 59.58 ms
```

Decision:
- Rejected and reverted.
- Batch 50 regressed and batch 100 gain was too small/noisy to justify extra code.
- `FLOW.GET RETURN META` bottleneck is likely not atom key conversion in compact response encoding.

Related observation:
- Native protocol currently does not expose data-structure list/hash/set/zset families as first-class commands; they remain older protocol coverage today.

### Flow value refs vs Flow metadata read, current-server sample

Date: 2026-06-11

Server:

```text
source server already running on ferric://127.0.0.1:16388
not restarted for clean baseline before this sample
```

Commands:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation value-mget \
  --flows 100000 \
  --batch-size 100 \
  --setup-batch-size 100 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 64 \
  --read-duration 30 \
  --pretty
```

Result:

```text
FLOW.VALUE.MGET: 47,647,600 values in 30.0008s
throughput: 1,588,213 values/s
batch latency: p50 4.02 ms, p95 4.24 ms, p99 4.49 ms, max 5.17 ms
errors: 0
client CPU: 112%
```

Command:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-get-meta \
  --flows 100000 \
  --batch-size 100 \
  --setup-batch-size 100 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 64 \
  --read-duration 30 \
  --pretty
```

Result:

```text
FLOW.GET RETURN META: 100,000 reads in 0.8814s measured
throughput: 113,453 reads/s
batch latency: p50 55.72 ms, p95 69.63 ms, p99 78.80 ms, max 80.75 ms
errors: 0
client CPU: 19.1%
```

Takeaway:

```text
FLOW.VALUE.MGET is the primary hot value-read path. It reads value refs directly and is ~14x faster than FLOW.GET META in this sample.
FLOW.GET META is still important for record metadata/debug/control paths, but it should not be used as the bulk value-fetch path.
```

### Native protocol hash/list/set/zset first samples

Date: 2026-06-11

Server:

```text
source server restarted from current code
native enabled on ferric://127.0.0.1:16388
16 shards
FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data
```

Benchmark shape:

```text
examples/protocol_kv_benchmark.py
--request-mode pipeline
--pipeline 100
--inflight-batches 64
--protocol-lanes 64
--clients 1 --threads 1 --processes 1
--binary-keys
```

Data-structure write sample:

```text
HSET, 30s, key_count=100000, no warmup:
requests: 83,300
throughput: 2,567/s
batch latency: p50 2459.5 ms, p95 3000.9 ms, p99 3096.6 ms
errors: 0
```

Conclusion:

```text
Native HSET is functionally correct but performance-bad. It falls through generic per-command execution and does not get a Raft/write batch fast path like SET/MSET.
```

Data-structure read samples with key_count=1000 warmup:

```text
HGET:
throughput: 208,882/s
batch latency: p50 30.57 ms, p95 31.51 ms, p99 35.06 ms
errors: 0
client CPU: ~102%

SISMEMBER:
throughput: 175,927/s
batch latency: p50 36.18 ms, p95 37.52 ms, p99 41.66 ms
errors: 0
client CPU: ~102%

LRANGE, one-element lists:
throughput: 9,810/s
batch latency: p50 681.64 ms, p95 754.63 ms, p99 823.00 ms
errors: 0
client CPU: ~10%

ZRANGE, one-member zsets:
throughput: 3,468/s
batch latency: p50 1899.58 ms, p95 2116.09 ms, p99 2249.49 ms
errors: 0
client CPU: ~4%
```

Conclusion:

```text
Hash/set point reads are usable but still far below GET/MGET native fast paths.
List/sorted-set range reads are server-side bottlenecked and need dedicated read fast paths if they are expected to be hot.
```

### Native protocol hash/list/set/zset after compact point-read modes

Date: 2026-06-11

Server:

```text
source server restarted from current code
native enabled on ferric://127.0.0.1:16388
16 shards
FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data
```

Benchmark shape:

```text
examples/protocol_kv_benchmark.py
--request-mode pipeline
--pipeline 100
--inflight-batches 64
--protocol-lanes 64
--clients 1 --threads 1 --processes 1
--binary-keys
30 second timed runs
```

Point reads after compact pipeline modes:

```text
HGET, key_count=1000 warmup:
throughput: 802,437/s
batch latency: p50 7.94 ms, p95 8.85 ms, p99 9.52 ms
errors: 0
client CPU: ~105.5%

SISMEMBER, key_count=1000 warmup:
throughput: 499,948/s
batch latency: p50 12.75 ms, p95 14.04 ms, p99 14.77 ms
errors: 0
client CPU: ~103.7%
```

Range reads, still generic response path:

```text
LRANGE, key_count=1000 warmup, one-element lists:
throughput: 8,250/s
batch latency: p50 806.86 ms, p95 952.47 ms, p99 1059.42 ms
errors: 0
client CPU: ~9.7%

ZRANGE, key_count=1000 warmup, one-member zsets:
throughput: 3,211/s
batch latency: p50 2069.97 ms, p95 2368.41 ms, p99 2425.64 ms
errors: 0
client CPU: ~3.3%
```

Compound writes, still generic mutation path:

```text
HSET, key_count=100000:
throughput: 2,197/s
batch latency: p50 2853.47 ms, p95 3274.34 ms, p99 3328.82 ms
errors: 0
client CPU: ~1.8%

LPUSH, key_count=100000:
throughput: 3,238/s
batch latency: p50 1870.04 ms, p95 2339.30 ms, p99 2400.48 ms
errors: 0
client CPU: ~2.3%

SADD, key_count=100000:
throughput: 587/s
batch latency: p50 10891.44 ms, p95 11126.64 ms, p99 11266.75 ms
errors: 0
client CPU: ~0.5%

ZADD, key_count=100000:
throughput: 538/s
batch latency: p50 11846.41 ms, p95 12019.98 ms, p99 12065.06 ms
errors: 0
client CPU: ~0.5%
```

Takeaway:

```text
Compact point-read modes are worth keeping: HGET improved from ~209k/s to ~802k/s, and SISMEMBER improved from ~176k/s to ~500k/s.
LRANGE/ZRANGE and compound writes are not wire/client bound. They need server-side data-structure read/write batch paths or store-path optimization.
```

### Native protocol LRANGE/ZRANGE after compact range request modes

Date: 2026-06-11

Change:

```text
Added compact PIPELINE modes for normal LRANGE and ZRANGE commands.
The SDK still exposes standard LRANGE/ZRANGE; no benchmark-only command name is required.
Responses use the existing compact pipeline wrapper and fall back to typed values for list results.
```

Benchmark shape:

```text
examples/protocol_kv_benchmark.py
--request-mode pipeline
--pipeline 100
--inflight-batches 64
--protocol-lanes 64
--clients 1 --threads 1 --processes 1
--binary-keys
30 second timed runs
```

Results:

```text
LRANGE, key_count=1000 warmup, one-element lists:
throughput: 257,111/s
batch latency: p50 25.14 ms, p95 31.17 ms, p99 37.90 ms
errors: 0
client CPU: ~90.7%

ZRANGE, key_count=1000 warmup, one-member zsets:
throughput: 119,099/s
batch latency: p50 55.96 ms, p95 63.41 ms, p99 66.47 ms
errors: 0
client CPU: ~57.5%
```

Takeaway:

```text
Range-read slowness was mostly native pipeline command/result overhead, not the underlying list/zset read operation.
LRANGE improved from ~8.25k/s to ~257k/s.
ZRANGE improved from ~3.21k/s to ~119k/s.
Compound writes remain slow because each pipelined write still executes as an independent mutation.
```

### Rejected experiment: compact single-item write modes for HSET/LPUSH/SADD/ZADD

Date: 2026-06-11

Experiment:

```text
Tried compact PIPELINE request modes for single-item HSET, LPUSH, SADD, and ZADD.
The modes removed typed command-map construction, but still executed each mutation independently.
```

Measured results before reverting:

```text
HSET: 2,445/s, p99 batch 3771 ms  (prior generic sample: 2,197/s)
LPUSH: 2,929/s, p99 batch 2540 ms (prior generic sample: 3,238/s)
SADD: 464/s, p99 batch 14917 ms   (prior generic sample: 587/s)
ZADD: 385/s, p99 batch 17201 ms   (prior generic sample: 538/s)
```

Decision:

```text
Rejected and removed.
Wire/request compaction alone does not solve compound write slowness and can make set/zset writes worse.
The next write optimization must group mutations into real shard/apply batches while preserving data-structure command semantics.
```

### Final kept DS read compact modes after write-mode revert

Date: 2026-06-11

Final kept code:

```text
Kept compact PIPELINE modes for HGET, SISMEMBER, LRANGE, ZRANGE.
Removed compact single-item write modes for HSET, LPUSH, SADD, ZADD after benchmarks showed no benefit/regression.
```

Final source-server samples:

```text
LRANGE, key_count=1000 warmup, one-element lists:
throughput: 284,478/s
batch latency: p50 22.44 ms, p95 28.18 ms, p99 33.30 ms
errors: 0
client CPU: ~95.9%

ZRANGE, key_count=1000 warmup, one-member zsets:
throughput: 137,453/s
batch latency: p50 47.80 ms, p95 53.16 ms, p99 59.95 ms
errors: 0
client CPU: ~60.3%
```

Current DS protocol status:

```text
HGET: ~802k/s after compact mode
SISMEMBER: ~500k/s after compact mode
LRANGE: ~284k/s final kept compact mode
ZRANGE: ~137k/s final kept compact mode
HSET/LPUSH/SADD/ZADD: still slow; needs true compound mutation batching, not wire-only compaction
```

## Native protocol HSET shard-batch fast path

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`.

Command:

```bash
PYTHONPATH=src python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --command hset \
  --test-time 30 \
  --request-mode pipeline \
  --pipeline 100 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --clients 1 --threads 1 --processes 1 \
  --key-prefix proto-ds-hset-batch \
  --key-count 100000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

Result after semantic `:hset_single` Ra/apply batch path:

```text
HSET: 146,030/s
batch latency p50: 43.799 ms
batch latency p95: 47.087 ms
batch latency p99: 50.229 ms
errors: 0
client CPU: 100.97%
```

Previous generic native HSET write path was about `2.2k/s`. Smaller compact write frames alone were rejected earlier because they did not remove the per-command Ra/apply cost.

## Native protocol LPUSH shard-batch fast path

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`.

Command:

```bash
PYTHONPATH=src python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --command lpush \
  --test-time 30 \
  --request-mode pipeline \
  --pipeline 100 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --clients 1 --threads 1 --processes 1 \
  --key-prefix proto-ds-lpush-batch \
  --key-count 100000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

Result after semantic `:lpush_single` Ra/apply batch path:

```text
LPUSH: 147,845/s
batch latency p50: 43.281 ms
batch latency p95: 46.746 ms
batch latency p99: 49.997 ms
errors: 0
client CPU: 100.94%
```

Previous generic native LPUSH write path was about `3.2k/s`.

## Native protocol SADD/ZADD shard-batch fast paths

Source server from current FerricStore worktree, clean startup, 16 shards, native port `16388`.

Commands use the same shape as HSET/LPUSH:

```bash
PYTHONPATH=src python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --command sadd|zadd \
  --test-time 30 \
  --request-mode pipeline \
  --pipeline 100 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --clients 1 --threads 1 --processes 1 \
  --key-count 100000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

Sequential clean results after semantic Ra/apply batch paths:

```text
SADD: 144,840/s
batch latency p50: 44.208 ms
batch latency p95: 46.552 ms
batch latency p99: 48.314 ms
errors: 0
client CPU: 101.53%

ZADD: 124,731/s
batch latency p50: 51.224 ms
batch latency p95: 53.848 ms
batch latency p99: 54.988 ms
errors: 0
client CPU: 101.59%
```

Concurrent stress run of SADD and ZADD together on the same server showed `SADD ~146.8k/s` and `ZADD ~128.9k/s`, but the sequential numbers above are the apples-to-apples command baselines.

Previous generic native write paths were about `SADD ~587/s` and `ZADD ~538/s`.

## Native protocol Flow read duration benchmarks

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`.

Common shape:

```bash
PYTHONPATH=src python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-get|flow-get-meta|value-mget \
  --flows 100000 \
  --batch-size 100 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 16 \
  --read-duration 30 \
  --pretty
```

Results after adding true duration mode for Flow pipeline reads and compact values-only pipeline Flow record-list responses:

```text
FLOW.GET: 90,117/s
batch latency p50: 70.810 ms
batch latency p95: 88.593 ms
batch latency p99: 100.480 ms
errors: 0
client CPU: 94.32%

FLOW.GET RETURN META: 117,331/s
batch latency p50: 54.457 ms
batch latency p95: 60.210 ms
batch latency p99: 63.550 ms
errors: 0
client CPU: 94.51%

FLOW.VALUE.MGET: 1,528,097 refs/s
batch latency p50: 4.176 ms
batch latency p95: 4.430 ms
batch latency p99: 4.615 ms
errors: 0
client CPU: 110.71%
```

Notes:

- Before compact values-only Flow record-list response, `FLOW.GET` was ~89,931/s and `FLOW.GET RETURN META` was ~113,559/s.
- The response wrapper removal helped `FLOW.GET RETURN META` by about 3%, but full `FLOW.GET` stayed flat. Full `FLOW.GET` bottleneck is likely Flow record hydration/record field decode rather than pipeline wrapper overhead.
- `FLOW.VALUE.MGET` is already in the high-throughput path because one command carries many refs and uses compact KV-MGET-style response encoding.

## Native protocol compact FLOW.GET direct read path

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`.

Change: compact pipeline `FLOW.GET` modes (`9`, `16`, `17`) bypass generic Flow pipeline command parsing and read directly through `Router.flow_batch_get/3`, while preserving decode/nil/error behavior and existing pipeline-read telemetry.

Same benchmark shape as the Flow read duration section above.

```text
FLOW.GET: 91,471/s
batch latency p50: 69.767 ms
batch latency p95: 86.054 ms
batch latency p99: 102.460 ms
errors: 0
client CPU: 94.26%

FLOW.GET RETURN META: 118,492/s
batch latency p50: 53.914 ms
batch latency p95: 60.258 ms
batch latency p99: 64.030 ms
errors: 0
client CPU: 94.48%
```

Compared with the immediately prior compact values-only run:

```text
FLOW.GET: 90,117/s -> 91,471/s (+1.5%)
FLOW.GET RETURN META: 117,331/s -> 118,492/s (+1.0%)
```

This is a small positive cut. The remaining full `FLOW.GET` cost is mostly record decode/materialization and client object construction, not pipeline wrapper overhead.

## Native protocol KV sanity check after Flow read changes

Source server from current FerricStore worktree, clean startup before Flow/KV checks, 16 shards, native port `16388`.

Command shape:

```bash
PYTHONPATH=src python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --command set|get \
  --test-time 30 \
  --request-mode pipeline \
  --pipeline 100 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --clients 1 --threads 1 --processes 1 \
  --key-count 100000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

```text
SET: 371,164/s
batch latency p50: 16.869 ms
batch latency p95: 22.562 ms
batch latency p99: 28.291 ms
errors: 0
client CPU: 28.85%

GET: 1,188,226/s
batch latency p50: 5.365 ms
batch latency p95: 6.188 ms
batch latency p99: 6.661 ms
errors: 0
client CPU: 114.67%
```

These runs verify the Flow read response/direct-read changes did not break native protocol KV pipeline behavior.

## Native protocol FLOW.GET Python compact builder fast path

Source server from current FerricStore worktree, clean startup, 16 shards, native port `16388`.

Change: Python protocol adapter now compacts raw `FLOW.GET` pipeline tuples directly instead of rebuilding full protocol command payload maps per item before compacting. Supports the hot shapes:

```text
FLOW.GET id
FLOW.GET id PARTITION partition
FLOW.GET id PARTITION partition RETURN META
```

Complex/unknown option shapes still fall back to the generic path.

Same 30s benchmark shape as above.

```text
FLOW.GET: 103,015/s
batch latency p50: 61.876 ms
batch latency p95: 81.714 ms
batch latency p99: 95.830 ms
errors: 0
client CPU: 94.24%

FLOW.GET RETURN META: 144,900/s
batch latency p50: 44.075 ms
batch latency p95: 52.004 ms
batch latency p99: 56.750 ms
errors: 0
client CPU: 94.67%
```

Compared with the direct server read path before the Python compact-builder change:

```text
FLOW.GET: 91,471/s -> 103,015/s (+12.6%)
FLOW.GET RETURN META: 118,492/s -> 144,900/s (+22.3%)
```

## Native protocol FLOW.HISTORY Python compact builder fast path

Source server from current FerricStore worktree, clean startup, 16 shards, native port `16388`.

Change: Python protocol adapter now compacts raw `FLOW.HISTORY` pipeline tuples directly instead of rebuilding full protocol command payload maps per item before compacting. Supports the hot benchmark/read shape with `COUNT`, `PARTITION`, `INCLUDE_COLD`, and `CONSISTENT_PROJECTION` options. Unknown option shapes still fall back.

Benchmark shape:

```bash
PYTHONPATH=src python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation flow-history \
  --flows 100000 \
  --batch-size 100 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 16 \
  --read-duration 30 \
  --flow-read-consistency eventual \
  --pretty
```

```text
FLOW.HISTORY: 388,325/s
batch latency p50: 16.423 ms
batch latency p95: 17.310 ms
batch latency p99: 17.939 ms
errors: 0
client CPU: 95.97%
```

Compared with the same run before the direct parser:

```text
FLOW.HISTORY: 226,878/s -> 388,325/s (+71%)
p99 batch latency: 31.367 ms -> 17.939 ms
```

## Native protocol FLOW.GET validation-safe direct read confirmation

Source server from code, clean `/tmp/ferricstore-protocol-bench`, 16 shards, one native connection, 64 lanes, batch size 100, inflight 64, 100k setup flows, 30s read window.

Validation-safe compact `FLOW.GET` fast path kept the previous direct-read/client-builder gains:

| Operation | Throughput | p50 batch | p99 batch | Client CPU |
| --- | ---: | ---: | ---: | ---: |
| `FLOW.GET` | 104,663/s | 60.92 ms | 91.14 ms | 94.5% |
| `FLOW.GET RETURN META` | 143,082/s | 44.64 ms | 56.65 ms | 94.9% |

Read: server validation guard did not erase the optimized path. Full `FLOW.GET` is still client decode/object-build heavy; meta is meaningfully cheaper.

## Native protocol data-structure compact pipeline snapshot

Date: 2026-06-11 22:41 IDT.

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`, one native connection, 64 lanes, pipeline 100, inflight batches 64, 100k keys, 16-byte values, 30s measured window.

Benchmark shape:

```bash
PYTHONPATH=src python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --command <hget|lrange|zrange> \
  --request-mode pipeline \
  --pipeline 100 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --clients 1 \
  --threads 1 \
  --processes 1 \
  --key-count 100000 \
  --value-bytes 16 \
  --test-time 30 \
  --pretty
```

| Command | Throughput | p50 batch | p95 batch | p99 batch | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| `HGET` | 1,001,304/s | 6.367 ms | 6.750 ms | 6.965 ms | 0 |
| `LRANGE` | 506,119/s | 12.625 ms | 13.537 ms | 13.993 ms | 0 |
| `ZRANGE` | 349,664/s | 19.389 ms | 21.990 ms | 23.293 ms | 0 |

`ZRANGE` before the routed compound type-marker fix was ~3,460/s with p99 above 2s on the same 100k-key shape. Root cause was compact `ZRANGE` reading the zset type marker by hashing the internal marker key instead of reading it from the zset key's shard, causing slow typed fallback scans.

## Native protocol Flow read snapshot after data-structure fixes

Date: 2026-06-11 22:45 IDT.

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`, one native connection, 64 lanes, batch size 100, inflight batches 64, 100k setup flows, 16-byte payloads, 30s measured read window.

Benchmark shape:

```bash
PYTHONPATH=src python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation <value-mget|flow-get|flow-get-meta> \
  --flows 100000 \
  --batch-size 100 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 16 \
  --read-duration 30 \
  --flow-read-consistency eventual \
  --pretty
```

| Operation | Throughput | p50 batch | p95 batch | p99 batch | Client CPU | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `FLOW.VALUE.MGET` | 1,538,859/s | 4.149 ms | 4.395 ms | 4.533 ms | 111.0% | 0 |
| `FLOW.GET` | 103,980/s | 61.481 ms | 82.990 ms | 93.693 ms | 93.7% | 0 |
| `FLOW.GET RETURN META` | 144,191/s | 44.289 ms | 51.822 ms | 56.714 ms | 94.6% | 0 |

Read: value refs are not the bottleneck. `FLOW.VALUE.MGET` is faster than raw hash/list/zset reads in this shape. Full `FLOW.GET` remains response-object/decode heavy and needs a different compact/direct representation if we want it near raw KV latency.

## Native protocol Flow value write duration benchmark

Date: 2026-06-11 22:56 IDT.

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`, one native connection, 64 lanes, inflight batches 64, 16-byte values, 30s measured write window. The benchmark now supports `--read-duration` for shared `value-put` and `value-put-ok`; owned value-put remains count-based because repeated owned names are meant to duplicate-fail.

Benchmark shape:

```bash
PYTHONPATH=src python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --operation <value-put-ok|value-put> \
  --flows 100000 \
  --batch-size <500|100> \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 16 \
  --read-duration 30 \
  --pretty
```

| Operation | Batch | Throughput | p50 batch | p95 batch | p99 batch | Client CPU | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `FLOW.VALUE.PUT RETURN OK_ON_SUCCESS` | 500 | 811,856/s | 39.048 ms | 41.781 ms | 45.617 ms | 100.3% | 0 |
| `FLOW.VALUE.PUT` | 100 | 212,267/s | 31.705 ms | 35.711 ms | 39.320 ms | 77.1% | 0 |

Read: the raw write path is fast when the client only needs success. Returning a ref per item is intentionally more expensive because the server constructs refs and the client decodes a larger response.

## Native protocol KV compact pipeline snapshot

Date: 2026-06-11 23:05 IDT.

Source server from current FerricStore worktree, clean `/tmp/ferricstore-protocol-bench`, 16 shards, native port `16388`, one native connection, 64 lanes, pipeline 100, inflight batches 64, 100k keys, 16-byte values, 30s measured window.

Benchmark shape:

```bash
PYTHONPATH=src python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --command <set|get> \
  --request-mode pipeline \
  --pipeline 100 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --clients 1 \
  --threads 1 \
  --processes 1 \
  --key-count 100000 \
  --value-bytes 16 \
  --test-time 30 \
  --pretty
```

| Command | Throughput | p50 batch | p95 batch | p99 batch | Client CPU | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `SET` | 547,957/s | 11.010 ms | 16.182 ms | 23.401 ms | 46.3% | 0 |
| `GET` | 1,221,277/s | 5.208 ms | 5.691 ms | 6.032 ms | 108.1% | 0 |

Read: one native socket is strong for latency and GET throughput, but this is not the same shape as the historical RESP memtier baseline, which used 4 threads, `--clients=200`, and `--pipeline=50` per thread, for 800 total TCP connections. Native protocol needs either more sockets/processes for a fair max-throughput comparison, or more single-socket compact batching if the target is fewer connections.

## 2026-06-11 compact data-structure write optimization

Server setup:

```text
Source server, native protocol enabled, 16 shards, port 16388.
Data dir was cleaned before the final ZADD first-create validation run.
```

Change under test:

```text
Native PIPELINE compact modes for HSET/LPUSH/RPUSH/SADD/ZADD.
ZADD first-create path now marks proven-new zset index ready without clearing.
```

Correctness checks:

```bash
cd /Users/yoavgea/repos/ferricstore
mix test apps/ferricstore/test/ferricstore/store/shard/zset_index_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace

cd /Users/yoavgea/repos/ferricstore-python
pytest -q tests/test_protocol.py
```

Results:

```text
Server targeted tests: 10 ZSetIndex tests + 66 native command tests, 0 failures
Python protocol tests: 85 passed
```

30s protocol write samples, one Python process, one native connection, pipeline=100, inflight_batches=64, protocol_lanes=64:

```text
HSET: 344,956/s, p50 18.199 ms, p95 23.181 ms, p99 28.346 ms
LPUSH: 338,792/s, p50 18.345 ms, p95 23.377 ms, p99 42.590 ms
SADD: 363,560/s, p50 17.406 ms, p95 21.890 ms, p99 25.173 ms
ZADD before no-clear ready path, clean first-create: 262,016/s, p99 278.805 ms
ZADD after no-clear ready path, clean first-create: 342,586/s, p50 17.825 ms, p95 22.886 ms, p99 55.812 ms
ZADD update-only/dirty same-prefix sample: 349,974/s, p99 25.707 ms
```

Interpretation:

```text
The old generic map PIPELINE path made data-structure writes much slower, especially ZADD.
Compact native modes bring HSET/LPUSH/SADD/ZADD into the same broad throughput band as durable protocol writes.
ZADD first-create latency was mostly caused by unnecessary ready-index clearing; proven-new ready marking removed most of that tail.
```

## 2026-06-11 compact ZRANGE negative-stop fix

Problem found:

```text
Native compact PIPELINE ZRANGE treated stop=-1 as unsupported and fell back to the generic FerricStore.Impl.zrange path.
That made normal ZRANGE usage like ZRANGE key 0 -1 look like a server stall under load.
```

Fix:

```text
Normalize negative rank bounds inside the native compact ZRANGE path using the zset index count.
Use the ready ETS zset rank index directly for bounded/full-rank reads.
Fall back to the existing generic path only when the zset index is unavailable or not ready.
```

Correctness check:

```bash
cd /Users/yoavgea/repos/ferricstore
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace
```

Result:

```text
67 tests, 0 failures
```

Benchmark shape:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --command zrange \
  --url ferric://127.0.0.1:16388 \
  --test-time 10 \
  --threads 1 \
  --processes 1 \
  --clients 1 \
  --pipeline 100 \
  --request-mode pipeline \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 100000 \
  --value-bytes 16 \
  --range-start 0 \
  --range-stop -1 \
  --pretty \
  --no-warmup
```

Before fix, same server/run shape:

```text
ZRANGE 0 -1: 1,245/s, p50 batch 5247.723 ms, p95 5386.972 ms, p99 5386.978 ms, errors 0
```

After fix, clean source server with 16 shards:

```text
ZADD warm/populate: 304,328/s, p50 batch 18.231 ms, p95 27.086 ms, p99 95.774 ms, errors 0
ZRANGE 0 -1: 300,705/s, p50 batch 21.368 ms, p95 26.084 ms, p99 32.320 ms, errors 0
```

Interpretation:

```text
This was a real native-protocol compact ZRANGE performance bug, not a durability/WARaft stall.
The server looked stuck because full-range ZRANGE fell into the slow generic path.
The fixed path is roughly 240x faster on this benchmark shape and keeps latency bounded.
```

## 2026-06-12 compact LRANGE regular-list fast path

Problem found:

```text
Native compact PIPELINE LRANGE still called FerricStore.Impl.lrange for every item.
That path routes LRANGE through list_op/Ra and scans/materializes list data, so even LRANGE 0 0 looked like a server stall after LPUSH load.
```

Fix:

```text
For compact native LRANGE, read list type/meta directly from compound storage.
When list metadata proves regular step-position layout, compute requested element compound keys and read them with compound_batch_get.
Missing keys, irregular metadata, malformed metadata, or other unsafe cases fall back to existing FerricStore.Impl.lrange semantics.
Known non-list type markers return the same WRONGTYPE error directly.
```

Correctness check:

```bash
cd /Users/yoavgea/repos/ferricstore
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace
```

Result:

```text
68 tests, 0 failures
```

Benchmark shape:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
python examples/protocol_kv_benchmark.py \
  --command lrange \
  --url ferric://127.0.0.1:16388 \
  --test-time 10 \
  --threads 1 \
  --processes 1 \
  --clients 1 \
  --pipeline 100 \
  --request-mode pipeline \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 100000 \
  --value-bytes 16 \
  --range-start 0 \
  --range-stop <0|-1> \
  --pretty \
  --no-warmup
```

Before fix, after healthy LPUSH load:

```text
LPUSH: 326,006/s, p50 batch 18.191 ms, p95 24.918 ms, p99 57.267 ms
LRANGE 0 0: 248/s, p50 batch 25775.581 ms, p95 25829.641 ms, p99 25829.849 ms
LRANGE 0 -1: 252/s, p50 batch 25429.241 ms, p95 25430.978 ms, p99 25431.150 ms
```

After fix, clean source server with 16 shards:

```text
LPUSH: 330,970/s, p50 batch 18.904 ms, p95 24.498 ms, p99 32.594 ms
LRANGE 0 0: 486,644/s, p50 batch 13.118 ms, p95 14.478 ms, p99 15.622 ms
LRANGE 0 -1: 8,745/s, p50 batch 738.622 ms, p95 861.128 ms, p99 1005.818 ms
```

Interpretation:

```text
Bounded LRANGE was a real native-protocol performance bug and now uses direct compact reads.
Full-range LRANGE is no longer a server-stall path, but remains much slower when each key has many list values because responses are large.
Benchmark defaults should prefer bounded range windows when measuring command-path overhead.
```

## 2026-06-12 KV sanity after ZRANGE/LRANGE fixes

Shape:

```text
Source server, native protocol enabled, 16 shards, port 16388.
One Python process, one native connection, pipeline=100, inflight_batches=64, protocol_lanes=64, key_count=100000, value_bytes=16, 30s measured window, no warmup.
```

Results:

```text
SET: 550,649/s, p50 batch 11.196 ms, p95 14.804 ms, p99 19.700 ms, errors 0
GET: 1,140,787/s, p50 batch 5.571 ms, p95 6.005 ms, p99 6.499 ms, errors 0
```

Interpretation:

```text
Core compact KV throughput was not hurt by the range-path fixes.
```

## 2026-06-12 Flow native protocol command sweep after range fixes

Shape:

```text
Source server, native protocol enabled, 16 shards, port 16388.
One native connection, protocol_lanes=32, inflight_batches=64, partitions=16, payload_bytes=16.
20k-flow setup for mutation/read command coverage. Value/read duration commands used 5s measured windows.
```

Results:

```text
FLOW.CREATE_MANY: 132,537/s, p50 batch 110.946 ms, p95 140.440 ms, p99 140.744 ms, errors 0
FLOW.CLAIM_DUE: 242,584/s, p50 batch 75.165 ms, p95 81.232 ms, p99 81.453 ms, errors 0
FLOW.COMPLETE_MANY: 178,859/s, p50 batch 73.842 ms, p95 92.113 ms, p99 96.719 ms, errors 0
FLOW.TRANSITION_MANY: 207,984/s, p50 batch 60.949 ms, p95 71.446 ms, p99 72.043 ms, errors 0
FLOW.VALUE.PUT ok-only: 652,699/s, p50 batch 45.832 ms, p95 74.726 ms, p99 100.495 ms, errors 0
FLOW.VALUE.MGET: 2,036,510/s, p50 batch 15.672 ms, p95 16.401 ms, p99 16.783 ms, errors 0
FLOW.GET meta: 129,542/s, p50 batch 24.595 ms, p95 27.511 ms, p99 31.174 ms, errors 0
```

Interpretation:

```text
Range-path fixes did not break native Flow command coverage.
Value refs are now in the same high-throughput range as core native reads/writes.
Flow metadata reads are lower than raw value reads because they hydrate and trim Flow records, but stayed stable with bounded latency.
```

## 2026-06-12 Flow native protocol extended command sweep

Shape:

```text
Source server, native protocol enabled, 16 shards, port 16388.
One native connection, protocol_lanes=32, inflight_batches=64, partitions=16, payload_bytes=16.
20k-flow setup for command coverage.
```

Results:

```text
FLOW.START_AND_CLAIM: 65,878/s, p50 batch 26.603 ms, p95 145.661 ms, p99 148.207 ms, errors 0
FLOW.SIGNAL: 172,674/s, p50 batch 47.255 ms, p95 79.990 ms, p99 84.473 ms, errors 0
FLOW.STEP_CONTINUE: 71,452/s, p50 batch 40.936 ms, p95 75.675 ms, p99 76.265 ms, errors 0
FLOW.RETRY_MANY: 156,946/s, p50 batch 104.998 ms, p95 109.578 ms, p99 112.184 ms, errors 0
FLOW.FAIL_MANY: 133,500/s, p50 batch 104.439 ms, p95 127.709 ms, p99 134.369 ms, errors 0
FLOW.CANCEL_MANY: 161,050/s, p50 batch 85.168 ms, p95 103.767 ms, p99 104.753 ms, errors 0
FLOW.HISTORY hot/eventual: 274,308/s, p50 batch 5.769 ms, p95 6.705 ms, p99 7.183 ms, errors 0
FLOW.LIST: 10,397/s, p50 batch 310.819 ms, p95 422.977 ms, p99 476.902 ms, errors 0
```

Interpretation:

```text
Most native Flow command families are functioning and bounded.
FLOW.LIST is the next likely protocol/server bottleneck: low throughput and high batch latency compared with direct Flow history/meta reads.
```

## 2026-06-12 compact ZRANGE missing-key fast path

Context: user reported ZRANGE looked stuck. Server was alive, but native compact `ZRANGE 0 -1` on unwarmed/missing keys fell through to the generic Ra-backed `FerricStore.Impl.zrange` path because the zset index was not marked ready. Existing/non-empty zsets were already fast.

Fix: compact native ZRANGE now returns `[]` for truly missing keys without generic fallback, while preserving WRONGTYPE for plain string keys and non-zset compound keys.

Validation:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace
69 tests, 0 failures
```

Bench shape:

```text
server: source, MIX_ENV=prod, 16 shards, native port 16388, clean data dir
client: one native connection, pipeline=50, inflight_batches=16, protocol_lanes=16, key_count=1000, value_bytes=16, test_time=3s
```

Before fix, unwarmed/missing ZRANGE:

```text
ZRANGE 0 -1 no warmup: ~524/s, batch p50 ~92.97ms, p99 ~129.71ms
```

After fix:

```text
ZRANGE 0 -1 no warmup: 113,574/s, batch p50 0.411ms, p99 0.632ms, errors=0
ZRANGE 0 -1 warmup:    153,251/s, batch p50 0.298ms, p99 0.555ms, errors=0
GET sanity warmup:      403,082/s, batch p50 0.102ms, p99 0.244ms, errors=0
```

## 2026-06-12 native KV throughput presets after ZRANGE missing-key fix

Context: after fixing compact ZRANGE missing-key fallback, reran the optimized 30s native KV presets. A manual `request_mode=batch` run showed much lower SET throughput, but that is not the optimized throughput shape; the benchmark presets use `request_mode=many`, larger batches, and prebuilt keys.

Server:

```text
source server, MIX_ENV=prod, 16 shards, native port 16388, clean data dir at start of run window
```

Commands:

```bash
python examples/protocol_kv_benchmark.py --preset set-throughput --url ferric://127.0.0.1:16388
python examples/protocol_kv_benchmark.py --preset get-throughput --url ferric://127.0.0.1:16388
```

Results:

```text
SET preset: 510,680/s, p50 batch 67.032ms, p95 92.666ms, p99 107.288ms, errors=0
GET preset: 1,137,388/s, p50 batch 55.632ms, p95 64.473ms, p99 71.313ms, errors=0
```

Interpretation:

```text
Optimized native KV throughput remains in the same range after the ZRANGE fix.
The slower default batch run measures a different client/server shape and should not be compared to the throughput preset.
```

## 2026-06-12 native data-structure and Flow sweep after ZRANGE missing-key fix

Server:

```text
source server, MIX_ENV=prod, 16 shards, native port 16388
```

KV/data-structure sweep shape:

```text
one native connection, request_mode=pipeline, pipeline=100, inflight_batches=64, protocol_lanes=64, key_count=10000, value_bytes=16, 3s per command
```

Results:

```text
HSET:      271,998/s, p50 batch 21.424ms, p99 59.456ms, errors=0
HGET:      724,771/s, p50 batch  8.805ms, p99 10.158ms, errors=0
LPUSH:     263,254/s, p50 batch 24.098ms, p99 34.363ms, errors=0
LRANGE 0:  389,947/s, p50 batch 16.377ms, p99 18.135ms, errors=0
SADD:      296,275/s, p50 batch 21.463ms, p99 29.838ms, errors=0
SISMEMBER: 789,213/s, p50 batch  8.078ms, p99  9.137ms, errors=0
ZADD:      197,060/s, p50 batch 20.521ms, p99 521.418ms, errors=0
ZRANGE 0:  282,961/s, p50 batch 23.110ms, p99 30.453ms, errors=0
ZRANGE missing/full: 285,120/s, p50 batch 22.881ms, p99 29.776ms, errors=0
```

Follow-up ZADD rerun with fresh prefix and 10s window:

```text
ZADD: 317,687/s, p50 batch 18.568ms, p95 23.665ms, p99 70.307ms, errors=0
```

Interpretation:

```text
No remaining data-structure stall reproduced. ZADD had one p99 spike in the broad sweep but rerun was bounded.
```

Flow sweep shape:

```text
one native connection, protocol_lanes=32, inflight_batches=64, partitions=16, payload_bytes=16, 20k-flow setup
```

Results:

```text
FLOW.CREATE_MANY:     116,609/s, p50 batch 145.894ms, p99 161.638ms, errors=0
FLOW.CLAIM_DUE:       196,999/s, p50 batch  28.960ms, p99  35.242ms, errors=0
FLOW.COMPLETE_MANY:   195,157/s, p50 batch  66.772ms, p99  88.179ms, errors=0
FLOW.TRANSITION_MANY: 213,485/s, p50 batch  65.307ms, p99  74.250ms, errors=0
FLOW.START_AND_CLAIM: 134,549/s, p50 batch  21.358ms, p99  28.128ms, errors=0
FLOW.GET meta:         84,314/s, p50 batch   3.777ms, p99   5.198ms, errors=0, batch_size=5
FLOW.GET meta:        115,339/s, p50 batch  55.975ms, p99  69.074ms, errors=0, batch_size=100
FLOW.HISTORY:         141,387/s, p50 batch   2.229ms, p99   2.818ms, errors=0
FLOW.VALUE.MGET:    2,129,891/s, p50 batch  14.983ms, p99  16.255ms, errors=0
FLOW.LIST:             29,380/s, p50 batch 109.080ms, p99 157.157ms, errors=0
```

Interpretation:

```text
FLOW.LIST remains the slowest Flow read family because no-partition list fans into hidden auto buckets and hydrates records.
The earlier ~10k/s FLOW.LIST number did not reproduce on current code, but list is still the next architectural optimization target if we care about high-rate query workloads.
```

## 2026-06-12 Flow auto partition key-list allocation cut

Change:

```text
Ferricstore.Flow.Keys.auto_partition_keys/0 now returns a compile-time literal list instead of allocating 256 bucket key binaries per call.
```

Reason:

```text
FLOW.LIST without partition uses the hidden auto-bucket fan-in path. Every call needs the 256 hidden bucket keys to build state index keys. This change removes avoidable per-call bucket-list construction without changing external semantics.
```

Validation:

```text
mix test apps/ferricstore/test/ferricstore/flow_test.exs --trace
220 tests, 0 failures
```

Benchmark shape:

```text
source server, MIX_ENV=prod, 16 shards, native port 16388, clean data dir
one native connection, protocol_lanes=32, inflight_batches=64, partitions=16, payload_bytes=16
operation=flow-list, flows=20000, batch_size=50
```

Results after restart with patched code:

```text
FLOW.LIST sample 1: 105,151/s, p50 batch 34.083ms, p99 41.390ms, errors=0
FLOW.LIST sample 2:  85,425/s, p50 batch 38.820ms, p99 43.790ms, errors=0
```

Interpretation:

```text
FLOW.LIST is no longer reproducing the earlier ~10k/s stall. The allocation cut is safe and keeps list in the ~85k-105k/s range on this shape, but list remains the highest-latency Flow read because it fans into auto buckets and hydrates records.
```

## 2026-06-12 Flow read benchmark defaults tuned for native throughput

Change:

```text
protocol_flow_commands_benchmark.py default batch_size for FLOW.GET, FLOW.GET RETURN META, and FLOW.HISTORY changed from 5 to 250.
```

Reason:

```text
The old default measured low-latency tiny batches and underfilled the native multiplexed protocol. That made broad command sweeps under-report the optimized read path. Users can still pass --batch-size 5 explicitly for latency probes.
```

Validation:

```text
pytest -q tests/test_protocol_flow_commands_benchmark.py
29 passed
```

Benchmark shape:

```text
source server, MIX_ENV=prod, 16 shards, native port 16388
one native connection, protocol_lanes=32, inflight_batches=64, partitions=16, payload_bytes=16
flows=20000, read_duration=3s, default batch_size after change
```

Results:

```text
FLOW.GET RETURN META default: 145,594/s, batch_size=250, p50 batch 109.094ms, p99 140.144ms, errors=0
FLOW.HISTORY default:         404,123/s, batch_size=250, p50 batch  39.349ms, p99  42.899ms, errors=0
```

Interpretation:

```text
Benchmark defaults now use the optimized native protocol shape for Flow read throughput. Batch latency is per 250-command batch; use smaller --batch-size for single-request latency probes.
```

## Native disconnect overload guard - lane cleanup

Observed issue:

- A killed native DBOS run with high lanes/connections left Beam burning roughly 400-500% CPU.
- RESP `PING` and missing-key `ZRANGE` still replied, so the server was not globally wedged.
- Root cause was native lane cleanup: connection close sent `:shutdown` into lane mailboxes, so lanes drained queued frames before stopping.

Fix:

- Native connection cleanup now terminates lane processes immediately through `Lane.stop/1`.
- Added lane unit coverage and TCP integration coverage that active native lanes die on connection close.

Local validation on source server, 16 shards, native port 16388:

```text
RESP ZRANGE missing key: 17.239 ms, returned *0
Native close probe: sent 12,000 SET frames then closed socket immediately
Beam CPU after 1s: 7.3%
Beam CPU after 5s: 3.9%
```

Result:

- ZRANGE missing-key path is not stuck.
- Native overload/disconnect no longer leaves queued lane work draining for minutes after client death.

## Native KV preset rerun after lane cleanup

Server:

```text
source server, MIX_ENV=prod
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
FERRICSTORE_PORT=16379
FERRICSTORE_NATIVE_PORT=16388
fresh data dir: /tmp/ferricstore-protocol-bench/data
```

Commands:

```bash
python examples/protocol_kv_benchmark.py --preset set-throughput --url ferric://127.0.0.1:16388
python examples/protocol_kv_benchmark.py --preset get-throughput --url ferric://127.0.0.1:16388
```

Results:

```text
SET: 1,949,456/s, batch p50 15.895 ms, p95 20.680 ms, p99 26.061 ms, errors 0
GET: 2,485,960/s, batch p50 25.705 ms, p95 26.902 ms, p99 27.696 ms, errors 0
```

Notes:

- These are protocol preset runs: one native connection, `request_mode=many`, prebuilt keys.
- Lane cleanup change is not on the normal request hot path; no regression visible in KV preset runs.

## Native DBOS queue rerun after lane cleanup

Server:

```text
source server, MIX_ENV=prod
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
fresh data dir: /tmp/ferricstore-protocol-bench/data
```

Primary queue shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 100 \
  --claim-partition-batch-size 500 \
  --claim-drain-batches 8 \
  --create-batch-size 500 \
  --complete-async-depth 8 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode blocking \
  --partition-mode auto \
  --claim-job-only \
  --reclaim-expired
```

Result:

```text
100k flows, 1 connection / 32 lanes:
  e2e: 44,108/s
  create: 48,979/s
  process: 44,158/s
  queue latency p50/p95/p99: 77.707 / 311.888 / 582.810 ms
  claim calls: 1,134
  empty claims: 14
  avg claim batch: 88.18
  duplicates: 0
```

Previously problematic overload shape:

```text
50k flows, 2 connections / 64 lanes:
  e2e: 39,019/s
  create: 45,366/s
  process: 39,103/s
  queue latency p50/p95/p99: 16.708 / 51.922 / 72.834 ms
  claim calls: 561
  empty claims: 10
  avg claim batch: 89.13
  duplicates: 0
```

Conclusion:

- 2 connections / 64 lanes no longer gets stuck after the lane cleanup fix.
- It is slower for this workload, so 1 connection / 32 lanes remains the default/native queue benchmark shape.

## Native data-structure sweep after lane cleanup

Shape:

```text
source server, native port 16388
5s per command
1 native connection
request_mode=pipeline
pipeline=100
inflight_batches=64
protocol_lanes=64
key_count=10000
unique prefix per command
```

Results:

```text
HSET:      233,383/s, p50 26.913 ms, p95 34.630 ms, p99 40.623 ms, errors 0
HGET:      798,808/s, p50 7.894 ms,  p95 9.515 ms,  p99 10.507 ms, errors 0
LPUSH:     214,176/s, p50 28.425 ms, p95 43.894 ms, p99 60.805 ms, errors 0
LRANGE 0:  407,537/s, p50 15.642 ms, p95 18.160 ms, p99 20.119 ms, errors 0
SADD:      246,489/s, p50 25.587 ms, p95 31.927 ms, p99 36.005 ms, errors 0
SISMEMBER: 919,878/s, p50 6.858 ms,  p95 8.284 ms,  p99 9.142 ms,  errors 0
ZADD:      174,746/s, p50 26.533 ms, p95 38.330 ms, p99 468.453 ms, errors 0
ZRANGE 0:  173,160/s, p50 37.023 ms, p95 45.783 ms, p99 49.239 ms, errors 0
```

Notes:

- No command stuck.
- ZADD p99 spike repeated as the main suspicious data-structure tail-latency item.

## Native Flow command sweep after lane cleanup

Shape:

```text
source server, native port 16388
fresh data dir
1 native connection
protocol_lanes=32
partitions=16
flows=10000
read_duration=5s for duration-style commands
```

Results:

```text
FLOW.CREATE_MANY:       171,181/s, p50 55.266 ms, p95 57.685 ms, p99 58.273 ms, errors 0
FLOW.CLAIM_DUE:         280,966/s, p50 34.039 ms, p95 34.974 ms, p99 35.074 ms, errors 0
FLOW.COMPLETE_MANY:     197,586/s, p50 45.110 ms, p95 48.534 ms, p99 48.914 ms, errors 0
FLOW.TRANSITION_MANY:   231,463/s, p50 34.167 ms, p95 39.487 ms, p99 40.089 ms, errors 0
FLOW.RETRY_MANY:        180,488/s, p50 48.790 ms, p95 52.455 ms, p99 52.778 ms, errors 0
FLOW.FAIL_MANY:         183,109/s, p50 48.318 ms, p95 51.667 ms, p99 51.922 ms, errors 0
FLOW.CANCEL_MANY:       268,415/s, p50 29.863 ms, p95 34.490 ms, p99 34.729 ms, errors 0
FLOW.VALUE.PUT:         169,147/s, p50 36.830 ms, p95 52.676 ms, p99 65.584 ms, errors 0
FLOW.VALUE.PUT OK:      546,803/s, p50 56.545 ms, p95 76.683 ms, p99 92.910 ms, errors 0
FLOW.VALUE.PUT owned:    69,995/s, p50 21.330 ms, p95 31.146 ms, p99 31.217 ms, errors 0
FLOW.VALUE.MGET:      1,893,640/s, p50 16.850 ms, p95 17.857 ms, p99 18.527 ms, errors 0
FLOW.START_AND_CLAIM:   124,559/s, p50 22.654 ms, p95 26.148 ms, p99 26.316 ms, errors 0
FLOW.GET meta:          135,549/s, p50 117.696 ms, p95 137.637 ms, p99 146.441 ms, errors 0
FLOW.HISTORY:           353,447/s, p50 45.325 ms, p95 47.262 ms, p99 48.173 ms, errors 0
FLOW.LIST:               27,763/s, p50 123.175 ms, p95 162.066 ms, p99 162.180 ms, errors 0
FLOW.SIGNAL:            142,715/s, p50 26.105 ms, p95 39.798 ms, p99 41.882 ms, errors 0
FLOW.STEP:               42,692/s, p50 64.075 ms, p95 83.184 ms, p99 86.230 ms, errors 0
```

Notes:

- Broad Flow command coverage is clean: no stuck command and no benchmark errors.
- `FLOW.LIST` remains the slowest Flow query shape because it fans into auto partition buckets and hydrates records.
- `FLOW.STEP` is intentionally heavier than claim/complete primitives because it combines transition + claim semantics.

## FLOW.LIST and ZADD isolated follow-up

FLOW.LIST batch/lane samples:

```text
flows=20000, 1 connection, 32 lanes:
  batch 50:  16,381/s, p50 199.110 ms, p99 271.529 ms
  batch 100: 40,237/s, p50 145.113 ms, p99 239.264 ms
  batch 250: 71,276/s, p50 225.550 ms, p99 248.278 ms

flows=20000, batch 100:
  lanes 16: 27,456/s, p99 285.521 ms
  lanes 32: 40,237/s, p99 239.264 ms
  lanes 64: 26,434/s, p99 293.910 ms
```

Conclusion:

- `FLOW.LIST` is batch-size sensitive.
- One connection / 32 lanes remains the better native shape.
- Throughput default should use a larger list batch, while explicit smaller batch remains useful for latency probes.

ZADD isolated 10s samples:

```text
sample 1: 227,705/s, p50 22.118 ms, p95 62.468 ms, p99 88.758 ms
sample 2: 294,474/s, p50 20.836 ms, p95 28.777 ms, p99 34.180 ms
sample 3: 292,180/s, p50 20.989 ms, p95 28.180 ms, p99 34.467 ms
```

Conclusion:

- The earlier 468ms ZADD p99 was not stable in isolated samples.
- Keep ZADD as a tail-watch item, but no immediate correctness fix is indicated.

## FLOW.LIST benchmark default updated

Change:

```text
protocol_flow_commands_benchmark.py:
  flow-list default batch_size: 50 -> 250
```

Validation:

```text
pytest -q tests/test_protocol_flow_commands_benchmark.py
29 passed

Default FLOW.LIST sample after change:
  batch_size: 250
  flows: 20000
  e2e/items: 46,246/s
  p50/p95/p99 batch latency: 409.679 / 411.461 / 411.645 ms
  errors: 0
```

Note:

- The default now reflects the measured throughput shape.
- Users can still pass `--batch-size 50` for smaller latency probe batches.

## Native ZRANGE stuck-server check

Environment:
- Source server on fresh temp data dir
- `FERRICSTORE_SHARD_COUNT=16`
- RESP port `17379`, protocol port `17388`

Correctness/protection checks:
- RESP `ZRANGE` missing key: returned empty list.
- RESP `ZRANGE` wrong type: returned WRONGTYPE.
- RESP `ZRANGE` over 1,000-member zset: returned successfully.
- Native one-key/5,000-member full `ZRANGE 0 -1`: returned successfully; expensive but not stuck.
- Native queued large full-range ZRANGE followed by immediate socket close: server returned to idle CPU.
- Added server regression test: native connection close terminates lane with queued large ZRANGE responses.

30s native bounded ZRANGE benchmark:
- Command: `zrange`
- URL: `ferric://127.0.0.1:17388`
- Shape: `clients=1`, `threads=1`, `pipeline=100`, `request_mode=pipeline`, `inflight_batches=64`, `protocol_lanes=64`, `range=0..0`
- Throughput: `453,083/s`
- p50 batch latency: `14.061 ms`
- p95 batch latency: `15.257 ms`
- p99 batch latency: `15.895 ms`
- Errors: `0`

Conclusion:
- No reproducible server stuck in current ZRANGE path.
- Large full-range ZRANGE is response-materialization expensive and should be benchmarked separately from bounded rank reads.

## Native compact binary-list response encoding

Change:
- Added compact protocol encoding for binary-list results inside pipeline responses.
- Added top-level compact binary-list-list payload for values-only pipeline reads.
- Affects bounded `ZRANGE`, `LRANGE`, and similar list-of-binary read results.
- High-level SDK result shape remains unchanged.

Tests:
- FerricStore server: `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
  - Result: `112 tests, 0 failures`
- Python SDK: `pytest -q tests/test_protocol.py`
  - Result: `87 passed`

Fresh source server:
- `FERRICSTORE_SHARD_COUNT=16`
- RESP port `17379`, protocol port `17388`
- Temp data dir: `/tmp/ferricstore-range-compact.iiTcXA`

30s native bounded ZRANGE after change:
- Shape: `clients=1`, `threads=1`, `pipeline=100`, `request_mode=pipeline`, `inflight_batches=64`, `protocol_lanes=64`, `range=0..0`
- Throughput: `596,895/s`
- p50 batch latency: `10.686 ms`
- p95 batch latency: `11.168 ms`
- p99 batch latency: `11.481 ms`
- Errors: `0`

Prior comparable native bounded ZRANGE sample:
- Throughput: `453,083/s`
- p99 batch latency: `15.895 ms`

Short LRANGE sanity after change:
- Shape: same, `test_time=10`, `range=0..0`
- Throughput: `645,801/s`
- p99 batch latency: `10.908 ms`
- Errors: `0`

Conclusion:
- Compact binary-list encoding materially improves bounded ZRANGE response throughput and latency.
- Large full-range range reads still depend mostly on response size and should be treated as materialization-heavy operations.

## Native compact SMEMBERS pipeline mode

Change:
- Added compact pipeline mode `27` for `SMEMBERS`.
- Added Python protocol encoder support for compact `SMEMBERS` batches.
- Added `smembers` to `protocol_kv_benchmark.py` command coverage.
- Response uses compact binary-list encoding, preserving SDK result shape.

Tests:
- FerricStore server: `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
  - Result: `113 tests, 0 failures`
- Python SDK: `pytest -q tests/test_protocol.py tests/test_protocol_kv_benchmark.py`
  - Result: `107 passed`

Fresh source server:
- `FERRICSTORE_SHARD_COUNT=16`
- RESP port `17379`, protocol port `17388`
- Temp data dir: `/tmp/ferricstore-smembers-compact.Lmq1Si`

30s native SMEMBERS benchmark:
- Shape: `clients=1`, `threads=1`, `pipeline=100`, `request_mode=pipeline`, `inflight_batches=64`, `protocol_lanes=64`, `key_count=1000`
- Throughput: `573,867/s`
- p50 batch latency: `11.479 ms`
- p95 batch latency: `14.332 ms`
- p99 batch latency: `16.894 ms`
- Errors: `0`

Conclusion:
- `SMEMBERS` is now included in protocol benchmark coverage and uses compact binary-list response encoding.

## Native ZRANGE large-response pressure guard

Finding:
- Bounded ZRANGE was healthy and did not wedge the server.
- Full-range ZRANGE over one 5000-member sorted set is materialization-heavy, but the server drained back to idle after client disconnect/completion.
- Risk was not sorted-set corruption; risk was native response coalescing by response count only, where a small number of large ZRANGE responses can create a huge socket-send batch.

Change:
- Added `native_response_coalesce_bytes` server setting.
- Default: `8 MiB`.
- Native connection stops opportunistic response coalescing when the accumulated response iodata crosses the byte limit.
- `WINDOW_UPDATE` now reports `response_coalesce_bytes` so protocol clients can observe the server-side guard.

Tests:
- FerricStore server: `mix test apps/ferricstore_server/test/ferricstore_server/native/integration_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace`
  - Result: `100 tests, 0 failures`

Fresh source server:
- `FERRICSTORE_SHARD_COUNT=16`
- RESP port `17379`, protocol port `17388`
- Temp data dir: `/tmp/ferricstore-zrange-fix.gRyPr0`

Large full-range ZRANGE pressure check:
- Preload: one sorted set, `5000` unique members.
- Shape: `clients=1`, `threads=1`, `pipeline=10`, `request_mode=pipeline`, `inflight_batches=8`, `protocol_lanes=8`, `range=0..4999`, `test_time=3s`.
- Throughput: `775/s` full-range requests.
- p50 batch latency: `102.485 ms`.
- p95 batch latency: `127.521 ms`.
- p99 batch latency: `140.599 ms`.
- Errors: `0`.
- Server after drain: CPU returned to idle, RSS returned near baseline.

Quick hot-path smoke after guard:
- GET, 5s: `1,167,086/s`, p99 batch `6.957 ms`, errors `0`.
- SET, 5s: `513,959/s`, p99 batch `28.734 ms`, errors `0`.

Conclusion:
- No reproducible ZRANGE infinite loop/stuck server.
- Large ZRANGE is expensive because the response itself is large.
- The guard reduces native connection burst memory/CPU pressure without changing command semantics.

### ZRANGE bounded no-count fast path

Date: 2026-06-12
Server: local source server, 16 shards, native protocol, clean temp data dir.
Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17388 \
  --command zrange \
  --test-time 10 \
  --clients 1 \
  --threads 1 \
  --pipeline 100 \
  --request-mode pipeline \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 1000
```

Before no-count fast path:

```text
ZRANGE 0 0: 332,538/s, p99 batch 25.742 ms
```

After no-count fast path:

```text
ZRANGE 0 0: 599,798/s, p50 batch 10.638 ms, p95 11.102 ms, p99 11.383 ms, errors 0
```

Large full-range smoke on patched server:

```text
5000-member ZSET, ZRANGE 0 -1: one read 31.240 ms, 20 reads 585.065 ms, returned 5000, timeout/errors 0
```

Conclusion: no evidence of a ZRANGE infinite-loop/server-stuck bug. The real risk is large-response pressure when clients pipeline many full-range reads; byte-aware native response coalescing limits that burst pressure, and bounded ZRANGE now avoids an unnecessary count lookup.

### Native protocol data-structure read optimization pass

Date: 2026-06-12
Server: local source server, 16 shards, native protocol, clean temp dirs per run unless noted.
Shape for targeted command runs:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17388 \
  --test-time 10 \
  --clients 1 \
  --threads 1 \
  --pipeline 100 \
  --request-mode pipeline \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 1000
```

Changes:

```text
SMEMBERS compact mode: direct type-marker check + direct compound_scan member extraction, avoiding Set.handle_ast.
HGETALL compact mode: direct type-marker check + direct compound_scan.
HGETALL compact-values: encode scanned {field,value} entries directly as existing 0x87 binary map-list wire format, avoiding per-result map construction.
ZRANGE bounded positive ranges: skip count() and call rank_range directly.
```

Focused tests:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --trace
116 tests, 0 failures
```

Targeted results:

```text
SMEMBERS before: 188,257/s, p99 45.051 ms
SMEMBERS after: 622,882/s and 626,718/s on fresh targeted run, p99 ~15.3 ms
SMEMBERS after direct scan final samples: 425,545/s and 412,475/s, p99 ~21 ms

HGETALL before: 300,926/s, p99 27.168 ms
HGETALL after direct entry encoder: 572,500/s and 574,868/s, p99 ~17.3-17.5 ms

ZRANGE 0 0 before: 332,538/s, p99 25.742 ms
ZRANGE 0 0 after: 599,798/s, p99 11.383 ms
```

30s GET/SET sanity after changes:

```text
GET: 1,156,649/s, p50 5.499 ms, p99 6.237 ms, errors 0
SET: 539,043/s, p50 11.421 ms, p99 19.907 ms, errors 0
```

Scalability finding:

```text
After a 30s SET run with 100k string keys, HGETALL and SMEMBERS collapsed to ~33-35k/s with p99 >220 ms.
Reason: hash/set full-member reads use compound prefix scans over the shard keydir ETS table. The keydir table is :set, so prefix_scan_entries uses full-table ETS select and cost grows with total shard key count, not just collection size.
This is a real scalability issue for HGETALL/SMEMBERS on large unrelated keyspaces. Stable performance needs a per-collection member index or an ordered compound-key side index; protocol encoding alone cannot fix it.
```

## Native protocol sanity after rejecting ordered-keydir experiment

Server: source build, 16 shards, `FERRICSTORE_NATIVE_ENABLED=true`, clean temp data dir.
Client: one ferric socket, `--pipeline 100`, `--inflight-batches 64`, `--protocol-lanes 64`.

Kept changes:
- Compact ZRANGE bounded positive range skips count.
- Compact SMEMBERS uses direct type check + direct compound scan.
- Compact HGETALL uses direct type check + direct entry-list encoder.

Rejected change:
- Main keydir `:ordered_set` experiment. It did not fix large-keyspace HGETALL/SMEMBERS collapse and was reverted to avoid broad storage risk.

Final mixed-order run, with 30s GET/SET before data-structure reads:

```text
GET:      1,159,348/s, p50 5.495 ms, p99 6.198 ms, errors 0
SET:        535,659/s, p50 11.518 ms, p99 20.554 ms, errors 0
ZRANGE:     344,641/s, p50 18.527 ms, p99 24.570 ms, errors 0
HGETALL:     36,492/s, p50 183.023 ms, p99 240.019 ms, errors 0
SMEMBERS:    35,679/s, p50 186.428 ms, p99 241.289 ms, errors 0
```

Conclusion:
- ZRANGE does not appear stuck or looping; bounded ZRANGE improved materially on clean focused runs.
- HGETALL/SMEMBERS still have a real scalability issue after a large unrelated keyspace is present.
- Correct next design is a rebuildable compound-member index, not broad ordered-keydir replacement.

## 2026-06-12 - native compound member index fix

Source server from `/Users/yoavgea/repos/ferricstore`, clean temp data dir, 16 shards, native protocol on `127.0.0.1:17388`.

Benchmark command shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17388 \
  --clients 1 --threads 1 \
  --pipeline 100 \
  --request-mode pipeline \
  --inflight-batches 64 \
  --protocol-lanes 64
```

Results:

| Command | Duration | Throughput | p50 batch latency | p99 batch latency | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 30s | 1,153,350/s | 5.518 ms | 6.296 ms | 0 |
| SET | 30s | 534,220/s | 11.597 ms | 21.029 ms | 0 |
| HGETALL | 10s | 642,291/s | 9.937 ms | 12.489 ms | 0 |
| SMEMBERS | 10s | 775,577/s | 8.215 ms | 10.011 ms | 0 |
| ZRANGE | 10s | 349,782/s | 18.286 ms | 24.220 ms | 0 |

Finding: ZRANGE was not stuck. The real regression was compound-member scans (`HGETALL`, `SMEMBERS`) falling back to keydir prefix scan after large unrelated keyspaces. The fix adds a rebuildable compound member index and wires WARaft segment projection into it.

## 2026-06-12 - native broad command sweep after compound member index fix

Environment:

- FerricStore source server, clean `/tmp` data dirs per run.
- Native protocol: `ferric://127.0.0.1:17388`.
- Server shards: 16.
- KV broad sweep used one client process/thread/connection with protocol lanes unless noted.

KV highlights:

| command | shape | throughput | p99 |
| --- | --- | ---: | ---: |
| SET | 30s preset, `request-mode=many`, pipeline 500 | 2,011,445/s | 24.223 ms |
| GET | 30s preset, `request-mode=many`, pipeline 1000 | 2,677,520/s | 25.565 ms |
| HSET | 10s, pipeline 100 | 328,378/s | 27.847 ms |
| HGET | 10s, pipeline 100 | 918,356/s | 7.910 ms |
| HMGET | 10s, pipeline 100 | 564,236/s | 13.088 ms |
| HGETALL | 10s, pipeline 100 | 638,225/s | 12.601 ms |
| LPUSH | 10s, pipeline 100 | 319,970/s | 27.354 ms |
| LRANGE | 10s, pipeline 100 | 621,223/s | 12.067 ms |
| SADD | 10s, pipeline 100 | 346,914/s | 25.775 ms |
| SMEMBERS | 10s, pipeline 100 | 749,160/s | 11.072 ms |
| SISMEMBER | 10s, pipeline 100 | 930,414/s | 7.778 ms |
| ZADD | 10s, pipeline 100 | 324,556/s | 29.009 ms |
| ZRANGE | 10s, pipeline 100 | 343,630/s | 24.512 ms |
| ZSCORE | 10s, pipeline 100 | 824,756/s | 8.910 ms |
| MIXED | 10s, pipeline 100 | 405,479/s | 25.404 ms |

Flow command sweep:

| operation | throughput | p99 |
| --- | ---: | ---: |
| create-many | 247,291/s | 132.241 ms |
| transition-many | 246,581/s | 139.553 ms |
| complete-many | 195,092/s | 228.240 ms |
| retry-many | 197,493/s | 192.465 ms |
| fail-many | 200,771/s | 218.593 ms |
| cancel-many | 253,882/s | 123.145 ms |
| claim-due | 281,006/s | 139.291 ms |
| start-and-claim | 64,249/s | 523.378 ms |
| step | 82,388/s | 419.784 ms |
| signal | 205,969/s | 143.348 ms |
| flow-get | 106,863/s | 293.613 ms |
| flow-history | 465,211/s | 70.403 ms |
| flow-list | 10,042/s | 4,946.052 ms |
| value-put-owned | 93,430/s | 420.496 ms |
| value-mget | 1,977,067/s | 16.126 ms |

Clean native DBOS-style queued run:

| flows | worker api | protocol connections | protocol lanes | e2e | create | process | queue p99 | empty claims |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | queue | 1 | 32 | 57,956/s | 59,848/s | 58,055/s | 225.472 ms | 0 |

Notes:

- The previous suspected `ZRANGE` hang was not reproducible on a clean server. RESP `ZADD/ZRANGE` and native `zrange` both returned correctly.
- The actual correctness bug found in this area was missing WARaft segment projection updates for the compound member index, which made commands like `HGETALL` / `SMEMBERS` fall back to empty or slow scans after durable writes.
- `flow-list`, `start-and-claim`, `step`, `flow-get`, and `value-put-owned` are the next obvious protocol/Flow bottlenecks from this sweep.

## 2026-06-12 - native protocol mixed create_many fix + clean ZRANGE check

Context:
- Source FerricStore server from `/Users/yoavgea/repos/ferricstore`.
- Fresh data dir: `/tmp/ferricstore-native-clean`.
- Native protocol: `ferric://127.0.0.1:17388`.
- Server: 16 shards, WARaft durable path.
- Fixed native `FLOW.CREATE_MANY MIXED` compact support and benchmark explicit-partition batching.

Correctness checks:
- RESP `ZRANGE WITHSCORES` returned in ~2ms on a live server.
- Native bounded `ZRANGE` 30s completed without hangs/errors.
- Direct native `FlowClient.create_many(None, items_with_partition_key, return_ok_on_success=True)` now succeeds and created jobs are claimable by item partition key.

Focused tests:
- Python: `pytest -q tests/test_protocol.py tests/test_client.py tests/test_dbos_style_benchmark.py -k 'flow_create_many_compact_payload or enqueue_many_groups or bench_flow_client_uses_enqueue_many'`
  - Result: 5 passed.
- Elixir: `mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs`
  - Result: 118 passed.

KV protocol 30s runs, one native connection, 32 lanes:

```text
SET many pipeline=500:      1,102,580/s, p50 12.781 ms, p99 46.413 ms, errors 0
GET pipeline=1000:          1,616,724/s, p50 19.576 ms, p99 20.764 ms, errors 0
ZRANGE pipeline=1000 0..0:    702,681/s, p50 45.142 ms, p99 51.572 ms, errors 0
```

DBOS-style queued workflow, live, explicit partitions, one native connection, 32 lanes, 16 worker lanes:

```text
flows: 100,000
create_batch_size: 500
claim_batch_size: 1000
claim_partition_batch_size: 16
claim_drain_batches: 4
complete_async_depth: 4

created: 100,000
completed: 100,000
end_to_end: 61,225/s
create: 63,439/s
process: 61,260/s
queue latency p50: 19.080 ms
queue latency p95: 47.652 ms
queue latency p99: 58.167 ms
claim calls: 211
empty claims: 0
avg claim batch: 473.934
max claim batch: 500
```

Notes:
- The suspected `ZRANGE` hang was not reproduced on a clean server. The actual issue found in this pass was native mixed `FLOW.CREATE_MANY` correctness plus benchmark explicit partition batching.
- Before the fix, the 1k/100k DBOS explicit native benchmark could hang because the benchmark used mixed per-item partition create-many through the native path.
- After the fix, the same shape completes consistently.

Additional DBOS comparison after mixed create_many fix:

```text
100k live explicit, workers=16:      61,225/s e2e, create 63,439/s, process 61,260/s, p99 queue 58.167 ms
100k live auto, workers=16:          47,809/s e2e, create 49,127/s, process 47,878/s, p99 queue 101.235 ms
100k preloaded explicit, workers=16: 42,713/s e2e sequential, create 75,457/s, process 98,429/s
```

Read:
- Explicit partition mode is currently faster than auto for this benchmark because auto spreads across 256 buckets and fragments claim batches.
- Preloaded shows process path can exceed 98k/s; live mode is limited by durable create/complete contention and queue scheduling overlap, not by `ZRANGE` or native frame decode.

## 2026-06-12 - compact integer-list pipeline responses

Environment:

- Source server from `/Users/yoavgea/repos/ferricstore`
- Protocol port: `ferric://127.0.0.1:17388`
- `MIX_ENV=prod`
- `FERRICSTORE_NATIVE_ENABLED=true`
- `FERRICSTORE_SHARD_COUNT=16`
- `FERRICSTORE_DATA_DIR=/tmp/ferricstore-native-clean`
- `ERL_FLAGS='+sbwt none +sbwtdcpu none +sbwtdio none'`
- Clean data dir before server restart

30s KV protocol baselines using preset benchmark shapes:

| command | mode | pipeline | lanes | inflight batches | requests/s | p50 batch ms | p99 batch ms | errors |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SET | many/MSET same-value | 500 | 64 | 64 | 1,690,325 | 18.679 | 28.860 | 0 |
| GET | many/MGET | 1000 | 64 | 64 | 2,604,868 | 24.554 | 26.501 | 0 |

Before compact integer-list response marker, 5s data-structure write sweep:

| command | requests/s | p50 batch ms | p99 batch ms | errors |
| --- | ---: | ---: | ---: | ---: |
| HSET | 409,558 | 19.251 | 32.093 | 0 |
| LPUSH | 347,264 | 22.802 | 33.012 | 0 |
| SADD | 392,409 | 19.962 | 28.232 | 0 |
| ZADD | 355,698 | 20.917 | 34.799 | 0 |

Change:

- Added protocol compact integer-list response marker `0x88` for values-only pipeline responses.
- Server uses it when compact pipeline values are all integers.
- Python protocol decodes it to normal `list[int]` results.
- User-visible command results stay unchanged.

Focused tests:

```bash
cd /Users/yoavgea/repos/ferricstore
mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --only test
# 120 tests, 0 failures

cd /Users/yoavgea/repos/ferricstore-python
pytest -q tests/test_protocol.py -k 'integer_list or compact_binary_map_list_as_pipeline_values'
# 2 passed, 88 deselected
```

After compact integer-list response marker, 5s data-structure write sweep:

| command | requests/s | p50 batch ms | p99 batch ms | errors | delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| HSET | 417,827 | 18.433 | 32.847 | 0 | +2.0% |
| LPUSH | 399,388 | 19.255 | 32.732 | 0 | +15.0% |
| SADD | 468,876 | 16.557 | 25.113 | 0 | +19.5% |
| ZADD | 400,972 | 17.996 | 31.216 | 0 | +12.7% |

SDK smoke:

```text
execute_batch([HSET,HSET,SADD,SADD,ZADD,ZADD]) -> [1, 0, 1, 0, 1, 0]
```

Read/data-structure sweep showed no current `ZRANGE` hang. Bounded native `ZRANGE` remained healthy in both direct checks and pipelined benchmark runs.

## 2026-06-12 - Flow protocol sweep and Python Flow record decode cut

Environment same as previous section unless noted.

Flow protocol command sweep, one protocol connection, `protocol_lanes=64`, `inflight_batches=32`, `batch_size=500`, `flows=10000`, `partitions=16`:

| operation | items/s | p50 batch ms | p99 batch ms | errors | note |
| --- | ---: | ---: | ---: | ---: | --- |
| create-many | 86,966 | 111.537 | 114.873 | 0 | durable create write path |
| claim-due | 249,749 | 38.464 | 39.518 | 0 | multiplexed claim |
| complete-many | 192,708 | 46.027 | 50.174 | 0 | terminal write path |
| transition-many | 147,610 | 57.628 | 64.335 | 0 | transition write path |
| retry-many | 170,656 | 53.076 | 56.617 | 0 | retry write path |
| fail-many | 185,375 | 48.342 | 52.223 | 0 | terminal write path |
| cancel-many | 237,509 | 34.840 | 39.875 | 0 | cancel write path |
| value-put-ok | 482,730 | 32.509 | 49.107 | 0 | value ref write, ok-only return |
| value-mget | 1,991,188 | 7.996 | 8.738 | 0 | value ref read path |
| start-and-claim | 61,010 | 112.274 | 132.863 | 0 | fused create+claim path |
| flow-get | 100,686 | 155.884 | 220.183 | 0 | before Python decoder cut |
| flow-get-meta | 144,375 | 108.965 | 141.296 | 0 | before Python decoder cut |
| flow-history | 467,255 | 33.967 | 36.521 | 0 | hot history read |
| flow-list | 72,959 | 130.974 | 136.659 | 0 | query/list path |
| signal | 188,609 | 21.434 | 41.686 | 0 | signal write path |
| step | 84,886 | 90.191 | 96.102 | 0 | step path |

Finding:

- `FLOW.GET` full records decode 28 compact fields per record.
- `FLOW.GET ... RETURN META` decodes 18 compact fields per record.
- Python profile showed most time in `_read_custom_flow_record` and `_decode_flow_record_value_at`.

Change:

- Cached Flow record field-key length in the Python decoder.
- Localized the Flow record value decode function inside the hot loop.
- Replaced generic `struct.unpack_from` calls with cached `_COMPACT_I64` / `_COMPACT_F64` unpackers.
- Added direct empty list/map fast paths inside compact Flow record values.

Focused tests:

```bash
cd /Users/yoavgea/repos/ferricstore-python
pytest -q tests/test_protocol.py -k 'compact_flow_record or flow_get_meta or partitioned_flow_get or integer_list'
# 7 passed, 83 deselected
```

After Python decoder cut, same 5s shape:

| operation | before items/s | after items/s | delta | p50 batch ms | p99 batch ms | errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| flow-get | 100,686 | 113,741 | +13.0% | 140.260 | 184.576 | 0 |
| flow-get-meta | 144,375 | 154,569 | +7.1% | 103.164 | 120.488 | 0 |

Remaining Flow protocol bottlenecks:

- `start-and-claim` and `step` are slower write/fused paths.
- `flow-list` is query/list path limited.
- `flow-get` remains client-CPU heavy because full records contain many fields; bigger gains require a smaller default response shape or native/Rust/Python-extension decode, not just Python loop cleanup.

## 2026-06-12 - compact claim-jobs pipeline values for worker paths

Finding:

- `FLOW.START_AND_CLAIM ... RETURN JOBS_COMPACT` and `FLOW.STEP_CONTINUE` worker paths were returning job tuples through generic pipeline value encoding.
- Python profile showed heavy `decode_value` / `_decode_binary` time even though the command was logically job-only.
- The server already had compact claim-jobs encoding for `FLOW.CLAIM_DUE`; it was not used for pipeline values.

Change:

- Server `compact_pipeline_values_payload/1` now emits existing compact claim-jobs payload marker `0x80` when pipeline values are job tuples.
- Python protocol now decodes marker `0x80` directly for pipeline responses.
- This preserves SDK-visible job tuple shape: `[id, partition_key, lease_token, fencing_token]`.

Focused tests:

```bash
cd /Users/yoavgea/repos/ferricstore
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs --only test
# 72 tests, 0 failures

cd /Users/yoavgea/repos/ferricstore-python
pytest -q tests/test_protocol.py -k 'claim_jobs_as_pipeline_values or integer_list or compact_flow_record'
# 5 passed, 86 deselected
```

Targeted benchmark, one protocol connection, `protocol_lanes=64`, `inflight_batches=32`, `batch_size=500`, `flows=20000`, `partitions=16`:

| operation | before items/s | after items/s | delta | p50 batch ms | p99 batch ms | errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| start-and-claim | 61,010 | 159,400 | +161% | 70.268 | 91.277 | 0 |
| step | 84,886 | 142,567 | +68% | 76.128 | 108.941 | 0 |

Interpretation:

- Job-only worker paths are now using compact protocol data instead of generic value decoding.
- Full-record `FLOW.GET` remains slower because it intentionally returns 18-28 fields per record.

DBOS-style queued protocol sanity after compact claim-jobs pipeline values:

Command shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:17388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 1000 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 4 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode blocking \
  --partition-mode explicit \
  --claim-job-only
```

Result:

| metric | value |
| --- | ---: |
| created | 100,000 |
| completed | 100,000 |
| e2e workflows/s | 66,406 |
| create workflows/s | 69,012 |
| process workflows/s | 66,461 |
| queue latency p50 | 24.284 ms |
| queue latency p95 | 36.074 ms |
| queue latency p99 | 40.177 ms |
| claim calls | 212 |
| empty claims | 0 |
| avg claim batch | 471.7 |
| max claim batch | 500 |
| client CPU | 49.5% |

Post-change 30s KV preset rerun after compact Flow response changes:

| command | requests/s | p50 batch ms | p99 batch ms | errors | note |
| --- | ---: | ---: | ---: | ---: | --- |
| SET | 1,356,162 | 22.625 | 37.977 | 0 | clean source server rerun, lower than prior 1.69M local sample |
| GET | 2,146,558 | 29.688 | 33.483 | 0 | clean source server rerun, lower than prior 2.60M local sample |

Notes:

- No errors observed.
- Server idle after run by `top`; no current stuck `ZRANGE` or lane drain behavior.
- Local variance/server dirty state can move these one-client max-throughput samples materially; keep future comparisons tied to clean data-dir and low background load.

## Native ZRANGE stuck-path check

Context: user suspected `ZRANGE` could wedge the server during native protocol benchmarking. Checked current source server on `ferric://127.0.0.1:17388` / RESP `17379`, PID `60578`.

Bounded native protocol benchmark:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17388 \
  --command zrange \
  --test-time 30 \
  --clients 1 \
  --threads 1 \
  --pipeline 1000 \
  --request-mode pipeline \
  --protocol-lanes 32 \
  --key-count 1000 \
  --range-start 0 \
  --range-stop 0 \
  --timeout 5 \
  --warmup \
  --pretty
```

Result:

| command | requests/s | p50 batch ms | p99 batch ms | errors | warmed keys |
| --- | ---: | ---: | ---: | ---: | ---: |
| ZRANGE 0 0 | 696,648 | 91.305 | 102.316 | 0 | 1,000 |

Large full-range materialization stress:

- One sorted set with `20,000` members.
- Repeated native `ZRANGE key 0 -1` for `5s`.
- Completed `29` full-range calls, `580,000` members returned.
- Max call latency: `181.94ms`.
- Server sampled idle after run: `0% CPU`, about `725MB` memory.

Interpretation:

- Current source server does not reproduce a server-side `ZRANGE` infinite loop or lane drain wedge.
- Full-range `ZRANGE 0 -1` is naturally expensive because it materializes the whole sorted set; use bounded ranges for throughput tests.
- If a future run appears stuck, first distinguish client decode/backpressure from BEAM server health with `PING`, `top`, and a bounded raw RESP `ZRANGE` probe.

## FLOW.LIST RETURN META native response shaping

Change:

- Normalize native `return` option values case-insensitively (`RETURN META` -> `:meta`).
- Add `FLOW.LIST RETURN META` response shaping through the native command path and Flow read pipeline helper.
- Add `flow-list-meta` operation to the protocol Flow command benchmark.

Correctness check:

- Full `FLOW.LIST` response: `28` fields.
- `FLOW.LIST RETURN META` response: `18` fields.
- Heavy/non-meta fields like `child_groups`, `history_*`, `retention_ttl_ms`, lineage/correlation fields are omitted.

Benchmark shape, fresh source server, one native connection:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:17388 \
  --operation flow-list \
  --flows 20000 \
  --batch-size 500 \
  --read-duration 5 \
  --protocol-lanes 64 \
  --inflight-batches 32 \
  --partitions 16 \
  --pretty
```

| operation | items/s | p50 batch ms | p99 batch ms | errors | fields/record |
| --- | ---: | ---: | ---: | ---: | ---: |
| flow-list | 118,074 | 136.915 | 138.794 | 0 | 28 |
| flow-list-meta | 179,873 | 92.906 | 93.480 | 0 | 18 |

Interpretation:

- `RETURN META` is a real protocol/read optimization when callers do not need full Flow record context.
- Default `FLOW.LIST` remains full-record compatible.

Follow-up correctness fix:

- Direct native `FLOW.GET RETURN META` previously failed with `ERR flow return option is not supported` because only compact pipeline `FLOW.GET` handled meta mode.
- Direct native `FLOW.GET` now strips `return` before calling the embedded API and shapes the response in the native layer, same as `FLOW.LIST`.
- Focused tests:
  - `mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs` -> `74 tests, 0 failures`
  - `pytest -q tests/test_protocol.py -k 'flow_list_return_meta or claim_jobs_as_pipeline_values or integer_list or compact_flow_record'` -> `6 passed, 86 deselected`

## 2026-06-12 - compact binary-list-list decoder fast path

Context:
- Source server from current worktree, 4 shards, native port 17388.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`, 3s quick directional run.
- Change: Python protocol decoder inlines compact binary-list-list parsing and fast-paths singleton/empty inner lists.

Before quick scan on same 4-shard shape:
- HMGET: 701,501/s, p50 90.381 ms, p99 97.812 ms
- LRANGE: 793,278/s, p50 79.907 ms, p99 87.529 ms
- ZRANGE: 737,341/s, p50 86.395 ms, p99 93.998 ms
- SMEMBERS: 987,547/s, p50 64.219 ms, p99 73.211 ms

After:
- HMGET: 830,328/s, p50 76.719 ms, p99 82.236 ms
- LRANGE: 957,436/s, p50 66.338 ms, p99 73.160 ms
- ZRANGE: 857,438/s, p50 73.869 ms, p99 82.953 ms
- SMEMBERS: 1,262,922/s, p50 50.291 ms, p99 56.474 ms

Result:
- Directional win across all compact binary-list-list response paths.
- Server did not wedge; ZRANGE bounded and full-range direct checks completed normally.

## 2026-06-12 - protocol GET/SET 30s sanity after binary-list-list decoder change

Context:
- Source server from current worktree, 4 shards, native port 17388.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`, 30s run.
- Client Python process is CPU saturated, so these are local-current sanity numbers, not publication baselines.

Results:
- GET: 1,621,556/s, p50 39.288 ms, p99 41.000 ms, errors 0
- SET: 1,972,667/s, p50 32.236 ms, p99 34.503 ms, errors 0

## 2026-06-12 - compact binary-map-list decoder fast path

Context:
- Same source server as prior decoder run: current worktree, 4 shards, native port 17388.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`, 3s quick directional run.
- Change: Python protocol decoder inlines compact binary-map-list parsing for `HGETALL` pipeline responses.

Before quick scan on same 4-shard shape:
- HGETALL: 825,063/s, p50 77.147 ms, p99 83.681 ms

After:
- HGETALL: 1,077,602/s, p50 59.015 ms, p99 63.719 ms

Result:
- Directional win for compact map-list response path.

## 2026-06-12 - broader 4-shard protocol KV quick scan after decoder fast paths

Context:
- Source server from current worktree, 4 shards, native port 17388.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`, 3s per command.
- All commands completed with errors 0. Client CPU was ~100% on each run.

Results:
- HGET: 1,273,388/s, p50 50.023 ms, p99 51.264 ms
- HMGET: 827,919/s, p50 76.751 ms, p99 84.270 ms
- HGETALL: 1,077,395/s, p50 59.060 ms, p99 64.318 ms
- LRANGE: 950,152/s, p50 66.938 ms, p99 72.858 ms
- SMEMBERS: 1,257,820/s, p50 50.539 ms, p99 56.849 ms
- SISMEMBER: 1,400,615/s, p50 45.395 ms, p99 46.293 ms
- ZRANGE: 863,509/s, p50 73.589 ms, p99 82.351 ms
- ZSCORE: 1,261,710/s, p50 50.232 ms, p99 54.056 ms
- GET: 1,620,020/s, p50 39.307 ms, p99 41.478 ms
- SET: 2,007,833/s, p50 31.482 ms, p99 35.879 ms

## 2026-06-12 - Flow protocol quick scan on current 4-shard source server

Context:
- Source server from current worktree, 4 shards, native port 17388.
- One protocol socket, `--connections 1`, `--protocol-lanes 32`, `--batch-size 500`, `--inflight-batches 32`.
- 10k flow setup where needed, 3s read-duration for duration-style read/value benchmarks.
- All commands completed with errors 0.

Results:
- FLOW.CREATE_MANY: 86,257/s, p50 63.567 ms, p99 110.379 ms
- FLOW.START_AND_CLAIM: 124,344/s, p50 51.087 ms, p99 57.805 ms
- FLOW.GET RETURN META: 155,644/s, p50 100.680 ms, p99 123.058 ms
- FLOW.LIST RETURN META: 121,445/s, p50 81.760 ms, p99 81.996 ms
- FLOW.VALUE.PUT ok response: 788,249/s, p50 19.394 ms, p99 29.469 ms
- FLOW.VALUE.MGET: 2,079,221/s, p50 7.643 ms, p99 8.299 ms
- FLOW.STEP: 94,639/s, p50 88.738 ms, p99 97.862 ms

Follow-up observation:
- Flow read decode remains CPU-heavy for record-map responses.
- Flow value ref mget path is strong and close to raw KV get shape.

## 2026-06-12 - 16-shard source-server protocol GET/SET 30s sanity

Context:
- Fresh source server from current FerricStore worktree.
- `MIX_ENV=prod`, `FERRICSTORE_SHARD_COUNT=16`, native port 17488, clean tmp data dir.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`, 30s run.
- Client process is near CPU saturation; these measure current one-socket Python protocol shape.

Results:
- GET: 1,533,786/s, p50 41.506 ms, p99 42.651 ms, errors 0
- SET: 1,867,335/s, p50 33.632 ms, p99 45.044 ms, errors 0

## 2026-06-12 - 16-shard source-server protocol KV quick scan after decoder fast paths

Context:
- Fresh source server from current FerricStore worktree.
- `MIX_ENV=prod`, `FERRICSTORE_SHARD_COUNT=16`, native port 17488, clean tmp data dir.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`, 3s per command.
- All commands completed with errors 0. Client CPU is saturated for read/decode-heavy paths.

Results:
- HGET: 1,262,924/s, p50 50.433 ms, p99 51.795 ms
- HMGET: 799,537/s, p50 79.116 ms, p99 90.346 ms
- HGETALL: 1,056,511/s, p50 60.137 ms, p99 65.524 ms
- LRANGE: 919,971/s, p50 68.636 ms, p99 79.684 ms
- SMEMBERS: 1,190,579/s, p50 53.043 ms, p99 62.831 ms
- SISMEMBER: 1,360,585/s, p50 46.725 ms, p99 48.415 ms
- ZRANGE: 874,744/s, p50 72.446 ms, p99 81.902 ms
- ZSCORE: 1,239,894/s, p50 51.222 ms, p99 52.973 ms
- GET: 1,655,144/s, p50 38.442 ms, p99 39.601 ms
- SET: 1,695,764/s, p50 36.732 ms, p99 47.394 ms

## 2026-06-12 - native compact HGETALL/SMEMBERS no-sort scan

Context:
- Server change: native compact `HGETALL`/`SMEMBERS` use `Router.compound_scan_raw/3`, preserving public `Router.compound_scan/3` sorted semantics for other callers.
- Fresh source server, 16 shards, native port 17488, clean tmp data dir.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`.

Targeted 3s results:
- HGETALL: 1,073,557/s, p50 59.188 ms, p99 63.166 ms, errors 0
- SMEMBERS: 1,267,362/s, p50 50.210 ms, p99 56.593 ms, errors 0

30s sanity:
- GET: 1,613,991/s, p50 39.478 ms, p99 41.384 ms, errors 0
- SET: 1,807,983/s, p50 35.142 ms, p99 44.712 ms, errors 0

Result:
- Small but clear win for unordered native collection reads.
- No observed GET/SET regression in one-socket 16-shard sanity run.

## 2026-06-12 - direct Ra apply path for native compact HSET single

Context:
- Server change: `{:hset_single, key, field, value}` now uses a direct state-machine apply path instead of calling generic Hash command handling inside Ra apply.
- Fresh source server, 16 shards, native port 17488, clean tmp data dir.
- One protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, `--request-mode pipeline`.

Targeted 3s result:
- HSET: 1,254,819/s, p50 49.958 ms, p99 58.112 ms, errors 0

Prior comparable scan:
- HSET: 793,180/s, p50 80.135 ms, p99 83.888 ms

30s sanity:
- GET: 1,635,915/s, p50 38.962 ms, p99 40.907 ms, errors 0
- SET: 1,846,572/s, p50 34.402 ms, p99 41.896 ms, errors 0

Result:
- Large HSET improvement from removing generic command allocation/dispatch in the single-field native pipeline apply path.
- No observed GET/SET regression in one-socket 16-shard sanity run.

## 2026-06-12 - rejected direct SADD single apply experiment

Context:
- Experiment: direct state-machine apply for `{:sadd_single, key, member}` analogous to direct HSET.
- Fresh 16-shard source server, one protocol socket, `--pipeline 1000`, `--protocol-lanes 32`, 3s targeted run.

Result:
- SADD: 1,529,903/s, p50 41.198 ms, p99 44.842 ms, errors 0

Decision:
- Not kept. This is effectively flat versus prior SADD scans (~1.53M/s), while adding extra semantic surface for set type-marker edge cases.
- Reverted to the generic set command path for `sadd_single`.

## 2026-06-12 - compact Flow record atom field-id lookup

Context:
- Server change: compact Flow record encoder recognizes known atom keys directly instead of converting atom keys to strings before field-id lookup.
- Fresh 16-shard source server, native port 17488, clean tmp data dir.
- One protocol socket, `--connections 1`, `--protocol-lanes 32`, `--batch-size 500`, `--inflight-batches 32`.

Flow targeted results:
- FLOW.GET RETURN META: 155,412/s, p50 100.614 ms, p99 119.566 ms, errors 0
- FLOW.LIST RETURN META: 161,909/s, p50 60.963 ms, p99 61.376 ms, errors 0

30s sanity:
- GET: 1,614,944/s, p50 39.458 ms, p99 41.258 ms, errors 0
- SET: 1,765,368/s, p50 35.165 ms, p99 48.600 ms, errors 0

Result:
- Flow GET meta remains client decode / record-map heavy; no major movement.
- Flow LIST meta was better in this short run, but treat as directional due setup/run variance.
- Kept because wire output is identical and it removes avoidable atom conversion in compact Flow encoding.

## Native protocol benchmark harness: RPUSH and multi-member ZRANGE coverage

Change:
- Added `rpush` to `examples/protocol_kv_benchmark.py` so direct list append optimization can be measured symmetrically with `lpush`.
- Added `--zset-members-per-key` for `zrange` warmup so `ZRANGE 0 -1` can stress realistic multi-member sorted-set replies instead of one-member keys only.
- Chunked generated zrange warmup `ZADD` commands by benchmark pipeline size to avoid tripping the native max-command guard during large warmups.

Tests:

```text
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
pytest -q tests/test_protocol_kv_benchmark.py

22 passed in 0.08s
```

Server shape:

```text
source server, 16 shards, ferric://127.0.0.1:17488
clients=1 threads=1 pipeline=1000 request_mode=pipeline protocol_lanes=32 key_count=1000
```

RPUSH, 3 seconds:

```text
requests_per_sec: 987,877/s
p50 batch: 63.299 ms
p95 batch: 71.758 ms
p99 batch: 78.774 ms
errors: 0
```

ZRANGE 0 -1 with 100 members/key, 3 seconds:

```text
zset_members_per_key: 100
requests_per_sec: 56,673/s
p50 batch: 1102.494 ms
p95 batch: 1417.768 ms
p99 batch: 1477.167 ms
errors: 0
note: each pipeline batch returns about 100,000 members, so this measures large reply/backpressure behavior rather than point-range throughput.
```

30-second sanity after the above:

```text
GET: 1,614,126/s, p50 39.437 ms, p99 41.650 ms, errors 0
SET: 1,839,304/s, p50 34.474 ms, p99 43.702 ms, errors 0
```

## Native compact HMGET single-field fast path

Change:
- Compact pipeline `HMGET` now detects the common one-field shape and uses the cheaper `HGET` read path, then wraps the value as `[value]` to preserve HMGET response semantics.
- Missing fields still return `[nil]` and errors still propagate unchanged.

Server tests:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs

74 tests, 0 failures
```

Server shape:

```text
source server, 16 shards, ferric://127.0.0.1:17588
clients=1 threads=1 pipeline=1000 request_mode=pipeline protocol_lanes=32 key_count=1000
```

Before, from same broad sweep before this patch:

```text
HMGET: 562,862/s, p50 112.900 ms, p99 127.545 ms, errors 0
HGET:  855,390/s, p50 74.322 ms, p99 77.782 ms, errors 0
```

After, short repeated samples:

```text
HMGET: 822,681/s, p50 77.342 ms, p99 85.057 ms, errors 0
HMGET: 817,519/s, p50 77.463 ms, p99 84.737 ms, errors 0
HGET:  1,275,600/s, p50 49.938 ms, p99 51.151 ms, errors 0
HGET:  1,282,373/s, p50 49.563 ms, p99 51.293 ms, errors 0
```

30-second HMGET sample after patch:

```text
HMGET: 739,851/s, p50 86.319 ms, p99 98.620 ms, errors 0
```

30-second GET/SET sanity after patch:

```text
GET: 1,643,938/s, p50 38.619 ms, p99 41.405 ms, errors 0
SET: 1,698,056/s, p50 37.734 ms, p99 46.168 ms, errors 0
```

## Rejected native compact HGET direct read experiment

Tried:
- Replaced compact pipeline `HGET` generic Hash command call with direct type-marker + hash-field lookup.
- Routed compact single-field `HMGET` through that direct helper.

Result:
- No stable throughput gain for HGET.
- HMGET was slightly worse than the earlier single-field `Impl.hget` fast path.

Decision:
- Reverted direct HGET helper.
- Kept only the compact HMGET single-field fast path that calls `Impl.hget` and wraps the value as `[value]`.
- Kept extra compact read tests for missing-field/wrongtype behavior.

Final-code samples after restart:

```text
HGET:  1,275,378/s, p50 49.879 ms, p99 51.361 ms, errors 0
HMGET:   820,333/s, p50 77.272 ms, p99 84.932 ms, errors 0
HGET:  1,278,020/s, p50 49.817 ms, p99 50.908 ms, errors 0
HMGET:   815,120/s, p50 77.959 ms, p99 86.819 ms, errors 0
```

Final tests:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs

74 tests, 0 failures
```

Final 30-second sanity after restart:

```text
GET: 1,534,676/s, p50 38.925 ms, p99 76.253 ms, errors 0
SET: 1,719,201/s, p50 34.956 ms, p99 50.841 ms, errors 0
```

## Protocol Flow command sweep and owned value-put default tuning

Flow sweep shape:

```text
source server, 16 shards, ferric://127.0.0.1:17588
flows=5000 partitions=16 connections=1 protocol_lanes=32 inflight_batches=64
```

Short Flow command sweep:

```text
create-many:       101,029/s, p50 47.616 ms, p99 49.379 ms, errors 0
claim-due:          82,509/s, p50 59.220 ms, p99 60.295 ms, errors 0
complete-many:     158,259/s, p50 18.727 ms, p99 29.486 ms, errors 0
transition-many:   171,181/s, p50 23.588 ms, p99 26.034 ms, errors 0
retry-many:        117,868/s, p50 38.725 ms, p99 39.850 ms, errors 0
fail-many:         160,333/s, p50 27.547 ms, p99 29.232 ms, errors 0
cancel-many:       186,171/s, p50 21.459 ms, p99 23.279 ms, errors 0
value-put:         254,975/s, p50 24.805 ms, p99 43.169 ms, errors 0
value-put-ok:      762,864/s, p50 39.848 ms, p99 60.623 ms, errors 0
value-put-owned:    67,060/s, p50 21.091 ms, p99 27.704 ms, errors 0
value-mget:      2,027,772/s, p50 15.709 ms, p99 17.391 ms, errors 0
start-and-claim:   140,074/s, p50 14.725 ms, p99 18.434 ms, errors 0
flow-get:          109,349/s, p50 140.752 ms, p99 181.317 ms, errors 0
flow-get-meta:     149,380/s, p50 105.513 ms, p99 125.979 ms, errors 0
flow-history:      467,155/s, p50 34.142 ms, p99 35.393 ms, errors 0
flow-list:          81,193/s, p50 60.775 ms, p99 61.208 ms, errors 0
flow-list-meta:    116,456/s, p50 30.000 ms, p99 42.616 ms, errors 0
signal:            133,560/s, p50 17.332 ms, p99 20.756 ms, errors 0
step:               95,415/s, p50 27.817 ms, p99 31.004 ms, errors 0
```

Owned value put batch-size sweep:

```text
batch=25:   70,222/s, p50 18.298 ms, p99 32.051 ms, errors 0
batch=50:   85,104/s, p50 26.485 ms, p99 36.429 ms, errors 0
batch=100: 103,556/s, p50 27.166 ms, p99 37.474 ms, errors 0
batch=250:  92,025/s, p50 43.169 ms, p99 48.111 ms, errors 0
```

Change kept:
- `examples/protocol_flow_commands_benchmark.py` now defaults `value-put-owned` batch size to `100` instead of `25`.
- This is benchmark/SDK-shape tuning only; server behavior is unchanged.

Verification:

```text
pytest -q tests/test_protocol_flow_commands_benchmark.py
29 passed in 0.05s
```

Default `value-put-owned` after tuning:

```text
batch_size: 100
items_per_sec: 105,409/s
p50 26.202 ms, p99 37.039 ms, errors 0
```

30-second GET/SET sanity after Flow sweep:

```text
GET: 1,610,749/s, p50 39.565 ms, p99 41.533 ms, errors 0
SET: 1,468,861/s, p50 42.386 ms, p99 64.038 ms, errors 0
```

Note:
- `FLOW.GET META` still decodes the full Flow record then projects meta; further gain likely requires native/partial Flow record decode rather than a small Elixir branch.

## Protocol Flow batch default tuning: step and start-and-claim

Flow tuning shape:

```text
source server, 16 shards, ferric://127.0.0.1:17588
flows=5000 partitions=16 connections=1 protocol_lanes=32 inflight_batches=64
```

Batch-size sweep:

```text
claim-due:
  batch=100:    40,089/s, p50 88.881 ms, p99 123.676 ms, errors 0
  batch=250:    95,049/s, p50 51.299 ms, p99 52.012 ms, errors 0
  batch=500:   129,015/s, p50 37.900 ms, p99 38.443 ms, errors 0
  batch=1000:   83,947/s, p50 58.908 ms, p99 59.338 ms, errors 0

flow-list:
  batch=100:    81,516/s, p50 59.668 ms, p99 60.657 ms, errors 0
  batch=250:   110,272/s, p50 44.672 ms, p99 44.995 ms, errors 0
  batch=500:   110,420/s, p50 37.421 ms, p99 44.988 ms, errors 0
  batch=1000:  105,157/s, p50 32.868 ms, p99 47.425 ms, errors 0

flow-list-meta:
  batch=100:   109,432/s, p50 38.736 ms, p99 45.021 ms, errors 0
  batch=250:   135,625/s, p50 31.770 ms, p99 36.537 ms, errors 0
  batch=500:   126,894/s, p50 39.007 ms, p99 39.165 ms, errors 0
  batch=1000:  135,316/s, p50 36.637 ms, p99 36.752 ms, errors 0

flow-get:
  batch=100:   106,804/s, p50 59.385 ms, p99 76.536 ms, errors 0
  batch=250:   113,750/s, p50 140.799 ms, p99 171.909 ms, errors 0
  batch=500:   115,307/s, p50 261.781 ms, p99 316.794 ms, errors 0
  batch=1000:  112,032/s, p50 519.672 ms, p99 746.618 ms, errors 0

flow-get-meta:
  batch=100:   143,643/s, p50 44.474 ms, p99 64.164 ms, errors 0
  batch=250:   155,299/s, p50 103.275 ms, p99 124.171 ms, errors 0
  batch=500:   155,273/s, p50 197.398 ms, p99 229.277 ms, errors 0
  batch=1000:  158,860/s, p50 364.679 ms, p99 483.815 ms, errors 0

step:
  batch=25:     82,479/s, p50 15.108 ms, p99 23.556 ms, errors 0
  batch=50:    109,105/s, p50 23.385 ms, p99 26.068 ms, errors 0
  batch=100:   116,874/s, p50 23.607 ms, p99 33.888 ms, errors 0
  batch=250:   113,856/s, p50 32.769 ms, p99 37.582 ms, errors 0

start-and-claim:
  batch=25:     96,627/s, p50 14.397 ms, p99 16.578 ms, errors 0
  batch=50:    118,606/s, p50 18.278 ms, p99 23.987 ms, errors 0
  batch=100:   146,151/s, p50 16.976 ms, p99 21.669 ms, errors 0
  batch=250:   165,922/s, p50 15.485 ms, p99 20.028 ms, errors 0
```

Change kept:
- `step` default batch size changed from `50` to `100`.
- `start-and-claim` default batch size changed from `50` to `250`.
- `claim-due` already defaults to the best tested size (`500`), so unchanged.
- Flow read/list defaults were left unchanged because larger batches only gave small throughput gains with materially higher batch latency.

Repeat check for step:

```text
step batch=50:  106,051/s, p50 24.357 ms, p99 28.993 ms, errors 0
step batch=100: 117,876/s, p50 22.759 ms, p99 30.529 ms, errors 0
step batch=50:  105,290/s, p50 24.261 ms, p99 28.250 ms, errors 0
step batch=100: 112,225/s, p50 23.498 ms, p99 37.437 ms, errors 0
```

Verification:

```text
pytest -q tests/test_protocol_flow_commands_benchmark.py
29 passed in 0.05s
```

30-second GET/SET sanity after Flow tuning sweep:

```text
GET: 1,617,752/s, p50 39.391 ms, p99 41.253 ms, errors 0
SET: 1,431,838/s, p50 43.610 ms, p99 70.243 ms, errors 0
```

Note:
- The SET sanity ran after many Flow benchmark writes on the same temp server and had lower client CPU utilization; this tuning changed only Python benchmark defaults, not server write code.

## Protocol KV ZRANGE large-response diagnostic

Shape:

```text
source server, 16 shards, ferric://127.0.0.1:17688
command=zrange request_mode=pipeline pipeline=1000 protocol_lanes=32
key_count=1000 range=0..-1 zset_members_per_key=100
```

Result:

```text
10s run: 55,571/s, p50 batch 1159.015 ms, p99 batch 1379.756 ms, errors 0
BEAM RSS peak observed during run: ~750 MB, then dropped
GET sanity after run: 1,239,778/s, errors 0
```

Interpretation:

```text
pipeline=1000 and range=0..-1 with 100 members/key returns about 100,000 values per pipeline batch.
This is response materialization pressure, not a reproduced server deadlock.
```

Benchmark change:

```text
protocol_kv_benchmark now reports:
response_items_per_request_estimate
response_items_per_batch_estimate
large_response_warning
```

## Protocol KV short command sweep after ZRANGE guard

Shape:

```text
source server, 16 shards, ferric://127.0.0.1:17688
test_time=2s clients=1 threads=1 pipeline=1000 request_mode=pipeline protocol_lanes=32
key_count=1000 value_bytes=16
ZRANGE uses range=0..0 and zset_members_per_key=1
```

Results:

```text
GET:      1,638,231/s, p50 38.885 ms, p99 40.780 ms, errors 0
SET:      1,902,828/s, p50 32.129 ms, p99 49.356 ms, errors 0
HSET:     1,249,596/s, p50 50.432 ms, p99 59.013 ms, errors 0
HGET:     1,256,925/s, p50 50.636 ms, p99 51.857 ms, errors 0
HMGET:      807,568/s, p50 78.484 ms, p99 86.254 ms, errors 0
HGETALL:  1,054,418/s, p50 60.233 ms, p99 67.467 ms, errors 0
RPUSH:    1,000,348/s, p50 63.053 ms, p99 80.064 ms, errors 0
LRANGE:     917,824/s, p50 69.042 ms, p99 77.572 ms, errors 0
SADD:     1,428,050/s, p50 43.888 ms, p99 45.916 ms, errors 0
SMEMBERS: 1,161,807/s, p50 54.325 ms, p99 62.850 ms, errors 0
ZADD:     1,168,617/s, p50 52.959 ms, p99 97.700 ms, errors 0
ZRANGE:     872,987/s, p50 72.396 ms, p99 78.460 ms, errors 0
ZSCORE:   1,193,594/s, p50 53.219 ms, p99 55.468 ms, errors 0
```

Next likely targets:

```text
HMGET single-field response encoding/shape
bounded ZRANGE/LRANGE response encoding
list push/read storage path only after correctness tests, because list position metadata is sensitive
```

## Protocol KV follow-up after HMGET single-field response fast path

Shape:

```text
source server, 16 shards, ferric://127.0.0.1:17688
test_time=5s clients=1 threads=1 pipeline=1000 request_mode=pipeline protocol_lanes=32
key_count=1000 value_bytes=16
```

Focused result:

```text
HMGET: 813,345/s, p50 78.245 ms, p99 85.170 ms, errors 0
HGET:  1,272,957/s, p50 50.009 ms, p99 51.288 ms, errors 0
```

Interpretation:

```text
The compact HMGET single-field response shape did not materially improve throughput versus the prior 807,568/s sample.
This means HMGET is not primarily bottlenecked by nested response decode/shape for this benchmark.
Next HMGET work should target command execution, compact pipeline decode, or avoid HMGET for single-field SDK hot paths in favor of HGET.
```

30-second GET/SET sanity on the same source server:

```text
GET: 1,625,567/s, p50 39.173 ms, p99 41.403 ms, errors 0
SET: 1,835,985/s, p50 34.431 ms, p99 45.684 ms, errors 0
```

Short broad command sweep on the same source server:

```text
HSET:     1,175,365/s, p50 53.688 ms, p99 58.255 ms, errors 0
HGET:     1,244,679/s, p50 51.095 ms, p99 52.432 ms, errors 0
HMGET:      808,620/s, p50 78.625 ms, p99 85.556 ms, errors 0
HGETALL:  1,030,226/s, p50 61.693 ms, p99 66.623 ms, errors 0
RPUSH:      909,360/s, p50 69.552 ms, p99 77.049 ms, errors 0
LRANGE:     896,799/s, p50 70.772 ms, p99 80.212 ms, errors 0
SADD:     1,440,962/s, p50 43.956 ms, p99 46.306 ms, errors 0
SMEMBERS: 1,167,316/s, p50 53.989 ms, p99 61.922 ms, errors 0
ZADD:     1,093,296/s, p50 56.485 ms, p99 98.305 ms, errors 0
ZRANGE:     852,368/s, p50 74.118 ms, p99 82.946 ms, errors 0
ZSCORE:   1,154,280/s, p50 55.202 ms, p99 57.078 ms, errors 0
```

## Native collection response guard for oversized ZRANGE batches

Change:

```text
FerricStore native protocol now has native_max_collection_response_items.
Default: 10,000 returned collection items per native collection pipeline response.
Env override: FERRICSTORE_NATIVE_MAX_COLLECTION_RESPONSE_ITEMS
Set to 0 to disable the guard.
```

Reason:

```text
Deep native pipeline + wide collection ranges can build huge responses.
Example: pipeline=1000 and ZRANGE 0 -1 over 100-member zsets returns about 100,000 values per response batch.
That looked like a server hang, but was response materialization pressure.
```

Clean source server, 16 shards, one native connection, pipeline=1000, protocol_lanes=32.

30-second GET/SET sanity after adding the guard:

```text
GET: 1,634,110/s, p50 38.985 ms, p99 41.200 ms, errors 0
SET: 1,784,496/s, p50 35.223 ms, p99 43.989 ms, errors 0
```

Clean bounded collection/control sweep before oversized pressure:

```text
HGET:     1,271,964/s, p50 50.024 ms, p99 51.619 ms, errors 0
HMGET:      831,670/s, p50 76.194 ms, p99 82.957 ms, errors 0
HGETALL:  1,060,430/s, p50 59.885 ms, p99 65.060 ms, errors 0
LRANGE:     954,131/s, p50 66.314 ms, p99 73.128 ms, errors 0
SMEMBERS: 1,199,996/s, p50 52.905 ms, p99 60.096 ms, errors 0
ZRANGE:     870,560/s, p50 72.916 ms, p99 81.149 ms, errors 0
GET:      1,635,824/s, p50 38.895 ms, p99 40.839 ms, errors 0
SET:      1,806,454/s, p50 34.535 ms, p99 39.927 ms, errors 0
```

Oversized ZRANGE shape with default guard:

```text
command=zrange range=0..-1 zset_members_per_key=100 pipeline=1000
estimated returned items per batch: 100,000
result: 1,199,880 rejected req/s, p50 52.239 ms, p99 55.874 ms, errors 2,428,000 / 2,428,000
```

Interpretation:

```text
Default behavior now fails oversized native collection batches quickly instead of spending about 1s+ per batch materializing giant replies.
Normal bounded GET/SET and one-item collection ranges stayed in the previous performance envelope.
```

## Native Flow command sweep after collection guard

Clean DBOS-style queued benchmark shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 1000 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 4 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode blocking \
  --partition-mode explicit \
  --claim-job-only
```

Result:

```text
created: 100,000
completed: 100,000
e2e: 59,986/s
create: 61,763/s
process: 60,033/s
queue latency p50/p95/p99: 21.405 / 51.006 / 77.113 ms
claim calls: 211
empty claims: 0
avg claim batch: 473.934
max claim batch: 500
```

Flow command micro-sweep, one native connection, 32 lanes, 50k flows, batch=500 unless noted:

```text
create-many:      245,057/s, p50 118.384 ms, p99 137.354 ms, errors 0
claim-due:        309,279/s, p50  73.259 ms, p99 132.697 ms, errors 0
complete-many:    180,941/s, p50 123.786 ms, p99 192.546 ms, errors 0
start-and-claim:  185,806/s, p50 102.834 ms, p99 133.119 ms, errors 0
flow-get-meta:    148,661/s, p50 208.589 ms, p99 257.118 ms, errors 0
flow-history:     470,052/s, p50  67.562 ms, p99  71.727 ms, errors 0
flow-list-meta:    61,670/s, p50 463.739 ms, p99 724.815 ms, errors 0
value-put-ok:     666,453/s, p50  46.517 ms, p99  61.932 ms, errors 0
value-mget:     1,993,358/s, p50  15.924 ms, p99  18.424 ms, errors 0
step:             136,498/s, p50 168.339 ms, p99 209.214 ms, errors 0
```

Step batch-size tuning on a dirty server suggested smaller batches reduce latency:

```text
batch=100:  123,315/s, p50  50.566 ms, p99  71.819 ms
batch=250:   80,949/s, p50 168.168 ms, p99 246.949 ms
batch=500:   86,885/s, p50 306.358 ms, p99 352.384 ms
batch=1000:  78,804/s, p50 393.674 ms, p99 519.025 ms
```

Fresh start-and-claim comparison did not justify changing the default:

```text
batch=100: 181,799/s, p50 32.417 ms, p99 48.974 ms
batch=250: 199,000/s, p50 65.171 ms, p99 71.417 ms
```

Decision:

```text
Keep start-and-claim default batch_size=250 for throughput.
Use batch_size=100 when optimizing for lower tail latency.
Next server-side targets from the sweep: flow-list-meta response/query cost, flow-get-meta hydration/encoding, and step durable write path.
```

## 2026-06-12 native protocol checkpoint: ZRANGE guard and Flow meta decode experiment

Environment: local source FerricStore, `FERRICSTORE_SHARD_COUNT=16`, native port `17688`, one `ferric://` client connection unless noted.

30s KV baseline after ZRANGE guard:

```text
GET pipeline=1000 lanes=64 key_count=100000: 1,625,759/s, p50 batch 39.111 ms, p99 batch 41.607 ms, errors 0
SET pipeline=1000 lanes=64 key_count=100000: 1,815,584/s, p50 batch 35.052 ms, p99 batch 44.290 ms, errors 0
```

ZRANGE stuck check:

```text
ZRANGE 0 0, members/key=100, pipeline=1000: 881,689/s, errors 0
ZRANGE 0 -1, members/key=100, pipeline=1000: rejected by native collection response guard, server remained responsive
Post-rejection GET smoke: 1,631,196/s, errors 0
```

Short KV command sweep, bounded collection reads:

```text
HGET: 1,035,719/s, p99 batch 72.015 ms
HMGET: 626,855/s, p99 batch 112.248 ms
HGETALL: 720,783/s, p99 batch 100.549 ms
LRANGE 0 0: 794,180/s, p99 batch 90.675 ms
SMEMBERS: 762,353/s, p99 batch 94.808 ms
ZRANGE 0 0: 848,470/s, p99 batch 83.305 ms
```

Short Flow command sweep, `flows=20000`, `batch_size=250` unless noted:

```text
FLOW.VALUE.MGET: 1,921,754/s, p99 batch 9.016 ms
FLOW.GET RETURN META: 157,602/s, p99 batch 120.114 ms
FLOW.LIST RETURN META: 135,435/s, p99 batch 129.640 ms
FLOW.STEP_CONTINUE: 151,894/s, p99 batch 82.446 ms
```

Rejected optimization:

```text
Tried Rust/Elixir Flow record meta-only decode for native compact FLOW.GET RETURN META.
Result regressed to ~109k/s from ~155-158k/s on the same benchmark shape.
Change was reverted; keep full record decode + projection until profiling shows a better fused/native response path.
```

## 2026-06-12 native Flow read batch_get hot-path cleanup

Change kept in FerricStore server:

```text
Router.flow_batch_get/3 now keeps the stale-LMDB protection pre-scan, but returns :none when no Flow state keys are expired/deleted.
Hot path avoids allocating an empty MapSet and avoids MapSet.member? checks when no blocked state keys exist.
```

Correctness checks:

```text
mix test apps/ferricstore/test/ferricstore/flow_lmdb_test.exs: 138 tests, 0 failures
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs: 75 tests, 0 failures when run standalone
```

Note: running those two modules in one command exposed existing test isolation/order noise where the server native command test default instance was not ready after the LMDB suite. Standalone native command coverage passed.

Flow read benchmark, local source server, 16 shards, one ferric:// connection:

```text
FLOW.GET RETURN META, flows=20000, batch=250: 154,615/s, p99 batch 127.470 ms
FLOW.GET RETURN META, flows=20000, batch=1000: 159,263/s, p99 batch 444.426 ms
FLOW.LIST RETURN META, flows=20000, batch=1000: 169,134-171,863/s, p99 batch 115.942-117.871 ms
```

Clean 30s KV sanity after the change:

```text
GET pipeline=1000 lanes=64 key_count=100000: 1,628,962/s, p99 batch 41.756 ms, errors 0
SET pipeline=1000 lanes=64 key_count=100000: 1,719,884/s then 1,739,489/s, p99 batch 42.488-48.979 ms, errors 0
```

## ZRANGE response-cap early rejection

Context: native compact `ZRANGE` could still materialize a huge single-key range before the aggregate response guard noticed the over-cap result. That could make the server look stuck for `ZRANGE key 0 -1` on large zsets.

Change: compact `ZRANGE` now passes remaining collection-response budget into the ready-index path and rejects over-cap rank windows before `ZSetIndex.rank_range/5` builds the member list. Bounded positive windows only count first when the requested window exceeds the remaining cap, so normal `ZRANGE key 0 0` stays on the fast path.

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
76 tests, 0 failures
```

Repro checks:

```text
FERRICSTORE_NATIVE_MAX_COLLECTION_RESPONSE_ITEMS=100
ZRANGE 0 -1 over 200 members: returned expected error in ~0.10s, server stayed responsive
GET after rejected ZRANGE: 0 errors
```

Bounded 30s sanity, source server, native protocol:

```text
command: zrange
shape: pipeline=1000, lanes=64, key_count=10000, zset_members_per_key=1, range=0..0
throughput: 826,433/s
p50: 75.916 ms per 1000-command batch
p99: 92.884 ms per 1000-command batch
errors: 0
```

Rejected benchmark shape:

```text
key_count=100000, zset_members_per_key=100
```

This is a 10M-ZADD warmup before reads, so it is not a valid ZRANGE-read regression check and can make the server appear stuck while it is actually processing warmup writes.

Follow-up: the cold/not-ready ZRANGE fallback now also checks `ZCARD` before materializing when the requested window could exceed the remaining response cap. This keeps the hot ready-index path unchanged for bounded reads and closes the fallback materialization hole.

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
76 tests, 0 failures
```

## LRANGE response-cap early rejection

Context: native compact `LRANGE` used the aggregate collection-response guard only after materializing each per-key range. Large single-key ranges such as `LRANGE key 0 -1` could allocate/fetch a response that would be rejected anyway.

Change: compact `LRANGE` now passes remaining collection-response budget into the list read path. Regular list metadata calculates the bounded window before `compound_batch_get`; cold/invalid fallback checks `LLEN` first when the requested window can exceed the remaining cap.

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
77 tests, 0 failures
```

Bounded 30s sanity, source server, native protocol:

```text
command: lrange
shape: pipeline=1000, lanes=64, key_count=10000, range=0..0
throughput: 929,831/s
p50: 68.237 ms per 1000-command batch
p99: 76.486 ms per 1000-command batch
errors: 0
```

KV sanity after patch:

```text
SET 30s, pipeline=1000, key_count=100000: 1,741,842/s, p99 49.322 ms, errors 0
GET 30s, clean server, pipeline=1000, key_count=100000: 1,613,549/s, p99 41.878 ms, errors 0
```

Note: one intermediate GET run on a dirty server after concurrent SET/LRANGE checks measured ~1.25M/s. A fresh standalone rerun recovered to baseline, so that sample was treated as benchmark noise/dirty-server pressure, not a regression.

## ZRANGE clean protocol error propagation

Context: response-cap rejection for compact pipeline collection commands returned the right text through `Commands.execute/3`, but the direct protocol path wrapped it in an internal `no case clause matching` error because `execute_compact_pipeline/4` did not handle `{:error, reason}` from fast paths.

Change: compact pipeline fast path errors now become clean `{:bad_request, reason, state}` responses. Tests assert the exact resource-limit error so internal exception wrappers cannot pass.

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
79 tests, 0 failures
```

Wire repro, source server, native protocol:

```text
FERRICSTORE_NATIVE_MAX_COLLECTION_RESPONSE_ITEMS=100
ZADD 200 members: 20.56 ms
ZRANGE 0 -1: FerricStoreError ERR native collection response item limit exceeded, 0.21 ms
ZRANGE 0 0: returned [[b'm0']], 0.12 ms
PING after rejected ZRANGE: PONG
```

Rejected 5s benchmark shape, source server, native protocol:

```text
command: zrange
shape: pipeline=1000, lanes=64, key_count=1000, zset_members_per_key=200, range=0..-1
requests: 6,307,000
rejected throughput: 1,261,067/s
p50: 50.440 ms per 1000-command batch
p99: 55.423 ms per 1000-command batch
server responsiveness after run: PING OK
```

## LPUSH/RPUSH compact pipeline fallback correctness

Context: compact pipeline `LPUSH`/`RPUSH` had two paths: a grouped same-key path and a generic compact data-write batch path. Multi-key list batches accidentally returned `:fallback` from the same-key clause, which skipped the generic compact data-write path and reached normal pipeline formatting with `:values`; that produced `no function clause matching in FerricstoreServer.Native.Commands.format_pipeline_results/2` and benchmarked as all-batch errors.

Change: multi-key `LPUSH`/`RPUSH` now routes to the generic compact data-write batch path. The same-key grouped path remains because it is faster than generic for one hot list key.

Correctness:

```text
LPUSH same-key small repro: [1, 2, 3]
RPUSH same-key small repro: [1, 2, 3]
LPUSH/RPUSH multi-key execute_batch and submit_batch small repro: [1, 1, 1, 1, 1]
```

Focused tests:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:890 \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:931 \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:1461
3 tests, 0 failures
```

Final 30s benchmark, source server, native protocol, one connection, pipeline=1000, lanes=64:

```text
LPUSH key_count=1:   219,438/s, p50 291.619 ms, p99 306.899 ms, errors 0
RPUSH key_count=1:   214,797/s, p50 297.476 ms, p99 309.187 ms, errors 0
LPUSH key_count=100: 712,376/s, p50 89.328 ms,  p99 106.667 ms, errors 0
RPUSH key_count=100: 636,429/s, p50 100.868 ms, p99 109.606 ms, errors 0
```

Rejected alternative:

```text
Removed grouped same-key path and used generic batch for all list writes.
LPUSH key_count=1: ~99,612/s, p99 699.728 ms
RPUSH key_count=1: ~92,098/s, p99 728.527 ms
```

Decision: keep grouped same-key path, but route multi-key fallback to generic compact batch.

GET/SET 30s sanity after list changes, source server, native protocol:

```text
GET: 1,669,185/s, p50 38.123 ms, p99 40.024 ms, errors 0
SET: 1,749,803/s, p50 36.385 ms, p99 43.337 ms, errors 0
```

## Direct native collection response cap correctness

Context: direct native collection reads bypassed `native_max_collection_response_items`. `ZRANGE 0 -1` over more than the configured cap could materialize a very large response before the client saw anything, making the server look stuck under huge ranges. Compact pipeline paths already had stronger guards; direct opcode paths did not.

Change: direct native collection reads now enforce the same cap. Range reads (`LRANGE`, `ZRANGE`) precheck count/bounds before materializing. Destructive list pops (`LPOP`, `RPOP`) precheck `LLEN` before mutation when requested count can exceed the cap, so rejected pops do not mutate data.

Covered commands:

```text
MGET
HMGET
HGETALL
LPOP/RPOP
LRANGE
SMEMBERS
ZRANGE
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
81 tests, 0 failures
```

Wire smoke, source server, native protocol, `FERRICSTORE_NATIVE_MAX_COLLECTION_RESPONSE_ITEMS=2`:

```text
MGET 3 keys: rejected with ERR native collection response item limit exceeded
HGETALL 3 fields: rejected with ERR native collection response item limit exceeded
LRANGE 0 -1 over 4 items: rejected with ERR native collection response item limit exceeded
LPOP count=3 over 4 items: rejected with ERR native collection response item limit exceeded
SMEMBERS 3 members: rejected with ERR native collection response item limit exceeded
ZRANGE 0 -1 over 3 members: rejected with ERR native collection response item limit exceeded
PING after rejected reads: PONG
LRANGE 0 1 after rejected LPOP: [a, b]
```

Direct ZRANGE precheck repro, source server, default cap `10000`:

```text
ZADD 10050 members: completed
ZRANGE 0 -1: ERR native collection response item limit exceeded, 9.02 ms
ZRANGE 0 9999: returned 10000 items, 55.34 ms
ZRANGE 0 0: returned 1 item, 12.78 ms
PING after rejected ZRANGE: PONG, 0.13 ms
```

## GET/SET 30s sanity after direct collection guard changes

Context: after adding direct native collection response caps for large reads, reran a full 30s native KV sanity gate to check the hot GET/SET path remained healthy.

Benchmark shape:

```text
source server from ferricstore repo
FERRICSTORE_SHARD_COUNT=16
native protocol: ferric://127.0.0.1:17688
one connection
protocol_lanes=64
request_mode=pipeline
pipeline=1000
key_count=100000
value_bytes=16
```

Results:

```text
SET: 1,858,616/s, p50 34.063 ms, p95 36.269 ms, p99 45.134 ms, errors 0
GET: 1,619,853/s, p50 39.262 ms, p95 40.167 ms, p99 40.780 ms, errors 0
```

## SADD/ZADD compact pipeline correctness and 30s benchmarks

Context: native compact pipeline write coverage already existed for typed SADD/ZADD, but compact-values integer-list coverage was weaker than list/hash write coverage. Added focused regression coverage for duplicate-member semantics and compact integer-list response encoding.

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
82 tests, 0 failures
```

Regression behavior:

```text
compact SADD same member twice: [1, 0]
compact ZADD same member with new score: [1, 0]
```

Benchmark shape:

```text
source server from ferricstore repo
FERRICSTORE_SHARD_COUNT=16
native protocol: ferric://127.0.0.1:17688
one connection
protocol_lanes=64
request_mode=pipeline
pipeline=1000
test_time=30s
value_bytes=16
```

Results:

```text
SADD key_count=1:   844,985/s, p50 75.068 ms,  p95 80.032 ms,  p99 82.503 ms,  errors 0
SADD key_count=100: 1,486,996/s, p50 42.660 ms, p95 44.759 ms,  p99 51.446 ms,  errors 0
ZADD key_count=1:   624,615/s, p50 102.004 ms, p95 107.188 ms, p99 109.888 ms, errors 0
ZADD key_count=100: 1,257,585/s, p50 50.478 ms, p95 52.502 ms,  p99 55.107 ms,  errors 0
```

Observation: same-key collection writes are materially slower than distributed-key writes because one logical collection/index becomes the hot object. This is expected and useful to keep visible in benchmark history.

## Hash and bounded collection read 30s benchmarks

Context: after fixing direct native collection response caps, ran the hash and bounded collection-read side of the native protocol matrix. These runs verify no obvious correctness/performance issue in common read-heavy data-structure commands.

Benchmark shape:

```text
source server from ferricstore repo
FERRICSTORE_SHARD_COUNT=16
native protocol: ferric://127.0.0.1:17688
one connection
protocol_lanes=64
request_mode=pipeline
pipeline=1000
key_count=100
test_time=30s
value_bytes=16
read warmup enabled for read commands
bounded range reads: range_start=0, range_stop=0
zrange warmup: zset_members_per_key=1
```

Results:

```text
HSET:    1,271,426/s, p50 49.740 ms, p95 54.039 ms, p99 59.736 ms, errors 0
HGET:    1,220,138/s, p50 52.121 ms, p95 53.900 ms, p99 54.790 ms, errors 0
HMGET:     777,830/s, p50 82.175 ms, p95 87.681 ms, p99 93.042 ms, errors 0
HGETALL: 1,055,110/s, p50 60.372 ms, p95 62.691 ms, p99 64.279 ms, errors 0
LRANGE:    949,458/s, p50 66.804 ms, p95 70.581 ms, p99 74.264 ms, errors 0
SMEMBERS: 1,221,825/s, p50 52.149 ms, p95 56.937 ms, p99 58.883 ms, errors 0
ZRANGE:    865,408/s, p50 73.592 ms, p95 78.053 ms, p99 80.786 ms, errors 0
```

## Flow command protocol benchmark sweep

Context: ran dedicated native Flow command benchmarks after KV/data-structure coverage. These use the high-level protocol Flow command builder and compact Flow wire encodings where available.

Correctness precheck:

```text
pytest -q tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_dbos_benchmark.py
32 passed
```

Benchmark shape unless noted:

```text
source server from ferricstore repo
FERRICSTORE_SHARD_COUNT=16
native protocol: ferric://127.0.0.1:17688
one connection
protocol_lanes=64
batch_size=100
setup_batch_size=100
flows=10000
payload_bytes=16
```

Core write/claim commands:

```text
create-many, partitions=1024:     171,138/s, p50 42.002 ms, p99 43.131 ms, errors 0
claim-due, partitions=1024:           358/s, p50 17829.810 ms, p99 17837.123 ms, errors 0
claim-due, partitions=16:         199,909/s, p50 31.162 ms, p99 32.212 ms, errors 0
claim-due, partitions=64:         161,812/s, p50 34.962 ms, p99 35.685 ms, errors 0
complete-many, partitions=1024:   209,365/s, p50 26.471 ms, p99 30.565 ms, errors 0
transition-many, partitions=1024: 265,420/s, p50 22.124 ms, p99 28.575 ms, errors 0
```

Claim observation:

```text
The bad 1024-partition claim_due number is benchmark/client request shape: every claim request sends 1024 partition keys. With realistic 16-64 partition-key fan-in, claim_due is ~162k-200k/s. Keep 1024 partition fan-in as a stress/pathology case, not the normal hot claim result.
```

Read/value commands, partitions=64, read_duration=30s where supported:

```text
flow-get-meta: 146,667/s,  p50 43.568 ms, p95 49.070 ms, p99 53.493 ms, errors 0
value-put-ok:  293,506/s,  p50 21.461 ms, p95 27.871 ms, p99 33.255 ms, errors 0
value-mget:  1,388,019/s, p50 4.586 ms,  p95 5.296 ms,  p99 5.658 ms,  errors 0
flow-history:  347,224/s, p50 18.414 ms, p95 20.338 ms, p99 21.339 ms, errors 0
flow-list-meta one 10k pass: 44,424/s, p50 148.744 ms, p99 148.989 ms, errors 0
```

Terminal/fused commands, partitions=64:

```text
retry-many:      95,495/s, p50 47.737 ms, p95 80.113 ms,  p99 80.347 ms,  errors 0
fail-many:      152,168/s, p50 34.827 ms, p95 44.534 ms,  p99 44.537 ms,  errors 0
cancel-many:    216,245/s, p50 25.152 ms, p95 29.846 ms,  p99 30.393 ms,  errors 0
start-and-claim: 64,880/s, p50 35.084 ms, p95 107.011 ms, p99 108.430 ms, errors 0
step:           146,136/s, p50 29.688 ms, p95 34.425 ms,  p99 34.474 ms,  errors 0
```

## Native DBOS-style queue sanity after ZRANGE cap check

Source server from current FerricStore worktree, clean temp data dir, 16 shards, native protocol on `ferric://127.0.0.1:17688`.

Command:

```bash
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:17688 --flows 100000
python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:17688 --flows 1000000
```

Results:

```text
100k flows:
  end_to_end_flows_per_sec: 56,580.77/s
  create_flows_per_sec:     58,118.41/s
  process_flows_per_sec:    56,670.41/s
  queue_latency_p50_ms:     17.78
  queue_latency_p99_ms:     144.23
  process_claim_calls:      259
  process_empty_claims:     1

1M flows:
  end_to_end_flows_per_sec: 60,940.81/s
  create_flows_per_sec:     61,163.88/s
  process_flows_per_sec:    60,962.04/s
  queue_latency_p50_ms:     24.65
  queue_latency_p99_ms:     87.02
  process_claim_calls:      2057
  process_empty_claims:     1
```

Read: ZRANGE cap fix did not make the server stuck. DBOS-style native queue has clean completion and almost no empty claims; current ceiling is durable create+complete throughput, not wake churn.

## Native KV 30s guardrail after ZRANGE cap check

Source server from current FerricStore worktree, clean temp data dir, 16 shards, native protocol on `ferric://127.0.0.1:17688`.

Commands:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:17688 --preset set-throughput --key-count 1000000 --value-bytes 32
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:17688 --preset get-throughput --key-count 1000000 --value-bytes 32
```

Results:

```text
SET:
  requests_per_sec: 1,991,914.77/s
  p50 batch:        15.568 ms
  p95 batch:        21.081 ms
  p99 batch:        26.172 ms
  errors:           0

GET:
  requests_per_sec: 2,623,621.30/s
  p50 batch:        24.230 ms
  p95 batch:        25.570 ms
  p99 batch:        27.350 ms
  errors:           0
```

Read: native KV is stable after collection response guards. SET is above recent native source samples. GET is healthy but still below old RESP/memtier GET peak, so future native read optimization should focus on response decode/client CPU and server compact GET reply path.

## Native KV 30s after Python compact KV encoder join optimization

Change:
- Python protocol compact KV request encoders now build payloads with `b"".join(parts)` instead of repeated `bytearray.extend` calls.
- Wire shape unchanged; focused protocol tests pass.

Focused tests:

```text
pytest -q tests/test_protocol.py::test_compact_kv_payloads_keep_exact_wire_shape \
  tests/test_protocol.py::test_protocol_submit_mget_sends_direct_compact_bulk_frame \
  tests/test_protocol.py::test_protocol_submit_mset_same_value_sends_direct_compact_bulk_frame \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_many_mode_uses_bulk_commands \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_many_mode_uses_prebuilt_fast_bulk_methods

5 passed
```

Source server from current FerricStore worktree, clean temp data dir, 16 shards, native protocol on `ferric://127.0.0.1:17688`.

Commands:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:17688 --preset set-throughput --key-count 1000000 --value-bytes 32
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:17688 --preset get-throughput --key-count 1000000 --value-bytes 32
```

Results:

```text
SET:
  requests_per_sec: 2,028,947.99/s
  p50 batch:        14.972 ms
  p95 batch:        21.059 ms
  p99 batch:        28.509 ms
  errors:           0

GET:
  requests_per_sec: 2,765,586.13/s
  p50 batch:        22.936 ms
  p95 batch:        24.665 ms
  p99 batch:        26.633 ms
  errors:           0
```

Comparison to previous clean guardrail:

```text
SET: 1,991,915/s -> 2,028,948/s (+1.9%)
GET: 2,623,621/s -> 2,765,586/s (+5.4%)
```

## Native GET 30s after pre-encoded MGET benchmark payload path

Change:
- Added `ProtocolAdapter.submit_mget_payload(payload)` for already-built compact MGET payloads.
- `protocol_kv_benchmark.py` now uses pre-encoded key fragments for `--request-mode many --command get --prebuild-keys`, avoiding repeated per-key length packing/type checks in the client hot loop.
- Wire semantics unchanged: still sends normal compact MGET requests.

Focused tests:

```text
pytest -q tests/test_protocol.py::test_protocol_submit_mget_payload_sends_preencoded_direct_compact_bulk_frame \
  tests/test_protocol.py::test_compact_kv_payloads_keep_exact_wire_shape \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_many_mode_uses_prebuilt_fast_bulk_methods \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_many_mode_uses_preencoded_mget_payload_when_available

4 passed
```

Source server from current FerricStore worktree, clean temp data dir, 16 shards, native protocol on `ferric://127.0.0.1:17688`.

Command:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:17688 --preset get-throughput --key-count 1000000 --value-bytes 32
```

Result:

```text
GET:
  requests_per_sec: 4,152,191.67/s
  p50 batch:        15.278 ms
  p95 batch:        22.827 ms
  p99 batch:        25.557 ms
  errors:           0
```

Comparison:

```text
Before compact encoder join optimization: 2,623,621/s
After join optimization:                 2,765,586/s
After pre-encoded MGET payload path:     4,152,192/s
```

## Native GET 30s after compact MGET decoder branch/bounds optimization

Change:
- `_try_decode_custom_kv_mget` now uses explicit bounds checks and a value-present-first branch instead of exception-guarded loop structure.
- Return type and semantics unchanged: `list[bytes | None]`.

Validation:

```text
pytest -q tests/test_protocol.py tests/test_protocol_kv_benchmark.py
122 passed
```

Source server from current FerricStore worktree, clean temp data dir, 16 shards, native protocol on `ferric://127.0.0.1:17688`.

Command:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:17688 --preset get-throughput --key-count 1000000 --value-bytes 32
```

Result:

```text
GET:
  requests_per_sec: 4,605,302.85/s
  p50 batch:        13.782 ms
  p95 batch:        20.408 ms
  p99 batch:        22.892 ms
  errors:           0
```

Comparison:

```text
Before compact encoder join optimization: 2,623,621/s
After join optimization:                 2,765,586/s
After pre-encoded MGET payload path:     4,152,192/s
After decoder branch/bounds optimization:4,605,303/s
```

## 2026-06-12 - preencoded MSET payload path

Change:
- Added `ProtocolAdapter.submit_mset_payload(payload)` and pool delegation.
- `protocol_kv_benchmark.py` `set-throughput`/`many`/`prebuild-keys` can now send prebuilt compact MSET payloads directly.
- Added symmetric MSET payload tests next to the existing MGET preencoded path.

Validation:

```text
pytest -q tests/test_protocol.py::test_protocol_submit_mset_payload_sends_preencoded_direct_compact_bulk_frame \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_many_mode_uses_preencoded_mset_payload_when_available \
  tests/test_protocol.py::test_protocol_submit_mget_payload_sends_preencoded_direct_compact_bulk_frame \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_many_mode_uses_preencoded_mget_payload_when_available
4 passed

pytest -q tests/test_protocol.py tests/test_protocol_kv_benchmark.py
124 passed
```

Clean source-server guardrail:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
command: python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:17688 --preset set-throughput --key-count 1000000 --value-bytes 32
SET: 2,057,335.86/s
batch latency p50: 15.025 ms
batch latency p95: 19.840 ms
batch latency p99: 24.803 ms
errors: 0
client CPU: 23.37%
connections: 1
lanes: 64
pipeline: 500
```

## 2026-06-12 - generic compact pipeline payload + HSET/HGET benchmark prebuild

Change:
- Added `ProtocolAdapter.submit_pipeline_payload(payload, count)` and pool delegation.
- `protocol_kv_benchmark.py` can prebuild compact pipeline payloads for `HSET` and `HGET` when `--prebuild-keys` is enabled.
- This avoids Python command tuple/list construction for high-throughput data-structure command benchmarks.

Validation:

```text
pytest -q tests/test_protocol.py::test_protocol_submit_pipeline_payload_sends_preencoded_compact_pipeline_frame \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_hset_payload_when_available \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_hget_payload_when_available
3 passed

pytest -q tests/test_protocol.py tests/test_protocol_kv_benchmark.py
127 passed
```

Clean source-server benchmark shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
common args: --request-mode pipeline --requests 1 --test-time 30 --pipeline 500 --clients 1 --threads 1 --inflight-batches 64 --protocol-lanes 64 --key-count 1000000 --value-bytes 32 --no-warmup
```

HSET:

```text
prebuild true:  1,084,698.19/s, p50 28.657 ms, p99 44.177 ms, client CPU 21.61%, errors 0
prebuild false: 1,068,580.63/s, p50 28.613 ms, p99 40.711 ms, client CPU 91.84%, errors 0
```

HGET:

```text
prebuild true:  2,741,586.74/s, p50 12.223 ms, p99 17.799 ms, client CPU 82.65%, errors 0
prebuild false: 1,232,031.29/s, p50 25.870 ms, p99 26.723 ms, client CPU 102.49%, errors 0
```

Read setup:
- HGET used `--key-prefix protocol-kv:hset` to read keys written by the HSET run on the same isolated server.

Conclusion:
- HSET is server/durable-write bound; preencoding mainly cuts client CPU.
- HGET benefits materially from avoiding Python command tuple construction and raw compact payload rebuild.

## 2026-06-12 - preencoded compact read payloads for data structures

Change:
- Added benchmark preencoded compact payload builders for key-only, two-binary, and range commands.
- Covered fast payload generation for `HGETALL`, `SMEMBERS`, `SISMEMBER`, `ZSCORE`, `LRANGE`, and `ZRANGE`.
- Server code was not changed; this uses existing native compact pipeline modes through `submit_pipeline_payload(payload, count)`.

Validation:

```text
pytest -q tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_hgetall_payload_when_available \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_zscore_payload_when_available \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_zrange_payload_when_available
3 passed

pytest -q tests/test_protocol.py tests/test_protocol_kv_benchmark.py
130 passed
```

Clean source-server benchmark shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
common args: --request-mode pipeline --requests 1 --test-time 30 --pipeline 500 --clients 1 --threads 1 --inflight-batches 64 --protocol-lanes 64 --key-count 1000000 --value-bytes 32
```

HGETALL:

```text
prebuild true:  1,473,840.78/s, p50 23.769 ms, p99 33.916 ms, client CPU 92.28%, errors 0, warmed_keys 1000000
prebuild false: 1,042,072.66/s, p50 30.547 ms, p99 35.016 ms, client CPU 101.69%, errors 0, warmup skipped after first run
```

ZSCORE:

```text
prebuild true:  2,563,075.96/s, p50 12.542 ms, p99 17.474 ms, client CPU 80.17%, errors 0, warmed_keys 1000000
prebuild false: 1,247,154.70/s, p50 25.515 ms, p99 26.070 ms, client CPU 102.54%, errors 0, warmup skipped after first run
```

ZRANGE 0 0, one member per key:

```text
prebuild true:  1,176,047.69/s, p50 27.022 ms, p99 38.081 ms, client CPU 59.45%, errors 0, warmed_keys 1000000
prebuild false:   856,971.37/s, p50 37.205 ms, p99 40.620 ms, client CPU 101.92%, errors 0, warmup skipped after first run
```

Note:
- A parallel no-prebuild baseline attempt was discarded because the three commands contended with each other on the same server.
- The numbers above are from sequential runs only.

## 2026-06-12 - preencoded compact write payloads for data structures

Change:
- Added benchmark preencoded compact payload builders for `SADD`, `ZADD`, and `RPUSH` through the native compact pipeline frame.
- Fixed the preencoded `ZADD` benchmark payload to encode scores as binary float64, matching the SDK compact encoder and server validator.
- Server code was not changed for this benchmark slice; this exercises existing native compact pipeline modes through `submit_pipeline_payload(payload, count)`.

Validation:

```text
pytest -q tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_sadd_payload_when_available \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_zadd_payload_when_available
2 passed

pytest -q tests/test_protocol.py tests/test_protocol_kv_benchmark.py
132 passed
```

Clean source-server benchmark shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
common args: --request-mode pipeline --requests 1 --test-time 30 --pipeline 500 --clients 1 --threads 1 --inflight-batches 64 --protocol-lanes 64 --key-count 1000000 --value-bytes 32 --no-warmup
```

SADD, fresh distinct prefixes:

```text
prebuild true:  1,279,330.13/s, p50 24.554 ms, p99 37.088 ms, client CPU 26.52%, errors 0
prebuild false: 1,082,457.79/s, p50 29.602 ms, p99 37.821 ms, client CPU 99.28%, errors 0
```

ZADD, clean server, each mode first on its own fresh prefix:

```text
prebuild true:  1,018,353.87/s, p50 25.435 ms, p99 223.399 ms, client CPU 27.87%, errors 0
prebuild false:   907,640.10/s, p50 27.092 ms, p99 235.660 ms, client CPU 77.22%, errors 0
```

RPUSH, fresh distinct prefixes:

```text
prebuild true:    793,858.40/s, p50 39.262 ms, p99 58.898 ms, client CPU 15.75%, errors 0
prebuild false:   703,051.37/s, p50 44.933 ms, p99 60.430 ms, client CPU 51.93%, errors 0
```

Discarded attempts:
- Initial preencoded `ZADD` run before the float64 fix returned invalid-payload errors and is not a valid performance sample.
- A dirty `ZADD` baseline after a prior preencoded run collapsed to about 12k/s because it was not clean/apples-to-apples.
- Initial `SADD` order reused duplicate members and was not apples-to-apples; the numbers above use distinct fresh prefixes.

Conclusion:
- Preencoded compact write payloads mainly reduce Python client CPU for durable writes.
- Throughput improves modestly for write-heavy commands because server Ra/WAL/write work remains dominant.
- The optimization is still useful because it leaves more client CPU for real application work and keeps one-socket native benchmarks from becoming Python-encoding bound.

## 2026-06-12 - ZRANGE stuck check

Observation:
- A suspected ZRANGE server hang was investigated against the active native benchmark server on port 17688 and the older default native server on port 6388.
- Both servers answered `PING` before and after ZRANGE checks.

Checks:

```text
Direct ZADD + ZRANGE: ok, returned [a, b]
30s ZRANGE pipeline, 0..0, empty/missing keys: 1,152,631.89/s, p99 34.733 ms, errors 0
Seeded ZRANGE 0..-1, 16 members/key: 229,622.05/s, p99 8.924 ms, errors 0
Seeded ZRANGE 0..-1, 100 members/key: 27,178.89/s, p99 15.437 ms, errors 0
```

Conclusion:
- No active ZRANGE deadlock/hang reproduced.
- Large materialized ranges are naturally response-size bound; they lower throughput but did not block the server.
- If ZRANGE appears stuck again, capture the exact range, `WITHSCORES` usage, pipeline depth, members per key, and whether warmup populated large sorted sets.

## 2026-06-12 - preencoded Flow create-many payloads

Change:
- Added `ProtocolAdapter.submit_flow_many_payload(command, payload, count)` and pool delegation for already-built compact Flow many payloads.
- Added `--prebuild-payloads/--no-prebuild-payloads` to `protocol_flow_commands_benchmark.py`.
- `FLOW.CREATE_MANY` benchmark now builds compact partitioned create-many bytes directly when retention TTL is disabled, avoiding Python tuple construction plus protocol builder reparsing.
- Retention TTL runs still use the existing tuple path because the compact create-many wire shape does not carry retention TTL.

Validation:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py
131 passed
```

Fresh source-server shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
command: python examples/protocol_flow_commands_benchmark.py --url ferric://127.0.0.1:17688 --operation create-many --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0
```

Samples before explicit apples-to-apples flag comparison:

```text
prebuild true: 242,452/s, p50 126.841 ms, p99 141.812 ms, client CPU 8.87%, errors 0
prebuild true: 275,612/s, p50 108.821 ms, p99 124.514 ms, client CPU 10.24%, errors 0
prebuild true: 244,119/s, p50 118.081 ms, p99 130.189 ms, client CPU 9.53%, errors 0
```

Same fresh server, explicit prebuild comparison:

```text
prebuild true:  304,696/s, p50 97.189 ms, p99 108.932 ms, client CPU 12.01%, errors 0
prebuild false: 237,029/s, p50 128.387 ms, p99 142.757 ms, client CPU 16.60%, errors 0
```

Conclusion:
- Direct compact payload building materially helps the Flow producer benchmark when compared apples-to-apples on the same server.
- Client CPU drops and batch latency improves because the benchmark no longer creates command tuples and then reparses them into compact Flow wire bytes.
- Absolute throughput remains server/WARaft/write-path sensitive and short 100k microbench runs are noisy, so future comparisons should use `--prebuild-payloads` vs `--no-prebuild-payloads` explicitly.

## 2026-06-12 - preencoded Flow terminal-many payloads

Change:
- Added direct compact payload generation for benchmark `FLOW.COMPLETE_MANY`, `FLOW.RETRY_MANY`, and `FLOW.FAIL_MANY` when `--prebuild-payloads` is enabled.
- Kept `FLOW.CANCEL_MANY` on the existing tuple/compact path because direct prebuild was slower in repeated samples.
- The implementation uses `ProtocolAdapter.submit_flow_many_payload(...)`, preserving current server protocol and response validation.

Validation:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py
133 passed
```

Fresh source-server shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
common args: --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0
```

COMPLETE_MANY:

```text
prebuild true:  221,701/s, p50 112.026 ms, p99 196.149 ms, client CPU 5.69%, errors 0
prebuild false: 200,469/s, p50 128.366 ms, p99 218.730 ms, client CPU 6.20%, errors 0
```

RETRY_MANY:

```text
prebuild true:  162,737/s, p50 151.844 ms, p99 273.623 ms, client CPU 5.44%, errors 0
prebuild false: 140,561/s, p50 198.941 ms, p99 272.471 ms, client CPU 3.15%, errors 0
```

FAIL_MANY:

```text
prebuild true:  117,932/s, p50 194.239 ms, p99 398.128 ms, client CPU 3.43%, errors 0
prebuild false: 111,293/s, p50 244.358 ms, p99 380.854 ms, client CPU 6.57%, errors 0
```

CANCEL_MANY rejected direct prebuild:

```text
prebuild true attempt 1: 230,788/s, p50 119.666 ms, p99 179.240 ms, client CPU 5.80%, errors 0
prebuild false:         265,864/s, p50 106.536 ms, p99 132.764 ms, client CPU 7.64%, errors 0
prebuild true attempt 2: 183,529/s, p50 148.163 ms, p99 209.153 ms, client CPU 5.45%, errors 0
```

Conclusion:
- Direct preencoded payloads help claimed terminal/retry commands, especially batch latency and Python object churn.
- Direct cancel-many payloads are not kept because benchmark evidence showed worse throughput and latency.
- Continue using the existing tuple path for cancel-many until server-side evidence points to a better direct shape.

## 2026-06-12 - rejected direct Flow value-put prebuild

Attempted change:
- Tested direct prebuilt compact pipeline payloads for `FLOW.VALUE.PUT` shared refs and owned refs.
- The path reduced Python client CPU but reduced throughput and worsened latency, so it was rejected.
- Tests now keep value-put on the existing tuple path, where the adapter still compacts to the same server wire protocol internally.

Validation after rejection:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py
135 passed
```

Fresh source-server shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
shared args: --operation value-put-ok --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 128
owned args: --operation value-put-owned --flows 100000 --batch-size 100 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 128
```

Shared `VALUE.PUT RETURN OK_ON_SUCCESS`:

```text
prebuild true attempt 1:  645,168/s, p50 41.368 ms, p99 65.368 ms, client CPU 7.56%, errors 0
prebuild false attempt 1: 770,271/s, p50 37.514 ms, p99 38.479 ms, client CPU 92.28%, errors 0
prebuild true attempt 2:  630,627/s, p50 46.256 ms, p99 57.839 ms, client CPU 8.42%, errors 0
prebuild false attempt 2: 708,679/s, p50 37.827 ms, p99 39.331 ms, client CPU 85.90%, errors 0
```

Owned `VALUE.PUT`:

```text
prebuild true:  129,964/s, p50 46.069 ms, p99 107.845 ms, client CPU 6.31%, errors 0
prebuild false: 135,849/s, p50 47.026 ms, p99 68.223 ms, client CPU 12.38%, errors 0
```

Conclusion:
- Direct prebuild saves Python CPU, but the existing tuple path has better throughput/latency for value-put on this server.
- Keep current value-put behavior for now; revisit only if profiling shows client CPU is the bottleneck for a real application workload.

## 2026-06-12 - ZRANGE large-response guard in benchmark

Concern:
- A deep native ZRANGE benchmark looked like it might have stuck the server.
- Direct probes showed the server was still responsive.

Validation against the running source server on `ferric://127.0.0.1:6388`:

```text
PING: ok ~0.15 ms
SET: ok ~3.59 ms
GET: ok ~0.18 ms
ZRANGE small: ok ~4.28 ms
```

Safe ZRANGE benchmark shape:

```text
command: zrange
pipeline: 100
range: 0..0
members/key: 10
inflight_batches: 16
protocol_lanes: 16
test_time: 2s
```

Result:

```text
~680,626/s, p50 2.277 ms, p99 2.869 ms, errors 0
```

Unsafe shape tested:

```text
command: zrange
pipeline: 1000
range: 0..-1
members/key: 100
estimated returned values/frame: 100,000
```

Before the guard:

```text
completed without deadlock, but every request was rejected by the native collection-response guard
errors: 1,454,000 / 1,454,000
```

Fix:
- `protocol_kv_benchmark.py` now fails fast by default when `pipeline * estimated_zrange_items >= 10,000`.
- Use `--allow-large-response-batches` only when intentionally testing guarded rejection or full collection materialization.

Validation:

```text
pytest -q tests/test_protocol_kv_benchmark.py
34 passed
```

## 2026-06-12 - direct Flow value-mget prebuild kept

Change:
- Added `ProtocolAdapter.submit_flow_value_mget_payload(payload)` for direct compact `FLOW.VALUE.MGET` frames.
- `protocol_flow_commands_benchmark.py --operation value-mget` now uses prebuilt compact payloads when `--prebuild-payloads` is enabled.
- Tuple path remains available with `--no-prebuild-payloads`.

Validation:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
171 passed
```

Fresh source-server shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
args: --operation value-mget --flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 128 --read-duration 30
```

30-second read-duration results:

```text
prebuild true:  4,518,719 value refs/s, p50 7.016 ms,  p99 7.474 ms,  client CPU 107.34%, errors 0
prebuild false: 2,064,474 value refs/s, p50 15.473 ms, p99 16.507 ms, client CPU 103.30%, errors 0
```

Conclusion:
- Keep direct `FLOW.VALUE.MGET` prebuild.
- This is a strong read-side Flow value-ref win: roughly 2.2x throughput and about 55% lower p99 batch latency on the measured shape.

## 2026-06-12 - direct HMGET benchmark prebuild kept

Change:
- `protocol_kv_benchmark.py --command hmget --prebuild-keys` now emits direct compact pipeline payloads using mode `28`.
- HMGET wire shape is key + field-count + field bytes, not the HGET two-binary shape.

Validation:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
172 passed
```

Fresh source-server shape:

```text
server: source, isolated tmp dir, 16 shards, native port 17688
args: --command hmget --request-mode pipeline --pipeline 500 --clients 1 --threads 1 --inflight-batches 64 --protocol-lanes 64 --test-time 30 --key-count 100000 --value-bytes 16
```

30-second results:

```text
prebuild true:  2,236,256/s, p50 14.352 ms, p99 22.268 ms, client CPU 94.71%, errors 0
prebuild false:   818,784/s, p50 38.758 ms, p99 42.271 ms, client CPU 101.66%, errors 0
```

Conclusion:
- Keep HMGET prebuild in the benchmark.
- This is primarily a benchmark/client protocol-path fix: the server already had compact HMGET mode, but the benchmark was not using it.

## 2026-06-12 - Native protocol ZRANGE hang check and pop/remove coverage

- Existing server on `ferric://127.0.0.1:6388` was responsive; no benchmark process was left running.
- Tiny native probes completed: `SET` pipeline and bounded `ZRANGE 0 0` pipeline both returned with `errors=0`.
- The earlier ZRANGE "stuck" shape was caused by an unsafe response explosion: `ZRANGE 0 -1` with large pipeline and many members/key can ask the server/client to materialize very large nested responses per frame.
- `protocol_kv_benchmark.py` now fails fast for unsafe large ZRANGE response batches unless `--allow-large-response-batches` is passed.
- Added benchmark coverage for native `lpop`, `rpop`, `srem`, and `zrem` command modes.
- Bounded sanity checks on existing server completed with `errors=0` for `lpop`, `rpop`, `srem`, and `zrem`.

## 2026-06-12 - Native GET/SET direct compact pipeline fast path

Shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --command <set|get> \
  --request-mode pipeline \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --test-time 30 \
  --key-count 1000000 \
  --value-bytes 16 \
  --prebuild-keys
```

Change:

- `pipeline + --prebuild-keys + GET/SET` now submits preencoded compact PIPELINE payloads directly.
- The direct payload uses values-only compact mode (`0x80 | mode`) so the server returns the same efficient compact response shape as the generic adapter path.
- Generic pipeline mode without `--prebuild-keys` still uses `submit_batch` for normal SDK-like command construction.

Before this change on the same running source server:

- SET: `1,729,199/s`, p50 `17.551 ms`, p99 `32.324 ms`, client CPU `98.46%`
- GET: `1,586,316/s`, p50 `19.841 ms`, p99 `20.457 ms`, client CPU `104.65%`

After this change:

- SET: `1,941,747/s`, p50 `16.024 ms`, p99 `26.678 ms`, client CPU `22.67%`
- GET: `4,127,347/s`, p50 `7.683 ms`, p99 `12.676 ms`, client CPU `105.32%`

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
174 passed
```

## 2026-06-12 - Native hash command sweep after GET/SET fast path

Shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --command <hset|hget> \
  --request-mode pipeline \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --test-time 30 \
  --key-count 100000 \
  --value-bytes 16 \
  --prebuild-keys
```

Results on the same running source server:

- HSET: `1,014,082/s`, p50 `30.815 ms`, p99 `46.714 ms`, client CPU `20.95%`
- HGET: `2,707,318/s`, p50 `12.231 ms`, p99 `15.580 ms`, client CPU `87.40%`

Read:

- HSET is server/durable-write limited under this one-socket shape.
- HGET still has meaningful client-side cost, but is already using compact pipeline payloads.

## 2026-06-12 - Native Flow read sweep and FLOW.GET direct compact payload

Flow read sweep shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation <flow-get-meta|flow-history|flow-list-meta|value-mget> \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --read-duration 30 \
  --prebuild-payloads
```

Before direct `FLOW.GET` payload path:

- FLOW.GET META: `158,250/s`, p50 `200.600 ms`, p99 `242.610 ms`, client CPU `92.68%`
- FLOW.HISTORY: `493,228/s`, p50 `64.528 ms`, p99 `66.584 ms`, client CPU `93.89%`
- FLOW.LIST META: `88,282/s`, p50 `374.818 ms`, p99 `414.293 ms`, client CPU `16.77%`
- FLOW.VALUE.MGET: `4,498,962/s`, p50 `7.040 ms`, p99 `7.803 ms`, client CPU `107.32%`

Change:

- `flow-get` and `flow-get-meta` benchmark paths now build compact `FLOW.GET` pipeline payloads directly when `--prebuild-payloads` is enabled.
- This avoids constructing hundreds of Python command tuples per batch before adapter compaction.
- Server semantics and adapter generic paths are unchanged.

After direct `FLOW.GET` payload path:

- FLOW.GET META: `181,636/s`, p50 `175.196 ms`, p99 `222.038 ms`, client CPU `93.10%`
- FLOW.GET: `121,999/s`, p50 `261.710 ms`, p99 `344.080 ms`, client CPU `92.90%`

Read:

- FLOW.GET META improved about `15%` but remains client decode/object-construction bound.
- Plain FLOW.GET is lower because it returns materialized payload/record data; metadata/value-ref reads are the better default for hot paths.
- FLOW.VALUE.MGET remains near native GET throughput and is the best path for explicit value-ref reads.

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
177 passed
```

## 2026-06-12 - Native Flow write command sweep

Shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation <create-many|complete-many|transition-many|retry-many|fail-many|cancel-many> \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --prebuild-payloads
```

Results:

- FLOW.CREATE_MANY: `190,126/s`, p50 `141.174 ms`, p99 `194.877 ms`, client CPU `7.10%`
- FLOW.COMPLETE_MANY: `176,541/s`, p50 `143.945 ms`, p99 `283.376 ms`, client CPU `5.15%`
- FLOW.TRANSITION_MANY: `170,748/s`, p50 `184.827 ms`, p99 `210.939 ms`, client CPU `7.15%`
- FLOW.RETRY_MANY: `149,281/s`, p50 `181.229 ms`, p99 `287.770 ms`, client CPU `3.88%`
- FLOW.FAIL_MANY: `152,578/s`, p50 `159.994 ms`, p99 `314.596 ms`, client CPU `2.82%`
- FLOW.CANCEL_MANY: `174,491/s`, p50 `198.236 ms`, p99 `212.093 ms`, client CPU `6.56%`

Read:

- Flow writes are server/WAL/apply limited in this shape, not Python/native protocol client limited.
- Further wins here require server-side apply/WAL/index work, not more benchmark-side payload compaction.

## 2026-06-12 - Native FLOW.HISTORY direct compact payload

Shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation flow-history \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --read-duration 30 \
  --prebuild-payloads
```

Change:

- `flow-history` benchmark path now builds compact `FLOW.HISTORY` pipeline payloads directly when `--prebuild-payloads` is enabled.
- This avoids Python tuple command construction before adapter compaction.
- Server semantics and generic adapter paths are unchanged.

Before:

- FLOW.HISTORY: `493,228/s`, p50 `64.528 ms`, p99 `66.584 ms`, client CPU `93.89%`

After:

- FLOW.HISTORY: `570,931/s`, p50 `54.552 ms`, p99 `74.671 ms`, client CPU `77.05%`

Read:

- Throughput improved about `15.8%` and median latency improved.
- Tail latency was worse in this single run, so keep watching p99 in later sweeps.

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
178 passed
```

## 2026-06-12 - Native misc Flow sweep and FLOW.VALUE.PUT direct compact payload

Misc Flow sweep shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation <value-put|value-put-ok|value-put-owned|start-and-claim|signal|step> \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --prebuild-payloads
```

Before direct shared value-put payload path:

- FLOW.VALUE.PUT: `107,715/s`, p50 `285.221 ms`, p99 `403.909 ms`, client CPU `99.86%`
- FLOW.VALUE.PUT RETURN OK_ON_SUCCESS: `475,410/s`, p50 `62.515 ms`, p99 `64.104 ms`, client CPU `93.78%`
- FLOW.VALUE.PUT owned named value: `52,328/s`, p50 `594.062 ms`, p99 `694.969 ms`, client CPU `9.29%`
- FLOW.START_AND_CLAIM: `65,410/s`, p50 `422.708 ms`, p99 `657.470 ms`, client CPU `48.53%`
- FLOW.SIGNAL: `96,772/s`, p50 `280.908 ms`, p99 `328.226 ms`, client CPU `16.58%`
- FLOW.STEP_CONTINUE: `68,391/s`, p50 `385.133 ms`, p99 `580.350 ms`, client CPU `47.23%`

Change:

- `value-put` and `value-put-ok` benchmark paths now build compact shared `FLOW.VALUE.PUT` pipeline payloads directly when `--prebuild-payloads` is enabled.
- Uses existing server compact modes `7` and `15`; no server semantics change.

After direct shared value-put payload path:

- FLOW.VALUE.PUT: `146,409/s`, p50 `182.938 ms`, p99 `290.021 ms`, client CPU `89.70%`
- FLOW.VALUE.PUT RETURN OK_ON_SUCCESS: `487,691/s`, p50 `56.206 ms`, p99 `66.004 ms`, client CPU `5.71%`

Read:

- Shared value-put improved about `36%` and significantly reduced tail latency.
- OK-only value-put throughput is roughly flat but client CPU drops sharply, so it leaves more headroom for realistic mixed client work.
- Owned named value-put/start-and-claim/signal/step are server/apply/Flow-path limited in this shape, not primarily client protocol encoding limited.

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
180 passed
```

## 2026-06-12 - Native START_AND_CLAIM and STEP_CONTINUE direct compact payloads

Shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation <start-and-claim|step> \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --prebuild-payloads
```

Change:

- `start-and-claim` now builds compact `FLOW.START_AND_CLAIM` pipeline payloads directly when `--prebuild-payloads` is enabled.
- `step` now builds compact `FLOW.STEP_CONTINUE` pipeline payloads directly from the setup claim records.
- Uses existing server compact modes `13` and `6`; no server semantics change.

Before:

- FLOW.START_AND_CLAIM: `65,410/s`, p50 `422.708 ms`, p99 `657.470 ms`, client CPU `48.53%`
- FLOW.STEP_CONTINUE: `68,391/s`, p50 `385.133 ms`, p99 `580.350 ms`, client CPU `47.23%`

After:

- FLOW.START_AND_CLAIM: `114,909/s`, p50 `231.402 ms`, p99 `396.701 ms`, client CPU `18.79%`
- FLOW.STEP_CONTINUE: `110,726/s`, p50 `255.575 ms`, p99 `358.528 ms`, client CPU `38.26%`

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
183 passed
```

## 2026-06-12 - Native owned FLOW.VALUE.PUT direct compact payload

Shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation value-put-owned \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --prebuild-payloads
```

Change:

- `value-put-owned` now builds compact named/owned `FLOW.VALUE.PUT` pipeline payloads directly when `--prebuild-payloads` is enabled.
- Uses existing server compact mode `14`; no server semantics change.

Before:

- FLOW.VALUE.PUT owned named value: `52,328/s`, p50 `594.062 ms`, p99 `694.969 ms`, client CPU `9.29%`

After:

- FLOW.VALUE.PUT owned named value: `69,064/s`, p50 `454.520 ms`, p99 `563.479 ms`, client CPU `3.71%`

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
184 passed
```

## 2026-06-12 - Native FLOW.SIGNAL direct compact payload

Shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation signal \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --prebuild-payloads
```

Change:

- `signal` now builds compact `FLOW.SIGNAL` pipeline payloads directly when `--prebuild-payloads` is enabled.
- Uses existing server compact mode `11`; no server semantics change.

Before:

- FLOW.SIGNAL: `96,772/s`, p50 `280.908 ms`, p99 `328.226 ms`, client CPU `16.58%`

After:

- FLOW.SIGNAL: `178,586/s`, p50 `176.157 ms`, p99 `190.399 ms`, client CPU `4.82%`

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
185 passed
```

## 2026-06-12 - Native TRANSITION_MANY and CANCEL_MANY direct compact payloads

Shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --operation <transition-many|cancel-many> \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 128 \
  --prebuild-payloads
```

Change:

- `transition-many` now builds compact `FLOW.TRANSITION_MANY` payloads directly and SDK direct Flow-many submit now allows this command.
- `cancel-many` now builds compact `FLOW.CANCEL_MANY` payloads directly.
- Uses existing server compact request types `0x9C` and `0x9A`; no server semantics change.

Before:

- FLOW.TRANSITION_MANY: `170,748/s`, p50 `184.827 ms`, p99 `210.939 ms`, client CPU `7.15%`
- FLOW.CANCEL_MANY: `174,491/s`, p50 `198.236 ms`, p99 `212.093 ms`, client CPU `6.56%`

After:

- FLOW.TRANSITION_MANY: `172,503/s`, p50 `181.241 ms`, p99 `196.833 ms`, client CPU `4.81%`
- FLOW.CANCEL_MANY: `146,112/s`, p50 `212.609 ms`, p99 `226.961 ms`, client CPU `4.34%`

Read:

- Transition-many is a small latency/client-CPU win, throughput roughly flat.
- Cancel-many direct payload was slower in this single run despite lower client CPU; this needs a retest before deciding whether to keep or revert the cancel-many fast path.

Tests:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
189 passed
```

## 2026-06-12 - CANCEL_MANY direct payload rejected after retest

Retest after the initial direct `FLOW.CANCEL_MANY` payload result:

- Direct payload retest 1: `163,152/s`, p50 `193.161 ms`, p99 `200.970 ms`, client CPU `4.58%`
- Direct payload retest 2: `150,222/s`, p50 `211.661 ms`, p99 `223.764 ms`, client CPU `4.37%`

Decision:

- Reverted only the cancel-many direct payload benchmark path.
- Kept the tuple/generic path because it benchmarks faster for this command shape.

After revert:

- FLOW.CANCEL_MANY: `190,750/s`, p50 `161.094 ms`, p99 `178.521 ms`, client CPU `5.98%`

Tests after revert:

```text
pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py tests/test_protocol_kv_benchmark.py
188 passed
```

## 2026-06-12 - Native ZRANGE guard and LPOP pipeline fast path

Server repo: `/Users/yoavgea/repos/ferricstore`
Python benchmark repo: `/Users/yoavgea/repos/ferricstore-python`

### ZRANGE response guard

Problem:

- Full-range `ZRANGE` inside a large native pipeline could materialize a huge response batch.
- Repro shape: `pipeline=1000`, `zset_members_per_key=200`, `ZRANGE 0 -1` -> about `200,000` returned members in one protocol response.
- Server looked stuck while spending time in native/Bitcask binary allocation and response construction.

Change:

- Native collection responses now default-cap returned collection items at `10,000`.
- `OPTIONS` advertises `limits.max_collection_response_items`.
- Explicit `native_max_collection_response_items = 0` keeps unlimited behavior for users that opt in.

Validation:

- Bounded `ZRANGE 0 0`, `10000` requests, `pipeline=1000`: `924,200/s`, p50 batch `6.776 ms`, p99 batch `8.322 ms`, `0` errors.
- Unsafe `ZRANGE 0 -1`, same shape with `200` members/key: clean rejection, server stayed responsive.

### LPOP pipeline fast path

Problem:

- Native pipeline had hot batch paths for `GET`, `SET`, and several write commands, but not `LPOP/RPOP`.
- `LPOP` fell back to independent per-command execution inside the pipeline.
- Queue-like shape produced high latency and left the server draining durable delete work after the client exited.

Change:

- Emptying list pops now batch-delete list metadata with the popped element tombstones.
- Homogeneous native pipeline `LPOP/RPOP` with default/count `1` now maps to `Router.pipeline_write_batch` using `{:list_op, key, {:lpop | :rpop, 1}}`.
- Counted pops still use the generic path because they can return large collections and need separate response guarding.

Benchmark shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --command lpop \
  --request-mode pipeline \
  --requests 100000 \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 100000 \
  --value-bytes 16 \
  --prebuild-keys
```

Before, quiet server:

- `3,125/s`, p50 batch `9552.582 ms`, p99 batch `9881.681 ms`.

After:

- `217,051/s`, p50 batch `141.129 ms`, p99 batch `145.970 ms`.
- Server CPU returned to idle after the run; no post-client backlog observed.

Focused tests:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs apps/ferricstore/test/ferricstore/store/list_ops_test.exs
104 tests, 0 failures
```

### Rejected compact LPOP/RPOP key-only mode

Tried adding compact key-only native pipeline modes:

```text
31 = LPOP
32 = RPOP
```

Result on same 100k `LPOP` shape:

- Compact mode sample: `106,700/s`, p50 batch `286.710 ms`, p99 batch `316.772 ms`, client CPU `11.19%`.

Decision:

- Rejected and reverted compact pop modes.
- It reduced client CPU, but throughput was worse than the normal native pipeline fast path.
- Kept only the server normal-pipeline `LPOP/RPOP` fast path plus list metadata/delete batching.

Additional kept-path samples after revert:

- `152,843/s`, p50 batch `186.019 ms`, p99 batch `240.051 ms`.
- `115,700/s`, p50 batch `255.927 ms`, p99 batch `304.560 ms`.

Read:

- The short 100k run is noisy because total measured command time is sub-second.
- Even the slower kept-path retests are far above the old fallback result of `3,125/s` and no longer leave server backlog after the client exits.

### SREM/ZREM pipeline delete cleanup fast path

Problem:

- Native pipeline `SREM/ZREM` initially fell back to per-command execution and could leave the server draining delete work.
- A first fast-path attempt was rejected by tests because duplicate removals inside one pipeline returned incorrect counts.
- Correct fast path still performed empty set/zset cleanup with full keydir prefix scans per item, making delete-heavy batches extremely slow.

Correctness fix:

- `SREM/ZREM` native pipeline now uses batch-aware state-machine commands.
- Duplicate removals in the same pipeline see earlier pending deletes.
- Last-member removal still deletes the type marker.
- `ZREM` also queues native sorted-set index deletes.

Performance fix:

- Added `CompoundMemberIndex.any_live?/3,4`.
- Empty collection cleanup now checks the ordered compound-member index and stops on the first live member.
- Pending-deleted members are ignored during the cleanup check.
- The old full keydir prefix count remains only as a fallback if the member index is unavailable.

Focused tests:

```text
mix test apps/ferricstore/test/ferricstore/store/shard/compound_member_index_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
94 tests, 0 failures
```

Benchmark shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --command srem \
  --request-mode pipeline \
  --requests 100000 \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 16 \
  --protocol-lanes 16 \
  --key-count 100000 \
  --value-bytes 16 \
  --prebuild-keys
```

Before cleanup optimization, isolated `SREM`:

- `4,840/s`, p50 batch `1470.734 ms`, p99 batch `4228.061 ms`, `0` errors.

After cleanup optimization, isolated `SREM`:

- `141,460/s`, p50 batch `55.682 ms`, p99 batch `61.469 ms`, `0` errors.

After cleanup optimization, isolated `ZREM`:

- `79,133/s`, p50 batch `98.615 ms`, p99 batch `121.995 ms`, `0` errors.

Read:

- The bottleneck was not wire framing; it was per-delete cleanup scanning the full keydir.
- `ZREM` remains slower than `SREM` because it also mutates the sorted-set index.

### Post-delete-optimization GET/SET 30-second sanity baseline

Purpose:

- Confirm the `SREM/ZREM` cleanup optimization did not regress unrelated native KV hot paths.

GET shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --command get \
  --request-mode pipeline \
  --test-time 30 \
  --requests 100000000 \
  --pipeline 1000 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 100000 \
  --value-bytes 16 \
  --prebuild-keys
```

GET result:

- `4,635,140/s`, p50 batch `13.885 ms`, p99 batch `23.654 ms`, `0` errors.

SET shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --command set \
  --request-mode pipeline \
  --test-time 30 \
  --requests 100000000 \
  --pipeline 1000 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --key-count 1000000 \
  --value-bytes 16 \
  --prebuild-keys
```

SET result:

- `1,899,548/s`, p50 batch `33.156 ms`, p99 batch `53.231 ms`, `0` errors.
- Server had elevated CPU immediately after the 30s SET client exited, then returned near idle after about 30 seconds.

Read:

- GET hot path remained strong.
- SET result is high, but the post-client drain means this sample should be treated as a throughput sanity check, not a final durability/steady-state claim.

### ZADD known-new zset fast index path

Change:

- For `ZADD` creating a new sorted set, the Ra apply path now passes the already-known member and numeric score into the staged zset index op.
- This avoids compound-key member extraction and score-string parsing in the index mutation path for the known-new case.

Focused correctness:

```bash
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
```

Result:

- `87 tests, 0 failures`.

Benchmark shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --command zadd \
  --request-mode pipeline \
  --requests 100000 \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 16 \
  --protocol-lanes 16 \
  --key-count 100000 \
  --key-prefix __native_zadd_fast100 \
  --value-bytes 16 \
  --prebuild-keys
```

ZADD result:

- `113,449/s`, p50 batch `67.616 ms`, p99 batch `76.757 ms`, `0` errors.

Same dataset read checks:

- `ZSCORE`: `878,769/s`, p50 batch `5.022 ms`, p99 batch `18.456 ms`, `0` errors.
- `ZRANGE 0 0`: `379,451/s`, p50 batch `15.188 ms`, p99 batch `46.574 ms`, `0` errors.

Same dataset delete check:

- `ZREM`: `45,064/s`, p50 batch `190.639 ms`, p99 batch `292.134 ms`, `0` errors.

Read:

- `ZADD` improved from the broad-matrix result of `16,281/s`, p99 `540.350 ms`, to `113,449/s`, p99 `76.757 ms` for this isolated shape.
- `ZREM` correctness is intact, but this run is slower than the earlier isolated `79,133/s`, p99 `121.995 ms`; treat delete-side zset cleanup/write pressure as the next optimization target.

Rejected ZREM experiment:

- Tried direct-member staged zset delete to skip compound-key extraction during `ZREM`.
- Focused native command tests passed, but benchmark worsened on fresh refill/delete:
  - Refill `ZADD`: `157,007/s`, p99 batch `51.965 ms`, `0` errors.
  - `ZREM`: `39,290/s`, p99 batch `308.258 ms`, `0` errors.
- Reverted the ZREM direct-member path. Keep investigating ZREM via delete/type-cleanup/WAL pressure instead.

Rejected ZREM count-delta experiment:

- Tried using zset index count plus per-apply batch deltas to avoid live-member scan during empty zset detection.
- Added a correctness test for staged multi-member `ZREM`; kept the test.
- Benchmark did not improve:
  - Refill `ZADD`: `104,372/s`, p99 batch `122.333 ms`, `0` errors.
  - `ZREM`: `43,286/s`, p99 batch `439.242 ms`, `0` errors.
- Reverted the count-delta optimization. Current evidence says `ZREM` pain is more likely durable delete/type-marker write pressure or WAL batching, not just empty-member scan.

### Final-code compact matrix after ZADD fast path

Shape:

```text
100k requests per command, pipeline=500, clients=1, threads=1,
inflight_batches=16, protocol_lanes=16, key_count=100k, one native connection.
Fresh server at start, but commands run sequentially in one matrix, so later write-heavy commands include accumulated storage/background pressure.
```

Results:

```text
hset      334,576/s p50 19.123 ms p99 26.415 ms errors 0
hget      421,340/s p50 11.844 ms p99 30.621 ms errors 0
hmget     404,629/s p50 15.197 ms p99 18.999 ms errors 0
hgetall   279,644/s p50 24.295 ms p99 36.783 ms errors 0
lpush      90,138/s p50 76.200 ms p99 123.254 ms errors 0
rpush      94,905/s p50 72.684 ms p99 102.345 ms errors 0
lrange    355,268/s p50 15.491 ms p99 30.468 ms errors 0
lpop       55,627/s p50 134.268 ms p99 251.314 ms errors 0
rpop       59,842/s p50 126.263 ms p99 170.836 ms errors 0
sadd      125,694/s p50 56.275 ms p99 76.764 ms errors 0
sismember 460,617/s p50 12.376 ms p99 20.378 ms errors 0
smembers  376,539/s p50 17.087 ms p99 20.184 ms errors 0
srem       45,725/s p50 157.645 ms p99 306.864 ms errors 0
zadd       16,662/s p50 473.274 ms p99 501.843 ms errors 0
zscore    431,014/s p50 13.456 ms p99 25.147 ms errors 0
zrange    220,091/s p50 30.262 ms p99 37.005 ms errors 0
zrem       13,998/s p50 565.936 ms p99 657.697 ms errors 0
```

Read:

- Isolated ZADD fast path is fixed, but ZADD collapses in a sequential dirty matrix after many prior writes.
- That points to durable write/delete pressure, WAL/background cleanup, or storage queue contention as the next bottleneck.
- Do not use the dirty-matrix ZADD number alone to judge the fast path; use it to investigate sustained mixed-write behavior.

Rejected ZREM hot metadata cleanup experiment:

- Found likely zset lookup metadata accumulation after deleting last member.
- Tried clearing ready/count metadata synchronously in the staged hot path after durable delete.
- Correctness tests passed, but performance was bad:
  - `ZADD A`: `51,614/s`, p99 batch `419.785 ms`.
  - `ZREM A`: `16,286/s`, p99 batch `1279.640 ms`.
  - `ZADD B`: `39,506/s`, p99 batch `260.347 ms`.
- Reverted. If we fix zset metadata accumulation, it should be a background/guarded cleanup that verifies the key is still empty before deleting metadata, not extra synchronous per-delete hot-path work.

Rejected ZADD combined-index-insert experiment:

- Tried replacing `mark_new_ready_empty + put_new_member` with one combined helper for known-new single-member zsets.
- Correctness tests passed, but isolated benchmark did not improve:
  - `ZADD`: `109,407/s`, p99 batch `99.165 ms`, `0` errors.
- Reverted. Keep the earlier proven optimization: pass known member and numeric score to the staged `new_put` op.

### Compact SREM/ZREM protocol modes and values-only write formatting

Change:

- Added compact native pipeline modes:
  - `31`: `SREM` as `{key, member}`.
  - `32`: `ZREM` as `{key, member}`.
- Python protocol benchmark now emits compact wire payloads for `srem` and `zrem` when using pipeline mode with prebuilt keys.
- Compact data writes with `compact_values=true` now encode values directly from command results instead of building synthetic request metadata and pair lists first.

Focused correctness:

```bash
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
```

Result:

- `89 tests, 0 failures`.

Benchmark shape:

```text
100k requests, pipeline=500, clients=1, threads=1,
inflight_batches=16, protocol_lanes=16, key_count=100k,
one native connection, fresh server per sample group.
```

Before direct values-only formatter, compact delete sample 2:

```text
SREM: 81,841/s, p50 95.505 ms, p99 128.339 ms, errors 0
ZREM: 37,710/s, p50 231.262 ms, p99 273.401 ms, errors 0
```

After direct values-only formatter:

```text
SREM: 82,984/s, p50 86.638 ms, p99 143.837 ms, errors 0
ZREM: 51,198/s, p50 121.833 ms, p99 232.229 ms, errors 0
```

Read:

- ZREM improved materially from the formatter cut.
- SREM did not materially improve; remaining SREM cost is not response formatting.
- Delete commands are still much slower than read commands, so remaining bottleneck is apply/storage/delete/index work.

### ZADD variance check after compact values-only formatter

Context:

- A clean ZADD sample after the compact values-only formatter previously showed only `46,190/s`, which conflicted with earlier `100k+` samples.
- Checked the active native server before rerun:
  - RSS: about `333 MB`.
  - CPU: about `2%` idle.
  - data dir: `8.6 MB`, `161` files.
- This ruled out obvious memory/disk pressure as the cause of the low sample.

Focused rerun against the same idle server:

```text
ZADD: 87,181/s, p50 batch 79.614 ms, p99 batch 296.028 ms, errors 0
```

Correctness checks:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
=> 89 tests, 0 failures

python -m py_compile examples/protocol_kv_benchmark.py
=> ok
```

Decision:

- Keep the compact values-only formatter.
- Treat the earlier `46k/s` clean ZADD sample as benchmark variance until repeated evidence says otherwise.
- ZADD remains noisy and sensitive to durable write/index pressure, so judge it with repeated samples and mixed-write context, not a single short run.

### 30-second native GET/SET baseline after compact formatter

Server shape:

```text
source server, MIX_ENV=prod
FERRICSTORE_SHARD_COUNT=16
native port 17688
one native client connection
pipeline=500
inflight_batches=16
protocol_lanes=16
key_count=1,000,000
binary/prebuilt keys
```

GET, clean server with warmup:

```text
GET: 1,465,931/s, p50 batch 5.029 ms, p99 batch 12.063 ms, errors 0
client CPU: 79.2%
warmed_keys: 1,000,000
```

SET, clean server:

```text
SET: 189,508/s, p50 batch 35.873 ms, p99 batch 134.818 ms, errors 0
client CPU: 7.5%
```

Read:

- GET is still client/protocol-response heavy with one connection; Python client CPU is high.
- SET is server durable apply/storage limited; client CPU is low.
- Native one-socket GET is not comparable to old RESP memtier `800`-connection GET headline; use it as a native protocol one-connection baseline.

### FLOW.LIST benchmark duration loop fix

Problem:

- `flow-list` / `flow-list-meta` accepted `--read-duration`, but the benchmark ignored it and always stopped after one `--flows` pass.
- That made short list samples noisy and made them look like server regressions when setup/query contention varied.

Change:

- Added `run_flow_list_reads(...)` in `protocol_flow_commands_benchmark.py`.
- `flow-list` and `flow-list-meta` now use the existing duration runner when `--read-duration > 0`.
- Added a deterministic unit test with a fake clock proving `FLOW.LIST ... RETURN META` repeats until the deadline.

Tests:

```text
pytest -q tests/test_protocol_flow_commands_benchmark.py
=> 50 passed

python -m py_compile examples/protocol_flow_commands_benchmark.py
=> ok
```

Benchmark shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --operation flow-list-meta \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 0 \
  --read-duration 10 \
  --prebuild-payloads
```

Before fix, same visible args stopped after one pass:

```text
FLOW.LIST META: 68,462/s, p50 440.480 ms, p99 650.727 ms, completed 100,000
```

After fix, true duration loop:

```text
FLOW.LIST META: 87,049/s, p50 362.899 ms, p99 642.689 ms, completed 885,500
```

Read:

- The previous low sample was mostly benchmark-shape noise, not a proven server regression.
- Throughput is back near the earlier recorded `88,282/s` Flow LIST META reference.
- Tail latency remains high because each `FLOW.LIST COUNT 500` returns 500 records and exercises the query/list response path; next optimization would require server-side Flow LIST request/response specialization, not just benchmark loop fixes.

### Compact FLOW.LIST request payload

Problem:

- `FLOW.LIST` native requests still used generic map payload encoding.
- The fixed duration benchmark showed client CPU around `77%`, so request encode/decode overhead was still visible even though the server query/response path remains dominant.

Change:

- Added compact request tag `0x9F` for simple `FLOW.LIST`:
  - `type`
  - optional `state`
  - `count`
  - optional `RETURN META`
- Unsupported filters/options keep the generic map payload path, so no query semantics are dropped.
- Server decodes `0x9F` into the same payload map consumed by the existing Flow LIST implementation.

Focused tests:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
=> 139 tests, 0 failures

pytest -q tests/test_protocol.py tests/test_protocol_flow_commands_benchmark.py
=> 153 passed
```

Benchmark shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:17688 \
  --operation flow-list-meta \
  --flows 100000 \
  --batch-size 500 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 64 \
  --partitions 16 \
  --payload-bytes 0 \
  --read-duration 10 \
  --prebuild-payloads
```

Before compact request payload, after duration-loop fix:

```text
FLOW.LIST META: 87,049/s, p50 362.899 ms, p99 642.689 ms, client CPU 77.18%, errors 0
```

After compact request payload:

```text
FLOW.LIST META: 90,632/s, p50 341.384 ms, p99 684.096 ms, client CPU 71.80%, errors 0
```

Read:

- Throughput improved about `4.1%` and client CPU dropped about `5.4 percentage points`.
- Tail latency did not improve; remaining bottleneck is Flow LIST query/response materialization.
- Keep this change: it is small, strict, covered by fallback tests, and does not touch hot write paths.

### High-shape native GET/SET guard after compact FLOW.LIST work

Purpose:

- Check that compact `FLOW.LIST` request work did not break unrelated native KV paths.
- This is a regression guard, not a final peak claim; local source runs are noisy and SET can include post-client durable drain behavior.

Server shape:

```text
source server, MIX_ENV=prod
FERRICSTORE_SHARD_COUNT=16
native port 17688
clean temp data dir per command
```

Client shape:

```text
one native connection
pipeline=1000
inflight_batches=64
protocol_lanes=64
threads=1
clients=1
binary/prebuilt keys
value_bytes=16
```

GET 30s:

```text
GET: 2,278,462/s, p50 batch 28.282 ms, p99 batch 52.876 ms, errors 0
client CPU: 98.04%
warmed_keys: 100,000
```

SET 30s:

```text
SET: 613,967/s, p50 batch 99.241 ms, p99 batch 231.792 ms, errors 0
client CPU: 10.60%
```

Read:

- GET remains client/protocol CPU-bound in this one-process shape.
- SET is server durable-write limited in this run; lower than prior bursty `1.9M/s` samples, but still no correctness errors.
- The compact Flow LIST change did not touch KV command routing or SET/GET codecs except module load, so this is a guardrail result, not evidence that Flow LIST caused SET variance.

SET repeat on fresh clean server, same shape:

```text
SET: 612,628/s, p50 batch 100.761 ms, p99 batch 158.715 ms, errors 0
client CPU: 11.48%
```

Read:

- The ~613k/s SET number repeated, so treat it as current durable-write behavior on this local source setup.
- The older ~1.9M/s SET sample should be treated as burst/previous-shape history until reproduced; current stable evidence is lower.
- Because client CPU is low, next SET improvement target is server durable batching/apply/storage, not native request encoding.

Post-run server check after SET repeat:

```text
BEAM RSS: ~1.18 GB
BEAM CPU immediately after client exit: ~446%
data dir: 849 MB, 161 files
```

Caveat:

- SET client throughput completed without errors, but the server was still doing work immediately after the client exited.
- Treat this as a pressure/throughput guard, not a clean steady-state durability number.
- A proper sustained SET result needs a drain-aware benchmark or server-side WAL/apply telemetry.

### Native SET quorum metrics snapshot after high-shape guard

Source:

```text
FERRICSTORE.METRICS over native protocol after the high-shape SET guard run.
```

Observed:

```text
avg quorum submit duration: ~62.8 ms
avg submitted commands per quorum submit: ~1,581
avg represented client batches per quorum submit: ~25.3
max single-shard quorum submit duration: ~504 ms
```

Read:

- Native request encoding is not the current SET bottleneck in this shape.
- The server is coalescing many client batches into each shard submit.
- Available exported metrics for this run exposed quorum-submit counters, but not apply/WAL/Bitcask stage counters.
- Next durable-write investigation should add or fix lower-stage visibility before changing WAL/apply batching again.

### WARaft segment projection metrics added for native SET path

Reason:

- Native SET pipeline uses WARaft `put_batch` segment projection.
- That path does not append a separate Bitcask batch; the committed WARaft segment is the durable source and projection updates the hot state.
- Previous metrics therefore showed quorum submit timing, but no apply/Bitcask counters for this path.

Change:

```text
Added Prometheus metric family:

ferricstore_waraft_segment_projection_apply_total
ferricstore_waraft_segment_projection_apply_duration_us_total
ferricstore_waraft_segment_projection_apply_duration_us_max
ferricstore_waraft_segment_projection_apply_batch_size_total
```

Labels:

```text
shard_index
command_shape
result
```

Correctness checks:

```text
mix test apps/ferricstore/test/ferricstore/metrics_test.exs
=> 19 tests, 0 failures

mix test apps/ferricstore/test/ferricstore/raft/waraft_storage_hot_path_guard_test.exs
=> 5 tests, 0 failures
```

Benchmark note:

- A local source/prod server attempt exited normally after startup before the client connected, without a crash log.
- No throughput number is recorded for this change to avoid mixing runner failure with protocol performance.
- Next benchmark should be run against the usual stable server runner/container/release, then inspect the new segment-projection metrics together with quorum-submit metrics.

### Local release-container KV benchmark after segment-projection metrics

Server:

```text
local Docker image built from current tree
image: ferricstore-native-metrics:local
container: ferricstore-native-metrics-bench
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_NATIVE_PORT=6388
FERRICSTORE_SHARD_COUNT=16
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500
host native port: 16388
```

Build correctness:

```text
Dockerfile now copies apps/ferricstore_server/native so native_protocol_nif builds in release image.
docker build -t ferricstore-native-metrics:local .
=> success
```

SET command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --key-count 1000000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

SET result:

```text
requests_per_sec: 850,716/s
requests: 25,548,500
seconds: 30.03
batch p50: 35.576 ms
batch p95: 48.356 ms
batch p99: 74.573 ms
batch max: 99.184 ms
client CPU: 17.44%
errors: 0
shape: one native connection, request_mode=many, pipeline=500, protocol_lanes=64
```

Metrics after SET:

```text
quorum_submit_total: 29,533
avg_quorum_submit_us: 27,019
max_quorum_submit_us: 96,514
avg_quorum_commands: 865
avg_quorum_callers: 27.7

segment_projection_total: 29,533
avg_segment_projection_us: 1,019
max_segment_projection_us: 7,850
avg_segment_projection_records: 865
```

Read:

- Native SET is not bottlenecked on request encoding or segment projection apply.
- Segment projection is about `1.0ms` average out of `27.0ms` average quorum submit.
- Next SET optimization target is commit/WARaft submit batching/sync behavior, not Flow/KV projection apply.

GET command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --key-count 1000000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

GET result:

```text
requests_per_sec: 2,737,816/s
requests: 82,190,000
seconds: 30.02
batch p50: 23.077 ms
batch p95: 33.885 ms
batch p99: 38.278 ms
batch max: 47.461 ms
client CPU: 101.95%
errors: 0
shape: one native connection, request_mode=many, pipeline=1000, protocol_lanes=64
warmed_keys: 1,000,000
```

Read:

- Native GET is client/protocol CPU-bound in this one-process shape.
- Server-side SET path still has room below quorum submit.

### Local release-container DBOS-style Flow benchmark after metrics work

Server:

```text
same local release container as above, restarted with clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
host native port: 16388
```

Command:

```bash
python examples/dbos_style_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --mode queued \
  --queued-shape live \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 500 \
  --worker-api queue \
  --claim-state queued \
  --claim-job-only \
  --complete-batch \
  --independent-many \
  --complete-independent-many \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --server-shards 16 \
  --protocol-wake-hints
```

Result:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 8.351
end_to_end_flows_per_sec: 11,975/s
create_flows_per_sec: 22,280/s
process_flows_per_sec: 11,979/s
client CPU: 19.86%

queue_latency_avg_ms: 307.1
queue_latency_p50_ms: 114.4
queue_latency_p95_ms: 1,278.1
queue_latency_p99_ms: 4,090.4
queue_latency_max_ms: 4,931.1

process_claim_calls: 1,318
process_empty_claims: 157
process_fallback_claims: 212
process_avg_claim_batch: 75.9
process_max_claim_batch: 200
wake_notifications: 256
wake_credits: 100,000
```

Metrics after Flow run:

```text
avg_quorum_submit_us: 46,415
max_quorum_submit_us: 939,806
avg_quorum_commands: 1.10
avg_quorum_callers: 1.10
segment_projection_total: 0
bitcask_append_total: 0
```

Read:

- Correctness is good: all flows completed with no duplicate completions.
- This local Docker Flow result is much lower than previous optimized local-source DBOS numbers.
- Quorum batch metrics count Ra commands, not flow items inside `FLOW.CREATE_MANY`/terminal many, so `avg_quorum_commands=1.10` does not mean one flow per submit.
- Queue latency and claim fallback/empty claims are the visible Flow bottleneck here, not native protocol client CPU.
- Need Flow-specific item-count/write-stage telemetry to diagnose many-command throughput the same way segment-projection telemetry now diagnoses SET.

## Native Flow DBOS-style, Flow item telemetry complete

Environment:

```text
local release container rebuilt from source branch
clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
host native port: 16388
```

Command:

```bash
python examples/dbos_style_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --mode queued \
  --queued-shape live \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 100 \
  --create-batch-size 500 \
  --worker-api queue \
  --claim-state queued \
  --claim-job-only \
  --complete-batch \
  --independent-many \
  --complete-independent-many \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --server-shards 16 \
  --protocol-wake-hints
```

Result:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 6.031
end_to_end_flows_per_sec: 16,581/s
create_flows_per_sec: 23,780/s
process_flows_per_sec: 16,590/s
client CPU: 25.58%

queue_latency_avg_ms: 220.2
queue_latency_p50_ms: 120.1
queue_latency_p95_ms: 850.3
queue_latency_p99_ms: 1,427.2
queue_latency_max_ms: 3,945.0

process_claim_calls: 1,232
process_empty_claims: 59
process_fallback_claims: 105
process_avg_claim_batch: 81.2
process_max_claim_batch: 200
wake_notifications: 256
wake_credits: 100,000
```

Aggregated server metrics:

```text
FLOW APPLY
flow_create_pipeline_batch: applies=256 items=100,000 results=100,000 avg_apply_ms=9.344 avg_items_per_apply=390.6 max_ms=261.247
flow_claim_due: applies=1,146 items=1,146 results=100,000 avg_apply_ms=2.483 avg_results_per_apply=87.3 max_ms=114.481
flow_terminal_pipeline_batch: applies=1,146 items=100,000 results=100,000 avg_apply_ms=2.571 avg_items_per_apply=87.3 max_ms=110.879

QUORUM
submit_total=1,264
avg_submit_ms=43.872
avg_commands_per_submit=1.11
max_submit_ms=5,880.079
apply_total=2,410
avg_apply_ms=3.537
max_apply_ms=347.484
wal_sync_total=0
```

Read:

- Flow item telemetry now reconciles to the benchmark totals: 100k creates, 100k claimed items, 100k terminal writes.
- Flow apply CPU is not the main wall-clock bottleneck in this local Docker run. Apply averages are small relative to quorum submit latency.
- Create batching is good: ~391 created items per create apply.
- Claim and terminal batches are smaller: ~87 claimed/completed items per apply. This is the next likely optimization target for Flow throughput and queue latency.
- Quorum submit latency is still the largest measured server-side number. Need deeper WARaft submit/commit metrics, or tune adaptive commit batching based on queue wait and batch size.

## Native Flow DBOS-style, larger claim/worker batch experiment

Environment:

```text
same local release container/image as previous section
clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
host native port: 16388
```

Changed knobs versus previous run:

```text
claim_batch_size: 200
worker_capacity: 200
complete_async_depth: 8
claim_drain_batches: 3
```

Result:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 4.783
end_to_end_flows_per_sec: 20,908/s
create_flows_per_sec: 26,658/s
process_flows_per_sec: 20,921/s
client CPU: 28.95%

queue_latency_avg_ms: 247.9
queue_latency_p50_ms: 130.0
queue_latency_p95_ms: 962.5
queue_latency_p99_ms: 1,443.2
queue_latency_max_ms: 3,718.5

process_claim_calls: 702
process_empty_claims: 78
process_fallback_claims: 61
process_avg_claim_batch: 142.5
process_max_claim_batch: 329
wake_notifications: 256
wake_credits: 100,000
```

Aggregated server metrics:

```text
FLOW APPLY
flow_create_pipeline_batch: applies=256 items=100,000 results=100,000 avg_apply_ms=10.701 avg_items_per_apply=390.6 max_ms=276.806
flow_claim_due: applies=611 items=611 results=100,000 avg_apply_ms=4.399 avg_results_per_apply=163.7 max_ms=188.919
flow_terminal_pipeline_batch: applies=611 items=100,000 results=100,000 avg_apply_ms=4.922 avg_items_per_apply=163.7 max_ms=178.404

QUORUM
submit_total=837
avg_submit_ms=41.431
avg_commands_per_submit=1.04
max_submit_ms=4,327.109
apply_total=1,448
avg_apply_ms=5.749
max_apply_ms=335.263
wal_sync_total=0
```

Read:

- Larger worker/claim capacity improved e2e from ~16.6k/s to ~20.9k/s.
- The improvement came from denser claim/terminal batches: ~164 items/apply instead of ~87.
- Create batching stayed at ~391 items/apply.
- Quorum submit latency remains high relative to apply CPU. This still points to commit/batching/scheduling, not protocol frame decode.

## Native Flow DBOS-style, claim/worker batch 500

Environment:

```text
same local release container/image as previous section
clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
host native port: 16388
```

Changed knobs versus baseline:

```text
claim_batch_size: 500
worker_capacity: 500
complete_async_depth: 16
claim_drain_batches: 3
```

Result:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 3.958
end_to_end_flows_per_sec: 25,267/s
create_flows_per_sec: 28,381/s
process_flows_per_sec: 25,287/s
client CPU: 32.96%

queue_latency_avg_ms: 78.7
queue_latency_p50_ms: 55.9
queue_latency_p95_ms: 190.7
queue_latency_p99_ms: 545.6
queue_latency_max_ms: 790.3

process_claim_calls: 449
process_empty_claims: 17
process_fallback_claims: 32
process_avg_claim_batch: 222.7
process_max_claim_batch: 500
wake_notifications: 256
wake_credits: 100,000
```

Aggregated server metrics:

```text
FLOW APPLY
flow_create_pipeline_batch: applies=256 items=100,000 results=100,000 avg_apply_ms=10.732 avg_items_per_apply=390.6 max_ms=286.994
flow_claim_due: applies=417 items=417 results=100,000 avg_apply_ms=6.202 avg_results_per_apply=239.8 max_ms=234.495
flow_terminal_pipeline_batch: applies=417 items=100,000 results=100,000 avg_apply_ms=7.623 avg_items_per_apply=239.8 max_ms=298.731

QUORUM
submit_total=649
avg_submit_ms=43.663
avg_commands_per_submit=1.04
max_submit_ms=4,504.307
apply_total=1,066
avg_apply_ms=7.885
max_apply_ms=371.068
wal_sync_total=0
```

Read:

- Batch 500 improved e2e to ~25.3k/s and lowered queue p99 to ~546ms.
- Claim/terminal density increased to ~240 result items/apply.
- The system is trading larger individual apply work for fewer quorum/apply cycles, and wall-clock improves.

## Native Flow DBOS-style, claim/worker batch 1000

Environment:

```text
same local release container/image as previous section
clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
host native port: 16388
```

Changed knobs versus baseline:

```text
claim_batch_size: 1000
worker_capacity: 1000
complete_async_depth: 16
claim_drain_batches: 3
```

Result:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 3.897
end_to_end_flows_per_sec: 25,661/s
create_flows_per_sec: 28,649/s
process_flows_per_sec: 25,680/s
client CPU: 31.65%

queue_latency_avg_ms: 79.6
queue_latency_p50_ms: 37.5
queue_latency_p95_ms: 157.5
queue_latency_p99_ms: 1,113.8
queue_latency_max_ms: 3,282.0

process_claim_calls: 329
process_empty_claims: 18
process_fallback_claims: 29
process_avg_claim_batch: 304.0
process_max_claim_batch: 1000
wake_notifications: 256
wake_credits: 100,000
```

Aggregated server metrics:

```text
FLOW APPLY
flow_create_pipeline_batch: applies=256 items=100,000 results=100,000 avg_apply_ms=11.042 avg_items_per_apply=390.6 max_ms=307.962
flow_claim_due: applies=298 items=298 results=100,000 avg_apply_ms=8.671 avg_results_per_apply=335.6 max_ms=312.386
flow_terminal_pipeline_batch: applies=298 items=100,000 results=100,000 avg_apply_ms=10.823 avg_items_per_apply=335.6 max_ms=417.960

QUORUM
submit_total=526
avg_submit_ms=45.465
avg_commands_per_submit=1.05
max_submit_ms=2,962.201
apply_total=824
avg_apply_ms=10.448
max_apply_ms=467.047
wal_sync_total=0
```

Read:

- Batch 1000 is only slightly faster than batch 500 on 100k, but queue p99 is worse.
- Batch 500 looks like the better local latency/throughput balance for this shape.

## Native Flow DBOS-style, 1M sustained, claim/worker batch 500

Environment:

```text
same local release container/image as previous section
clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
host native port: 16388
```

Key knobs:

```text
flows: 1,000,000
claim_batch_size: 500
worker_capacity: 500
create_batch_size: 500
complete_async_depth: 16
claim_drain_batches: 3
protocol_worker_connections: 1
protocol_lanes: 32
```

Result:

```text
created: 1,000,000
completed: 1,000,000
claimed_items: 1,000,000
duplicates: 0
total_seconds: 35.600
end_to_end_flows_per_sec: 28,090/s
create_flows_per_sec: 29,044/s
process_flows_per_sec: 28,100/s
client CPU: 34.16%

queue_latency_avg_ms: 98.2
queue_latency_p50_ms: 64.1
queue_latency_p95_ms: 240.6
queue_latency_p99_ms: 358.7
queue_latency_max_ms: 5,618.3

process_claim_calls: 2,267
process_empty_claims: 25
process_fallback_claims: 37
process_avg_claim_batch: 441.1
process_max_claim_batch: 500
wake_notifications: 2,048
wake_credits: 1,000,000
```

Aggregated server metrics:

```text
FLOW APPLY
flow_create_pipeline_batch: applies=2,048 items=1,000,000 results=1,000,000 avg_apply_ms=17.515 avg_items_per_apply=488.3 max_ms=614.579
flow_claim_due: applies=2,235 items=2,235 results=1,000,000 avg_apply_ms=14.849 avg_results_per_apply=447.4 max_ms=532.110
flow_terminal_pipeline_batch: applies=2,231 items=1,000,000 results=1,000,000 avg_apply_ms=17.919 avg_items_per_apply=448.2 max_ms=645.979

QUORUM
submit_total=4,131
avg_submit_ms=56.857
avg_commands_per_submit=1.04
max_submit_ms=3,329.771
apply_total=6,366
avg_apply_ms=16.889
max_apply_ms=798.837
wal_sync_total=0
```

Read:

- The 1M run sustains ~28.1k/s with one native worker connection.
- Empty claims are effectively solved in this shape: 25 empty out of 2,267 claim calls.
- Create and process are now balanced, both around 28-29k/s.
- Remaining throughput work is likely fewer/larger Ra submissions or faster quorum submit/commit, not native socket parsing.

## Native Flow DBOS-style, create batch 1000 negative result

Environment:

```text
same local release container/image as previous section
clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
host native port: 16388
```

Changed knobs versus batch-500 run:

```text
create_batch_size: 1000
claim_batch_size: 500
worker_capacity: 500
complete_async_depth: 16
claim_drain_batches: 3
```

Result:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 4.241
end_to_end_flows_per_sec: 23,580/s
create_flows_per_sec: 26,420/s
process_flows_per_sec: 23,599/s

queue_latency_p50_ms: 54.1
queue_latency_p95_ms: 174.5
queue_latency_p99_ms: 948.2

process_claim_calls: 456
process_empty_claims: 15
process_avg_claim_batch: 219.3
```

Read:

- Create batch 1000 is worse than create batch 500 for this local shape.
- Keep create batch 500 as the better balance unless a later server-side batching change shifts the optimum.

## Native KV 30s after commit-stage telemetry

Environment:

```text
local release container/image: ferricstore-native-metrics:local
container: ferricstore-native-metrics-bench
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
RESP metrics port: 16379
native port: 16388
client: one process, one native connection
```

SET command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --key-count 1000000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

SET result:

```text
requests: 31,753,500
requests_per_sec: 1,057,224/s
batch_latency_p50_ms: 29.299
batch_latency_p95_ms: 34.776
batch_latency_p99_ms: 44.332
batch_latency_max_ms: 53.045
client_cpu_percent: 18.45
errors: 0
pipeline: 500
protocol_lanes: 64
inflight_batches: 64
```

GET command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --key-count 1000000 \
  --value-bytes 32 \
  --binary-keys \
  --pretty
```

GET result:

```text
requests: 86,786,000
requests_per_sec: 2,890,362/s
batch_latency_p50_ms: 21.852
batch_latency_p95_ms: 31.494
batch_latency_p99_ms: 34.884
batch_latency_max_ms: 41.264
client_cpu_percent: 102.23
errors: 0
pipeline: 1000
protocol_lanes: 64
inflight_batches: 64
warmed_keys: 1,000,000
```

Aggregated server metrics after SET+GET:

```text
WARaft commit stage:
put_batch sync path: count=85,369 avg=13.093ms max=52.653ms

Quorum submit:
count=85,369 avg=21.381ms max=84.971ms

Segment projection:
put_batch apply: count=85,369 avg=0.816ms max=8.082ms avg_batch=737.2
```

Read:

- Native SET improved to ~1.06M/s on one native connection in this shape.
- Native GET improved to ~2.89M/s, but the client is CPU-saturated, so the next GET gains are mostly client/codec/benchmark-side unless server CPU says otherwise.
- SET bottleneck is still quorum submit/commit. Segment projection is not the SET bottleneck in this run.

## Native Flow DBOS-style 100k after commit-stage result fix

Environment:

```text
local release container/image: ferricstore-native-metrics:local
clean /data
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500
native port: 16388
client: one process, one native worker connection
```

Command:

```bash
python examples/dbos_style_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --mode queued \
  --queued-shape live \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 500 \
  --worker-capacity 500 \
  --create-batch-size 500 \
  --complete-async-depth 16 \
  --claim-drain-batches 3 \
  --worker-api queue \
  --claim-state queued \
  --claim-job-only \
  --complete-batch \
  --independent-many \
  --complete-independent-many \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --server-shards 16 \
  --protocol-wake-hints
```

Result:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 3.937
end_to_end_flows_per_sec: 25,398/s
create_flows_per_sec: 29,243/s
process_flows_per_sec: 25,419/s
client_cpu_percent: 32.43

queue_latency_p50_ms: 51.1
queue_latency_p95_ms: 209.0
queue_latency_p99_ms: 691.0

process_claim_calls: 463
process_empty_claims: 22
process_avg_claim_batch: 216.0
process_max_claim_batch: 500
wake_notifications: 256
wake_credits: 100,000
```

Aggregated server metrics:

```text
FLOW APPLY
flow_create_pipeline_batch: applies=256 items=100,000 results=100,000 avg_apply_ms=10.536 avg_items_per_apply=390.6 max_ms=36.040
flow_claim_due: applies=430 results=100,000 avg_apply_ms=6.026 avg_results_per_apply=232.6 max_ms=22.664
flow_terminal_pipeline_batch: applies=430 items=100,000 results=100,000 avg_apply_ms=7.190 avg_items_per_apply=232.6 max_ms=55.159

WARaft commit stage
flow_create_pipeline_batch: count=218 avg_commit_ms=38.585 max_ms=568.348 result=ok
flow_claim_due: count=430 avg_commit_ms=31.602 max_ms=475.861 result=ok
flow_terminal_pipeline_batch: count=405 avg_commit_ms=33.552 max_ms=424.314 result=ok
batch: count=31 avg_commit_ms=41.479 max_ms=82.968 result=ok

QUORUM
submit_total=654
avg_submit_ms=42.503
max_submit_ms=571.003
```

Read:

- Commit-stage result labels are now correct for Flow batch commands.
- Empty claims are not the bottleneck in this shape.
- Flow apply time is much lower than commit time; the next optimization should target durable commit batching/submission, not Flow record mutation.

## Native Flow worker connection comparison and startup fix

Context:

- A clean-container immediate run with `--protocol-worker-connections 2` initially exposed a server startup failure:
  `fsync_dir /data/waraft/ferricstore_waraft_backend.16/segment_log: No such file or directory`.
- Fix applied server-side: `DataDir.ensure_layout!/2` now creates WARaft 1-based partition `segment_log` directories before WARaft opens/reset logs.
- Fix applied SDK-side: sync `ProtocolAdapter` retries transient startup socket reset/closed/timeout errors during cold connect.

Validation command shape:

```text
flows: 100,000
claim_batch_size: 500
worker_capacity: 500
create_batch_size: 500
complete_async_depth: 16
claim_drain_batches: 3
protocol_lanes: 32
protocol_worker_connections: 2
server_shards: 16
```

Result after fixes:

```text
created: 100,000
completed: 100,000
claimed_items: 100,000
duplicates: 0
total_seconds: 4.023
end_to_end_flows_per_sec: 24,858/s
create_flows_per_sec: 27,116/s
process_flows_per_sec: 24,880/s
client_cpu_percent: 32.89

queue_latency_p50_ms: 50.6
queue_latency_p95_ms: 243.6
queue_latency_p99_ms: 662.3

process_claim_calls: 437
process_empty_claims: 8
process_avg_claim_batch: 228.8
```

Read:

- Two worker connections do not improve E2E throughput versus the one-connection run in this local shape.
- One native socket is not the main Flow throughput limiter.
- The next useful optimization remains durable commit batching/submission, because Flow apply is much cheaper than commit-stage time.

## 2026-06-12 - Native Flow selective zero-window WARaft batch scheduling

Change under test:
- Keep global `waraft_generic_batch_window_ms=1`.
- Bypass the fixed generic window only for Flow pipeline/batch commands in `WARaftBackend.Batcher.write_batch/2`.
- Rationale: Flow pipeline commands already carry a server-side batch; the extra namespace window adds queue latency without improving packing. KV keeps the existing coalescing path.

Validation:
- `mix format --check-formatted apps/ferricstore/lib/ferricstore/raft/waraft_backend/batcher.ex apps/ferricstore/test/ferricstore/raft/waraft_backend_test/sections/waraft_generic_batches_coalesce_behind_in_flight_flush_default.exs`
- `FERRICSTORE_DATA_DIR=/tmp/ferricstore-test-batcher-line-$RANDOM mix test apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs:207 --include raft`
- Result: `19 tests, 0 failures` for the affected section context.
- Docker release rebuild passed.

Flow benchmark command shape:
- `examples/dbos_style_benchmark.py --url ferric://127.0.0.1:<port> --mode queued --queued-shape live --transport many --flows 100000 --workers 16 --producers 4 --partitions 16 --claim-batch-size 500 --worker-capacity 500 --create-batch-size 500 --complete-async-depth 16 --claim-drain-batches 3 --worker-api queue --claim-state queued --claim-job-only --complete-batch --independent-many --complete-independent-many --protocol-worker-connections 1 --protocol-lanes 32 --server-shards 16 --protocol-wake-hints`

Flow 100k result, default config with selective bypass:
- E2E: `28,845/s`
- Create: `33,416/s`
- Process: `28,876/s`
- Queue latency: p50 `51.7ms`, p95 `145.2ms`, p99 `402.8ms`, max `2434.3ms`
- Claim calls: `469`, empty claims: `12`, avg claim batch: `213.2`, max claim batch: `500`
- This beat previous local default runs around `23.8k-25.4k/s` and also beat blanket `FERRICSTORE_WARAFT_GENERIC_BATCH_WINDOW_MS=0` (`25.8k/s`).

KV 30s clean result, same image/default config:
- SET preset: `976,199/s`, p50 batch `31.802ms`, p95 `38.437ms`, p99 `42.637ms`, errors `0`
- GET preset: `2,841,752/s`, p50 batch `22.387ms`, p95 `31.869ms`, p99 `35.469ms`, errors `0`
- Note: an earlier stale FerricStore benchmark container was still running and depressed SET to ~`890-906k/s`; after stopping it, SET recovered to ~`976k/s`.
- Best prior local native sample was SET ~`1.057M/s`, GET ~`2.890M/s`; current GET is close, SET is below best sample but much better than the polluted run.

Final predicate follow-up:
- Predicate tightened so only pure Flow command batches use the zero-window policy; mixed Flow+KV batches keep generic coalescing.
- Flow tag set expanded to cover singular and many Flow write command shapes.
- Rebuilt Docker image and reran Flow 100k.

Flow 100k final result:
- E2E: `29,303/s`
- Create: `36,356/s`
- Process: `29,332/s`
- Queue latency: p50 `51.6ms`, p95 `151.3ms`, p99 `345.8ms`, max `2895.8ms`
- Claim calls: `463`, empty claims: `22`, avg claim batch: `216.0`, max claim batch: `562`

## 2026-06-12 - Native HGET compact pipeline experiments (rejected)

Environment: local Docker image `ferricstore-native-metrics:local`, one native connection, `--protocol-lanes 64`, `--pipeline 1000`, `--inflight-batches 64`, binary keys, 100k warmed hash keys, 32-byte values.

Baseline reference from earlier sweep:

```text
HGET compact pipeline: ~997,525/s, p50 62.677 ms, p95 78.484 ms, p99 81.752 ms, 15s exploratory run
```

Rejected experiment 1: prebuild route-key batches for type markers and hash fields.

```text
30s HGET: 890,793/s, p50 64.897 ms, p95 106.855 ms, p99 110.734 ms
```

Rejected experiment 2: thin direct HGET loop using `Router.compound_get/3` without full command dispatch.

```text
30s HGET: 595,910/s, p50 106.556 ms, p95 130.737 ms, p99 248.119 ms
```

Decision: do not keep either HGET optimization. Existing generic compact HGET path remains faster. A future HGET optimization should be a lower-level core/native primitive that batches compound routed lookups without extra BEAM list construction or repeated public command wrappers.

Current generic HGET re-baseline after revert:

```text
30s HGET: 985,056/s, p50 63.845 ms, p95 73.143 ms, p99 79.892 ms
```

This confirms the original generic compact HGET path is currently the best measured implementation.

## 2026-06-12 - Native Flow read benchmark notes

Environment: local Docker image `ferricstore-native-metrics:local`, one native connection, 16 server shards, hot Flow records, 100k created flows, `submit_batch` compact Flow pipelines, batch 500, 64 in-flight batches, 30s read window.

`FLOW.GET` full record:

```text
30s FLOW.GET: 84,200/s, p50 batch 375.735 ms, p95 494.292 ms, p99 545.008 ms
create warmup: 100k flows in 1.707s
```

Found and fixed SDK protocol bug: batched `FLOW.GET RETURN META` without explicit partition selected compact mode 17 but omitted the required nil optional-partition marker. Added SDK coverage for no-partition meta encoding.

`FLOW.GET RETURN META` after SDK fix:

```text
30s FLOW.GET RETURN META: 111,250/s, p50 batch 295.237 ms, p95 369.794 ms, p99 421.435 ms
create warmup: 100k flows in 1.510s
```

Read: meta is faster, but Flow reads are still far below KV GET because server decodes Flow records and client decodes compact Flow record maps. Future optimization should target compact meta-only record encoding/decoding and/or lower-allocation Flow record projection.

`FLOW.LIST RETURN META COUNT 100` hot auto-partition query:

```text
30s FLOW.LIST RETURN META COUNT 100: 37.8 requests/s, 3,783 records/s
p50 request 1704.452 ms, p95 2393.747 ms, p99 2736.911 ms
create warmup: 100k flows in 2.098s
```

Read: this is not a protocol-framing bottleneck. Current auto-list path counts/scans across hidden auto buckets to preserve global ordering, so every no-partition list query fans across many hot indexes. A real optimization needs an aggregate per-type/state auto index or cursor-aware list primitive; blindly sampling fewer buckets would break ordering/correctness.

## 2026-06-12 - Reproducible native Flow read benchmark tool

Added `examples/protocol_flow_read_benchmark.py` with modes:

```text
flow-get
flow-get-meta
flow-list-meta
flow-value-mget
```

The benchmark uses native `ferric://`, compact Flow batching where supported, 30s measured windows, and records both request rate and item/record rate.

Environment: local Docker image `ferricstore-native-metrics:local`, one native connection, 16 server shards, `--flows 100000`, `--create-batch-size 500`, `--read-batch-size 500`, `--inflight-batches 64`, `--value-bytes 32`.

`FLOW.GET RETURN META`:

```text
116,233 items/s
p50 batch 267.833 ms, p95 364.205 ms, p99 398.963 ms
create warmup 0.877s
```

`FLOW.GET` full record:

```text
70,683 items/s
p50 batch 460.053 ms, p95 517.685 ms, p99 546.127 ms
create warmup 2.194s
```

`FLOW.VALUE.MGET` with 500 refs/request:

```text
247,633 values/s, 495.3 requests/s
p50 batch 125.318 ms, p95 159.100 ms, p99 192.961 ms
create warmup 1.887s, value-put warmup 2.304s
```

`FLOW.LIST RETURN META COUNT 100`, no explicit partition / auto hidden buckets:

```text
27.0 requests/s, 2,697 records/s
p50 request 2378.785 ms, p95 3226.219 ms, p99 4131.537 ms
```

`FLOW.LIST RETURN META COUNT 100`, explicit `--partition-key tenant-a`:

```text
145.1 requests/s, 14,510 records/s
p50 request 448.444 ms, p95 563.849 ms, p99 611.669 ms
```

A separate single-flight explicit-partition sample showed p50 around 4.25ms for COUNT 100, so the high-concurrency partitioned run is mostly queueing/materialization under load. The no-partition auto-bucket path remains algorithmically expensive because it merges hidden buckets to preserve ordering.

`FLOW.GET RETURN META` with explicit single partition:

```text
89,817 items/s
p50 batch 360.823 ms, p95 441.575 ms, p99 492.819 ms
```

Read: explicit single partition hurts high-throughput GET because all records route to one shard. Auto-spread is better for point-read throughput. The explicit partition win is specific to `FLOW.LIST`, where it avoids the 256 hidden-bucket merge.

## 2026-06-12 - Native FLOW.GET RETURN META server meta decode

Change kept:
- Added `Ferricstore.Flow.decode_record_meta/1` backed by Rust NIF `flow_record_decode_meta`.
- Native compact pipeline mode 17 (`FLOW.GET RETURN META`) now decodes only meta fields server-side before response encoding.
- Durable Flow record format is unchanged.
- Added parity coverage: meta-only decode must equal full decode plus `Ferricstore.Flow.RecordProjection.meta/1`, including sidecar `value_refs`.

Validation:

```text
mix test apps/ferricstore/test/ferricstore/flow_codec_test.exs
9 tests, 0 failures

mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:2368
1 test, 0 failures
```

Local decode microbench, 300k in-process decodes:

```text
full decode + projection: ~253k/s
meta decode:              ~441k/s
```

Protocol benchmark shape:

```bash
python examples/protocol_flow_read_benchmark.py \
  --url ferric://127.0.0.1:17788 \
  --mode flow-get-meta \
  --flows 100000 \
  --test-time 30 \
  --create-batch-size 500 \
  --read-batch-size 500 \
  --inflight-batches 64 \
  --value-bytes 32 \
  --pretty
```

Fresh local source server, 16 shards, clean temp data dir.

Same-environment A/B:

```text
old full-decode mode 17: 93,533/s, p50 313.287 ms, p95 563.577 ms, p99 747.435 ms
new meta-decode mode 17: 97,283/s, p50 302.066 ms, p95 563.309 ms, p99 844.778 ms
```

Read:
- The retained server meta decode is a small protocol-path win in this noisy local source setup, and a clear decode-only CPU win.
- A separate first source sample with the meta NIF reached 133,150/s, but later samples were lower while create warmup was also slower. Treat that as environment noise, not the official result.
- A second-pass removal that skipped the final `RecordProjection.meta/1` over already-meta maps was rejected: two clean samples were ~95k/s and did not beat the retained path.

## 2026-06-12 - Native FLOW.LIST cross-partition hydration cut

Change kept:
- `FLOW.LIST` no-partition hot path still uses the same Rust OrderedIndex rank/merge semantics.
- Final record hydration for auto-bucket list results now builds state keys and uses one generic cross-shard `Router.batch_get/2` call through `RecordLoader.records_for_partitioned_entries/2`.
- This replaces many tiny per-partition `flow_batch_get/3` calls after the rank merge.
- Semantics remain current-state hydration from durable state keys; nil/missing records are skipped like the existing loader path.

Validation:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
91 tests, 0 failures
```

Added regression coverage:
- Native `FLOW.LIST RETURN META` across multiple auto-partitioned flows preserves rank order.

Benchmark shape:

```bash
python examples/protocol_flow_read_benchmark.py \
  --url ferric://127.0.0.1:17888 \
  --mode flow-list-meta \
  --flows 100000 \
  --test-time 30 \
  --create-batch-size 500 \
  --read-batch-size 500 \
  --inflight-batches 64 \
  --value-bytes 32 \
  --list-count 100 \
  --pretty
```

Fresh local source server, 16 shards, clean temp data dir.

No explicit partition / auto hidden buckets:

```text
before reproducible tool baseline: 27.0 req/s, 2,697 records/s, p50 2378.785 ms, p99 4131.537 ms
current source run:              173.5 req/s, 17,350 records/s, p50 346.933 ms, p99 680.814 ms
```

Explicit partition sample on the same source server:

```text
1021.0 req/s, 102,103 records/s, p50 60.448 ms, p99 107.311 ms
```

Read:
- This does not remove the 256 auto-bucket count/rank merge cost.
- It removes the expensive post-merge hydration pattern where COUNT 100 could turn into many tiny partition-specific state reads.
- A larger future cut still needs an aggregate/cursor-aware auto index if we want no-partition list to approach explicit-partition latency.

## 2026-06-12 - Rejected FLOW.LIST meta-only hydration

Experiment:
- Threaded a private `read_return: :meta` option from native `FLOW.LIST RETURN META` into `RecordRead`.
- Used `Flow.decode_record_meta/1` during list hydration instead of full `decode_record/1`.

Validation passed before benchmark:

```text
mix compile --warnings-as-errors
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
91 tests, 0 failures
```

Benchmark shape:

```bash
python examples/protocol_flow_read_benchmark.py \
  --url ferric://127.0.0.1:<port> \
  --mode flow-list-meta \
  --flows 100000 \
  --test-time 30 \
  --create-batch-size 500 \
  --read-batch-size 500 \
  --inflight-batches 64 \
  --value-bytes 32 \
  --list-count 100 \
  --pretty
```

Result with meta-only hydration:

```text
141.3 req/s, 14,130 records/s, p50 418.661 ms, p99 897.123 ms
```

This was worse than the previous kept cross-partition hydration result:

```text
173.5 req/s, 17,350 records/s, p50 346.933 ms, p99 680.814 ms
```

Decision:
- Reverted the meta-only list hydration path.
- Kept cross-partition batch hydration.
- Confirmation after revert in a fresh source run:

```text
152.2 req/s, 15,220 records/s, p50 391.377 ms, p99 965.737 ms
```

Read:
- Meta-only record decode is useful for point `FLOW.GET RETURN META`, but not clearly useful inside high-concurrency `FLOW.LIST` in this shape.
- The list bottleneck remains index fan/merge and request queueing, not just record decode.

## 2026-06-12 - Rust OrderedIndex range_slice BTree seek

Change kept:
- `FlowOrderedIndex.range_slice` now starts iteration from the requested key's BTree range instead of scanning the whole `ordered` set and filtering by key.
- Reverse scans use an exclusive upper key (`key <> <<0>>`) and walk backward only through the exact key's run.
- No write-path changes and no new aggregate indexes.

Validation:

```text
cargo test --manifest-path apps/ferricstore/native/ferricstore_bitcask/Cargo.toml flow --lib
10 tests passed

mix test apps/ferricstore/test/ferricstore/flow/ordered_index_test.exs
21 tests, 0 failures

mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
91 tests, 0 failures
```

Added coverage:
- Native range slice remains isolated by exact key even with sibling/prefix-like keys.
- Offset and reverse order still work.

`FLOW.LIST RETURN META COUNT 100`, no explicit partition, 100k flows, 30s:

```text
before initial cross-partition hydration: 27.0 req/s, 2,697 records/s, p99 4131.537 ms
cross-partition hydration only:        173.5 req/s, 17,350 records/s, p99 680.814 ms
current BTree seek range_slice:        754.8 req/s, 75,480 records/s, p99 189.463 ms
```

Explicit partition sample on same source server:

```text
1013.2 req/s, 101,320 records/s, p50 60.704 ms, p99 102.037 ms
```

KV protocol sanity on separate clean source server, 16 shards, native protocol, binary keys:

```text
SET preset: 508,817/s, p50 60.558 ms, p99 133.113 ms
GET preset: 1,497,256/s, p50 41.920 ms, p99 69.959 ms
```

Read:
- The list improvement came from removing an accidental full-index scan in the Rust hot index read path.
- This should also help other `rank_range` readers such as hot history/index queries.
- KV numbers are recorded as current sanity only; this change does not touch KV paths, and older best KV protocol samples used different cleanliness/runtime conditions.

### DBOS-style sanity after OrderedIndex range_slice change

Clean source server, 16 shards, native protocol, tuned `dbos_style_benchmark.py` queue shape:

```bash
python examples/dbos_style_benchmark.py \
  --url ferric://127.0.0.1:18488 \
  --mode queued \
  --queued-shape live \
  --transport many \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --claim-batch-size 500 \
  --worker-capacity 500 \
  --create-batch-size 500 \
  --complete-async-depth 16 \
  --claim-drain-batches 3 \
  --worker-api queue \
  --claim-state queued \
  --claim-job-only \
  --complete-batch \
  --independent-many \
  --complete-independent-many \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --server-shards 16 \
  --protocol-wake-hints
```

Result:

```text
E2E: 17,122/s
create: 18,269/s
process: 17,131/s
queue latency: p50 64.816 ms, p95 534.148 ms, p99 1681.114 ms
claim calls: 473, empty claims: 22, avg claim batch: 211.4
```

Read:
- This is lower than older best DBOS-style local samples.
- The kept range_slice change only affects Flow index reads such as list/history/range; it does not touch create/claim/complete write paths.
- Treat this as a current clean-machine sanity datapoint, not as an apples-to-apples write-path regression proof.

## 2026-06-12 - Native KV current clean-source repro and rejected PIPELINE SET status helper

Purpose:

- Re-check current native KV numbers after a low sanity sample appeared after Flow LIST work.
- Separate real protocol regression from dirty data-dir / background-pressure effects.

Source server shape:

```text
MIX_ENV=prod mix run --no-halt
FERRICSTORE_SHARD_COUNT=16
FERRICSTORE_NATIVE_ENABLED=true
fresh /tmp data dirs
one native client connection
```

Clean `set-throughput` preset, compact `MSET`/many mode, binary keys, 32-byte values:

```text
SET: 1,004,326/s
p50 batch: 26.227 ms
p95 batch: 63.141 ms
p99 batch: 89.947 ms
errors: 0
```

Immediate `get-throughput` after writes was polluted by post-write/background pressure and should not be treated as a clean read number:

```text
GET immediately after SET: 1,386,029/s, p99 72.508 ms
GET after idle:            1,135,256/s, p99 178.649 ms
```

Same server, compact `PIPELINE GET`, prebuilt binary keys, no warmup:

```text
GET pipeline: 1,625,211/s
p50 batch: 38.640 ms
p99 batch: 66.835 ms
errors: 0
```

Exact historical direct compact pipeline shape on fresh source server, non-binary prebuilt keys, 16-byte values:

```text
SET pipeline before experiment: 626,792/s
p50 batch: 43.636 ms
p99 batch: 105.089 ms
errors: 0
```

Rejected experiment:

- Tried routing compact `PIPELINE SET` values-only mode through `Router.batch_quorum_put_status/2` and encoding `OK` count directly.
- Correctness focused test passed, but benchmark was much worse.

Result after experiment on fresh source server, same exact shape:

```text
SET pipeline with status helper: 288,754/s
p50 batch: 91.486 ms
p99 batch: 513.252 ms
errors: 0
```

Decision:

- Reverted the status-helper shortcut.
- Do not retry this path unless `batch_quorum_put_status/2` itself is made equivalent to the faster result-producing WARaft path.
- Current good durable write reference for this worktree is the `set-throughput` many/MSET path around `1.0M/s` on clean source server.
- Current compact `PIPELINE SET` is materially slower than older historical `~1.9M/s` samples and needs deeper investigation in the pipeline/request path or current runtime shape before changing server semantics.

Validation after revert:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:1886
=> 1 test, 0 failures
```

## 2026-06-12 - Exact release comparison: native MSET/many vs compact PIPELINE SET

Runtime:

- FerricStore source release: `_build/prod/rel/ferricstore/bin/ferricstore start`
- `FERRICSTORE_NATIVE_ENABLED=true`
- `FERRICSTORE_SHARD_COUNT=16`
- Fresh data dirs per run
- One native connection
- `protocol_lanes=64`
- `inflight_batches=64`
- `pipeline=500`
- `test_time=30s`
- non-binary keys
- `value_bytes=16`
- `key_count=1_000_000`
- `prebuild_keys=true`
- `no_warmup=true`

MSET/many shape:

```text
request_mode: many
requests_per_sec: 1,166,355/s
requests: 35,064,000
batch_latency_p50_ms: 23.370
batch_latency_p95_ms: 49.410
batch_latency_p99_ms: 61.622
client_cpu_percent: 20.03
errors: 0
```

Compact PIPELINE SET shape:

```text
request_mode: pipeline
requests_per_sec: 637,176/s
requests: 19,205,000
batch_latency_p50_ms: 43.648
batch_latency_p95_ms: 88.121
batch_latency_p99_ms: 107.393
client_cpu_percent: 13.51
errors: 0
```

Conclusion:

- The gap is real under the same release runtime and exact key/value shape.
- Native `MSET/many` is about 1.83x faster than compact `PIPELINE SET`.
- Client CPU is lower on the slower pipeline run, so this is not primarily client CPU pressure.
- Next target: native compact pipeline SET execution/response path, not the durable SET engine itself.

## 2026-06-12 - Kept optimization: status-only compact PIPELINE SET values path

Change:

- Native compact `PIPELINE SET` with `compact_values=true` now uses `Router.batch_quorum_put_status/2` and returns a compact `OK count` directly.
- Pair-return mode remains unchanged and still preserves per-item status pairs.
- Public command shape unchanged: this is still `PIPELINE` carrying SET commands in one frame.

Correctness checks:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:1881 apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:1829 apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:362
=> 3 tests, 0 failures

mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
=> 91 tests, 0 failures
```

Benchmark after change:

Runtime:

- FerricStore source release: `_build/prod/rel/ferricstore/bin/ferricstore start`
- `FERRICSTORE_NATIVE_ENABLED=true`
- `FERRICSTORE_SHARD_COUNT=16`
- Fresh data dir
- One native connection
- `protocol_lanes=64`
- `inflight_batches=64`
- `pipeline=500`
- `test_time=30s`
- non-binary keys
- `value_bytes=16`
- `key_count=1_000_000`
- `prebuild_keys=true`
- `no_warmup=true`

```text
request_mode: pipeline
requests_per_sec: 1,243,234/s
requests: 37,363,500
batch_latency_p50_ms: 23.152
batch_latency_p95_ms: 41.805
batch_latency_p99_ms: 48.198
client_cpu_percent: 20.95
errors: 0
```

Delta from pre-change exact release pipeline run:

```text
throughput: 637,176/s -> 1,243,234/s  (+95.1%)
p50:        43.648ms  -> 23.152ms
p95:        88.121ms  -> 41.805ms
p99:       107.393ms  -> 48.198ms
```

Conclusion:

- Keep the optimization.
- The previous rejected status-helper result was not reliable because it used the unstable source `mix run --no-halt` runner path.
- Under the production release runner, status-only compact pipeline SET is clearly faster and close to/above exact MSET/many throughput on this machine.

## 2026-06-12 - Native GET pipeline vs MGET/many release comparison

Runtime:

- Same release/server as the kept compact SET optimization benchmark
- Existing 1M keyspace written by prior SET pipeline run
- One native connection
- `protocol_lanes=64`
- `inflight_batches=64`
- `pipeline=1000`
- `test_time=30s`
- non-binary keys
- `value_bytes=16`
- `key_count=1_000_000`
- `prebuild_keys=true`
- `no_warmup=true`

Compact PIPELINE GET:

```text
request_mode: pipeline
requests_per_sec: 1,640,096/s
requests: 49,259,000
batch_latency_p50_ms: 39.140
batch_latency_p95_ms: 58.455
batch_latency_p99_ms: 71.652
client_cpu_percent: 79.17
errors: 0
```

MGET/many:

```text
request_mode: many
requests_per_sec: 2,653,743/s
requests: 79,671,000
batch_latency_p50_ms: 23.798
batch_latency_p95_ms: 34.020
batch_latency_p99_ms: 38.621
client_cpu_percent: 101.77
errors: 0
```

Conclusion:

- Direct `MGET/many` is about 1.62x faster than compact `PIPELINE GET` on this one-connection release run.
- Unlike SET before the fix, GET is already client CPU saturated on the faster path.
- Next GET target should be protocol response decode/client CPU and compact pipeline GET overhead, not storage durability.

## Native protocol compact ZADD pipeline fast path

Server: local source release, 16 shards, native port 19088, fresh `/tmp/ferricstore-native-zadd-test` data dir.
Client: one native connection, `protocol_lanes=64`, `inflight_batches=32`, `pipeline=500`, `test_time=30`, `key_count=200000`, non-binary keys, `value_bytes=16`, prebuilt keys, no warmup.

Baseline from command-family scan before this fast path:

```text
ZADD: 11,856/s, p50 batch 1279.338 ms, p99 batch 1560.670 ms, client CPU 1.6%, errors 0
```

After routing homogeneous native ZADD pipelines as one `:zadd_many_single` Ra command per shard, while still applying each item through existing `zadd_single` semantics inside one pending-write batch:

```text
ZADD: 362,552/s, p50 batch 28.498 ms, p95 batch 70.345 ms, p99 batch 612.009 ms, client CPU 18.09%, errors 0
```

Correctness checks:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:1334 \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:1551 \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:1562
# 3 tests, 0 failures

mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures
```

Notes:

```text
The large p99 spike means sustained tail still needs follow-up. Throughput improved ~30x versus the previous compact ZADD path.
```

## Native protocol ZADD pipeline follow-up: fused apply + backend write_many

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-zadd-writemany` data dir.
Client: one native connection, `protocol_lanes=64`, `inflight_batches=32`, `pipeline=500`, `test_time=30`, `key_count=200000`, non-binary keys, `value_bytes=16`, prebuilt keys, no warmup.

Kept changes:

```text
- Homogeneous native ZADD pipeline routes as one :zadd_many_single Ra/WARaft command per shard.
- State machine applies ZADD entries through a fused helper that preserves key-lock and zadd_single semantics but avoids per-item command tuple dispatch.
- Router uses Ferricstore.Raft.Backend.write_many/1 for multi-shard fanout instead of spawning one BEAM Task per shard per pipeline request.
```

Current accepted result:

```text
ZADD: 230,227/s, p50 batch 48.418 ms, p95 batch 102.284 ms, p99 batch 713.969 ms, client CPU 12.93%, errors 0
```

Rejected experiment:

```text
A broader :semantic_write_many path for HSET/LPUSH/RPUSH/SADD/SREM/ZREM was tested and removed.
Reason: HSET/LPUSH/RPUSH did not show clear improvement versus earlier scan numbers; keep only proven hot-path changes.
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures
```

Read:

```text
ZADD is now much faster than the original compact pipeline path, but still has long-tail latency. Next ZADD work should target zset index move/update cost, not protocol parsing.
```

## Native protocol compact member reads: SISMEMBER and ZSCORE batch lookup

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-member-read-batch` data dir.
Client: one native connection, `protocol_lanes=64`, `inflight_batches=32`, `pipeline=500`, `test_time=30`, `key_count=200000`, non-binary keys, `value_bytes=16`, prebuilt keys, no warmup.

Change:

```text
Compact SISMEMBER and ZSCORE pipeline modes now batch-read compound type/member keys through Router.batch_get_on_route_keys/2.
Missing type still checks the base key to preserve WRONGTYPE behavior.
```

Results:

```text
ZADD populate:     459,852/s, p50 batch 23.406 ms, p99 batch 455.097 ms, errors 0
ZSCORE read:       956,139/s, p50 batch 16.432 ms, p99 batch 29.970 ms, errors 0
SADD populate:     264,386/s, p50 batch 57.412 ms, p99 batch 111.688 ms, errors 0
SISMEMBER read:    907,608/s, p50 batch 17.163 ms, p99 batch 32.886 ms, errors 0
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures
```

Read:

```text
This removes the old per-item FerricStore.Impl.sismember/zscore loop from compact native pipeline reads. Next similar target: HGET/HMGET compact reads.
```

## Native protocol compact HGET batch lookup

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-hget-batch` data dir.
Client: one native connection, `protocol_lanes=64`, `inflight_batches=32`, `pipeline=500`, `test_time=30`, `key_count=200000`, non-binary keys, `value_bytes=16`, prebuilt keys, no warmup.

Change:

```text
Compact HGET pipeline mode now batch-reads hash type/field keys through Router.batch_get_on_route_keys/2.
Missing type still checks the base key to preserve WRONGTYPE behavior.
```

Results:

```text
HSET populate: 204,197/s, p50 batch 80.141 ms, p99 batch 141.447 ms, errors 0
HGET read:     928,138/s, p50 batch 16.858 ms, p99 batch 31.572 ms, errors 0
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures
```

## Native protocol bounded ZRANGE sanity check

Server: local source release, 16 shards, `/tmp/ferricstore-native-hget-batch` server.
Client: one native connection, `protocol_lanes=64`, `inflight_batches=32`, `pipeline=500`, `test_time=30`, `key_count=200000`, non-binary keys, `value_bytes=16`, prebuilt keys, no warmup.

Command:

```text
ZRANGE 0 0 with --zset-members-per-key 1
```

Result:

```text
ZRANGE: 359,164/s, p50 batch 45.318 ms, p95 batch 60.447 ms, p99 batch 78.158 ms, errors 0
```

Read:

```text
No server hang reproduced for bounded ZRANGE. Earlier issue was likely bad benchmark invocation / large-response guard confusion, not this bounded path.
```

## Native protocol compact HMGET single-field batch lookup

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-hmget-batch` data dir.
Client: one native connection, `protocol_lanes=64`, `inflight_batches=32`, `pipeline=500`, `test_time=30`, `key_count=200000`, non-binary keys, `value_bytes=16`, prebuilt keys, no warmup.

Change:

```text
Compact HMGET where every item requests one field now reuses compact HGET batch lookup and wraps each value as [value].
Multi-field HMGET stays on the existing path.
```

Results:

```text
HSET populate: 209,962/s, p50 batch 76.588 ms, p99 batch 154.603 ms, errors 0
HMGET read:    408,352/s, p50 batch 39.367 ms, p99 batch 68.777 ms, errors 0
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures
```

## Native protocol Flow command sweep after KV/member-read optimizations

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-flow-sweep` data dir, native port 19188.
Client: one native connection, `protocol_lanes=32`, `inflight_batches=64`, `batch_size=500`, `setup_batch_size=500`, `flows=100000`, `partitions=16`, `payload_bytes=0`, `retention_ttl_ms=0`, `flow_read_consistency=eventual`, `read_duration=30` for read-duration operations.

Results:

```text
create-many:       250,587/s, batch p99 133.395 ms, errors 0
transition-many:   154,334/s, batch p99 234.754 ms, errors 0
complete-many:     107,500/s, batch p99 428.008 ms, errors 0
retry-many:         89,346/s, batch p99 450.848 ms, errors 0
fail-many:          59,935/s, batch p99 743.764 ms, errors 0
cancel-many:       108,227/s, batch p99 320.326 ms, errors 0
claim-due:          84,116/s, batch p99 406.831 ms, errors 0
start-and-claim:    51,060/s, batch p99 649.860 ms, errors 0
signal:             46,064/s, batch p99 813.262 ms, errors 0
step:               26,238/s, batch p99 1397.729 ms, errors 0
value-put-owned:    17,888/s, batch p99 2347.735 ms, errors 0
flow-get:           49,192/s, batch p99 832.970 ms, errors 0
flow-history:      227,714/s, batch p99 222.588 ms, errors 0
flow-list:          38,814/s, batch p99 1140.772 ms, errors 0
```

Read:

```text
Weakest paths are owned value writes, step, list/get response path, and signal. Flow history is already healthy. Next investigation should prefer a low-risk protocol/response or batching issue before deeper Flow state-machine rewrites.
```

## Native protocol Flow GET/LIST meta-vs-full check

Server: same local source release and `/tmp/ferricstore-native-flow-sweep` server as the Flow sweep.
Client: one native connection, `protocol_lanes=32`, `inflight_batches=64`, `batch_size=500`, `setup_batch_size=500`, `flows=100000`, `partitions=16`, `payload_bytes=0`, `read_duration=30`.

Results:

```text
flow-get:       49,192/s, batch p99 832.970 ms, client CPU 74.5%, errors 0
flow-get-meta:  58,300/s, batch p99 733.241 ms, client CPU 60.9%, errors 0
flow-list:      38,814/s, batch p99 1140.772 ms, client CPU 54.7%, errors 0
flow-list-meta: 53,216/s, batch p99 834.936 ms, client CPU 50.2%, errors 0
```

Read:

```text
Meta responses help, especially list, but not enough to explain the whole gap. Flow read path still spends substantial time in lookup/hydration/server response work.
```

## Native protocol owned FLOW.VALUE.PUT compact mode 14 fast path

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-value-owned-fastpath` data dir, native port 19288.
Client: one native connection, `protocol_lanes=32`, `inflight_batches=64`, `batch_size=500`, `setup_batch_size=500`, `flows=100000`, `partitions=16`, `payload_bytes=0`, `retention_ttl_ms=0`.

Bug/perf gap:

```text
Compact owned FLOW.VALUE.PUT uses compact pipeline mode 14.
Mode 14 validated and built commands through the mode-8 shape, but execute_compact_pipeline_fast_path only matched mode 8, so mode 14 fell back through command materialization instead of the direct Flow pipeline write path.
```

Change:

```text
execute_compact_pipeline_fast_path for owned FLOW.VALUE.PUT now matches mode in [8, 14].
```

Result:

```text
Before: value-put-owned 17,888/s, p99 batch 2347.735 ms, errors 0
After:  value-put-owned 91,195/s, p99 batch 463.878 ms, errors 0
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures
```

## Native protocol FLOW.STEP_CONTINUE shard-batched step-many path

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-step-batch` data dir, native port 19388.
Client: one native connection, `protocol_lanes=32`, `inflight_batches=64`, `batch_size=500`, `setup_batch_size=500`, `flows=100000`, `partitions=16`, `payload_bytes=0`, `retention_ttl_ms=0`.

Bug/perf gap:

```text
FLOW.STEP_CONTINUE compact mode 6 had a native protocol fast path, but Flow.PipelineWrite did not extract homogeneous step commands into a specialized shard batch. They fell through to the generic command batch path.
```

Change:

```text
- Flow.PipelineWrite extracts step_continue attrs from homogeneous pipeline runs.
- Router.flow_step_continue_batch groups records by shard.
- Raft state machine handles :flow_step_continue_many by applying existing do_flow_step_continue/2 per record inside one pending-write batch.
```

Result:

```text
Before: step 26,238/s, p99 batch 1397.729 ms, errors 0
After:  step 141,601/s, p99 batch 241.304 ms, errors 0
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures

mix test apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:1106
# 1 test, 0 failures
```

## Native protocol FLOW.SIGNAL shard-batched signal-many path

Server: local source release, 16 shards, fresh `/tmp/ferricstore-native-signal-batch` data dir, native port 19488.
Client: one native connection, `protocol_lanes=32`, `inflight_batches=64`, `batch_size=500`, `setup_batch_size=500`, `flows=100000`, `partitions=16`, `payload_bytes=0`, `retention_ttl_ms=0`.

Bug/perf gap:

```text
FLOW.SIGNAL compact mode 11 had a native protocol fast path, but Flow.PipelineWrite did not extract homogeneous signal commands into a specialized shard batch. They fell through to the generic command batch path.
```

Change:

```text
- Flow.PipelineWrite extracts signal attrs from homogeneous pipeline runs.
- Router.flow_signal_batch groups records by shard.
- Raft state machine handles :flow_signal_many by applying existing do_flow_signal/2 per record inside one pending-write batch.
```

Result:

```text
Before: signal 46,064/s, p99 batch 813.262 ms, errors 0
After:  signal 197,795/s, p99 batch 172.041 ms, errors 0
```

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
# 91 tests, 0 failures
```

## 2026-06-12 - Native Flow terminal independent pipeline skips cross-terminal pre-read

Change:

- `Ferricstore.Flow.PipelineWrite.batch_independent/3` now routes terminal runs through `Router.flow_terminal_command_batch_independent/2`.
- This keeps normal public terminal semantics unchanged, but native compact independent pipelines no longer pre-read/decode Flow records just to decide cross-shard terminal handling.
- Added regression coverage proving `pipeline_write_batch_independent` terminal commands do not call the cross-terminal pre-read hook.

Correctness checks:

```text
mix test apps/ferricstore/test/ferricstore/flow_test.exs:490
23 tests, 0 failures

mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
91 tests, 0 failures

mix test apps/ferricstore/test/ferricstore/flow_write_contract_test.exs
56 tests, 0 failures
```

Server:

```text
source release, native enabled, 16 shards
FERRICSTORE_NATIVE_PORT=7386
```

Benchmark shape:

```text
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --flows 100000 \
  --batch-size 500 \
  --inflight-batches 64 \
  --partitions 16 \
  --protocol-lanes 32
```

Results:

```text
complete-many: 159,231/s, p50 160.320 ms, p99 275.734 ms
retry-many:    165,078/s, p50 172.055 ms, p99 224.655 ms
fail-many:     143,100/s, p50 177.034 ms, p99 286.025 ms
cancel-many:   223,648/s, p50 124.924 ms, p99 139.625 ms
```

Comparison to earlier sweep:

```text
complete-many: 107,500/s -> 159,231/s
retry-many:     89,346/s -> 165,078/s
fail-many:      59,935/s -> 143,100/s
cancel-many:   108,227/s -> 223,648/s
```

Notes:

- The optimization is specific to independent terminal pipelines. Parent/child cross-shard terminal orchestration remains available through the normal non-independent paths.
- Setup time is excluded from `items_per_sec`, same as prior Flow command sweep.

## 2026-06-12 - Native Flow claim/read samples after terminal pipeline fix

Server:

```text
same source release run as terminal benchmark
native enabled, 16 shards, one native connection
```

Benchmark shape:

```text
--flows 100000 --inflight-batches 64 --partitions 16 --protocol-lanes 32
```

Results:

```text
claim-due, batch 500:     171,085/s, p50 177.457 ms, p99 205.014 ms
flow-get-meta, batch 250:  86,870/s, p50 176.133 ms, p99 249.752 ms
flow-list-meta, batch 250: 94,002/s, p50 159.895 ms, p99 239.621 ms
flow-get, batch 250:       58,072/s, p50 271.470 ms, p99 367.197 ms
flow-list, batch 250:      61,638/s, p50 237.457 ms, p99 334.632 ms
```

Interpretation:

- `claim-due` is no longer the low-throughput path in this command sweep.
- Full `FLOW.GET`/`FLOW.LIST` are materially slower than `RETURN META`, so the remaining read cost is mostly full record materialization/encoding rather than index discovery.

## 2026-06-12 - Rejected Flow record fixed-field encoder attempt

Attempt:

- Tried a fixed known-atom-field encoder for compact Flow records, with fallback to the dynamic encoder for unknown extension fields.
- Added codec tests for known atom-field records and unknown extension field preservation.

Correctness:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs
52 tests, 0 failures
```

Benchmark shape:

```text
same native source release shape as previous read samples
--flows 100000 --batch-size 250 --inflight-batches 64 --partitions 16 --protocol-lanes 32
```

Result after fast-path attempt:

```text
flow-get:      53,076/s, p50 284.989 ms, p99 438.423 ms
flow-list:     54,718/s, p50 280.010 ms, p99 393.206 ms
flow-get-meta: 74,078/s, p50 208.036 ms, p99 305.205 ms
```

Decision:

- Rejected and reverted server fast path because it regressed throughput versus the previous samples.
- Kept the codec correctness tests because they protect compact Flow record behavior.

## 2026-06-12 - Accepted FLOW.GET compact-values direct encoding

Change:

- `FLOW.GET` compact pipeline with `compact_values=true` now encodes directly from raw Flow results.
- Avoids building request triples and `[status, value]` pairs only to strip them back into values.
- Pair/map return modes keep the previous path.

Correctness checks:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
91 tests, 0 failures

mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs
52 tests, 0 failures
```

Benchmark shape:

```text
source release, native enabled, 16 shards, one native connection
--flows 100000 --batch-size 250 --inflight-batches 64 --partitions 16 --protocol-lanes 32
```

Results after accepted change:

```text
flow-get:      70,895/s, p50 217.258 ms, p99 275.398 ms
flow-get-meta: 102,222/s, p50 153.088 ms, p99 197.112 ms
flow-list:     77,029/s, p50 205.885 ms, p99 274.082 ms
```

Comparison to pre-change same-session samples:

```text
flow-get:      58,072/s -> 70,895/s
flow-get-meta: 86,870/s -> 102,222/s
flow-list:     61,638/s -> 77,029/s
```

Notes:

- `flow-list` did not directly use this branch, but improved on the fresh restarted server. Treat it as a fresh sample, not a causal claim.

## 2026-06-12 - Native KV 30-second GET/SET baseline after Flow protocol optimizations

Server:

```text
source release, native enabled, 16 shards, one native protocol connection
native URL: ferric://127.0.0.1:7386
```

Commands:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --preset set-throughput \
  --test-time 30 \
  --pretty

python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --preset get-throughput \
  --test-time 30 \
  --pretty
```

Results:

```text
SET: 240,950/s, p50 128.286 ms, p95 199.004 ms, p99 233.415 ms
GET: 1,340,240/s, p50 46.628 ms, p95 78.615 ms, p99 99.724 ms
```

Run details:

```text
SET: request_mode=many, pipeline=500, protocol_lanes=64, total_connections=1, requests=7,265,000, errors=0
GET: request_mode=many, pipeline=1000, protocol_lanes=64, total_connections=1, requests=40,235,000, warmed_keys=100000, errors=0
```

Notes:

- These are native protocol one-connection preset numbers.
- Do not compare directly to RESP/memtier c=200, threads=4, pipeline=50 numbers, which use 800 total connections.

## 2026-06-12 - Accepted compact-values shortcut for native collection reads

Change:

- Compact pipeline collection/read commands now skip `["ok", value]` pair allocation when the requested return format is values.
- Pair, map, and compact response modes keep the previous formatting path.
- Affected native compact modes include HGET, HMGET, HGETALL, SISMEMBER, SMEMBERS, LRANGE, ZSCORE, ZRANGE, and grouped LPUSH/RPUSH fallback-success reads/writes where applicable.

Correctness check:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
91 tests, 0 failures
```

Benchmark shape:

```text
source release, native enabled, 16 shards, one native connection
--test-time 30 --request-mode many --pipeline 1000 --protocol-lanes 64 --clients 1 --threads 1
LRANGE/ZRANGE use bounded range 0..0
```

Results after change:

```text
HGET:      556,737/s, p50 112.916 ms, p99 139.636 ms
HMGET:     319,038/s, p50 197.835 ms, p99 242.623 ms
HGETALL:   329,070/s, p50 191.054 ms, p99 329.253 ms
SISMEMBER: 557,260/s, p50 111.293 ms, p99 170.483 ms
SMEMBERS:  367,742/s, p50 168.238 ms, p99 273.962 ms
LRANGE:    349,739/s, p50 177.847 ms, p99 253.990 ms
ZSCORE:    475,162/s, p50 131.886 ms, p99 187.960 ms
ZRANGE:    322,505/s, p50 191.056 ms, p99 280.316 ms
```

Notes:

- These are one-connection native protocol numbers.
- Client CPU is high on scalar reads, so the next gains likely require client decode/encode improvement or more compact dedicated scalar payloads, not more server pair-format cleanup alone.

## 2026-06-12 - Native Flow command coverage after collection-read shortcut

Server:

```text
source release, native enabled, 16 shards, one native connection
native URL: ferric://127.0.0.1:7386
```

Benchmark shape:

```text
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --flows 100000 \
  --batch-size 250 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 128 \
  --read-duration 30
```

Results:

```text
value-put:       115,107/s, p50 146.905 ms, p99 235.530 ms, duration window
value-put-ok:    211,973/s, p50  72.863 ms, p99 121.902 ms, duration window
value-put-owned:  33,491/s, p50 400.956 ms, p99 833.332 ms, fixed 100k items plus setup
value-mget:    1,633,069/s, p50   9.724 ms, p99  12.351 ms, duration window
flow-history:    365,256/s, p50  42.755 ms, p99  68.969 ms, hot/eventual, duration window
flow-list:        58,728/s, p50 273.561 ms, p99 354.275 ms, duration window
flow-list-meta:   86,791/s, p50 185.751 ms, p99 242.469 ms, duration window
signal:           72,216/s, p50 195.921 ms, p99 330.594 ms, fixed 100k items plus setup
step:             42,313/s, p50 367.998 ms, p99 494.711 ms, fixed 100k items plus setup
```

Notes:

- `FLOW.VALUE.MGET` is strong and appears client CPU-bound.
- Hot/eventual `FLOW.HISTORY` is not the bottleneck in this shape.
- `FLOW.LIST` full records are much slower than `FLOW.LIST RETURN META`, pointing to full record materialization/encoding cost.
- `STEP` is slower than signal and should be inspected separately if step-style workflows become a primary path.

## 2026-06-12 - FLOW.LIST RETURN META embedded correctness and benchmark hygiene finding

Change kept:

- `Ferricstore.Flow.list(..., return: :meta)` now trims heavy fields through the embedded/public API.
- Native/protocol `FLOW.LIST RETURN META` keeps the previous fast path: read full records, then trim in the native command layer.
- A attempted meta-only decode path using `decode_record_meta/1` was rejected because it did not improve protocol throughput in repeated samples.

Correctness checks:

```text
mix test apps/ferricstore/test/ferricstore/flow_test.exs:496
5 tests, 0 failures

mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
91 tests, 0 failures
```

Important benchmark hygiene finding:

- A stale FerricStore release BEAM was found consuming ~558% CPU during earlier `FLOW.LIST` samples.
- The low `flow-list-meta` samples around 70k-83k/s were contaminated and should not be treated as product numbers.
- For trustworthy local protocol benchmarks, kill stale `_build/prod/rel/ferricstore` BEAM/erlexec processes before starting one clean source release server.

Clean-server native protocol samples after stale process cleanup:

```text
flow-list-meta sample 1: 139,433/s, p50 109.373 ms, p99 183.917 ms, setup 1.966s
flow-list-meta sample 2: 110,206/s, p50 144.179 ms, p99 205.319 ms, setup 2.436s
```

Notes:

- Same benchmark shape as previous Flow read coverage: one native connection, 32 lanes, 100k prepared flows, 30s read window.
- The second sample ran on the same server after more benchmark-created data accumulated; clean-server first samples are the better comparison point.

## 2026-06-12 - Rejected FLOW.STEP_CONTINUE batched state-machine fast path

Attempt:

- Added a `FLOW.STEP_CONTINUE` batch fast path inside the Raft state machine.
- Reused existing transition-many batched value writes, index moves, state record writes, and async history projection.
- Added a TDD guard that duplicate step items in one native pipeline preserve independent per-item behavior: first succeeds, second returns `ERR stale flow lease`.

Correctness checks:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
92 tests, 0 failures

mix test apps/ferricstore/test/ferricstore/flow_write_contract_test.exs
56 tests, 0 failures
```

Benchmark shape:

```text
source release, native enabled, 16 shards, one native connection
operation=step, flows=100000, batch_size=250, setup_batch_size=500,
inflight_batches=64, protocol_lanes=32, partitions=16, payload_bytes=128
```

Results:

```text
Previous accepted comparable sample: ~42,313/s, p50 367.998 ms, p99 494.711 ms
Fast path using generic batch read: 33,565/s, p50 356.135 ms, p99 1125.708 ms
Fast path using direct per-record reads: 27,744/s, p50 475.611 ms, p99 1480.227 ms
Runtime fast path reverted: 34,453/s, p50 367.770 ms, p99 964.967 ms
```

Decision:

- Rejected runtime fast path; it regressed throughput and tail latency.
- Kept only the duplicate pipeline correctness test.
- Likely reason: `FLOW.STEP_CONTINUE` is not helped by the transition-many batch primitives at this batch shape; extra planning/list work and batch write shape outweigh the savings.
- Future step optimization should focus on native/fused record update or protocol/client response encoding, not simply reusing transition-many apply.

## 2026-06-12 - Accepted FLOW.LIST auto-hot chunk-size tuning

Question answered:

- Default hot `FLOW.LIST` does **not** use LMDB for index discovery.
- Hot/default list uses RAM/native ordered indexes:
  - count active auto-partition state indexes
  - rank-range slice matching RAM/native indexes
  - hydrate current Flow state records from hot storage/keydir path
- LMDB is used when `include_cold`, `consistent`, or terminal/cold query behavior requires it.
- Cold hydration can use batched pread paths; on Linux those may use io_uring-backed Bitcask/ColdRead support, but hot list is not "LMDB index then io_uring fetch" by default.

Change:

- `FLOW.LIST` auto-hot chunk size changed from `ceil(count / active_source_count)` to `min(count, 64)`.
- Purpose: reduce repeated native index rank-range calls for top-N list over many auto buckets.
- This overfetches index entries only; record hydration still happens after global top-N merge.

Correctness checks:

```text
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
92 tests, 0 failures

mix test apps/ferricstore/test/ferricstore/flow_test.exs
222 tests, 0 failures
```

Benchmark shape:

```text
source release, native enabled, 16 shards, one native connection
flows=100000, batch_size=250, setup_batch_size=500, inflight_batches=64,
protocol_lanes=32, partitions=16, payload_bytes=128, read_duration=30
```

Results after change:

```text
flow-list-meta: 98,160/s, p50 160.219 ms, p99 234.971 ms, errors 0
flow-list:      71,151/s, p50 216.998 ms, p99 339.063 ms, errors 0
```

Baseline reference from prior accepted command coverage:

```text
flow-list-meta: 86,791/s, p50 185.751 ms, p99 242.469 ms
flow-list:      58,728/s, p50 273.561 ms, p99 354.275 ms
```

Decision:

- Accepted. This improves hot auto-list throughput and latency without changing write path or durability semantics.

## 2026-06-12 - Accepted FLOW.VALUE.PUT owned encode/digest reuse

Change:
- Optimized normal non-blob owned `FLOW.VALUE.PUT` to encode the value once, hash the encoded binary, and write the same encoded binary.
- Kept blob-ref values on the existing generic path.
- Added native protocol regression coverage for idempotent same-digest owned value puts in one compact pipeline.

Correctness:
- `mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs`
  - 93 tests, 0 failures
- `mix test apps/ferricstore/test/ferricstore/flow_named_values_test.exs apps/ferricstore/test/ferricstore/flow/pipeline_write_test.exs`
  - 20 tests, 0 failures

Benchmark shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --operation value-put-owned \
  --flows 100000 \
  --batch-size 250 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 128 \
  --read-duration 30
```

Result:
- `value-put-owned`: 39,866.82/s
- p50: 383.752 ms
- p95: 500.898 ms
- p99: 505.423 ms
- errors: 0
- client CPU: 5.55%

Comparable recent baseline:
- `value-put-owned`: ~33,491/s
- p50: ~400.956 ms
- p99: ~833.332 ms

Decision: keep. It improves throughput and tail latency without changing externally visible semantics.

## 2026-06-12 - Accepted compact FLOW.GET meta projection removal

Change:
- Removed redundant `FlowRecordProjection.meta/1` map copy in native compact `FLOW.GET` meta mode.
- `FlowCodec.decode_record_meta/1` already returns the same meta projection.
- Full `FLOW.GET` mode remains unchanged.

Correctness:
- `mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs`
  - 93 tests, 0 failures

Benchmark shape:

```bash
python examples/protocol_flow_commands_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --operation flow-get-meta \
  --flows 100000 \
  --batch-size 250 \
  --setup-batch-size 500 \
  --inflight-batches 64 \
  --connections 1 \
  --protocol-lanes 32 \
  --partitions 16 \
  --payload-bytes 128 \
  --read-duration 30
```

Result:
- `flow-get-meta`: 100,681.88/s
- p50: 153.368 ms
- p95: 214.233 ms
- p99: 231.045 ms
- errors: 0
- client CPU: 92.20%

Comparable baseline from same session:
- `flow-get-meta`: 95,898.35/s
- p50: 162.358 ms
- p99: 239.421 ms

Regression check:
- `flow-get`: 72,096.34/s, p50 214.984 ms, p99 328.892 ms
- Previous same-session sample: 73,402.07/s, p50 203.001 ms, p99 354.423 ms

Decision: keep. Meta reads improve ~5%, and full reads remain within normal run variance.

## 2026-06-12 - Native KV one-connection 30s baseline sweep

Server:
- Local source release
- Native protocol enabled on `ferric://127.0.0.1:7386`
- 16 shards
- Fresh data dir

Preset baseline:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:7386 --preset set-throughput --pretty
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:7386 --preset get-throughput --pretty
```

Results:
- `SET`, many/MSET shape, one connection, lanes=64, pipeline=500: 493,826/s, p50 63.265 ms, p99 131.322 ms, errors 0
- `GET`, many/MGET shape, one connection, lanes=64, pipeline=1000: 1,625,276/s, p50 39.296 ms, p99 73.299 ms, errors 0

Binary-key capacity check:

```bash
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:7386 --preset get-throughput --binary-keys --pretty
python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:7386 --preset set-throughput --binary-keys --pretty
```

Results:
- `GET`, binary keys: 2,140,576/s, p50 28.342 ms, p99 61.921 ms, errors 0
- `SET`, binary keys: 508,852/s, p50 60.066 ms, p99 137.482 ms, errors 0

Pipeline-vs-many check:
- `GET`, pipeline mode, one connection, lanes=64, pipeline=1000: 1,648,916/s, p50 38.734 ms, p99 73.417 ms
- `SET`, pipeline mode, one connection, lanes=64, pipeline=500: 511,934/s, p50 62.181 ms, p99 110.131 ms

Read:
- Pipeline and explicit many are similar for scalar KV.
- Binary-key result shows client/key encoding shape is a large part of one-client GET throughput.
- SET remains durable-write/server-side dominated.

## 2026-06-12 - Native data-structure protocol sweep, one connection

Server:
- Local source release
- Native protocol enabled on `ferric://127.0.0.1:7386`
- 16 shards
- Same clean server as KV baseline sweep

Benchmark shape for all commands:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --command <cmd> \
  --request-mode pipeline \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --test-time 30 \
  --prebuild-keys \
  --pretty
```

Results:

| Command | Throughput | p50 batch ms | p99 batch ms | Client CPU | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| HSET | 243,697/s | 132.489 | 271.041 | 9.29% | durable compound write |
| HGET | 1,150,613/s | 27.911 | 52.382 | 68.36% | read/decode heavy |
| LPUSH | 171,268/s | 185.008 | 327.240 | 6.25% | slowest write in sweep |
| LRANGE 0..0 | 540,037/s | 60.332 | 91.207 | 51.96% | bounded one item |
| SADD | 430,397/s | 72.191 | 122.338 | 15.25% | write path |
| SISMEMBER | 1,233,662/s | 26.645 | 40.981 | 59.41% | scalar read |
| SMEMBERS | 442,864/s | 73.779 | 127.907 | 38.32% | one-member list response |
| ZADD | 339,584/s | 71.526 | 1,311.432 | 20.38% | write path; tail spike in this run |
| ZSCORE | 1,095,296/s | 29.853 | 47.125 | 65.34% | scalar read |
| ZRANGE 0..0 | 475,360/s | 64.334 | 117.809 | 46.84% | bounded one item |

Read:
- Scalar reads are mostly client/decode constrained at one connection.
- Compound writes are server/durable-path constrained.
- `LPUSH` is the slowest write family in this sweep and is the next candidate to inspect.

## 2026-06-12 - Native LRANGE boundary fast path accepted

Change:
- Added a ListOps fast path for `LRANGE 0 0` and `LRANGE -1 -1` on multi-element lists.
- The command now uses list metadata boundaries to read the requested edge element directly instead of scanning the whole list.
- Rejected compact list metadata storage after benchmark: it made LPUSH gains unstable and initially regressed LRANGE. Metadata stays Erlang term encoded; only the boundary read fast path remains.

Correctness checks:

```bash
mix test \
  apps/ferricstore/test/ferricstore/store/list_ops_test.exs \
  apps/ferricstore/test/ferricstore/store/list_op_quorum_test.exs \
  apps/ferricstore/test/ferricstore/commands/list_test.exs \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
```

Result:
- `166 tests, 0 failures` in `ferricstore`
- `93 tests, 0 failures` in `ferricstore_server`

Benchmark shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --command lrange \
  --request-mode pipeline \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --test-time 30 \
  --prebuild-keys \
  --pretty
```

Server:
- Local source release
- Native protocol enabled on `ferric://127.0.0.1:7386`
- 16 shards
- Clean data dir before LPUSH/LRANGE validation

Results:

| Command | Throughput | p50 batch ms | p99 batch ms | Notes |
| --- | ---: | ---: | ---: | --- |
| LRANGE 0..0 baseline | 540,037/s | 60.332 | 91.207 | historical sweep |
| LRANGE 0..0 first accepted sample | 596,747/s | 50.338 | 131.107 | after LPUSH-created multi-element lists |
| LRANGE 0..0 repeat accepted sample | 686,477/s | 47.191 | 75.182 | repeated on same multi-element shape |

Read:
- Accepted: LRANGE boundary fast path improves throughput and median/tail latency on repeat sample.
- Rejected: compact binary list metadata storage. It was not kept because it did not produce stable LPUSH improvement and hurt LRANGE before the boundary fast path.

## 2026-06-12 - Native ZRANGE current baseline check, no code change

Reason:
- After LRANGE optimization, inspected `ZRANGE 0 0` for a similar bounded-read issue.
- `ZRANGE` already uses `Ops.zset_rank_range`, which can hit the zset index path; no low-risk code change was needed.

Benchmark shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --command zrange \
  --request-mode pipeline \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --test-time 30 \
  --prebuild-keys \
  --pretty
```

Server:
- Local source release
- Native protocol enabled on `ferric://127.0.0.1:7386`
- 16 shards
- Clean data dir

Results:

| Command | Throughput | p50 batch ms | p99 batch ms | Notes |
| --- | ---: | ---: | ---: | --- |
| ZRANGE 0..0 historical sweep | 475,360/s | 64.334 | 117.809 | old data-structure sweep |
| ZRANGE 0..0 current sample | 929,127/s | 32.570 | 61.219 | no code change in this step |

Read:
- No ZRANGE refactor accepted here; current path is already healthy enough relative to the prior sweep.

## 2026-06-12 - Native candidate command current baselines, no code change

Reason:
- Rechecked old weak candidates before editing. Several old sweep numbers were stale after previous protocol/client/server changes.

Benchmark shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --command <cmd> \
  --request-mode pipeline \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --test-time 30 \
  --prebuild-keys \
  --pretty
```

Server:
- Local source release
- Native protocol enabled on `ferric://127.0.0.1:7386`
- 16 shards
- Clean data dir

Results:

| Command | Historical throughput | Current throughput | Current p50 batch ms | Current p99 batch ms | Read |
| --- | ---: | ---: | ---: | ---: | --- |
| SMEMBERS | 442,864/s | 950,030/s | 29.866 | 69.992 | healthy now; no code change |
| HGET | 1,150,613/s | 1,580,388/s | 20.098 | 34.133 | healthy/client-bound; no code change |
| HSET | 243,697/s | 253,318/s | 129.506 | 267.171 | durable-write bound; no code change |

Read:
- Do not optimize SMEMBERS/HGET based on old numbers; current path is much healthier.
- HSET remains a durable compound write path and is not a quick command-layer read optimization.

## 2026-06-12 - Native compact LPUSH mixed-key early fallback accepted

Change:
- Optimized compact LPUSH/RPUSH handling for mixed-key batches.
- Previous path built a full key-group map, then fell back when more than one key existed.
- New path detects the second distinct key and immediately falls back to the shard batch write path.
- Same-key compact LPUSH/RPUSH still uses grouped high-level push so duplicate-key per-command replies stay correct.

Correctness check:

```bash
mix test apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
```

Result:
- `93 tests, 0 failures`

Benchmark shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --command lpush \
  --request-mode <many|pipeline> \
  --pipeline 500 \
  --clients 1 \
  --threads 1 \
  --inflight-batches 64 \
  --protocol-lanes 64 \
  --test-time 30 \
  --prebuild-keys \
  --pretty
```

Server:
- Local source release
- Native protocol enabled on `ferric://127.0.0.1:7386`
- 16 shards
- Clean data dir

Results:

| Command | Mode | Throughput | p50 batch ms | p99 batch ms | Notes |
| --- | --- | ---: | ---: | ---: | --- |
| LPUSH | many/compact | 218,062/s | 157.311 | 239.666 | compact mode; mixed-key early fallback applies |
| LPUSH | pipeline | 179,641/s | 183.058 | 271.388 | no regression vs historical pipeline sweep |

Read:
- Accepted. The change reduces wasted server-side grouping work in compact mixed-key LPUSH/RPUSH and preserves same-key grouped semantics.

## 2026-06-12 - Native protocol DBOS-style Flow sustained throughput check

Reason:
- Rechecked the native protocol DBOS-style queue/workflow path after protocol and projection changes.
- The 100k run is acceptable, but the fresh 1M run exposes a sustained throughput regression that still needs investigation.

Benchmark shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:7386 \
  --flows <100000|1000000> \
  --server-shards 16
```

Server:
- Local source release
- Native protocol enabled on `ferric://127.0.0.1:7386`
- 16 shards
- Clean data dir before the fresh 1M run

Results:

| Flows | E2E workflows/s | Create workflows/s | Process workflows/s | Queue p50 ms | Queue p95 ms | Queue p99 ms | Claim calls | Empty claims | Avg claim batch | Notes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100,000 | 55,087 | 57,475 | 55,185 | 20.134 | 89.758 | 268.724 | 264 | 1 | 378.79 | good short run |
| 1,000,000 | 11,065 | 11,088 | 11,066 | 137.244 | 401.596 | 586.771 | 2,057 | 0 | 486.14 | fresh server/data; sustained regression |

Read:
- The 1M bottleneck is not obvious worker wake churn: empty claims were zero and average claim batch was high.
- Client CPU was low in the 1M run, so this does not look primarily client-bound.
- Current suspicion is durable Flow create/complete apply pressure: WARaft/Bitcask batching, state/history writes, or async projection descriptor construction/enqueueing.
- Keep this as an open investigation before accepting the native Flow path as done.

## 2026-06-13 IDT protocol defaults latency recovery

Server source checkout: `/Users/yoavgea/repos/ferricstore`, branch `native-protocol`.
Python SDK checkout: `/Users/yoavgea/repos/ferricstore-python`, branch `native-tcp-transport`.
Transport: `ferric://127.0.0.1:16388` only, 16 FerricStore shards, one protocol connection for worker/KV presets.
Data dir: clean `/tmp/ferricstore-protocol-bench/default-final-data` for the final DBOS/KV sequence.

Change validated before final run:

- `waraft_commit_batch_adaptive` default changed to `true` in normal, bench, and runtime config.
- Protocol DBOS producer queue-latency target default changed from `1000ms` to `40ms`.
- `producer_max_pending_credits` remains `0` because a `4000` credit cap reduced throughput and worsened p99 in this local run.

### DBOS-style queued Flow, 100k, protocol default after fix

Command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
created: 100000
completed: 100000
errors/duplicates: 0
end_to_end_flows_per_sec: 76,934.60/s
create_flows_per_sec: 81,194.31/s
process_flows_per_sec: 77,108.03/s
queue_latency_avg_ms: 20.850
queue_latency_p50_ms: 18.056
queue_latency_p95_ms: 38.579
queue_latency_p99_ms: 65.312
queue_latency_max_ms: 105.855
process_claim_calls: 261
process_empty_claims: 2
process_avg_claim_batch: 383.14
process_max_claim_batch: 778
producer_backpressure_limited_batches: 11
producer_backpressure_wait_ms: 0.0
```

Regression comparison from this session:

```text
Before adaptive default fix, same 100k shape: ~53.9k-54.9k/s, p99 ~98-115ms.
With explicit FERRICSTORE_WARAFT_COMMIT_BATCH_ADAPTIVE=true: ~72.9k/s, p99 ~51.9ms.
After making adaptive + 40ms queue target defaults: ~76.9k/s, p99 ~65.3ms.
```

Trace finding:

```text
With fixed WAL batching, traced single FLOW.CREATE/CLAIM_DUE/COMPLETE was dominated by server_ra_wait_us / server_waraft_acceptor_commit_us at ~7.6-8.1ms avg. Flow apply/index/Bitcask work was sub-ms. This identified Ra/WAL commit batching config as the main regression lever, not Flow state-machine logic.
```

### Protocol KV SET throughput, durable write path, 30s

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --key-prefix proto-final-set \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: set
request_mode: many
pipeline: 500
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
requests: 43,141,500
requests_per_sec: 1,437,107.81/s
batch_latency_avg_ms: 21.990
batch_latency_p50_ms: 14.291
batch_latency_p95_ms: 53.768
batch_latency_p99_ms: 213.279
batch_latency_max_ms: 264.605
errors: 0
client_cpu_percent: 16.96
```

### Protocol KV GET throughput, 30s

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --key-prefix proto-final-get \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: get
request_mode: many
pipeline: 1000
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
warmed_keys: 1,000,000
requests: 131,719,000
requests_per_sec: 4,388,579.39/s
batch_latency_avg_ms: 14.403
batch_latency_p50_ms: 14.596
batch_latency_p95_ms: 22.013
batch_latency_p99_ms: 24.344
batch_latency_max_ms: 31.463
errors: 0
client_cpu_percent: 102.35
```

## 2026-06-13 IDT protocol DBOS default retune and KV final check

Reason:
- The `40ms` DBOS producer queue-latency target recovered short-run latency, but throttled sustained 1M runs too aggressively.
- Retuned the default target to `250ms`, keeping the lower-latency producer feedback loop without the 1M throughput collapse.
- All runs below used the `ferric://` protocol path only.

Server:
- Local source checkout: `/Users/yoavgea/repos/ferricstore`, branch `native-protocol`
- `MIX_ENV=prod`
- Native protocol: `ferric://127.0.0.1:16388`
- RESP port was started by the server but not used by these benchmark clients
- `FERRICSTORE_NATIVE_ENABLED=true`
- `FERRICSTORE_SHARD_COUNT=16`
- `FERRICSTORE_RELEASE_CURSOR_INTERVAL=500`
- Clean data dir before each DBOS tuning/final run: `/tmp/ferricstore-protocol-bench/data`
- Normal defaults kept after testing: `FERRICSTORE_FLOW_LMDB_FLUSH_INTERVAL_MS=1000`

Correctness/default test:

```bash
pytest tests/test_protocol_dbos_benchmark.py tests/test_dbos_style_benchmark.py -q
```

Result:

```text
31 passed
```

### DBOS producer target tuning, 1M flows

Same command shape for each row, changing only `--producer-target-queue-latency-ms`.

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 1000000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Results:

| Target ms | E2E workflows/s | Create/s | Process/s | Queue p50 ms | Queue p95 ms | Queue p99 ms | Producer limited batches | Empty claims | Avg claim batch | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 40 | 47,828 | 47,978 | 47,854 | 33.970 | 76.223 | 120.408 | 958 | 0 | 487.57 | rejected: over-throttles sustained 1M |
| 100 | 45,444 | 45,591 | 45,467 | 37.719 | 84.661 | 128.442 | 21 | 2 | 487.09 | rejected: worse throughput and tail |
| 250 | 59,636 | 59,865 | 59,674 | 27.830 | 67.239 | 97.251 | 0 | 1 | 487.80 | accepted default target |
| 1000 | 58,766 | 58,991 | 58,805 | 27.870 | 65.628 | 100.293 | 0 | 1 | 486.85 | acceptable but less protective default |

Additional check:
- `FERRICSTORE_FLOW_LMDB_FLUSH_INTERVAL_MS=5000` with target `250ms` produced `53,423/s`, p99 `108.902ms`.
- This did not justify changing the normal LMDB projection flush interval.

### DBOS-style queued Flow, 100k, final default target 250ms

Command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
created: 100000
completed: 100000
errors/duplicates: 0
end_to_end_flows_per_sec: 71,737.73/s
create_flows_per_sec: 77,854.40/s
process_flows_per_sec: 71,881.45/s
queue_latency_avg_ms: 21.308
queue_latency_p50_ms: 17.854
queue_latency_p95_ms: 37.217
queue_latency_p99_ms: 89.675
queue_latency_max_ms: 111.435
process_claim_calls: 270
process_empty_claims: 2
process_avg_claim_batch: 370.37
process_max_claim_batch: 780
producer_backpressure_limited_batches: 0
producer_backpressure_wait_ms: 0.0
```

### DBOS-style queued Flow, 1M, final default target 250ms

Command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 1000000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
created: 1000000
completed: 1000000
errors/duplicates: 0
end_to_end_flows_per_sec: 52,860.30/s
create_flows_per_sec: 53,071.30/s
process_flows_per_sec: 52,890.74/s
queue_latency_avg_ms: 36.152
queue_latency_p50_ms: 30.612
queue_latency_p95_ms: 74.540
queue_latency_p99_ms: 114.509
queue_latency_max_ms: 184.712
process_claim_calls: 2053
process_empty_claims: 1
process_avg_claim_batch: 487.09
process_max_claim_batch: 1000
producer_backpressure_limited_batches: 0
producer_backpressure_wait_ms: 0.0
```

Read:
- The accepted default is based on the best clean tuning sample (`250ms`: `59.6k/s`, p99 `97.3ms`).
- The final default 1M sample showed lower throughput (`52.9k/s`) despite no producer limiting. Treat this as local sustained-run variance or server-side background contention, not as a producer-default regression.
- Stale BEAM benchmark servers were found earlier in the session and killed before these final runs; future runs should always check for old `mix run --no-halt` processes before trusting a regression.

### Protocol KV SET throughput, durable write path, 30s

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --key-prefix proto-default250-set \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: set
request_mode: many
pipeline: 500
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
requests: 53,504,500
requests_per_sec: 1,782,129.85/s
batch_latency_avg_ms: 17.714
batch_latency_p50_ms: 17.534
batch_latency_p95_ms: 22.342
batch_latency_p99_ms: 27.233
batch_latency_max_ms: 47.292
errors: 0
client_cpu_percent: 23.99
```

### Protocol KV GET throughput, 30s

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --key-prefix proto-default250-get \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
command: get
request_mode: many
pipeline: 1000
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
warmed_keys: 1,000,000
requests: 120,398,000
requests_per_sec: 4,011,190.97/s
batch_latency_avg_ms: 15.751
batch_latency_p50_ms: 15.868
batch_latency_p95_ms: 23.779
batch_latency_p99_ms: 26.456
batch_latency_max_ms: 33.187
errors: 0
client_cpu_percent: 104.10
```

## 2026-06-13 IDT WAL commit delay experiments for protocol latency

Reason:
- DBOS queue latency is sensitive to WARaft commit wait.
- KV SET throughput and tail latency benefit from larger durable commit batches.
- Tested lower `FERRICSTORE_WAL_COMMIT_DELAY_US` values without changing correctness: the Ra/Bitcask durable path is the same; only the batching wait window changes.

Server shape:
- Local source checkout: `/Users/yoavgea/repos/ferricstore`, branch `native-protocol`
- Native protocol only: `ferric://127.0.0.1:16388`
- `MIX_ENV=prod`
- `FERRICSTORE_NATIVE_ENABLED=true`
- `FERRICSTORE_SHARD_COUNT=16`
- `FERRICSTORE_RELEASE_CURSOR_INTERVAL=500`
- Clean `/tmp/ferricstore-protocol-bench/data` before each accepted comparison run

DBOS command shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows <100000|1000000> \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

DBOS results:

| WAL delay us | Flows | E2E workflows/s | Create/s | Process/s | Queue p50 ms | Queue p95 ms | Queue p99 ms | Empty claims | Avg claim batch | Read |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 6000 default | 100,000 | 71,738 | 77,854 | 71,881 | 17.854 | 37.217 | 89.675 | 2 | 370.37 | balanced default sample |
| 1000 | 100,000 | 72,431 | 77,596 | 72,611 | 17.814 | 37.219 | 86.490 | 1 | 381.68 | no strong win |
| 0 | 100,000 | 72,467 | 78,230 | 72,659 | 17.523 | 39.553 | 71.574 | 1 | 377.36 | better DBOS p99, but hurts KV SET |
| 3000 | 100,000 | 72,660 | 78,717 | 72,823 | 17.416 | 35.399 | 66.470 | 0 | 377.36 | best short DBOS p99, but hurts KV SET |
| 0 | 1,000,000 | 58,274 | 58,498 | 58,305 | 29.029 | 67.956 | 96.285 | 0 | 488.04 | good Flow latency profile |

KV SET command shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

KV SET results:

| WAL delay us | Requests/s | p50 batch ms | p95 batch ms | p99 batch ms | Max batch ms | Read |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 6000 default | 1,782,130 | 17.534 | 22.342 | 27.233 | 47.292 | best balanced KV SET result |
| 3000 | 1,430,321 | 22.678 | 30.507 | 36.609 | 66.576 | rejected as global default |
| 0 | 1,064,106 | 30.730 | 42.814 | 48.467 | 77.578 | rejected as global default |

Decision:
- Keep `FERRICSTORE_WAL_COMMIT_DELAY_US=6000` as the balanced default because KV SET is a first-class goal and regresses with lower delay.
- For Flow/DBOS latency-only profiling, `3000us` and `0us` are useful workload knobs.
- Do not change server defaults from this experiment.

## 2026-06-13 IDT protocol KV multi-process throughput check

Reason:
- One protocol connection/process GET was client decode CPU-bound at roughly one full Python core.
- Checked whether protocol server read capacity is higher when the benchmark uses more client processes/connections.
- This is a throughput shape, not the one-socket low-latency shape.

Server:
- Local source checkout: `/Users/yoavgea/repos/ferricstore`, branch `native-protocol`
- Native protocol only: `ferric://127.0.0.1:16388`
- `MIX_ENV=prod`
- `FERRICSTORE_NATIVE_ENABLED=true`
- `FERRICSTORE_SHARD_COUNT=16`
- `FERRICSTORE_RELEASE_CURSOR_INTERVAL=500`
- Default `FERRICSTORE_WAL_COMMIT_DELAY_US=6000`
- Clean `/tmp/ferricstore-protocol-bench/data` before each command

### Protocol KV GET, 4 client processes, 4 total protocol connections

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --processes 4 \
  --key-prefix proto-default-mp4-get \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
requests_per_sec: 8,084,460.97/s
requests: 243,611,000
pipeline: 1000
inflight_batches: 64
protocol_lanes: 64
total_connections: 4
batch_latency_avg_ms: 31.151
batch_latency_p50_ms: 31.298
batch_latency_p95_ms: 47.053
batch_latency_p99_ms: 54.218
batch_latency_max_ms: 79.111
errors: 0
client_cpu_percent: 206.51
```

Read:
- Server read path has substantially more capacity than the one-process result shows.
- One-process GET remains the better low-latency shape; four processes are useful when the goal is maximum aggregate read throughput.

### Protocol KV SET, 4 client processes, 4 total protocol connections

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --processes 4 \
  --key-prefix proto-default-mp4-set \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
requests_per_sec: 2,247,860.30/s
requests: 67,721,500
pipeline: 500
inflight_batches: 64
protocol_lanes: 64
total_connections: 4
batch_latency_avg_ms: 56.044
batch_latency_p50_ms: 55.569
batch_latency_p95_ms: 72.648
batch_latency_p99_ms: 80.446
batch_latency_max_ms: 89.793
errors: 0
client_cpu_percent: 30.58
```

Read:
- Four processes improve durable SET throughput from the one-socket `1.78M/s` sample to `2.25M/s`.
- The tail is much worse than one-socket SET (`p99 80ms` vs `27ms`), so one-socket remains the better latency default.

## 2026-06-13 IDT rejected WARaft mailbox-aware adaptive batching experiment

Reason:
- WARaft's adaptive commit decision function accepts a `MailboxBacklog` test parameter, but the live path currently passes `false`.
- Tried changing the live path to inspect leader `message_queue_len` and delay when backlog exists, then flush when the mailbox drains.
- The goal was to keep KV SET batching while reducing Flow DBOS tail latency.

Patch tested:

```text
commit_batch_decision(...) computed:
MailboxBacklog = Adaptive andalso erlang:process_info(self(), message_queue_len) > 0

adaptive decision became:
interval 0 -> flush
pending_count > max -> flush
adaptive && mailbox_not_backlogged -> flush
otherwise -> delay
```

WARaft focused unit test:

```bash
cd /Users/yoavgea/repos/waraft
rebar3 eunit --module=wa_raft_server_adaptive_batch_test
```

Result while patched:

```text
7 tests, 0 failures
```

FerricStore broad restart test:

```bash
mix test apps/ferricstore/test/ferricstore/raft/waraft_backend_test.exs
```

Result:
- Rejected as correctness evidence.
- The broad test produced restart/recovery failures in list/set/zset restart-survival cases and was stopped before completion.
- Those failures may be broader test isolation/fault-injection issues, but this patch is not acceptable without clean correctness evidence.

Protocol DBOS 100k with patched WARaft dependency:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
end_to_end_flows_per_sec: 67,423.65/s
create_flows_per_sec: 69,400.03/s
process_flows_per_sec: 67,573.13/s
queue_latency_avg_ms: 23.225
queue_latency_p50_ms: 17.853
queue_latency_p95_ms: 50.872
queue_latency_p99_ms: 125.035
queue_latency_max_ms: 148.681
process_claim_calls: 261
process_empty_claims: 0
process_avg_claim_batch: 383.14
errors/duplicates: 0
```

Decision:
- Rejected.
- It worsened DBOS p99 versus the accepted default sample and lacked clean correctness evidence.
- WARaft source and FerricStore local `deps/wa_raft` were reverted to original behavior after the experiment.
- Reverted WARaft focused unit test result:

```text
6 tests, 0 failures
```

## 2026-06-13 IDT native protocol trace and current DBOS/KV check

Server:

```bash
MIX_ENV=prod \
FERRICSTORE_NATIVE_ENABLED=true \
FERRICSTORE_SHARD_COUNT=16 \
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 \
FERRICSTORE_DATA_DIR=/tmp/ferricstore-protocol-bench/data \
FERRICSTORE_PORT=16379 \
FERRICSTORE_HEALTH_PORT=16380 \
FERRICSTORE_NATIVE_PORT=16388 \
mix run --no-halt
```

Trace server additionally used:

```bash
FERRICSTORE_NATIVE_TRACE_ENABLED=true
```

Protocol URL only:

```text
ferric://127.0.0.1:16388
```

### Per-stage protocol trace, sequential commands

Command shape:

```bash
python examples/protocol_trace_latency.py --url ferric://127.0.0.1:16388 --op set --samples 120 --warmup 20 --value-bytes 32
python examples/protocol_trace_latency.py --url ferric://127.0.0.1:16388 --op get --samples 120 --warmup 20 --value-bytes 32
python examples/protocol_trace_latency.py --url ferric://127.0.0.1:16388 --op flow --samples 120 --warmup 20 --value-bytes 32
```

Key trace results:

```text
SET:
  client response_read avg/p99: 1000us / 1446us
  server_ra_wait avg/p99:       883us / 1324us
  server_apply avg/p99:         15us / 37us
  bitcask_append avg/p99:       2us / 9us

GET:
  client response_read avg/p99: 62us / 122us
  client decode avg/p99:        6us / 9us
  server_execute avg/p99:       1us / 2us

FLOW.CREATE:
  client response_read avg/p99: 1142us / 1603us
  server_ra_wait avg/p99:       975us / 1420us
  server_apply avg/p99:         120us / 208us
  flow_index_update avg/p99:    3us / 8us

FLOW.CLAIM_DUE:
  client response_read avg/p99: 1053us / 1545us
  server_ra_wait avg/p99:       899us / 1349us
  server_apply avg/p99:         59us / 94us
  flow_index_update avg/p99:    6us / 10us

FLOW.COMPLETE:
  client response_read avg/p99: 1072us / 1556us
  server_ra_wait avg/p99:       931us / 1429us
  server_apply avg/p99:         66us / 134us
  flow_index_update avg/p99:    7us / 21us
```

Read:
- Single-command native protocol path is not the DBOS latency regression source.
- Flow apply/index/Bitcask work is small; the durable command floor is mostly WARaft commit wait.
- GET is already a pure read path and is client decode/read-loop limited at high throughput.

### DBOS native protocol, clean server, 100k

Command shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
end_to_end_flows_per_sec: 77,138.21/s
create_flows_per_sec:     83,558.83/s
process_flows_per_sec:    77,316.25/s
queue_latency_avg_ms:     19.644
queue_latency_p50_ms:     16.975
queue_latency_p95_ms:     33.870
queue_latency_p99_ms:     51.592
queue_latency_max_ms:     105.795
process_claim_calls:      264
process_empty_claims:     2
avg_claim_batch:          378.79
errors/duplicates:        0
```

### DBOS native protocol, clean server, 1M

Same shape, with `--flows 1000000`.

Result:

```text
end_to_end_flows_per_sec: 59,997.08/s
create_flows_per_sec:     60,238.48/s
process_flows_per_sec:    60,036.64/s
queue_latency_avg_ms:     32.015
queue_latency_p50_ms:     27.788
queue_latency_p95_ms:     65.294
queue_latency_p99_ms:     87.719
queue_latency_max_ms:     185.610
process_claim_calls:      2052
process_empty_claims:     0
avg_claim_batch:          487.33
errors/duplicates:        0
```

Read:
- Current native DBOS path is healthy again.
- 100k is ~77k/s with p99 ~52ms.
- Sustained 1M is ~60k/s with p99 ~88ms.
- Empty claims are effectively gone.

### Protocol KV on same machine after DBOS/with local CPU noise

A non-apples-to-apples KV run after heavy Flow writes showed lower numbers:

```text
SET after DBOS-loaded dir:
  requests_per_sec: 646,170.78/s
  p99 batch latency: 121.500ms

GET after DBOS-loaded dir:
  requests_per_sec: 3,674,972.49/s
  p99 batch latency: 29.680ms
```

A fresh-dir KV rerun while the local desktop was busy showed:

```text
SET clean dir:
  requests_per_sec: 1,446,536.18/s
  p99 batch latency: 34.020ms

GET clean dir:
  requests_per_sec: 2,792,925.01/s
  p99 batch latency: 38.070ms

GET clean dir rerun:
  requests_per_sec: 2,883,730.24/s
  p99 batch latency: 36.800ms
```

Local background pressure during these KV runs included Chrome GPU, WindowServer, virtualization, WebKit, Docker Desktop, and Codex processes using visible CPU. Treat these KV numbers as noisy local samples, not accepted regression evidence. The profile still showed GET as single-client Python CPU/read-loop limited.

### Short cProfile on protocol GET

Command:

```bash
python -m cProfile -o /tmp/ferricstore-protocol-get.prof examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --key-prefix proto-profile-get \
  --key-count 1000000 \
  --value-bytes 32 \
  --test-time 5 \
  --pretty
```

Relevant profile read:

```text
_try_decode_custom_kv_mget: main Python decode hotspot
socket.recv/read loop:      dominant wall time including warmup/response waits
client_cpu_percent:         ~102%
```

Decision:
- Do not change server based on this KV sample.
- Next useful GET optimization would need a Python decode fast path or native extension; server trace does not show a server-side GET bottleneck.

## 2026-06-13 IDT compact MGET decode allocation cleanup

Change:
- SDK compact MGET decode now appends returned values instead of preallocating a list and assigning by index.
- Semantics unchanged: returned value is still `list[bytes | None]`.
- This targets the one-socket protocol GET client-side decode hotspot found in cProfile.

Correctness:

```bash
pytest tests/test_protocol.py::test_protocol_submit_mget_sends_direct_compact_bulk_frame \
  tests/test_protocol.py::test_protocol_submit_mget_payload_sends_preencoded_direct_compact_bulk_frame \
  tests/test_protocol.py::test_protocol_fast_decodes_custom_kv_values_from_frame_body_offset \
  tests/test_protocol.py::test_protocol_fast_decodes_pipeline_values_payloads_from_frame_body_offset \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_pipeline_mode_uses_preencoded_get_set_payload_when_available \
  tests/test_protocol_kv_benchmark.py::test_protocol_kv_many_mode_uses_preencoded_mget_payload_when_available -q
```

Result:

```text
7 passed
```

Full protocol SDK tests:

```bash
pytest tests/test_protocol.py -q
```

Result:

```text
105 passed
```

### Protocol KV GET, clean server, one native socket

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --key-prefix proto-decode-patch-get \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
requests_per_sec: 4,362,275.77/s
requests: 130,946,000
pipeline: 1000
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
batch_latency_avg_ms: 14.484
batch_latency_p50_ms: 14.450
batch_latency_p95_ms: 20.854
batch_latency_p99_ms: 23.714
batch_latency_max_ms: 33.261
errors: 0
client_cpu_percent: 103.75
```

Read:
- Better than the previous noisy clean GET samples around `2.8-2.9M/s` and p99 `36-38ms`.
- Still client CPU-bound at one full Python core.

### Protocol KV SET, clean server after GET warmup, one native socket

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --key-prefix proto-decode-patch-set \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Result:

```text
requests_per_sec: 1,950,454.04/s
requests: 58,561,500
pipeline: 500
inflight_batches: 64
protocol_lanes: 64
total_connections: 1
batch_latency_avg_ms: 16.191
batch_latency_p50_ms: 15.908
batch_latency_p95_ms: 20.546
batch_latency_p99_ms: 27.416
batch_latency_max_ms: 123.887
errors: 0
client_cpu_percent: 23.31
```

Read:
- Strong durable SET sample for one native socket.
- The max spike is local noise; p99 stayed under 28ms.

### DBOS 100k sanity on fresh clean server

Command shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
end_to_end_flows_per_sec: 67,396.05/s
create_flows_per_sec:     70,076.20/s
process_flows_per_sec:    67,538.71/s
queue_latency_avg_ms:     23.314
queue_latency_p50_ms:     17.652
queue_latency_p95_ms:     46.645
queue_latency_p99_ms:     159.208
queue_latency_max_ms:     167.890
process_claim_calls:      260
process_empty_claims:     0
avg_claim_batch:          384.62
errors/duplicates:        0
```

Read:
- Functional and correct; no duplicate/claim errors.
- Lower than the prior 77k/s sample, but this change only affects compact KV MGET decode, not Flow create/claim/complete response decode.
- Keep watching DBOS with repeated clean samples before attributing variance to code.

## 2026-06-13 IDT repeated clean DBOS 100k samples after decode cleanup

Purpose:
- Verify whether the lower `67k/s` DBOS sample was a real regression or local variance.
- Each sample used a fresh server and fresh `/tmp/ferricstore-protocol-bench/data`.
- Native protocol only: `ferric://127.0.0.1:16388`.

Command shape for each sample:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Results:

```text
sample 1:
  e2e:                 74,198.99/s
  create:              83,875.49/s
  process:             74,377.15/s
  queue p50/p95/p99:   17.604ms / 35.071ms / 69.883ms
  empty claims:        1 / 271
  avg claim batch:     369.00
  errors/duplicates:   0

sample 2:
  e2e:                 71,204.39/s
  create:              76,016.22/s
  process:             71,356.52/s
  queue p50/p95/p99:   17.452ms / 35.761ms / 148.819ms
  empty claims:        0 / 264
  avg claim batch:     378.79
  errors/duplicates:   0

sample 3:
  e2e:                 68,053.13/s
  create:              72,927.49/s
  process:             68,207.49/s
  queue p50/p95/p99:   17.933ms / 60.553ms / 200.351ms
  empty claims:        1 / 269
  avg claim batch:     371.75
  errors/duplicates:   0
```

Read:
- The previous `67k/s` clean DBOS result is within current local variance, not proof that compact MGET decode cleanup hurt Flow.
- Median queue latency is stable around `17-18ms`.
- Tail latency variance remains the DBOS target: p99 moved from `~70ms` to `~200ms` across clean samples even with zero/near-zero empty claims.

## 2026-06-13 IDT DBOS latency-focused smaller batch experiments

Purpose:
- Check whether DBOS tail latency comes mainly from producer create bursts / large claim batches.
- Fresh server and fresh data dir per variant.
- Native protocol only.

Base shape remains the same as the DBOS 100k benchmark, except changed batch sizes.

### Variant: `create_batch_size=250`, `claim_batch_size=500`

Result:

```text
e2e:                 53,215.48/s
create:              54,379.23/s
process:             53,301.47/s
queue p50/p95/p99:   13.305ms / 29.867ms / 65.007ms
queue max:           174.939ms
empty claims:        0 / 504
avg claim batch:     198.41
errors/duplicates:   0
```

Read:
- Median and p95 improved.
- Throughput dropped too much versus the default `~68-74k/s` band.
- Not accepted as default.

### Variant: `create_batch_size=250`, `claim_batch_size=250`

Result:

```text
e2e:                 51,854.05/s
create:              53,038.23/s
process:             51,933.04/s
queue p50/p95/p99:   11.179ms / 37.107ms / 92.348ms
queue max:           169.844ms
empty claims:        0 / 515
avg claim batch:     194.17
errors/duplicates:   0
```

Read:
- Median queue latency improved further, but p99 did not beat the best default sample and throughput dropped more.
- Not accepted as default.

Decision:
- Smaller create/claim batches are a latency/throughput tradeoff users can tune, but they are not better for the default DBOS benchmark shape.
- Next better knobs are protocol lanes / completion async depth because they may reduce client scheduling wait without reducing server batch efficiency.

## 2026-06-13 IDT DBOS protocol lanes and complete-depth experiments

Purpose:
- Test whether tail latency improves by increasing native protocol lanes or allowing deeper async completion batches.
- Fresh server and fresh data dir per variant.
- Native protocol only.

Base DBOS 100k shape, except changed `--protocol-lanes` and/or `--complete-async-depth`.

### Variant: `protocol_lanes=64`, `complete_async_depth=4`

Result:

```text
e2e:                 64,821.67/s
create:              66,761.58/s
process:             64,941.52/s
queue p50/p95/p99:   17.807ms / 60.809ms / 183.430ms
queue max:           281.869ms
empty claims:        2 / 261
avg claim batch:     383.14
errors/duplicates:   0
```

Decision:
- Worse than default lanes 32.
- Reject for DBOS default.

### Variant: `protocol_lanes=32`, `complete_async_depth=8`

Result:

```text
e2e:                 70,858.28/s
create:              76,113.75/s
process:             71,030.50/s
queue p50/p95/p99:   17.988ms / 48.610ms / 114.174ms
queue max:           158.782ms
empty claims:        0 / 261
avg claim batch:     383.14
errors/duplicates:   0
```

Decision:
- Acceptable, but not clearly better than the default depth 4 samples.
- Do not change default from 4 based on one sample.

### Variant: `protocol_lanes=64`, `complete_async_depth=8`

Result:

```text
e2e:                 64,842.69/s
create:              66,980.45/s
process:             64,975.57/s
queue p50/p95/p99:   17.678ms / 44.471ms / 129.005ms
queue max:           143.243ms
empty claims:        0 / 262
avg claim batch:     381.68
errors/duplicates:   0
```

Decision:
- Worse throughput than default lanes 32.
- Reject for DBOS default.

Read:
- Native lanes 64 does not help this one-socket DBOS shape.
- Completion depth 8 is safe-looking but not proven better than depth 4.
- Current default remains: lanes 32, complete async depth 4, create/claim batch 500.

## 2026-06-13 IDT protocol GET pipeline latency curve, exploratory 10s

Purpose:
- Tune one-socket protocol GET for latency, not only max throughput.
- Same clean server for all GET variants; each run uses its own key prefix and warmup.
- Native protocol only.

Command shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --pipeline <N> \
  --key-prefix proto-get-p<N> \
  --key-count 1000000 \
  --value-bytes 32 \
  --test-time 10 \
  --pretty
```

Results:

```text
pipeline=1000:
  requests_per_sec: 4,319,480.52/s
  p50/p95/p99:      14.495ms / 21.111ms / 23.635ms
  max:              28.255ms
  errors:           0

pipeline=500:
  requests_per_sec: 3,916,639.07/s
  p50/p95/p99:      7.920ms / 11.618ms / 13.042ms
  max:              19.278ms
  errors:           0

pipeline=250:
  requests_per_sec: 3,399,083.46/s
  p50/p95/p99:      4.549ms / 6.042ms / 6.990ms
  max:              12.257ms
  errors:           0

pipeline=100:
  requests_per_sec: 2,319,713.30/s
  p50/p95/p99:      2.661ms / 3.147ms / 3.436ms
  max:              11.132ms
  errors:           0
```

Read:
- `pipeline=1000` is max one-socket throughput.
- `pipeline=500` is the better balanced GET shape: only ~9% less throughput than 1000 with ~45% lower p99 batch latency.
- `pipeline=250` is a strong low-latency shape: ~3.4M/s and p99 under 7ms.
- `pipeline=100` reaches RESP-like p99 batch latency around 3.4ms while still doing ~2.3M/s on one native socket.

## 2026-06-13 IDT protocol SET pipeline latency curve, exploratory 10s

Purpose:
- Tune one-socket durable protocol SET for latency/throughput.
- Fresh server and fresh data dir per variant.
- Native protocol only.

Command shape:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --pipeline <N> \
  --key-prefix proto-set-p<N> \
  --key-count 1000000 \
  --value-bytes 32 \
  --test-time 10 \
  --pretty
```

Results:

```text
pipeline=500:
  requests_per_sec: 1,967,294.62/s
  p50/p95/p99:      15.249ms / 21.688ms / 25.731ms
  max:              51.949ms
  errors:           0

pipeline=250:
  requests_per_sec: 1,253,978.42/s
  p50/p95/p99:      11.603ms / 16.933ms / 21.244ms
  max:              62.552ms
  errors:           0

pipeline=100:
  requests_per_sec: 538,458.94/s
  p50/p95/p99:      11.136ms / 14.738ms / 20.615ms
  max:              44.820ms
  errors:           0
```

Read:
- Durable SET benefits strongly from `pipeline=500`.
- Lower pipeline depths reduce p99 only modestly and cost too much throughput.
- Keep `pipeline=500` for SET throughput/default shape.

## 2026-06-13 IDT current-state DBOS 1M sustained check

Purpose:
- Verify current SDK/server state after compact MGET decode cleanup and tuning experiments.
- Fresh server and fresh data dir.
- Native protocol only.

Command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 1000000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --protocol-worker-connections 1 \
  --protocol-lanes 32 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
e2e:                 59,679.16/s
create:              59,931.71/s
process:             59,715.81/s
queue avg:           33.240ms
queue p50/p95/p99:   27.529ms / 68.421ms / 100.770ms
queue max:           239.291ms
empty claims:        0 / 2052
avg claim batch:     487.33
max claim batch:     1000
errors/duplicates:   0
```

Read:
- Sustained 1M DBOS remains around `60k/s` with p99 around `100ms`.
- Empty claims are zero; worker wake/claim scheduling is no longer wasting calls.
- Current sustained bottleneck is durable create+complete write throughput / WARaft batching, not claim churn.

## 2026-06-13 IDT DBOS client profile note

Purpose:
- Identify whether current DBOS tail/throughput is limited by SDK CPU after claim-churn fixes.
- Profile was run against native protocol with the normal DBOS 100k shape.
- Important caveat: `cProfile` distorts thread scheduling and should not be treated as benchmark evidence.

Useful profile signal:

```text
Visible client CPU hotspots:
  protocol._try_decode_custom_claim_jobs
  client._decode_claim_due_response
  ClaimedItem.from_resp
  protocol._compact_flow_claimed_many_payload
  protocol.encode_value for generic command building
```

Distorted profiled output showed high queue latency and lower throughput, so only relative CPU hotspots are useful.

Read:
- A possible future DBOS SDK optimization is a FlowClient-only compact claim decode path that constructs `ClaimedItem` directly from the compact protocol payload.
- Do not change raw `ProtocolAdapter.execute_command("FLOW.CLAIM_DUE", ...)` to return SDK objects; that would break the low-level protocol contract.
- This was recorded as evidence only; no code change made from this profile.

## 2026-06-13 IDT Flow claim compact-row SDK decode helper

Change:
- Added `ClaimedItem.from_compact_rows(...)`.
- Sync and async Flow clients use it for `job_only` claim responses.
- Raw protocol adapter output remains unchanged; low-level `execute_command("FLOW.CLAIM_DUE", ...)` still returns protocol-shaped rows, not SDK objects.

Correctness:

```bash
pytest \
  tests/test_client.py::test_claimed_item_decodes_compact_rows_without_resp_dict \
  tests/test_client.py::test_claimed_item_compact_rows_fallback_to_resp_maps \
  tests/test_client.py::test_claim_due_can_return_job_only_items \
  tests/test_client.py::test_claim_jobs_can_request_compact_state_items \
  tests/test_client.py::test_claim_jobs_future_uses_protocol_submit_and_decodes_items \
  tests/test_async_client.py::test_async_claim_jobs_and_complete_jobs_use_hot_compact_paths \
  tests/test_async_client.py::test_async_claim_jobs_can_request_compact_state_items \
  tests/test_protocol.py::test_protocol_fast_decodes_custom_flow_claim_jobs \
  tests/test_protocol.py::test_protocol_fast_decodes_custom_claim_jobs_from_frame_body_offset \
  tests/test_protocol.py::test_protocol_decodes_compact_claim_jobs_as_pipeline_values \
  -q
```

Result:

```text
10 passed
```

Broader SDK protocol/client correctness:

```bash
pytest tests/test_client.py tests/test_async_client.py tests/test_protocol.py -q
```

Result:

```text
215 passed
```

Micro-benchmark:

```text
from_resp_loop:      ~945,777 items/s
from_compact_rows:   ~980,096 items/s
```

Read:
- Small CPU win in isolation.
- Worth keeping because it is narrower and preserves protocol semantics.

### DBOS 100k repeated clean samples after helper

Same native protocol DBOS 100k shape as default.

```text
sample 1:
  e2e:                 74,810.17/s
  create:              79,426.34/s
  process:             74,977.70/s
  queue p50/p95/p99:   17.255ms / 33.452ms / 70.953ms
  empty claims:        1 / 261
  avg claim batch:     383.14
  errors/duplicates:   0

sample 2:
  e2e:                 73,413.25/s
  create:              77,662.08/s
  process:             73,571.41/s
  queue p50/p95/p99:   17.487ms / 37.546ms / 82.565ms
  empty claims:        1 / 262
  avg claim batch:     381.68
  errors/duplicates:   0

sample 3:
  e2e:                 60,820.44/s
  create:              62,878.04/s
  process:             60,941.84/s
  queue p50/p95/p99:   18.894ms / 74.346ms / 208.108ms
  empty claims:        0 / 260
  avg claim batch:     384.62
  errors/duplicates:   0
```

Read:
- First two samples are strong and slightly above the previous repeated-sample band.
- Third sample shows local variance remains significant.
- No correctness errors.

### DBOS 1M sustained after helper

Same native protocol DBOS shape with `--flows 1000000`.

```text
e2e:                 60,122.95/s
create:              60,373.23/s
process:             60,159.54/s
queue avg:           33.229ms
queue p50/p95/p99:   27.817ms / 69.968ms / 104.814ms
queue max:           182.478ms
empty claims:        0 / 2052
avg claim batch:     487.33
errors/duplicates:   0
```

Read:
- Sustained 1M remains stable around `60k/s` with p99 around `100ms`.
- Claim churn remains fixed: zero empty claims.

## 2026-06-13 IDT DBOS producer target retune after claim decode helper

Purpose:
- Re-test producer queue-latency target after compact claim-row SDK decode cleanup.
- Fresh server and fresh data dir per 100k variant.
- Native protocol only.

Default DBOS shape, changing only `--producer-target-queue-latency-ms`.

### 100k target sweep

```text
target=150ms:
  e2e:                 71,240.86/s
  create:              92,371.61/s
  process:             71,391.09/s
  queue p50/p95/p99:   17.139ms / 39.966ms / 70.623ms
  queue max:           290.833ms
  empty claims:        4 / 271
  avg claim batch:     369.00
  errors/duplicates:   0

target=100ms:
  e2e:                 74,678.90/s
  create:              83,083.58/s
  process:             74,849.31/s
  queue p50/p95/p99:   17.912ms / 35.688ms / 51.083ms
  queue max:           176.919ms
  empty claims:        2 / 269
  avg claim batch:     371.75
  errors/duplicates:   0

target=75ms:
  e2e:                 62,650.90/s
  create:              65,604.03/s
  process:             62,766.39/s
  queue p50/p95/p99:   18.535ms / 57.080ms / 166.281ms
  queue max:           198.838ms
  empty claims:        1 / 262
  avg claim batch:     381.68
  errors/duplicates:   0
```

Read:
- `100ms` was the only clear winner in this sweep: strong throughput and best p99.
- `75ms` is too aggressive and lowers throughput.
- `150ms` is fine but not better than `100ms`.

### 1M validation with target 100ms

Same DBOS shape, `--flows 1000000 --producer-target-queue-latency-ms 100`.

```text
e2e:                 60,110.95/s
create:              60,387.42/s
process:             60,150.26/s
queue avg:           32.297ms
queue p50/p95/p99:   27.931ms / 61.087ms / 96.214ms
queue max:           253.801ms
empty claims:        0 / 2055
avg claim batch:     486.62
errors/duplicates:   0
```

Decision:
- Change protocol DBOS benchmark default target from `250ms` to `100ms`.
- Rationale: same sustained throughput area as `250ms`, slightly better 1M p99, and much better 100k p99 in this sweep.

Code/test change:

```text
examples/dbos_style_benchmark.py: producer_target_queue_latency_ms default 250.0 -> 100.0
examples/protocol_dbos_benchmark.py: --producer-target-queue-latency-ms default 250.0 -> 100.0
tests updated accordingly
```

Correctness/tests:

```bash
pytest tests/test_dbos_style_benchmark.py tests/test_protocol_dbos_benchmark.py tests/test_client.py tests/test_async_client.py tests/test_protocol.py -q
```

Result:

```text
246 passed
```

## 2026-06-13 IDT protocol KV latency presets

Change:
- Added explicit protocol KV benchmark presets:
  - `get-balanced`: one native socket, `MGET`-style compact bulk, pipeline 500.
  - `get-low-latency`: one native socket, compact bulk, pipeline 100.
  - `set-latency`: one native socket, compact durable bulk, pipeline 100.
- Existing `get-throughput`, `set-throughput`, and `get-latency` presets remain unchanged.
- Updated docs to show preset-based protocol benchmark commands.

Correctness/tests:

```bash
pytest tests/test_protocol_kv_benchmark.py tests/test_protocol.py tests/test_client.py tests/test_async_client.py -q
```

Result:

```text
255 passed
```

### Preset validation, clean native server, 10s override

Command shapes:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-balanced \
  --test-time 10 \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-low-latency \
  --test-time 10 \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-latency \
  --test-time 10 \
  --key-count 1000000 \
  --value-bytes 32 \
  --pretty
```

Results:

```text
get-balanced:
  requests_per_sec: 3,865,989.11/s
  pipeline:         500
  p50/p95/p99:      8.016ms / 11.012ms / 12.660ms
  max:              17.858ms
  errors:           0

get-low-latency:
  requests_per_sec: 2,271,655.28/s
  pipeline:         100
  p50/p95/p99:      2.714ms / 3.192ms / 3.472ms
  max:              12.966ms
  errors:           0

set-latency:
  requests_per_sec: 527,832.62/s
  pipeline:         100
  p50/p95/p99:      11.063ms / 15.499ms / 21.487ms
  max:              50.536ms
  errors:           0
```

Read:
- `get-balanced` is the recommended GET latency/throughput shape.
- `get-low-latency` is the low-tail GET shape and reaches p99 around `3.5ms` on one native socket.
- `set-latency` reduces durable SET p99 only modestly while giving up throughput; keep `set-throughput` as the default throughput shape.

## 2026-06-13 - native fixed-size MGET response tag (`0x89`)

Change:
- Server emits compact KV MGET fixed-size payload `0x89` when all returned values are present and equal-sized.
- Mixed/missing MGET values keep the existing `0x83` payload.
- Python protocol decoder already supports both tags; added tests for direct MGET and pipeline decode.

Correctness:

```bash
cd /Users/yoavgea/repos/ferricstore
mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs \
  apps/ferricstore_server/test/ferricstore_server/native/integration_test.exs
# 83 tests, 0 failures

cd /Users/yoavgea/repos/ferricstore-python
pytest tests/test_protocol.py tests/test_protocol_kv_benchmark.py -q
# 145 passed
```

Server:
- local source server, native protocol only
- `ferric://127.0.0.1:16388`
- 16 shards
- fresh `/tmp/ferricstore-protocol-bench/data` before KV group and again before Flow DBOS group

Native KV, one socket:

| preset | requests/s | p50 batch | p95 batch | p99 batch | notes |
|---|---:|---:|---:|---:|---|
| get-throughput | 8,416,372/s | 7.230 ms | 11.772 ms | 14.138 ms | pipeline 1000, lanes 64, fixed `0x89` response |
| get-low-latency | 3,465,632/s | 1.805 ms | 2.229 ms | 2.448 ms | pipeline 100, lanes 64, fixed `0x89` response |
| set-latency | 517,368/s | 11.927 ms | 15.663 ms | 20.642 ms | durable Ra/WAL/Bitcask path |

Native Flow DBOS-style, one socket, 100k samples:

| sample | e2e flows/s | create/s | process/s | p50 queue | p95 queue | p99 queue | empty claims |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 63,251/s | 65,758/s | 63,384/s | 18.802 ms | 69.070 ms | 193.246 ms | 0 |
| 2 | 67,880/s | 70,856/s | 68,017/s | 19.441 ms | 42.927 ms | 64.007 ms | 0 |
| 3 | 63,833/s | 67,379/s | 64,049/s | 21.297 ms | 44.631 ms | 73.396 ms | 1 |

Native Flow DBOS-style, one socket, 1M sustained:

| flows | e2e flows/s | create/s | process/s | p50 queue | p95 queue | p99 queue | empty claims | avg claim batch |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 45,895/s | 46,161/s | 45,921/s | 36.508 ms | 78.393 ms | 113.734 ms | 1 | 484.7 |

Read:
- KV GET got a large win from the fixed-size compact response.
- Flow DBOS did not improve and is below older best samples, while empty claims are near zero. Next investigation should focus durable create/complete write path, WARaft batching, or background projection/retention contention.

### Fresh rerun after DBOS low-sample investigation

Reason:
- The first 1M DBOS run was executed after several KV/Flow runs in the same server/data dir and showed `45,895/s`.
- A fresh source server/data dir was used to check whether this was a real protocol/code regression or accumulated background/data-dir pressure.

Fresh native Flow DBOS-style, one socket:

| flows | e2e flows/s | create/s | process/s | p50 queue | p95 queue | p99 queue | empty claims | avg claim batch | read |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 100,000 | 72,267/s | 77,118/s | 72,424/s | 17.502 ms | 37.140 ms | 85.175 ms | 1 | 383.1 | clean sample back in expected 70k range |
| 1,000,000 | 59,971/s | 60,195/s | 60,010/s | 27.721 ms | 68.297 ms | 98.093 ms | 1 | 487.1 | clean sustained sample back in expected ~60k range |

Conclusion:
- Native DBOS path is not regressed by the fixed MGET protocol change.
- Bad `45.9k/s` 1M sample was caused by non-fresh/background-contended state, not the DBOS command path.

## 2026-06-13 - Flow DBOS fused complete+claim experiment

Change tested:
- Exposed `--fuse-complete-claim/--no-fuse-complete-claim` for DBOS-style protocol queue runs.
- Fusion uses existing SDK support to submit completion and next claim together when explicitly enabled.
- Default remains off because the measured benefit was not clear.

Correctness:

```bash
pytest tests/test_dbos_style_benchmark.py \
  tests/test_protocol_dbos_benchmark.py \
  tests/test_flow_worker_scheduler.py \
  tests/test_client.py::test_complete_jobs_and_claim_jobs_batches_ack_then_next_claim \
  tests/test_client.py::test_submit_complete_jobs_and_claim_jobs_returns_independent_futures -q
# 74 passed
```

Fresh native Flow DBOS 100k comparison, one socket:

| mode | e2e flows/s | create/s | process/s | p50 queue | p95 queue | p99 queue | empty claims | read |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| fused complete+claim on | 70,970/s | 75,874/s | 71,118/s | 17.440 ms | 41.409 ms | 114.639 ms | 1 | not enough win; worse p99 |
| fused complete+claim off | 70,651/s | 76,948/s | 70,791/s | 18.062 ms | 43.324 ms | 88.931 ms | 3 | keep as default |

Decision:
- Keep the flag for explicit experiments.
- Do not enable by default; current bottleneck is still durable create/complete write path, not one transport round-trip in the worker loop.

## 2026-06-13 - Flow protocol latency trace before WARaft delay tuning

Server:
- local source server, native protocol only
- `FERRICSTORE_NATIVE_TRACE_ENABLED=true`
- 16 shards, fresh `/tmp/ferricstore-protocol-bench/data`

Command:

```bash
python examples/protocol_trace_latency.py \
  --url ferric://127.0.0.1:16388 \
  --op flow \
  --samples 200 \
  --warmup 20 \
  --timeout 5
```

Key p50 / p99 stage latencies:

| op | client response read p50/p99 | server command execute p50/p99 | server Ra wait p50/p99 | server apply p50/p99 | Bitcask append p50/p99 | read |
|---|---:|---:|---:|---:|---:|---|
| FLOW.CREATE | 1122 / 1471 us | 972 / 1309 us | 957 / 1295 us | 116 / 218 us | 2 / 5 us | WARaft wait dominates |
| FLOW.CLAIM_DUE | 998 / 1596 us | 868 / 1454 us | 848 / 1435 us | 59 / 118 us | 2 / 4 us | WARaft wait dominates |
| FLOW.COMPLETE | 1036 / 1514 us | 911 / 1394 us | 894 / 1379 us | 66 / 114 us | 4 / 16 us | WARaft wait dominates |

Read:
- Flow record/apply/index work is not the dominant single-command latency cost.
- Next experiment should tune WARaft commit/batching behavior, not Flow codec/index internals.

## 2026-06-13 - Flow protocol latency trace with `FERRICSTORE_WAL_COMMIT_DELAY_US=0`

Server:
- same as above, plus `FERRICSTORE_WAL_COMMIT_DELAY_US=0`

Key p50 / p99 stage latencies:

| op | client response read p50/p99 | server command execute p50/p99 | server Ra wait p50/p99 | server apply p50/p99 | Bitcask append p50/p99 | read |
|---|---:|---:|---:|---:|---:|---|
| FLOW.CREATE | 1106 / 1542 us | 958 / 1385 us | 943 / 1371 us | 109 / 197 us | 2 / 5 us | no meaningful latency win |
| FLOW.CLAIM_DUE | 987 / 1469 us | 862 / 1333 us | 842 / 1315 us | 57 / 108 us | 2 / 4 us | no meaningful latency win |
| FLOW.COMPLETE | 1032 / 1427 us | 911 / 1320 us | 894 / 1304 us | 63 / 112 us | 4 / 14 us | no meaningful latency win |

Decision:
- Do not change the WAL commit delay default based on this trace.
- The ~0.8-0.9ms Ra wait floor remains even with commit delay set to zero, so the next latency cut is probably inside WARaft acceptor/commit scheduling or the protocol benchmark’s command shape, not the configured delay floor.

## 2026-06-13 native DBOS producer inflight default tuning

Goal: keep native protocol queue benchmark on the user-facing wrapper path, one native connection, and improve DBOS-style Flow throughput/latency without changing server correctness. These runs use `ferric://127.0.0.1:16388`, source server, `FERRICSTORE_SHARD_COUNT=16`, fresh data dirs per sample, queue API, `claim_batch_size=500`, `claim_partition_batch_size=16`, `claim_drain_batches=2`, `create_batch_size=500`, `complete_async_depth=4`, `claim_job_only=true`, `fuse_complete_claim=false`.

### 100k tuning matrix

| Shape | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| default old: connections=1 lanes=32 create_inflight=1 target=100ms | 61,024 | 62,909 | 61,130 | 20.314 | 86.538 | 137.686 | 1 |
| lanes=64 create_inflight=1 target=100ms | 61,914 | n/a | n/a | 19.275 | 61.185 | 103.650 | 0 |
| connections=2 lanes=32 create_inflight=1 target=100ms | 61,907 | n/a | n/a | 18.842 | 62.632 | 124.901 | 0 |
| connections=1 lanes=32 create_inflight=2 target=100ms | 78,559 | 94,988 | 78,750 | 34.246 | 93.686 | 134.883 | 4 |
| connections=1 lanes=64 create_inflight=2 target=100ms | 70,812 | 80,011 | 70,961 | 27.415 | 86.038 | 112.529 | 6 |
| connections=2 lanes=32 create_inflight=2 target=100ms | 69,423 | 77,643 | 69,571 | 32.637 | 108.025 | 155.573 | 2 |
| connections=1 lanes=32 create_inflight=2 target=75ms | 77,749 | 97,977 | 77,927 | 29.812 | 65.564 | 84.647 | 10 |

Decision: set protocol DBOS benchmark defaults to `protocol_create_inflight_batches=2` and `producer_target_queue_latency_ms=75.0`. This keeps one native socket and the existing correctness path, but lets producers keep durable Ra/WAL batches fuller without waiting for each create batch to settle.

### Fresh default validation after changing wrapper defaults

Command intentionally omits `--protocol-create-inflight-batches` and `--producer-target-queue-latency-ms` so the wrapper defaults are tested.

| Flows | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Notes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100,000 | 92,977 | 97,215 | 93,255 | 27.605 | 83.524 | 135.357 | 0 / 259 | fresh source server, default wrapper printed `protocol_create_inflight_batches=2` |
| 1,000,000 | 62,623 | 62,998 | 62,646 | 45.623 | 134.806 | 198.279 | 3 / 2061 | sustained run hits durable create/complete/backpressure pressure, not empty-claim churn |

Focused tests after default change: `pytest tests/test_dbos_style_benchmark.py tests/test_protocol_dbos_benchmark.py -q` -> `32 passed`.

### 2026-06-13 native DBOS 1M sustained negative tuning samples

Same native one-socket/source-server shape as above. These were run to check whether sustained 1M could improve by changing completion concurrency or producer pressure limits.

| Shape | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| complete_async_depth=8, inflight=2, target=75ms | 62,295 | 62,691 | 62,319 | 46.701 | 137.396 | 240.834 | 0 / 2062 | no throughput gain, worse tail |
| inflight=3, target=75ms, complete_async_depth=4 | 61,247 | 61,724 | 61,270 | 56.462 | 225.353 | 367.334 | 2 / 2066 | more producer inflight overloads tail |
| inflight=2, target=100ms, complete_async_depth=4 | 39,966 | 40,114 | 39,976 | 80.665 | 249.395 | 371.613 | 0 / 2052 | looser target causes sustained backlog collapse |
| inflight=2, target=50ms, complete_async_depth=4 | 52,243 | 52,566 | 52,259 | 52.908 | 161.887 | 265.273 | 3 / 2065 | stricter target throttles too much and still hurts tail |
| inflight=2, target=75ms, complete_async_depth=2 | 30,837 | 35,058 | 30,844 | 139.365 | 8009.502 | 11579.694 | 128 / 2208 | completion bottleneck starves processing |

Decision: keep `protocol_create_inflight_batches=2`, `producer_target_queue_latency_ms=75.0`, and `complete_async_depth=4`. The next useful work is not more blind knob raising; it is producer limiter/worker scheduling or server durable write path optimization.

### 2026-06-13 rejected client auto-bucket precompute

Tried precomputing auto bucket partitions once per DBOS run and reusing them for producer splitting plus wake-credit publishing, to avoid duplicate Python hash work.

| Shape | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100k, precomputed auto partitions | 86,917 | 101,306 | 87,140 | 28.282 | 58.613 | 82.770 | 7 / 276 | lower tail but lower throughput than previous default validation |
| 1M, precomputed auto partitions | 58,847 | 59,172 | 58,875 | 46.345 | 144.946 | 238.905 | 0 / 2062 | worse sustained throughput and p99 |

Decision: rejected and reverted. The large Python partition list/cache effect costs more than the duplicate hash it removes on sustained runs. Focus should stay on worker scheduling or server durable write path.

Focused tests after revert: `pytest tests/test_dbos_style_benchmark.py tests/test_protocol_dbos_benchmark.py -q` -> `32 passed`.

## 2026-06-13 native KV current-state 30s samples

Source server from code, native protocol only, `ferric://127.0.0.1:16388`, `FERRICSTORE_SHARD_COUNT=16`, fresh data dir. These runs distinguish standard pipelined GET/SET command frames from explicit bulk `many` commands.

| Command/path | Request mode | Pipeline | Inflight batches | Lanes | Prebuilt keys | Requests/s | p50 batch ms | p95 batch ms | p99 batch ms | Max batch ms | Client CPU % | Notes |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| SET | many | 500 | 64 | 64 | true | 2,190,427 | 14.258 | 18.259 | 23.640 | 54.351 | 23.4 | optimized explicit bulk path, durable write path |
| GET | many | 100 | 64 | 64 | true | 3,319,708 | 1.920 | 2.321 | 2.526 | 9.287 | 132.5 | optimized explicit bulk path |
| SET | pipeline | 500 | 64 | 64 | false | 1,795,215 | 17.312 | 18.987 | 30.354 | 61.855 | 100.1 | standard command frames, Python key construction included |
| GET | pipeline | 100 | 64 | 64 | false | 1,385,436 | 4.598 | 4.964 | 5.160 | 10.220 | 118.1 | standard command frames, Python key construction included |
| SET | pipeline | 500 | 64 | 64 | true | 1,886,087 | 16.626 | 21.587 | 26.020 | 48.874 | 23.4 | standard command frames, cleaner protocol signal |
| GET | pipeline | 100 | 64 | 64 | true | 3,083,083 | 2.067 | 2.526 | 2.759 | 7.223 | 130.6 | standard command frames, cleaner protocol signal |

Read: native protocol KV is now strong on one socket. For honest user-facing numbers, report both modes: `pipeline` is normal command framing, `many` is explicit bulk command API. Prebuilt keys are required for clean protocol benchmarking because otherwise Python key construction dominates GET pipeline throughput.

## 2026-06-13 native DBOS phase split

Purpose: separate create capacity from claim/complete processing capacity. Same source server/native one-socket shape as current defaults, but `--queued-shape preloaded` creates all flows first, then starts processing.

| Flows | Shape | Create/s | Process/s | E2E flows/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Read |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1,000,000 | preloaded | 155,741 | 86,048 | 55,425 | 8848.639 | 11621.430 | 12191.351 | 156 / 2228 | create-only is much higher than live; processing/complete path plus live contention is the DBOS bottleneck |

Read: native Flow create capacity is not the sustained bottleneck in the current DBOS shape. The next target is queue worker processing and terminal completion path, not producer create throughput.

### 2026-06-13 native DBOS pending-credit cap samples

Purpose: check whether limiting created-but-not-yet-claimed wake credits reduces live 1M queue tail. Same native one-socket default shape, changing only `--producer-max-pending-credits`.

| Cap | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 64,000 | 62,894 | 63,134 | 62,923 | 45.804 | 148.420 | 234.555 | 2 / 2058 | neutral throughput, worse p99 |
| 32,000 | 39,928 | 40,073 | 39,943 | 72.065 | 264.079 | 451.802 | 0 / 2055 | over-throttles producers and worsens tail |

Decision: keep `producer_max_pending_credits=0` default. Credit caps are not the next DBOS latency win; preloaded phase split points to terminal completion / durable write path under live contention.

### 2026-06-13 native DBOS worker capacity samples

Hypothesis: `worker_capacity=500` might underuse `complete_async_depth=4`. Tested larger per-worker capacity through the wrapper passthrough `-- --worker-capacity N`.

| Flows | Worker capacity | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100,000 | 1,000 | 86,374 | 100,695 | 86,614 | 34.423 | 139.975 | 402.471 | 5 / 323 | worse tail, no throughput win |
| 100,000 | 2,500 | 77,602 | 98,726 | 77,810 | 31.341 | 70.732 | 145.913 | 14 / 286 | lower throughput |

Decision: keep `worker_capacity=500` for the optimized DBOS wrapper. More outstanding claimed work does not improve the one-socket queue path; it increases live scheduling/tail pressure.

## 2026-06-13 native Flow command breakdown

Purpose: isolate the raw native Flow command capacity outside the DBOS worker scheduler. Source server, native protocol, fresh data dirs per operation, `flows=100000`, `batch_size=500`, `inflight_batches=64`, one connection, `protocol_lanes=64`, `partitions=16`.

| Operation | Items/s | Seconds | p50 batch ms | p95 batch ms | p99 batch ms | Max batch ms | Setup seconds | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| create-many | 226,315 | 0.442 | 138.988 | 143.675 | 143.728 | 143.753 | 0.000 | raw create batch path is much faster than live DBOS create rate |
| claim-due | 312,703 | 0.320 | 97.790 | 139.511 | 140.450 | 140.719 | 1.803 | raw claim path is not the DBOS bottleneck |
| complete-many | 200,448 | 0.499 | 124.104 | 203.301 | 207.475 | 208.152 | 2.427 + claim setup 3.389 | raw terminal complete path is high capacity alone |

Read: standalone command capacity is high. The DBOS sustained gap is from live coordination/competition between create, claim, complete, worker scheduling, and durable apply batching, not a single slow native command codec.

### 2026-06-13 native DBOS queue API vs lowlevel worker

Same fresh source server/native one-socket 100k shape, changing only `--worker-api`.

| Worker API | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Claim calls | Empty claims | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| queue | 85,624 | 101,482 | 85,840 | 29.853 | 74.305 | 116.012 | 280 | 9 | preferred high-level SDK path |
| lowlevel | 56,899 | 61,894 | 57,031 | 66.186 | 217.155 | 344.462 | 518 | 277 | worse throughput and much worse empty-claim rate |

Decision: keep optimizing the QueueFlowWorker path. The lowlevel polling path is not a performance fallback for DBOS-style live queue workloads.

### 2026-06-13 rejected DBOS partition-key cache

Tried caching per-worker partition key strings for wake-drain claims to avoid rebuilding bucket key strings each claim loop.

| Flows | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100,000 | 89,588 | 102,350 | 89,823 | 30.140 | 72.567 | 85.757 | 0 / 272 | good small-run tail, not enough to accept alone |
| 1,000,000 | 62,549 | 63,008 | 62,572 | 43.745 | 143.204 | 234.072 | 1 / 2063 | neutral throughput, worse sustained tail than prior default sample |

Decision: rejected and reverted. This was a benchmark-loop allocation cleanup, but sustained 1M p99 regressed. Keep the simpler current loop until a stronger scheduling/server-side change is proven.

Focused tests after revert: `pytest tests/test_dbos_style_benchmark.py tests/test_protocol_dbos_benchmark.py tests/test_flow_worker_scheduler.py -q` -> `72 passed`.

### 2026-06-13 native DBOS shard-count comparison

Fresh source server/native one-socket 100k DBOS live run. Changed `FERRICSTORE_SHARD_COUNT` and wrapper `--server-shards` together.

| Server shards | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Read |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 8 | 84,257 | 88,101 | 84,582 | 30.084 | 69.164 | 84.197 | 1 / 263 | good tail, lower throughput |
| 16 | current default | current default | current default | current default | current default | current default | current default | best known throughput/tail balance on this machine |
| 32 | 50,962 | 53,610 | 51,055 | 33.395 | 236.787 | 731.013 | 1 / 278 | too much overhead/pressure |

Decision: keep `16` shard default for the local native DBOS benchmark shape. More shards are not automatically better; above local CPU/scheduler capacity they hurt both throughput and p99.

## 2026-06-13 native DBOS live contention trace

Measurement-only run. Server started with `FERRICSTORE_NATIVE_TRACE_ENABLED=true`, native DBOS 1M live benchmark running in background, then `protocol_trace_latency.py --op flow --samples 80` sampled traced create/claim/complete commands during load. Do not use this as headline throughput because trace adds work; use it for stage attribution.

DBOS background result during trace: `62,396/s` E2E, create `62,991/s`, process `62,419/s`, p50 queue `46.229ms`, p95 `141.987ms`, p99 `205.847ms`, empty claims `4 / 2065`.

### Key traced server stages under live DBOS load

| Command | Stage | p50 us | p95 us | p99 us | Read |
| --- | --- | ---: | ---: | ---: | --- |
| FLOW.CREATE | server_ra_wait_us | 33,456 | 137,323 | 171,306 | dominant |
| FLOW.CREATE | server_waraft_acceptor_commit_us | 33,417 | 137,273 | 171,284 | same as Ra wait |
| FLOW.CREATE | server_apply_us | 477 | 1,588 | 5,997 | small compared with Ra wait |
| FLOW.CREATE | server_bitcask_append_us | 4 | 9 | 46 | not bottleneck in traced path |
| FLOW.CLAIM_DUE | server_ra_wait_us | 29,609 | 88,674 | 154,462 | dominant |
| FLOW.CLAIM_DUE | server_waraft_acceptor_commit_us | 29,572 | 88,623 | 154,438 | same as Ra wait |
| FLOW.CLAIM_DUE | server_apply_us | 115 | 284 | 1,422 | small |
| FLOW.COMPLETE | server_ra_wait_us | 27,737 | 110,967 | 240,253 | dominant |
| FLOW.COMPLETE | server_waraft_acceptor_commit_us | 27,692 | 110,945 | 240,226 | same as Ra wait |
| FLOW.COMPLETE | server_apply_us | 129 | 361 | 778 | small |

Read: live DBOS latency is dominated by WARaft acceptor commit wait. Flow apply, Bitcask append, index mutation, native decode/encode, and lane queue wait are not the measured bottleneck. Next target should be WARaft commit batching/fairness/config, not more Flow codec/index micro-optimization.

## 2026-06-13 native DBOS WARaft config experiments

Purpose: live trace showed DBOS latency dominated by `server_waraft_acceptor_commit_us`, so tested existing low-risk WARaft env knobs before changing code. Fresh source server, native one-socket 1M DBOS live shape.

| WAL delay us | Commit batch max | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Read |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 0 | 10,000 | 62,316 | 62,670 | 62,339 | 45.565 | 140.461 | 220.204 | neutral throughput, worse p99 than best default sample |
| 3,000 | 10,000 | 40,664 | 40,840 | 40,675 | 71.788 | 237.347 | 410.383 | bad, pressure/backlog grows |
| 6,000 | 2,000 | 28,606 | 28,940 | 28,615 | 109.042 | 306.324 | 450.071 | bad, smaller commit batches collapse throughput |

Decision: keep current WARaft defaults: `FERRICSTORE_WAL_COMMIT_DELAY_US=6000`, `FERRICSTORE_WARAFT_COMMIT_BATCH_MAX=10000`, adaptive batching enabled. Simple config tuning does not beat default. Next useful work would require a real WARaft batching/fairness/code change, not config twiddling.

### 2026-06-13 rejected DBOS complete-independent-many

Hypothesis: queue completions are independent, so `--complete-independent-many` might reduce terminal batch coupling overhead. Tested through wrapper passthrough on fresh native 100k live run.

| complete_independent_many | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| true | 76,641 | 96,444 | 76,810 | 26.824 | 80.155 | 99.314 | 7 / 276 | worse processing throughput; keep default false |

Decision: rejected for benchmark default. It may remain useful semantically for some user workloads, but it is not the optimized DBOS path.

## 2026-06-13 rejected WARaft high-priority commit experiment

Live trace showed DBOS latency dominated by WARaft acceptor commit wait. FerricStore currently submits commits to `wa_raft_acceptor` with `:low` priority; `wa_raft_acceptor.commit_async/3` defaults to `:high`. Tested changing both sync and async commit calls from `:low` to `:high`.

Correctness smoke with high priority before benchmark: `mix test apps/ferricstore/test/ferricstore/raft/state_machine_latency_trace_test.exs apps/ferricstore/test/ferricstore/flow/invariant_test.exs` -> `5 tests, 0 failures`.

| Priority | Flows | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| high | 100,000 | 93,085 | 106,174 | 93,341 | 28.612 | 75.865 | 99.160 | 1 / 271 | good short-run result |
| high | 1,000,000 | 59,361 | 59,599 | 59,382 | 48.050 | 171.761 | 265.344 | 0 / 2054 | worse sustained throughput and p99 |

Decision: rejected and reverted to `:low`. High priority lowers short-run latency but hurts sustained 1M. The current workload benefits from low-priority batching under sustained durable write pressure.

Correctness smoke after revert: same command -> passed.

### 2026-06-13 native DBOS lane-count comparison

Fresh native one-socket 1M DBOS live run. Changed only `--protocol-lanes`; server shards stayed 16.

| Protocol lanes | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 16 | 62,501 | 63,056 | 62,524 | 53.039 | 239.479 | 455.916 | 1 / 2065 | throughput neutral, tail much worse |
| 32 | current default | current default | current default | current default | current default | current default | current default | best known one-socket balance |
| 64 | 44,119 | 44,389 | 44,133 | 69.383 | 208.627 | 349.237 | 1 / 2063 | too much lane pressure, throughput collapse |

Decision: keep `protocol_lanes=32` for DBOS one-socket default.

## 2026-06-13 native DBOS WARaft generic batch window experiments

Purpose: check whether a tiny pre-commit namespace/generic batch window can reduce WARaft commit pressure. Fresh source server, native one-socket 1M DBOS live shape, changed only `FERRICSTORE_WARAFT_GENERIC_BATCH_WINDOW_MS`.

| Generic batch window ms | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 62,544 | 62,947 | 62,567 | 43.145 | 142.978 | 208.042 | 2 / 2064 | neutral, not enough to change default |
| 2 | 42,883 | 43,154 | 42,896 | 64.866 | 214.950 | 327.704 | 4 / 2067 | bad, adds queue pressure |

Decision: keep `FERRICSTORE_WARAFT_GENERIC_BATCH_WINDOW_MS=0`. Tiny window `1ms` is not a clear win; `2ms` clearly hurts.

### 2026-06-13 rejected DBOS producer min-rate 20k

Purpose: check whether default `producer_min_rate_per_sec=50000` keeps too much pressure during sustained 1M. Fresh native one-socket 1M DBOS live run with `--producer-min-rate-per-sec 20000`.

| Producer min rate/s | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Producer wait ms | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 20,000 | 59,077 | 59,260 | 59,098 | 37.427 | 118.669 | 200.118 | 23,879 | lower p50/p95, but lower throughput and no meaningful p99 win |

Decision: keep `producer_min_rate_per_sec=50000` for benchmark default. Lower min rate is an opt-in smoother mode, not the lowest-latency/high-throughput default.

## 2026-06-13 rejected selective high-priority FLOW.CLAIM_DUE commit

Hypothesis: keep create/complete low-priority batched, but make only `{:flow_claim_due, key, attrs}` high priority to reduce worker unlock latency. Implemented as an experiment, with all other commands remaining low priority.

Correctness smoke before benchmark: `mix test apps/ferricstore/test/ferricstore/raft/state_machine_latency_trace_test.exs apps/ferricstore/test/ferricstore/flow/invariant_test.exs apps/ferricstore/test/ferricstore/flow/fencing_retry_test.exs` -> `11 tests, 0 failures`.

| Claim priority | Flows | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| high only for claim_due | 100,000 | 81,692 | 98,215 | 81,900 | 27.536 | 68.519 | 91.416 | 4 / 275 | p99 good, throughput worse |
| high only for claim_due | 1,000,000 | 58,658 | 58,938 | 58,678 | 44.353 | 141.045 | 245.339 | 2 / 2054 | sustained throughput and p99 worse |

Decision: rejected and reverted. Claim_due high priority disrupts the sustained low-priority batching balance more than it helps worker wake latency.

Correctness smoke after revert: same command -> passed.

### 2026-06-13 native DBOS connection-count comparison

Fresh native 1M DBOS live run, changed only `--protocol-worker-connections` from default `1` to `2`; lanes stayed 32.

| Connections | E2E flows/s | Create/s | Process/s | p50 queue ms | p95 queue ms | p99 queue ms | Empty claims | Decision |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2 | 61,886 | 62,335 | 61,909 | 44.934 | 141.731 | 227.123 | 2 / 2064 | no win over one-socket default |

Decision: keep one native connection for default DBOS benchmark. More sockets are an opt-in escape hatch, not the current best local shape.

### 2026-06-13T23:46:26 - native DBOS 100k, Ra low-priority flush default 512

- Result: e2e `82588/s`, create `97587/s`, process `82800/s`, p50 queue `30.250ms`, p99 queue `101.656ms`, max queue `467.912ms`.
- Config: source server, `ferric://127.0.0.1:16388`, 16 shards, 1 native worker connection, lanes 32, batch 500, complete_async_depth 4, `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE` unset => new default 512.
- Decision: not accepted yet; 100k throughput is below best prior default, tail is decent. Needs 1M sustained result.

### 2026-06-13T23:47:11 - native DBOS 1M, Ra low-priority flush default 512

- Result: e2e `63926/s`, create `64166/s`, process `63952/s`, p50 queue `45.902ms`, p99 queue `224.584ms`, max queue `411.756ms`.
- Config: source server, `ferric://127.0.0.1:16388`, 16 shards, 1 native worker connection, lanes 32, batch 500, complete_async_depth 4, `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE` unset => new default 512.
- Decision: compare to prior 1M default around 62k/s and p99 around 198-205ms; keep only if sustained throughput/tail improves or remains within gate.

### 2026-06-13T23:48:14 - native DBOS 1M, Ra low-priority flush 256

- Result: e2e `63323/s`, create `63801/s`, process `63347/s`, p50 queue `43.500ms`, p99 queue `210.085ms`, max queue `1093.050ms`.
- Config: source server, `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE=256`, other native DBOS defaults unchanged.
- Decision: compare against 512 and prior implicit 128; keep only if latency improves without meaningful throughput loss.

### 2026-06-13T23:49:07 - native DBOS 1M, Ra low-priority flush 128

- Result: e2e `61023/s`, create `61532/s`, process `61045/s`, p50 queue `45.788ms`, p99 queue `229.968ms`, max queue `1301.234ms`.
- Config: source server, `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE=128`, other native DBOS defaults unchanged.
- Decision: explicit old Ra default comparison for the new exposed knob.

### 2026-06-13T23:49:54 - native DBOS 1M, Ra low-priority flush 384

- Result: e2e `59279/s`, create `59699/s`, process `59301/s`, p50 queue `46.452ms`, p99 queue `213.941ms`, max queue `557.957ms`.
- Config: source server, `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE=384`, other native DBOS defaults unchanged.
- Decision: middle-value test between 256 tail and 512 throughput.

### 2026-06-13T23:50:56 - native DBOS 1M, Ra low-priority flush 256 repeat

- Result: e2e `38789/s`, create `39024/s`, process `38798/s`, p50 queue `80.307ms`, p99 queue `426.378ms`, max queue `1011.158ms`.
- Config: source server, `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE=256`, repeat sample, other native DBOS defaults unchanged.
- Decision: if consistent, prefer 256 default for latency over 512 throughput-biased default.

### 2026-06-13T23:53:19 - native DBOS 100k, Ra low-priority flush default 256

- Result: e2e `93817/s`, create `105140/s`, process `94087/s`, p50 queue `30.326ms`, p99 queue `80.499ms`, max queue `88.246ms`.
- Config: source server, default `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE` unset => 256, other native DBOS defaults unchanged.
- Decision: keep config knob; default is latency-biased at 256, while 512 remains available for throughput-biased tuning.

### 2026-06-13T23:54:12 - native DBOS 1M, Ra low-priority flush default 256 final

- Result: e2e `56125/s`, create `56296/s`, process `56144/s`, p50 queue `49.757ms`, p99 queue `249.071ms`, max queue `495.910ms`.
- Config: source server, default `FERRICSTORE_RA_LOW_PRIORITY_COMMANDS_FLUSH_SIZE` unset => 256, other native DBOS defaults unchanged.
- Decision: final sustained gate for the latency-biased Ra low-priority flush-size default.

### Native DBOS after compact Flow wire-normalized item decode (100k)
- Time: 2026-06-14T00:02:48
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Change: compact Flow create/terminal many decoders emit normalized items and Commands skips duplicate item normalization for trusted wire payloads.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-5e939a5146164f45a24b923830da7455', 'flows': 100000, 'created': 100000, 'completed': 100000, 'claimed_items': 100000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 320000.0, 'producer_backpressure_waits': 0, 'producer_backpressure_wait_ms': 0.0, 'producer_backpressure_limited_batches': 1, 'producer_queue_latency_ewma_ms': 86.72456540285752, 'queue_latency_tracked': 1000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 1000, 'queue_latency_avg_ms': 34.758191387, 'queue_latency_p50_ms': 27.016833, 'queue_latency_p95_ms': 74.632667, 'queue_latency_p99_ms': 87.103209, 'queue_latency_max_ms': 496.927208, 'wake_notifications': 256, 'wake_credits': 100000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 276, 'process_empty_claims': 7, 'process_fallback_claims': 22, 'process_avg_claim_batch': 362.3188405797101, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 1.0111310420325026, 'process_seconds': 1.2372512919828296, 'total_seconds': 1.2402108330279589, 'client_cpu_seconds': 0.690141, 'client_cpu_percent': 55.64707077384815, 'create_flows_per_sec': 98899.1494109282, 'process_flows_per_sec': 80824.3245717199, 'end_to_end_flows_per_sec': 80631.45179586223}`
- E2E: None/s; create: None/s; process: None/s; p50: None ms; p99: None ms; max: None ms.

### Native DBOS after compact Flow wire-normalized item decode (1M)
- Time: 2026-06-14T00:03:37
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Change: compact Flow create/terminal many decoders emit normalized items and Commands skips duplicate item normalization for trusted wire payloads.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-b2b6ddd464c347859d9cc6f66d648c67', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 303256.40565258363, 'producer_backpressure_waits': 818, 'producer_backpressure_wait_ms': 4226.791778043351, 'producer_backpressure_limited_batches': 617, 'producer_queue_latency_ewma_ms': 310.55288523488355, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 63.2808959408, 'queue_latency_p50_ms': 49.0125, 'queue_latency_p95_ms': 148.83025, 'queue_latency_p99_ms': 250.974792, 'queue_latency_max_ms': 1896.280917, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2063, 'process_empty_claims': 1, 'process_fallback_claims': 16, 'process_avg_claim_batch': 484.7309743092584, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 17.02962866704911, 'process_seconds': 17.14192429196555, 'total_seconds': 17.148278624983504, 'client_cpu_seconds': 7.628718999999999, 'client_cpu_percent': 44.486791746465094, 'create_flows_per_sec': 58721.18644224553, 'process_flows_per_sec': 58336.507790359436, 'end_to_end_flows_per_sec': 58314.89106685552}`
- E2E: 58315/s; create: 58721/s; process: 58337/s; queue p50: 49.013 ms; queue p99: 250.975 ms; queue max: 1896.281 ms; claim calls: 2063; empty claims: 1.

### Native DBOS after compact Flow wire-normalized item decode (1M repeat 2)
- Time: 2026-06-14T00:04:26
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-46b70be79dd543aaa68cff02bacf42b8', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 320000.0, 'producer_backpressure_waits': 1170, 'producer_backpressure_wait_ms': 6045.018547244095, 'producer_backpressure_limited_batches': 546, 'producer_queue_latency_ewma_ms': 308.0457002901417, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 61.5316866915, 'queue_latency_p50_ms': 46.714125, 'queue_latency_p95_ms': 145.664041, 'queue_latency_p99_ms': 248.0215, 'queue_latency_max_ms': 1945.338958, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2061, 'process_empty_claims': 2, 'process_fallback_claims': 13, 'process_avg_claim_batch': 485.201358563804, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 16.803625041036867, 'process_seconds': 16.89927308296319, 'total_seconds': 16.90523245802615, 'client_cpu_seconds': 7.457871, 'client_cpu_percent': 44.115755394177995, 'create_flows_per_sec': 59510.96847006859, 'process_flows_per_sec': 59174.142881218875, 'end_to_end_flows_per_sec': 59153.28301358122}`
- E2E: 59153/s; create: 59511/s; process: 59174/s; queue p50: 46.714 ms; queue p99: 248.022 ms; queue max: 1945.339 ms; producer waits: 1170; claim calls: 2061; empty claims: 2.

### Native DBOS after compact Flow wire-normalized item decode (1M repeat 3)
- Time: 2026-06-14T00:05:12
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-431b60cb63584b159ece2a618a8c675b', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 50000.0, 'producer_backpressure_waits': 163, 'producer_backpressure_wait_ms': 921.6991052134994, 'producer_backpressure_limited_batches': 1845, 'producer_queue_latency_ewma_ms': 742.78417199105, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 145.7382988506, 'queue_latency_p50_ms': 117.098584, 'queue_latency_p95_ms': 356.588958, 'queue_latency_p99_ms': 536.021708, 'queue_latency_max_ms': 4501.684167, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2074, 'process_empty_claims': 8, 'process_fallback_claims': 26, 'process_avg_claim_batch': 482.1600771456123, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 37.96974579105154, 'process_seconds': 38.424615291994996, 'total_seconds': 38.432394042029046, 'client_cpu_seconds': 11.871553, 'client_cpu_percent': 30.889444428097455, 'create_flows_per_sec': 26336.757836173703, 'process_flows_per_sec': 26024.98404735701, 'end_to_end_flows_per_sec': 26019.716567914456}`
- E2E: 26020/s; create: 26337/s; process: 26025/s; queue p50: 117.099 ms; queue p99: 536.022 ms; queue max: 4501.684 ms; producer waits: 163; claim calls: 2074; empty claims: 8.

### Native DBOS after tuple terminal-many compact decode (100k)
- Time: 2026-06-14T00:06:56
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Change: compact create-many emits create tuples; compact terminal-many emits core tuple items; Commands skips duplicate item normalization only for trusted compact wire payloads.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-5f19b1b86c01413baa27301cebe8b459', 'flows': 100000, 'created': 100000, 'completed': 100000, 'claimed_items': 100000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 320000.0, 'producer_backpressure_waits': 0, 'producer_backpressure_wait_ms': 0.0, 'producer_backpressure_limited_batches': 3, 'producer_queue_latency_ewma_ms': 98.71733699450311, 'queue_latency_tracked': 1000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 1000, 'queue_latency_avg_ms': 37.752510563, 'queue_latency_p50_ms': 29.817917, 'queue_latency_p95_ms': 71.167333, 'queue_latency_p99_ms': 95.034, 'queue_latency_max_ms': 528.093417, 'wake_notifications': 256, 'wake_credits': 100000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 278, 'process_empty_claims': 5, 'process_fallback_claims': 22, 'process_avg_claim_batch': 359.71223021582733, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 1.0301324169849977, 'process_seconds': 1.2072433329885826, 'total_seconds': 1.210291916038841, 'client_cpu_seconds': 0.722084, 'client_cpu_percent': 59.66197001160724, 'create_flows_per_sec': 97074.89867436756, 'process_flows_per_sec': 82833.34210050738, 'end_to_end_flows_per_sec': 82624.69464993995}`
- E2E: 82625/s; create: 97075/s; process: 82833/s; queue p50: 29.818 ms; queue p99: 95.034 ms; queue max: 528.093 ms; claim calls: 278; empty claims: 5.

### Native DBOS after tuple terminal-many compact decode (1M)
- Time: 2026-06-14T00:07:41
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-00ff107af5cb46eb977118d9a2c8a4f2', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 0.0, 'producer_backpressure_waits': 1393, 'producer_backpressure_wait_ms': 7583.862117483789, 'producer_backpressure_limited_batches': 641, 'producer_queue_latency_ewma_ms': 17.59723610476159, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 64.5359251136, 'queue_latency_p50_ms': 48.634791, 'queue_latency_p95_ms': 165.655166, 'queue_latency_p99_ms': 268.901333, 'queue_latency_max_ms': 454.82325, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2055, 'process_empty_claims': 0, 'process_fallback_claims': 5, 'process_avg_claim_batch': 486.61800486618006, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 17.165724375052378, 'process_seconds': 17.212902041967027, 'total_seconds': 17.218977084034123, 'client_cpu_seconds': 7.944424000000001, 'client_cpu_percent': 46.1376071367577, 'create_flows_per_sec': 58255.62488078507, 'process_flows_per_sec': 58095.95601961165, 'end_to_end_flows_per_sec': 58075.45913556187}`
- E2E: 58075/s; create: 58256/s; process: 58096/s; queue p50: 48.635 ms; queue p99: 268.901 ms; queue max: 454.823 ms; producer waits: 1393; claim calls: 2055; empty claims: 0.

### Native DBOS after compact Flow wire-normalized items + wire opts (100k)
- Time: 2026-06-14T00:12:09
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Change: compact Flow decoders emit normalized tuple items and prebuilt keyword opts; Commands skips duplicate item/opts normalization only for trusted compact wire payloads.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-6c9a40e5707c49d5a45835a301f026f3', 'flows': 100000, 'created': 100000, 'completed': 100000, 'claimed_items': 100000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 0.0, 'producer_backpressure_waits': 0, 'producer_backpressure_wait_ms': 0.0, 'producer_backpressure_limited_batches': 3, 'producer_queue_latency_ewma_ms': 30.086626350944655, 'queue_latency_tracked': 1000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 1000, 'queue_latency_avg_ms': 32.775822512, 'queue_latency_p50_ms': 27.916875, 'queue_latency_p95_ms': 71.892625, 'queue_latency_p99_ms': 104.292125, 'queue_latency_max_ms': 127.358333, 'wake_notifications': 256, 'wake_credits': 100000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 267, 'process_empty_claims': 0, 'process_fallback_claims': 11, 'process_avg_claim_batch': 374.53183520599254, 'process_max_claim_batch': 796, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 0.9967185000423342, 'process_seconds': 1.0741516659036279, 'total_seconds': 1.0773787909420207, 'client_cpu_seconds': 0.679617, 'client_cpu_percent': 63.080599480315335, 'create_flows_per_sec': 100329.23036519604, 'process_flows_per_sec': 93096.72290632738, 'end_to_end_flows_per_sec': 92817.86576897773}`
- E2E: 92818/s; create: 100329/s; process: 93097/s; queue p50: 27.917 ms; queue p99: 104.292 ms; queue max: 127.358 ms; claim calls: 267; empty claims: 0.

### Native DBOS after compact Flow wire-normalized items + wire opts (1M)
- Time: 2026-06-14T00:12:55
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default Ra low-priority flush 512.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-3f5ff7b03ff14ef286de2d8d40ad228f', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 0.0, 'producer_backpressure_waits': 1135, 'producer_backpressure_wait_ms': 5917.541107470982, 'producer_backpressure_limited_batches': 486, 'producer_queue_latency_ewma_ms': 23.984457133106062, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 58.5232604554, 'queue_latency_p50_ms': 46.972042, 'queue_latency_p95_ms': 136.973334, 'queue_latency_p99_ms': 200.453792, 'queue_latency_max_ms': 383.496292, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2056, 'process_empty_claims': 1, 'process_fallback_claims': 8, 'process_avg_claim_batch': 486.38132295719845, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 16.81294574995991, 'process_seconds': 16.87978108297102, 'total_seconds': 16.88591745798476, 'client_cpu_seconds': 7.479171999999999, 'client_cpu_percent': 44.29236385058462, 'create_flows_per_sec': 59477.97696322101, 'process_flows_per_sec': 59242.474477873344, 'end_to_end_flows_per_sec': 59220.94564824104}`
- E2E: 59221/s; create: 59478/s; process: 59242/s; queue p50: 46.972 ms; queue p99: 200.454 ms; queue max: 383.496 ms; producer waits: 1135; claim calls: 2056; empty claims: 1.

### Native KV GET after lane compact MGET coalescing
- Time: 2026-06-14T00:19:25
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Change: Native lane coalesces queued compact MGET frames into one Router.batch_get and splits compact responses per request.
- Command: `python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset get-throughput --key-count 100000 --value-bytes 16 --pretty`
- Ops/sec: None; p50 batch ms: None; p99 batch ms: None; max batch ms: None; errors: 0.
- Raw: `{"batch_latency_avg_ms": 7.1116439539481995, "batch_latency_max_ms": 23.001292, "batch_latency_p50_ms": 6.61475, "batch_latency_p95_ms": 11.966917, "batch_latency_p99_ms": 14.387584, "batch_latency_samples": 269262, "benchmark": "protocol_kv", "binary_keys": false, "client_cpu_percent": 90.78997650961188, "client_cpu_seconds": 27.242535, "clients_per_thread": 1, "command": "get", "configured_requests": null, "errors": 0, "inflight_batches": 64, "key_count": 100000, "large_response_warning": null, "pipeline": 1000, "prebuild_keys": true, "preset": "get-throughput", "processes": 1, "protocol_lanes": 64, "range_start": null, "range_stop": null, "read_percent": null, "request_mode": "many", "requests": 269262000, "requests_per_sec": 8973574.102017714, "response_items_per_batch_estimate": null, "response_items_per_request_estimate": null, "seconds": 30.00610424997285, "test_time": 30.0, "threads": 1, "total_connections": 1, "url": "ferric://127.0.0.1:16388", "value_bytes": 16, "warmed_keys": 100000, "zset_members_per_key": null}`

### Native KV GET after lane compact MGET coalescing - corrected summary
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Command: `python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset get-throughput --key-count 100000 --value-bytes 16 --pretty`
- Result: 8,973,574 GET/s; batch p50 6.615 ms; batch p99 14.388 ms; batch max 23.001 ms; errors 0; one native connection; protocol_lanes 64; pipeline 1000.
- Note: previous entry for this same run used old field names and may show null summary values; this corrected summary uses `requests_per_sec` and `batch_latency_*` fields.

### Native KV SET after lane compact MGET coalescing
- Time: 2026-06-14T00:20:31
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Change: Same tree as MGET coalescing; SET path unchanged except shared compile/runtime.
- Command: `python examples/protocol_kv_benchmark.py --url ferric://127.0.0.1:16388 --preset set-throughput --key-count 100000 --value-bytes 16 --pretty`
- Result: 2046242 SET/s; batch p50 15.176 ms; batch p99 25.798 ms; batch max 53.224 ms; errors 0; one native connection; protocol_lanes 64; pipeline 500.
- Raw: `{"batch_latency_avg_ms": 15.592963856336858, "batch_latency_max_ms": 53.224458, "batch_latency_p50_ms": 15.17575, "batch_latency_p95_ms": 20.214708, "batch_latency_p99_ms": 25.797708, "batch_latency_samples": 122829, "benchmark": "protocol_kv", "binary_keys": false, "client_cpu_percent": 22.659906900511963, "client_cpu_seconds": 6.800987999999999, "clients_per_thread": 1, "command": "set", "configured_requests": null, "errors": 0, "inflight_batches": 64, "key_count": 100000, "large_response_warning": null, "pipeline": 500, "prebuild_keys": true, "preset": "set-throughput", "processes": 1, "protocol_lanes": 64, "range_start": null, "range_stop": null, "read_percent": null, "request_mode": "many", "requests": 61414500, "requests_per_sec": 2046242.1817851937, "response_items_per_batch_estimate": null, "response_items_per_request_estimate": null, "seconds": 30.0133095420897, "test_time": 30.0, "threads": 1, "total_connections": 1, "url": "ferric://127.0.0.1:16388", "value_bytes": 16, "warmed_keys": 0, "zset_members_per_key": null}`

### Native DBOS sanity after lane compact MGET coalescing (100k)
- Time: 2026-06-14T00:21:04
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Purpose: verify Flow DBOS path after adding read-only compact MGET lane coalescing.
- Result: 82299/s E2E; create 98802/s; process 82516/s; queue p50 31.893 ms; queue p99 87.679 ms; max 101.983 ms; empty claims 5.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-88290ddb903d40ada36de5f0e6adee2c', 'flows': 100000, 'created': 100000, 'completed': 100000, 'claimed_items': 100000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 0.0, 'producer_backpressure_waits': 0, 'producer_backpressure_wait_ms': 0.0, 'producer_backpressure_limited_batches': 1, 'producer_queue_latency_ewma_ms': 25.87000276708708, 'queue_latency_tracked': 1000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 1000, 'queue_latency_avg_ms': 35.182729181, 'queue_latency_p50_ms': 31.893375, 'queue_latency_p95_ms': 75.366875, 'queue_latency_p99_ms': 87.679416, 'queue_latency_max_ms': 101.982541, 'wake_notifications': 256, 'wake_credits': 100000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 277, 'process_empty_claims': 5, 'process_fallback_claims': 22, 'process_avg_claim_batch': 361.01083032490976, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 1.0121296249562874, 'process_seconds': 1.2118880829075351, 'total_seconds': 1.2150744160171598, 'client_cpu_seconds': 0.692861, 'client_cpu_percent': 57.02210423219174, 'create_flows_per_sec': 98801.57396274105, 'process_flows_per_sec': 82515.87040948716, 'end_to_end_flows_per_sec': 82299.48609056036}`

### Native DBOS current sustained baseline after codec/MGET lane changes (1M)
- Time: 2026-06-14T00:22:36
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Command: `python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 1000000 --workers 16 --producers 4 --partitions 16 --server-shards 16 --claim-batch-size 500 --claim-partition-batch-size 16 --claim-drain-batches 2 --create-batch-size 500 --complete-async-depth 4 --transport many --queued-shape live --worker-api queue --worker-mode polling --partition-mode auto --claim-job-only`
- Result: 61382/s E2E; create 61682/s; process 61404/s; queue p50 45.290 ms; queue p99 238.036 ms; max 393.406 ms; empty claims 2; producer waits 1252.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-1a1a885cf7384b57a9b11e83d5e421dc', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 54000.0, 'producer_backpressure_waits': 1252, 'producer_backpressure_wait_ms': 6513.678238911334, 'producer_backpressure_limited_batches': 519, 'producer_queue_latency_ewma_ms': 44.98343822154675, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 59.2167107538, 'queue_latency_p50_ms': 45.290292, 'queue_latency_p95_ms': 144.360667, 'queue_latency_p99_ms': 238.035792, 'queue_latency_max_ms': 393.406125, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2057, 'process_empty_claims': 2, 'process_fallback_claims': 9, 'process_avg_claim_batch': 486.1448711716091, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 16.212066459003836, 'process_seconds': 16.285570084000938, 'total_seconds': 16.29147254198324, 'client_cpu_seconds': 7.320224, 'client_cpu_percent': 44.93285662874077, 'create_flows_per_sec': 61682.45131049419, 'process_flows_per_sec': 61404.052473570286, 'end_to_end_flows_per_sec': 61381.805568710435}`

### Native DBOS 1M with less conservative producer backpressure
- Time: 2026-06-14T00:23:57
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Backpressure: target queue latency 150ms, min producer rate 80k/s.
- Result: 60314/s E2E; create 60724/s; process 60337/s; queue p50 50.288 ms; queue p99 289.125 ms; max 1660.075 ms; empty claims 2; producer waits 0; final producer rate 409600.0.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-914f6da06b8249e8afd2c6325fec6ec9', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 409600.0, 'producer_backpressure_waits': 0, 'producer_backpressure_wait_ms': 0.0, 'producer_backpressure_limited_batches': 83, 'producer_queue_latency_ewma_ms': 323.3941714654838, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 67.7758650932, 'queue_latency_p50_ms': 50.287791, 'queue_latency_p95_ms': 170.2975, 'queue_latency_p99_ms': 289.125, 'queue_latency_max_ms': 1660.07525, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2066, 'process_empty_claims': 2, 'process_fallback_claims': 17, 'process_avg_claim_batch': 484.027105517909, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 16.467989124939777, 'process_seconds': 16.573697875021026, 'total_seconds': 16.57984908297658, 'client_cpu_seconds': 7.333484, 'client_cpu_percent': 44.23130731346453, 'create_flows_per_sec': 60723.86813066085, 'process_flows_per_sec': 60336.56505269989, 'end_to_end_flows_per_sec': 60314.17988157405}`

### Native DBOS 1M with create/claim batch 1000
- Time: 2026-06-14T00:25:24
- Server: source, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Batch config: create 1000, claim 1000, claim partition batch 16, drain batches 2.
- Result: 61475/s E2E; create 61762/s; process 61498/s; queue p50 63.816 ms; queue p99 265.735 ms; max 416.984 ms; avg claim 963.3911368015414; max claim 2000; empty claims 1; producer waits 2976.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-8561f68bf0b94d5093def8e132b15374', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 1000, 'worker_capacity': 1000, 'create_batch_size': 1000, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 107946.24986363939, 'producer_backpressure_waits': 2976, 'producer_backpressure_wait_ms': 28145.34624246681, 'producer_backpressure_limited_batches': 535, 'producer_queue_latency_ewma_ms': 36.41610767821017, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 78.2190434242, 'queue_latency_p50_ms': 63.815958, 'queue_latency_p95_ms': 181.9005, 'queue_latency_p99_ms': 265.735292, 'queue_latency_max_ms': 416.984334, 'wake_notifications': 1024, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 1038, 'process_empty_claims': 1, 'process_fallback_claims': 13, 'process_avg_claim_batch': 963.3911368015414, 'process_max_claim_batch': 2000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 16.19113191694487, 'process_seconds': 16.2606038749218, 'total_seconds': 16.266716458951123, 'client_cpu_seconds': 6.745141, 'client_cpu_percent': 41.46590381052801, 'create_flows_per_sec': 61762.204466597395, 'process_flows_per_sec': 61498.3310393698, 'end_to_end_flows_per_sec': 61475.22166034484}`

### Native DBOS after lean compact Flow-many decode map cleanup (1M clean)
- Time: 2026-06-14T00:36:24
- Server: source, live tool session, clean data dir `/tmp/ferricstore-protocol-lean-decode-1m-clean/data`, native ferric://127.0.0.1:16388, 16 shards, default settings.
- Change: compact Flow many decode now keeps only dispatch-required payload keys plus trusted `__wire_flow_opts__`; mixed create items use tuple shape instead of per-item maps.
- Command: `python examples/protocol_dbos_benchmark.py --url ferric://127.0.0.1:16388 --flows 1000000 --workers 16 --producers 4 --partitions 16 --server-shards 16 --claim-batch-size 500 --claim-partition-batch-size 16 --claim-drain-batches 2 --create-batch-size 500 --complete-async-depth 4 --transport many --queued-shape live --worker-api queue --worker-mode polling --partition-mode auto --claim-job-only`
- Result: 60998/s E2E; create 61449/s; process 61022/s; queue p50 47.870 ms; queue p99 266.556 ms; max 519.676 ms; empty claims 3; producer waits 1149.
- Raw: `{'mode': 'queued', 'queued_shape': 'live', 'flow_type': 'dbos_python_sdk_bench:py-sdk-bench-d392e0296dc04b22ba7dae48ec40f9bd', 'flows': 1000000, 'created': 1000000, 'completed': 1000000, 'claimed_items': 1000000, 'duplicate_completions': 0, 'workers': 16, 'worker_lanes': 16, 'producers': 4, 'partitions': 16, 'claim_any': False, 'partition_mode': 'auto', 'worker_mode': 'queue-api', 'worker_api': 'queue', 'claim_batch_size': 500, 'worker_capacity': 500, 'create_batch_size': 500, 'complete_batch': True, 'complete_async_depth': 4, 'fuse_complete_claim': False, 'independent_many': True, 'complete_independent_many': False, 'transport': 'many', 'payload_bytes': 0, 'result_bytes': 0, 'retention_ttl_ms': 0, 'work_command': 'none', 'idle_sleep_ms': 10.0, 'max_idle_sleep_ms': 50.0, 'wake_coalesce_ms': 5.0, 'partial_claim_retries': 1, 'partial_claim_delay_ms': 1.0, 'reclaim_expired': False, 'reclaim_ratio': 25, 'claim_priority': 0, 'claim_state': 'queued', 'claim_states': '', 'claim_job_only': True, 'claim_block_ms': -1, 'claim_drain_block_ms': -1, 'track_duplicates': False, 'claim_partition_batch_size': 16, 'claim_drain_batches': 2, 'claim_prefetch': 0, 'effective_claim_prefetch': 0, 'server_shards': 16, 'protocol_worker_connections': 1, 'protocol_lanes': 32, 'protocol_create_inflight_batches': 2, 'producer_max_pending_credits': 0, 'protocol_wake_hints': False, 'latency_sample_rate': 100, 'adaptive_producer_backpressure': True, 'producer_backpressure_rate_per_sec': 58773.123072000024, 'producer_backpressure_waits': 1149, 'producer_backpressure_wait_ms': 5692.870073737961, 'producer_backpressure_limited_batches': 561, 'producer_queue_latency_ewma_ms': 98.63391849272934, 'queue_latency_tracked': 10000, 'queue_latency_pending': 0, 'queue_latency_sample_count': 10000, 'queue_latency_avg_ms': 61.7008041861, 'queue_latency_p50_ms': 47.869584, 'queue_latency_p95_ms': 157.203791, 'queue_latency_p99_ms': 266.555583, 'queue_latency_max_ms': 519.675709, 'wake_notifications': 2048, 'wake_credits': 1000000, 'process_wake_coalesce_sleeps': 0, 'process_wake_coalesce_ms': 0.0, 'process_claim_calls': 2064, 'process_empty_claims': 3, 'process_fallback_claims': 17, 'process_avg_claim_batch': 484.49612403100775, 'process_max_claim_batch': 1000, 'create_pipeline_flushes': 0, 'create_pipeline_commands': 0, 'create_pipeline_max_depth': 0, 'process_pipeline_flushes': 0, 'process_pipeline_commands': 0, 'process_pipeline_max_depth': 0, 'create_seconds': 16.27374333399348, 'process_seconds': 16.387489999993704, 'total_seconds': 16.39410724991467, 'client_cpu_seconds': 7.376633, 'client_cpu_percent': 44.99563707586697, 'create_flows_per_sec': 61448.67714062724, 'process_flows_per_sec': 61022.15775572612, 'end_to_end_flows_per_sec': 60997.527023327544}`

## 2026-06-14 IDT clean current native baseline after DBOS regression investigation

Context:
- Source FerricStore server, not Docker.
- Native protocol on `ferric://127.0.0.1:16388`.
- `FERRICSTORE_SHARD_COUNT=16`.
- Clean data directory per run.
- SDK raw tuple parser experiment was reverted before these runs; `pytest tests/test_protocol.py -q` passed with `106 passed`.
- A parallel KV SET+GET run was discarded because it contaminated both results.

### Native KV GET, clean

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset get-throughput \
  --key-count 100000 \
  --value-bytes 16 \
  --pretty
```

Result:

```text
GET throughput: 9,092,883.811 ops/s
batch latency p50: 6.652 ms
batch latency p95: 11.459833 ms
batch latency p99: 13.531167 ms
batch latency max: 24.342834 ms
client CPU: 91.6003%
errors: 0
```

### Native KV SET, clean

Command:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --preset set-throughput \
  --key-count 100000 \
  --value-bytes 16 \
  --pretty
```

Result:

```text
SET throughput: 2,172,602.448 ops/s
batch latency p50: 14.361416 ms
batch latency p95: 18.881875 ms
batch latency p99: 23.619083 ms
batch latency max: 63.815083 ms
client CPU: 22.9244%
errors: 0
```

### Native DBOS-style live queue, 100k flows, clean

Command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
E2E throughput: 81,424.127 flows/s
create throughput: 97,835.887 flows/s
process throughput: 81,615.861 flows/s
queue latency p50: 28.789417 ms
queue latency p95: 77.702083 ms
queue latency p99: 150.1085 ms
queue latency max: 882.863208 ms
claim calls: 276
empty claims: 5
avg claim batch: 362.319
errors/duplicates: 0
```

### Native DBOS-style live queue, 1M flows, clean

Command:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 1000000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Result:

```text
E2E throughput: 62,215.474 flows/s
create throughput: 62,389.458 flows/s
process throughput: 62,240.410 flows/s
queue latency p50: 45.8615 ms
queue latency p95: 130.433791 ms
queue latency p99: 198.618958 ms
queue latency max: 381.246709 ms
claim calls: 2052
empty claims: 1
avg claim batch: 487.329
max claim batch: 1000
producer backpressure waits: 1267
producer backpressure wait: 6809.933 ms
errors/duplicates: 0
```

Read:
- Current KV protocol path is not regressed versus recent native baselines.
- Current 1M DBOS live path is in the sustained `~60k/s` class, not the isolated `~200k/s` class.
- The `~200k/s` numbers were isolated Flow command-capacity runs, for example `create_many`, `complete_many`, or `claim_due` alone. They are not full live DBOS E2E because DBOS E2E performs durable create, claim coordination, durable complete, and worker scheduling.

## 2026-06-14 IDT DBOS latency tuning probes, 100k flows

Context:
- Same source server process, native protocol, 16 shards.
- Data directory was clean at server start, then each probe used a unique flow type on the same running server.
- These are tuning probes, not final clean baselines.
- Goal was to reduce queue tail latency without losing DBOS live throughput.

Common command shape:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:16388 \
  --flows 100000 \
  --workers 16 \
  --producers 4 \
  --partitions 16 \
  --server-shards 16 \
  --claim-batch-size 500 \
  --claim-partition-batch-size 16 \
  --claim-drain-batches 2 \
  --create-batch-size 500 \
  --complete-async-depth 4 \
  --transport many \
  --queued-shape live \
  --worker-api queue \
  --worker-mode polling \
  --partition-mode auto \
  --claim-job-only
```

Results:

```text
baseline75:
  E2E: 77,547/s
  p50: 29.386 ms
  p95: 83.522 ms
  p99: 100.501 ms
  max: 161.927 ms
  empty claims: 12 / 283

target30 (--producer-target-queue-latency-ms 30 --producer-min-rate-per-sec 20000):
  E2E: 49,903/s
  p50: 18.375 ms
  p95: 52.457 ms
  p99: 86.778 ms
  max: 128.040 ms
  empty claims: 1 / 264
  read: lower tail, but too much throughput loss.

inflight1_target30 (--protocol-create-inflight-batches 1 --producer-target-queue-latency-ms 30 --producer-min-rate-per-sec 20000):
  E2E: 48,006/s
  p50: 19.195 ms
  p95: 49.130 ms
  p99: 66.843 ms
  max: 79.859 ms
  empty claims: 1 / 261
  read: best p99, but too much throughput loss for default/high-throughput mode.

batch250_target30 (--claim-batch-size 250 --create-batch-size 250 --producer-target-queue-latency-ms 30 --producer-min-rate-per-sec 20000):
  E2E: 41,053/s
  p50: 15.348 ms
  p95: 64.341 ms
  p99: 89.244 ms
  max: 114.367 ms
  empty claims: 0 / 513
  read: more calls and much lower throughput.

target50 (--producer-target-queue-latency-ms 50 --producer-min-rate-per-sec 50000):
  E2E: 46,683/s
  p50: 40.974 ms
  p95: 148.203 ms
  p99: 330.966 ms
  max: 377.353 ms
  empty claims: 3 / 273
  read: worse throughput and worse tail in this probe.

pending20000 (--producer-max-pending-credits 20000):
  E2E: 39,219/s
  p50: 64.254 ms
  p95: 178.407 ms
  p99: 346.143 ms
  max: 722.021 ms
  empty claims: 6 / 276
  read: rejected.

pending10000 (--producer-max-pending-credits 10000):
  E2E: 49,519/s
  p50: 51.323 ms
  p95: 142.960 ms
  p99: 206.477 ms
  max: 231.173 ms
  empty claims: 2 / 265
  read: rejected.

complete8 (--complete-async-depth 8):
  E2E: 59,757/s
  p50: 43.385 ms
  p95: 127.608 ms
  p99: 169.006 ms
  max: 701.740 ms
  empty claims: 1 / 272
  read: rejected versus baseline.

fuse_complete_claim (--fuse-complete-claim):
  E2E: 71,473/s
  p50: 34.615 ms
  p95: 78.992 ms
  p99: 152.888 ms
  max: 221.486 ms
  empty claims: 11 / 283
  read: rejected for this workload.

lanes64 (--protocol-lanes 64):
  E2E: 58,705/s
  p50: 38.288 ms
  p95: 119.789 ms
  p99: 165.421 ms
  max: 252.877 ms
  empty claims: 0 / 264
  read: rejected; more lanes increased contention.

conns2 (--protocol-worker-connections 2):
  E2E: 56,140/s
  p50: 48.442 ms
  p95: 125.027 ms
  p99: 170.297 ms
  max: 1,483.098 ms
  empty claims: 0 / 272
  read: rejected; one native connection remains better for this benchmark.

drain1 (--claim-drain-batches 1):
  E2E: 58,229/s
  p50: 43.606 ms
  p95: 139.517 ms
  p99: 1,123.567 ms
  max: 1,456.817 ms
  empty claims: 7 / 278
  read: rejected.

no_backpressure (-- --no-adaptive-producer-backpressure):
  E2E: 35,279/s
  p50: 75.952 ms
  p95: 229.688 ms
  p99: 275.226 ms
  max: 1,279.605 ms
  empty claims: 3 / 273
  read: rejected; adaptive backpressure is needed.
```

Conclusion:
- Current high-throughput default shape remains the best balance among tested knobs.
- A low-latency mode exists by using target `30ms` and one create inflight batch, but it trades throughput down to roughly `48k/s` on 100k-flow probes.
- Extra native lanes, two worker connections, fused complete+claim, lower drain count, and pending-credit caps did not improve this benchmark.

## 2026-06-14 IDT isolated Flow command check during DBOS 100k investigation

Context:
- Source FerricStore server, native protocol, 16 shards.
- Command shape: `--flows 100000 --batch-size 500 --setup-batch-size 500 --inflight-batches 64 --connections 1 --protocol-lanes 32 --partitions 16 --payload-bytes 0`.
- First group ran sequentially on one clean-at-start server; second group ran selected operations with a fresh clean server per operation.

Sequential same-server results:

```text
create-many:
  throughput: 259,928/s
  p50 batch: 114.654 ms
  p99 batch: 141.090 ms
  errors: 0

complete-many:
  throughput: 148,755/s
  p50 batch: 161.968 ms
  p99 batch: 296.868 ms
  errors: 0

transition-many:
  throughput: 199,769/s
  p50 batch: 152.208 ms
  p99 batch: 162.669 ms
  errors: 0

start-and-claim:
  throughput: 133,342/s
  p50 batch: 220.028 ms
  p99 batch: 272.584 ms
  errors: 0
```

Fresh server per-operation results:

```text
complete-many:
  throughput: 146,184/s
  p50 batch: 171.559 ms
  p99 batch: 287.689 ms
  errors: 0

transition-many:
  throughput: 200,060/s
  p50 batch: 140.922 ms
  p99 batch: 162.050 ms
  errors: 0

start-and-claim:
  throughput: 180,574/s
  p50 batch: 168.239 ms
  p99 batch: 189.644 ms
  errors: 0
```

Read:
- `create-many` is still in the expected high command-capacity range.
- `complete-many` is materially lower than older native Flow command samples around `~220k/s`.
- `transition-many` is around `200k/s`, lower than the older `~290k/s` sample but still high.
- DBOS live E2E is likely capped partly by terminal/complete throughput plus durable create/complete write contention.
- Next investigation should focus on terminal command apply/encoding and any extra projection/history/value-ref work in complete path.

## 2026-06-14 Restate-shaped direct-step benchmark work

Added `examples/protocol_restate_latency_benchmark.py` to measure direct workflow latency using native `FLOW.START_AND_CLAIM`, `FLOW.STEP_CONTINUE`, and terminal complete, separate from queue/`claim_due` scheduling.

Focused tests:

```bash
pytest tests/test_protocol_restate_latency_benchmark.py -q
```

Result:

```text
5 passed in 0.04s
```

Clean-ish local source server shape:

```text
MIX_ENV=prod
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_SHARD_COUNT=16
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500
FERRICSTORE_DATA_DIR=/tmp/ferricstore-restate-latency/data
FERRICSTORE_PORT=17379
FERRICSTORE_HEALTH_PORT=17380
FERRICSTORE_NATIVE_PORT=17388
```

Important debugging findings:

```text
1. Per-workflow serial direct path is not the right Restate high-load shape.
   A single 1-step direct workflow can be ~4.8ms, but repeated serial calls fall onto WAL/fsync cadence.

2. Compact START_AND_CLAIM batching requires shared batch fields.
   Varying WORKER per item forced generic PIPELINE and made 500 starts take ~616ms.
   Fixed wave mode to use one worker identity per wave.

3. Existing protocol_flow_commands microbench on the same server showed START_AND_CLAIM compact path is fast:
   operation=start-and-claim, flows=1000, batch=500, inflight=1
   items_per_sec: 34,108/s
   batch p50: 10.73ms
   batch p99: 18.03ms

4. Existing complete-many compact path is fast:
   operation=complete-many, flows=1000, batch=500, inflight=1
   items_per_sec: 80,060/s
   batch p50: 5.99ms
   batch p99: 6.05ms

5. Initial direct wave mode after shared worker fix, auto partition, steps=1, workflows=1000, batch=500:
   workflows_per_sec: 18,232/s
   p50: 24.41ms
   p99: 29.39ms
   This beats Restate 1-step p99 target (<40ms) but not their high-load throughput target (23,131/s).

6. Increasing inflight waves improved throughput but worsened tail latency:
   steps=1, workflows=20k, batch=250, inflight=4
   workflows_per_sec: 21,636/s
   p50: 36.36ms
   p99: 187.47ms

7. Multi-step direct workflow still misses Restate high-load targets.
   The current client-driven path pays one durable batch/roundtrip per logical step.
   To beat Restate 3-step/9-step latency, next work should fuse direct step execution or add compact job-return STEP_CONTINUE plus better partition/shard grouping.
```

Interpretation:

```text
We can beat Restate-style p99 for 1-step latency on this local source setup, but not yet the combined high-load throughput+p99 gate.
For 3-step and 9-step, the current client-driven direct-step path is structurally too roundtrip-heavy.
The next real optimization is a direct workflow/step batch primitive that durably records multiple step transitions in one Ra apply/WAL batch, or at minimum a compact STEP_CONTINUE return mode plus shard-local wave scheduling.
```

### 2026-06-14 Restate-shaped step-return fix benchmark

Fixed native compact `FLOW.STEP_CONTINUE RETURN JOBS_COMPACT` server path:

- `Native.Codec` already decoded compact pipeline mode `33`.
- `Native.Commands.validate_compact_pipeline/4` did not allow mode `33`, causing live native server to reject SDK frames with `ERR native compact PIPELINE payload is invalid`.
- `Flow.MutationAttrs.step_continue_attrs/5` rejected response-only `return: :jobs_compact`; now accepts `:return` without adding it to mutation attrs.

Focused tests:

```bash
cd /Users/yoavgea/repos/ferricstore
MIX_ENV=test mix test \
  apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs:2979 \
  apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs:232 \
  apps/ferricstore/test/ferricstore/flow_write_contract_test.exs:1106

cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
pytest \
  tests/test_protocol.py::test_protocol_compacts_batched_step_continue_with_job_return_mode \
  tests/test_protocol.py::test_flow_client_step_continue_can_return_compact_job \
  tests/test_protocol_restate_latency_benchmark.py -q
```

Results:

- Server focused: `3 tests, 0 failures` across selected locations.
- Python focused: `7 passed in 0.04s`.

Clean source server:

```bash
MIX_ENV=prod \
FERRICSTORE_NATIVE_ENABLED=true \
FERRICSTORE_SHARD_COUNT=16 \
FERRICSTORE_RELEASE_CURSOR_INTERVAL=500 \
FERRICSTORE_DATA_DIR=/tmp/ferricstore-restate-latency/data \
FERRICSTORE_PORT=18379 \
FERRICSTORE_HEALTH_PORT=18380 \
FERRICSTORE_NATIVE_PORT=18388 \
mix run --no-halt
```

Restate-shaped wave benchmark, one native connection, 64 lanes, batch 250, inflight 1:

| Shape | Workflows | Workflow/s | Durable cmd/s | p50 | p90 | p99 | Restate high target |
|---|---:|---:|---:|---:|---:|---:|---|
| 1-step | 10,000 | 11,359/s | 22,718/s | 21.21ms | 23.01ms | 40.19ms | 23,131 rps, p50 16ms, p99 40ms |
| 3-step | 5,000 | 5,002/s | 20,007/s | 40.66ms | 42.84ms | 200.39ms | 16,844 rps, p50 58ms, p90 76ms, p99 98ms |
| 9-step | 2,000 | 2,499/s | 24,992/s | 100.86ms | 110.59ms | 110.59ms | 8,571 rps, p50 116ms, p99 163ms |

Interpretation:

- The mode-33 compact step-return path is now functional.
- 9-step latency beats Restate high-load p50/p99 on this local source run, but throughput is lower because this benchmark is client-driven and performs one durable command per step.
- 3-step p50/p90 are strong, but p99 has one batch outlier. Next target is tail-latency consistency around Ra/WAL batch wait, shard scheduling, or benchmark wave timing.

### 2026-06-14 Restate-shaped warmup latency run

Changes before this run:

- Added SDK `return_ok_on_success` to `FlowClient.complete_many()` and `FlowClient.complete_jobs()`.
- Restate-shaped benchmark now omits `PAYLOAD` when `payload_bytes=0` instead of sending an explicit empty payload.
- Added explicit `--warmup-workflows` so warmup is disclosed, not hidden.

Focused SDK tests:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
pytest \
  tests/test_protocol_restate_latency_benchmark.py \
  tests/test_client.py::test_complete_many_can_return_ok_on_success \
  tests/test_client.py::test_claim_jobs_and_complete_jobs_hide_hot_path_options -q
```

Result: `7 passed in 0.05s`.

Clean source server, same as above, then Restate-shaped wave benchmark with one native connection, 64 lanes, batch 250, inflight 1, `--warmup-workflows 1000`.

| Shape | Workflows | Warmup | Workflow/s | Durable cmd/s | p50 | p90 | p99 | Restate high p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1-step | 10,000 | 1,000 | 11,864/s | 23,728/s | 20.71ms | 22.51ms | 24.11ms | 40ms |
| 3-step | 5,000 | 1,000 | 6,146/s | 24,582/s | 40.41ms | 41.33ms | 42.32ms | 98ms |
| 9-step | 2,000 | 1,000 | 2,545/s | 25,447/s | 97.52ms | 103.92ms | 103.92ms | 163ms |

Interpretation:

- Latency target is now beaten for all three high-load Restate p99 rows on this local single-node source run.
- Throughput is not an apples-to-apples Restate RPS claim: this benchmark is client-driven and still performs one durable command per step (`steps + 1` durable commands per workflow).
- Warmup is explicit and reported by the benchmark output.

### 2026-06-14 Restate target-rate/open-loop benchmark

Added explicit target-rate scheduling to `examples/protocol_restate_latency_benchmark.py`:

- `--target-rps N` schedules wave batches at `batch_size / N` seconds.
- Result now reports `target_rps`, `target_achieved_ratio`, and `beats_restate_high_load_latency_only`.
- This avoids mistaking a lower-throughput closed-loop run for an equivalent high-load run.

Focused tests:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate
pytest tests/test_protocol_restate_latency_benchmark.py -q
```

Result: `5 passed in 0.04s`.

Clean source server target-rate matrix, one native connection, 64 lanes, warmup 1000, batch 250, inflight 8:

| Shape | Target rps | Achieved rps | Achieved ratio | p50 | p90 | p99 | Restate high p99 | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1-step | 23,131/s | 22,641/s | 97.9% | 29.17ms | 35.46ms | 43.63ms | 40ms | close, not green |
| 3-step | 16,844/s | 16,115/s | 95.7% | 46.71ms | 73.23ms | 136.28ms | 98ms | not green |
| 9-step | 8,571/s | 7,457/s | 87.0% | 130.74ms | 234.16ms | 334.90ms | 163ms | not green |

Additional tuning attempts on same server:

| Shape | Inflight | Achieved rps | Ratio | p99 |
|---|---:|---:|---:|---:|
| 1-step | 3 | 18,474/s | 79.9% | 143.35ms |
| 1-step | 4 | 20,119/s | 87.0% | 148.49ms |
| 3-step | 4 | 15,975/s | 94.8% | 143.84ms |

Interpretation:

- Closed-loop/warm latency is strong, but target-rate high-load equivalence is not solved yet.
- Tail latency rises when the benchmark tries to sustain Restate's high-load RPS, especially for 9-step because FerricFlow currently performs `steps + 1` durable commands per workflow.
- Next real product work is either:
  - profile WAL/Ra/apply p99 under target-rate load, then tune adaptive commit/apply batching, or
  - add a durable fused workflow-step primitive for server-side step chains so a 9-step no-op workflow does not require 10 client-driven durable command batches.

## Restate latency public SDK profile controls

Added reproducibility controls to `examples/protocol_restate_latency_benchmark.py` after clean-dir runs showed startup noise when the native listener was open before all Raft shards finished leader bootstrap.

New options:

```bash
--profile restate-high-load
--startup-settle-seconds 6
```

The `restate-high-load` profile keeps the benchmark on the public SDK `run_steps_many` path and applies the tuned high-load shape unless explicit values are supplied:

```text
steps=1: batch_size=180, inflight_batches=4
steps=3: batch_size=500, inflight_batches=4
steps=9: batch_size=500, inflight_batches=4
```

Latest clean source-server runs with fresh data dir, 16 shards, one native connection, 64 protocol lanes, and startup settle before measurement:

```text
1-step: workflows/sec ~50.1k, p50 13.13ms, p90 16.32ms, p99 21.32ms
3-step: workflows/sec ~112.5k, p50 16.06ms, p90 17.56ms, p99 18.36ms
9-step: workflows/sec ~103.6k, p50 17.59ms, p90 19.50ms, p99 20.62ms
```

Focused validation:

```bash
pytest tests/test_protocol_restate_latency_benchmark.py -q
# 16 passed
```

## Restate latency readiness and correctness sampling

Added optional benchmark proof controls so the Restate-style latency run can separate measurement from startup/readiness and correctness checks:

```bash
--readiness-probes N
--verify-sample N
```

Behavior:

```text
readiness_probes: durable one-item FLOW.RUN_STEPS_MANY commands before warmup/measurement
verify_sample: post-measurement FLOW.GET samples that assert terminal completed state and expected version floor
```

These controls run outside the timed measurement window and are reported in result JSON:

```text
readiness_probes
readiness_completed
verify_sample_requested
verify_sample_checked
verify_sample_errors
```

Recommended evidence run shape:

```bash
python examples/protocol_restate_latency_benchmark.py \
  --url ferric://127.0.0.1:18388 \
  --steps 3 \
  --workflows 100000 \
  --warmup-workflows 10000 \
  --chain-submit-mode run-steps-many \
  --profile restate-high-load \
  --startup-settle-seconds 6 \
  --readiness-probes 16 \
  --verify-sample 32 \
  --pretty
```

Focused validation:

```bash
pytest tests/test_protocol_restate_latency_benchmark.py -q
# 19 passed
```

## Restate latency profile retune after proof runs

Proof-shaped runs with readiness probes and post-run `FLOW.GET` verification showed that the earlier `steps=3` profile of `batch_size=500, inflight_batches=4` could produce unacceptable batch-level tail latency on this local machine, even though sampled state verification remained correct.

Observed problematic 3-step proof run, fresh source server, 16 shards, one native connection, 64 lanes:

```text
batch=500, inflight=4
workflows/sec ~47.4k
p50 24.43ms, p90 80.56ms, p99 235.96ms
verify_sample_errors 0
```

Retuned 3-step matrix sample on same server:

```text
batch=250, inflight=4
workflows/sec ~38.6k
p50 16.05ms, p90 60.77ms, p99 86.16ms
verify_sample_errors 0
```

That shape beats Restate 3-step high-load thresholds for throughput and p50/p90/p99 latency in the sampled run. Updated `--profile restate-high-load` for `steps=3` to:

```text
batch_size=250, inflight_batches=4
```

1-step remains not proven against the full high-load target in the current evidence. Some tested shapes pass either throughput or p99, but not both p50/p99/throughput together. Do not claim full 1-step high-load victory yet.

## Clean 1-step proof check

Fresh source server, clean data dir, 16 shards, one native connection, 64 lanes, startup settle, readiness probes, and `FLOW.GET` sampling.

Tested two 1-step shapes without continuing a destructive matrix on the same data dir:

```text
batch=250, inflight=1
workflows/sec ~19.2k
p50 11.35ms, p90 12.94ms, p99 64.28ms
verify_sample_errors 0

batch=100, inflight=8
workflows/sec ~36.3k
p50 17.85ms, p90 22.11ms, p99 117.68ms
verify_sample_errors 0
```

Conclusion: 1-step is not yet proven against the full Restate high-load target (`rps >= 23.1k`, `p50 < 16ms`, `p99 < 40ms`). Current evidence shows correctness is intact, but batch/request tail latency needs another server-side or benchmark-shape optimization before claiming 1-step high-load victory.

## 1-step target-rate proof and client allocation cut

Added an auto-partition benchmark fast path for `run-steps-many` that passes plain flow ids directly to `FlowClient.run_steps_many` instead of building `WorkflowSpec` objects and per-item dicts before the public SDK call. This keeps the public SDK path but reduces benchmark-side allocation.

Focused validation:

```bash
pytest tests/test_protocol_restate_latency_benchmark.py -q
# 21 passed
```

Target-rate 1-step proof on clean source server, 16 shards, one native connection, 64 lanes, target `23131/s`, readiness probes and `FLOW.GET` verification:

```text
Before allocation cut, batch=250, inflight=2:
workflows/sec ~22.7k, target ratio ~0.982
p50 14.16ms, p90 19.16ms, p99 123.20ms
verify_sample_errors 0

Before allocation cut, batch=250, inflight=4:
workflows/sec ~23.1k, target ratio ~0.999
p50 12.80ms, p90 25.86ms, p99 125.35ms
verify_sample_errors 0

After allocation cut, batch=250, inflight=4 on already-loaded server:
workflows/sec ~23.0k, target ratio ~0.992
p50 32.84ms, p90 55.40ms, p99 128.58ms
verify_sample_errors 0
```

Conclusion: the allocation cut is a code-quality/benchmark-overhead improvement, but current evidence says it does not solve 1-step high-load p99. The remaining issue is rare batch stalls under write load. Next useful step is stage timing/profiling for the 1-step `FLOW.RUN_STEPS_MANY` path or a lower-jitter async protocol runner.

## 2026-06-14 - Rejected compact `FLOW.RUN_STEPS_MANY` request path

Goal: reduce 1-step Restate-style latency by replacing the generic native map payload for `FLOW.RUN_STEPS_MANY` with a custom compact request body.

Result: rejected. The compact request body was decoded in Elixir and regressed the benchmark versus the existing generic NIF body decode path.

Baseline before experiment, 1-step target-rate shape (`batch=250`, `inflight=4`, `target_rps=23131`, 100k workflows, 16 shards, one native connection):

```text
workflows_per_sec: 23107.5
p50: 13.08ms
p90: 39.84ms
p99: 115.48ms
verify_sample_errors: 0
```

Compact request experiment samples:

```text
sample 1: workflows_per_sec 15971.5, p50 45.37ms, p99 187.12ms
sample 2: workflows_per_sec 6977.3, p50 123.83ms, p99 401.99ms
```

Decision:

```text
Do not add an Elixir-decoded compact FLOW.RUN_STEPS_MANY request format.
If we revisit compact RUN_STEPS_MANY, decode/build must happen in native/Rust or avoid extra Elixir binary parsing/allocation.
```

Validation after reverting rejected path:

```text
pytest tests/test_protocol.py::test_encodes_protocol_flow_run_steps_many tests/test_protocol_restate_latency_benchmark.py -q
24 passed

mix test apps/ferricstore_server/test/ferricstore_server/native/codec_test.exs apps/ferricstore_server/test/ferricstore_server/native/commands_test.exs
150 tests, 0 failures
```

## 2026-06-14 Restate-style workflow latency profile correction

### Benchmark profile semantics

`examples/protocol_restate_latency_benchmark.py --profile restate-high-load` now applies the tuned public-SDK batch/in-flight shape and compares against Restate high-load targets, but it does not automatically set `--target-rps`.

Reason:

```text
Restate high-load numbers are comparison targets/observed throughput, not a Python sleep-based input rate.
Auto target pacing caused local sleep jitter to catch up in bursts, inflating Ra wait and p99 latency.
Use --target-rps explicitly only when testing fixed-rate generator behavior.
```

### Clean source server

```bash
cd /Users/yoavgea/repos/ferricstore
MIX_ENV=prod \
FERRICSTORE_NATIVE_ENABLED=true \
FERRICSTORE_NATIVE_TRACE_ENABLED=false \
FERRICSTORE_SHARD_COUNT=16 \
FERRICSTORE_DATA_DIR=/tmp/ferricstore-restate-3step-final/data \
FERRICSTORE_PORT=18215 \
FERRICSTORE_HEALTH_PORT=18216 \
FERRICSTORE_NATIVE_PORT=19299 \
mix run --no-halt
```

### Public SDK 3-step high-load run

```bash
cd /Users/yoavgea/repos/ferricstore-python
PYTHONPATH=src python examples/protocol_restate_latency_benchmark.py \
  --url ferric://127.0.0.1:19299 \
  --profile restate-high-load \
  --steps 3 \
  --workflows 100000 \
  --warmup-workflows 0 \
  --chain-submit-mode run-steps-many \
  --startup-settle-seconds 2 \
  --readiness-probes 8 \
  --verify-sample 32 \
  --slow-wave-count 5 \
  --slow-wave-min-ms 40 \
  --pretty
```

Result:

```text
completed: 100000
errors: 0
verify_sample_errors: 0
batch_size: 500
inflight_batches: 4
workflows_per_sec: 106885/s
latency_p50_ms: 17.826
latency_p90_ms: 21.761
latency_p99_ms: 25.646
latency_max_ms: 31.228
beats_restate_high_load_all: true
restate target: 16844/s, p50 58ms, p90 76ms, p99 98ms
```

Notes:

```text
This uses the public FlowClient.run_steps_many workflow surface over ferric://.
Durability path remains WARaft + Bitcask; trace was disabled for the published sample.
Trace diagnostics added in the same session showed slow fixed-rate runs were dominated by server_ra_wait_us, not Flow apply, Bitcask append, index mutation, or protocol encode/decode.
```

## 2026-06-14 Restate high-load profile final local validation

Profile behavior:

```text
--profile restate-high-load chooses the public-SDK optimized mode by step count:
  steps=1 -> run-steps-many-shard-local, batch=250, inflight=4, shard_local_submit_concurrency=8
  steps=3 -> run-steps-many, batch=500, inflight=4
  steps=9 -> run-steps-many, batch=500, inflight=1
No automatic --target-rps pacing. Fixed-rate pacing is opt-in.
```

All runs below used:

```text
source FerricStore server
MIX_ENV=prod
FERRICSTORE_NATIVE_ENABLED=true
FERRICSTORE_NATIVE_TRACE_ENABLED=false
FERRICSTORE_SHARD_COUNT=16
fresh data dir per run
public FlowClient path over ferric://
workflows=100000
warmup_workflows=0
readiness_probes=8
verify_sample=32
payload/result bytes=0
```

Final local samples:

```text
steps=1:
  mode: run-steps-many-shard-local
  batch: 250
  inflight: 4
  shard_local_submit_concurrency: 8
  workflows_per_sec: 31839/s
  p50: 3.212ms
  p90: 5.469ms
  p99: 18.833ms
  max: 86.393ms
  verify_sample_errors: 0
  beats_restate_high_load_all: true
  target: 23131/s, p50 16ms, p99 40ms

steps=3:
  mode: run-steps-many
  batch: 500
  inflight: 4
  workflows_per_sec: 104696/s
  p50: 18.081ms
  p90: 23.659ms
  p99: 27.017ms
  max: 31.294ms
  verify_sample_errors: 0
  beats_restate_high_load_all: true
  target: 16844/s, p50 58ms, p90 76ms, p99 98ms

steps=9:
  mode: run-steps-many
  batch: 500
  inflight: 1
  workflows_per_sec: 32470/s
  p50: 11.312ms
  p90: 26.494ms
  p99: 71.523ms
  max: 76.730ms
  verify_sample_errors: 0
  beats_restate_high_load_all: true
  target: 8571/s, p50 116ms, p99 163ms
```

Diagnostic note:

```text
Trace-enabled slow samples showed p99 spikes in fixed-rate runs were dominated by server_ra_wait_us. Flow apply, Bitcask append, index mutation, and protocol encode/decode were not the bottleneck. The final profile avoids Python fixed-rate catch-up bursts and uses per-shape batch/inflight settings that preserve durable WARaft + Bitcask writes.
```

## 2026-06-14 - Native GET 256B pipeline=10 latency preset retune

Goal: compare closer to Dragonfly's documented `--pipeline=10` GET shape and reduce tail latency for small batches.

Change:

```text
examples/protocol_kv_benchmark.py --preset get-latency
request_mode: many
pipeline: 10
protocol_lanes: 8
inflight_batches: 8
prebuild_keys: true
```

Reason:

```text
The previous get-latency preset used request_mode=pipeline, protocol_lanes=1, inflight_batches=64, prebuild_keys=false.
That shape measured Python/scheduler overhead more than server latency. Small batch latency improved by lowering queued in-flight work and using the compact many/MGET path with prebuilt keys.
```

Command:

```bash
PYTHONPATH=src python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:19670 \
  --preset get-latency \
  --value-bytes 256 \
  --pretty
```

Result:

```text
GET 256B pipeline=10
requests_per_sec: 526,508/s
p50 batch: 0.149ms
p95 batch: 0.197ms
p99 batch: 0.227ms
max batch: 3.117ms
errors: 0
client_cpu_percent: 125%
```

Interpretation:

```text
This is the low-latency one-client Python SDK shape, not the max-throughput shape.
It trades deep batching throughput for much lower batch tail latency.
For max aggregate GET throughput, keep get-throughput: pipeline=1000, lanes=64, inflight=64.
```
