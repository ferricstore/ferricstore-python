# FerricStore protocol transport

The SDK can use FerricStore's protocol TCP transport without changing Queue or Workflow code.

## Sync client

```python
from ferricstore import FlowClient

client = FlowClient.from_url("ferric://127.0.0.1:6388")
client.command("SET", "k", b"v")
assert client.command("GET", "k") == b"v"
```

## Async client

```python
from ferricstore import AsyncFlowClient

client = AsyncFlowClient.from_url("ferric://127.0.0.1:6388")
await client.command("SET", "k", b"v")
assert await client.command("GET", "k") == b"v"
await client.close()
```

## Auth and TLS

```python
client = FlowClient.from_url("ferric://default:secret@127.0.0.1:6388")
tls_client = FlowClient.from_url("ferrics://default:secret@store.example.com:6389")
```

## Queue and workflow usage

Existing high-level APIs keep the same shape:

```python
from ferricstore import QueueClient, WorkflowClient

queue = QueueClient.from_url("ferric://127.0.0.1:6388")
workflow = WorkflowClient.from_url("ferric://127.0.0.1:6388")
```

## Defaults

The protocol SDK defaults are latency-first:

- `ferric://` queue and workflow clients use one multiplexed command connection by default.
- A protocol connection uses 8 request lanes by default.
- Worker claim traffic reuses the same multiplexed protocol connection by default.

For throughput benchmarks or very high concurrency, override explicitly:

```python
queue = QueueClient.from_url("ferric://127.0.0.1:6388", max_connections=2)
client = FlowClient.from_url("ferric://127.0.0.1:6388", lanes=64)
```

## Benchmark usage

Run the protocol SET/GET benchmark after starting FerricStore with the protocol
listener enabled:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --preset set-throughput
```

For GET throughput:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --preset get-throughput
```

For lower GET latency:

```bash
python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --preset get-latency
```

Run the DBOS-style queued workflow benchmark over the protocol transport:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --flows 1000000 \
  --server-shards 16
```

The wrapper expands to `examples/dbos_style_benchmark.py` with protocol defaults.
You can still call the underlying benchmark directly:

```bash
python examples/dbos_style_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --mode queued \
  --transport many \
  --flows 1000000 \
  --server-shards 16
```

FerricStore protocol transport uses typed binary frames instead of RESP. `ferric://` is for plain TCP. `ferrics://` enables TLS. 

Current implementation supports sync and async SDK calls, request-id multiplexing, protocol PIPELINE for SDK pipelines, auth, TLS, zlib negotiation, chunked responses, KV/custom commands, Flow commands, and FerricStore admin/observability commands exposed by the SDK.
