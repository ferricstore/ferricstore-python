# Native FerricStore transport

The SDK can use FerricStore's native TCP protocol without changing Queue or Workflow code.

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

## Benchmark usage

Run the same benchmark scripts with a native URL after starting FerricStore with the native listener enabled:

```bash
python examples/dbos_style_benchmark.py \
  --url ferric://127.0.0.1:6388 \
  --mode queued \
  --transport many \
  --flows 1000000 \
  --server-shards 16
```

Native transport uses typed binary frames instead of RESP. `ferric://` is for plain TCP. `ferrics://` enables TLS. `native://` and `native+tls://` remain compatibility aliases.

Current implementation supports sync and async SDK calls, request-id multiplexing, native BATCH for SDK pipelines, auth, TLS, zlib negotiation, chunked responses, KV/custom commands, Flow commands, and FerricStore admin/observability commands exposed by the SDK.
