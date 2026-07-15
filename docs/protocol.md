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
- Each connection admits at most 4,096 in-flight requests and 64 MiB of cumulative encoded request data.
- Queue and workflow worker facades use a separate bounded claim pool (one connection by default).

For throughput benchmarks or very high concurrency, override explicitly:

```python
queue = QueueClient.from_url("ferric://127.0.0.1:6388", max_connections=2)
client = FlowClient.from_url("ferric://127.0.0.1:6388", lanes=64)
```

`max_connections` and `lanes` must both be positive. With topology routing,
`max_connections` is the pool size for each discovered endpoint, not a global
cluster-wide total.

Tune the admission guards with `max_inflight_requests`,
`max_pending_request_bytes`, and `max_batch_items`. A request that would cross a
wire bound is rejected before its frame is written; a batch over the default
10,000-item planning limit is rejected before per-command objects are allocated.
Sync `submit_*` futures use the adapter `timeout` as their response deadline, so
abandoned futures cannot remain registered forever. Set an individual guard to
`None` only when an outer layer enforces an equivalent finite bound.

Generic protocol values are decoded in one linear pass and permit at most 128
levels of list/map nesting. Excessive nesting is rejected as a protocol error
instead of exhausting the Python call stack.

Generic values accept `None`, booleans, signed 64-bit integers, floats, strings,
bytes, bytearrays, lists, tuples, and maps whose keys are strings or bytes. Map
keys that collapse to the same wire bytes are rejected instead of silently
overwriting a decoded entry, and compressed responses must contain exactly one
zlib stream with no trailing data.

Response collections are limited to a cumulative 100,000 decoded items by
default, before any result list is allocated. One budget is shared by generic
and compact lists/maps at every nesting level, including records embedded in
pipeline responses. Configure `max_decoded_collection_items` on `from_url(...)`
when a workload deliberately needs a different cap. Pipelines, `MGET`, and Flow
many responses must also match the exact cardinality carried by their request,
whether the server uses compact encoding or the legacy generic value encoding.
Counted command sections reject booleans, negative counts, declarations larger
than the remaining input, and nested cardinality mismatches before parsing or
allocating their items.

Logical responses also permit at most 1,024 total chunks by default.
Configure `max_response_chunks` on `from_url(...)` when a trusted deployment
needs a different limit; this bound is independent of encoded response bytes
and prevents zero-length chunks from creating unbounded framing overhead.

## HA topology routing

Constructing a client with multiple URLs enables SHARDS-based topology routing:

```python
client = FlowClient.from_urls(
    ["ferrics://store-a.example.com:6389", "ferrics://store-b.example.com:6389"],
    endpoint_policy="seed_hosts",
    max_connections=2,
)
```

Every learned route is an exact `(endpoint, lane_id)` destination. Commands,
submitted futures, keyed sessions, and batches preserve that lane; batches are
grouped by both endpoint and lane while their results remain in request order.
For Flow commands, routing uses the server's physical hash tag for the effective
partition: explicit logical partitions use the server-compatible SHA-256 tag,
while unpartitioned Flow IDs use `{fa:<crc32(id) % 256>}`. Cross-partition Flow
operations and schedule control operations stay on the control path. The SDK
caches only a bounded number of small partition digests; oversized logical keys
are hashed without being retained.

Discovery is rejected unless `route_epoch` is non-negative, `shard_count` is
valid, all 1024 slots are covered exactly once, every declared shard appears,
lane IDs fit an unsigned 32-bit integer, and learned hosts and ports are valid.
Installed topology epochs are monotonic: an older epoch and a conflicting view
at the current epoch are rejected, while an identical current-epoch refresh is
a no-op that does not churn adapters or the routing generation.
TLS connections identify and reuse endpoints by `native_tls_port`; plaintext
connections use `native_port`.

After discovery, unkeyed control traffic prefers the last seed that returned a
valid topology. If that endpoint later fails, the SDK refreshes discovery for
subsequent traffic. Read-only `PING`, `SHARDS`, and `OPTIONS` calls may be
retried once on the newly healthy endpoint; commands with possible side
effects are never replayed automatically.

## Connection-affine commands

Transactions must reserve one physical connection. When a pool or topology
router is configured, use the transaction context and provide the routing key:

```python
with client.transaction(key="account:{42}", watch=["account:{42}"]) as tx:
    tx.command("SET", "account:{42}", b"updated")
```

`WATCH` is sent before `MULTI` on the same reserved connection. Direct
`MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH` calls are rejected on shared
root executors, including a direct native adapter; only an explicitly acquired
session may carry raw transaction state. A topology router also validates the
transaction routing key and every watched key before reserving the connection;
all of them must hash to one slot.

Topology-discovered endpoints are checked against `endpoint_policy` before a
connection is opened. Keyed pipelines are grouped by routed leader while result
order remains the same as request order. Within a routed leader, async SDK
pipelines execute compact mutation groups in command order; explicitly
independent transport batches can still use bounded concurrent fanout. Direct
multi-key `MGET`, `MSET`, and `DEL` commands must also hash to one slot; use a shared hash tag such as
`account:{42}:name` and `account:{42}:status`. Cross-slot commands are rejected
client-side instead of being ambiguously sent to a seed node.

The default `endpoint_policy="seed_hosts"` allows exact configured seed
endpoints and learned endpoints on trusted seed hosts. `"none"` permits only
the exact configured seed endpoints, `("allow_hosts", [...])` permits an
explicit host allowlist, and `"any"` opts out of host restrictions. A custom
`endpoint_validator` rejects an endpoint only when it returns `False`; returning
`None` means no additional decision. Credentials embedded in a seed URL remain
scoped to that physical seed (including when it is rediscovered in topology)
and are never copied to another learned endpoint. Explicit `username=` or
`password=` keyword arguments remain global by design.

Pre-encoded payload helpers do not expose their keys to the topology router.
They are therefore supported only when discovery reports exactly one exact
endpoint-and-lane route. If one endpoint serves multiple route lanes, or the
topology has multiple leaders, submit decoded commands so each command can be
grouped and routed safely.

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
