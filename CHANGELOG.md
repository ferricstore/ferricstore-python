# Changelog

All notable changes to the FerricStore Python SDK will be documented here.

The project is currently public alpha. APIs may change before `1.0`.

## Unreleased

- Replaced the 5,000-line client and 11,000-line protocol monoliths with bounded domain, codec, transport, pool, and topology modules behind API-compatible facades; added architecture dependency/size guards and public signature characterization tests.
- Made worker run/close transitions atomic, bounded blocking sync cleanup by the caller deadline, fixed autobatch mutation and callback-reentrancy races, aligned compact decode budgets with generic nesting, strictly validated protocol limits, removed large-frame and response-slice copies, made pending drains linear, and changed the package root to lazy public imports.
- Bounded pending protocol requests by count and encoded bytes, expired abandoned sync submission futures with one compact deadline scheduler per connection, isolated pipeline-future cancellation, restored pooled-event liveness after session contention, and split async workflow creation from explicit worker startup while retaining a deprecated compatibility alias.
- Scoped sync pending requests to their transport generation, made future completion races cleanup-safe, published async terminal event state before writer teardown, reserved pooled adapters during event polling, serialized topology wake-filter updates, and retried retained adapter cleanup on later lifecycle triggers.
- Invalidated ambiguous sync/async writes, retained transports and retired topology adapters until cleanup succeeds, drained removed endpoints on idle without another refresh, moved topology discovery and endpoint construction outside routing locks with contention-safe singleflight, bounded response chunks, reused sync fanout executors, preserved operation timeouts during shutdown, and rejoined timed-out async close operations.
- Preserved compact mutation order inside async user pipelines without disabling independent batch fanout, made lazy claim-pool creation atomic with sync client shutdown, retained failed topology adapters for close retry, and made direct async socket close rejoinable after caller cancellation.
- Decoded compact Flow-many item statuses consistently, made budget settlement idempotent and cancellation-retryable, made owned client construction and close failure-atomic, enforced sync worker close deadlines for standalone calls, and bounded async compact-batch and wake-subscription fanout.
- Serialized async effect reservation/settlement across cancellation, made async workflow close terminal and retryable, prevented deadline cleanup leaks, globally bounded topology warm/close fanout, added async `partition_by` and `run_steps_many` parity, and cached batch-reference fingerprints on coalescing hot paths.
- Phased async queue shutdown so workers settle before owned transports, made budget commit failures release reservations and follow configured error policy, aligned bounded sync/async fanout failure semantics, retired stale topology adapters after active work drained, and completed async workflow-context Flow command parity.
- Unified sync/async Flow enqueue planning, fixed async explicit-partition shard routing, bounded fanout and handler scheduling memory, enforced one cumulative nested decode budget across generic and compact collections, made async cleanup cancellation-safe, and preserved primary handler/transaction failures when cleanup also fails.
- Made async workflow fail-fast execution retain active sibling ownership, and made queue/workflow composite close attempt every owned resource while preserving the first failure.
- Made worker/pool/topology construction transactional, bounded direct-run workflow shutdown, replaced recursive-slicing protocol decoding with one linear bounded codec, and retained the last healthy HA control seed.
- Fixed explicit-partition Flow routing after flag-form `PAYLOAD`, caller-managed worker shutdown, async topology adapter rollback, and bounded deque-backed autobatching.
- Fixed connection-affine transactions and Pub/Sub sessions when protocol pools are enabled.
- Fixed async request cancellation/startup cleanup, end-to-end request deadlines, bounded zlib response decoding, and native domain-error classification.
- Enforced safe autobatch limits and cancellation handling, and rejected lossy mixed `create_many` metadata.
- Added async parity for `run_steps_many`, compact `step_continue`, OK-only completions, and compact batch protocol paths.
- Made queue/workflow `claim_connections` create a distinct bounded claim pool and honor per-workload overrides.
- Hardened protocol pool lifecycle, affine-session fairness, transaction heartbeat suspension, and dirty Pub/Sub/transaction connection invalidation.
- Replaced polled, list-backed push-event queues with bounded deques and pool-wide notifications, including close/error wakeups.
- Scoped shared producer backpressure by transport and added a default elapsed retry budget.
- Added async wake subscriptions and fused completion-and-claim parity, backed by shared sync/async scheduler and command-building primitives.
- Made autobatching fail closed on response-cardinality mismatches and retain pool leases until underlying wire futures complete.
- Propagated independent many-command item failures through workers, workflows, fused completion, and autobatching instead of counting partial failures as success.
- Made transactions shard-affine with `transaction(key=..., watch=...)`, rejected unsafe direct transaction commands on rotating pools, and routed topology batches per leader.
- Made worker shutdown race-safe and bounded, prevented standalone async fusion from retaining unhandled leases, and replayed wake subscriptions across new/reconnected adapters.
- Pipelined heterogeneous queue/workflow completion results, hardened batch writes and cancelled futures, and added legacy compact-claim fallback parity.
- Split topology routing, wake-subscription state, worker lifecycle/deadlines, and heterogeneous mutation planning into shared sync/async architecture primitives.
- Rejected ambiguous cross-slot multi-key commands and transactions, activated wake subscriptions on newly connected leaders, and refreshed topology after routed batch/connection failures without replaying writes.
- Made worker task failures observable, kept cleanup reliable when tasks fail, preserved in-flight async claims during shutdown, and applied one deadline across shutdown stages.
- Pipelined heterogeneous complete/transition/retry/fail outcomes and distinct worker failures in one routed batch while retaining compact uniform-many fast paths.
- Applied one deadline to whole socket batch writes, routed submitted batches before sending, and made heartbeat replacement interrupt obsolete sleeping threads.
- Routed opaque pre-encoded payloads to the sole learned leader and rejected them before writing when a multi-leader topology makes routing ambiguous.
- Centralized command routing and batch equivalence/response invariants, including strict type-preserving coalescing, exact pipeline cardinality, and unambiguous status-envelope decoding.
- Fixed cold async leader routing, cancellation-safe pending-request teardown, transactional wake-subscription activation, and explicit empty partition handling.
- Added ordered, opt-in bounded fanout for independent auto-partition create groups; native sync and async transports use at most 16 concurrent calls while custom executors remain sequential by default.
- Pinned the fixed `msgpack` release in development tooling so dependency-audit environments do not resolve the vulnerable transitive version.
- Routed topology commands, batches, submitted futures, affine sessions, and opaque payloads by the exact SHARDS endpoint-and-lane destination; Flow IDs now use their effective explicit or canonical auto partition.
- Made TLS topology identity use the actual `native_tls_port`, applied `max_connections` per discovered endpoint, and propagated push listeners and reconnect subscriptions through endpoint pools.
- Strictly validated complete SHARDS slot tables, declared shard counts, epochs, lane IDs, endpoint hosts/ports, and positive protocol pool/lane sizes before opening routed traffic.
- Prevented close/reconnect races from resurrecting sockets and made async socket, pool, and topology cleanup continue safely when the close caller is cancelled.
- Preserved mapping-only Flow-many error diagnostics so `message`, `error`, and `reason` fields retain domain-specific error classification.

## 0.3.3

- Added Flow state mode policy support for FerricStore `0.7.5`, including FIFO/PARALLEL state policy command frames.
- Added sync and async Enterprise invocation helpers over the public native command contract.
- Updated Docker integration defaults to `ghcr.io/ferricstore/ferricstore:0.7.5`.

## 0.3.2

- Updated native protocol coverage for FerricStore `0.7.2`, including opcode parity checks against live `OPTIONS`.
- Added sync and async `FLOW.SEARCH` helpers with attribute and state metadata filters.
- Added sync and async management/control-plane helpers for capabilities, ACL, namespace, quota, and telemetry commands.
- Restricted default learned endpoint trust to exact seed endpoints unless a host is explicitly added to `trusted_hosts`.
- Made async topology `warm_connections=True` open learned adapters instead of only constructing them.
- Updated Docker and CI integration defaults to `ghcr.io/ferricstore/ferricstore:0.7.2`.

## 0.3.1

- Added sync and async native-protocol reconnect handling after heartbeat or idle
  socket drops, while keeping explicit user closes final.

## 0.3.0

- Added state-scoped Flow metadata with `state_meta` on sync and async mutation commands.
- Added `indexed_state_meta` policy support for indexing one state metadata key per workflow type.
- Added `FlowRecord.state_meta` and `FlowRecord.indexed_state_meta` decoding.
- Passed state metadata through queue/workflow outcomes and autobatch-safe mutation paths.
- Updated Docker integration defaults to FerricStore `0.6.0` and covered state metadata in live integration tests.

## 0.1.1

- Fixed Flow command compatibility issues found by live FerricStore integration tests.
- Stabilized claimed-job transition tests to use the claimed/current state.
- Expanded live integration coverage across FerricFlow repair, index, value-ref, signal, and store command paths.
- Relaxed live index assertions where the server returns a valid empty projection.
- Added integration workflow coverage, stronger CI/security gates, and tagged-release PyPI publishing.

- Added a root Apache-2.0 license file and PEP 561 `py.typed` marker for typed consumers.
- Simplified the README around high-level queue/workflow clients and moved examples away from low-level worker internals.
- Added tagged-release PyPI publishing workflow and release checklist.
- OSS readiness docs and CI.
- Expanded SDK docs for configuration, data placement, web/serverless, async,
  testing, security, and comparisons.

## 0.1.0

- Initial public-alpha SDK shape.
- Sync and async Flow clients.
- High-level queue and workflow clients.
- Worker runtimes.
- Retry and exception policy helpers.
- Named values and value refs.
- Query/history helpers.
- DBOS-style benchmark examples.
