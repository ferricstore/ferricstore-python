# Changelog

All notable changes to the FerricStore Python SDK will be documented here.

The project is currently public alpha. APIs may change before `1.0`.

## Unreleased

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
