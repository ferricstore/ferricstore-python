# Changelog

All notable changes to the FerricStore Python SDK will be documented here.

The project is currently public alpha. APIs may change before `1.0`.

## Unreleased

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
