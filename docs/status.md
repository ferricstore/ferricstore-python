# Project Status

FerricStore Python SDK is public alpha.

Current version:

```text
0.3.2
```

## What alpha means

- APIs may change before `1.0`.
- Server protocol may change before `1.0`.
- Production-shape testing is welcome.
- The high-level client direction is expected to remain:
  - `QueueClient`
  - `WorkflowClient`
  - `AsyncQueueClient`
  - `AsyncWorkflowClient`

## Stable enough to try

The SDK includes:

- Sync and async Flow clients.
- High-level queue and workflow clients.
- Worker runtimes.
- Retry/backoff policy helpers.
- Named values and value refs.
- Query/history helpers.
- Tests for command construction and SDK runtime behavior.

## Not yet stable

Expect changes around:

- Server protocol details.
- Advanced workflow/fanout options.
- Benchmark tuning flags.
- Worker tuning defaults.
- Some low-level `FlowClient` command shapes.

## Feedback wanted

The most useful feedback is:

- Real queue/workflow API friction.
- Serverless/web framework integration gaps.
- Missing production configuration knobs.
- Confusing retry/error semantics.
- Payload/value-ref ergonomics.
