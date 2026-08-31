# Project Status

FerricStore Python SDK is public alpha.

Current version:

```text
0.13.0
```

Python SDK `0.13.0` requires FerricStore `0.11.4` or newer and validates the
native TCP and request/response HTTP transports against FerricStore 0.11.14.
Native wire protocol v1 is unchanged.

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
- Sync and async HTTP(S) transport for ordinary commands and pipelines.
- High-level queue and workflow clients.
- Worker runtimes.
- Retry/backoff policy helpers.
- Named values and value refs.
- Query/history helpers.
- LangGraph checkpoint persistence and cross-thread memory stores.
- LangChain agent memory and FerricFlow-to-LangGraph execution bridges.
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
