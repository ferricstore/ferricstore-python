# Retry and Errors

FerricFlow separates business outcomes from unexpected handler exceptions.
That distinction matters for correctness.

## Business failures are explicit

A business failure is part of the domain model. The handler should return the
next durable outcome directly.

```python
from ferricstore import fail, retry, transition


@workflow.state("charge")
def charge(job):
    result = payment_gateway.charge(job.payload)

    if result.declined:
        return fail(error=b"card declined")

    if result.rate_limited:
        return retry(error=b"rate limited", run_at_ms=next_attempt_ms)

    return transition("ship", values={"charge": result.id.encode()})
```

Use explicit outcomes for expected cases:

| Outcome | Use when |
| --- | --- |
| `transition(...)` | Work succeeded and the flow should move to another state. |
| `complete(...)` | Workflow is done. |
| `retry(...)` | Work should run again later. |
| `fail(...)` | Workflow should become terminal failed. |

The SDK should not guess these decisions.

## Handler exceptions use a policy

A handler exception is Python code raising unexpectedly: timeout, HTTP 500,
bug, dependency error, deploy interruption, and similar cases.

The SDK keyword is `exception_policy`. It applies only when the handler raises.

Default:

```text
exception_policy=ExceptionPolicy.RETRY
```

That means normal worker examples can omit it.

```python
@workflow.state("charge")
def charge(job):
    payment_gateway.charge(job.payload)
    return transition("ship")
```

If the handler raises, the SDK sends `FLOW.RETRY`.

Advanced override:

```python
from ferricstore import ExceptionPolicy


@workflow.state("validate", exception_policy=ExceptionPolicy.FAIL)
def validate(job):
    validate_input(job.payload)
    return transition("charge")
```

Policy behavior:

| `exception_policy` | Behavior |
| --- | --- |
| `ExceptionPolicy.RETRY` | Handler exception sends `FLOW.RETRY`. Best default for transient work. |
| `ExceptionPolicy.FAIL` | Handler exception sends `FLOW.FAIL`. Useful when exceptions mean invalid data. |
| `ExceptionPolicy.RAISE` | Exception escapes. Useful for tests or external supervisors. |

Prefer `exception_policy=ExceptionPolicy.RETRY` in new code.

## What happens internally

With retry:

```text
worker claims flow
handler raises
SDK catches exception
SDK sends FLOW.RETRY
flow becomes due again according to retry policy
worker keeps processing other flows
```

With fail:

```text
worker claims flow
handler raises
SDK catches exception
SDK sends FLOW.FAIL
flow becomes terminal failed
history records failure
```

With raise:

```text
worker claims flow
handler raises
SDK does not mutate the flow
exception escapes
lease eventually expires
another worker may reclaim later
```

## Retry policy defaults

Put the normal retry policy on the high-level client. Workflows and queues
created from that client inherit it.

```python
from ferricstore import RetryPolicy, WorkflowClient

client = WorkflowClient.from_url(
    url,
    retry_policy=RetryPolicy(
        max_retries=10,
        backoff="exponential",
        base_ms=500,
        max_ms=60_000,
        jitter_pct=20,
    ),
)
```

Override only where behavior is different:

```python
workflow = client.workflow(type="order", initial_state="charge")


@workflow.state("charge", retry_policy=RetryPolicy(max_retries=3))
def charge(job):
    payment_gateway.charge(job.payload)
    return transition("ship")
```

Install the policy for the workflow type:

```python
workflow.install_policy()
```

For queues:

```python
from ferricstore import QueueClient, RetryPolicy

client = QueueClient.from_url(url, retry_policy=RetryPolicy(max_retries=20))
emails = client.queue(type="email")
emails.install_policy()
```

## Low-level policy install

```python
from ferricstore import RetryPolicy

client.install_policy(
    "order",
    states={
        "charge": RetryPolicy(
            max_retries=10,
            backoff="exponential",
            base_ms=1_000,
            max_ms=86_400_000,
            jitter_pct=20,
            exhausted_to="failed",
        )
    },
)
```

## Backoff Kinds

Supported policy values:

* `none`
* `fixed`
* `linear`
* `exponential`

## Max Retries

FerricStore caps retry policy server-side. Do not use infinite retries. For long
retry windows, use larger `max_ms`, not unbounded retry count.
