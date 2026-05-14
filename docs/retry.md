# Retry and Errors

FerricFlow retry is explicit and stored on the flow. The SDK exposes two layers:

* command-level `client.retry(...)`
* workflow-level handler exception policy

## Handler Exception Policy

```python
@state("charge", on_error="retry")
def charge(self, job):
    raise RuntimeError("gateway down")
```

Sends `FLOW.RETRY`.

```python
@state("validate", on_error="fail")
def validate(self, job):
    raise ValueError("bad input")
```

Sends `FLOW.FAIL`.

## Return Retry Explicitly

```python
from ferricstore import retry

return retry(error=b"temporary", run_at_ms=next_attempt_ms)
```

## Install Retry Policy

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

