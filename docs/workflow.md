# Workflow DSL

The workflow DSL wraps the low-level client. It gives a readable state-machine
style similar in ergonomics to Temporal/DBOS decorators, while staying explicit.

## Define Workflow

```python
from ferricstore import Workflow, complete, fail, retry, state, transition


class PaymentWorkflow(Workflow):
    type = "payment"
    initial_state = "created"
    partition_by = ("tenant_id", "payment_id")

    @state("created", lease_ms=30_000, on_error="retry", return_record=False)
    def created(self, job):
        return transition("charged", payload=job.payload)

    @state("charged", lease_ms=30_000, on_error="fail")
    def charged(self, job):
        return complete(result=b"ok")
```

## Create

```python
workflow = PaymentWorkflow(client)

record = workflow.create(
    "payment-1",
    tenant_id="tenant-a",
    payment_id="payment-1",
    payload=b"raw payment request",
)
```

`partition_by` builds:

```text
tenant-a:payment-1
```

You can override:

```python
workflow.create("payment-1", partition_key="custom-key", payload=b"...")
```

## Run One State

```python
workflow.run_once("created", worker="worker-1", partition_key=record.partition_key)
```

This does:

```text
FLOW.CLAIM_DUE payment STATE created ...
handler(job)
FLOW.TRANSITION / FLOW.COMPLETE / FLOW.RETRY / FLOW.FAIL
```

`@state(..., return_record=False)` makes the SDK use ack-only mutators for that
state. Use it when the handler result is not read by local code; it avoids a
follow-up `FLOW.GET` after each mutation.

## Handler Outcomes

Transition:

```python
return transition("next_state", payload=b"new payload")
```

Complete:

```python
return complete(result=b"ok")
```

Retry:

```python
return retry(error=b"temporary", run_at_ms=next_attempt_ms)
```

Fail:

```python
return fail(error=b"permanent")
```

## Exceptions

If handler raises:

* `on_error="retry"` sends `FLOW.RETRY`.
* `on_error="fail"` sends `FLOW.FAIL`.

Default is retry.

## What This Is Not

This DSL does not replay Python code. It does not require deterministic Python
execution. Each handler is one durable state boundary. That is intentional.
