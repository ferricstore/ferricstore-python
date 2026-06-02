# Data in Workflows

FerricFlow keeps the hot state small and lets you attach larger data only when
handlers need it.

## Storage choices

| Data | Put it in |
| --- | --- |
| Small current-state routing data | `payload` |
| Per-flow data used by later states | `values={...}` |
| Large or reusable per-flow bytes | `value_put(...)` plus `value_refs={...}` |
| Final output | `complete(result=...)` |
| Search/debug metadata | flow id, type, state, correlation id, parent/root fields |

## Payload

Payload is the current state input. Keep it small.

```python
orders.enqueue(
    "order-1",
    payload={"tenant_id": "tenant-a", "order_id": "order-1"},
)
```

Payload is convenient, but it can be on hot paths. Do not put MB-scale documents
or model outputs in payload by default.

## Named values

Use named values when different states need different pieces of data.

```python
orders.enqueue(
    "order-1",
    payload={"tenant_id": "tenant-a", "order_id": "order-1"},
    values={
        "order": encode_order(order),
        "profile": encode_profile(profile),
    },
)
```

Claim only what the handler needs:

```python
@order.state("charge", claim_values=["order"])
def charge(job):
    order = decode_order(job.value("order"))
    return transition("ship")
```

## Value refs

Use `value_put` when data is large, reusable, or produced outside the current
transition.

```python
meta = client.value_put(
    pdf_bytes,
    owner_flow_id="order-1",
    name="invoice_pdf",
    partition_key="tenant-a:order-1",
)

order.start(
    "order-1",
    tenant_id="tenant-a",
    order_id="order-1",
    payload={"tenant_id": "tenant-a", "order_id": "order-1"},
    value_refs={"invoice_pdf": meta["ref"]},
)
```

Fetch later:

```python
@order.state("email_invoice", claim_values=["template"])
def email_invoice(job):
    template = job.value("template")
    invoice_pdf = job.value("invoice_pdf")
    send_invoice(template, invoice_pdf)
    return complete(result=b"sent")
```

## Idempotent value writes

Named values are useful for retry-safe state outputs. By default, do not
overwrite values on retry:

```python
return transition(
    "ship",
    values={"charge": charge_id.encode()},
)
```

Use override only when replacement is intentional:

```python
return transition(
    "draft_ready",
    values={"draft": new_draft_bytes},
    override_values=["draft"],
)
```

## Size caps

Always cap hydrated values in production:

```python
from ferricstore import ValueConfig, WorkflowClient

client = WorkflowClient.from_url(
    url,
    value_config=ValueConfig(value_max_bytes=64 * 1024),
)
```

For one exceptional read:

```python
record = client.get(
    "order-1",
    partition_key="tenant-a:order-1",
    values=["invoice_pdf"],
    value_max_bytes=512 * 1024,
)
```

## Local cache

`local_cache` is per-job, in-process memory. It is useful only when the same
handler reads the same named value more than once.

```python
invoice = job.value("invoice_pdf", local_cache=True)
```

Default is `False`. Keep it false for large values unless repeated reads are
known and memory is bounded.

## Recommended rules

| Rule | Reason |
| --- | --- |
| Keep payload small | Hot path stays predictable. |
| Use named values per logical artifact | States hydrate only what they need. |
| Use `value_refs` for large bytes | Avoid repeated storage and accidental hydration. |
| Set `value_max_bytes` | Prevent one flow from materializing huge data unexpectedly. |
| Keep `local_cache=False` by default | Avoid hidden RAM growth. |

