# Patterns and Recipes

For complete worked examples, read [Use Case Examples](use-cases.md).

This page is the shorter pattern index.

| Pattern | Use |
| --- | --- |
| Durable queue | `QueueClient.queue(...)`. |
| Explicit state machine | `WorkflowClient.workflow(...)` / `AsyncWorkflowClient.workflow(...)` with named states. |
| Saga | States for each step plus explicit compensation states. |
| IoT fanout | Parent flow plus child device-command flows; device ACKs use `signal`. |
| AI orchestration | States for plan, tools, draft, review, final answer. |
| Human approval | Long wait state plus external `signal`. |
| Batch fanout | Parent flow splits work into children and waits for completion. |
| Webhook ingestion | Deterministic flow id plus `idempotent=True`. |
| Periodic jobs | Deterministic flow id per schedule window plus `run_at_ms`. |

## Web apps and serverless

Web handlers and serverless functions should enqueue/start work and return.
Long-running workers should run in a separate process that claims and completes
work. See [Web server and worker split](production.md#web-server-and-worker-split)
for a full FastAPI plus worker example.

## Hot-path defaults

Use these defaults unless the handler explicitly needs more data:

```python
from ferricstore import QueueClient

client = QueueClient.from_url("ferric://127.0.0.1:6388")
emails = client.queue(type="email")

emails.enqueue("job-1", payload=b"...")
emails.worker(
    batch_size=1000,
    lease_ms=60_000,
    complete_async_depth=4,
).run(send_email)
```

## Data placement

| Data | Best place |
| --- | --- |
| routing/current-state bytes | `payload` |
| per-flow large bytes | named values |
| reusable large bytes | `value_put` and `value_refs` |
| external API result | named value for future states, terminal `result` for final output |
| audit trail | Flow history plus named values |

## Idempotency

Use stable keys:

```text
<flow_id>:<state>
<flow_id>:<state>:<external-operation>
```

Use `fencing_token` for stale local writes and external idempotency keys for
side effects.

## Production checklist

See [Production Readiness](production.md).
