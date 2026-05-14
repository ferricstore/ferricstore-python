# Testing

## Unit Tests

Use fake adapters for command-building and workflow behavior:

```python
class FakeRedis:
    def __init__(self):
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        return {
            b"id": b"f1",
            b"type": b"order",
            b"state": b"created",
            b"partition_key": b"tenant:order",
        }
```

Run:

```bash
pytest
```

## Integration Tests

Use real FerricStore standalone for integration:

```python
client = FlowClient.from_url("redis://127.0.0.1:6379/0")
```

Test:

* create flow
* claim due
* transition
* claim next state
* complete
* get final record
* history count

## Performance Tests

Keep performance tests separate from unit tests. Use:

* fixed payload size
* fixed shard count
* clear warmup
* p50/p95/p99
* server-side and client-side timing

