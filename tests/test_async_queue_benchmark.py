import asyncio
import importlib.util
from pathlib import Path

_BENCH_PATH = Path(__file__).resolve().parents[1] / "examples" / "async_queue_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("async_queue_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_auto_many_uses_requested_batch_size_without_private_prebucketing(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.batches = []

        async def enqueue_many(self, items, **_kwargs):
            self.batches.append(items)
            return [b"OK"] * len(items)

        async def close(self):
            pass

    client = FakeClient()
    monkeypatch.setattr(
        bench.AsyncFlowClient,
        "from_url",
        staticmethod(lambda _url, **_kwargs: client),
    )

    result = asyncio.run(
        bench.create_flows(
            url="ferric://example:6388",
            run_id="run",
            flow_type="email",
            indices=list(range(10)),
            partitions=16,
            partition_mode="auto",
            create_mode="many",
            create_batch_size=4,
            create_inflight=2,
            create_backpressure_credit=0,
            payload=b"payload",
            independent_many=True,
            wake_coordinator=None,
        )
    )

    assert result == {"created": 10}
    assert [len(batch) for batch in client.batches] == [4, 4, 2]
    assert all(item.partition_key is None for batch in client.batches for item in batch)
