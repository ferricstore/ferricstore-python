import importlib.util
from pathlib import Path

_BENCH_PATH = (
    Path(__file__).resolve().parents[1] / "examples" / "state_machine_workflow_benchmark.py"
)
_SPEC = importlib.util.spec_from_file_location("state_machine_workflow_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_auto_many_uses_requested_batch_size_without_private_prebucketing(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.batches = []

        def enqueue_many(self, items, **_kwargs):
            self.batches.append(items)
            return [b"OK"] * len(items)

    client = FakeClient()
    monkeypatch.setattr(
        bench.FlowClient,
        "from_url",
        staticmethod(lambda _url, **_kwargs: client),
    )

    result = bench.create_workflows(
        url="ferric://example:6388",
        run_id="run",
        flow_type="email",
        indices=list(range(10)),
        partitions=16,
        partition_mode="auto",
        payload=b"payload",
        create_batch_size=4,
        create_mode="many",
        independent_many=True,
        wake_coordinator=None,
        server_shards=16,
    )

    assert result == {"created": 10}
    assert [len(batch) for batch in client.batches] == [4, 4, 2]
    assert all(item.partition_key is None for batch in client.batches for item in batch)
