import asyncio
import importlib.util
from pathlib import Path

_BENCH_PATH = (
    Path(__file__).resolve().parents[1] / "examples" / "async_state_machine_workflow_benchmark.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "async_state_machine_workflow_benchmark",
    _BENCH_PATH,
)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_any_state_claim_mode_omits_explicit_state_list():
    states = ["queued", "retry"]

    assert bench.claim_states_for_mode("all", states) == states
    assert bench.claim_states_for_mode("any", states) is None


def test_pipeline_create_mode_uses_create_inflight(monkeypatch):
    class FakePipeline:
        def __init__(self, client):
            self.client = client
            self.commands = []

        def command(self, *args):
            self.commands.append(args)
            return self

        async def execute(self):
            self.client.inflight += 1
            self.client.max_inflight = max(self.client.max_inflight, self.client.inflight)
            await asyncio.sleep(0)
            self.client.inflight -= 1
            self.client.pipeline_depths.append(len(self.commands))
            return [b"OK"] * len(self.commands)

    class FakeClient:
        def __init__(self):
            self.inflight = 0
            self.max_inflight = 0
            self.pipeline_depths = []

        def pipeline(self):
            return FakePipeline(self)

        async def close(self):
            pass

    client = FakeClient()
    monkeypatch.setattr(
        bench.AsyncFlowClient,
        "from_url",
        staticmethod(lambda _url, **_kwargs: client),
    )

    result = asyncio.run(
        bench.create_workflows(
            url="ferric://example:6388",
            run_id="run",
            flow_type="email",
            indices=list(range(6)),
            partitions=16,
            partition_mode="auto",
            payload=b"payload",
            create_batch_size=1,
            create_inflight=3,
            create_rate_per_sec=0,
            create_mode="pipeline",
            independent_many=True,
            retention_ttl_ms=0,
            run_at_delay_ms=0,
            create_now_ms=None,
            wake_coordinator=None,
            progress=None,
        )
    )

    assert result == {"created": 6}
    assert client.max_inflight == 3
    assert client.pipeline_depths == [1, 1, 1, 1, 1, 1]


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
        bench.create_workflows(
            url="ferric://example:6388",
            run_id="run",
            flow_type="email",
            indices=list(range(10)),
            partitions=16,
            partition_mode="auto",
            payload=b"payload",
            create_batch_size=4,
            create_inflight=2,
            create_rate_per_sec=0,
            create_mode="many",
            independent_many=True,
            retention_ttl_ms=0,
            run_at_delay_ms=0,
            create_now_ms=1,
            wake_coordinator=None,
            progress=None,
        )
    )

    assert result == {"created": 10}
    assert [len(batch) for batch in client.batches] == [4, 4, 2]
    assert all(item.partition_key is None for batch in client.batches for item in batch)


def test_auto_worker_is_not_disabled_by_synthetic_shard_ownership(monkeypatch):
    clients = []

    class FakeClient:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    def new_client(*_args, **_kwargs):
        client = FakeClient()
        clients.append(client)
        return client

    monkeypatch.setattr(bench.AsyncFlowClient, "from_url", staticmethod(new_client))

    async def exercise():
        return await bench.run_workflow_worker(
            url="ferric://example:6388",
            run_id="run",
            flow_type="email",
            worker_index=31,
            workers=32,
            partitions=16,
            partition_mode="auto",
            states=["queued"],
            claim_batch_size=10,
            claim_partition_batch_size=4,
            apply_inflight=0,
            idle_sleep_ms=0,
            max_idle_sleep_ms=0,
            transition_payload=None,
            terminal_payload=None,
            terminal_mode="complete",
            result_payload=None,
            claim_states_mode="cursor",
            reclaim_expired=False,
            independent_many=True,
            counters=bench.AsyncCounters(0),
            producers_done=asyncio.Event(),
            wake_coordinator=None,
            wake_coalesce_ms=0,
            claim_block_ms=None,
            server_shards=16,
            claim_now_ms=None,
            progress=None,
        )

    result = asyncio.run(exercise())

    assert result["claimed_actions"] == 0
    assert len(clients) == 2
    assert all(client.closed for client in clients)


def test_polling_worker_mode_disables_claim_blocking():
    assert bench.effective_claim_block_ms("polling", 5000) is None
    assert bench.effective_claim_block_ms("blocking", -1) is None
    assert bench.effective_claim_block_ms("blocking", None) is None
    assert bench.effective_claim_block_ms("blocking", 5000) == 5000
