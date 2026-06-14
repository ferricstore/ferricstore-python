import importlib.util
from concurrent.futures import Future
from pathlib import Path


_BENCH_PATH = Path(__file__).resolve().parents[1] / "examples" / "protocol_flow_commands_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("protocol_flow_commands_benchmark", _BENCH_PATH)
bench = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(bench)


def test_protocol_flow_commands_defaults_are_one_socket_native_shape():
    args = bench.parse_args([])

    assert args.url == "ferric://127.0.0.1:6388"
    assert args.operation == "create-many"
    assert args.flows == 100_000
    assert args.batch_size == 500
    assert args.inflight_batches == 64
    assert args.connections == 1
    assert args.protocol_lanes == 32
    assert args.flow_read_consistency == "eventual"
    assert args.flow_history_include_cold is False
    assert args.claim_mode == "multiplexed"


def test_protocol_flow_create_many_command_uses_partitioned_compact_shape():
    command = bench.create_many_command(
        run_id="r",
        flow_type="email",
        start=1000,
        count=2,
        batch_size=500,
        partitions=16,
        payload=b"p",
        retention_ttl_ms=123,
    )

    assert command[:16] == (
        "FLOW.CREATE_MANY",
        "__flow_auto__:2",
        "TYPE",
        "email",
        "STATE",
        "queued",
        "NOW",
        command[7],
        "RUN_AT",
        command[9],
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "RETENTION_TTL_MS",
        123,
    )
    assert command[16:] == ("ITEMS", "r:flow:1000", b"p", "r:flow:1001", b"p")


def test_protocol_flow_many_command_item_order_matches_wire_parser():
    jobs = [("f1", "p1", b"lease1", 7), ("f2", "p2", b"lease2", 8)]

    assert bench.complete_many_command(jobs)[8:] == (
        "ITEMS",
        "f1",
        "p1",
        b"lease1",
        7,
        "f2",
        "p2",
        b"lease2",
        8,
    )
    assert bench.transition_many_command(
        run_id="r", start=0, count=2, batch_size=500, partitions=16
    )[12:] == (
        "ITEMS",
        "r:flow:0",
        "__flow_auto__:0",
        0,
        None,
        "r:flow:1",
        "__flow_auto__:0",
        0,
        None,
    )
    assert bench.cancel_many_command(jobs)[8:] == (
        "ITEMS",
        "f1",
        "p1",
        7,
        "f2",
        "p2",
        8,
    )
    assert bench.cancel_many_created_command(
        run_id="r", start=0, count=2, batch_size=500, partitions=16
    )[8:] == (
        "ITEMS",
        "r:flow:0",
        "__flow_auto__:0",
        0,
        "r:flow:1",
        "__flow_auto__:0",
        0,
    )


def test_protocol_flow_signal_command_uses_configured_batch_partition():
    command = bench.signal_command("r", 750, 250, 16)

    assert command == (
        "FLOW.SIGNAL",
        "r:flow:750",
        "SIGNAL",
        "bench_signal",
        "PARTITION",
        "__flow_auto__:3",
        "IF_STATE",
        "queued",
        "TRANSITION_TO",
        "next",
        "NOW",
        command[-1],
    )


def test_protocol_flow_owned_value_put_command_uses_owner_and_partition():
    command = bench.owned_value_put_command(
        run_id="r",
        index=750,
        batch_size=250,
        partitions=16,
        value=b"value",
    )

    assert command == (
        "FLOW.VALUE.PUT",
        b"value",
        "OWNER_FLOW_ID",
        "r:flow:750",
        "NAME",
        "bench_value",
        "PARTITION",
        "__flow_auto__:3",
        "NOW",
        command[-3],
        "RETURN",
        "OK_ON_SUCCESS",
    )


def test_protocol_flow_read_query_commands_use_partitioned_shape():
    assert bench.flow_get_command("r", 750, 250, 16) == (
        "FLOW.GET",
        "r:flow:750",
        "PARTITION",
        "__flow_auto__:3",
    )
    assert bench.flow_history_command("r", 750, 250, 16) == (
        "FLOW.HISTORY",
        "r:flow:750",
        "COUNT",
        10,
        "PARTITION",
        "__flow_auto__:3",
        "INCLUDE_COLD",
        False,
    )
    assert bench.flow_list_command("email", 25) == (
        "FLOW.LIST",
        "email",
        "STATE",
        "queued",
        "COUNT",
        25,
    )


def test_protocol_flow_owned_value_put_uses_tuned_default_batch_size():
    args = bench.parse_args(["--operation", "value-put-owned"])

    assert args.operation == "value-put-owned"
    assert args.batch_size == 100


def test_protocol_flow_step_uses_balanced_default_batch_size():
    args = bench.parse_args(["--operation", "step"])

    assert args.operation == "step"
    assert args.batch_size == 100


def test_protocol_flow_start_and_claim_uses_balanced_default_batch_size():
    args = bench.parse_args(["--operation", "start-and-claim"])

    assert args.operation == "start-and-claim"
    assert args.batch_size == 250


def test_protocol_flow_step_continue_accepts_compact_job_tuple():
    command = bench.step_continue_command(("flow-1", "__flow_auto__:2", b"lease-1", 7))

    assert command == (
        "FLOW.STEP_CONTINUE",
        "flow-1",
        b"lease-1",
        "queued",
        "next",
        "FENCING",
        7,
        "LEASE_MS",
        30_000,
        "PARTITION",
        "__flow_auto__:2",
        "NOW",
        command[-1],
    )


def test_protocol_flow_shared_value_put_uses_tuned_default_batch_size():
    args = bench.parse_args(["--operation", "value-put"])

    assert args.operation == "value-put"
    assert args.batch_size == 100


def test_protocol_flow_shared_value_put_ok_uses_throughput_default_batch_size():
    args = bench.parse_args(["--operation", "value-put-ok"])

    assert args.operation == "value-put-ok"
    assert args.batch_size == 500


def test_protocol_flow_shared_value_put_payload_batch_uses_compact_shape():
    payload = bench.shared_value_put_payload_batch(value=b"abc", count=2, return_ok=True)

    assert payload is not None
    assert payload[:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 15,
        2,
    )
    assert payload.count(bench.compact_binary(b"abc")) == 2


def test_protocol_flow_owned_value_put_payload_batch_uses_compact_shape():
    payload = bench.owned_value_put_payload_batch(
        run_id="r",
        start=0,
        count=2,
        batch_size=2,
        partitions=16,
        value=b"payload",
    )

    assert payload is not None
    assert payload[:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 14,
        2,
    )
    assert payload.count(bench.compact_binary(b"payload")) == 2
    assert payload.count(bench.compact_binary(b"bench_value")) == 2
    assert bench.compact_binary(b"r:flow:0") in payload
    assert bench.compact_binary(b"r:flow:1") in payload


def test_protocol_flow_pipeline_duration_reads_cycle_over_setup_records():
    class FakeClock:
        def __init__(self):
            self.current = 0.0

        def __call__(self):
            self.current += 0.001
            return self.current

    class FakeAdapter:
        def __init__(self):
            self.batches = []

        def submit_batch(self, commands):
            self.batches.append(commands)
            future = Future()
            future.set_result([b"value"] * len(commands))
            return future

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_submit_pipeline_batches_for_duration(
        adapter,
        duration_seconds=0.004,
        batch_size=4,
        inflight_batches=64,
        item_count=3,
        build=lambda index: ("FLOW.GET", f"flow-{index}"),
        clock=FakeClock(),
    )

    assert completed == 4
    assert errors == 0
    assert len(latencies) == 1
    assert adapter.batches == [
        [("FLOW.GET", "flow-0"), ("FLOW.GET", "flow-1"), ("FLOW.GET", "flow-2"), ("FLOW.GET", "flow-0")]
    ]


def test_protocol_flow_pipeline_payload_duration_runner_uses_preencoded_payloads():
    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([b"value"] * count)
            return future

        def submit_batch(self, commands):
            raise AssertionError(f"generic submit_batch should not be used: {commands!r}")

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_submit_pipeline_payload_batches(
        adapter,
        total_items=5,
        batch_size=2,
        inflight_batches=2,
        build=lambda start, count: bench.flow_get_payload_batch(
            run_id="r",
            start=start,
            count=count,
            item_count=5,
            batch_size=2,
            partitions=16,
            return_meta=True,
        ),
    )

    assert completed == 5
    assert errors == 0
    assert len(latencies) == 3
    assert [count for _payload, count in adapter.payloads] == [2, 2, 1]


def test_protocol_flow_value_put_uses_preencoded_payload_when_available():
    class Args:
        flows = 5
        batch_size = 2
        inflight_batches = 2
        read_duration = 0.0
        prebuild_payloads = True

    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([b"OK"] * count)
            return future

        def submit_batch(self, commands):
            raise AssertionError(f"generic submit_batch should not be used: {commands!r}")

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_value_put(
        adapter,
        Args(),
        b"payload",
        bench.value_put_ok_command,
    )

    assert completed == 5
    assert errors == 0
    assert len(latencies) == 3
    assert [count for _payload, count in adapter.payloads] == [2, 2, 1]
    assert adapter.payloads[0][0][:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 15,
        2,
    )


def test_protocol_flow_start_and_claim_payload_batch_uses_compact_shape():
    payload = bench.start_and_claim_payload_batch(
        run_id="r",
        flow_type="email",
        start=0,
        count=2,
        batch_size=2,
        partitions=16,
        payload=b"payload",
        job_only=True,
    )

    assert payload is not None
    assert payload[:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 13,
        2,
    )
    assert bench.compact_binary(b"email") in payload
    assert bench.compact_binary(b"queued") in payload
    assert bench.compact_binary(b"protocol-flow-bench") in payload
    assert bench.compact_binary(b"r:flow:0") in payload
    assert bench.compact_binary(b"r:flow:1") in payload
    assert payload.count(bench.compact_binary(b"payload")) == 2


def test_protocol_flow_step_continue_payload_batch_uses_compact_shape():
    payload = bench.step_continue_payload_batch(
        [
            ("r:flow:0", "__flow_auto__:0", b"lease-0", 7),
            ("r:flow:1", "__flow_auto__:0", b"lease-1", 8),
        ],
        start=0,
        count=2,
    )

    assert payload is not None
    assert payload[:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 6,
        2,
    )
    assert bench.compact_binary(b"queued") in payload
    assert bench.compact_binary(b"next") in payload
    assert bench.compact_binary(b"r:flow:0") in payload
    assert bench.compact_binary(b"r:flow:1") in payload
    assert bench.compact_binary(b"lease-0") in payload
    assert bench.compact_binary(b"lease-1") in payload


def test_protocol_flow_signal_payload_batch_uses_compact_shape():
    payload = bench.signal_payload_batch(
        run_id="r",
        start=0,
        count=2,
        batch_size=2,
        partitions=16,
    )

    assert payload is not None
    assert payload[:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 11,
        2,
    )
    assert bench.compact_binary(b"bench_signal") in payload
    assert bench.compact_binary(b"queued") in payload
    assert bench.compact_binary(b"next") in payload
    assert bench.compact_binary(b"r:flow:0") in payload
    assert bench.compact_binary(b"r:flow:1") in payload


def test_protocol_flow_pipeline_payload_collect_runner_returns_batch_results():
    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([f"item-{index}" for index in range(count)])
            return future

    adapter = FakeAdapter()
    results, errors, latencies = bench.run_submit_pipeline_payload_batches_collect(
        adapter,
        total_items=5,
        batch_size=2,
        inflight_batches=2,
        build=lambda start, count: bench.start_and_claim_payload_batch(
            run_id="r",
            flow_type="email",
            start=start,
            count=count,
            batch_size=2,
            partitions=16,
            payload=b"payload",
            job_only=True,
        ),
    )

    assert errors == 0
    assert len(results) == 5
    assert len(latencies) == 3
    assert [count for _payload, count in adapter.payloads] == [2, 2, 1]


def test_protocol_flow_read_queries_use_throughput_tuned_default_batch_size():
    assert bench.parse_args(["--operation", "flow-get"]).batch_size == 250
    assert bench.parse_args(["--operation", "flow-get-meta"]).batch_size == 250
    assert bench.parse_args(["--operation", "flow-history"]).batch_size == 250
    assert bench.parse_args(["--operation", "flow-list"]).batch_size == 250


def test_protocol_flow_setup_batch_defaults_to_fast_partition_shape():
    args = bench.parse_args(["--operation", "flow-get"])

    assert args.batch_size == 250
    assert bench.effective_setup_batch_size(args) == 500
    assert bench.flow_get_command("r", 750, bench.effective_setup_batch_size(args), 16) == (
        "FLOW.GET",
        "r:flow:750",
        "PARTITION",
        "__flow_auto__:1",
    )


def test_protocol_flow_explicit_setup_batch_preserves_partition_shape():
    args = bench.parse_args(["--operation", "flow-get", "--setup-batch-size", "250"])

    assert bench.effective_setup_batch_size(args) == 250
    assert bench.flow_get_command("r", 750, bench.effective_setup_batch_size(args), 16) == (
        "FLOW.GET",
        "r:flow:750",
        "PARTITION",
        "__flow_auto__:3",
    )


def test_protocol_flow_transition_many_payload_batch_uses_compact_shape():
    payload = bench.transition_many_payload_batch(
        run_id="r",
        start=0,
        count=2,
        batch_size=2,
        partitions=16,
    )

    assert payload is not None
    assert payload[0] == bench._COMPACT_TRANSITION_MANY_OK_REQUEST
    assert bench.compact_binary(b"queued") in payload
    assert bench.compact_binary(b"next") in payload
    assert bench.compact_binary(b"r:flow:0") in payload
    assert bench.compact_binary(b"r:flow:1") in payload


def test_protocol_flow_get_meta_command_requests_meta_return():
    args = bench.parse_args(["--operation", "flow-get-meta", "--setup-batch-size", "250"])

    assert args.batch_size == 250
    assert bench.effective_setup_batch_size(args) == 250
    assert bench.flow_get_meta_command("r", 750, bench.effective_setup_batch_size(args), 16) == (
        "FLOW.GET",
        "r:flow:750",
        "PARTITION",
        "__flow_auto__:3",
        "RETURN",
        "META",
    )


def test_protocol_flow_get_payload_batch_uses_compact_partitioned_pipeline_shape():
    payload = bench.flow_get_payload_batch(
        run_id="r",
        start=750,
        count=2,
        item_count=1000,
        batch_size=250,
        partitions=16,
        return_meta=True,
    )

    assert payload is not None

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    assert payload == (
        bench._COMPACT_PIPELINE_HEADER.pack(
            bench._COMPACT_PIPELINE_REQUEST,
            0x80 | 17,
            2,
        )
        + binary(b"r:flow:750")
        + binary(b"__flow_auto__:3")
        + binary(b"r:flow:751")
        + binary(b"__flow_auto__:3")
    )


def test_protocol_flow_get_payload_batch_wraps_duration_reads():
    payload = bench.flow_get_payload_batch(
        run_id="r",
        start=999,
        count=2,
        item_count=1000,
        batch_size=250,
        partitions=16,
        return_meta=False,
    )

    assert payload is not None

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    assert payload == (
        bench._COMPACT_PIPELINE_HEADER.pack(
            bench._COMPACT_PIPELINE_REQUEST,
            0x80 | 16,
            2,
        )
        + binary(b"r:flow:999")
        + binary(b"__flow_auto__:3")
        + binary(b"r:flow:0")
        + binary(b"__flow_auto__:0")
    )


def test_protocol_flow_history_command_can_disable_per_read_consistency():
    command = bench.flow_history_command(
        "r",
        0,
        500,
        16,
        consistent_projection=False,
    )

    assert command[-2:] == ("CONSISTENT_PROJECTION", False)


def test_protocol_flow_history_command_can_include_cold_projection():
    command = bench.flow_history_command(
        "r",
        0,
        500,
        16,
        consistent_projection=False,
        include_cold=True,
    )

    assert "INCLUDE_COLD" not in command
    assert command[-2:] == ("CONSISTENT_PROJECTION", False)


def test_protocol_flow_history_payload_batch_uses_compact_pipeline_shape():
    payload = bench.flow_history_payload_batch(
        run_id="r",
        start=750,
        count=2,
        item_count=1000,
        batch_size=250,
        partitions=16,
        history_count=10,
        include_cold=False,
        consistent_projection=False,
    )

    assert payload is not None

    def binary(value: bytes) -> bytes:
        return bench._COMPACT_U32.pack(len(value)) + value

    assert payload == (
        bench._COMPACT_PIPELINE_HEADER.pack(
            bench._COMPACT_PIPELINE_REQUEST,
            0x80 | 10,
            2,
        )
        + bench._COMPACT_I64.pack(10)
        + bytes([bench._COMPACT_BOOL_FALSE, bench._COMPACT_BOOL_FALSE])
        + binary(b"r:flow:750")
        + binary(b"__flow_auto__:3")
        + binary(b"r:flow:751")
        + binary(b"__flow_auto__:3")
    )


def test_protocol_flow_history_consistency_mode_can_be_explicit():
    args = bench.parse_args(["--operation", "flow-history", "--flow-read-consistency", "consistent"])

    assert args.flow_read_consistency == "consistent"


def test_protocol_flow_history_include_cold_is_explicit():
    args = bench.parse_args(["--operation", "flow-history", "--flow-history-include-cold"])

    assert args.flow_history_include_cold is True


def test_protocol_flow_explicit_batch_size_overrides_operation_default():
    args = bench.parse_args(["--operation", "step", "--batch-size", "100"])

    assert args.batch_size == 100


def test_protocol_flow_claim_mode_can_select_serial_runner():
    args = bench.parse_args(["--operation", "claim-due", "--claim-mode", "serial"])

    assert args.claim_mode == "serial"


def test_protocol_flow_submit_batch_runner_counts_items_and_errors():
    class FakeAdapter:
        def __init__(self):
            self.commands = []

        def submit_command(self, *args):
            self.commands.append(args)
            future = Future()
            future.set_result(b"OK")
            return future

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_submit_command_batches(
        adapter,
        total_items=5,
        batch_size=2,
        inflight_batches=2,
        build=lambda start, count: ("MSET", start, count),
    )

    assert completed == 5
    assert errors == 0
    assert [command for command in adapter.commands] == [("MSET", 0, 2), ("MSET", 2, 2), ("MSET", 4, 1)]
    assert len(latencies) == 3


def test_protocol_flow_list_duration_reuses_list_command_until_deadline():
    class Args:
        operation = "flow-list-meta"
        flows = 5
        batch_size = 2
        inflight_batches = 2
        read_duration = 0.008

    class FakeClock:
        def __init__(self):
            self.current = 0.0

        def __call__(self):
            self.current += 0.001
            return self.current

    class FakeAdapter:
        def __init__(self):
            self.commands = []

        def submit_command(self, *args):
            self.commands.append(args)
            future = Future()
            future.set_result([b"ok"] * args[-3])
            return future

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_flow_list_reads(
        adapter, Args(), "email", clock=FakeClock()
    )

    assert completed == 4
    assert errors == 0
    assert len(latencies) == 2
    assert adapter.commands == [
        ("FLOW.LIST", "email", "STATE", "queued", "COUNT", 2, "RETURN", "META"),
        ("FLOW.LIST", "email", "STATE", "queued", "COUNT", 2, "RETURN", "META"),
    ]


def test_protocol_flow_create_many_uses_preencoded_payload_when_available():
    class Args:
        flows = 5
        batch_size = 2
        inflight_batches = 2
        partitions = 16
        retention_ttl_ms = 0

    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_flow_many_payload(self, command, payload, count):
            self.payloads.append((command, payload, count))
            future = Future()
            future.set_result([b"OK"] * count)
            return future

        def submit_command(self, *args):
            raise AssertionError(f"unexpected tuple command path: {args!r}")

    adapter = FakeAdapter()

    completed, errors, latencies = bench.run_create_many(
        adapter,
        Args(),
        run_id="run",
        flow_type="email",
        payload=b"payload",
    )

    assert completed == 5
    assert errors == 0
    assert len(latencies) == 3
    assert [item[0] for item in adapter.payloads] == ["FLOW.CREATE_MANY"] * 3
    assert [item[2] for item in adapter.payloads] == [2, 2, 1]
    assert adapter.payloads[0][1][0] == 0x96


def test_protocol_flow_create_many_can_disable_preencoded_payloads():
    class Args:
        flows = 3
        batch_size = 2
        inflight_batches = 2
        partitions = 16
        retention_ttl_ms = 0
        prebuild_payloads = False

    class FakeAdapter:
        def __init__(self):
            self.commands = []

        def submit_flow_many_payload(self, *_args):
            raise AssertionError("preencoded path should be disabled")

        def submit_command(self, *args):
            self.commands.append(args)
            future = Future()
            future.set_result(b"OK")
            return future

    adapter = FakeAdapter()

    completed, errors, _latencies = bench.run_create_many(
        adapter,
        Args(),
        run_id="run",
        flow_type="email",
        payload=b"payload",
    )

    assert completed == 3
    assert errors == 0
    assert [command[0] for command in adapter.commands] == ["FLOW.CREATE_MANY", "FLOW.CREATE_MANY"]


def test_protocol_flow_complete_many_uses_preencoded_payload_when_available():
    class Args:
        batch_size = 2
        inflight_batches = 2
        prebuild_payloads = True

    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_flow_many_payload(self, command, payload, count):
            self.payloads.append((command, payload, count))
            future = Future()
            future.set_result([b"OK"] * count)
            return future

        def submit_command(self, *args):
            raise AssertionError(f"unexpected tuple command path: {args!r}")

    jobs = [("f1", "p1", b"lease1", 7), ("f2", "p2", b"lease2", 8), ("f3", "p3", b"lease3", 9)]
    adapter = FakeAdapter()

    completed, errors, latencies = bench.run_claimed_many(
        adapter, Args(), jobs, operation="complete-many"
    )

    assert completed == 3
    assert errors == 0
    assert len(latencies) == 2
    assert [item[0] for item in adapter.payloads] == ["FLOW.COMPLETE_MANY", "FLOW.COMPLETE_MANY"]
    assert [item[2] for item in adapter.payloads] == [2, 1]
    assert adapter.payloads[0][1][0] == 0x93


def test_protocol_flow_cancel_many_keeps_tuple_path_after_direct_payload_regression():
    class Args:
        flows = 3
        batch_size = 2
        setup_batch_size = 2
        inflight_batches = 2
        partitions = 16
        prebuild_payloads = True

    class FakeAdapter:
        def __init__(self):
            self.commands = []

        def submit_flow_many_payload(self, command, payload, count):
            raise AssertionError("cancel-many direct payload path regressed benchmark throughput")

        def submit_command(self, *args):
            self.commands.append(args)
            future = Future()
            future.set_result(b"OK")
            return future

    adapter = FakeAdapter()

    completed, errors, latencies = bench.run_cancel_many_created(adapter, Args(), run_id="run")

    assert completed == 3
    assert errors == 0
    assert len(latencies) == 2
    assert [command[0] for command in adapter.commands] == ["FLOW.CANCEL_MANY", "FLOW.CANCEL_MANY"]


def test_protocol_flow_transition_many_payload_runs_through_flow_many_submitter():
    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_flow_many_payload(self, command, payload, count):
            self.payloads.append((command, payload, count))
            future = Future()
            future.set_result([b"OK"] * count)
            return future

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_submit_flow_payload_batches(
        adapter,
        total_items=3,
        batch_size=2,
        inflight_batches=2,
        command="FLOW.TRANSITION_MANY",
        build=lambda start, count: bench.transition_many_payload_batch(
            run_id="run",
            start=start,
            count=count,
            batch_size=2,
            partitions=16,
        ),
    )

    assert completed == 3
    assert errors == 0
    assert len(latencies) == 2
    assert [item[0] for item in adapter.payloads] == [
        "FLOW.TRANSITION_MANY",
        "FLOW.TRANSITION_MANY",
    ]
    assert adapter.payloads[0][1][0] == bench._COMPACT_TRANSITION_MANY_OK_REQUEST


def test_protocol_flow_value_mget_setup_uses_pipelined_value_puts():
    class Args:
        flows = 5
        batch_size = 2
        inflight_batches = 2
        read_duration = 0.0

    class FakeAdapter:
        def __init__(self):
            self.batches = []
            self.mget_commands = []

        def execute_command(self, *args):
            raise AssertionError(f"unexpected sync command: {args!r}")

        def submit_batch(self, commands):
            self.batches.append(commands)
            future = Future()
            future.set_result([f"ref:{len(self.batches)}:{index}" for index, _ in enumerate(commands)])
            return future

        def submit_command(self, *args):
            self.mget_commands.append(args)
            future = Future()
            future.set_result([b"value"] * (len(args) - 1))
            return future

    adapter = FakeAdapter()
    completed, errors, latencies, setup_seconds = bench.run_value_mget(adapter, Args(), b"value")

    assert completed == 5
    assert errors == 0
    assert setup_seconds >= 0
    assert len(latencies) == 3
    assert [len(batch) for batch in adapter.batches] == [2, 2, 1]
    assert [command[0] for command in adapter.mget_commands] == ["FLOW.VALUE.MGET"] * 3


def test_protocol_flow_value_put_duration_wraps_write_pool():
    class Args:
        flows = 3
        batch_size = 4
        inflight_batches = 64
        read_duration = 0.004

    class FakeClock:
        def __init__(self):
            self.current = 0.0

        def __call__(self):
            self.current += 0.001
            return self.current

    class FakeAdapter:
        def __init__(self):
            self.batches = []

        def submit_batch(self, commands):
            self.batches.append(commands)
            future = Future()
            future.set_result([b"OK"] * len(commands))
            return future

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_value_put(
        adapter,
        Args(),
        b"value",
        bench.value_put_ok_command,
        clock=FakeClock(),
    )

    assert completed == 4
    assert errors == 0
    assert len(latencies) == 1
    assert adapter.batches == [
        [
            ("FLOW.VALUE.PUT", b"value", "NOW", adapter.batches[0][0][3], "RETURN", "OK_ON_SUCCESS"),
            ("FLOW.VALUE.PUT", b"value", "NOW", adapter.batches[0][1][3], "RETURN", "OK_ON_SUCCESS"),
            ("FLOW.VALUE.PUT", b"value", "NOW", adapter.batches[0][2][3], "RETURN", "OK_ON_SUCCESS"),
            ("FLOW.VALUE.PUT", b"value", "NOW", adapter.batches[0][3][3], "RETURN", "OK_ON_SUCCESS"),
        ]
    ]


def test_protocol_flow_value_put_ok_uses_direct_payload_when_prebuild_enabled():
    class Args:
        flows = 5
        batch_size = 2
        inflight_batches = 2
        read_duration = 0.0
        prebuild_payloads = True

    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([b"OK"] * count)
            return future

        def submit_batch(self, commands):
            raise AssertionError(f"generic submit_batch should not be used: {commands!r}")

    adapter = FakeAdapter()

    completed, errors, latencies = bench.run_value_put(
        adapter,
        Args(),
        b"value",
        bench.value_put_ok_command,
    )

    assert completed == 5
    assert errors == 0
    assert len(latencies) == 3
    assert [count for _payload, count in adapter.payloads] == [2, 2, 1]
    assert adapter.payloads[0][0][:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 15,
        2,
    )


def test_protocol_flow_owned_value_put_uses_direct_payload_when_prebuild_enabled():
    class Args:
        flows = 3
        batch_size = 2
        setup_batch_size = 2
        inflight_batches = 2
        partitions = 16
        prebuild_payloads = True

    class FakeAdapter:
        def __init__(self):
            self.payloads = []

        def submit_pipeline_payload(self, payload, count):
            self.payloads.append((payload, count))
            future = Future()
            future.set_result([b"OK"] * count)
            return future

        def submit_batch(self, commands):
            raise AssertionError(f"generic submit_batch should not be used: {commands!r}")

    adapter = FakeAdapter()

    completed, errors, latencies = bench.run_owned_value_put(
        adapter,
        Args(),
        run_id="run",
        payload=b"value",
    )

    assert completed == 3
    assert errors == 0
    assert len(latencies) == 2
    assert [count for _payload, count in adapter.payloads] == [2, 1]
    assert adapter.payloads[0][0][:6] == bench._COMPACT_PIPELINE_HEADER.pack(
        bench._COMPACT_PIPELINE_REQUEST,
        0x80 | 14,
        2,
    )


def test_protocol_flow_value_mget_cyclic_refs_wraps_read_pool():
    assert bench.cyclic_refs(["r1", "r2", "r3"], 0, 2) == ["r1", "r2"]
    assert bench.cyclic_refs(["r1", "r2", "r3"], 2, 3) == ["r3", "r1", "r2"]
    assert bench.cyclic_refs(["r1", "r2", "r3"], 5, 2) == ["r3", "r1"]


def test_protocol_flow_value_mget_uses_preencoded_payload_when_available():
    class Args:
        flows = 5
        batch_size = 2
        inflight_batches = 2
        read_duration = 0.0
        prebuild_payloads = True

    class FakeAdapter:
        def __init__(self):
            self.setup_batches = []
            self.payloads = []

        def submit_batch(self, commands):
            self.setup_batches.append(commands)
            future = Future()
            start = sum(len(batch) for batch in self.setup_batches[:-1])
            future.set_result([f"ref-{start + index}" for index in range(len(commands))])
            return future

        def submit_command(self, *args):
            raise AssertionError(f"tuple FLOW.VALUE.MGET path should not be used: {args!r}")

        def submit_flow_value_mget_payload(self, payload):
            self.payloads.append(payload)
            future = Future()
            future.set_result([b"value"])
            return future

    adapter = FakeAdapter()

    completed, errors, latencies, setup_seconds = bench.run_value_mget(
        adapter,
        Args(),
        b"value",
    )

    assert completed == 5
    assert errors == 0
    assert setup_seconds >= 0.0
    assert len(latencies) == 3
    assert [len(batch) for batch in adapter.setup_batches] == [2, 2, 1]
    assert len(adapter.payloads) == 3
    assert all(payload[0] == 0x9D for payload in adapter.payloads)


def test_protocol_flow_claim_due_runner_uses_bounded_native_inflight():
    class Args:
        flows = 5
        batch_size = 2
        inflight_batches = 2
        partitions = 16

    class FakeAdapter:
        def __init__(self):
            self.commands = []

        def execute_command(self, *args):
            raise AssertionError(f"unexpected sync command: {args!r}")

        def submit_command(self, *args):
            self.commands.append(args)
            limit = int(args[args.index("LIMIT") + 1])
            future = Future()
            future.set_result(
                [
                    (f"flow-{len(self.commands)}-{index}", "__flow_auto__:0", b"lease", index)
                    for index in range(limit)
                ]
            )
            return future

    adapter = FakeAdapter()
    completed, errors, latencies = bench.run_claim_due(adapter, Args(), "email")

    assert completed == 5
    assert errors == 0
    assert len(latencies) == 3
    assert [command[command.index("LIMIT") + 1] for command in adapter.commands] == [2, 2, 1]


def test_protocol_flow_step_setup_requests_compact_jobs():
    class Args:
        flows = 3
        batch_size = 2
        setup_batch_size = 2
        inflight_batches = 2
        partitions = 16

    class FakeAdapter:
        def __init__(self):
            self.batches = []

        def submit_batch(self, commands):
            self.batches.append(commands)
            future = Future()
            if commands and commands[0][0] == "FLOW.START_AND_CLAIM":
                future.set_result(
                    [
                        (f"flow-{len(self.batches)}-{index}", "__flow_auto__:0", b"lease", index)
                        for index, _command in enumerate(commands)
                    ]
                )
            else:
                future.set_result([b"OK" for _command in commands])
            return future

    adapter = FakeAdapter()
    completed, errors, latencies, setup_seconds = bench.run_step(
        adapter, Args(), "run", "email", b"payload"
    )

    assert completed == 3
    assert errors == 0
    assert setup_seconds >= 0
    assert len(latencies) == 2
    assert all("JOBS_COMPACT" in command for command in adapter.batches[0])
