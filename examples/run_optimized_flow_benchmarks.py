#!/usr/bin/env python3
"""Run FerricFlow queue + workflow throughput benchmarks with optimized defaults.

This wrapper exists so benchmark runs are reproducible and apples-to-apples.
When --start-server is used, it starts FerricStore with production server
defaults and only sets isolation/logging env by default.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"
DEFAULT_SERVER_REPO = Path("/Users/yoavgea/repos/ferricstore")


def default_server_shards() -> int:
    return max(os.cpu_count() or 16, 1)


def run(name: str, argv: list[str]) -> None:
    print(f"\n=== {name} ===", flush=True)
    print(" ".join(argv), flush=True)
    subprocess.run(argv, cwd=ROOT, check=True)


def server_address(url: str) -> tuple[str, int]:
    parsed = urlparse(url)
    return parsed.hostname or "127.0.0.1", parsed.port or 6379


def wait_for_port(url: str, timeout_s: float = 60.0) -> None:
    host, port = server_address(url)
    deadline = time.monotonic() + timeout_s
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"server did not listen on {host}:{port}: {last_error}")


def port_is_open(url: str) -> bool:
    host, port = server_address(url)
    try:
        with socket.create_connection((host, port), timeout=0.2):
            return True
    except OSError:
        return False


def start_server(args: argparse.Namespace) -> tuple[subprocess.Popen, str, object]:
    server_repo = Path(args.server_repo)
    host, port = server_address(args.url)
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("--start-server only supports local benchmark URLs")
    if port_is_open(args.url):
        raise RuntimeError(
            f"--start-server refused to reuse already-listening server at {host}:{port}"
        )

    data_dir = tempfile.mkdtemp(prefix="ferricstore-flow-bench.")
    log_path = Path(args.server_log)
    log_file = log_path.open("ab")

    env = os.environ.copy()
    env.update(
        {
            "MIX_ENV": "prod",
            "FERRICSTORE_PORT": str(port),
            "FERRICSTORE_DATA_DIR": data_dir,
            "FERRICSTORE_LOG_LEVEL": args.server_log_level,
        }
    )
    if args.server_shard_count is not None:
        env["FERRICSTORE_SHARD_COUNT"] = str(args.server_shard_count)
    if args.server_max_memory is not None:
        env["FERRICSTORE_MAX_MEMORY"] = str(args.server_max_memory)
    if args.server_protected_mode is not None:
        env["FERRICSTORE_PROTECTED_MODE"] = "true" if args.server_protected_mode else "false"

    proc = subprocess.Popen(
        ["mix", "run", "--no-halt"],
        cwd=server_repo,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    try:
        wait_for_port(args.url, timeout_s=args.server_start_timeout_s)
    except Exception:
        stop_server(proc, log_file)
        raise
    return proc, data_dir, log_file


def stop_server(proc: subprocess.Popen, log_file: object) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    log_file.close()


def run_with_optional_server(name: str, argv: list[str], args: argparse.Namespace) -> None:
    if not args.start_server:
        run(name, argv)
        return

    proc, data_dir, log_file = start_server(args)
    try:
        print(f"server data_dir={data_dir}", flush=True)
        run(name, argv)
    finally:
        stop_server(proc, log_file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run optimized FerricFlow queue and workflow benchmarks."
    )
    parser.add_argument("--url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--which", choices=["all", "queue", "workflow"], default="all")
    parser.add_argument("--runtime", choices=["sync", "async", "both"], default="sync")
    parser.add_argument("--start-server", action="store_true")
    parser.add_argument("--fresh-server-per-benchmark", action="store_true")
    parser.add_argument("--server-repo", default=str(DEFAULT_SERVER_REPO))
    parser.add_argument("--server-log", default="/tmp/ferricstore-flow-bench-server.log")
    parser.add_argument("--server-log-level", default="warning")
    parser.add_argument("--server-max-memory", type=int, default=None)
    parser.add_argument("--server-shard-count", type=int, default=None)
    parser.add_argument(
        "--server-protected-mode",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument("--server-start-timeout-s", type=float, default=60.0)
    parser.add_argument("--queue-shape", choices=["live", "preloaded"], default="live")
    parser.add_argument("--workflow-shape", choices=["live", "preloaded"], default="live")
    parser.add_argument("--flows", type=int, default=1_000_000)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--producers", type=int, default=32)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--server-shards", type=int, default=default_server_shards())
    parser.add_argument("--claim-batch-size", type=int, default=500)
    parser.add_argument("--claim-partition-batch-size", type=int, default=2)
    parser.add_argument("--queue-create-batch-size", type=int, default=500)
    parser.add_argument("--workflow-create-batch-size", type=int, default=1000)
    parser.add_argument("--complete-async-depth", type=int, default=4)
    parser.add_argument("--workflow-apply-async-depth", type=int, default=4)
    parser.add_argument("--async-create-inflight", type=int, default=32)
    parser.add_argument("--async-create-backpressure-credit", type=int, default=0)
    parser.add_argument(
        "--async-producer-loop-thread", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--workflow-steps", type=int, default=1)
    return parser.parse_args()


def queue_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(EXAMPLES / "dbos_style_benchmark.py"),
        "--url",
        args.url,
        "--mode",
        "queued",
        "--queued-shape",
        args.queue_shape,
        "--transport",
        "many",
        "--worker-api",
        "lowlevel",
        "--worker-mode",
        "polling",
        "--partition-mode",
        "auto",
        "--flows",
        str(args.flows),
        "--workers",
        str(args.workers),
        "--producers",
        str(args.producers),
        "--partitions",
        str(args.partitions),
        "--claim-batch-size",
        str(args.claim_batch_size),
        "--claim-partition-batch-size",
        str(args.claim_partition_batch_size),
        "--create-batch-size",
        str(args.queue_create_batch_size),
        "--complete-async-depth",
        str(args.complete_async_depth),
        "--server-shards",
        str(args.server_shards),
        "--claim-job-only",
    ]


def workflow_command(args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(EXAMPLES / "state_machine_workflow_benchmark.py"),
        "--url",
        args.url,
        "--shape",
        args.workflow_shape,
        "--flows",
        str(args.flows),
        "--steps",
        str(args.workflow_steps),
        "--workers",
        str(args.workers),
        "--producers",
        str(args.producers),
        "--partitions",
        str(args.partitions),
        "--partition-mode",
        "auto",
        "--create-mode",
        "many",
        "--create-batch-size",
        str(args.workflow_create_batch_size),
        "--claim-batch-size",
        str(args.claim_batch_size),
        "--claim-partition-batch-size",
        str(args.claim_partition_batch_size),
        "--apply-async-depth",
        str(args.workflow_apply_async_depth),
        "--worker-mode",
        "blocking",
        "--server-shards",
        str(args.server_shards),
    ]


def async_queue_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(EXAMPLES / "async_queue_benchmark.py"),
        "--url",
        args.url,
        "--shape",
        args.queue_shape,
        "--create-mode",
        "many",
        "--worker-mode",
        "blocking",
        "--partition-mode",
        "auto",
        "--flows",
        str(args.flows),
        "--workers",
        str(args.workers),
        "--producers",
        str(args.producers),
        "--partitions",
        str(args.partitions),
        "--claim-batch-size",
        str(args.claim_batch_size),
        "--claim-partition-batch-size",
        str(args.claim_partition_batch_size),
        "--create-batch-size",
        str(args.queue_create_batch_size),
        "--create-inflight",
        str(args.async_create_inflight),
        "--create-backpressure-credit",
        str(args.async_create_backpressure_credit),
        "--complete-inflight",
        str(args.complete_async_depth),
        "--server-shards",
        str(args.server_shards),
    ]
    if args.async_producer_loop_thread:
        command.append("--producer-loop-thread")
    return command


def async_workflow_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(EXAMPLES / "async_state_machine_workflow_benchmark.py"),
        "--url",
        args.url,
        "--shape",
        args.workflow_shape,
        "--flows",
        str(args.flows),
        "--steps",
        str(args.workflow_steps),
        "--workers",
        str(args.workers),
        "--producers",
        str(args.producers),
        "--partitions",
        str(args.partitions),
        "--partition-mode",
        "auto",
        "--create-mode",
        "many",
        "--create-batch-size",
        str(args.workflow_create_batch_size),
        "--create-inflight",
        str(args.async_create_inflight),
        "--claim-batch-size",
        str(args.claim_batch_size),
        "--claim-partition-batch-size",
        str(args.claim_partition_batch_size),
        "--apply-inflight",
        str(args.workflow_apply_async_depth),
        "--worker-mode",
        "blocking",
        "--server-shards",
        str(args.server_shards),
    ]
    if args.async_producer_loop_thread:
        command.append("--producer-loop-thread")
    return command


def main() -> None:
    args = parse_args()
    selected = []
    if args.runtime in ("sync", "both") and args.which in ("all", "queue"):
        selected.append(("queue", queue_command(args)))
    if args.runtime in ("sync", "both") and args.which in ("all", "workflow"):
        selected.append(("workflow", workflow_command(args)))
    if args.runtime in ("async", "both") and args.which in ("all", "queue"):
        selected.append(("async-queue", async_queue_command(args)))
    if args.runtime in ("async", "both") and args.which in ("all", "workflow"):
        selected.append(("async-workflow", async_workflow_command(args)))

    if args.start_server and not args.fresh_server_per_benchmark:
        proc, data_dir, log_file = start_server(args)
        try:
            print(f"server data_dir={data_dir}", flush=True)
            for name, argv in selected:
                run(name, argv)
        finally:
            stop_server(proc, log_file)
        return

    for name, argv in selected:
        run_with_optional_server(name, argv, args)


if __name__ == "__main__":
    main()
