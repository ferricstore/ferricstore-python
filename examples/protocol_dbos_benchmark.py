import argparse
import shlex
import subprocess
import sys
from pathlib import Path


EXAMPLES_DIR = Path(__file__).resolve().parent
DBOS_SCRIPT = EXAMPLES_DIR / "dbos_style_benchmark.py"
DEFAULT_URL = "ferric://127.0.0.1:6388"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the DBOS-style queued workflow benchmark over ferric:// protocol transport"
    )
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--flows", type=int, default=1_000_000)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--server-shards", type=int, default=16)
    parser.add_argument("--claim-batch-size", type=int, default=500)
    parser.add_argument("--claim-partition-batch-size", type=int, default=16)
    parser.add_argument("--claim-drain-batches", type=int, default=2)
    parser.add_argument("--create-batch-size", type=int, default=500)
    parser.add_argument("--complete-async-depth", type=int, default=4)
    parser.add_argument("--fuse-complete-claim", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--retention-ttl-ms", type=int, default=0)
    parser.add_argument("--protocol-worker-connections", type=int, default=1)
    parser.add_argument("--protocol-lanes", type=int, default=32)
    parser.add_argument("--protocol-create-inflight-batches", type=int, default=2)
    parser.add_argument("--producer-max-pending-credits", type=int, default=0)
    parser.add_argument("--producer-target-queue-latency-ms", type=float, default=75.0)
    parser.add_argument("--producer-min-rate-per-sec", type=float, default=50000.0)
    parser.add_argument("--producer-max-rate-per-sec", type=float, default=0.0)
    parser.add_argument("--transport", choices=("many", "pipeline", "autobatch"), default="many")
    parser.add_argument("--queued-shape", choices=("live", "preloaded"), default="live")
    parser.add_argument("--worker-api", choices=("queue", "lowlevel"), default="queue")
    parser.add_argument("--worker-mode", choices=("polling", "blocking"), default="polling")
    parser.add_argument("--partition-mode", choices=("auto", "explicit"), default="auto")
    parser.add_argument("--claim-job-only", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--reclaim-expired", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("extra", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    if args.extra and args.extra[0] == "--":
        args.extra = args.extra[1:]
    if args.retention_ttl_ms < 0:
        parser.error("--retention-ttl-ms must be non-negative")
    if args.producer_max_pending_credits < 0:
        parser.error("--producer-max-pending-credits must be non-negative")
    if args.producer_target_queue_latency_ms <= 0:
        parser.error("--producer-target-queue-latency-ms must be positive")
    if args.producer_min_rate_per_sec <= 0:
        parser.error("--producer-min-rate-per-sec must be positive")
    if args.producer_max_rate_per_sec < 0:
        parser.error("--producer-max-rate-per-sec must be non-negative")
    return args


def build_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(DBOS_SCRIPT),
        "--url",
        args.url,
        "--mode",
        "queued",
        "--queued-shape",
        args.queued_shape,
        "--transport",
        args.transport,
        "--worker-api",
        args.worker_api,
        "--worker-mode",
        args.worker_mode,
        "--partition-mode",
        args.partition_mode,
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
        "--claim-drain-batches",
        str(args.claim_drain_batches),
        "--create-batch-size",
        str(args.create_batch_size),
        "--complete-async-depth",
        str(args.complete_async_depth),
        "--fuse-complete-claim" if args.fuse_complete_claim else "--no-fuse-complete-claim",
        "--retention-ttl-ms",
        str(args.retention_ttl_ms),
        "--server-shards",
        str(args.server_shards),
        "--protocol-worker-connections",
        str(args.protocol_worker_connections),
        "--protocol-lanes",
        str(args.protocol_lanes),
        "--protocol-create-inflight-batches",
        str(args.protocol_create_inflight_batches),
        "--producer-max-pending-credits",
        str(args.producer_max_pending_credits),
        "--producer-target-queue-latency-ms",
        str(args.producer_target_queue_latency_ms),
        "--producer-min-rate-per-sec",
        str(args.producer_min_rate_per_sec),
        "--producer-max-rate-per-sec",
        str(args.producer_max_rate_per_sec),
    ]
    command.append("--claim-job-only" if args.claim_job_only else "--no-claim-job-only")
    command.append("--reclaim-expired" if args.reclaim_expired else "--no-reclaim-expired")
    command.extend(args.extra)
    return command


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    command = build_command(args)
    if args.dry_run:
        print(" ".join(shlex.quote(part) for part in command))
        return
    raise SystemExit(subprocess.run(command, check=False).returncode)


if __name__ == "__main__":
    main(sys.argv[1:])
