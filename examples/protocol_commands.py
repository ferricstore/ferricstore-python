from __future__ import annotations

from ferricstore import FlowClient


def main() -> None:
    client = FlowClient.from_url("ferric://127.0.0.1:6388")

    if client.lock("lock:report:42", "worker-1", ttl_ms=30_000):
        try:
            result = client.fetch_or_compute("report:42", ttl_ms=60_000)
            if result.hit:
                print("cached report", result.value)
            else:
                report = b"expensive report"
                client.fetch_or_compute_result(
                    "report:42",
                    result.ownership_token,
                    report,
                    ttl_ms=60_000,
                )
        finally:
            client.unlock("lock:report:42", "worker-1")

    limit = client.ratelimit_add("rl:user:42", window_ms=1_000, max=10)
    print("allowed", limit.allowed, "remaining", limit.remaining)

    client.command("SET", "normal:data:key", "value")
    print(client.command("GET", "normal:data:key"))


if __name__ == "__main__":
    main()
