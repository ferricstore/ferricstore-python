from __future__ import annotations

from ferricstore import CreateItem, FlowClient, QueueFlowWorker


def main() -> None:
    client = FlowClient.from_url("redis://127.0.0.1:6379/0")

    client.enqueue_many(
        [CreateItem(f"email-{idx}", f"user-{idx}".encode()) for idx in range(100)],
        type="email",
    )

    worker = QueueFlowWorker(
        client,
        type="email",
        state="queued",
        concurrency=16,
        batch_size=100,
        idle_sleep_s=0.05,
    )

    def handle(job):
        print("send email", job.id)

    worker.run_once(handle)


if __name__ == "__main__":
    main()
