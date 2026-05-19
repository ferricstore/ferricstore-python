from __future__ import annotations

import asyncio

from ferricstore import AsyncFlowClient, AsyncQueueFlowWorker, CreateItem


async def main() -> None:
    client = AsyncFlowClient.from_url("redis://127.0.0.1:6379/0")

    await client.enqueue_many(
        [CreateItem(f"async-email-{idx}", f"user-{idx}".encode()) for idx in range(100)],
        type="email",
    )

    worker = AsyncQueueFlowWorker(
        client,
        type="email",
        state="queued",
        concurrency=100,
        batch_size=100,
    )

    async def handle(job):
        print("send async email", job.id)

    await worker.run_once(handle)
    await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
