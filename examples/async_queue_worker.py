import asyncio

from ferricstore import AsyncQueueClient


async def send_email(job) -> bytes:
    print(f"send email: {job.id} payload={job.payload!r}")
    return b"sent"


async def main() -> None:
    client = AsyncQueueClient.from_url("ferric://127.0.0.1:6388")
    emails = client.queue(type="email")

    await emails.enqueue("email-1", payload=b"welcome:user-1", idempotent=True)
    await emails.worker(concurrency=10, batch_size=100).run(send_email)


if __name__ == "__main__":
    asyncio.run(main())
