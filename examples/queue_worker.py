from ferricstore import CreateItem, QueueClient


def main() -> None:
    client = QueueClient.from_url("redis://127.0.0.1:6379/0")
    emails = client.queue(type="email")

    emails.enqueue_many(
        [CreateItem(f"email-{idx}", f"payload-{idx}".encode()) for idx in range(10)]
    )

    def handler(job):
        print("send", job.id)

    emails.worker(concurrency=4, batch_size=10).run_once(handler)


if __name__ == "__main__":
    main()
