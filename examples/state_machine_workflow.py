from __future__ import annotations

from ferricstore import WorkflowClient, WorkflowWorker, complete, transition


def main() -> None:
    client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
    signup = client.workflow(
        type="signup",
        initial_state="created",
        partition_by=("tenant_id", "user_id"),
    )

    @signup.state("created", claim_payload=True)
    def created(job):
        print("create account", job.id)
        return transition("email_sent", payload=b"account-created")

    @signup.state("email_sent", claim_payload=False)
    def email_sent(job):
        print("finish signup", job.id)
        return complete(result=b"ok")

    signup.start(
        "signup-1",
        tenant_id="tenant-a",
        user_id="signup-1",
        payload=b"user payload",
    )

    worker = WorkflowWorker(signup, states=["created", "email_sent"])
    worker.run_once()
    worker.run_once()


if __name__ == "__main__":
    main()
