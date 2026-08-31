from __future__ import annotations

from ferricstore import WorkflowClient, WorkflowWorker, complete, transition


def create_account(payload: bytes, *, idempotency_key: str) -> bytes:
    print(f"create account from {payload!r} with idempotency key {idempotency_key}")
    return b"account-created"


def main() -> None:
    client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
    signup = client.workflow(
        type="signup",
        initial_state="created",
        partition_by=("tenant_id", "user_id"),
    )

    @signup.state("created", claim_payload=True)
    def created(ctx):
        account = ctx.step(
            name="create-account:v1",
            run=lambda: create_account(
                ctx.payload,
                idempotency_key=f"{ctx.id}:create-account:v1",
            ),
            to_state="account_created",
        )
        return transition("email_pending", payload=account)

    @signup.state("email_pending", claim_payload=False)
    def email_pending(job):
        print("finish signup", job.id)
        return complete(result=b"ok")

    signup.start(
        "signup-1",
        tenant_id="tenant-a",
        user_id="signup-1",
        payload=b"user payload",
    )

    worker = WorkflowWorker(signup, states=["created", "email_pending"])
    worker.run_once()
    worker.run_once()


if __name__ == "__main__":
    main()
