from __future__ import annotations

from ferricstore import FlowClient, Workflow, WorkflowWorker, complete, state, transition


class SignupWorkflow(Workflow):
    type = "signup"
    initial_state = "created"
    partition_by = ("tenant_id", "user_id")

    @state("created", claim_payload=True, return_record=False)
    def created(self, job):
        print("create account", job.id)
        return transition("email_sent", payload=b"account-created")

    @state("email_sent", claim_payload=False, return_record=False)
    def email_sent(self, job):
        print("finish signup", job.id)
        return complete(result=b"ok")


def main() -> None:
    client = FlowClient.from_url("redis://127.0.0.1:6379/0")
    workflow = SignupWorkflow(client)

    workflow.enqueue(
        "signup-1",
        tenant_id="tenant-a",
        user_id="signup-1",
        payload=b"user payload",
    )

    worker = WorkflowWorker(workflow, states=["created", "email_sent"], batch_size=100)
    worker.run_once()
    worker.run_once()


if __name__ == "__main__":
    main()
