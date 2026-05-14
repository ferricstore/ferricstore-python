import argparse
import time
import uuid

from ferricstore import FlowClient, Workflow, complete, state, transition


class CounterWorkflow(Workflow):
    type = "dbos_python_sdk_bench"
    initial_state = "step_1"

    @state("step_1", lease_ms=30_000)
    def step_1(self, job):
        return transition("step_2")


def run(client: FlowClient, steps: int) -> float:
    flow_id = f"py-sdk-bench-{uuid.uuid4().hex}"
    partition = flow_id
    started = time.perf_counter()
    client.create(flow_id, type=CounterWorkflow.type, state="step_1", partition_key=partition)

    for step in range(1, steps + 1):
        jobs = client.claim_due(
            CounterWorkflow.type,
            state=f"step_{step}",
            worker="python-sdk-bench",
            partition_key=partition,
            limit=1,
        )
        job = jobs[0]
        client.executor.execute_command("INCR", "python-sdk-bench:counter")
        if step == steps:
            client.complete(
                job.id,
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=job.partition_key,
                result=b"ok",
            )
        else:
            client.transition(
                job.id,
                from_state=job.state,
                to_state=f"step_{step + 1}",
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=job.partition_key,
            )
    return (time.perf_counter() - started) * 1000.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--steps", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()

    client = FlowClient.from_url(args.url)
    runtimes = [run(client, args.steps) for _ in range(args.iterations)]
    print(
        {
            "steps": args.steps,
            "iterations": args.iterations,
            "avg_ms": sum(runtimes) / len(runtimes),
            "min_ms": min(runtimes),
            "max_ms": max(runtimes),
        }
    )


if __name__ == "__main__":
    main()
