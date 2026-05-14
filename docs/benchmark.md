# DBOS-Style Benchmark

`examples/dbos_style_benchmark.py` mirrors the DBOS workflow benchmark shape.

DBOS benchmark idea:

* one workflow execution per request/iteration
* workflow has `N` sequential durable steps
* each step performs one small durable mutation
* runtime is total workflow duration

FerricFlow mapping:

```text
FLOW.CREATE
for step in 1..N:
  FLOW.CLAIM_DUE
  INCR counter
  FLOW.TRANSITION
last step:
  FLOW.COMPLETE
```

For `N = 10`, one workflow execution sends:

* 1 `FLOW.CREATE`
* 10 `FLOW.CLAIM_DUE`
* 10 `INCR`
* 9 `FLOW.TRANSITION`
* 1 `FLOW.COMPLETE`

Total: 31 FerricStore commands.

The benchmark uses ack-only mutators (`return_record=False`) for create,
transition, and complete. That matches the command count above and avoids
measuring extra post-mutation `FLOW.GET` calls.

## Run

Start FerricStore standalone, then:

```bash
python examples/dbos_style_benchmark.py --steps 10 --iterations 100
```

Output:

```text
{
  "steps": 10,
  "iterations": 100,
  "avg_ms": ...,
  "min_ms": ...,
  "max_ms": ...
}
```

## Why `INCR`

DBOS benchmark step does a database transaction that updates a counter row.
FerricFlow equivalent uses `INCR` as a small durable state mutation. This avoids
measuring an empty workflow only.
