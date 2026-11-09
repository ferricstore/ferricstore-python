# Children and Fanout

FerricFlow supports parent/child flows. Use this for fanout:

* the parent waits for its children without holding a lease or a worker
* children can run on the same or different partitions
* the parent moves to a success or failure state when the child group resolves

## Wait for children

Call `ctx.wait_for_children(...)` in a workflow handler and return nothing
after it. Creating the children and parking the parent in `wait_state` is one
fenced command:

```python
from ferricstore import ChildSpec, WorkflowClient, complete

client = WorkflowClient.from_url("ferric://127.0.0.1:6388")
campaign = client.workflow(type="campaign", initial_state="dispatch")


@campaign.state("dispatch")
def dispatch(ctx):
    ctx.wait_for_children(
        [
            ChildSpec(f"email:{ctx.id}", "email", b"email payload"),
            ChildSpec(f"audit:{ctx.id}", "audit", b"audit payload"),
        ],
        group_id="notify-and-audit",
        wait_state="waiting_children",
        success="children_done",
        failure="children_failed",
    )


@campaign.state("children_done")
def children_done(ctx):
    group = ctx.flow.child_results("notify-and-audit")
    return complete(result=b"%d children done" % group.summary.completed)
```

The async context has the same method: `await ctx.wait_for_children(...)` and
`await ctx.flow.child_results(...)`.

## Group policies

* `wait="all"` (the default) resolves the group when every child is terminal.
  `wait="any"` resolves it when the first child completes.
* `on_child_failed="fail_parent"` (the default) resolves the group to `failure`
  at the first failed or cancelled child. `"ignore"` waits for the group and
  lets the `success` handler decide what a failed child means.
* When a group resolves while children are still running, FerricStore cancels
  them, unless `on_parent_closed="abandon_children"` was set.
* If the spawn response is lost and the same group is sent again, the server
  treats it as already applied instead of creating a second set of children.

Fan-in needs FerricStore 0.11.19 or newer. Older servers never make a parent
claimable again after its group resolves.

## Child results

`ctx.flow.child_results(group_id)` returns a `ChildGroupResults`: the group's
`wait`, `resolved` (`"success"`, `"failure"`, or `None`), `summary` counts, and
each child as a `ChildResult` with its `id`, `partition_key`, `status`, and
decoded `result` or `error`, ordered by child id. It costs one `FLOW.GET` and one
`FLOW.VALUE.MGET`. `client.child_results(parent_id, partition_key=..., group_id=...)`
reads any parent's group and returns `None` when the parent or group is missing.

## ChildSpec

```python
from ferricstore import ChildSpec

children = [
    ChildSpec("email-1", "email", b"email payload"),
    ChildSpec("audit-1", "audit", b"audit payload"),
]
```

## Partitioning

Same partition:

```python
ChildSpec("child-1", "email", b"...")
```

Mixed partitions:

```python
ChildSpec("device-1-work", "device_work", b"...", partition_key="tenant-a:device-1")
ChildSpec("device-2-work", "device_work", b"...", partition_key="tenant-a:device-2")
```

Use mixed partitions for IoT/device fanout where each device can progress
independently.

## Low-level spawn

`client.spawn_children(parent_id, children, ...)` sends `FLOW.SPAWN_CHILDREN`
directly, for custom claim loops. It needs the parent's lease and fencing token,
and `wait_state`, `success`, and `failure` whenever `wait` is `"all"` or `"any"`.
A workflow handler should use `ctx.wait_for_children(...)` instead: returning a
transition after a direct spawn races with children that finish first.

## Atomicity

Parent update and child creation are handled by FerricStore command semantics.
Across shards, resolution is idempotent and eventually reconciled. Handlers still
must be idempotent.
