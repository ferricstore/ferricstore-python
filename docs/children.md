# Children and Fanout

FerricFlow supports parent/child flows. Use this for fanout:

* parent waits for children
* children can run on same or different partitions
* parent advances when child group resolves

## ChildSpec

```python
from ferricstore import ChildSpec

children = [
    ChildSpec("email-1", "email", b"email payload"),
    ChildSpec("audit-1", "audit", b"audit payload"),
]
```

## Spawn Children

```python
client.spawn_children(
    parent.id,
    children,
    partition_key=parent.partition_key,
    lease_token=parent.lease_token,
    fencing_token=parent.fencing_token,
    group_id="notify-and-audit",
    wait="all",
    wait_state="waiting_children",
    success="children_done",
    failure="children_failed",
)
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

## Atomicity

Parent update and child creation are handled by FerricStore command semantics.
Across shards, resolution is idempotent and eventually reconciled. Handlers still
must be idempotent.

