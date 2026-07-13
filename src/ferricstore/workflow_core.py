from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast


def workflow_partition_key(
    attrs: Mapping[str, Any],
    partition_by: Sequence[str],
) -> str | None:
    """Build a workflow partition key from configured routing attributes."""
    if not partition_by:
        return None
    return ":".join(str(attrs[name]) for name in partition_by)


def pop_workflow_partition_key(
    attrs: dict[str, Any],
    partition_by: Sequence[str],
    *,
    resolver: Callable[[dict[str, Any]], str | None] | None = None,
) -> str | None:
    """Resolve routing and remove configuration-only partition attributes."""
    explicit = attrs.pop("partition_key", None)
    partition_key: str | None
    if explicit is not None:
        partition_key = cast(str, explicit)
    elif resolver is not None:
        partition_key = resolver(attrs)
    else:
        partition_key = workflow_partition_key(attrs, partition_by)
    for name in partition_by:
        attrs.pop(name, None)
    return partition_key
