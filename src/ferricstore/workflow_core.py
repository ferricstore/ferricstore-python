from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast


def workflow_partition_key(
    attrs: Mapping[str, Any],
    partition_by: Sequence[str],
) -> str | bytes | None:
    """Build a collision-free workflow partition key from routing attributes."""
    if not partition_by:
        return None

    components: list[bytes] = []
    binary = False
    for name in partition_by:
        value = attrs[name]
        if isinstance(value, bytes):
            component = value
            binary = True
        elif isinstance(value, (bytearray, memoryview)):
            component = bytes(value)
            binary = True
        else:
            component = str(value).encode("utf-8")
        components.append(component)

    encoded = b"fpk:" + b"".join(
        str(len(component)).encode("ascii") + b":" + component for component in components
    )
    return encoded if binary else encoded.decode("utf-8")


def pop_workflow_partition_key(
    attrs: dict[str, Any],
    partition_by: Sequence[str],
    *,
    resolver: Callable[[dict[str, Any]], str | bytes | None] | None = None,
) -> str | bytes | None:
    """Resolve routing and remove configuration-only partition attributes."""
    explicit = attrs.pop("partition_key", None)
    partition_key: str | bytes | None
    if explicit is not None:
        partition_key = cast(str | bytes, explicit)
    elif resolver is not None:
        partition_key = resolver(attrs)
    else:
        partition_key = workflow_partition_key(attrs, partition_by)
    for name in partition_by:
        attrs.pop(name, None)
    return partition_key
