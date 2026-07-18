from __future__ import annotations

from ferricstore.config_validation import (
    validate_bounded_positive_int,
    validate_positive_int,
)

SERVER_SLOT_COUNT = 1024


def _validate_server_shards(server_shards: object) -> int:
    return validate_bounded_positive_int(
        server_shards,
        name="server_shards",
        maximum=SERVER_SLOT_COUNT,
    )


def _validate_auto_partition_workers(workers: object) -> int:
    return validate_positive_int(workers, name="workers")


__all__ = [
    "SERVER_SLOT_COUNT",
    "_validate_auto_partition_workers",
    "_validate_server_shards",
]
