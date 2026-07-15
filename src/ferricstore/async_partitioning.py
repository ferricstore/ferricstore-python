from __future__ import annotations

import zlib
from functools import lru_cache

from ferricstore.command_core import (
    FLOW_AUTO_PARTITION_BUCKETS,
    FLOW_AUTO_PARTITION_PREFIX,
    flow_auto_partition_index,
    flow_auto_partition_key_for_index,
)
from ferricstore.config_validation import validate_bounded_positive_int

AUTO_PARTITION_BUCKETS = FLOW_AUTO_PARTITION_BUCKETS
AUTO_PARTITION_PREFIX = FLOW_AUTO_PARTITION_PREFIX
SERVER_SLOT_COUNT = 1024


def _auto_partition_key(index: int) -> str:
    return flow_auto_partition_key_for_index(index)


def _auto_partition_index_for_id(id: str) -> int:
    return flow_auto_partition_index(id)


def _validate_server_shards(server_shards: object) -> int:
    return validate_bounded_positive_int(
        server_shards,
        name="server_shards",
        maximum=SERVER_SLOT_COUNT,
    )


def _server_shard_for_slot(slot: int, server_shards: int) -> int:
    server_shards = _validate_server_shards(server_shards)
    return _server_shard_for_slot_validated(slot, server_shards)


def _server_shard_for_slot_validated(slot: int, server_shards: int) -> int:
    slots_per_shard = SERVER_SLOT_COUNT // server_shards
    remainder = SERVER_SLOT_COUNT % server_shards
    wide_slots = (slots_per_shard + 1) * remainder
    slot = slot % SERVER_SLOT_COUNT
    if slot < wide_slots:
        return slot // (slots_per_shard + 1)
    return remainder + ((slot - wide_slots) // slots_per_shard)


def _auto_partition_server_shard(index: int, server_shards: int) -> int:
    server_shards = _validate_server_shards(server_shards)
    return _auto_partition_server_shard_validated(index, server_shards)


def _auto_partition_server_shard_validated(index: int, server_shards: int) -> int:
    tag = f"fa:{index % AUTO_PARTITION_BUCKETS}"
    slot = zlib.crc32(tag.encode()) & (SERVER_SLOT_COUNT - 1)
    return _server_shard_for_slot_validated(slot, server_shards)


def _validate_auto_partition_workers(workers: object) -> int:
    return validate_bounded_positive_int(
        workers,
        name="workers",
        maximum=AUTO_PARTITION_BUCKETS,
    )


@lru_cache(maxsize=128, typed=True)
def _auto_partition_assignments(
    workers: int,
    server_shards: int,
) -> tuple[tuple[str, ...], ...]:
    """Build a complete, non-empty ownership plan once per worker topology."""

    workers = _validate_auto_partition_workers(workers)
    server_shards = _validate_server_shards(server_shards)
    shard_buckets: dict[int, list[int]] = {}
    for index in range(AUTO_PARTITION_BUCKETS):
        shard = _auto_partition_server_shard_validated(index, server_shards)
        shard_buckets.setdefault(shard, []).append(index)

    groups = sorted(shard_buckets.items())
    assignments: list[list[int]] = [[] for _ in range(workers)]
    if workers <= len(groups):
        for position, (_shard, buckets) in enumerate(groups):
            assignments[position % workers].extend(buckets)
    else:
        workers_per_group = [1] * len(groups)
        remaining = workers - len(groups)
        while remaining:
            candidates = [
                position
                for position, (_shard, buckets) in enumerate(groups)
                if workers_per_group[position] < len(buckets)
            ]
            position = max(
                candidates,
                key=lambda candidate: (
                    len(groups[candidate][1]) / workers_per_group[candidate],
                    len(groups[candidate][1]),
                    -candidate,
                ),
            )
            workers_per_group[position] += 1
            remaining -= 1

        worker_offset = 0
        for position, (_shard, buckets) in enumerate(groups):
            group_workers = workers_per_group[position]
            for bucket_offset, bucket in enumerate(buckets):
                assignments[worker_offset + bucket_offset % group_workers].append(bucket)
            worker_offset += group_workers

    if any(not assignment for assignment in assignments):
        raise RuntimeError("auto-partition ownership produced an empty worker assignment")
    return tuple(
        tuple(_auto_partition_key(index) for index in assignment) for assignment in assignments
    )


@lru_cache(maxsize=128, typed=True)
def _auto_partition_owners(workers: int, server_shards: int) -> tuple[int, ...]:
    owners = [-1] * AUTO_PARTITION_BUCKETS
    for worker_index, keys in enumerate(_auto_partition_assignments(workers, server_shards)):
        for key in keys:
            index = int(key.removeprefix(AUTO_PARTITION_PREFIX))
            owners[index] = worker_index
    return tuple(owners)


def _auto_partition_owner(index: int, workers: int, server_shards: int) -> int:
    return _auto_partition_owners(workers, server_shards)[index % AUTO_PARTITION_BUCKETS]


def _owned_auto_partition_keys(
    *,
    worker_index: int,
    workers: int,
    server_shards: int,
) -> list[str]:
    assignments = _auto_partition_assignments(workers, server_shards)
    return list(assignments[worker_index % workers])


__all__ = [
    "AUTO_PARTITION_BUCKETS",
    "AUTO_PARTITION_PREFIX",
    "SERVER_SLOT_COUNT",
    "_auto_partition_assignments",
    "_auto_partition_index_for_id",
    "_auto_partition_key",
    "_auto_partition_owner",
    "_auto_partition_owners",
    "_auto_partition_server_shard",
    "_owned_auto_partition_keys",
    "_server_shard_for_slot",
    "_validate_auto_partition_workers",
    "_validate_server_shards",
]
