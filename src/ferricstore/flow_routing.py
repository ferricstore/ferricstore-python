from __future__ import annotations

import base64
import contextlib
import hashlib
import re
import zlib
from collections.abc import Sequence
from functools import lru_cache
from typing import Any

from ferricstore.command_grammar import split_flow_value_mget
from ferricstore.flow_options import FlowOptionPlan

FLOW_AUTO_PARTITION_PREFIX = "__flow_auto__:"
FLOW_AUTO_PARTITION_BUCKETS = 256

_AUTO_PARTITION_RE = re.compile(r"^__flow_auto__:(0|[1-9]\d{0,2})$")
_FLOW_PARTITION_ROUTE_CACHE_SIZE = 2_048
_FLOW_PARTITION_ROUTE_CACHE_MAX_KEY_BYTES = 4_096
_FLOW_ROUTING_TEXT_CHUNK_CHARS = 4_096
_FLOW_GLOBAL_ROUTE_KEY = "f:{f}:route"
_FLOW_AUTO_PARTITION_MAX_KEY_BYTES = len(
    f"{FLOW_AUTO_PARTITION_PREFIX}{FLOW_AUTO_PARTITION_BUCKETS - 1}".encode("ascii")
)
_FLOW_ROUTE_SELECTORS = frozenset({"AUTO", "ANY", "GLOBAL", "MIXED", "NONE"})
_FLOW_ROUTE_SELECTOR_BYTES = {
    selector.encode("ascii"): selector for selector in _FLOW_ROUTE_SELECTORS
}
_FLOW_ROUTE_SELECTOR_MAX_BYTES = max(map(len, _FLOW_ROUTE_SELECTOR_BYTES))

_FLOW_GLOBAL_COMMANDS = frozenset({"FLOW.POLICY.GET", "FLOW.POLICY.SET"})

_FLOW_GLOBAL_DEFAULT_PARTITION_COMMANDS = frozenset(
    {"FLOW.BY_CORRELATION", "FLOW.BY_PARENT", "FLOW.BY_ROOT"}
)

_FLOW_POSITIONAL_PARTITION_COMMANDS = frozenset(
    {
        "FLOW.CANCEL_MANY",
        "FLOW.COMPLETE_MANY",
        "FLOW.CREATE_MANY",
        "FLOW.FAIL_MANY",
        "FLOW.RETRY_MANY",
        "FLOW.TRANSITION_MANY",
    }
)

_FLOW_STATE_ID_COMMANDS = frozenset(
    {
        "FLOW.CANCEL",
        "FLOW.COMPLETE",
        "FLOW.CREATE",
        "FLOW.EFFECT.COMPENSATE",
        "FLOW.EFFECT.CONFIRM",
        "FLOW.EFFECT.FAIL",
        "FLOW.EFFECT.GET",
        "FLOW.EFFECT.RESERVE",
        "FLOW.EXTEND_LEASE",
        "FLOW.FAIL",
        "FLOW.GET",
        "FLOW.GOVERNANCE.LEDGER",
        "FLOW.HISTORY",
        "FLOW.RETRY",
        "FLOW.REWIND",
        "FLOW.SIGNAL",
        "FLOW.SPAWN_CHILDREN",
        "FLOW.START_AND_CLAIM",
        "FLOW.STEP_CONTINUE",
        "FLOW.TRANSITION",
    }
)

_FLOW_SCHEDULE_COMMANDS = frozenset(
    {
        "FLOW.SCHEDULE.CREATE",
        "FLOW.SCHEDULE.DELETE",
        "FLOW.SCHEDULE.FIRE",
        "FLOW.SCHEDULE.FIRE_DUE",
        "FLOW.SCHEDULE.GET",
        "FLOW.SCHEDULE.LIST",
        "FLOW.SCHEDULE.PAUSE",
        "FLOW.SCHEDULE.RESUME",
    }
)

_FLOW_APPROVAL_ID_COMMANDS = frozenset(
    {
        "FLOW.APPROVAL.APPROVE",
        "FLOW.APPROVAL.GET",
        "FLOW.APPROVAL.REJECT",
        "FLOW.APPROVAL.REQUEST",
    }
)

_FLOW_GOVERNANCE_SCOPE_COMMANDS = frozenset(
    {
        "FLOW.BUDGET.COMMIT",
        "FLOW.BUDGET.GET",
        "FLOW.BUDGET.RELEASE",
        "FLOW.BUDGET.RESERVE",
        "FLOW.CIRCUIT.CLOSE",
        "FLOW.CIRCUIT.GET",
        "FLOW.CIRCUIT.OPEN",
        "FLOW.LIMIT.GET",
        "FLOW.LIMIT.LEASE",
        "FLOW.LIMIT.RELEASE",
        "FLOW.LIMIT.SPEND",
    }
)

_FLOW_OPTION_STARTS = {
    "FLOW.APPROVAL.LIST": 0,
    "FLOW.ATTRIBUTE_VALUES": 2,
    "FLOW.ATTRIBUTES": 1,
    "FLOW.BUDGET.LIST": 0,
    "FLOW.BY_CORRELATION": 1,
    "FLOW.BY_PARENT": 1,
    "FLOW.BY_ROOT": 1,
    "FLOW.CANCEL": 1,
    "FLOW.CLAIM_DUE": 1,
    "FLOW.COMPLETE": 2,
    "FLOW.CREATE": 1,
    "FLOW.EFFECT.COMPENSATE": 1,
    "FLOW.EFFECT.CONFIRM": 1,
    "FLOW.EFFECT.FAIL": 1,
    "FLOW.EFFECT.GET": 1,
    "FLOW.EFFECT.RESERVE": 1,
    "FLOW.EXTEND_LEASE": 2,
    "FLOW.FAIL": 2,
    "FLOW.FAILURES": 1,
    "FLOW.GET": 1,
    "FLOW.GOVERNANCE.LEDGER": 1,
    "FLOW.GOVERNANCE.OVERVIEW": 0,
    "FLOW.HISTORY": 1,
    "FLOW.INFO": 1,
    "FLOW.LIMIT.LIST": 0,
    "FLOW.LIST": 1,
    "FLOW.RECLAIM": 1,
    "FLOW.RETRY": 2,
    "FLOW.REWIND": 1,
    "FLOW.RUN_STEPS_MANY": 0,
    "FLOW.SEARCH": 1,
    "FLOW.SIGNAL": 1,
    "FLOW.SPAWN_CHILDREN": 1,
    "FLOW.START_AND_CLAIM": 1,
    "FLOW.STATS": 1,
    "FLOW.STEP_CONTINUE": 4,
    "FLOW.STUCK": 1,
    "FLOW.TERMINALS": 1,
    "FLOW.TRANSITION": 3,
}

_FLOW_CLAIM_COMMANDS = frozenset({"FLOW.CLAIM_DUE", "FLOW.RECLAIM"})


def flow_auto_partition_key(id: str | bytes) -> str:
    return flow_auto_partition_key_for_index(flow_auto_partition_index(id))


def flow_auto_partition_index(id: str | bytes) -> int:
    checksum = _routing_crc32(id)
    if checksum is None:
        raise TypeError("id must be str or bytes")
    return checksum % FLOW_AUTO_PARTITION_BUCKETS


def flow_auto_partition_key_for_index(index: int) -> str:
    return f"{FLOW_AUTO_PARTITION_PREFIX}{index % FLOW_AUTO_PARTITION_BUCKETS}"


def flow_auto_id_routing_key(value: Any) -> str | None:
    checksum = _routing_crc32(value)
    if checksum is None:
        return None
    return f"f:{{fa:{checksum & 0xFF}}}:route"


def flow_logical_partition_routing_key(value: Any) -> str | None:
    if not isinstance(value, (str, bytes)):
        return None
    if isinstance(value, str) and len(value) > _FLOW_PARTITION_ROUTE_CACHE_MAX_KEY_BYTES:
        return _logical_partition_text_routing_key(value)
    encoded = value if isinstance(value, bytes) else value.encode()
    if len(encoded) > _FLOW_PARTITION_ROUTE_CACHE_MAX_KEY_BYTES:
        return _logical_partition_routing_key(encoded)
    return _cached_logical_partition_routing_key(encoded)


@lru_cache(maxsize=_FLOW_PARTITION_ROUTE_CACHE_SIZE)
def _cached_logical_partition_routing_key(value: str | bytes) -> str:
    encoded = value if isinstance(value, bytes) else value.encode()
    return _logical_partition_routing_key(encoded)


def _logical_partition_routing_key(encoded: bytes) -> str:
    text = ""
    if len(encoded) <= _FLOW_AUTO_PARTITION_MAX_KEY_BYTES:
        with contextlib.suppress(UnicodeDecodeError):
            text = encoded.decode("ascii")
    match = _AUTO_PARTITION_RE.fullmatch(text)
    if match is not None:
        bucket = int(match.group(1))
        if bucket < FLOW_AUTO_PARTITION_BUCKETS:
            return f"f:{{fa:{bucket}}}:route"

    digest = base64.urlsafe_b64encode(hashlib.sha256(encoded).digest()).rstrip(b"=").decode()
    return f"f:{{f:{digest}}}:route"


def _logical_partition_text_routing_key(value: str) -> str:
    hasher = hashlib.sha256()
    for start in range(0, len(value), _FLOW_ROUTING_TEXT_CHUNK_CHARS):
        hasher.update(value[start : start + _FLOW_ROUTING_TEXT_CHUNK_CHARS].encode())
    digest = base64.urlsafe_b64encode(hasher.digest()).rstrip(b"=").decode()
    return f"f:{{f:{digest}}}:route"


def flow_command_route_keys(name: str, args: Sequence[Any]) -> tuple[Any, ...]:
    """Return physical server routing keys for one normalized Flow command.

    An empty tuple deliberately selects the control path. Flow commands never
    fall through to generic payload-field guessing because their public IDs and
    logical partitions are not physical routing keys.
    """
    values = tuple(args)
    if not values:
        return ()
    if name in _FLOW_SCHEDULE_COMMANDS:
        return ()
    if name in _FLOW_GLOBAL_COMMANDS:
        return (_FLOW_GLOBAL_ROUTE_KEY,)
    if name == "FLOW.VALUE.MGET":
        return _value_mget_route_keys(values)
    if name in _FLOW_APPROVAL_ID_COMMANDS:
        return _single_logical_partition_key(values[0])
    if name in _FLOW_GOVERNANCE_SCOPE_COMMANDS:
        return _single_logical_partition_key(values[0])
    if name in _FLOW_POSITIONAL_PARTITION_COMMANDS:
        marker = _route_selector_token(values[0])
        if marker in {"AUTO", "MIXED", "NONE"}:
            return ()
        return _single_logical_partition_key(values[0])

    option_start = _FLOW_OPTION_STARTS.get(name)
    if option_start is not None:
        option_keys = _partition_option_route_keys(
            values,
            option_start,
            claim=name in _FLOW_CLAIM_COMMANDS,
        )
        if option_keys is not None:
            return option_keys
        if name in _FLOW_GLOBAL_DEFAULT_PARTITION_COMMANDS:
            return (_FLOW_GLOBAL_ROUTE_KEY,)

    if name == "FLOW.VALUE.PUT":
        partition_keys = _partition_option_route_keys(values, 1, claim=False)
        if partition_keys is not None:
            return partition_keys
        owner = _option_value(values, 1, "OWNER_FLOW_ID")
        value_name = _option_value(values, 1, "NAME")
        if isinstance(owner, (str, bytes)) and isinstance(value_name, (str, bytes)):
            key = flow_auto_id_routing_key(owner)
            return () if key is None else (key,)
        return (_FLOW_GLOBAL_ROUTE_KEY,)
    if name in _FLOW_STATE_ID_COMMANDS:
        key = flow_auto_id_routing_key(values[0])
        return () if key is None else (key,)
    return ()


def _partition_option_route_keys(
    args: tuple[Any, ...],
    start: int,
    *,
    claim: bool,
) -> tuple[Any, ...] | None:
    plan = FlowOptionPlan(args)
    index = start
    while index < len(args):
        token = plan.tokens[index]
        if token == "PARTITION":
            if index + 1 >= len(args):
                return ()
            key = (
                _claim_partition_routing_key(args[index + 1])
                if claim
                else (flow_logical_partition_routing_key(args[index + 1]))
            )
            return () if key is None else (key,)
        if token == "PARTITIONS":
            count = _non_negative_int(args[index + 1] if index + 1 < len(args) else None)
            if count is None or index + 2 + count > len(args):
                return ()
            partitions = args[index + 2 : index + 2 + count]
            mapper = _claim_partition_routing_key if claim else flow_logical_partition_routing_key
            keys = tuple(mapper(partition) for partition in partitions)
            return () if not keys or any(key is None for key in keys) else keys
        if token in {"ITEMS", "ITEMS_EXT"}:
            return None
        width = plan.option_width(index)
        if width is None or index + width > len(args):
            return ()
        index += width
    return None


def _claim_partition_routing_key(value: Any) -> str | None:
    selector = _route_selector_token(value)
    if selector in {"AUTO", "ANY"}:
        return None
    if selector == "GLOBAL":
        return _FLOW_GLOBAL_ROUTE_KEY
    return flow_logical_partition_routing_key(value)


def _single_logical_partition_key(value: Any) -> tuple[Any, ...]:
    key = flow_logical_partition_routing_key(value)
    return () if key is None else (key,)


def _value_mget_route_keys(args: tuple[Any, ...]) -> tuple[Any, ...]:
    refs, _max_bytes = split_flow_value_mget(args)
    if not refs or any(not isinstance(ref, (str, bytes)) for ref in refs):
        return ()
    return refs


def _option_value(args: tuple[Any, ...], start: int, wanted: str) -> Any | None:
    plan = FlowOptionPlan(args)
    index = start
    while index < len(args):
        token = plan.tokens[index]
        if token == wanted:
            return args[index + 1] if index + 1 < len(args) else None
        width = plan.option_width(index)
        if width is None or index + width > len(args):
            return None
        index += width
    return None


def _routing_crc32(value: Any) -> int | None:
    if isinstance(value, bytes):
        return zlib.crc32(value)
    if not isinstance(value, str):
        return None
    checksum = 0
    for start in range(0, len(value), _FLOW_ROUTING_TEXT_CHUNK_CHARS):
        checksum = zlib.crc32(
            value[start : start + _FLOW_ROUTING_TEXT_CHUNK_CHARS].encode(),
            checksum,
        )
    return checksum


def _route_selector_token(value: Any) -> str | None:
    if isinstance(value, bytes):
        if len(value) > _FLOW_ROUTE_SELECTOR_MAX_BYTES:
            return None
        return _FLOW_ROUTE_SELECTOR_BYTES.get(bytes(value).upper())
    if not isinstance(value, str) or len(value) > _FLOW_ROUTE_SELECTOR_MAX_BYTES:
        return None
    normalized = value.upper()
    return normalized if normalized in _FLOW_ROUTE_SELECTORS else None


def _non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


__all__ = [
    "FLOW_AUTO_PARTITION_BUCKETS",
    "FLOW_AUTO_PARTITION_PREFIX",
    "flow_auto_id_routing_key",
    "flow_auto_partition_index",
    "flow_auto_partition_key",
    "flow_auto_partition_key_for_index",
    "flow_command_route_keys",
    "flow_logical_partition_routing_key",
]
