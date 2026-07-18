from __future__ import annotations

import threading
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar

from ferricstore.command_core import command_route_keys
from ferricstore.errors import FerricStoreError, InvalidCommandError


class ControlEndpointSelector:
    """Keep one ordered, last-known-good control endpoint view for sync and async pools."""

    def __init__(self, seed_urls: Sequence[str]) -> None:
        self._seed_urls = tuple(dict.fromkeys(seed_urls))
        self._last_good_url: str | None = None

    def mark_success(self, url: str) -> None:
        self._last_good_url = url

    @property
    def preferred_url(self) -> str | None:
        return self._last_good_url

    def candidates(self, discovered_urls: Iterable[str] = ()) -> list[str]:
        urls: list[str] = []
        if self._last_good_url is not None:
            urls.append(self._last_good_url)
        urls.extend(self._seed_urls)
        urls.extend(discovered_urls)
        return list(dict.fromkeys(urls))


class RouteKind(Enum):
    """The complete routing decision for one protocol command."""

    CONTROL = "control"
    SINGLE_SHARD = "single_shard"
    CROSS_SHARD = "cross_shard"


@dataclass(frozen=True, slots=True)
class RouteDecision:
    kind: RouteKind
    key: str | bytes | None = None
    slots: tuple[int, ...] = ()

    def require_routable_key(self) -> str | bytes | None:
        if self.kind is RouteKind.CROSS_SHARD:
            raise InvalidCommandError(
                "multi-key commands require all keys to hash to the same slot"
            )
        return self.key


def route_for_keys(
    keys: Sequence[Any],
    *,
    slot_for_key: Callable[[str | bytes], int],
) -> RouteDecision:
    first_key: str | bytes | None = None
    first_slot = 0
    for key in keys:
        if not isinstance(key, (str, bytes)):
            continue
        slot = slot_for_key(key)
        if first_key is None:
            first_key = key
            first_slot = slot
        elif slot != first_slot:
            return RouteDecision(RouteKind.CROSS_SHARD, slots=(first_slot, slot))

    if first_key is None:
        return RouteDecision(RouteKind.CONTROL)
    return RouteDecision(RouteKind.SINGLE_SHARD, key=first_key, slots=(first_slot,))


def route_for_command(
    args: tuple[Any, ...],
    *,
    opcode: int,
    payload: Mapping[Any, Any] | bytes,
    control_opcodes: set[int],
    command_name: Callable[[Any], str],
    slot_for_key: Callable[[str | bytes], int],
) -> RouteDecision:
    """Classify a command without conflating control and cross-shard routes."""
    if not args:
        return RouteDecision(RouteKind.CONTROL)
    name = command_name(args[0])
    if opcode in control_opcodes or name in {"CLUSTER.KEYSLOT", "SHARDS", "ROUTE"}:
        return RouteDecision(RouteKind.CONTROL)
    command_args = args[1:]
    if name == "COMMAND_EXEC" and command_args:
        name = command_name(command_args[0])
        command_args = command_args[1:]
    route_keys = command_route_keys(name, command_args)
    if route_keys:
        decision = route_for_keys(route_keys, slot_for_key=slot_for_key)
        if name.startswith("FLOW.") and decision.kind is RouteKind.CROSS_SHARD:
            return RouteDecision(RouteKind.CONTROL)
        return decision
    if name.startswith("FLOW."):
        return RouteDecision(RouteKind.CONTROL)
    if not isinstance(payload, Mapping):
        return RouteDecision(RouteKind.CONTROL)
    for field in (
        "key",
        "partition_key",
        "id",
        "owner_flow_id",
        "parent_flow_id",
        "root_flow_id",
        "correlation_id",
        "scope",
    ):
        value = _map_get(payload, field)
        if isinstance(value, (str, bytes)):
            return route_for_keys((value,), slot_for_key=slot_for_key)
    keys = _map_get(payload, "keys")
    if isinstance(keys, list):
        return route_for_keys(keys, slot_for_key=slot_for_key)
    pairs = _map_get(payload, "pairs")
    if isinstance(pairs, list):
        return route_for_keys(
            [pair[0] for pair in pairs if isinstance(pair, list) and pair],
            slot_for_key=slot_for_key,
        )
    return RouteDecision(RouteKind.CONTROL)


@dataclass(frozen=True, slots=True)
class FlowWakeSubscription:
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class FlowWakeSubscriptionRegistry:
    """Own the server's single active flow-wake filter across connections."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._item: FlowWakeSubscription | None = None

    def remember(self, args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> bool:
        # Subscription filters contain caller-owned ``states`` and
        # ``partition_keys`` lists.  Snapshot collection values so reconnects
        # keep the filter that was actually installed on live connections.
        snapshot = {
            key: list(value) if isinstance(value, list) else value for key, value in kwargs.items()
        }
        item = FlowWakeSubscription(args, snapshot)
        with self._lock:
            changed = item != self._item
            self._item = item
            return changed

    def snapshot(self) -> tuple[FlowWakeSubscription, ...]:
        with self._lock:
            return () if self._item is None else (self._item,)

    def activate_sync(self, adapter: Any) -> list[Any]:
        """Activate subscriptions on an eagerly connected synchronous adapter."""
        replies: list[Any] = []
        subscribe = getattr(adapter, "subscribe_flow_wake", None)
        register = getattr(adapter, "register_flow_wake_subscription", None)
        for item in self.snapshot():
            if callable(subscribe):
                replies.append(subscribe(*item.args, **item.kwargs))
            elif callable(register):
                register(*item.args, **item.kwargs)
        return replies

    def register_for_reconnect(self, adapter: Any) -> None:
        """Record subscriptions on a lazy adapter for its next connection."""
        register = getattr(adapter, "register_flow_wake_subscription", None)
        if not callable(register):
            return
        for item in self.snapshot():
            register(*item.args, **item.kwargs)


TargetT = TypeVar("TargetT")
CommandT = TypeVar("CommandT")


@dataclass(slots=True)
class RouteBatchGroup(Generic[TargetT, CommandT]):
    target: TargetT
    indexes: list[int]
    commands: list[CommandT]


@dataclass(frozen=True, slots=True)
class RoutedBatchTarget(Generic[TargetT]):
    """A transport target plus the exact server lane selected by topology."""

    adapter: TargetT
    lane_id: int | None


@dataclass(frozen=True, slots=True)
class RouteBatchPlan(Generic[TargetT, CommandT]):
    groups: tuple[RouteBatchGroup[TargetT, CommandT], ...]
    command_count: int

    @classmethod
    def build(
        cls,
        routed_commands: Iterable[tuple[TargetT, CommandT]],
        *,
        group_key: Callable[[TargetT], Hashable] | None = None,
    ) -> RouteBatchPlan[TargetT, CommandT]:
        groups: dict[Hashable, RouteBatchGroup[TargetT, CommandT]] = {}
        count = 0
        for index, (target, command) in enumerate(routed_commands):
            key = id(target) if group_key is None else group_key(target)
            group = groups.setdefault(key, RouteBatchGroup(target, [], []))
            group.indexes.append(index)
            group.commands.append(command)
            count += 1
        return cls(tuple(groups.values()), count)

    def merge(self, group_values: Sequence[Sequence[Any]]) -> list[Any]:
        if len(group_values) != len(self.groups):
            raise FerricStoreError("topology batch returned invalid group cardinality")
        results: list[Any] = [None] * self.command_count
        for group, values in zip(self.groups, group_values, strict=True):
            if len(values) != len(group.indexes):
                raise FerricStoreError(
                    "topology batch returned invalid response cardinality",
                    raw=values,
                )
            for index, value in zip(group.indexes, values, strict=True):
                results[index] = value
        return results


def validate_single_slot(
    keys: Sequence[str | bytes],
    *,
    slot_for_key: Callable[[str | bytes], int],
) -> str | bytes | None:
    decision = route_for_keys(keys, slot_for_key=slot_for_key)
    return decision.require_routable_key()


def _map_get(value: Mapping[Any, Any], key: str) -> Any:
    if key in value:
        return value[key]
    return value.get(key.encode("utf-8"))
