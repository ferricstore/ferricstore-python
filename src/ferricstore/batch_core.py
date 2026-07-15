from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable, Hashable, Iterable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Executor, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from importlib import import_module
from typing import Any, TypeVar, cast

from ferricstore.command_core import flow_auto_partition_key
from ferricstore.config_validation import validate_positive_int
from ferricstore.errors import FerricStoreError
from ferricstore.types import CreateItem

_PIPELINE_STATUSES = {"ok", "busy", "error"}
_DEFAULT_FANOUT_LIMIT = 16

_Input = TypeVar("_Input")
_Result = TypeVar("_Result")


class SyncFanoutExecutor:
    """Lazy, bounded thread-pool owner for repeated synchronous fanout."""

    def __init__(
        self,
        *,
        max_concurrency: int = _DEFAULT_FANOUT_LIMIT,
        thread_name_prefix: str = "ferricstore-fanout",
    ) -> None:
        self.max_concurrency = validate_positive_int(
            max_concurrency,
            name="max_concurrency",
        )
        self.thread_name_prefix = thread_name_prefix
        self._condition = threading.Condition()
        self._executor: ThreadPoolExecutor | None = None
        self._active_calls = 0
        self._close_started = False
        self._close_running = False
        self._close_complete = False

    def run(
        self,
        items: Sequence[_Input],
        operation: Callable[[_Input], _Result],
        *,
        concurrent: bool,
        stop_on_error: bool = False,
    ) -> list[_Result]:
        with self._condition:
            if self._close_started:
                raise RuntimeError("sync fanout executor is closed")
            self._active_calls += 1
            executor: ThreadPoolExecutor | None = None
            if concurrent and len(items) >= 2:
                executor = self._executor
                if executor is None:
                    executor = ThreadPoolExecutor(
                        max_workers=self.max_concurrency,
                        thread_name_prefix=self.thread_name_prefix,
                    )
                    self._executor = executor
        try:
            if executor is None:
                return [operation(item) for item in items]
            return run_sync_fanout_on_executor(
                items,
                operation,
                executor=executor,
                max_concurrency=self.max_concurrency,
                stop_on_error=stop_on_error,
            )
        finally:
            with self._condition:
                self._active_calls -= 1
                self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            while self._close_running:
                self._condition.wait()
            if self._close_complete:
                return
            self._close_started = True
            self._close_running = True
            while self._active_calls:
                self._condition.wait()
            executor = self._executor
        try:
            if executor is not None:
                executor.shutdown(wait=True)
        except BaseException:
            with self._condition:
                self._close_running = False
                self._condition.notify_all()
            raise
        else:
            with self._condition:
                self._executor = None
                self._close_complete = True
                self._close_running = False
                self._condition.notify_all()


def ordered_batch_executor(executor: Any) -> Callable[..., Any] | None:
    """Prefer ordered batching without bypassing a legacy subclass override."""
    ordered = getattr(executor, "execute_batch_ordered", None)
    regular = getattr(executor, "execute_batch", None)
    if not callable(ordered):
        return cast(Callable[..., Any], regular) if callable(regular) else None
    if not callable(regular):
        return cast(Callable[..., Any], ordered)

    instance_values = getattr(executor, "__dict__", {})
    if "execute_batch" in instance_values and "execute_batch_ordered" not in instance_values:
        return cast(Callable[..., Any], regular)

    regular_owner_index: int | None = None
    ordered_owner_index: int | None = None
    for index, owner in enumerate(type(executor).__mro__):
        if regular_owner_index is None and "execute_batch" in owner.__dict__:
            regular_owner_index = index
        if ordered_owner_index is None and "execute_batch_ordered" in owner.__dict__:
            ordered_owner_index = index
    if (
        regular_owner_index is not None
        and ordered_owner_index is not None
        and regular_owner_index < ordered_owner_index
    ):
        return cast(Callable[..., Any], regular)
    return cast(Callable[..., Any], ordered)


@dataclass(frozen=True, slots=True)
class CreateManyGroup:
    """One shard-routable group in an ordered create-many plan."""

    partition_key: str
    indexed_items: tuple[tuple[int, CreateItem], ...]

    @property
    def items(self) -> list[CreateItem]:
        return [item for _index, item in self.indexed_items]


@dataclass(frozen=True, slots=True)
class CreateManyPlan:
    """Shared sync/async grouping, timestamp, and ordered response plan."""

    groups: tuple[CreateManyGroup, ...]
    item_count: int
    now_ms: int

    @classmethod
    def build(
        cls,
        items: Sequence[CreateItem],
        *,
        now_ms: int | None,
        clock: Callable[[], int],
    ) -> CreateManyPlan:
        grouped: dict[str, list[tuple[int, CreateItem]]] = {}
        for index, item in enumerate(items):
            group_key = (
                item.partition_key
                if item.partition_key is not None
                else flow_auto_partition_key(item.id)
            )
            grouped.setdefault(group_key, []).append((index, item))
        groups = tuple(
            CreateManyGroup(partition_key, tuple(indexed_items))
            for partition_key, indexed_items in grouped.items()
        )
        return cls(
            groups=groups,
            item_count=len(items),
            now_ms=now_ms if now_ms is not None else clock(),
        )

    def merge(
        self,
        responses: Sequence[Any],
        expand: Callable[[Any, int], list[Any]],
    ) -> list[Any]:
        if len(responses) != len(self.groups):
            raise FerricStoreError(
                f"create-many returned {len(responses)} groups; expected {len(self.groups)}",
                raw=responses,
            )
        results: list[Any] = [None] * self.item_count
        for group, response in zip(self.groups, responses, strict=True):
            expanded = expand(response, len(group.indexed_items))
            for (index, _item), item_result in zip(
                group.indexed_items,
                expanded,
                strict=True,
            ):
                results[index] = item_result
        return results


def batch_fingerprint(value: Any) -> Hashable:
    """Return a type-preserving structural key for immediate value comparison.

    Unknown application objects are keyed by identity instead of equality or repr.
    """
    return _batch_fingerprint(value, set())


def queued_batch_fingerprint(value: Any) -> Hashable:
    """Return a key that remains safe while caller-owned values wait in a queue."""
    return _queued_batch_fingerprint(value, set())


def batch_values_equal(left: Any, right: Any) -> bool:
    if left is right:
        return True
    return batch_fingerprint(left) == batch_fingerprint(right)


class BatchValueMatcher:
    """Compare many immutable batch values against one lazily fingerprinted reference."""

    __slots__ = ("_fingerprint", "reference")

    def __init__(self, reference: Any) -> None:
        self.reference = reference
        self._fingerprint: Hashable | None = None

    def matches(self, value: Any) -> bool:
        if value is self.reference:
            return True
        fingerprint = self._fingerprint
        if fingerprint is None:
            fingerprint = batch_fingerprint(self.reference)
            self._fingerprint = fingerprint
        return batch_fingerprint(value) == fingerprint


def require_batch_items(value: Any, expected: int, *, operation: str) -> list[Any]:
    if not isinstance(value, list):
        raise FerricStoreError(f"{operation} must return a list", raw=value)
    if len(value) != expected:
        raise FerricStoreError(
            f"{operation} returned {len(value)} items; expected {expected}",
            raw=value,
        )
    return value


def is_pipeline_status_batch(value: list[Any]) -> bool:
    """Recognize protocol status envelopes without guessing from result shape."""
    return all(_pipeline_status(item) in _PIPELINE_STATUSES for item in value)


def run_sync_fanout(
    items: Sequence[_Input],
    operation: Callable[[_Input], _Result],
    *,
    concurrent: bool,
    max_concurrency: int = _DEFAULT_FANOUT_LIMIT,
    stop_on_error: bool = False,
) -> list[_Result]:
    """Map independent operations in order, with explicit bounded concurrency."""
    max_concurrency = validate_positive_int(max_concurrency, name="max_concurrency")
    if not concurrent or len(items) < 2:
        return [operation(item) for item in items]
    worker_count = min(max_concurrency, len(items))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        return run_sync_fanout_on_executor(
            items,
            operation,
            executor=executor,
            max_concurrency=worker_count,
            stop_on_error=stop_on_error,
        )


def run_sync_fanout_on_executor(
    items: Sequence[_Input],
    operation: Callable[[_Input], _Result],
    *,
    executor: Executor,
    max_concurrency: int = _DEFAULT_FANOUT_LIMIT,
    stop_on_error: bool = False,
) -> list[_Result]:
    """Map through an existing executor without submitting the whole input at once."""
    max_concurrency = validate_positive_int(max_concurrency, name="max_concurrency")
    if not items:
        return []

    pending_limit = min(max_concurrency, len(items))
    results: list[_Result | None] = [None] * len(items)
    errors: dict[int, BaseException] = {}
    item_iterator = iter(enumerate(items))
    pending: dict[Future[_Result], int] = {}
    for _ in range(pending_limit):
        index, item = next(item_iterator)
        pending[executor.submit(operation, item)] = index

    while pending:
        completed, _not_done = wait(pending, return_when=FIRST_COMPLETED)
        for future in sorted(completed, key=pending.__getitem__):
            index = pending.pop(future)
            try:
                results[index] = future.result()
            except BaseException as exc:
                errors[index] = exc

        if errors and stop_on_error:
            continue

        while len(pending) < pending_limit:
            try:
                next_index, next_item = next(item_iterator)
            except StopIteration:
                break
            pending[executor.submit(operation, next_item)] = next_index

    if errors:
        raise errors[min(errors)]
    return cast(list[_Result], results)


async def run_async_fanout(
    items: Sequence[_Input],
    operation: Callable[[_Input], Awaitable[_Result]],
    *,
    concurrent: bool,
    max_concurrency: int = _DEFAULT_FANOUT_LIMIT,
    stop_on_error: bool = False,
) -> list[_Result]:
    """Async counterpart to :func:`run_sync_fanout`, preserving input order."""
    import asyncio

    max_concurrency = validate_positive_int(max_concurrency, name="max_concurrency")
    if not concurrent or len(items) < 2:
        return [await operation(item) for item in items]

    worker_count = min(max_concurrency, len(items))
    results: list[_Result | None] = [None] * len(items)
    errors: dict[int, BaseException] = {}
    next_index = 0
    stop_requested = False

    async def worker() -> None:
        nonlocal next_index, stop_requested
        while not stop_requested and next_index < len(items):
            index = next_index
            next_index += 1
            try:
                results[index] = await operation(items[index])
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                errors[index] = exc
                if stop_on_error:
                    stop_requested = True

    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    try:
        await asyncio.gather(*workers)
    except BaseException:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise

    if errors:
        raise errors[min(errors)]
    return cast(list[_Result], results)


def __getattr__(name: str) -> Any:
    if name == "asyncio":
        value = import_module("asyncio")
        globals()[name] = value
        return value
    raise AttributeError(name)


def _batch_fingerprint(value: Any, active: set[int]) -> Hashable:
    value_type = type(value)
    if value is None:
        return ("none",)
    if value_type is bool:
        return ("bool", value)
    if value_type is int:
        return ("int", value)
    if value_type is float:
        return ("float", value.hex())
    if value_type is str:
        return ("str", value)
    if value_type is bytes:
        return ("bytes", value)
    if value_type is bytearray:
        return ("bytearray", bytes(value))
    if isinstance(value, Enum):
        return (
            "enum",
            value_type.__module__,
            value_type.__qualname__,
            _batch_fingerprint(value.value, active),
        )

    identity = id(value)
    if identity in active:
        return ("recursive-identity", identity)

    if is_dataclass(value) and not isinstance(value, type):
        active.add(identity)
        try:
            return (
                "dataclass",
                value_type.__module__,
                value_type.__qualname__,
                tuple(
                    (field.name, _batch_fingerprint(getattr(value, field.name), active))
                    for field in fields(value)
                ),
            )
        finally:
            active.remove(identity)

    if value_type in {list, tuple}:
        active.add(identity)
        try:
            return (
                value_type.__name__,
                tuple(_batch_fingerprint(item, active) for item in value),
            )
        finally:
            active.remove(identity)

    if isinstance(value, Mapping):
        active.add(identity)
        try:
            return (
                "mapping",
                value_type.__module__,
                value_type.__qualname__,
                _fingerprint_multiset(
                    (
                        _batch_fingerprint(key, active),
                        _batch_fingerprint(item, active),
                    )
                    for key, item in value.items()
                ),
            )
        finally:
            active.remove(identity)

    if value_type in {set, frozenset}:
        active.add(identity)
        try:
            return (
                value_type.__name__,
                _fingerprint_multiset(_batch_fingerprint(item, active) for item in value),
            )
        finally:
            active.remove(identity)

    return ("identity", identity)


def _queued_batch_fingerprint(value: Any, active: set[int]) -> Hashable:
    value_type = type(value)
    if value is None:
        return ("none",)
    if value_type is bool:
        return ("bool", value)
    if value_type is int:
        return ("int", value)
    if value_type is float:
        return ("float", value.hex())
    if value_type is str:
        return ("str", value)
    if value_type is bytes:
        return ("bytes", value)

    identity = id(value)
    if value_type is bytearray:
        return ("mutable-identity", identity)
    if identity in active:
        return ("recursive-identity", identity)
    if isinstance(value, Enum):
        return (
            "enum",
            value_type.__module__,
            value_type.__qualname__,
            _queued_batch_fingerprint(value.value, active),
        )
    if is_dataclass(value) and not isinstance(value, type):
        parameters = getattr(value_type, "__dataclass_params__", None)
        if parameters is None or not parameters.frozen:
            return ("mutable-identity", identity)
        active.add(identity)
        try:
            return (
                "dataclass",
                value_type.__module__,
                value_type.__qualname__,
                tuple(
                    (
                        field.name,
                        _queued_batch_fingerprint(getattr(value, field.name), active),
                    )
                    for field in fields(value)
                ),
            )
        finally:
            active.remove(identity)
    if value_type in {list, set} or isinstance(value, Mapping):
        return ("mutable-identity", identity)
    if value_type is tuple:
        active.add(identity)
        try:
            return (
                "tuple",
                tuple(_queued_batch_fingerprint(item, active) for item in value),
            )
        finally:
            active.remove(identity)
    if value_type is frozenset:
        active.add(identity)
        try:
            return (
                "frozenset",
                _fingerprint_multiset(_queued_batch_fingerprint(item, active) for item in value),
            )
        finally:
            active.remove(identity)
    return ("identity", identity)


def _fingerprint_multiset(values: Iterable[Hashable]) -> frozenset[tuple[Hashable, int]]:
    """Keep collision multiplicity while remaining independent of iteration order."""
    counts: dict[Hashable, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return frozenset(counts.items())


def _pipeline_status(item: Any) -> str | None:
    status: Any = None
    if isinstance(item, (list, tuple)) and len(item) == 2:
        status = item[0]
    elif isinstance(item, Mapping):
        status = item.get("status", item.get(b"status"))
    if isinstance(status, str):
        return status.lower()
    return None
