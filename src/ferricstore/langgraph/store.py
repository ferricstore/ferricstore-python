from __future__ import annotations

import asyncio
import base64
import builtins
import hashlib
import inspect
import json
from collections.abc import Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol, TypeVar, cast

from langgraph.store.base import (
    BaseStore,
    GetOp,
    InvalidNamespaceError,
    Item,
    ListNamespacesOp,
    MatchCondition,
    Op,
    PutOp,
    Result,
    SearchItem,
    SearchOp,
)

from ferricstore.langgraph._locks import run_async_with_locks, run_sync_with_locks


class _SyncCommandClient(Protocol):
    def command(self, *args: Any) -> Any: ...


class _AsyncCommandClient(Protocol):
    async def command(self, *args: Any) -> Any: ...


_FORMAT_VERSION = 1
_ITEM_PREFIX = "item:"
_Command = tuple[Any, ...]
_ResultT = TypeVar("_ResultT")


@dataclass(frozen=True, slots=True)
class _CommandBatch:
    commands: tuple[_Command, ...]


_Request = _Command | _CommandBatch
_CommandPlan = Generator[_Request, Any, _ResultT]


@dataclass(frozen=True, slots=True)
class _PendingPut:
    op: PutOp
    timestamp: datetime


def _raw_bytes(value: Any, *, name: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"FerricStore returned a non-binary {name}")


def _batch_items(value: Any, expected: int) -> builtins.list[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("FerricStore command batch returned an invalid response")
    if len(value) != expected:
        raise ValueError(
            f"FerricStore command batch returned {len(value)} responses for "
            f"{expected} commands"
        )
    return list(value)


def _execute_sync_request(client: _SyncCommandClient, request: _Request) -> Any:
    if not isinstance(request, _CommandBatch):
        return client.command(*request)
    pipeline_factory = getattr(client, "pipeline", None)
    if callable(pipeline_factory):
        pipeline = pipeline_factory()
        for command in request.commands:
            pipeline.command(*command)
        return _batch_items(pipeline.execute(), len(request.commands))
    return [client.command(*command) for command in request.commands]


async def _execute_async_request(client: _AsyncCommandClient, request: _Request) -> Any:
    if not isinstance(request, _CommandBatch):
        return await client.command(*request)
    pipeline_factory = getattr(client, "pipeline", None)
    if callable(pipeline_factory):
        pipeline = pipeline_factory()
        for command in request.commands:
            pipeline.command(*command)
        response = pipeline.execute()
        if inspect.isawaitable(response):
            response = await response
        return _batch_items(response, len(request.commands))
    return [await client.command(*command) for command in request.commands]


def _run_sync(client: _SyncCommandClient, plan: _CommandPlan[_ResultT]) -> _ResultT:
    try:
        request = next(plan)
    except StopIteration as stopped:
        return cast(_ResultT, stopped.value)
    while True:
        response = _execute_sync_request(client, request)
        try:
            request = plan.send(response)
        except StopIteration as stopped:
            return cast(_ResultT, stopped.value)


async def _run_async(
    client: _AsyncCommandClient,
    plan: _CommandPlan[_ResultT],
) -> _ResultT:
    try:
        request = next(plan)
    except StopIteration as stopped:
        return cast(_ResultT, stopped.value)
    while True:
        response = await _execute_async_request(client, request)
        try:
            request = plan.send(response)
        except StopIteration as stopped:
            return cast(_ResultT, stopped.value)


def _validate_namespace(namespace: tuple[str, ...]) -> None:
    if not namespace:
        raise InvalidNamespaceError("Namespace cannot be empty.")
    for label in namespace:
        if not isinstance(label, str):
            raise InvalidNamespaceError("Namespace labels must be strings.")
        if not label:
            raise InvalidNamespaceError("Namespace labels cannot be empty strings.")
        if "." in label:
            raise InvalidNamespaceError("Namespace labels cannot contain periods ('.').")
    if namespace[0] == "langgraph":
        raise InvalidNamespaceError('Root namespace label cannot be "langgraph".')


def _validate_json_keys(value: Any) -> None:
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("LangGraph store JSON object keys must be strings")
        for nested in value.values():
            _validate_json_keys(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _validate_json_keys(nested)


def _namespace_identity(namespace: tuple[str, ...]) -> bytes:
    identity = bytearray()
    for component in namespace:
        payload = component.encode("utf-8")
        identity.extend(len(payload).to_bytes(8, "big"))
        identity.extend(payload)
    return bytes(identity)


def _ordered_text(value: str) -> bytes:
    """Encode text so byte ordering matches Python string ordering."""
    encoded = bytearray()
    for value_byte in value.encode("utf-8"):
        if value_byte == 0:
            encoded.extend((0, 255))
        else:
            encoded.append(value_byte)
    encoded.extend((0, 0))
    return bytes(encoded)


def _decode_ordered_text(value: bytes, offset: int) -> tuple[str, int]:
    decoded = bytearray()
    while offset < len(value):
        value_byte = value[offset]
        offset += 1
        if value_byte != 0:
            decoded.append(value_byte)
            continue
        if offset >= len(value):
            raise ValueError("truncated FerricStore LangGraph catalog member")
        escaped = value[offset]
        offset += 1
        if escaped == 0:
            try:
                return decoded.decode("utf-8"), offset
            except UnicodeDecodeError as exc:
                raise ValueError(
                    "invalid UTF-8 in FerricStore LangGraph catalog member"
                ) from exc
        if escaped == 255:
            decoded.append(0)
            continue
        raise ValueError("invalid FerricStore LangGraph catalog member escape")
    raise ValueError("unterminated FerricStore LangGraph catalog member")


def _catalog_member(namespace: tuple[str, ...], key: str) -> bytes:
    encoded = bytearray()
    for component in namespace:
        encoded.extend(_ordered_text(component))
    encoded.extend((0, 0))
    encoded.extend(_ordered_text(key))
    return bytes(encoded)


def _decode_catalog_member(value: Any) -> tuple[tuple[str, ...], str]:
    encoded = _raw_bytes(value, name="store catalog member")
    namespace: builtins.list[str] = []
    offset = 0
    while True:
        if encoded[offset : offset + 2] == b"\x00\x00":
            offset += 2
            break
        component, offset = _decode_ordered_text(encoded, offset)
        namespace.append(component)
    key, offset = _decode_ordered_text(encoded, offset)
    if offset != len(encoded) or not namespace:
        raise ValueError("invalid FerricStore LangGraph catalog member")
    return tuple(namespace), key


def _encode_component(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).rstrip(b"=").decode("ascii")


def _compare_value(item_value: Any, filter_value: Any) -> bool:
    if isinstance(filter_value, dict):
        if any(str(key).startswith("$") for key in filter_value):
            return all(
                _apply_operator(item_value, str(operator), expected)
                for operator, expected in filter_value.items()
            )
        if not isinstance(item_value, dict):
            return False
        return all(
            _compare_value(item_value.get(key), expected)
            for key, expected in filter_value.items()
        )
    if isinstance(filter_value, (list, tuple)):
        return (
            isinstance(item_value, (list, tuple))
            and len(item_value) == len(filter_value)
            and all(
                _compare_value(actual, expected)
                for actual, expected in zip(item_value, filter_value, strict=False)
            )
        )
    return bool(item_value == filter_value)


def _apply_operator(value: Any, operator: str, expected: Any) -> bool:
    if operator == "$eq":
        return bool(value == expected)
    if operator == "$ne":
        return bool(value != expected)
    if operator == "$gt":
        return float(value) > float(expected)
    if operator == "$gte":
        return float(value) >= float(expected)
    if operator == "$lt":
        return float(value) < float(expected)
    if operator == "$lte":
        return float(value) <= float(expected)
    raise ValueError(f"Unsupported filter operator: {operator}")


def _matches_filter(value: dict[str, Any], filter: dict[str, Any] | None) -> bool:
    if not filter:
        return True
    return all(
        _compare_value(value.get(key), expected) for key, expected in filter.items()
    )


def _matches_namespace(condition: MatchCondition, namespace: tuple[str, ...]) -> bool:
    path = condition.path
    if len(namespace) < len(path):
        return False
    if condition.match_type == "prefix":
        values = zip(namespace, path, strict=False)
    elif condition.match_type == "suffix":
        values = zip(reversed(namespace), reversed(path), strict=False)
    else:
        raise ValueError(f"Unsupported namespace match type: {condition.match_type}")
    return all(pattern == "*" or actual == pattern for actual, pattern in values)


class _FerricStoreMemoryStorage:
    """Transport-independent LangGraph long-term-memory command plans."""

    def __init__(self, *, key_prefix: str, scan_count: int) -> None:
        if not isinstance(key_prefix, str):
            raise TypeError("key_prefix must be text")
        if not key_prefix or "\x00" in key_prefix:
            raise ValueError("key_prefix must be non-empty text without NUL bytes")
        if isinstance(scan_count, bool) or not isinstance(scan_count, int):
            raise TypeError("scan_count must be an integer")
        if scan_count <= 0:
            raise ValueError("scan_count must be positive")
        self.key_prefix = key_prefix.rstrip(":")
        if not self.key_prefix:
            raise ValueError("key_prefix must contain a character other than ':'")
        self.scan_count = scan_count

    @property
    def catalog_key(self) -> str:
        return f"{self.key_prefix}:namespaces"

    def namespace_key(self, namespace: tuple[str, ...]) -> str:
        digest = hashlib.sha256(_namespace_identity(namespace)).hexdigest()
        return f"{self.key_prefix}:{{lgs:{digest}}}:namespace"

    def item_lock_key(self, namespace: tuple[str, ...], key: str) -> str:
        identity = _namespace_identity(namespace)
        key_payload = key.encode("utf-8")
        digest = hashlib.sha256(
            identity + len(key_payload).to_bytes(8, "big") + key_payload
        ).hexdigest()
        return f"{self.key_prefix}:{{lgsi:{digest}}}:mutation-lock"

    def prefix_catalog_key(self, namespace_prefix: tuple[str, ...]) -> str:
        if not namespace_prefix:
            return self.catalog_key
        digest = hashlib.sha256(_namespace_identity(namespace_prefix)).hexdigest()
        return f"{self.key_prefix}:prefix:{digest}"

    @staticmethod
    def _item_field(key: str) -> str:
        return f"{_ITEM_PREFIX}{_encode_component(key)}"

    @staticmethod
    def _serialize(value: Mapping[str, Any]) -> bytes:
        try:
            return json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TypeError("LangGraph store values must be JSON serializable") from exc

    @staticmethod
    def _deserialize(value: Any, *, name: str) -> Mapping[str, Any]:
        try:
            decoded = json.loads(_raw_bytes(value, name=name))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid FerricStore LangGraph {name}") from exc
        if not isinstance(decoded, Mapping):
            raise TypeError(f"stored LangGraph {name} must be a mapping")
        return cast(Mapping[str, Any], decoded)

    def _item_record(
        self,
        op: PutOp,
        *,
        created_at: datetime,
        updated_at: datetime,
    ) -> bytes:
        value = op.value
        if not isinstance(value, dict) or not all(
            isinstance(key, str) for key in value
        ):
            raise TypeError("LangGraph store values must be dictionaries with string keys")
        return self._serialize(
            {
                "format_version": _FORMAT_VERSION,
                "namespace": list(op.namespace),
                "key": op.key,
                "value": value,
                "created_at": created_at.isoformat(),
                "updated_at": updated_at.isoformat(),
            }
        )

    def _validate_put(self, op: PutOp) -> None:
        _validate_namespace(op.namespace)
        if not isinstance(op.key, str):
            raise TypeError("LangGraph store keys must be text")
        if op.ttl is not None:
            raise NotImplementedError("TTL is not supported by FerricStoreStore")
        if op.value is None:
            return
        if not isinstance(op.value, dict) or not all(
            isinstance(key, str) for key in op.value
        ):
            raise TypeError("LangGraph store values must be dictionaries with string keys")
        _validate_json_keys(op.value)
        self._serialize(op.value)

    def mutation_lock_keys(self, ops: Sequence[Op]) -> builtins.list[str]:
        keys: builtins.list[str] = []
        for op in ops:
            if not isinstance(op, PutOp):
                continue
            self._validate_put(op)
            keys.append(self.item_lock_key(op.namespace, op.key))
        return keys

    def _decode_item(self, value: Any) -> Item:
        record = self._deserialize(value, name="store item")
        if record.get("format_version") != _FORMAT_VERSION:
            raise ValueError("unsupported FerricStore LangGraph store format")
        raw_namespace = record.get("namespace")
        key = record.get("key")
        item_value = record.get("value")
        created_at = record.get("created_at")
        updated_at = record.get("updated_at")
        if not isinstance(raw_namespace, list) or not all(
            isinstance(component, str) for component in raw_namespace
        ):
            raise TypeError("stored LangGraph item namespace must be a list of strings")
        if not isinstance(key, str):
            raise TypeError("stored LangGraph item key must be text")
        if not isinstance(item_value, dict) or not all(
            isinstance(name, str) for name in item_value
        ):
            raise TypeError("stored LangGraph item value must be a dictionary")
        if not isinstance(created_at, str) or not isinstance(updated_at, str):
            raise TypeError("stored LangGraph item timestamps must be text")
        return Item(
            namespace=tuple(raw_namespace),
            key=key,
            value=cast(dict[str, Any], item_value),
            created_at=datetime.fromisoformat(created_at),
            updated_at=datetime.fromisoformat(updated_at),
        )

    def _catalog_page(
        self,
        namespace_prefix: tuple[str, ...],
        start: int,
        count: int,
    ) -> _CommandPlan[builtins.list[tuple[tuple[str, ...], str]]]:
        response = yield (
            "ZRANGE",
            self.prefix_catalog_key(namespace_prefix),
            start,
            start + count - 1,
        )
        if response is None:
            return []
        if not isinstance(response, Sequence) or isinstance(
            response, (str, bytes, bytearray)
        ):
            raise TypeError("FerricStore ZRANGE returned an invalid response")
        return [_decode_catalog_member(member) for member in response]

    def _read_catalog_items(
        self,
        locators: Sequence[tuple[tuple[str, ...], str]],
    ) -> _CommandPlan[builtins.list[Item | None]]:
        if not locators:
            return []
        responses = _batch_items(
            (
                yield _CommandBatch(
                    tuple(
                        (
                            "HGET",
                            self.namespace_key(namespace),
                            self._item_field(key),
                        )
                        for namespace, key in locators
                    )
                )
            ),
            len(locators),
        )
        items: builtins.list[Item | None] = []
        for (namespace, key), response in zip(locators, responses, strict=True):
            if response is None:
                items.append(None)
                continue
            item = self._decode_item(response)
            if item.namespace != namespace or item.key != key:
                raise ValueError("FerricStore LangGraph item/catalog mismatch")
            items.append(item)
        return items

    def _get(self, op: GetOp) -> _CommandPlan[Item | None]:
        value = yield (
            "HGET",
            self.namespace_key(op.namespace),
            self._item_field(op.key),
        )
        if value is None:
            return None
        item = self._decode_item(value)
        if item.namespace != op.namespace or item.key != op.key:
            raise ValueError("FerricStore LangGraph item key mismatch")
        return item

    def _search(self, op: SearchOp) -> _CommandPlan[builtins.list[SearchItem]]:
        if op.limit <= 0:
            return []
        results: builtins.list[SearchItem] = []
        matched = 0
        start = 0
        while True:
            locators = yield from self._catalog_page(
                op.namespace_prefix,
                start,
                self.scan_count,
            )
            if not locators:
                return results
            items = yield from self._read_catalog_items(locators)
            for item in items:
                if item is None or not _matches_filter(item.value, op.filter):
                    continue
                if matched < op.offset:
                    matched += 1
                    continue
                results.append(
                    SearchItem(
                        namespace=item.namespace,
                        key=item.key,
                        value=item.value,
                        created_at=item.created_at,
                        updated_at=item.updated_at,
                        score=None,
                    )
                )
                if len(results) == op.limit:
                    return results
            start += len(locators)
            if len(locators) < self.scan_count:
                return results

    @staticmethod
    def _catalog_prefix(op: ListNamespacesOp) -> tuple[str, ...]:
        exact_prefixes = [
            condition.path
            for condition in (op.match_conditions or ())
            if condition.match_type == "prefix" and "*" not in condition.path
        ]
        return max(exact_prefixes, key=len, default=())

    def _list_namespaces(
        self,
        op: ListNamespacesOp,
    ) -> _CommandPlan[builtins.list[tuple[str, ...]]]:
        if op.limit <= 0:
            return []
        catalog_prefix = self._catalog_prefix(op)
        namespaces: builtins.list[tuple[str, ...]] = []
        matched = 0
        last_namespace: tuple[str, ...] | None = None
        start = 0
        while True:
            locators = yield from self._catalog_page(
                catalog_prefix,
                start,
                self.scan_count,
            )
            if not locators:
                return namespaces
            items = yield from self._read_catalog_items(locators)
            for item in items:
                if item is None:
                    continue
                namespace = item.namespace
                if op.match_conditions and not all(
                    _matches_namespace(condition, namespace)
                    for condition in op.match_conditions
                ):
                    continue
                selected = (
                    namespace[: op.max_depth]
                    if op.max_depth is not None
                    else namespace
                )
                if selected == last_namespace:
                    continue
                last_namespace = selected
                if matched < op.offset:
                    matched += 1
                    continue
                namespaces.append(selected)
                if len(namespaces) == op.limit:
                    return namespaces
            start += len(locators)
            if len(locators) < self.scan_count:
                return namespaces

    def _apply_puts(self, ops: Sequence[PutOp]) -> _CommandPlan[None]:
        if not ops:
            return None
        for op in ops:
            self._validate_put(op)
        deletes = [op for op in ops if op.value is None]
        pending = [
            _PendingPut(op=op, timestamp=datetime.now(timezone.utc))
            for op in ops
            if op.value is not None
        ]

        # Removing data is the delete visibility commit. Index entries are
        # removed afterwards, so an interrupted delete can leave only a safe
        # tombstone that searches already ignore, never a hidden live item.
        delete_items = tuple(
            (
                "HDEL",
                self.namespace_key(op.namespace),
                self._item_field(op.key),
            )
            for op in deletes
        )
        if delete_items:
            yield _CommandBatch(delete_items)
        delete_indexes = tuple(
            (
                "ZREM",
                self.prefix_catalog_key(op.namespace[:depth]),
                _catalog_member(op.namespace, op.key),
            )
            for op in deletes
            for depth in range(len(op.namespace) + 1)
        )
        if delete_indexes:
            yield _CommandBatch(delete_indexes)

        created_at: dict[tuple[tuple[str, ...], str], datetime] = {
            (item.op.namespace, item.op.key): item.timestamp for item in pending
        }
        if pending:
            existing_responses = _batch_items(
                (
                    yield _CommandBatch(
                        tuple(
                            (
                                "HGET",
                                self.namespace_key(item.op.namespace),
                                self._item_field(item.op.key),
                            )
                            for item in pending
                        )
                    )
                ),
                len(pending),
            )
            for item, response in zip(pending, existing_responses, strict=True):
                if response is None:
                    continue
                existing = self._decode_item(response)
                if (
                    existing.namespace != item.op.namespace
                    or existing.key != item.op.key
                ):
                    raise ValueError("FerricStore LangGraph item key mismatch")
                created_at[(item.op.namespace, item.op.key)] = existing.created_at

            # Publish every discovery path before the item data. If this batch
            # or the process fails, searches skip the incomplete locator. Once
            # HSET succeeds, get/search/list_namespaces agree even if the
            # response is lost and the caller retries.
            yield _CommandBatch(
                tuple(
                    (
                        "ZADD",
                        self.prefix_catalog_key(item.op.namespace[:depth]),
                        0,
                        _catalog_member(item.op.namespace, item.op.key),
                    )
                    for item in pending
                    for depth in range(len(item.op.namespace) + 1)
                )
            )

            yield _CommandBatch(
                tuple(
                    (
                        "HSET",
                        self.namespace_key(item.op.namespace),
                        self._item_field(item.op.key),
                        self._item_record(
                            item.op,
                            created_at=created_at[(item.op.namespace, item.op.key)],
                            updated_at=item.timestamp,
                        ),
                    )
                    for item in pending
                )
            )
        return None

    def batch(self, ops: Iterable[Op]) -> _CommandPlan[builtins.list[Result]]:
        operations = list(ops)
        results: builtins.list[Result] = [None] * len(operations)
        puts: dict[tuple[tuple[str, ...], str], PutOp] = {}
        get_indices: builtins.list[int] = []
        get_commands: builtins.list[_Command] = []
        for index, op in enumerate(operations):
            if isinstance(op, GetOp):
                get_indices.append(index)
                get_commands.append(
                    (
                        "HGET",
                        self.namespace_key(op.namespace),
                        self._item_field(op.key),
                    )
                )
            elif isinstance(op, SearchOp):
                results[index] = yield from self._search(op)
            elif isinstance(op, ListNamespacesOp):
                results[index] = yield from self._list_namespaces(op)
            elif isinstance(op, PutOp):
                puts[(op.namespace, op.key)] = op
            else:
                raise ValueError(f"Unknown LangGraph store operation: {type(op)!r}")

        if get_commands:
            get_responses = _batch_items(
                (yield _CommandBatch(tuple(get_commands))),
                len(get_commands),
            )
            for index, response in zip(get_indices, get_responses, strict=True):
                results[index] = None if response is None else self._decode_item(response)

        yield from self._apply_puts(list(puts.values()))
        return results


class _FerricStoreStoreBase(BaseStore):
    supports_ttl = False

    def __init__(
        self,
        *,
        key_prefix: str = "langgraph:store",
        scan_count: int = 256,
    ) -> None:
        self._storage = _FerricStoreMemoryStorage(
            key_prefix=key_prefix,
            scan_count=scan_count,
        )
        self.key_prefix = self._storage.key_prefix
        self.scan_count = self._storage.scan_count

    @property
    def _catalog_key(self) -> str:
        return self._storage.catalog_key


class FerricStoreStore(_FerricStoreStoreBase):
    """Synchronous LangGraph cross-thread store backed by FerricStore."""

    def __init__(
        self,
        client: _SyncCommandClient,
        *,
        key_prefix: str = "langgraph:store",
        scan_count: int = 256,
    ) -> None:
        super().__init__(key_prefix=key_prefix, scan_count=scan_count)
        self.client = client

    def batch(self, ops: Iterable[Op]) -> builtins.list[Result]:
        operations = list(ops)
        return run_sync_with_locks(
            self.client,
            self._storage.mutation_lock_keys(operations),
            lambda: _run_sync(self.client, self._storage.batch(operations)),
        )

    async def abatch(self, ops: Iterable[Op]) -> builtins.list[Result]:
        operations = list(ops)
        return await asyncio.to_thread(self.batch, operations)


class AsyncFerricStoreStore(_FerricStoreStoreBase):
    """Native-async LangGraph cross-thread store backed by FerricStore."""

    def __init__(
        self,
        client: _AsyncCommandClient,
        *,
        key_prefix: str = "langgraph:store",
        scan_count: int = 256,
    ) -> None:
        super().__init__(key_prefix=key_prefix, scan_count=scan_count)
        self.client = client

    def batch(self, ops: Iterable[Op]) -> builtins.list[Result]:
        del ops
        raise NotImplementedError("use asynchronous methods with AsyncFerricStoreStore")

    async def abatch(self, ops: Iterable[Op]) -> builtins.list[Result]:
        operations = list(ops)

        async def operation() -> builtins.list[Result]:
            return await _run_async(self.client, self._storage.batch(operations))

        return await run_async_with_locks(
            self.client,
            self._storage.mutation_lock_keys(operations),
            operation,
        )


__all__ = ["AsyncFerricStoreStore", "FerricStoreStore"]
