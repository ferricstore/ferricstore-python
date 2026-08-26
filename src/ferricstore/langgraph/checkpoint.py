from __future__ import annotations

import asyncio
import base64
import builtins
import hashlib
import struct
from collections.abc import AsyncIterator, Generator, Iterator, Mapping, Sequence
from typing import Any, Protocol, TypeVar, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from ferricstore.langgraph._locks import run_async_with_locks, run_sync_with_locks


class _SyncCommandClient(Protocol):
    def command(self, *args: Any) -> Any: ...


class _AsyncCommandClient(Protocol):
    async def command(self, *args: Any) -> Any: ...


_FORMAT_VERSION = 1
_DESCRIPTOR_FIELD = "descriptor"
_TYPE_LENGTH = struct.Struct(">H")
_Command = tuple[Any, ...]
_ResultT = TypeVar("_ResultT")
_CommandPlan = Generator[_Command, Any, _ResultT]


def _text(value: Any, *, name: str) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, bytearray):
        return bytes(value).decode("utf-8")
    if isinstance(value, memoryview):
        return value.tobytes().decode("utf-8")
    if value is None:
        raise ValueError(f"{name} is required")
    return str(value)


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


def _encode_component(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).rstrip(b"=").decode("ascii")


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


def _decode_ordered_text(value: bytes, offset: int = 0) -> tuple[str, int]:
    decoded = bytearray()
    while offset < len(value):
        value_byte = value[offset]
        offset += 1
        if value_byte != 0:
            decoded.append(value_byte)
            continue
        if offset >= len(value):
            raise ValueError("truncated FerricStore LangGraph checkpoint locator")
        escaped = value[offset]
        offset += 1
        if escaped == 0:
            try:
                return decoded.decode("utf-8"), offset
            except UnicodeDecodeError as exc:
                raise ValueError(
                    "invalid UTF-8 in FerricStore LangGraph checkpoint locator"
                ) from exc
        if escaped == 255:
            decoded.append(0)
            continue
        raise ValueError("invalid FerricStore LangGraph checkpoint locator escape")
    raise ValueError("unterminated FerricStore LangGraph checkpoint locator")


def _pack_typed(value: tuple[str, bytes]) -> bytes:
    type_name, payload = value
    encoded_type = type_name.encode("utf-8")
    if len(encoded_type) > 65_535:
        raise ValueError("serialized LangGraph type name exceeds 65535 bytes")
    return _TYPE_LENGTH.pack(len(encoded_type)) + encoded_type + payload


def _unpack_typed(value: Any) -> tuple[str, bytes]:
    payload = _raw_bytes(value, name="checkpoint payload")
    if len(payload) < _TYPE_LENGTH.size:
        raise ValueError("invalid FerricStore LangGraph payload")
    (type_length,) = _TYPE_LENGTH.unpack_from(payload)
    type_end = _TYPE_LENGTH.size + type_length
    if len(payload) < type_end:
        raise ValueError("truncated FerricStore LangGraph payload")
    return payload[_TYPE_LENGTH.size:type_end].decode("utf-8"), payload[type_end:]


def _scan_page(response: Any) -> tuple[int, builtins.list[tuple[Any, Any]]]:
    if not isinstance(response, Sequence) or isinstance(response, (str, bytes, bytearray)):
        raise TypeError("FerricStore HSCAN returned an invalid response")
    if len(response) != 2:
        raise ValueError("FerricStore HSCAN response must contain cursor and items")
    cursor = int(_text(response[0], name="HSCAN cursor"))
    raw_items = response[1]
    if isinstance(raw_items, Mapping):
        return cursor, list(raw_items.items())
    if not isinstance(raw_items, Sequence) or isinstance(
        raw_items, (str, bytes, bytearray)
    ):
        raise TypeError("FerricStore HSCAN items must be a mapping or sequence")
    if len(raw_items) % 2:
        raise ValueError("FerricStore HSCAN returned an odd number of field/value items")
    return cursor, [
        (raw_items[index], raw_items[index + 1]) for index in range(0, len(raw_items), 2)
    ]


def _config(thread_id: str, checkpoint_ns: str, checkpoint_id: str) -> RunnableConfig:
    return {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
        }
    }


def _run_sync(client: _SyncCommandClient, plan: _CommandPlan[_ResultT]) -> _ResultT:
    """Execute one shared storage command plan with a synchronous client."""
    try:
        command = next(plan)
    except StopIteration as stopped:
        return cast(_ResultT, stopped.value)
    while True:
        response = client.command(*command)
        try:
            command = plan.send(response)
        except StopIteration as stopped:
            return cast(_ResultT, stopped.value)


async def _run_async(
    client: _AsyncCommandClient, plan: _CommandPlan[_ResultT]
) -> _ResultT:
    """Execute one shared storage command plan with an asynchronous client."""
    try:
        command = next(plan)
    except StopIteration as stopped:
        return cast(_ResultT, stopped.value)
    while True:
        response = await client.command(*command)
        try:
            command = plan.send(response)
        except StopIteration as stopped:
            return cast(_ResultT, stopped.value)


class _FerricStoreCheckpointStorage:
    """Transport-independent FerricStore checkpoint operations.

    Every operation is represented as a command plan. The same plan is driven by
    either ``_run_sync`` or ``_run_async``, keeping storage semantics identical
    without forcing synchronous callers through an event loop.
    """

    def __init__(
        self,
        *,
        key_prefix: str,
        scan_count: int,
        serde: SerializerProtocol,
    ) -> None:
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
        self.serde = serde

    @property
    def catalog_key(self) -> str:
        return f"{self.key_prefix}:checkpoints"

    def thread_catalog_key(self, thread_id: str) -> str:
        digest = hashlib.sha256(thread_id.encode("utf-8")).hexdigest()
        return f"{self.key_prefix}:{{lgt:{digest}}}:namespaces"

    def thread_locator_catalog_key(self, thread_id: str) -> str:
        return f"{self.thread_catalog_key(thread_id)}:checkpoint-locators"

    def thread_lock_key(self, thread_id: str) -> str:
        digest = hashlib.sha256(thread_id.encode("utf-8")).hexdigest()
        return f"{self.key_prefix}:{{lgt:{digest}}}:mutation-lock"

    def thread_key(self, thread_id: str, checkpoint_ns: str) -> str:
        identity = (
            len(thread_id).to_bytes(8, "big")
            + thread_id.encode("utf-8")
            + len(checkpoint_ns).to_bytes(8, "big")
            + checkpoint_ns.encode("utf-8")
        )
        digest = hashlib.sha256(identity).hexdigest()
        return f"{self.key_prefix}:{{lg:{digest}}}:thread"

    @staticmethod
    def checkpoint_index_key(thread_key: str) -> str:
        return f"{thread_key}:checkpoint-index"

    @staticmethod
    def checkpoint_locator(checkpoint_id: str, thread_key: str) -> bytes:
        return _ordered_text(checkpoint_id) + thread_key.encode("utf-8")

    @staticmethod
    def decode_checkpoint_locator(value: Any) -> tuple[str, str]:
        encoded = _raw_bytes(value, name="checkpoint locator")
        checkpoint_id, offset = _decode_ordered_text(encoded)
        try:
            thread_key = encoded[offset:].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid UTF-8 in FerricStore checkpoint locator key") from exc
        if not thread_key:
            raise ValueError("FerricStore checkpoint locator has an empty thread key")
        return checkpoint_id, thread_key

    def _descriptor(self, thread_id: str, checkpoint_ns: str) -> bytes:
        return self._serialize(
            {
                "format_version": _FORMAT_VERSION,
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            }
        )

    @staticmethod
    def _checkpoint_field(checkpoint_id: str) -> str:
        return f"checkpoint:{_encode_component(checkpoint_id)}"

    @staticmethod
    def _writes_pattern(checkpoint_id: str) -> str:
        return f"write:{_encode_component(checkpoint_id)}:*"

    @staticmethod
    def _write_field(checkpoint_id: str, task_id: str, index: int) -> str:
        return (
            f"write:{_encode_component(checkpoint_id)}:"
            f"{_encode_component(task_id)}:{index}"
        )

    def _serialize(self, value: Any) -> bytes:
        return _pack_typed(self.serde.dumps_typed(value))

    def _deserialize(self, value: Any) -> Any:
        return self.serde.loads_typed(_unpack_typed(value))

    @staticmethod
    def _identity(config: RunnableConfig) -> tuple[str, str]:
        configurable = config.get("configurable")
        if not isinstance(configurable, Mapping):
            raise ValueError("LangGraph config must contain a configurable mapping")
        thread_id = _text(configurable.get("thread_id"), name="thread_id")
        checkpoint_ns = _text(configurable.get("checkpoint_ns", ""), name="checkpoint_ns")
        return thread_id, checkpoint_ns

    def _record(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
    ) -> dict[str, Any]:
        thread_id, checkpoint_ns = self._identity(config)
        checkpoint_id = _text(checkpoint.get("id"), name="checkpoint id")
        return {
            "format_version": _FORMAT_VERSION,
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "parent_checkpoint_id": get_checkpoint_id(config),
            "checkpoint": checkpoint,
            "metadata": get_checkpoint_metadata(config, metadata),
        }

    def _tuple_from_record(
        self,
        record: Mapping[str, Any],
        pending_writes: builtins.list[tuple[str, str, Any]],
    ) -> CheckpointTuple:
        if record.get("format_version") != _FORMAT_VERSION:
            raise ValueError("unsupported FerricStore LangGraph checkpoint format")
        thread_id = _text(record.get("thread_id"), name="stored thread_id")
        checkpoint_ns = _text(record.get("checkpoint_ns", ""), name="stored checkpoint_ns")
        checkpoint_id = _text(record.get("checkpoint_id"), name="stored checkpoint_id")
        parent_id = record.get("parent_checkpoint_id")
        checkpoint = record.get("checkpoint")
        metadata = record.get("metadata")
        if not isinstance(checkpoint, dict):
            raise TypeError("stored LangGraph checkpoint must be a mapping")
        if not isinstance(metadata, dict):
            raise TypeError("stored LangGraph checkpoint metadata must be a mapping")
        return CheckpointTuple(
            config=_config(thread_id, checkpoint_ns, checkpoint_id),
            checkpoint=cast(Checkpoint, checkpoint),
            metadata=cast(CheckpointMetadata, metadata),
            parent_config=(
                _config(thread_id, checkpoint_ns, _text(parent_id, name="parent checkpoint id"))
                if parent_id is not None
                else None
            ),
            pending_writes=pending_writes,
        )

    def _write_record(
        self,
        *,
        task_id: str,
        channel: str,
        value: Any,
        task_path: str,
        index: int,
    ) -> bytes:
        return self._serialize(
            {
                "format_version": _FORMAT_VERSION,
                "task_id": task_id,
                "channel": channel,
                "value": value,
                "task_path": task_path,
                "index": index,
            }
        )

    def _decode_pending_writes(
        self, values: Sequence[Any]
    ) -> builtins.list[tuple[str, str, Any]]:
        records: builtins.list[Mapping[str, Any]] = []
        for value in values:
            decoded = self._deserialize(value)
            if not isinstance(decoded, Mapping):
                raise TypeError("stored LangGraph pending write must be a mapping")
            if decoded.get("format_version") != _FORMAT_VERSION:
                raise ValueError("unsupported FerricStore LangGraph pending-write format")
            records.append(decoded)
        records.sort(
            key=lambda item: (
                _text(item.get("task_id"), name="stored task_id"),
                int(item.get("index", 0)),
            )
        )
        return [
            (
                _text(item.get("task_id"), name="stored task_id"),
                _text(item.get("channel"), name="stored channel"),
                item.get("value"),
            )
            for item in records
        ]

    def _scan_hash(
        self, key: str, pattern: str
    ) -> _CommandPlan[builtins.list[tuple[Any, Any]]]:
        cursor = 0
        items: builtins.list[tuple[Any, Any]] = []
        while True:
            response = yield (
                "HSCAN",
                key,
                cursor,
                "MATCH",
                pattern,
                "COUNT",
                self.scan_count,
            )
            cursor, page = _scan_page(response)
            items.extend(page)
            if cursor == 0:
                return items

    def _pending_writes(
        self, key: str, checkpoint_id: str
    ) -> _CommandPlan[builtins.list[tuple[str, str, Any]]]:
        items = yield from self._scan_hash(key, self._writes_pattern(checkpoint_id))
        return self._decode_pending_writes([value for _, value in items])

    def _read_record(
        self, key: str, checkpoint_id: str
    ) -> _CommandPlan[Mapping[str, Any] | None]:
        value = yield ("HGET", key, self._checkpoint_field(checkpoint_id))
        if value is None:
            return None
        record = self._deserialize(value)
        if not isinstance(record, Mapping):
            raise TypeError("stored LangGraph checkpoint record must be a mapping")
        return record

    def get_tuple(self, config: RunnableConfig) -> _CommandPlan[CheckpointTuple | None]:
        thread_id, checkpoint_ns = self._identity(config)
        key = self.thread_key(thread_id, checkpoint_ns)
        checkpoint_id = get_checkpoint_id(config)
        record: Mapping[str, Any] | None = None
        if checkpoint_id is not None:
            record = yield from self._read_record(key, checkpoint_id)
        else:
            # Index publication deliberately happens before the checkpoint hash
            # write. Skip an incomplete locator left by an interrupted put and
            # continue to the newest fully committed record.
            offset = 0
            while True:
                response = yield (
                    "ZREVRANGE",
                    self.checkpoint_index_key(key),
                    offset,
                    offset,
                )
                if not response:
                    return None
                if not isinstance(response, Sequence) or isinstance(
                    response, (str, bytes, bytearray)
                ):
                    raise TypeError("FerricStore ZREVRANGE returned an invalid response")
                checkpoint_id = _text(response[0], name="latest checkpoint id")
                record = yield from self._read_record(key, checkpoint_id)
                if record is not None:
                    break
                offset += 1
        if checkpoint_id is None or record is None:
            return None
        pending_writes = yield from self._pending_writes(key, checkpoint_id)
        return self._tuple_from_record(record, pending_writes)

    def _thread_keys(self, thread_id: str) -> _CommandPlan[builtins.list[str]]:
        values = (yield ("SMEMBERS", self.thread_catalog_key(thread_id))) or []
        return sorted(_text(value, name="thread checkpoint key") for value in values)

    def _candidate_keys(
        self, config: RunnableConfig | None
    ) -> _CommandPlan[builtins.list[str]]:
        if config is None:
            raise ValueError("global checkpoint listing uses the ordered locator index")
        configurable = config.get("configurable")
        if not isinstance(configurable, Mapping):
            raise ValueError("LangGraph config must contain a configurable mapping")
        thread_id = _text(configurable.get("thread_id"), name="thread_id")
        if "checkpoint_ns" in configurable:
            checkpoint_ns = _text(configurable.get("checkpoint_ns"), name="checkpoint_ns")
            return [self.thread_key(thread_id, checkpoint_ns)]
        return (yield from self._thread_keys(thread_id))

    def _checkpoint_ids(
        self,
        key: str,
        start: int,
        count: int | None,
    ) -> _CommandPlan[builtins.list[str]]:
        response = yield (
            "ZREVRANGE",
            self.checkpoint_index_key(key),
            start,
            -1 if count is None else start + count - 1,
        )
        if response is None:
            return []
        if not isinstance(response, Sequence) or isinstance(
            response, (str, bytes, bytearray)
        ):
            raise TypeError("FerricStore ZREVRANGE returned an invalid response")
        return [_text(item, name="checkpoint id") for item in response]

    def _global_locator_page(
        self,
        start: int,
    ) -> _CommandPlan[builtins.list[tuple[Any, str, str]]]:
        response = yield (
            "ZREVRANGE",
            self.catalog_key,
            start,
            start + self.scan_count - 1,
        )
        if response is None:
            return []
        if not isinstance(response, Sequence) or isinstance(
            response, (str, bytes, bytearray)
        ):
            raise TypeError("FerricStore ZREVRANGE returned an invalid response")
        return [
            (member, *self.decode_checkpoint_locator(member))
            for member in response
        ]

    def _list_global(
        self,
        *,
        filter: dict[str, Any] | None,
        before_id: str | None,
        limit: int | None,
    ) -> _CommandPlan[builtins.list[CheckpointTuple]]:
        records: builtins.list[tuple[str, str, Mapping[str, Any]]] = []
        start = 0
        while True:
            locators = yield from self._global_locator_page(start)
            if not locators:
                break
            for _member, checkpoint_id, key in locators:
                if before_id is not None and checkpoint_id >= before_id:
                    continue
                record = yield from self._read_record(key, checkpoint_id)
                if record is None:
                    # A put publishes its locator before its record. Missing
                    # records are either still in flight or safely retryable
                    # remnants of an interrupted put, and are never visible.
                    continue
                metadata = record.get("metadata")
                if not isinstance(metadata, Mapping):
                    raise TypeError("stored LangGraph checkpoint metadata must be a mapping")
                if filter and not all(
                    metadata.get(name) == value for name, value in filter.items()
                ):
                    continue
                records.append((checkpoint_id, key, record))
                if limit is not None and len(records) == limit:
                    break
            if limit is not None and len(records) == limit:
                break
            start += len(locators)
            if len(locators) < self.scan_count:
                break

        tuples: builtins.list[CheckpointTuple] = []
        for checkpoint_id, key, record in records:
            pending_writes = yield from self._pending_writes(key, checkpoint_id)
            tuples.append(self._tuple_from_record(record, pending_writes))
        return tuples

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None,
        before: RunnableConfig | None,
        limit: int | None,
    ) -> _CommandPlan[builtins.list[CheckpointTuple]]:
        if limit is not None and limit <= 0:
            return []
        expected_id = get_checkpoint_id(config) if config is not None else None
        before_id = get_checkpoint_id(before) if before is not None else None
        if config is None:
            return (
                yield from self._list_global(
                    filter=filter,
                    before_id=before_id,
                    limit=limit,
                )
            )
        results: builtins.list[tuple[str, str, Mapping[str, Any]]] = []
        for key in (yield from self._candidate_keys(config)):
            if expected_id is not None:
                checkpoint_ids = [expected_id]
                pages = [checkpoint_ids]
            else:
                pages = []
            start = 0
            key_matches = 0
            while expected_id is not None or not pages:
                if expected_id is None:
                    if limit is None:
                        count = None
                    elif filter is None and before_id is None:
                        count = max(limit - key_matches, 1)
                    else:
                        count = self.scan_count
                    checkpoint_ids = yield from self._checkpoint_ids(key, start, count)
                    if not checkpoint_ids:
                        break
                else:
                    checkpoint_ids = pages.pop()
                for checkpoint_id in checkpoint_ids:
                    if before_id is not None and checkpoint_id >= before_id:
                        continue
                    record = yield from self._read_record(key, checkpoint_id)
                    if record is None:
                        continue
                    metadata = record.get("metadata")
                    if not isinstance(metadata, Mapping):
                        raise TypeError("stored LangGraph checkpoint metadata must be a mapping")
                    if filter and not all(
                        metadata.get(name) == value for name, value in filter.items()
                    ):
                        continue
                    results.append((checkpoint_id, key, record))
                    key_matches += 1
                    if limit is not None and key_matches == limit:
                        break
                if expected_id is not None or (limit is not None and key_matches == limit):
                    break
                start += len(checkpoint_ids)
                if count is None or len(checkpoint_ids) < count:
                    break
        results.sort(key=lambda item: item[0], reverse=True)
        if limit is not None:
            results = results[:limit]
        tuples: builtins.list[CheckpointTuple] = []
        for checkpoint_id, key, record in results:
            pending_writes = yield from self._pending_writes(key, checkpoint_id)
            tuples.append(self._tuple_from_record(record, pending_writes))
        return tuples

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
    ) -> _CommandPlan[RunnableConfig]:
        record = self._record(config, checkpoint, metadata)
        thread_id = cast(str, record["thread_id"])
        checkpoint_ns = cast(str, record["checkpoint_ns"])
        checkpoint_id = cast(str, record["checkpoint_id"])
        key = self.thread_key(thread_id, checkpoint_ns)
        locator = self.checkpoint_locator(checkpoint_id, key)
        # Publish every discovery path before the record. If any command or
        # process fails before the final HSET, readers validate the locator and
        # skip it. Once HSET succeeds, latest/thread/global reads can all find
        # the checkpoint, including when the response is lost in transit.
        yield ("SADD", self.thread_catalog_key(thread_id), key)
        yield ("ZADD", self.thread_locator_catalog_key(thread_id), 0, locator)
        yield ("ZADD", self.catalog_key, 0, locator)
        yield (
            "ZADD",
            self.checkpoint_index_key(key),
            0,
            checkpoint_id,
        )
        yield (
            "HSET",
            key,
            _DESCRIPTOR_FIELD,
            self._descriptor(thread_id, checkpoint_ns),
            self._checkpoint_field(checkpoint_id),
            self._serialize(record),
        )
        return _config(thread_id, checkpoint_ns, checkpoint_id)

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str,
    ) -> _CommandPlan[None]:
        thread_id, checkpoint_ns = self._identity(config)
        checkpoint_id = get_checkpoint_id(config)
        if checkpoint_id is None:
            raise ValueError("put_writes requires checkpoint_id in configurable")
        key = self.thread_key(thread_id, checkpoint_ns)
        # The catalog is written first so a partially completed writes batch is
        # always discoverable and removable by delete_thread().
        yield ("SADD", self.thread_catalog_key(thread_id), key)
        yield (
            "HSETNX",
            key,
            _DESCRIPTOR_FIELD,
            self._descriptor(thread_id, checkpoint_ns),
        )
        for fallback_index, (channel, value) in enumerate(writes):
            index = WRITES_IDX_MAP.get(channel, fallback_index)
            command = "HSET" if index < 0 else "HSETNX"
            yield (
                command,
                key,
                self._write_field(checkpoint_id, task_id, index),
                self._write_record(
                    task_id=task_id,
                    channel=channel,
                    value=value,
                    task_path=task_path,
                    index=index,
                ),
            )
        return None

    def delete_thread(self, thread_id: str) -> _CommandPlan[None]:
        normalized_thread_id = _text(thread_id, name="thread_id")
        locator_catalog_key = self.thread_locator_catalog_key(normalized_thread_id)
        for key in (yield from self._thread_keys(normalized_thread_id)):
            yield ("DEL", key, self.checkpoint_index_key(key))
        while True:
            locators = yield ("ZRANGE", locator_catalog_key, 0, self.scan_count - 1)
            if not locators:
                break
            if not isinstance(locators, Sequence) or isinstance(
                locators, (str, bytes, bytearray)
            ):
                raise TypeError("FerricStore ZRANGE returned an invalid response")
            yield ("ZREM", self.catalog_key, *locators)
            yield ("ZREM", locator_catalog_key, *locators)
        yield (
            "DEL",
            self.thread_catalog_key(normalized_thread_id),
            locator_catalog_key,
        )
        return None


class _FerricStoreSaverBase(BaseCheckpointSaver[int]):
    """Shared LangGraph contract configuration for FerricStore savers."""

    def __init__(
        self,
        *,
        key_prefix: str = "langgraph:checkpoint",
        scan_count: int = 256,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self._storage = _FerricStoreCheckpointStorage(
            key_prefix=key_prefix,
            scan_count=scan_count,
            serde=self.serde,
        )
        self.key_prefix = self._storage.key_prefix
        self.scan_count = self._storage.scan_count

    @property
    def _catalog_key(self) -> str:
        return self._storage.catalog_key


class FerricStoreSaver(_FerricStoreSaverBase):
    """Synchronous LangGraph checkpointer backed by a FerricStore client.

    The saver stores each ``(thread_id, checkpoint_ns)`` in one FerricStore hash,
    deriving the latest checkpoint from an ordered checkpoint-ID index so
    concurrent writers cannot move thread state backward.
    The same saver also supports LangGraph's asynchronous graph APIs by moving
    synchronous client work to a worker thread.
    """

    def __init__(
        self,
        client: _SyncCommandClient,
        *,
        key_prefix: str = "langgraph:checkpoint",
        scan_count: int = 256,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(key_prefix=key_prefix, scan_count=scan_count, serde=serde)
        self.client = client

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return _run_sync(self.client, self._storage.get_tuple(config))

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        yield from _run_sync(
            self.client,
            self._storage.list(
                config,
                filter=filter,
                before=before,
                limit=limit,
            ),
        )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        del new_versions
        thread_id, _ = self._storage._identity(config)
        return run_sync_with_locks(
            self.client,
            [self._storage.thread_lock_key(thread_id)],
            lambda: _run_sync(
                self.client,
                self._storage.put(config, checkpoint, metadata),
            ),
        )

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        thread_id, _ = self._storage._identity(config)
        run_sync_with_locks(
            self.client,
            [self._storage.thread_lock_key(thread_id)],
            lambda: _run_sync(
                self.client,
                self._storage.put_writes(config, writes, task_id, task_path),
            ),
        )

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        items = await asyncio.to_thread(
            lambda: list(self.list(config, filter=filter, before=before, limit=limit))
        )
        for item in items:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await asyncio.to_thread(self.put, config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        normalized_thread_id = _text(thread_id, name="thread_id")
        run_sync_with_locks(
            self.client,
            [self._storage.thread_lock_key(normalized_thread_id)],
            lambda: _run_sync(
                self.client,
                self._storage.delete_thread(normalized_thread_id),
            ),
        )

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.to_thread(self.delete_thread, thread_id)


class AsyncFerricStoreSaver(_FerricStoreSaverBase):
    """Native-async LangGraph checkpointer backed by an AsyncFlowClient."""

    def __init__(
        self,
        client: _AsyncCommandClient,
        *,
        key_prefix: str = "langgraph:checkpoint",
        scan_count: int = 256,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(key_prefix=key_prefix, scan_count=scan_count, serde=serde)
        self.client = client

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        raise NotImplementedError("use aget_tuple() with AsyncFerricStoreSaver")

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        raise NotImplementedError("use alist() with AsyncFerricStoreSaver")
        yield

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        raise NotImplementedError("use aput() with AsyncFerricStoreSaver")

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        raise NotImplementedError("use aput_writes() with AsyncFerricStoreSaver")

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await _run_async(self.client, self._storage.get_tuple(config))

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        items = await _run_async(
            self.client,
            self._storage.list(
                config,
                filter=filter,
                before=before,
                limit=limit,
            ),
        )
        for item in items:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        del new_versions
        thread_id, _ = self._storage._identity(config)

        async def operation() -> RunnableConfig:
            return await _run_async(
                self.client,
                self._storage.put(config, checkpoint, metadata),
            )

        return await run_async_with_locks(
            self.client,
            [self._storage.thread_lock_key(thread_id)],
            operation,
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        thread_id, _ = self._storage._identity(config)

        async def operation() -> None:
            await _run_async(
                self.client,
                self._storage.put_writes(config, writes, task_id, task_path),
            )

        await run_async_with_locks(
            self.client,
            [self._storage.thread_lock_key(thread_id)],
            operation,
        )

    async def adelete_thread(self, thread_id: str) -> None:
        normalized_thread_id = _text(thread_id, name="thread_id")

        async def operation() -> None:
            await _run_async(
                self.client,
                self._storage.delete_thread(normalized_thread_id),
            )

        await run_async_with_locks(
            self.client,
            [self._storage.thread_lock_key(normalized_thread_id)],
            operation,
        )


__all__ = ["AsyncFerricStoreSaver", "FerricStoreSaver"]
