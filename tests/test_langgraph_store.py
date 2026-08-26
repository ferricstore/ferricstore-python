from __future__ import annotations

import asyncio
import fnmatch
import threading
from typing import Any

import pytest
from langchain.agents import create_agent
from langchain.tools import ToolRuntime, tool
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.base import GetOp

from ferricstore.langgraph import AsyncFerricStoreStore, FerricStoreStore


class MemoryPipeline:
    def __init__(self, client: MemoryCommandClient) -> None:
        self.client = client
        self.commands: list[tuple[Any, ...]] = []

    def command(self, *args: Any) -> MemoryPipeline:
        self.commands.append(args)
        return self

    def execute(self) -> list[Any]:
        self.client.pipeline_batches.append(tuple(self.commands))
        return [self.client.command(*command) for command in self.commands]


class MemoryCommandClient:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, Any]] = {}
        self.sets: dict[str, set[Any]] = {}
        self.zsets: dict[str, dict[Any, float]] = {}
        self.locks: dict[str, str] = {}
        self.commands: list[tuple[Any, ...]] = []
        self.pipeline_batches: list[tuple[tuple[Any, ...], ...]] = []
        self.lock = threading.Lock()

    def pipeline(self) -> MemoryPipeline:
        return MemoryPipeline(self)

    def command(self, *args: Any) -> Any:
        with self.lock:
            self.commands.append(args)
            return self._command(*args)

    def _command(self, *args: Any) -> Any:
        name = str(args[0]).upper()
        key = str(args[1])
        if name == "LOCK":
            owner = str(args[2])
            if key in self.locks and self.locks[key] != owner:
                return None
            self.locks[key] = owner
            return "OK"
        if name == "UNLOCK":
            owner = str(args[2])
            if self.locks.get(key) != owner:
                return 0
            del self.locks[key]
            return 1
        if name == "EXTEND":
            owner = str(args[2])
            return int(self.locks.get(key) == owner)
        if name == "SADD":
            values = self.sets.setdefault(key, set())
            before = len(values)
            values.update(args[2:])
            return len(values) - before
        if name == "SMEMBERS":
            return set(self.sets.get(key, set()))
        if name == "ZADD":
            values = self.zsets.setdefault(key, {})
            added = 0
            for index in range(2, len(args), 2):
                score = float(args[index])
                member = args[index + 1]
                if member not in values:
                    added += 1
                values[member] = score
            return added
        if name == "ZREM":
            values = self.zsets.get(key, {})
            removed = 0
            for member in args[2:]:
                if member in values:
                    del values[member]
                    removed += 1
            if not values:
                self.zsets.pop(key, None)
            return removed
        if name == "ZRANGE":
            start = int(args[2])
            stop = int(args[3])
            ordered = [
                member
                for member, _ in sorted(
                    self.zsets.get(key, {}).items(),
                    key=lambda item: (item[1], item[0]),
                )
            ]
            if stop < 0:
                stop += len(ordered)
            return ordered[start : stop + 1]
        if name == "HSET":
            values = self.hashes.setdefault(key, {})
            added = 0
            for index in range(2, len(args), 2):
                field = str(args[index])
                if field not in values:
                    added += 1
                values[field] = args[index + 1]
            return added
        if name == "HSETNX":
            values = self.hashes.setdefault(key, {})
            field = str(args[2])
            if field in values:
                return 0
            values[field] = args[3]
            return 1
        if name == "HGET":
            return self.hashes.get(key, {}).get(str(args[2]))
        if name == "HDEL":
            values = self.hashes.get(key, {})
            removed = 0
            for field in args[2:]:
                if str(field) in values:
                    del values[str(field)]
                    removed += 1
            if not values:
                self.hashes.pop(key, None)
            return removed
        if name == "HSCAN":
            pattern = str(args[4])
            items: list[Any] = []
            for field, value in self.hashes.get(key, {}).items():
                if fnmatch.fnmatchcase(field, pattern):
                    items.extend((field.encode(), value))
            return [b"0", items]
        raise AssertionError(f"unexpected FerricStore command: {args!r}")


class AsyncMemoryCommandClient:
    def __init__(self) -> None:
        self.sync = MemoryCommandClient()

    async def command(self, *args: Any) -> Any:
        return self.sync.command(*args)


def test_store_round_trip_update_delete_and_json_validation() -> None:
    client = MemoryCommandClient()
    store = FerricStoreStore(client)
    namespace = ("users", "user-1", "memories")

    store.put(namespace, "profile", {"name": "Bob", "score": 4})
    first = store.get(namespace, "profile")
    assert first is not None
    assert first.namespace == namespace
    assert first.key == "profile"
    assert first.value == {"name": "Bob", "score": 4}

    store.put(namespace, "profile", {"name": "Bob", "score": 5})
    updated = store.get(namespace, "profile")
    assert updated is not None
    assert updated.value["score"] == 5
    assert updated.created_at == first.created_at
    assert updated.updated_at >= first.updated_at
    assert store.list_namespaces() == [namespace]

    store.delete(namespace, "profile")
    assert store.get(namespace, "profile") is None
    assert store.list_namespaces() == []

    command_count = len(client.commands)
    with pytest.raises(TypeError, match="JSON serializable"):
        store.put(namespace, "invalid", {"value": object()})
    assert len(client.commands) == command_count
    with pytest.raises(TypeError, match="JSON object keys must be strings"):
        store.put(namespace, "nested-invalid", {"nested": {1: "value"}})
    assert len(client.commands) == command_count
    with pytest.raises(NotImplementedError, match="TTL"):
        store.put(namespace, "ttl", {"value": 1}, ttl=5)


def test_store_search_filters_namespace_prefix_and_pagination() -> None:
    store = FerricStoreStore(MemoryCommandClient())
    user_one = ("users", "user-1", "memories")
    user_two = ("users", "user-2", "memories")
    store.put(
        user_one,
        "a",
        {"kind": "preference", "score": 3, "profile": {"tier": "free"}},
    )
    store.put(
        user_one,
        "b",
        {"kind": "preference", "score": 8, "profile": {"tier": "pro"}},
    )
    store.put(user_one, "c", {"kind": "fact", "score": 10})
    store.put(user_two, "d", {"kind": "preference", "score": 9})

    results = store.search(
        ("users", "user-1"),
        filter={
            "kind": "preference",
            "score": {"$gte": 5, "$lt": 10},
            "profile": {"tier": "pro"},
        },
    )
    assert [(item.namespace, item.key, item.score) for item in results] == [
        (user_one, "b", None)
    ]

    paged = store.search(("users",), query="ignored without an index", limit=2, offset=1)
    assert [(item.namespace, item.key) for item in paged] == [
        (user_one, "b"),
        (user_one, "c"),
    ]


def test_store_lists_namespaces_with_wildcards_depth_and_suffix() -> None:
    store = FerricStoreStore(MemoryCommandClient())
    namespaces = [
        ("users", "user-1", "memories"),
        ("users", "user-1", "preferences"),
        ("users", "user-2", "memories"),
        ("systems", "v1", "cache"),
    ]
    for index, namespace in enumerate(namespaces):
        store.put(namespace, str(index), {"value": index})
        store.put(namespace, str(index), {"value": index})

    assert store.list_namespaces(prefix=("users", "*")) == namespaces[:3]
    assert store.list_namespaces(suffix=("memories",)) == [
        namespaces[0],
        namespaces[2],
    ]
    assert store.list_namespaces(prefix=("users",), max_depth=2) == [
        ("users", "user-1"),
        ("users", "user-2"),
    ]


def test_store_delete_removes_item_hashes_and_every_catalog_entry() -> None:
    client = MemoryCommandClient()
    store = FerricStoreStore(client)

    for index in range(100):
        namespace = ("ephemeral", f"user-{index}", "memories")
        store.put(namespace, "profile", {"value": index})
        store.delete(namespace, "profile")

    assert store.list_namespaces() == []
    assert client.hashes == {}
    assert client.zsets == {}


def test_store_put_and_delete_are_serialized_without_index_tombstones() -> None:
    class BlockingDeleteClient(MemoryCommandClient):
        def __init__(self) -> None:
            super().__init__()
            self.block_delete = False
            self.delete_entered = threading.Event()
            self.release_delete = threading.Event()

        def command(self, *args: Any) -> Any:
            if self.block_delete and str(args[0]).upper() == "ZREM":
                self.block_delete = False
                self.delete_entered.set()
                assert self.release_delete.wait(timeout=5)
            return super().command(*args)

    client = BlockingDeleteClient()
    store = FerricStoreStore(client)
    namespace = ("users", "race", "memories")
    store.put(namespace, "profile", {"value": 1})
    client.block_delete = True
    errors: list[BaseException] = []

    def delete_item() -> None:
        try:
            store.delete(namespace, "profile")
        except BaseException as exc:  # pragma: no cover - assertion aid
            errors.append(exc)

    def replace_item() -> None:
        try:
            store.put(namespace, "profile", {"value": 2})
        except BaseException as exc:  # pragma: no cover - assertion aid
            errors.append(exc)

    deleting = threading.Thread(target=delete_item)
    replacing = threading.Thread(target=replace_item)
    deleting.start()
    assert client.delete_entered.wait(timeout=5)
    replacing.start()
    client.release_delete.set()
    deleting.join(timeout=5)
    replacing.join(timeout=5)

    assert not errors
    assert not deleting.is_alive()
    assert not replacing.is_alive()
    item = store.get(namespace, "profile")
    assert item is not None
    assert item.value == {"value": 2}
    assert len(client.hashes) == 1
    assert len(client.zsets) == len(namespace) + 1
    assert all(len(members) == 1 for members in client.zsets.values())


def test_interrupted_store_mutations_never_hide_live_items() -> None:
    class FailingMutationClient(MemoryCommandClient):
        def __init__(self) -> None:
            super().__init__()
            self.fail_command: str | None = None

        def command(self, *args: Any) -> Any:
            name = str(args[0]).upper()
            if self.fail_command == name:
                self.fail_command = None
                raise ConnectionError(f"{name} mutation failed")
            return super().command(*args)

    client = FailingMutationClient()
    store = FerricStoreStore(client)
    namespace = ("users", "failure-atomic")

    client.fail_command = "HSET"
    with pytest.raises(ConnectionError, match="HSET mutation failed"):
        store.put(namespace, "profile", {"version": 1})
    assert store.get(namespace, "profile") is None
    assert store.search(("users",)) == []
    assert store.list_namespaces() == []

    store.put(namespace, "profile", {"version": 1})
    assert store.get(namespace, "profile") is not None
    assert [item.key for item in store.search(("users",))] == ["profile"]

    client.fail_command = "ZREM"
    with pytest.raises(ConnectionError, match="ZREM mutation failed"):
        store.delete(namespace, "profile")
    assert store.get(namespace, "profile") is None
    assert store.search(("users",)) == []
    assert store.list_namespaces() == []

    store.delete(namespace, "profile")
    assert client.hashes == {}
    assert client.zsets == {}


def test_store_search_and_namespace_limits_stop_on_ordered_index_pages() -> None:
    client = MemoryCommandClient()
    store = FerricStoreStore(client, scan_count=3)
    for index in range(20):
        store.put(("users", f"user-{index:02d}"), "profile", {"value": index})

    client.commands.clear()
    results = store.search(("users",), limit=1)
    assert [(item.namespace, item.key) for item in results] == [
        (("users", "user-00"), "profile")
    ]
    assert sum(command[0] == "ZRANGE" for command in client.commands) == 1
    assert sum(command[0] == "HGET" for command in client.commands) == 3
    assert not any(command[0] in {"SMEMBERS", "HSCAN"} for command in client.commands)

    client.commands.clear()
    assert store.list_namespaces(prefix=("users",), limit=1) == [
        ("users", "user-00")
    ]
    assert sum(command[0] == "ZRANGE" for command in client.commands) == 1
    assert sum(command[0] == "HGET" for command in client.commands) == 3


def test_store_ordered_index_pages_through_filters_and_duplicate_namespaces() -> None:
    store = FerricStoreStore(MemoryCommandClient(), scan_count=2)
    for index in range(8):
        namespace = ("users", f"user-{index // 2}")
        store.put(
            namespace,
            f"item-{index}",
            {"match": index == 7, "value": index},
        )

    matches = store.search(("users",), filter={"match": True}, limit=1)
    assert [(item.namespace, item.key) for item in matches] == [
        (("users", "user-3"), "item-7")
    ]
    assert store.list_namespaces(prefix=("users",), limit=2, offset=1) == [
        ("users", "user-1"),
        ("users", "user-2"),
    ]


def test_store_batches_gets_through_client_pipeline() -> None:
    client = MemoryCommandClient()
    store = FerricStoreStore(client)
    namespace = ("users", "batch")
    store.put(namespace, "a", {"value": 1})
    store.put(namespace, "b", {"value": 2})
    client.pipeline_batches.clear()

    results = store.batch([GetOp(namespace, "a"), GetOp(namespace, "b")])

    assert [item.value if item is not None else None for item in results] == [
        {"value": 1},
        {"value": 2},
    ]
    assert len(client.pipeline_batches) == 1
    assert len(client.pipeline_batches[0]) == 2


def test_sync_and_native_async_store_apis() -> None:
    sync_store = FerricStoreStore(MemoryCommandClient())
    async_store = AsyncFerricStoreStore(AsyncMemoryCommandClient())
    namespace = ("users", "async")

    async def run() -> None:
        await sync_store.aput(namespace, "sync", {"mode": "worker-thread"})
        sync_item = await sync_store.aget(namespace, "sync")
        assert sync_item is not None
        assert sync_item.value == {"mode": "worker-thread"}

        await async_store.aput(namespace, "native", {"mode": "native-async"})
        native_item = await async_store.aget(namespace, "native")
        assert native_item is not None
        assert native_item.value == {"mode": "native-async"}
        assert [item.key for item in await async_store.asearch(("users",))] == [
            "native"
        ]
        assert await async_store.alist_namespaces(prefix=("users",)) == [namespace]
        await async_store.adelete(namespace, "native")
        assert await async_store.aget(namespace, "native") is None

    asyncio.run(run())
    with pytest.raises(NotImplementedError, match="asynchronous"):
        async_store.get(namespace, "native")


class ToolCallingFakeModel(FakeMessagesListChatModel):
    def bind_tools(self, tools: Any, **kwargs: Any) -> ToolCallingFakeModel:
        del tools, kwargs
        return self


def test_langchain_agent_shares_store_memory_across_threads() -> None:
    store = FerricStoreStore(MemoryCommandClient())

    @tool
    def remember_user(user_id: str, name: str, runtime: ToolRuntime) -> str:
        """Remember a user's name."""
        runtime.store.put(("users", user_id), "profile", {"name": name})
        return "saved"

    @tool
    def recall_user(user_id: str, runtime: ToolRuntime) -> str:
        """Recall a user's name."""
        item = runtime.store.get(("users", user_id), "profile")
        return str(item.value["name"]) if item is not None else "unknown"

    model = ToolCallingFakeModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "remember_user",
                        "args": {"user_id": "user-1", "name": "Bob"},
                        "id": "remember-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Saved."),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "recall_user",
                        "args": {"user_id": "user-1"},
                        "id": "recall-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Recalled."),
        ]
    )
    agent = create_agent(
        model=model,
        tools=[remember_user, recall_user],
        checkpointer=InMemorySaver(),
        store=store,
    )

    agent.invoke(
        {"messages": [{"role": "user", "content": "Remember me."}]},
        {"configurable": {"thread_id": "thread-1"}},
    )
    result = agent.invoke(
        {"messages": [{"role": "user", "content": "Who am I?"}]},
        {"configurable": {"thread_id": "thread-2"}},
    )

    tool_messages = [
        message for message in result["messages"] if isinstance(message, ToolMessage)
    ]
    assert [message.content for message in tool_messages] == ["Bob"]
    stored = store.get(("users", "user-1"), "profile")
    assert stored is not None
    assert stored.value == {"name": "Bob"}
