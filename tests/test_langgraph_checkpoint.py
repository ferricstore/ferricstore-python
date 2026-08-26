from __future__ import annotations

import asyncio
import fnmatch
import threading
from collections.abc import Sequence
from typing import Any, TypedDict

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import create_checkpoint, empty_checkpoint
from langgraph.graph import END, START, StateGraph

from ferricstore.langgraph import AsyncFerricStoreSaver, FerricStoreSaver


class MemoryCommandClient:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, Any]] = {}
        self.sets: dict[str, set[Any]] = {}
        self.zsets: dict[str, dict[Any, float]] = {}
        self.locks: dict[str, str] = {}
        self.commands: list[tuple[Any, ...]] = []
        self.lock = threading.Lock()

    def command(self, *args: Any) -> Any:
        with self.lock:
            self.commands.append(args)
            return self._command(*args)

    def _command(self, *args: Any) -> Any:
        name = str(args[0]).upper()
        if name == "LOCK":
            key = str(args[1])
            owner = str(args[2])
            if key in self.locks and self.locks[key] != owner:
                return None
            self.locks[key] = owner
            return "OK"
        if name == "UNLOCK":
            key = str(args[1])
            owner = str(args[2])
            if self.locks.get(key) != owner:
                return 0
            del self.locks[key]
            return 1
        if name == "EXTEND":
            key = str(args[1])
            owner = str(args[2])
            return int(self.locks.get(key) == owner)
        if name == "SADD":
            set_values = self.sets.setdefault(str(args[1]), set())
            before = len(set_values)
            set_values.update(args[2:])
            return len(set_values) - before
        if name == "SMEMBERS":
            return set(self.sets.get(str(args[1]), set()))
        if name == "SREM":
            values = self.sets.setdefault(str(args[1]), set())
            removed = 0
            for item in args[2:]:
                if item in values:
                    values.remove(item)
                    removed += 1
            return removed
        if name == "ZADD":
            values = self.zsets.setdefault(str(args[1]), {})
            added = 0
            for index in range(2, len(args), 2):
                score = float(args[index])
                member = args[index + 1]
                if member not in values:
                    added += 1
                values[member] = score
            return added
        if name == "ZREVRANGE":
            values = self.zsets.get(str(args[1]), {})
            start = int(args[2])
            stop = int(args[3])
            ordered = [
                member
                for member, _ in sorted(
                    values.items(),
                    key=lambda item: (item[1], item[0]),
                    reverse=True,
                )
            ]
            if stop < 0:
                stop += len(ordered)
            return ordered[start : stop + 1]
        if name == "ZRANGE":
            values = self.zsets.get(str(args[1]), {})
            start = int(args[2])
            stop = int(args[3])
            ordered = [
                member
                for member, _ in sorted(
                    values.items(),
                    key=lambda item: (item[1], item[0]),
                )
            ]
            if stop < 0:
                stop += len(ordered)
            return ordered[start : stop + 1]
        if name == "ZREM":
            values = self.zsets.get(str(args[1]), {})
            removed = 0
            for member in args[2:]:
                if member in values:
                    del values[member]
                    removed += 1
            if not values:
                self.zsets.pop(str(args[1]), None)
            return removed
        if name == "DEL":
            removed = 0
            for key in args[1:]:
                if str(key) in self.hashes:
                    del self.hashes[str(key)]
                    removed += 1
                if str(key) in self.sets:
                    del self.sets[str(key)]
                    removed += 1
                if str(key) in self.zsets:
                    del self.zsets[str(key)]
                    removed += 1
            return removed
        if name == "HSET":
            hash_values = self.hashes.setdefault(str(args[1]), {})
            added = 0
            for index in range(2, len(args), 2):
                field = str(args[index])
                if field not in hash_values:
                    added += 1
                hash_values[field] = args[index + 1]
            return added
        if name == "HSETNX":
            hash_values = self.hashes.setdefault(str(args[1]), {})
            field = str(args[2])
            if field in hash_values:
                return 0
            hash_values[field] = args[3]
            return 1
        if name == "HGET":
            return self.hashes.get(str(args[1]), {}).get(str(args[2]))
        if name == "HSCAN":
            key = str(args[1])
            pattern = str(args[4]) if len(args) >= 5 and str(args[3]).upper() == "MATCH" else "*"
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


def checkpoint(checkpoint_id: str, value: int) -> Any:
    base = empty_checkpoint()
    base["channel_values"] = {"value": value}
    return create_checkpoint(base, None, value, id=checkpoint_id)


def config(
    thread_id: str,
    *,
    checkpoint_ns: str | None = "",
    checkpoint_id: str | None = None,
) -> RunnableConfig:
    configurable: dict[str, Any] = {"thread_id": thread_id}
    if checkpoint_ns is not None:
        configurable["checkpoint_ns"] = checkpoint_ns
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


def pending_value(
    writes: Sequence[tuple[str, str, Any]] | None, channel: str
) -> Any:
    assert writes is not None
    return next(value for _, stored_channel, value in writes if stored_channel == channel)


def execute_plan(
    client: MemoryCommandClient,
    plan: Any,
    command: tuple[Any, ...],
) -> Any:
    while True:
        response = client.command(*command)
        try:
            command = plan.send(response)
        except StopIteration as stopped:
            return stopped.value


def normalized_lock_commands(
    commands: list[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    normalized: list[tuple[Any, ...]] = []
    for command in commands:
        if str(command[0]).upper() in {"LOCK", "UNLOCK"}:
            normalized.append((*command[:2], "<owner>", *command[3:]))
        else:
            normalized.append(command)
    return normalized


def test_sync_saver_round_trip_history_filters_and_pending_write_semantics() -> None:
    client = MemoryCommandClient()
    saver = FerricStoreSaver(client)
    first_id = "1f000000-0000-6000-8000-000000000001"
    second_id = "1f000000-0000-6000-8000-000000000002"

    first_config = saver.put(
        config("thread-1"),
        checkpoint(first_id, 1),
        {"source": "input", "step": 1},
        {},
    )
    saver.put_writes(first_config, [("answer", "first")], "task-1", "node")
    saver.put_writes(first_config, [("answer", "ignored")], "task-1", "node")
    saver.put_writes(first_config, [("__error__", "temporary")], "task-1", "node")
    saver.put_writes(first_config, [("__error__", "final")], "task-1", "node")

    second_config = saver.put(
        first_config,
        checkpoint(second_id, 2),
        {"source": "loop", "step": 2},
        {},
    )

    latest = saver.get_tuple(config("thread-1"))
    assert latest is not None
    assert latest.config == second_config
    assert latest.checkpoint["channel_values"] == {"value": 2}
    assert latest.parent_config == first_config

    first = saver.get_tuple(first_config)
    assert first is not None
    assert pending_value(first.pending_writes, "answer") == "first"
    assert pending_value(first.pending_writes, "__error__") == "final"

    history = list(saver.list(config("thread-1", checkpoint_ns=None)))
    assert [item.config["configurable"]["checkpoint_id"] for item in history] == [
        second_id,
        first_id,
    ]
    assert [item.config for item in saver.list(config("thread-1"), limit=1)] == [
        second_config
    ]
    assert [item.config for item in saver.list(config("thread-1"), before=second_config)] == [
        first_config
    ]
    assert [
        item.config
        for item in saver.list(config("thread-1"), filter={"source": "input"})
    ] == [first_config]
    assert [item.config for item in saver.list(None)] == [second_config, first_config]


def test_sync_saver_keeps_checkpoint_namespaces_separate() -> None:
    saver = FerricStoreSaver(MemoryCommandClient(), key_prefix="test:langgraph")
    default_id = "1f000000-0000-6000-8000-000000000001"
    child_id = "1f000000-0000-6000-8000-000000000003"
    saver.put(config("thread-1"), checkpoint(default_id, 1), {}, {})
    saver.put(config("thread-1", checkpoint_ns="child"), checkpoint(child_id, 3), {}, {})

    assert saver.get_tuple(config("thread-1")) is not None
    assert saver.get_tuple(config("thread-1", checkpoint_ns="child")) is not None
    all_namespaces = list(saver.list(config("thread-1", checkpoint_ns=None)))
    assert {item.config["configurable"]["checkpoint_ns"] for item in all_namespaces} == {
        "",
        "child",
    }
    saver.delete_thread("thread-1")
    assert saver.get_tuple(config("thread-1")) is None
    assert list(saver.list(config("thread-1", checkpoint_ns=None))) == []


def test_latest_checkpoint_is_monotonic_when_older_put_commits_last() -> None:
    client = MemoryCommandClient()
    saver = FerricStoreSaver(client)
    older_id = "1f000000-0000-6000-8000-000000000001"
    newer_id = "1f000000-0000-6000-8000-000000000002"
    older = saver._storage.put(config("concurrent"), checkpoint(older_id, 1), {})
    newer = saver._storage.put(config("concurrent"), checkpoint(newer_id, 2), {})

    older_write = next(older)
    newer_write = next(newer)
    execute_plan(client, newer, newer_write)
    execute_plan(client, older, older_write)

    latest = saver.get_tuple(config("concurrent"))
    assert latest is not None
    assert latest.config["configurable"]["checkpoint_id"] == newer_id
    assert latest.checkpoint["channel_values"] == {"value": 2}
    assert any(command[0] == "ZREVRANGE" for command in client.commands)
    assert not any(
        command[0] == "HSCAN" and command[4] == "checkpoint:*"
        for command in client.commands
    )


def test_delete_thread_and_put_cannot_create_hidden_live_checkpoint() -> None:
    class BlockingDeleteClient(MemoryCommandClient):
        def __init__(self) -> None:
            super().__init__()
            self.block_delete = False
            self.delete_entered = threading.Event()
            self.release_delete = threading.Event()

        def command(self, *args: Any) -> Any:
            if self.block_delete and str(args[0]).upper() == "DEL":
                self.block_delete = False
                self.delete_entered.set()
                assert self.release_delete.wait(timeout=5)
            return super().command(*args)

    client = BlockingDeleteClient()
    saver = FerricStoreSaver(client)
    initial_id = "1f000000-0000-6000-8000-000000000001"
    replacement_id = "1f000000-0000-6000-8000-000000000002"
    saver.put(config("delete-race"), checkpoint(initial_id, 1), {}, {})
    client.block_delete = True
    replacement_configs: list[RunnableConfig] = []
    errors: list[BaseException] = []

    def delete_thread() -> None:
        try:
            saver.delete_thread("delete-race")
        except BaseException as exc:  # pragma: no cover - assertion aid
            errors.append(exc)

    def replace_checkpoint() -> None:
        try:
            replacement_configs.append(
                saver.put(
                    config("delete-race"),
                    checkpoint(replacement_id, 2),
                    {},
                    {},
                )
            )
        except BaseException as exc:  # pragma: no cover - assertion aid
            errors.append(exc)

    deleting = threading.Thread(target=delete_thread)
    replacing = threading.Thread(target=replace_checkpoint)
    deleting.start()
    assert client.delete_entered.wait(timeout=5)
    replacing.start()
    client.release_delete.set()
    deleting.join(timeout=5)
    replacing.join(timeout=5)

    assert not errors
    assert not deleting.is_alive()
    assert not replacing.is_alive()
    assert len(replacement_configs) == 1
    assert saver.get_tuple(config("delete-race")) is not None
    assert [item.config for item in saver.list(None)] == [replacement_configs[0]]
    thread_key = saver._storage.thread_key("delete-race", "")
    assert client.sets[saver._storage.thread_catalog_key("delete-race")] == {
        thread_key
    }
    locator = saver._storage.checkpoint_locator(replacement_id, thread_key)
    assert client.zsets[saver._storage.thread_locator_catalog_key("delete-race")] == {
        locator: 0.0
    }
    assert client.zsets[saver._storage.catalog_key] == {locator: 0.0}
    assert client.zsets[saver._storage.checkpoint_index_key(thread_key)] == {
        replacement_id: 0.0
    }


def test_delete_thread_uses_its_thread_catalog_instead_of_global_scan() -> None:
    client = MemoryCommandClient()
    saver = FerricStoreSaver(client)
    checkpoint_id = "1f000000-0000-6000-8000-000000000001"
    for index in range(20):
        saver.put(
            config(f"thread-{index}"),
            checkpoint(checkpoint_id, index),
            {},
            {},
        )

    client.commands.clear()
    saver.delete_thread("thread-7")

    smembers = [command for command in client.commands if command[0] == "SMEMBERS"]
    assert smembers == [("SMEMBERS", saver._storage.thread_catalog_key("thread-7"))]
    assert any(
        command[:2]
        == ("ZRANGE", saver._storage.thread_locator_catalog_key("thread-7"))
        for command in client.commands
    )
    assert not any(command[0] == "HGET" for command in client.commands)
    assert saver.get_tuple(config("thread-7")) is None
    assert saver.get_tuple(config("thread-8")) is not None


def test_checkpoint_list_limit_bounds_index_reads() -> None:
    client = MemoryCommandClient()
    saver = FerricStoreSaver(client)
    thread_config = config("bounded-history")
    for index in range(20):
        checkpoint_id = f"1f000000-0000-6000-8000-{index:012d}"
        saver.put(thread_config, checkpoint(checkpoint_id, index), {}, {})

    client.commands.clear()
    history = list(saver.list(thread_config, limit=1))

    assert len(history) == 1
    ranges = [command for command in client.commands if command[0] == "ZREVRANGE"]
    assert len(ranges) == 1
    assert ranges[0][2:] == (0, 0)
    assert not any(
        command[0] == "HSCAN" and command[4] == "checkpoint:*"
        for command in client.commands
    )


def test_interrupted_checkpoint_put_never_publishes_a_partial_record() -> None:
    class FailingRecordClient(MemoryCommandClient):
        def __init__(self) -> None:
            super().__init__()
            self.fail_record = False

        def command(self, *args: Any) -> Any:
            if self.fail_record and str(args[0]).upper() == "HSET":
                self.fail_record = False
                raise ConnectionError("checkpoint record write failed")
            return super().command(*args)

    client = FailingRecordClient()
    saver = FerricStoreSaver(client)
    older_id = "1f000000-0000-6000-8000-000000000001"
    newer_id = "1f000000-0000-6000-8000-000000000002"
    older_config = saver.put(config("failure-atomic"), checkpoint(older_id, 1), {}, {})

    client.fail_record = True
    with pytest.raises(ConnectionError, match="record write failed"):
        saver.put(config("failure-atomic"), checkpoint(newer_id, 2), {}, {})

    latest = saver.get_tuple(config("failure-atomic"))
    assert latest is not None
    assert latest.config == older_config
    assert saver.get_tuple(config("failure-atomic", checkpoint_id=newer_id)) is None
    assert [item.config for item in saver.list(config("failure-atomic"), limit=1)] == [
        older_config
    ]
    assert [item.config for item in saver.list(None)] == [older_config]

    repaired = saver.put(config("failure-atomic"), checkpoint(newer_id, 2), {}, {})
    assert saver.get_tuple(config("failure-atomic")).config == repaired  # type: ignore[union-attr]


def test_global_checkpoint_limit_uses_ordered_locator_index() -> None:
    client = MemoryCommandClient()
    saver = FerricStoreSaver(client, scan_count=4)
    for index in range(40):
        checkpoint_id = f"1f000000-0000-6000-8000-{index:012d}"
        saver.put(config(f"global-{index:02d}"), checkpoint(checkpoint_id, index), {}, {})

    client.commands.clear()
    history = list(saver.list(None, limit=1))

    assert len(history) == 1
    assert history[0].config["configurable"]["thread_id"] == "global-39"
    assert [command[:4] for command in client.commands if command[0] == "ZREVRANGE"] == [
        ("ZREVRANGE", saver._storage.catalog_key, 0, 3)
    ]
    assert sum(command[0] == "HGET" for command in client.commands) == 1
    assert not any(command[0] == "SMEMBERS" for command in client.commands)


def test_saver_rejects_invalid_storage_options() -> None:
    client = MemoryCommandClient()
    with pytest.raises(ValueError, match="other than ':'"):
        FerricStoreSaver(client, key_prefix=":::")
    with pytest.raises(ValueError, match="scan_count must be positive"):
        FerricStoreSaver(client, scan_count=0)


class CounterState(TypedDict):
    value: int


def build_counter_graph(saver: Any) -> Any:
    builder = StateGraph(CounterState)
    builder.add_node("increment", lambda state: {"value": state["value"] + 1})
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    return builder.compile(checkpointer=saver)


def test_sync_saver_runs_in_a_compiled_langgraph() -> None:
    saver = FerricStoreSaver(MemoryCommandClient())
    graph = build_counter_graph(saver)
    thread_config = config("counter-1")

    assert graph.invoke({"value": 4}, thread_config) == {"value": 5}
    assert graph.get_state(thread_config).values == {"value": 5}
    assert len(list(saver.list(thread_config))) >= 2


def test_sync_saver_persists_langchain_create_agent_messages() -> None:
    saver = FerricStoreSaver(MemoryCommandClient())
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(content="Hello, Bob."),
            AIMessage(content="Your name is Bob."),
        ]
    )
    agent = create_agent(model=model, tools=[], checkpointer=saver)
    thread_config = config("langchain-agent-1")

    first = agent.invoke(
        {"messages": [{"role": "user", "content": "My name is Bob."}]},
        thread_config,
    )
    second = agent.invoke(
        {"messages": [{"role": "user", "content": "What is my name?"}]},
        thread_config,
    )

    assert [message.content for message in first["messages"]] == [
        "My name is Bob.",
        "Hello, Bob.",
    ]
    assert [message.content for message in second["messages"]] == [
        "My name is Bob.",
        "Hello, Bob.",
        "What is my name?",
        "Your name is Bob.",
    ]
    assert saver.get_tuple(thread_config) is not None


def test_async_saver_round_trip_and_compiled_graph() -> None:
    async def run() -> None:
        saver = AsyncFerricStoreSaver(AsyncMemoryCommandClient())
        graph = build_counter_graph(saver)
        thread_config = config("async-counter-1")

        assert await graph.ainvoke({"value": 8}, thread_config) == {"value": 9}
        assert (await graph.aget_state(thread_config)).values == {"value": 9}
        history = [item async for item in saver.alist(thread_config)]
        assert len(history) >= 2
        await saver.adelete_thread("async-counter-1")
        assert await saver.aget_tuple(thread_config) is None

    asyncio.run(run())


def test_async_saver_persists_langchain_create_agent_messages() -> None:
    async def run() -> None:
        saver = AsyncFerricStoreSaver(AsyncMemoryCommandClient())
        model = FakeMessagesListChatModel(
            responses=[AIMessage(content="Persisted asynchronously.")]
        )
        agent = create_agent(model=model, tools=[], checkpointer=saver)
        thread_config = config("async-langchain-agent-1")

        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": "Remember this."}]},
            thread_config,
        )

        assert [message.content for message in result["messages"]] == [
            "Remember this.",
            "Persisted asynchronously.",
        ]
        assert await saver.aget_tuple(thread_config) is not None

    asyncio.run(run())


def test_sync_saver_supports_async_langgraph_api() -> None:
    async def run() -> None:
        saver = FerricStoreSaver(MemoryCommandClient())
        graph = build_counter_graph(saver)
        thread_config = config("sync-saver-async-graph")
        assert await graph.ainvoke({"value": 2}, thread_config) == {"value": 3}

    asyncio.run(run())


def test_sync_and_async_savers_execute_identical_storage_plans() -> None:
    sync_client = MemoryCommandClient()
    async_client = AsyncMemoryCommandClient()
    sync_saver = FerricStoreSaver(sync_client, key_prefix="parity:langgraph")
    async_saver = AsyncFerricStoreSaver(async_client, key_prefix="parity:langgraph")
    checkpoint_id = "1f000000-0000-6000-8000-000000000004"
    initial_config = config("parity-thread")
    stored_checkpoint = checkpoint(checkpoint_id, 4)
    metadata = {"source": "input", "step": 4}

    sync_config = sync_saver.put(initial_config, stored_checkpoint, metadata, {})
    sync_saver.put_writes(sync_config, [("answer", "four")], "task-4", "node")
    sync_tuple = sync_saver.get_tuple(initial_config)
    sync_history = list(sync_saver.list(initial_config))
    sync_saver.delete_thread("parity-thread")

    async def run_async() -> tuple[RunnableConfig, Any, list[Any]]:
        async_config = await async_saver.aput(
            initial_config,
            stored_checkpoint,
            metadata,
            {},
        )
        await async_saver.aput_writes(
            async_config,
            [("answer", "four")],
            "task-4",
            "node",
        )
        async_tuple = await async_saver.aget_tuple(initial_config)
        async_history = [item async for item in async_saver.alist(initial_config)]
        await async_saver.adelete_thread("parity-thread")
        return async_config, async_tuple, async_history

    async_config, async_tuple, async_history = asyncio.run(run_async())

    assert async_config == sync_config
    assert async_tuple == sync_tuple
    assert async_history == sync_history
    assert normalized_lock_commands(async_client.sync.commands) == normalized_lock_commands(
        sync_client.commands
    )
