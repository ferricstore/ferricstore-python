from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any, TypedDict

import pytest
from langchain.agents import create_agent
from langchain.tools import ToolRuntime, tool
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from ferricstore import AsyncFlowClient, FlowClient, JsonCodec, WorkflowClient, complete
from ferricstore.langgraph import (
    AsyncFerricStoreStore,
    FerricStoreSaver,
    FerricStoreStore,
    LangGraphFlow,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
)


class ToolCallingFakeModel(FakeMessagesListChatModel):
    def bind_tools(self, tools: Any, **kwargs: Any) -> ToolCallingFakeModel:
        del tools, kwargs
        return self


class State(TypedDict):
    value: int


def test_compiled_langgraph_persists_and_deletes_a_thread() -> None:
    suffix = uuid.uuid4().hex
    thread_id = f"langgraph-integration-{suffix}"
    client = FlowClient.from_url(
        os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388"),
        codec=JsonCodec(),
    )
    key_prefix = f"test:langgraph:{suffix}"
    saver = FerricStoreSaver(client, key_prefix=key_prefix)
    builder = StateGraph(State)
    builder.add_node("increment", lambda state: {"value": state["value"] + 1})
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    graph = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": thread_id}}

    try:
        assert graph.invoke({"value": 10}, config) == {"value": 11}
        assert graph.get_state(config).values == {"value": 11}
        assert len(list(saver.list(config))) >= 2
        global_latest = list(saver.list(None, limit=1))
        assert len(global_latest) == 1
        assert global_latest[0].config["configurable"]["thread_id"] == thread_id
        assert client.command("ZCARD", saver._storage.catalog_key) >= 2
        saver.delete_thread(thread_id)
        assert saver.get_tuple(config) is None
        assert client.command("ZCARD", saver._storage.catalog_key) == 0
        assert client.command("EXISTS", saver._storage.catalog_key) == 0
    finally:
        saver.delete_thread(thread_id)
        client.delete(saver._storage.catalog_key)
        client.close()


def test_langchain_create_agent_persists_messages_in_ferricstore() -> None:
    suffix = uuid.uuid4().hex
    thread_id = f"langchain-integration-{suffix}"
    key_prefix = f"test:langchain:{suffix}"
    client = FlowClient.from_url(
        os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388"),
        codec=JsonCodec(),
    )
    saver = FerricStoreSaver(client, key_prefix=key_prefix)
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(content="Hello, Bob."),
            AIMessage(content="Your name is Bob."),
        ]
    )
    agent = create_agent(model=model, tools=[], checkpointer=saver)
    config = {"configurable": {"thread_id": thread_id}}

    try:
        agent.invoke(
            {"messages": [{"role": "user", "content": "My name is Bob."}]},
            config,
        )
        result = agent.invoke(
            {"messages": [{"role": "user", "content": "What is my name?"}]},
            config,
        )

        assert [message.content for message in result["messages"]] == [
            "My name is Bob.",
            "Hello, Bob.",
            "What is my name?",
            "Your name is Bob.",
        ]
        assert saver.get_tuple(config) is not None
    finally:
        saver.delete_thread(thread_id)
        client.delete(saver._storage.catalog_key)
        client.close()


def test_langchain_agent_shares_ferricstore_memory_across_threads() -> None:
    suffix = uuid.uuid4().hex
    user_id = f"user-{suffix}"
    namespace = ("users", user_id)
    saver_prefix = f"test:langchain:checkpoint:{suffix}"
    store_prefix = f"test:langchain:store:{suffix}"
    client = FlowClient.from_url(
        os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388"),
        codec=JsonCodec(),
    )
    saver = FerricStoreSaver(client, key_prefix=saver_prefix)
    store = FerricStoreStore(client, key_prefix=store_prefix)

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
                        "args": {"user_id": user_id, "name": "Bob"},
                        "id": "remember-live",
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
                        "args": {"user_id": user_id},
                        "id": "recall-live",
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
        checkpointer=saver,
        store=store,
    )
    first_config = {"configurable": {"thread_id": f"thread-a-{suffix}"}}
    second_config = {"configurable": {"thread_id": f"thread-b-{suffix}"}}

    try:
        agent.invoke(
            {"messages": [{"role": "user", "content": "Remember me."}]},
            first_config,
        )
        result = agent.invoke(
            {"messages": [{"role": "user", "content": "Who am I?"}]},
            second_config,
        )

        tool_messages = [
            message
            for message in result["messages"]
            if isinstance(message, ToolMessage)
        ]
        assert [message.content for message in tool_messages] == ["Bob"]
        stored = store.get(namespace, "profile")
        assert stored is not None
        assert stored.value == {"name": "Bob"}
        assert saver.get_tuple(first_config) is not None
        assert saver.get_tuple(second_config) is not None
    finally:
        saver.delete_thread(first_config["configurable"]["thread_id"])
        saver.delete_thread(second_config["configurable"]["thread_id"])
        client.delete(
            store._storage.namespace_key(namespace),
            store._storage.catalog_key,
            store._storage.prefix_catalog_key(namespace[:1]),
            store._storage.prefix_catalog_key(namespace),
            saver._storage.catalog_key,
        )
        client.close()


def test_async_langgraph_store_uses_native_ferricstore_client() -> None:
    suffix = uuid.uuid4().hex
    key_prefix = f"test:langchain:async-store:{suffix}"
    namespace = ("users", f"async-{suffix}", "memories")

    async def run() -> None:
        client = AsyncFlowClient.from_url(
            os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388"),
            codec=JsonCodec(),
        )
        store = AsyncFerricStoreStore(client, key_prefix=key_prefix)
        try:
            await store.aput(namespace, "profile", {"name": "Async Bob", "score": 9})
            item = await store.aget(namespace, "profile")
            assert item is not None
            assert item.value == {"name": "Async Bob", "score": 9}
            matches = await store.asearch(
                namespace[:2],
                filter={"score": {"$gte": 8}},
            )
            assert [(match.namespace, match.key) for match in matches] == [
                (namespace, "profile")
            ]
            assert await store.alist_namespaces(prefix=namespace[:1]) == [namespace]
            await store.adelete(namespace, "profile")
            assert await store.aget(namespace, "profile") is None
        finally:
            await client.delete(
                store._storage.namespace_key(namespace),
                store._storage.catalog_key,
                store._storage.prefix_catalog_key(namespace[:1]),
                store._storage.prefix_catalog_key(namespace[:2]),
                store._storage.prefix_catalog_key(namespace),
            )
            await client.close()

    asyncio.run(run())


def test_langgraph_store_pages_ordered_indexes_and_cleans_deleted_data() -> None:
    suffix = uuid.uuid4().hex
    key_prefix = f"test:langchain:store-index:{suffix}"
    root = (f"tenant-{suffix}",)
    namespaces = [(*root, f"user-{index:02d}") for index in range(4)]
    client = FlowClient.from_url(
        os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388"),
        codec=JsonCodec(),
    )
    store = FerricStoreStore(client, key_prefix=key_prefix, scan_count=2)
    namespace_keys = [store._storage.namespace_key(namespace) for namespace in namespaces]
    catalog_keys = {
        store._storage.catalog_key,
        store._storage.prefix_catalog_key(root),
        *(
            store._storage.prefix_catalog_key(namespace)
            for namespace in namespaces
        ),
    }

    try:
        for index, namespace in enumerate(namespaces):
            store.put(namespace, "a", {"value": index * 2})
            store.put(namespace, "b", {"value": index * 2 + 1})

        results = store.search(root, limit=3, offset=1)
        assert [(item.namespace, item.key) for item in results] == [
            (namespaces[0], "b"),
            (namespaces[1], "a"),
            (namespaces[1], "b"),
        ]
        assert store.list_namespaces(prefix=root, limit=2, offset=1) == namespaces[1:3]

        for namespace in namespaces:
            store.delete(namespace, "a")
            store.delete(namespace, "b")

        assert all(client.command("EXISTS", key) == 0 for key in namespace_keys)
        assert all(client.command("ZCARD", key) == 0 for key in catalog_keys)
        assert all(client.command("EXISTS", key) == 0 for key in catalog_keys)
    finally:
        client.delete(*namespace_keys, *catalog_keys)
        client.close()


class ReviewState(TypedDict):
    approved: bool


def test_ferricflow_signal_resumes_checkpointed_langgraph() -> None:
    suffix = uuid.uuid4().hex
    flow_id = f"langgraph-flow-integration-{suffix}"
    key_prefix = f"test:langgraph:flow:{suffix}"
    url = os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")
    workflow_client = WorkflowClient.from_url(url, codec=JsonCodec())
    saver = FerricStoreSaver(workflow_client.flow, key_prefix=key_prefix)

    def review(state: ReviewState) -> ReviewState:
        decision = interrupt({"question": "approve?"})
        return {"approved": bool(decision)}

    builder = StateGraph(ReviewState)
    builder.add_node("review", review)
    builder.add_edge(START, "review")
    builder.add_edge("review", END)
    graph = builder.compile(checkpointer=saver)

    def flow_value(ctx: Any, name: str) -> Any:
        record = ctx.client.get(ctx.id, partition_key=ctx.partition_key)
        assert record is not None
        assert record.value_refs is not None
        metadata = record.value_refs[name]
        ref = metadata["ref"] if isinstance(metadata, dict) else metadata
        return ctx.client.value_mget([ref])[0]

    bridge = LangGraphFlow(
        graph,
        input_factory=lambda ctx: flow_value(ctx, "graph_input"),
        interrupt_state="waiting_review",
    )
    workflow = workflow_client.workflow(
        type=f"langgraph-review-{suffix}",
        initial_state="run_graph",
    )
    thread_ids: list[str] = []

    @workflow.state(
        "run_graph",
        claim_payload=False,
        claim_record=False,
        return_record=True,
    )
    def run_graph(ctx: Any) -> Any:
        thread_ids.append(bridge.thread_id(ctx))
        return bridge(ctx)

    @workflow.state(
        "resume_graph",
        claim_payload=False,
        claim_record=False,
        return_record=True,
    )
    def resume_graph(ctx: Any) -> Any:
        return bridge.resume(ctx, flow_value(ctx, "decision"))

    try:
        workflow.start(flow_id, values={"graph_input": {"approved": False}})
        first = workflow.run_once("run_graph", worker="langgraph-integration")
        assert len(first) == 1
        assert getattr(first[0], "error", None) is None
        assert len(thread_ids) == 1
        assert saver.get_tuple(
            {"configurable": {"thread_id": thread_ids[0]}}
        ) is not None
        waiting = workflow.get(flow_id)
        assert waiting is not None
        assert waiting.state == "waiting_review", (first, waiting.error, waiting.raw)

        workflow.signal(
            flow_id,
            signal="approved",
            if_state="waiting_review",
            transition_to="resume_graph",
            values={"decision": True},
        )
        second = workflow.run_once("resume_graph", worker="langgraph-integration")
        assert len(second) == 1
        completed = workflow.get(flow_id)
        assert completed is not None
        assert completed.state == "completed"
        resumed = saver.get_tuple(
            {"configurable": {"thread_id": thread_ids[0]}}
        )
        assert resumed is not None
        assert resumed.checkpoint["channel_values"]["approved"] is True
    finally:
        if thread_ids:
            saver.delete_thread(thread_ids[0])
        workflow_client.flow.delete(saver._storage.catalog_key)
        workflow_client.close()


def test_ferricflow_runs_checkpointed_langchain_agent() -> None:
    suffix = uuid.uuid4().hex
    flow_id = f"langchain-flow-integration-{suffix}"
    key_prefix = f"test:langchain:flow:{suffix}"
    url = os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")
    workflow_client = WorkflowClient.from_url(url, codec=JsonCodec())
    saver = FerricStoreSaver(workflow_client.flow, key_prefix=key_prefix)
    model = FakeMessagesListChatModel(
        responses=[AIMessage(content="Completed by LangChain.")]
    )
    agent = create_agent(model=model, tools=[], checkpointer=saver)

    def flow_value(ctx: Any, name: str) -> Any:
        record = ctx.client.get(ctx.id, partition_key=ctx.partition_key)
        assert record is not None
        assert record.value_refs is not None
        metadata = record.value_refs[name]
        ref = metadata["ref"] if isinstance(metadata, dict) else metadata
        return ctx.client.value_mget([ref])[0]

    bridge = LangGraphFlow(
        agent,
        input_factory=lambda ctx: flow_value(ctx, "agent_input"),
        on_complete=lambda run, _ctx: complete(
            result=run.value["messages"][-1].content
        ),
    )
    workflow = workflow_client.workflow(
        type=f"langchain-agent-{suffix}",
        initial_state="run_agent",
    )
    thread_ids: list[str] = []

    @workflow.state(
        "run_agent",
        claim_payload=False,
        claim_record=False,
        return_record=True,
    )
    def run_agent(ctx: Any) -> Any:
        thread_ids.append(bridge.thread_id(ctx))
        return bridge(ctx)

    try:
        workflow.start(
            flow_id,
            values={
                "agent_input": {
                    "messages": [{"role": "user", "content": "Handle this."}]
                }
            },
        )
        claimed = workflow.run_once("run_agent", worker="langchain-integration")
        assert len(claimed) == 1
        completed = workflow.get(flow_id)
        assert completed is not None
        assert completed.state == "completed", (claimed, completed.error, completed.raw)
        assert completed.raw is not None
        result_metadata = completed.raw.get(
            b"result_ref",
            completed.raw.get("result_ref"),
        )
        result_ref = (
            result_metadata.get("ref", result_metadata.get(b"ref"))
            if isinstance(result_metadata, dict)
            else result_metadata
        )
        assert workflow_client.flow.value_mget([result_ref])[0] == (
            "Completed by LangChain."
        )
        assert len(thread_ids) == 1
        stored = saver.get_tuple(
            {"configurable": {"thread_id": thread_ids[0]}}
        )
        assert stored is not None
        messages = stored.checkpoint["channel_values"]["messages"]
        assert [message.content for message in messages] == [
            "Handle this.",
            "Completed by LangChain.",
        ]
    finally:
        if thread_ids:
            saver.delete_thread(thread_ids[0])
        workflow_client.flow.delete(saver._storage.catalog_key)
        workflow_client.close()
