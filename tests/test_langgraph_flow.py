from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, TypedDict

import pytest
from langchain.agents import create_agent
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from langgraph.types import interrupt

from ferricstore import Complete, Fail, Transition, complete, transition
from ferricstore.langgraph import (
    AsyncLangGraphFlow,
    LangGraphFlow,
    LangGraphFlowContext,
)


@dataclass
class FakeFlowContext:
    id: str
    payload: Any
    partition_key: str | bytes | None = None
    type: str = "agent"
    state: str = "running"
    logical_state: str = "run_graph"
    values: dict[str, Any] = field(default_factory=dict)

    def value(self, name: str, default: Any = None) -> Any:
        return self.values.get(name, default)


class CounterState(TypedDict):
    value: int


def counter_graph(seen_flow_ids: list[str] | None = None) -> Any:
    def increment(
        state: CounterState,
        runtime: Runtime[LangGraphFlowContext],
    ) -> CounterState:
        if seen_flow_ids is not None:
            seen_flow_ids.append(runtime.context.id)
        return {"value": state["value"] + 1}

    builder = StateGraph(CounterState, context_schema=LangGraphFlowContext)
    builder.add_node("increment", increment)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    return builder.compile(checkpointer=InMemorySaver())


def test_sync_bridge_maps_flow_to_graph_context_and_completion() -> None:
    seen_flow_ids: list[str] = []
    bridge = LangGraphFlow(counter_graph(seen_flow_ids))
    ctx = FakeFlowContext("flow-1", {"value": 4}, partition_key="tenant-a")

    outcome = bridge(ctx)

    assert isinstance(outcome, Complete)
    assert outcome.result == {"value": 5}
    assert outcome.state_meta is not None
    assert outcome.state_meta["langgraph_thread_id"] == bridge.thread_id(ctx)
    assert outcome.state_meta["langgraph_interrupted"] is False
    assert seen_flow_ids == ["flow-1"]

    graph_config = bridge.config(ctx)
    assert graph_config["configurable"]["checkpoint_ns"] == ""
    assert graph_config["metadata"]["ferricflow_id"] == "flow-1"
    assert graph_config["metadata"]["ferricflow_type"] == "agent"
    assert graph_config["metadata"]["ferricflow_state"] == "run_graph"


def test_sync_bridge_recovers_existing_checkpoint_without_reexecuting_graph() -> None:
    seen_flow_ids: list[str] = []
    bridge = LangGraphFlow(counter_graph(seen_flow_ids))
    ctx = FakeFlowContext("retry-flow", {"value": 4})

    first = bridge(ctx)
    retried = bridge(ctx)

    assert isinstance(first, Complete)
    assert isinstance(retried, Complete)
    assert first.result == retried.result == {"value": 5}
    assert seen_flow_ids == ["retry-flow"]

    deliberate_new_run = bridge.handle(ctx, {"value": 9})
    assert isinstance(deliberate_new_run, Complete)
    assert deliberate_new_run.result == {"value": 10}
    assert seen_flow_ids == ["retry-flow", "retry-flow"]


def test_sync_bridge_runs_langchain_create_agent_and_recovers_its_checkpoint() -> None:
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(content="Handled by LangChain."),
            AIMessage(content="This response must not be used by a Flow retry."),
        ]
    )
    agent = create_agent(model=model, tools=[], checkpointer=InMemorySaver())
    bridge = LangGraphFlow(agent)
    ctx = FakeFlowContext(
        "langchain-flow-1",
        {"messages": [{"role": "user", "content": "Handle this."}]},
    )

    first = bridge(ctx)
    retried = bridge(ctx)

    assert isinstance(first, Complete)
    assert isinstance(retried, Complete)
    assert [message.content for message in first.result["messages"]] == [
        "Handle this.",
        "Handled by LangChain.",
    ]
    assert retried.result == first.result
    assert model.i == 1


def test_default_thread_identity_separates_types_and_partitions() -> None:
    bridge = LangGraphFlow(counter_graph())
    base = FakeFlowContext("same-id", {"value": 1}, partition_key="tenant-a")
    other_partition = FakeFlowContext(
        "same-id",
        {"value": 1},
        partition_key="tenant-b",
    )
    other_type = FakeFlowContext(
        "same-id",
        {"value": 1},
        partition_key="tenant-a",
        type="other-agent",
    )

    assert bridge.thread_id(base).startswith("ferricflow:")
    assert len(
        {
            bridge.thread_id(base),
            bridge.thread_id(other_partition),
            bridge.thread_id(other_type),
        }
    ) == 3


class ReviewState(TypedDict):
    approved: bool


def review_graph() -> Any:
    def ask(state: ReviewState) -> ReviewState:
        decision = interrupt({"question": "approve?"})
        return {"approved": bool(decision)}

    builder = StateGraph(ReviewState)
    builder.add_node("ask", ask)
    builder.add_edge(START, "ask")
    builder.add_edge("ask", END)
    return builder.compile(checkpointer=InMemorySaver())


def test_sync_bridge_transitions_on_interrupt_and_resumes_same_thread() -> None:
    bridge = LangGraphFlow(review_graph(), interrupt_state="waiting_review")
    ctx = FakeFlowContext("review-1", {"approved": False})

    waiting = bridge(ctx)
    assert isinstance(waiting, Transition)
    assert waiting.to_state == "waiting_review"
    assert waiting.state_meta is not None
    assert waiting.state_meta["langgraph_interrupt_count"] == 1

    finished = bridge.resume(ctx, True)
    assert isinstance(finished, Complete)
    assert finished.result == {"approved": True}


def test_sync_bridge_custom_interrupt_and_completion_mappers() -> None:
    def on_interrupt(run: Any, _ctx: Any) -> Transition:
        return transition(
            "waiting_review",
            values={"question": run.interrupt_values[0]},
        )

    def on_complete(run: Any, _ctx: Any) -> Complete:
        return complete(result=run.value["approved"])

    bridge = LangGraphFlow(
        review_graph(),
        on_interrupt=on_interrupt,
        on_complete=on_complete,
    )
    ctx = FakeFlowContext("review-2", {"approved": False})

    waiting = bridge(ctx)
    assert isinstance(waiting, Transition)
    assert waiting.values == {"question": {"question": "approve?"}}

    finished = bridge.resume(ctx, False)
    assert isinstance(finished, Complete)
    assert finished.result is False


def test_sync_bridge_fails_closed_for_unhandled_interrupt() -> None:
    bridge = LangGraphFlow(review_graph())
    outcome = bridge(FakeFlowContext("review-unhandled", {"approved": False}))

    assert isinstance(outcome, Fail)
    assert outcome.error["type"] == "unhandled_langgraph_interrupt"
    assert outcome.error["interrupt_count"] == 1


def test_bridge_merges_custom_config_but_owns_checkpoint_identity() -> None:
    bridge = LangGraphFlow(
        counter_graph(),
        thread_id=lambda ctx: f"custom:{ctx.id}",
        checkpoint_ns=lambda ctx: ctx.type,
        config_factory=lambda _ctx: {
            "recursion_limit": 12,
            "configurable": {"thread_id": "ignored", "custom": "kept"},
            "metadata": {"tenant": "acme"},
        },
    )
    ctx = FakeFlowContext("flow-2", {"value": 1})
    graph_config = bridge.config(ctx)

    assert graph_config["recursion_limit"] == 12
    assert graph_config["configurable"] == {
        "thread_id": "custom:flow-2",
        "checkpoint_ns": "agent",
        "custom": "kept",
    }
    assert graph_config["metadata"]["tenant"] == "acme"


def test_async_bridge_awaits_factories_graph_and_outcome_mapper() -> None:
    seen_flow_ids: list[str] = []
    input_calls = 0

    async def input_factory(ctx: FakeFlowContext) -> Any:
        nonlocal input_calls
        input_calls += 1
        await asyncio.sleep(0)
        return ctx.payload

    async def config_factory(_ctx: FakeFlowContext) -> Any:
        await asyncio.sleep(0)
        return {"metadata": {"mode": "async"}}

    async def on_complete(run: Any, _ctx: Any) -> Transition:
        await asyncio.sleep(0)
        return transition("next", values={"result": run.value})

    bridge = AsyncLangGraphFlow(
        counter_graph(seen_flow_ids),
        input_factory=input_factory,
        config_factory=config_factory,
        on_complete=on_complete,
    )
    ctx = FakeFlowContext("async-flow-1", {"value": 8}, partition_key=b"tenant-a")

    async def run() -> None:
        graph_config = await bridge.config(ctx)
        assert graph_config["metadata"]["mode"] == "async"
        outcome = await bridge(ctx)
        assert isinstance(outcome, Transition)
        assert outcome.to_state == "next"
        assert outcome.values == {"result": {"value": 9}}
        retried = await bridge(ctx)
        assert isinstance(retried, Transition)
        assert retried.values == {"result": {"value": 9}}

    asyncio.run(run())
    assert seen_flow_ids == ["async-flow-1"]
    assert input_calls == 1


def test_bridge_rejects_ambiguous_interrupt_and_reserved_options() -> None:
    graph = counter_graph()
    with pytest.raises(ValueError, match="mutually exclusive"):
        LangGraphFlow(
            graph,
            interrupt_state="waiting",
            on_interrupt=lambda run, ctx: complete(result=(run, ctx)),
        )
    with pytest.raises(ValueError, match="reserved options"):
        LangGraphFlow(graph, invoke_kwargs={"context": "invalid"})
