from __future__ import annotations

import uuid
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from ferricstore import JsonCodec, WorkflowClient
from ferricstore.langgraph import FerricStoreSaver, LangGraphFlow


class CounterState(TypedDict):
    value: int


def increment(state: CounterState) -> CounterState:
    return {"value": state["value"] + 1}


builder = StateGraph(CounterState)
builder.add_node("increment", increment)
builder.add_edge(START, "increment")
builder.add_edge("increment", END)

client = WorkflowClient.from_url(
    "ferric://127.0.0.1:6388",
    codec=JsonCodec(),
)
checkpointer = FerricStoreSaver(client.flow, key_prefix="example:langgraph:flow")
graph = builder.compile(checkpointer=checkpointer)


def flow_value(ctx, name):
    record = ctx.client.get(ctx.id, partition_key=ctx.partition_key)
    assert record is not None
    assert record.value_refs is not None
    metadata = record.value_refs[name]
    ref = metadata["ref"] if isinstance(metadata, dict) else metadata
    return ctx.client.value_mget([ref])[0]


graph_flow = LangGraphFlow(
    graph,
    input_factory=lambda ctx: flow_value(ctx, "graph_input"),
)

workflow = client.workflow(type="langgraph-counter", initial_state="run_graph")


thread_ids: list[str] = []


@workflow.state(
    "run_graph",
    claim_payload=False,
    claim_record=False,
    return_record=True,
)
def run_graph(ctx):
    thread_ids.append(graph_flow.thread_id(ctx))
    return graph_flow(ctx)


flow_id = f"counter-{uuid.uuid4().hex}"
workflow.start(flow_id, values={"graph_input": {"value": 4}})
workflow.run_once("run_graph", worker="langgraph-example")

record = workflow.get(flow_id)
assert record is not None
checkpoint = checkpointer.get_tuple(
    {"configurable": {"thread_id": thread_ids[0]}},
)
assert checkpoint is not None
print(record.state, checkpoint.checkpoint["channel_values"])
client.close()
