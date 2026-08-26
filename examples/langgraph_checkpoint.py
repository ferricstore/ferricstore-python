from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from ferricstore import FlowClient
from ferricstore.langgraph import FerricStoreSaver


class CounterState(TypedDict):
    value: int


def increment(state: CounterState) -> CounterState:
    return {"value": state["value"] + 1}


builder = StateGraph(CounterState)
builder.add_node("increment", increment)
builder.add_edge(START, "increment")
builder.add_edge("increment", END)

client = FlowClient.from_url("ferric://127.0.0.1:6388")
checkpointer = FerricStoreSaver(client, key_prefix="example:langgraph:checkpoint")
graph = builder.compile(checkpointer=checkpointer)

config = {"configurable": {"thread_id": "counter-example"}}
result = graph.invoke({"value": 4}, config)
print(result)
print(graph.get_state(config).values)
