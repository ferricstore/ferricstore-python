# LangGraph and LangChain Persistence

FerricStore can act as the checkpoint database for a locally compiled
LangGraph graph. The integration implements LangGraph's `BaseCheckpointSaver`
contract; it does not store the graph definition or run model/tool code inside
FerricStore.

```text
application code                FerricStore
--------------------------      ---------------------------------
nodes and edges                 thread checkpoints
model and tool functions   ->   checkpoint metadata
graph invocation                pending task writes
                                interrupt/resume state
```

For durable business orchestration, `LangGraphFlow` adds a second layer: a
FerricFlow state handler invokes the graph while FerricFlow owns leases,
retries, schedules, signals, and governance.

## Install

```bash
pip install "ferricstore[langgraph]"
```

The optional extra currently supports LangGraph `1.2.x`. The base FerricStore
SDK does not import or depend on LangGraph unless this integration is used.

For LangChain's `create_agent` API, install the LangChain extra plus the model
provider integration used by the application:

```bash
pip install "ferricstore[langchain]" langchain-openai
```

## LangChain agent memory

Modern LangChain agents are compiled LangGraph graphs. Pass the same
`FerricStoreSaver` directly to `create_agent`:

```python
from langchain.agents import create_agent

from ferricstore import FlowClient
from ferricstore.langgraph import FerricStoreSaver, FerricStoreStore

client = FlowClient.from_url("ferric://127.0.0.1:6388")
checkpointer = FerricStoreSaver(client)
memory_store = FerricStoreStore(client)
agent = create_agent(
    model="openai:gpt-5.4",
    tools=[search_tool],
    checkpointer=checkpointer,
    store=memory_store,
)

config = {"configurable": {"thread_id": "conversation-123"}}
first = agent.invoke(
    {"messages": [{"role": "user", "content": "My name is Bob."}]},
    config,
)
second = agent.invoke(
    {"messages": [{"role": "user", "content": "What is my name?"}]},
    config,
)
```

The second invocation loads the first invocation's messages from FerricStore.
This is LangChain's thread-level short-term memory. `FerricStoreStore` implements
LangGraph's separate `BaseStore` contract for long-term data shared across
threads.

Tools access shared memory through LangChain's runtime:

```python
from langchain.tools import ToolRuntime, tool


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
```

The same `("users", user_id)` namespace is available from every conversation
thread. Include tenant or user identity in the namespace so unrelated users
cannot read each other's memories.

## Long-term store operations

`FerricStoreStore` and `AsyncFerricStoreStore` support:

- synchronous and native asynchronous `BaseStore` APIs;
- batched get, put, and delete operations;
- hierarchical namespaces and namespace-prefix search;
- namespace listing with prefix, suffix, wildcard, depth, limit, and offset;
- exact, nested, and `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` filters;
- stable creation timestamps and updated timestamps;
- deterministic pagination ordered by namespace and key.

```python
from ferricstore.langgraph import FerricStoreStore

store = FerricStoreStore(client)
namespace = ("tenant-a", "user-123", "memories")

store.put(namespace, "food", {"preference": "pizza", "confidence": 0.9})
item = store.get(namespace, "food")
matches = store.search(
    ("tenant-a", "user-123"),
    filter={"confidence": {"$gte": 0.8}},
)
store.delete(namespace, "food")
```

For a high-concurrency async service, construct `AsyncFerricStoreStore` with an
`AsyncFlowClient` and use `aget`, `aput`, `asearch`, `adelete`, and
`alist_namespaces`. The synchronous store also provides async methods by moving
its client work to worker threads.

The initial store intentionally has no embedding index and advertises
`supports_ttl = False`. A `query=` search therefore returns normal scoreless
filtered results rather than semantic similarity, `index=` is ignored, and a
non-null TTL raises `NotImplementedError`. Semantic/vector search and sliding
per-memory TTL require dedicated storage and index lifecycle support and can be
added later without changing the basic store API.

## Synchronous graph

```python
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from ferricstore import FlowClient
from ferricstore.langgraph import FerricStoreSaver


class State(TypedDict):
    value: int


builder = StateGraph(State)
builder.add_node("increment", lambda state: {"value": state["value"] + 1})
builder.add_edge(START, "increment")
builder.add_edge("increment", END)

client = FlowClient.from_url("ferric://127.0.0.1:6388")
checkpointer = FerricStoreSaver(client)
graph = builder.compile(checkpointer=checkpointer)

config = {"configurable": {"thread_id": "counter-1"}}
assert graph.invoke({"value": 4}, config) == {"value": 5}
assert graph.get_state(config).values == {"value": 5}
```

`thread_id` identifies one persistent LangGraph execution thread. Reusing it
loads that thread's latest checkpoint. Use a new ID for an independent thread.
LangGraph assigns `checkpoint_ns` when it needs separate subgraph namespaces;
the saver keeps those namespaces isolated.

`FerricStoreSaver` also implements LangGraph's async checkpoint methods. When
it is used with `graph.ainvoke`, synchronous FerricStore work runs in worker
threads.

## Native asynchronous graph

For high-concurrency async services, use an async client and saver:

```python
from ferricstore import AsyncFlowClient
from ferricstore.langgraph import AsyncFerricStoreSaver

client = AsyncFlowClient.from_url("ferric://127.0.0.1:6388")
checkpointer = AsyncFerricStoreSaver(client)
graph = builder.compile(checkpointer=checkpointer)

config = {"configurable": {"thread_id": "counter-1"}}
result = await graph.ainvoke({"value": 4}, config)
```

The native async saver intentionally rejects synchronous `get`, `put`, and
`list` calls. Use it with LangGraph's asynchronous APIs.

## Run a graph from FerricFlow

`LangGraphFlow` turns a compiled graph into a normal synchronous FerricFlow
handler. The graph must still be compiled with a persistent checkpointer:

```python
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from ferricstore import JsonCodec, WorkflowClient
from ferricstore.langgraph import FerricStoreSaver, LangGraphFlow


class State(TypedDict):
    value: int


builder = StateGraph(State)
builder.add_node("increment", lambda state: {"value": state["value"] + 1})
builder.add_edge(START, "increment")
builder.add_edge("increment", END)

client = WorkflowClient.from_url(
    "ferric://127.0.0.1:6388",
    codec=JsonCodec(),
)
checkpointer = FerricStoreSaver(client.flow)
graph = builder.compile(checkpointer=checkpointer)
graph_flow = LangGraphFlow(graph)

workflow = client.workflow(type="agent", initial_state="run_graph")


@workflow.state("run_graph")
def run_graph(ctx):
    return graph_flow(ctx)


workflow.start("agent-1", payload={"value": 4})
workflow.run_once("run_graph", worker="agent-worker")
```

By default, the Flow payload is the graph input and graph completion becomes
`complete(result=graph_output)`. Graph exceptions propagate to the workflow
runtime, so the state's existing `exception_policy` and `retry_policy` decide
whether FerricFlow retries or fails the job.

The bridge is retry-safe at the graph boundary by default. Before submitting
the initial input, it checks for an existing checkpoint. A reclaimed or retried
Flow handler continues that checkpoint with `None`, which returns an already
completed result, preserves an interrupt, or retries pending graph work without
starting a second graph run. Passing an explicit input to `handle()` or
`invoke()` deliberately starts a new run. Set `recover_existing=False` only
when every Flow handler attempt should submit fresh graph input.

The default LangGraph `thread_id` is a deterministic SHA-256 identity derived
from Flow type, partition key, and Flow ID. This avoids collisions when two
partitions or workflow types reuse the same Flow ID. Override it only when an
application needs an existing thread naming scheme:

```python
graph_flow = LangGraphFlow(
    graph,
    thread_id=lambda ctx: f"tenant-agent:{ctx.id}",
    checkpoint_ns="business-agent",
)
```

### Run a LangChain agent from FerricFlow

A LangChain agent returned by `create_agent` can be passed directly to
`LangGraphFlow`. Map the Flow payload into LangChain's message input and map the
agent's final message into a result supported by the Flow payload codec:

```python
from ferricstore import complete
from ferricstore.langgraph import LangGraphFlow

agent_flow = LangGraphFlow(
    agent,
    input_factory=lambda ctx: {
        "messages": [{"role": "user", "content": ctx.payload["prompt"]}]
    },
    on_complete=lambda run, _ctx: complete(
        result=run.value["messages"][-1].content
    ),
)


@workflow.state("run_agent")
def run_agent(ctx):
    return agent_flow(ctx)
```

The explicit `on_complete` mapping is recommended when the workflow uses
`JsonCodec`, because the full LangChain output contains message objects. Those
objects remain safely serialized in the LangGraph checkpoint; the Flow result
only needs the application-facing value.

## Interrupt, signal, and resume

Map LangGraph interrupts to a durable waiting Flow state:

```python
graph_flow = LangGraphFlow(graph, interrupt_state="waiting_review")


@workflow.state("run_graph")
def run_graph(ctx):
    return graph_flow(ctx)


@workflow.state("resume_graph", claim_values=["decision"])
def resume_graph(ctx):
    return graph_flow.resume(ctx, ctx.value("decision"))
```

An external approval endpoint resumes the Flow first. When the worker claims
`resume_graph`, the bridge sends a LangGraph `Command(resume=decision)` to the
same checkpointed thread:

```python
workflow.signal(
    "agent-1",
    signal="approved",
    if_state="waiting_review",
    transition_to="resume_graph",
    values={"decision": True},
)
```

If a graph interrupts without `interrupt_state` or `on_interrupt`, the bridge
returns a terminal `Fail` outcome instead of creating an immediate retry loop.
Use callback mappers when the Flow should persist an interrupt prompt or map a
completed graph into another business state:

```python
from ferricstore import complete, transition


graph_flow = LangGraphFlow(
    graph,
    on_interrupt=lambda run, ctx: transition(
        "waiting_review",
        values={"question": run.interrupt_values[0]},
    ),
    on_complete=lambda run, ctx: complete(result=run.value["answer"]),
)
```

## Use FerricFlow abilities inside nodes

Compile the graph with `LangGraphFlowContext` as its context schema. Nodes can
then use the current Flow handler context for governed effects, budgets, child
flows, values, or other Flow commands without placing clients or lease tokens
inside checkpointed state:

```python
from langgraph.runtime import Runtime

from ferricstore.langgraph import LangGraphFlowContext


def call_tool(state, runtime: Runtime[LangGraphFlowContext]):
    flow = runtime.context.flow

    @flow.effect(
        "search",
        "tool.search",
        idempotency_key=f"{flow.id}:search",
    )
    def search():
        return search_api(state["query"])

    return {"search_result": search()}


builder = StateGraph(State, context_schema=LangGraphFlowContext)
```

`LangGraphFlowContext` is runtime-only and is not serialized into a LangGraph
checkpoint. Tool arguments and results that belong to graph state are still
checkpointed normally.

For async workflows, use `AsyncLangGraphFlow`. Its input, config, context, and
outcome factories may be synchronous functions or coroutines:

```python
from ferricstore.langgraph import AsyncLangGraphFlow

graph_flow = AsyncLangGraphFlow(graph, interrupt_state="waiting_review")


@workflow.state("run_graph")
async def run_graph(ctx):
    return await graph_flow(ctx)
```

Both bridges accept `input_factory`, `config_factory`, `context_factory`,
`on_complete`, `on_interrupt`, and graph `invoke_kwargs`. Per-call graph invoke
options can also be passed to `handle()`, `invoke()`, or `resume()`.

## Stored data

The default key prefix is `langgraph:checkpoint`. The saver creates:

- one global ordered checkpoint-locator index plus hashed namespace and
  locator catalogs for each thread;
- one hash for each `(thread_id, checkpoint_ns)` pair;
- one ordered checkpoint-ID index beside each thread hash;
- checkpoint records in the thread hash;
- pending task writes in the same hash.

The thread hash uses a SHA-256-derived physical hash tag. User-supplied thread
IDs are stored inside the serialized checkpoint descriptor, not copied into
FerricStore key names. Checkpoint and pending-write values use LangGraph's
configured serializer and are written as raw binary values, independently of
the `FlowClient` payload codec.

The long-term store uses the separate `langgraph:store` prefix. It keeps ordered
per-item catalogs for the root and each namespace prefix, plus one hash per
non-empty namespace containing JSON memory documents and timestamps. Deleting
the final item removes both its catalog entries and the now-empty hash. Search
and namespace listing consume the ordered catalogs in bounded pages and stop
when the requested page is complete. Physical namespace keys use
length-delimited SHA-256 identities, while the original namespace tuple and
memory key remain inside the validated stored record.

Checkpoint mutations and long-term-memory mutations publish discovery indexes
before item data and remove item data before indexes. Readers validate every
locator, so a process or connection failure can leave only an invisible,
idempotently retryable locator rather than a live record hidden from latest,
search, or namespace listing. The global checkpoint index lets limited history
queries page directly through the newest checkpoints instead of loading every
thread catalog.

Mutations use renewable FerricStore distributed locks. This serializes
overlapping writes and deletes across processes while leaving reads lock-free.
Lock keys contain only hashed identities, multi-item locks are acquired in
deterministic order, and an `EXTEND` heartbeat keeps ownership valid during
large or temporarily stalled mutations.

To isolate multiple applications that share a FerricStore deployment, set a
distinct prefix:

```python
checkpointer = FerricStoreSaver(
    client,
    key_prefix="my-service:langgraph:checkpoint",
)
```

## Lifecycle

Delete all namespaces and checkpoints belonging to a thread when they are no
longer required:

```python
checkpointer.delete_thread("counter-1")

# Async saver
await checkpointer.adelete_thread("counter-1")
```

The initial integration deliberately does not implement partial pruning,
thread copying, or deletion by run ID. LangGraph's delta-channel checkpoints
can depend on their ancestor chain, so deleting only some ancestors requires a
delta-aware retention algorithm.

## FerricFlow relationship

The persistence adapters and FerricFlow solve different problems:

| Component | Responsibility |
| --- | --- |
| LangGraph | AI/agent nodes, edges, loops, and routing |
| `FerricStoreSaver` | Internal LangGraph checkpoint state |
| `FerricStoreStore` | Long-term memory shared across graph threads |
| `LangGraphFlow` | Flow identity, graph invocation/resume, and outcome mapping |
| FerricFlow | Outer business states, leased work, retries, schedules, signals, and governance |

You can use only the checkpointer for a conversational agent, add the store
when memories must cross conversations, and use the bridge when a business
process should invoke or resume the graph under a FerricFlow lease. Keep
business lifecycle state in the Flow, agent-internal state in the LangGraph
thread, and user/application memory in explicit store namespaces.

## Operational notes

- Checkpoints and long-term memories can contain prompts, model output, tool
  results, user profiles, and application state. Protect them with FerricStore
  ACLs and TLS as sensitive data.
- A checkpoint records execution state; it does not make external side effects
  exactly-once. Tools that send email, charge cards, or mutate another system
  still need idempotency keys. Inside a Flow bridge, prefer `ctx.effect(...)`
  for effect fencing and replay handling.
- One graph invocation runs inside a claimed Flow lease. Size `lease_ms` for the
  longest graph step or extend the lease during long operations.
- Treat `LangGraphFlowContext.flow` as handler-scoped. Do not retain it in
  checkpoint state or detached background tasks after the handler returns.
- Do not reuse a `thread_id` for unrelated users or workflows.
- Use a production FerricStore cluster and normal backup/restore procedures for
  durable deployments.
