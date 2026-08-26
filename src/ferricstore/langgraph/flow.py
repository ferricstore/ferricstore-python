from __future__ import annotations

import hashlib
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

from langchain_core.runnables import RunnableConfig
from langgraph.types import Command

from ferricstore.workflow_types import complete, fail, transition


class _SyncGraph(Protocol):
    def invoke(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        *,
        context: Any | None = None,
        **kwargs: Any,
    ) -> Any: ...


class _AsyncGraph(Protocol):
    async def ainvoke(
        self,
        input: Any,
        config: RunnableConfig | None = None,
        *,
        context: Any | None = None,
        **kwargs: Any,
    ) -> Any: ...


_MISSING = object()
_INTERRUPT_KEY = "__interrupt__"
_Factory = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class LangGraphFlowContext:
    """Runtime-only bridge from a LangGraph node to its FerricFlow handler context.

    This object is supplied through LangGraph's ``context_schema`` mechanism and
    is not part of checkpointed graph state. ``flow`` is a synchronous
    ``WorkflowContext`` or asynchronous ``AsyncWorkflowContext``.
    """

    flow: Any
    thread_id: str
    checkpoint_ns: str

    @property
    def id(self) -> str:
        return cast(str, self.flow.id)

    @property
    def type(self) -> str:
        return cast(str, self.flow.type)

    @property
    def state(self) -> str:
        return cast(str, self.flow.state)

    @property
    def partition_key(self) -> str | bytes | None:
        return cast(str | bytes | None, self.flow.partition_key)

    @property
    def payload(self) -> Any:
        return self.flow.payload

    @property
    def values(self) -> dict[str, Any]:
        return cast(dict[str, Any], self.flow.values)


@dataclass(frozen=True, slots=True)
class LangGraphFlowRun:
    """One LangGraph invocation and the Flow identity bound to it."""

    value: Any
    thread_id: str
    checkpoint_ns: str
    interrupts: tuple[Any, ...] = ()

    @property
    def interrupted(self) -> bool:
        return bool(self.interrupts)

    @property
    def interrupt_values(self) -> tuple[Any, ...]:
        return tuple(getattr(item, "value", item) for item in self.interrupts)


_OutcomeMapper = Callable[[LangGraphFlowRun, Any], Any]


def _require_text(value: Any, *, name: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must return text")
    if not allow_empty and not value:
        raise ValueError(f"{name} must return non-empty text")
    if "\x00" in value:
        raise ValueError(f"{name} must not contain NUL bytes")
    return value


def _identity_component(value: str | bytes | None) -> bytes:
    if value is None:
        payload = b""
        marker = b"n"
    elif isinstance(value, bytes):
        payload = value
        marker = b"b"
    else:
        payload = value.encode("utf-8")
        marker = b"s"
    return marker + len(payload).to_bytes(8, "big") + payload


def _default_thread_id(flow_context: Any) -> str:
    flow_id = _require_text(getattr(flow_context, "id", None), name="Flow id")
    flow_type = _require_text(getattr(flow_context, "type", None), name="Flow type")
    partition_key = cast(
        str | bytes | None,
        getattr(flow_context, "partition_key", None),
    )
    identity = (
        _identity_component(flow_type)
        + _identity_component(partition_key)
        + _identity_component(flow_id)
    )
    return f"ferricflow:{hashlib.sha256(identity).hexdigest()}"


def _interrupts(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, Mapping):
        return ()
    raw = value.get(_INTERRUPT_KEY)
    if raw is None:
        return ()
    if isinstance(raw, (list, tuple)):
        return tuple(raw)
    return (raw,)


def _reject_awaitable(value: Any, *, name: str) -> Any:
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError(f"{name} returned an awaitable; use AsyncLangGraphFlow")
    return value


async def _resolve_awaitable(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class _LangGraphFlowBase:
    def __init__(
        self,
        graph: Any,
        *,
        input_factory: _Factory | None,
        thread_id: Callable[[Any], str] | None,
        checkpoint_ns: str | Callable[[Any], str],
        config_factory: _Factory | None,
        context_factory: _Factory | None,
        on_complete: _OutcomeMapper | None,
        on_interrupt: _OutcomeMapper | None,
        interrupt_state: str | None,
        recover_existing: bool,
        invoke_kwargs: Mapping[str, Any] | None,
    ) -> None:
        if on_interrupt is not None and interrupt_state is not None:
            raise ValueError("on_interrupt and interrupt_state are mutually exclusive")
        if interrupt_state is not None:
            _require_text(interrupt_state, name="interrupt_state")
        if not isinstance(recover_existing, bool):
            raise TypeError("recover_existing must be a boolean")
        options = dict(invoke_kwargs or {})
        reserved = {"config", "context", "input"}.intersection(options)
        if reserved:
            names = ", ".join(sorted(reserved))
            raise ValueError(f"invoke_kwargs cannot contain reserved options: {names}")
        self.graph = graph
        self.input_factory = input_factory
        self.thread_id_factory = thread_id or _default_thread_id
        self.checkpoint_ns_factory = checkpoint_ns
        self.config_factory = config_factory
        self.context_factory = context_factory
        self.on_complete = on_complete
        self.on_interrupt = on_interrupt
        self.interrupt_state = interrupt_state
        self.recover_existing = recover_existing
        self.invoke_kwargs = options

    def thread_id(self, flow_context: Any) -> str:
        value = _reject_awaitable(
            self.thread_id_factory(flow_context),
            name="thread_id",
        )
        return _require_text(value, name="thread_id")

    def checkpoint_ns(self, flow_context: Any) -> str:
        factory = self.checkpoint_ns_factory
        value = factory(flow_context) if callable(factory) else factory
        value = _reject_awaitable(value, name="checkpoint_ns")
        return _require_text(value, name="checkpoint_ns", allow_empty=True)

    def _build_config(
        self,
        flow_context: Any,
        additional: Mapping[str, Any] | None,
    ) -> RunnableConfig:
        thread_id = self.thread_id(flow_context)
        checkpoint_ns = self.checkpoint_ns(flow_context)
        raw_config = dict(additional or {})
        raw_configurable = raw_config.get("configurable", {})
        if not isinstance(raw_configurable, Mapping):
            raise TypeError("config_factory configurable must be a mapping")
        configurable = dict(raw_configurable)
        configurable["thread_id"] = thread_id
        configurable["checkpoint_ns"] = checkpoint_ns
        raw_config["configurable"] = configurable

        raw_metadata = raw_config.get("metadata", {})
        if not isinstance(raw_metadata, Mapping):
            raise TypeError("config_factory metadata must be a mapping")
        metadata = dict(raw_metadata)
        metadata.update(
            {
                "ferricflow_id": _require_text(
                    getattr(flow_context, "id", None),
                    name="Flow id",
                ),
                "ferricflow_type": _require_text(
                    getattr(flow_context, "type", None),
                    name="Flow type",
                ),
                "ferricflow_state": str(
                    getattr(
                        flow_context,
                        "logical_state",
                        getattr(flow_context, "run_state", getattr(flow_context, "state", "")),
                    )
                ),
            }
        )
        raw_config["metadata"] = metadata
        return cast(RunnableConfig, raw_config)

    def _default_graph_input(self, flow_context: Any) -> Any:
        if self.input_factory is None:
            return getattr(flow_context, "payload", None)
        return self.input_factory(flow_context)

    def _default_graph_context(
        self,
        flow_context: Any,
        *,
        thread_id: str,
        checkpoint_ns: str,
    ) -> Any:
        if self.context_factory is not None:
            return self.context_factory(flow_context)
        return LangGraphFlowContext(
            flow=flow_context,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
        )

    @staticmethod
    def _run(value: Any, config: RunnableConfig) -> LangGraphFlowRun:
        configurable = config["configurable"]
        return LangGraphFlowRun(
            value=value,
            thread_id=cast(str, configurable["thread_id"]),
            checkpoint_ns=cast(str, configurable["checkpoint_ns"]),
            interrupts=_interrupts(value),
        )

    @staticmethod
    def _state_meta(run: LangGraphFlowRun) -> dict[str, Any]:
        return {
            "langgraph_thread_id": run.thread_id,
            "langgraph_checkpoint_ns": run.checkpoint_ns,
            "langgraph_interrupted": run.interrupted,
            "langgraph_interrupt_count": len(run.interrupts),
        }

    def _default_outcome(self, run: LangGraphFlowRun) -> Any:
        state_meta = self._state_meta(run)
        if not run.interrupted:
            return complete(result=run.value, state_meta=state_meta)
        if self.interrupt_state is not None:
            return transition(self.interrupt_state, state_meta=state_meta)
        return fail(
            error={
                "type": "unhandled_langgraph_interrupt",
                "thread_id": run.thread_id,
                "checkpoint_ns": run.checkpoint_ns,
                "interrupt_count": len(run.interrupts),
            },
            state_meta=state_meta,
        )

    def _mapper(self, run: LangGraphFlowRun) -> _OutcomeMapper | None:
        return self.on_interrupt if run.interrupted else self.on_complete

    def _options(self, overrides: Mapping[str, Any]) -> dict[str, Any]:
        reserved = {"config", "context", "input"}.intersection(overrides)
        if reserved:
            names = ", ".join(sorted(reserved))
            raise ValueError(f"invocation options cannot contain reserved options: {names}")
        options = dict(self.invoke_kwargs)
        options.update(overrides)
        return options

    @staticmethod
    def _snapshot_has_checkpoint(snapshot: Any) -> bool:
        if snapshot is None:
            return False
        config = getattr(snapshot, "config", None)
        if isinstance(config, Mapping):
            configurable = config.get("configurable")
            if isinstance(configurable, Mapping) and configurable.get("checkpoint_id"):
                return True
        return (
            getattr(snapshot, "created_at", None) is not None
            or getattr(snapshot, "metadata", None) is not None
        )


class LangGraphFlow(_LangGraphFlowBase):
    """Run or resume a synchronous LangGraph from a FerricFlow state handler."""

    graph: _SyncGraph

    def __init__(
        self,
        graph: _SyncGraph,
        *,
        input_factory: _Factory | None = None,
        thread_id: Callable[[Any], str] | None = None,
        checkpoint_ns: str | Callable[[Any], str] = "",
        config_factory: _Factory | None = None,
        context_factory: _Factory | None = None,
        on_complete: _OutcomeMapper | None = None,
        on_interrupt: _OutcomeMapper | None = None,
        interrupt_state: str | None = None,
        recover_existing: bool = True,
        invoke_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            graph,
            input_factory=input_factory,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            config_factory=config_factory,
            context_factory=context_factory,
            on_complete=on_complete,
            on_interrupt=on_interrupt,
            interrupt_state=interrupt_state,
            recover_existing=recover_existing,
            invoke_kwargs=invoke_kwargs,
        )

    def config(self, flow_context: Any) -> RunnableConfig:
        additional = None
        if self.config_factory is not None:
            value = _reject_awaitable(
                self.config_factory(flow_context),
                name="config_factory",
            )
            if value is not None and not isinstance(value, Mapping):
                raise TypeError("config_factory must return a mapping or None")
            additional = cast(Mapping[str, Any] | None, value)
        return self._build_config(flow_context, additional)

    def invoke(
        self,
        flow_context: Any,
        graph_input: Any = _MISSING,
        *,
        graph_context: Any = _MISSING,
        **invoke_options: Any,
    ) -> LangGraphFlowRun:
        config = self.config(flow_context)
        configurable = config["configurable"]
        if graph_input is _MISSING:
            has_checkpoint = False
            get_state = getattr(self.graph, "get_state", None)
            if self.recover_existing and callable(get_state):
                snapshot = _reject_awaitable(
                    get_state(config),
                    name="graph.get_state",
                )
                has_checkpoint = self._snapshot_has_checkpoint(snapshot)
            graph_input = (
                None
                if has_checkpoint
                else _reject_awaitable(
                    self._default_graph_input(flow_context),
                    name="input_factory",
                )
            )
        if graph_context is _MISSING:
            graph_context = _reject_awaitable(
                self._default_graph_context(
                    flow_context,
                    thread_id=cast(str, configurable["thread_id"]),
                    checkpoint_ns=cast(str, configurable["checkpoint_ns"]),
                ),
                name="context_factory",
            )
        value = self.graph.invoke(
            graph_input,
            config,
            context=graph_context,
            **self._options(invoke_options),
        )
        value = _reject_awaitable(value, name="graph.invoke")
        return self._run(value, config)

    def outcome(self, run: LangGraphFlowRun, flow_context: Any) -> Any:
        mapper = self._mapper(run)
        if mapper is None:
            return self._default_outcome(run)
        return _reject_awaitable(
            mapper(run, flow_context),
            name="Flow outcome mapper",
        )

    def handle(
        self,
        flow_context: Any,
        graph_input: Any = _MISSING,
        *,
        graph_context: Any = _MISSING,
        **invoke_options: Any,
    ) -> Any:
        run = self.invoke(
            flow_context,
            graph_input,
            graph_context=graph_context,
            **invoke_options,
        )
        return self.outcome(run, flow_context)

    def resume(
        self,
        flow_context: Any,
        value: Any,
        *,
        graph_context: Any = _MISSING,
        **invoke_options: Any,
    ) -> Any:
        return self.handle(
            flow_context,
            Command(resume=value),
            graph_context=graph_context,
            **invoke_options,
        )

    def __call__(self, flow_context: Any) -> Any:
        return self.handle(flow_context)


class AsyncLangGraphFlow(_LangGraphFlowBase):
    """Run or resume a LangGraph from an asynchronous FerricFlow state handler."""

    graph: _AsyncGraph

    def __init__(
        self,
        graph: _AsyncGraph,
        *,
        input_factory: _Factory | None = None,
        thread_id: Callable[[Any], str] | None = None,
        checkpoint_ns: str | Callable[[Any], str] = "",
        config_factory: _Factory | None = None,
        context_factory: _Factory | None = None,
        on_complete: _OutcomeMapper | None = None,
        on_interrupt: _OutcomeMapper | None = None,
        interrupt_state: str | None = None,
        recover_existing: bool = True,
        invoke_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            graph,
            input_factory=input_factory,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            config_factory=config_factory,
            context_factory=context_factory,
            on_complete=on_complete,
            on_interrupt=on_interrupt,
            interrupt_state=interrupt_state,
            recover_existing=recover_existing,
            invoke_kwargs=invoke_kwargs,
        )

    async def config(self, flow_context: Any) -> RunnableConfig:
        additional = None
        if self.config_factory is not None:
            value = await _resolve_awaitable(self.config_factory(flow_context))
            if value is not None and not isinstance(value, Mapping):
                raise TypeError("config_factory must return a mapping or None")
            additional = cast(Mapping[str, Any] | None, value)
        return self._build_config(flow_context, additional)

    async def invoke(
        self,
        flow_context: Any,
        graph_input: Any = _MISSING,
        *,
        graph_context: Any = _MISSING,
        **invoke_options: Any,
    ) -> LangGraphFlowRun:
        config = await self.config(flow_context)
        configurable = config["configurable"]
        if graph_input is _MISSING:
            has_checkpoint = False
            get_state = getattr(self.graph, "aget_state", None)
            if self.recover_existing and callable(get_state):
                snapshot = await _resolve_awaitable(get_state(config))
                has_checkpoint = self._snapshot_has_checkpoint(snapshot)
            graph_input = (
                None
                if has_checkpoint
                else await _resolve_awaitable(self._default_graph_input(flow_context))
            )
        if graph_context is _MISSING:
            graph_context = await _resolve_awaitable(
                self._default_graph_context(
                    flow_context,
                    thread_id=cast(str, configurable["thread_id"]),
                    checkpoint_ns=cast(str, configurable["checkpoint_ns"]),
                )
            )
        value = await self.graph.ainvoke(
            graph_input,
            config,
            context=graph_context,
            **self._options(invoke_options),
        )
        return self._run(value, config)

    async def outcome(self, run: LangGraphFlowRun, flow_context: Any) -> Any:
        mapper = self._mapper(run)
        if mapper is None:
            return self._default_outcome(run)
        return await _resolve_awaitable(mapper(run, flow_context))

    async def handle(
        self,
        flow_context: Any,
        graph_input: Any = _MISSING,
        *,
        graph_context: Any = _MISSING,
        **invoke_options: Any,
    ) -> Any:
        run = await self.invoke(
            flow_context,
            graph_input,
            graph_context=graph_context,
            **invoke_options,
        )
        return await self.outcome(run, flow_context)

    async def resume(
        self,
        flow_context: Any,
        value: Any,
        *,
        graph_context: Any = _MISSING,
        **invoke_options: Any,
    ) -> Any:
        return await self.handle(
            flow_context,
            Command(resume=value),
            graph_context=graph_context,
            **invoke_options,
        )

    async def __call__(self, flow_context: Any) -> Any:
        return await self.handle(flow_context)


__all__ = [
    "AsyncLangGraphFlow",
    "LangGraphFlow",
    "LangGraphFlowContext",
    "LangGraphFlowRun",
]
