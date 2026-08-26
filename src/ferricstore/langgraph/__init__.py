"""LangGraph checkpoint persistence and cross-thread memory backed by FerricStore.

Install the optional dependency before importing this module::

    pip install "ferricstore[langgraph]"
"""

try:
    from ferricstore.langgraph.checkpoint import (
        AsyncFerricStoreSaver,
        FerricStoreSaver,
    )
    from ferricstore.langgraph.flow import (
        AsyncLangGraphFlow,
        LangGraphFlow,
        LangGraphFlowContext,
        LangGraphFlowRun,
    )
    from ferricstore.langgraph.store import (
        AsyncFerricStoreStore,
        FerricStoreStore,
    )
except ModuleNotFoundError as exc:
    if exc.name and (
        exc.name in {"langchain_core", "langgraph"}
        or exc.name.startswith(("langchain_core.", "langgraph."))
    ):
        raise ImportError(
            'FerricStore LangGraph support requires `pip install "ferricstore[langgraph]"`'
        ) from exc
    raise

__all__ = [
    "AsyncFerricStoreSaver",
    "AsyncFerricStoreStore",
    "AsyncLangGraphFlow",
    "FerricStoreSaver",
    "FerricStoreStore",
    "LangGraphFlow",
    "LangGraphFlowContext",
    "LangGraphFlowRun",
]
