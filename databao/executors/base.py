from abc import ABC, abstractmethod
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.configs.llm import LLMConfig
from databao.core.cache import Cache
from databao.core.executor import ExecutionResult, Executor, OutputModalityHints
from databao.core.opa import Opa
from databao.executors.connection_managers.duckdb_manager import DuckDBManager
from databao.executors.frontend.text_frontend import TextStreamFrontend
from databao.executors.opa_metadata import OpaMetadata

try:
    from duckdb import DuckDBPyConnection
except ImportError:
    DuckDBPyConnection = Any  # type: ignore


class GraphExecutor(Executor, ABC):
    """
    Base class for LangGraph executors that execute with a DuckDB connection and LLM configuration.
    Provides common functionality for graph caching, message handling, and OPA processing.
    """

    def __init__(self, cache: Cache, llm_config: LLMConfig, conn_manager: DuckDBManager, rows_limit: int = 12) -> None:
        """Initialize agent with graph caching infrastructure."""
        self._cache = cache
        self._llm_config = llm_config
        self.conn_manager = conn_manager
        self._rows_limit = rows_limit
        self._graph_recursion_limit = 50
        state = cache.get("state", {})
        self._opas: list[OpaMetadata] = state.get("opas", [])
        self._messages: list[BaseMessage] = state.get("messages", [])
        self._result: ExecutionResult | None = None

    @property
    def opas(self) -> list[OpaMetadata]:
        return self._opas

    def _process_new_opas(self) -> list[OpaMetadata]:
        new_opas = [op for op in self._opas if op.is_materialized is False]
        query = "\n\n".join([op.opa.query for op in new_opas])
        message = HumanMessage(content=query)
        for opa in new_opas:
            opa.message = message
        self._messages.append(message)
        return new_opas

    def add_opa(self, opa: Opa, lazy: bool, stream: bool, rows_limit: int | None = None) -> None:
        if rows_limit is not None:
            self._rows_limit = rows_limit
        self._opas.append(OpaMetadata(opa, is_materialized=False))
        if lazy:
            return
        self._result = self._execute(stream=stream)

    def drop_last_opa(self, n: int = 1) -> None:
        human_messages = [m for m in self._messages if isinstance(m, HumanMessage)]
        if len(human_messages) < n:
            raise ValueError(f"Cannot drop last {n} operations - only {len(human_messages)} operations found.")
        opas_to_drop = self._opas[-n:]
        messages_to_drop = []
        for opa in opas_to_drop:
            messages_to_drop.append(opa.message)
        idx = -1
        for id_, m in enumerate(self._messages):
            if m in messages_to_drop:
                idx = id_
        self._messages = self._messages[:idx]
        self._opas = self._opas[:-n]

        # If several opas were merged and one of them was dropped, mark remaining as unmaterialized
        for opa in self._opas:
            if opa.message in messages_to_drop:
                opa.is_materialized = False
                opa.message = None

    def _make_output_modality_hints(self, result: ExecutionResult) -> OutputModalityHints:
        # A separate LLM module could be used to fill out the hints
        vis_prompt = result.meta.get("visualization_prompt", None)
        if vis_prompt is not None and len(vis_prompt) == 0:
            vis_prompt = None
        df = result.df
        should_visualize = vis_prompt is not None and df is not None and len(df) >= 3
        return OutputModalityHints(visualization_prompt=vis_prompt, should_visualize=should_visualize)

    @staticmethod
    def _invoke_graph_sync(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        stream: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Invoke the graph with the given start state and return the output state."""
        if stream:
            return GraphExecutor._execute_stream_sync(compiled_graph, start_state, config=config, **kwargs)
        else:
            return compiled_graph.invoke(start_state, config=config)

    @staticmethod
    async def _execute_stream(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        writer = TextStreamFrontend(start_state)
        last_state = None
        async for mode, chunk in compiled_graph.astream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            writer.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        writer.end()
        assert last_state is not None
        return last_state

    @staticmethod
    def _execute_stream_sync(
        compiled_graph: CompiledStateGraph[Any],
        start_state: Any,
        *,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        writer = TextStreamFrontend(start_state)
        last_state = None
        for mode, chunk in compiled_graph.stream(
            start_state,
            stream_mode=["values", "messages"],
            config=config,
            **kwargs,
        ):
            writer.write_stream_chunk(mode, chunk)
            if mode == "values":
                last_state = chunk
        writer.end()
        assert last_state is not None
        return last_state

    def _update_message_history(self) -> None:
        """Update message history in cache with final messages from graph execution."""
        if self._messages and self._opas:
            messages_with_no_system = [m for m in self._messages if not isinstance(m, SystemMessage)]
            self._cache.put("state", {"messages": messages_with_no_system, "opas": self._opas})

    @abstractmethod
    def _execute(self, stream: bool = True) -> ExecutionResult:
        pass
