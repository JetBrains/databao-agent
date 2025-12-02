import logging
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.configs.llm import LLMConfig
from databao.core.cache import Cache
from databao.core.executor import ExecutionResult, OutputModalityHints
from databao.duckdb.react_tools import AgentResponse, execute_duckdb_sql, make_react_duckdb_agent
from databao.executors.base import GraphExecutor
from databao.executors.connection_managers.duckdb_manager import DuckDBManager

logger = logging.getLogger(__name__)


class ReactDuckDBExecutor(GraphExecutor):
    def __init__(self, cache: Cache, llm_config: LLMConfig, conn_manager: DuckDBManager, rows_limit: int = 12) -> None:
        """Initialize agent with lazy graph compilation."""
        super().__init__(cache, llm_config, conn_manager, rows_limit)
        self._compiled_graph: CompiledStateGraph[Any] | None = None

    def _create_graph(self, data_connection: Any) -> CompiledStateGraph[Any]:
        """Create and compile the ReAct DuckDB agent graph."""
        return make_react_duckdb_agent(data_connection, self._llm_config.new_chat_model())

    @property
    def result(self) -> ExecutionResult | None:
        return self._result

    def _execute(
        self,
        stream: bool = True,
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        compiled_graph = self._compiled_graph or self._create_graph(self.conn_manager.duckdb_connection)
        self._compiled_graph = compiled_graph

        self._process_new_opas()

        # Execute the graph
        init_state = {"messages": self._messages}
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph_sync(compiled_graph, init_state, config=invoke_config, stream=stream)
        answer: AgentResponse = last_state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = execute_duckdb_sql(answer.sql, self.conn_manager.duckdb_connection, limit=self._rows_limit)

        self._update_message_history()

        execution_result = ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
