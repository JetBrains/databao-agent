from pathlib import Path
from typing import Any

from langchain_core.messages import SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph.state import CompiledStateGraph

from databao.configs import LLMConfig
from databao.core.cache import Cache
from databao.core.executor import ExecutionResult, OutputModalityHints
from databao.duckdb.utils import describe_duckdb_schema
from databao.executors.base import GraphExecutor
from databao.executors.connection_managers.duckdb_manager import DuckDBManager
from databao.executors.lighthouse.graph import ExecuteSubmit
from databao.executors.lighthouse.history_cleaning import clean_tool_history
from databao.executors.lighthouse.utils import get_today_date_str, read_prompt_template


class LighthouseExecutor(GraphExecutor):
    def __init__(self, cache: Cache, llm_config: LLMConfig, conn_manager: DuckDBManager, rows_limit: int = 12) -> None:
        super().__init__(cache, llm_config, conn_manager, rows_limit)
        self._prompt_template = read_prompt_template(Path("system_prompt.jinja"))
        self._graph: ExecuteSubmit = ExecuteSubmit(conn_manager.duckdb_connection)
        self._compiled_graph: CompiledStateGraph[Any] | None = None

    def render_system_prompt(
        self,
        data_connection: Any,
    ) -> str:
        """Render system prompt with database schema."""
        db_schema = describe_duckdb_schema(data_connection)

        context = ""
        for db_name, source in self.conn_manager.sources.dbs.items():
            if source.context:
                context += f"## Context for DB {db_name}\n\n{source.context}\n\n"
        for df_name, source in self.conn_manager.sources.dfs.items():
            if source.context:
                context += (
                    f"## Context for DF {df_name} (fully qualified name 'temp.main.{df_name}')\n\n{source.context}\n\n"
                )
        for idx, add_ctx in enumerate(self.conn_manager.sources.additional_context, start=1):
            context += f"## General information {idx}\n\n{add_ctx.strip()}\n\n"
        context = context.strip()

        prompt = self._prompt_template.render(
            date=get_today_date_str(), db_schema=db_schema, context=context, tool_limit=self._graph_recursion_limit // 2
        )

        return prompt.strip()

    @property
    def result(self) -> ExecutionResult | None:
        if len([op for op in self.opas if op.is_materialized is False]) > 0:
            print("Some operations are not materialized yet. Executing them now.")
            self._result = self._execute(stream=False)
        return self._result

    def _get_compiled_graph(self) -> CompiledStateGraph[Any]:
        """Get a compiled graph."""
        compiled_graph = self._compiled_graph or self._graph.compile(self._llm_config)
        self._compiled_graph = compiled_graph

        return compiled_graph

    def _execute(
        self,
        stream: bool = True,
    ) -> ExecutionResult:
        new_opas = self._process_new_opas()

        # Prepare system message
        all_messages_with_system = [
            SystemMessage(self.render_system_prompt(self.conn_manager.duckdb_connection)),
            *[m for m in self._messages if not isinstance(m, SystemMessage)],
        ]
        cleaned_messages = clean_tool_history(all_messages_with_system, self._llm_config.max_tokens_before_cleaning)

        init_state = self._graph.init_state(cleaned_messages, limit_max_rows=self._rows_limit)
        invoke_config = RunnableConfig(recursion_limit=self._graph_recursion_limit)
        last_state = self._invoke_graph_sync(
            self._get_compiled_graph(), init_state, config=invoke_config, stream=stream
        )
        final_messages = last_state.get("messages", [])
        execution_result = self._graph.get_result(final_messages)

        # Update message history (excluding system message which we add dynamically)
        new_messages = final_messages[len(cleaned_messages) :]
        all_messages = all_messages_with_system + new_messages
        if execution_result.meta.get("messages"):
            execution_result.meta["messages"] = all_messages
        self._messages = all_messages

        for opa in new_opas:
            opa.is_materialized = True
        self._update_message_history()

        # Set modality hints
        execution_result.meta[OutputModalityHints.META_KEY] = self._make_output_modality_hints(execution_result)

        return execution_result
