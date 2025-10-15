from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from portus.agents.base import AgentExecutor
from portus.core import ExecutionResult, Opa, Session
from portus.duckdb.utils import describe_duckdb_schema

from .graph import ExecuteSubmit
from .utils import get_today_date_str, read_prompt_template


class LighthouseAgent(AgentExecutor):
    def __init__(self) -> None:
        """Initialize agent with lazy graph compilation."""
        self._cached_graph: ExecuteSubmit | None = None
        self._cached_compiled_graph: Any | None = None
        self._cached_connection_id: int | None = None
        self._cached_llm_config_id: int | None = None

    def render_system_prompt(self, data_connection: Any) -> str:
        """Render system prompt with database schema."""
        # TODO: Add Context support
        prompt_template = read_prompt_template(Path("system_prompt.jinja"))
        db_schema = describe_duckdb_schema(data_connection)

        prompt = prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
        )
        return prompt

    def _get_or_create_graph(self, session: Session) -> tuple[Any, ExecuteSubmit, Any]:
        """Get cached graph or create new one if connection/config changed."""
        data_connection = self._get_data_connection(session)
        llm_config = self._get_llm_config(session)

        connection_id = id(data_connection)
        llm_config_id = id(llm_config)

        # Check if we need to recompile (connection or config changed)
        if (
            self._cached_graph is None
            or self._cached_connection_id != connection_id
            or self._cached_llm_config_id != llm_config_id
        ):
            # Create and compile the graph
            self._cached_graph = ExecuteSubmit(data_connection)
            self._cached_compiled_graph = self._cached_graph.compile(llm_config)
            self._cached_connection_id = connection_id
            self._cached_llm_config_id = llm_config_id

        return data_connection, self._cached_graph, self._cached_compiled_graph

    def execute(
        self, session: Session, opas: list[Opa], *, rows_limit: int = 100, cache_scope: str = "common_cache"
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        data_connection, graph, compiled_graph = self._get_or_create_graph(session)

        # Get current state from cache
        messages = self._get_messages(session, cache_scope)
        processed_opa_count = self._get_processed_opa_count(session, cache_scope)

        # Only convert NEW opas to messages and append them (preserving history)
        new_opas = opas[processed_opa_count:]
        if new_opas:
            new_messages = [HumanMessage(content=opa.query) for opa in new_opas]
            messages.extend(new_messages)
            processed_opa_count = len(opas)
            # Update cache with new processed count
            self._set_processed_opa_count(session, cache_scope, processed_opa_count)

        # Prepend system message if not present
        messages_with_system = messages
        if not messages_with_system or messages_with_system[0].type != "system":
            messages_with_system = [SystemMessage(self.render_system_prompt(data_connection)), *messages_with_system]

        init_state = graph.init_state(messages_with_system)
        last_state: dict[str, Any] | None = None
        try:
            for chunk in compiled_graph.stream(
                init_state,
                stream_mode="values",
                config=RunnableConfig(recursion_limit=50),
            ):
                assert isinstance(chunk, dict)
                last_state = chunk
        except Exception as e:
            return ExecutionResult(text=str(e), meta={"messages": messages_with_system})
        assert last_state is not None

        # Update our message history with all messages from the graph execution
        # (includes AI responses, tool calls, tool responses, etc.)
        final_messages = last_state.get("messages", [])
        if final_messages:
            # Replace messages with the complete history from the graph
            # (excluding system message which we add dynamically)
            messages = [msg for msg in final_messages if msg.type != "system"]
            # Store updated messages in cache
            self._set_messages(session, cache_scope, messages)

        return graph.get_result(last_state)
