import json
import logging
from typing import Any

from duckdb import DuckDBPyConnection
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from pydantic import BaseModel

from portus.agents.base import AgentExecutor
from portus.core import ExecutionResult, Opa, Session

from .utils import describe_duckdb_schema

logger = logging.getLogger(__name__)


class AgentResponse(BaseModel):
    sql: str
    explanation: str


class ReactDuckDBAgent(AgentExecutor):
    def __init__(self) -> None:
        """Initialize agent with lazy graph compilation."""
        self._cached_compiled_graph: CompiledStateGraph[Any] | None = None
        self._cached_connection_id: int | None = None
        self._cached_llm_config_id: int | None = None

    @staticmethod
    def _make_duckdb_tool(con: DuckDBPyConnection) -> Any:
        @tool("execute_sql")
        def execute_sql(sql: str, limit: int = 10) -> str:
            """
            Execute any SQL against DuckDB.

            Args:
                sql: The SQL statement to execute (single statement).
                limit: Optional row cap for result-returning statements (10 by default).

            Returns:
                JSON string: { "columns": [...], "rows": str, "limit": int, "note": str }
            """
            statement = ReactDuckDBAgent._sql_strip(sql)
            try:
                sql_to_run = statement
                if limit and " LIMIT " not in statement.upper():
                    sql_to_run = f"{statement} LIMIT {int(limit)}"
                df = con.execute(sql_to_run).df()
                payload = {
                    "columns": list(df.columns),
                    "rows": df.to_string(index=False),
                    "limit": limit,
                    "note": "Query executed successfully",
                }
                return json.dumps(payload)
            except Exception as e:
                payload = {
                    "columns": [],
                    "rows": [],
                    "limit": limit,
                    "note": f"SQL error: {type(e).__name__}: {e}",
                }
                return json.dumps(payload)

        return execute_sql

    @staticmethod
    def _make_react_duckdb_agent(con: DuckDBPyConnection, llm: BaseChatModel) -> CompiledStateGraph[Any]:
        schema_text = describe_duckdb_schema(con)
        # TODO move to .jinja (and fix indendation)
        SYSTEM_PROMPT = f"""You are a careful data analyst using the ReAct pattern with tools.
    Use the `execute_sql` tool to run exactly one DuckDB SQL statement when needed.

    Guidelines:
    - Translate the NL question to ONE DuckDB SQL statement.
    - Use provided schema.
    - You can fetch extra details about schema/tables/columns if needed using SQL queries.
    - After running, write a concise, user-friendly explanation.
    - Do NOT write any tables/lists to the output.
    - Always include the exact SQL you ran.
    - Always use the full table name in query with db name and schema name.

    Available schema:
    {schema_text}
    """
        # LangGraph prebuilt ReAct agent
        execute_sql_tool = ReactDuckDBAgent._make_duckdb_tool(con)
        tools = [execute_sql_tool]
        agent = create_react_agent(
            llm,
            tools=tools,
            prompt=SYSTEM_PROMPT,
            response_format=AgentResponse,
        )
        return agent

    def _get_or_create_graph(self, session: Session) -> tuple[Any, CompiledStateGraph[Any]]:
        """Get cached graph or create new one if connection/config changed."""
        data_connection = self._get_data_connection(session)
        llm_config = self._get_llm_config(session)

        connection_id = id(data_connection)
        llm_config_id = id(llm_config)

        # Check if we need to recompile (connection or config changed)
        if (
            self._cached_compiled_graph is None
            or self._cached_connection_id != connection_id
            or self._cached_llm_config_id != llm_config_id
        ):
            # Create and compile the graph
            self._cached_compiled_graph = self._make_react_duckdb_agent(data_connection, llm_config.chat_model)
            self._cached_connection_id = connection_id
            self._cached_llm_config_id = llm_config_id

        return data_connection, self._cached_compiled_graph

    def execute(
        self, session: Session, opas: list[Opa], *, rows_limit: int = 100, cache_scope: str = "common_cache"
    ) -> ExecutionResult:
        # Get or create graph (cached after first use)
        data_connection, compiled_graph = self._get_or_create_graph(session)

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

        state = compiled_graph.invoke({"messages": messages})
        answer: AgentResponse = state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = data_connection.execute(f"SELECT * FROM ({self._sql_strip(answer.sql)}) t LIMIT {rows_limit}").df()

        # Update our message history with all messages from the graph execution
        # (includes AI responses, tool calls, tool responses, etc.)
        final_messages = state.get("messages", [])
        if final_messages:
            messages = final_messages
            # Store updated messages in cache
            self._set_messages(session, cache_scope, messages)

        return ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})

    @staticmethod
    def _sql_strip(query: str) -> str:
        return query.strip().rstrip(";")
