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

from portus.configs.llm import LLMConfig
from portus.core import AgentExecutor, ExecutionResult, Opa, Session

from ..utils import describe_duckdb_schema

logger = logging.getLogger(__name__)


class AgentResponse(BaseModel):
    sql: str
    explanation: str


class ReactDuckDBAgent(AgentExecutor):
    def __init__(
        self,
        data_connection: DuckDBPyConnection,
        llm_config: LLMConfig,
    ):
        super().__init__(data_connection, llm_config)
        self._compiled_graph = self._make_react_duckdb_agent(self._data_connection, self._llm_config.chat_model)
        self._rows_limit = 100

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

    def execute(
        self, session: Session, opas: list[Opa], llm: BaseChatModel, *, rows_limit: int = 100
    ) -> ExecutionResult:
        # Only convert NEW opas to messages and append them (preserving history)
        new_opas = opas[self._processed_opa_count :]
        if new_opas:
            new_messages = [HumanMessage(content=opa.query) for opa in new_opas]
            self._messages.extend(new_messages)
            self._processed_opa_count = len(opas)

        state = self._compiled_graph.invoke({"messages": self._messages})
        answer: AgentResponse = state["structured_response"]
        logger.info("Generated query: %s", answer.sql)
        df = self._data_connection.execute(f"SELECT * FROM ({self._sql_strip(answer.sql)}) t LIMIT {rows_limit}").df()

        # Update our message history with all messages from the graph execution
        # (includes AI responses, tool calls, tool responses, etc.)
        final_messages = state.get("messages", [])
        if final_messages:
            self._messages = final_messages

        return ExecutionResult(text=answer.explanation, code=answer.sql, df=df, meta={})

    @staticmethod
    def _sql_strip(query: str) -> str:
        return query.strip().rstrip(";")
