from pathlib import Path
from typing import Any

from duckdb import DuckDBPyConnection
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from portus.configs.llm import LLMConfig
from portus.core import AgentExecutor, ExecutionResult, Opa, Session

from ..utils import describe_duckdb_schema
from .graph import ExecuteSubmit
from .utils import get_today_date_str, read_prompt_template


class LighthouseAgent(AgentExecutor):
    def __init__(
        self,
        data_connection: DuckDBPyConnection,
        llm_config: LLMConfig,
    ):
        super().__init__(data_connection, llm_config)
        self._graph = ExecuteSubmit(self._data_connection)
        self._compiled_graph = self._graph.compile(self._llm_config)
        self._template_path = Path("system_prompt.jinja")

    def render_system_prompt(self) -> str:
        # TODO: Add Context support
        prompt_template = read_prompt_template(self._template_path)
        db_schema = describe_duckdb_schema(self._data_connection)

        prompt = prompt_template.render(
            date=get_today_date_str(),
            db_schema=db_schema,
        )
        return prompt

    def execute(
        self, session: Session, opas: list[Opa], llm: BaseChatModel, *, rows_limit: int = 100
    ) -> ExecutionResult:
        # Only convert NEW opas to messages and append them (preserving history)
        new_opas = opas[self._processed_opa_count :]
        if new_opas:
            new_messages = [HumanMessage(content=opa.query) for opa in new_opas]
            self._messages.extend(new_messages)
            self._processed_opa_count = len(opas)

        # Prepend system message if not present
        messages = self._messages
        if not messages or messages[0].type != "system":
            messages = [SystemMessage(self.render_system_prompt()), *messages]

        init_state = self._graph.init_state(messages)
        last_state: dict[str, Any] | None = None
        try:
            for chunk in self._compiled_graph.stream(
                init_state,
                stream_mode="values",
                config=RunnableConfig(recursion_limit=50),
            ):
                assert isinstance(chunk, dict)
                last_state = chunk
        except Exception as e:
            return ExecutionResult(text=str(e), meta={"messages": messages})
        assert last_state is not None

        # Update our message history with all messages from the graph execution
        # (includes AI responses, tool calls, tool responses, etc.)
        final_messages = last_state.get("messages", [])
        if final_messages:
            # Replace our internal messages with the complete history from the graph
            # (excluding system message which we add dynamically)
            self._messages = [msg for msg in final_messages if msg.type != "system"]

        return self._graph.get_result(last_state)
