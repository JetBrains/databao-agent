from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from portus.configs.llm import LLMConfig
from portus.core import Executor, Pipe, Session, Visualizer
from portus.core.pipe import PipeState
from portus.core.session import SessionState

from ..pipes.lazy import LazyPipe


class InMemSession(Session):
    def __init__(
        self,
        name: str,
        llm_config: LLMConfig,
        *,
        data_executor: Executor,
        visualizer: Visualizer,
        default_rows_limit: int = 1000,
    ):
        self._name = name
        self._llm = llm_config.chat_model
        self._state = SessionState()

        self._executor = data_executor
        self._visualizer = visualizer
        self._default_rows_limit = default_rows_limit

    def add_db(self, connection: Any, *, name: str | None = None) -> None:
        conn_name = name or f"db{len(self._state.dbs) + 1}"
        updated_dbs = {**self._state.dbs, conn_name: connection}
        self._state = self._state.model_copy(update={"dbs": updated_dbs})

    def add_df(self, df: DataFrame, *, name: str | None = None) -> None:
        df_name = name or f"df{len(self._state.dfs) + 1}"
        updated_dfs = {**self._state.dfs, df_name: df}
        self._state = self._state.model_copy(update={"dfs": updated_dfs})

    def ask(self, query: str) -> Pipe:
        new_pipe = LazyPipe(self, self._executor, default_rows_limit=self._default_rows_limit)
        return new_pipe.ask(query)

    def _update_pipe_state(self, pipe_id: str, pipe_state: PipeState) -> None:
        """Internal method to update a pipe's state in the frozen SessionState."""
        updated_pipe_states = {**self._state.pipe_states, pipe_id: pipe_state}
        self._state = self._state.model_copy(update={"pipe_states": updated_pipe_states})

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def dbs(self) -> dict[str, Any]:
        return dict(self._state.dbs)

    @property
    def dfs(self) -> dict[str, DataFrame]:
        return dict(self._state.dfs)

    @property
    def name(self) -> str:
        return self._name

    @property
    def llm(self) -> BaseChatModel:
        return self._llm

    # Session no longer exposes executor; visualizer is still provided

    @property
    def visualizer(self) -> Visualizer:
        return self._visualizer
