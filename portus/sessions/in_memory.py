from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame

from portus.configs.llm import LLMConfig
from portus.core import Executor, Pipe, Session, Visualizer

from ..agents.duckdb import SimpleDuckDBAgenticExecutor
from ..pipes.lazy import LazyPipe
from ..visualizers.dumb import DumbVisualizer


class InMemSession(Session):
    def __init__(
        self,
        name: str,
        llm_config: LLMConfig,
        *,
        data_executor: Executor | type[Executor] | None = None,
        visualizer: Visualizer | None = None,
        default_rows_limit: int = 1000,
    ):
        self._name = name
        self._llm = llm_config.chat_model

        self._dbs: dict[str, Any] = {}
        self._dfs: dict[str, DataFrame] = {}

        # Normalize to an executor type; always create per-pipe instance in ask()
        if isinstance(data_executor, type):
            self._executor_type = data_executor
        elif data_executor is not None:
            self._executor_type = type(data_executor)
        else:
            self._executor_type = SimpleDuckDBAgenticExecutor
        self._visualizer = visualizer or DumbVisualizer()
        self._default_rows_limit = default_rows_limit

    def add_db(self, connection: Any, *, name: str | None = None) -> None:
        conn_name = name or f"db{len(self._dbs) + 1}"
        self._dbs[conn_name] = connection

    def add_df(self, df: DataFrame, *, name: str | None = None) -> None:
        df_name = name or f"df{len(self._dfs) + 1}"
        self._dfs[df_name] = df

    def ask(self, query: str) -> Pipe:
        # Create a fresh executor per pipe
        executor = self._executor_type()
        return LazyPipe(self, executor, default_rows_limit=self._default_rows_limit).ask(query)

    @property
    def dbs(self) -> dict[str, Any]:
        return dict(self._dbs)

    @property
    def dfs(self) -> dict[str, DataFrame]:
        return dict(self._dfs)

    @property
    def name(self) -> str:
        return self._name

    @property
    def llm(self) -> BaseChatModel:
        return self._llm

    # Session no longer exposes executor; visualizer is still provided

    @property
    def visualizer(self) -> "Visualizer":
        return self._visualizer
