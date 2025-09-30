from pathlib import Path
from typing import Optional, Any

from pandas import DataFrame

from portus.agents.lighthouse_agent import LighthouseAgent
from portus.core.llms import LLMConfig
from portus.core.pipe import Pipe
from portus.data_source.data_collection import DataCollection
from portus.data_source.data_source import DataSource

from portus.langchain_graphs.execute_submit import ExecuteSubmit
from portus.session import BaseSession
from portus.pipe import BasePipe
from portus.vizualizer import DumbVisualizer, Visualizer

DEFAULT_TEMPLATE_PATH = Path("agent_system.jinja")


class Session(BaseSession):
    def __init__(
            self,
            name: str,
            llm_config: LLMConfig,
            *,
            visualizer=DumbVisualizer(),
            default_rows_limit: int = 1000
    ):
        self._name = name
        self._llm_config = llm_config

        self._visualizer = visualizer
        self._default_rows_limit = default_rows_limit

        self._data_collection = DataCollection()

    def add_db(self, connection: Any, *, name: Optional[str] = None) -> None:
        self._data_collection.add_db(connection, name)

    def add_df(self, df: DataFrame, *, name: Optional[str] = None) -> None:
        self._data_collection.add_df(df, name)

    def ask(self, query: str) -> BasePipe:
        agent = LighthouseAgent(self._data_collection, ExecuteSubmit(self._data_collection.get_connection()),
                                self._llm_config, DEFAULT_TEMPLATE_PATH)
        return Pipe(agent, self._visualizer).ask(query)

    @property
    def sources(self) -> list[DataSource]:
        return self._data_collection.get_sources()

    @property
    def name(self) -> str:
        return self._name

    @property
    def visualizer(self) -> Visualizer:
        return self._visualizer

