from typing import Optional, Any

from pandas import DataFrame

from portus.executor import ExecutionResult
from portus.opa import Opa
from portus.pipe import Pipe
from portus.session import Session
from portus.vizualizer import VisualisationResult


class LazyPipe(Pipe):
    def __init__(
            self,
            session: Session,
            *,
            default_rows_limit: int = 1000
    ):
        self.__session = session
        self.__default_rows_limit = default_rows_limit

        self.__data_materialized = False
        self.__data_materialized_rows: Optional[int] = None
        self.__data_result: Optional[ExecutionResult] = None
        self.__visualization_materialized = False
        self.__visualization_result: Optional[VisualisationResult] = None
        self.__opas: list[Opa] = []
        self.__meta = {}

    def __materialize_data(self, rows_limit: Optional[int]) -> ExecutionResult:
        rows_limit = rows_limit if rows_limit else self.__default_rows_limit
        if not self.__data_materialized or rows_limit != self.__data_materialized_rows:
            self.__data_result = self.__session.executor.execute(self.__session, self.__opas, self.__session.llm,
                                                                 rows_limit=rows_limit)
            self.__data_materialized = True
            self.__data_materialized_rows = rows_limit
            self.__meta.update(self.__data_result.meta)
        return self.__data_result

    def __materialize_visualization(self, request: str, rows_limit: Optional[int]) -> VisualisationResult:
        self.__materialize_data(rows_limit)
        if not self.__visualization_materialized:
            self.__visualization_result = self.__session.visualizer.visualize(request, self.__session.llm,
                                                                              self.__data_result)
            self.__visualization_materialized = True
            self.__meta.update(self.__visualization_result.meta)
            self.__meta["plot_code"] = self.__visualization_result.code  # maybe worth to expand as a property later
        return self.__visualization_result

    def df(self, *, rows_limit: Optional[int] = None) -> Optional[DataFrame]:
        return self.__materialize_data(rows_limit if rows_limit else self.__data_materialized_rows).df

    def plot(self, request: str = "visualize data", *, rows_limit: Optional[int] = None) -> Optional[Any]:
        return self.__materialize_visualization(request,
                                                rows_limit if rows_limit else self.__data_materialized_rows).plot

    def text(self) -> str:
        return self.__materialize_data(self.__data_materialized_rows).text

    def __str__(self):
        return self.text()

    def ask(self, query: str) -> Pipe:
        self.__opas.append(Opa(query=query))
        return self

    @property
    def meta(self) -> dict[str, Any]:
        return self.__meta

    @property
    def code(self) -> Optional[str]:
        return self.__materialize_data(self.__data_materialized_rows).code
