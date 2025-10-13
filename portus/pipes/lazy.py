from typing import Any

from pandas import DataFrame

from portus.core import ExecutionResult, Executor, Opa, Pipe, Session, VisualisationResult


class LazyPipe(Pipe):
    def __init__(self, session: Session, executor: Executor, *, default_rows_limit: int = 1000):
        self._session = session
        self._executor = executor
        self._default_rows_limit = default_rows_limit

        self._data_materialized = False
        self._data_materialized_rows: int | None = None
        self._data_result: ExecutionResult | None = None
        self._visualization_materialized = False
        self._visualization_result: VisualisationResult | None = None
        self._opas: list[Opa] = []
        self._meta: dict[str, Any] = {}

    def __materialize_data(self, rows_limit: int | None) -> ExecutionResult:
        rows_limit = rows_limit if rows_limit else self._default_rows_limit
        if not self._data_materialized or rows_limit != self._data_materialized_rows:
            self._data_result = self._executor.execute(
                self._session, self._opas, self._session.llm, rows_limit=rows_limit
            )
            self._data_materialized = True
            self._data_materialized_rows = rows_limit
            self._meta.update(self._data_result.meta)
        if self._data_result is None:
            raise RuntimeError("__data_result is None after materialization")
        return self._data_result

    def __materialize_visualization(self, request: str, rows_limit: int | None) -> VisualisationResult:
        self.__materialize_data(rows_limit)
        if self._data_result is None:
            raise RuntimeError("__data_result is None after materialization")
        if not self._visualization_materialized:
            self._visualization_result = self._session.visualizer.visualize(
                request, self._session.llm, self._data_result
            )
            self._visualization_materialized = True
            self._meta.update(self._visualization_result.meta)
            self._meta["plot_code"] = self._visualization_result.code  # maybe worth to expand as a property later
        if self._visualization_result is None:
            raise RuntimeError("__visualization_result is None after materialization")
        return self._visualization_result

    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        return self.__materialize_data(rows_limit if rows_limit else self._data_materialized_rows).df

    def plot(self, request: str = "visualize data", *, rows_limit: int | None = None) -> Any | None:
        return self.__materialize_visualization(
            request, rows_limit if rows_limit else self._data_materialized_rows
        ).plot

    def text(self) -> str:
        return self.__materialize_data(self._data_materialized_rows).text

    def __str__(self) -> str:
        return self.text()

    def ask(self, query: str) -> Pipe:
        self._opas.append(Opa(query=query))
        return self

    @property
    def meta(self) -> dict[str, Any]:
        return self._meta

    @property
    def code(self) -> str | None:
        return self.__materialize_data(self._data_materialized_rows).code
