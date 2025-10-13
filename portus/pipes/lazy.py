import uuid
from typing import Any

from pandas import DataFrame

from portus.core import ExecutionResult, Executor, Opa, Pipe, Session, VisualisationResult
from portus.core.pipe import PipeState


class LazyPipe(Pipe):
    def __init__(
        self, session: Session, executor: Executor, *, default_rows_limit: int = 1000, pipe_id: str | None = None
    ):
        self._session = session
        self._executor = executor
        self._default_rows_limit = default_rows_limit
        self._id = pipe_id or str(uuid.uuid4())

        # Initialize state in session if not exists
        if self._id not in self._session.state.pipe_states:
            self._session._update_pipe_state(self._id, PipeState())

    def __materialize_data(self, rows_limit: int | None) -> ExecutionResult:
        rows_limit = rows_limit if rows_limit else self._default_rows_limit
        state = self.state
        if not state.data_materialized or rows_limit != state.data_materialized_rows:
            # Executor is stateless: it takes current state and returns result + updated state
            result, updated_state = self._executor.execute(
                self._session, state, self._session.llm, rows_limit=rows_limit
            )
            # Update the session's pipe state with the returned state
            self._session._update_pipe_state(self._id, updated_state)
        if self.state.data_result is None:
            raise RuntimeError("__data_result is None after materialization")
        return self.state.data_result

    def __materialize_visualization(self, request: str, rows_limit: int | None) -> VisualisationResult:
        self.__materialize_data(rows_limit)
        state = self.state
        if state.data_result is None:
            raise RuntimeError("__data_result is None after materialization")
        if not state.visualization_materialized:
            visualization_result = self._session.visualizer.visualize(request, self._session.llm, state.data_result)
            # Create new state with updated visualization (PipeState is frozen)
            updated_meta = {**state.meta, **visualization_result.meta, "plot_code": visualization_result.code}
            updated_state = state.model_copy(
                update={
                    "visualization_result": visualization_result,
                    "visualization_materialized": True,
                    "meta": updated_meta,
                }
            )
            self._session._update_pipe_state(self._id, updated_state)
        if self.state.visualization_result is None:
            raise RuntimeError("__visualization_result is None after materialization")
        return self.state.visualization_result

    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        return self.__materialize_data(rows_limit if rows_limit else self.state.data_materialized_rows).df

    def plot(self, request: str = "visualize data", *, rows_limit: int | None = None) -> Any | None:
        return self.__materialize_visualization(
            request, rows_limit if rows_limit else self.state.data_materialized_rows
        ).plot

    def text(self) -> str:
        return self.__materialize_data(self.state.data_materialized_rows).text

    def __str__(self) -> str:
        return self.text()

    def ask(self, query: str) -> Pipe:
        # Create new state with appended opa (PipeState is frozen)
        current_state = self.state
        updated_state = current_state.model_copy(update={"opas": [*current_state.opas, Opa(query=query)]})
        self._session._update_pipe_state(self._id, updated_state)
        return self

    @property
    def id(self) -> str:
        return self._id

    @property
    def state(self) -> PipeState:
        return self._session.state.pipe_states[self._id]

    @property
    def meta(self) -> dict[str, Any]:
        return self.state.meta

    @property
    def code(self) -> str | None:
        return self.__materialize_data(self.state.data_materialized_rows).code
