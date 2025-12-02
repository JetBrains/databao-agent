import uuid
from typing import TYPE_CHECKING, Any, Literal

from pandas import DataFrame
from typing_extensions import Self

from databao.core.executor import Executor, OutputModalityHints
from databao.core.opa import Opa
from databao.executors.executor_factory import get_executor

if TYPE_CHECKING:
    from databao.core.agent import Agent
    from databao.core.visualizer import VisualisationResult


class Thread:
    """A single conversational thread within an agent.

    - Maintains its own message history (isolated from other threads).
    - Materializes data and visualizations eagerly or lazily and caches results per thread.
    - Exposes helpers to get the latest dataframe/text/plot/code.
    """

    def __init__(
        self,
        agent: "Agent",
        executor_name: Literal["lighthouse", "react"],
        *,
        rows_limit: int = 1000,
        stream_ask: bool = True,
        stream_plot: bool = False,
        lazy: bool = False,
        auto_output_modality: bool = True,
    ):
        self._agent = agent

        # A unique cache scope so executors can store per-thread state (e.g., message history)
        self._cache_scope = f"{self._agent.name}/{uuid.uuid4()}"
        self._cache = self._agent.cache.scoped(self._cache_scope)

        self._executor: Executor = get_executor(
            executor_name,
            self._cache,
            self._agent.llm_config,
            conn_manager=agent._connection_manager,
            rows_limit=rows_limit,
        )

        self._rows_limit = rows_limit

        self._lazy_mode = lazy

        self._auto_output_modality = auto_output_modality
        """Automatically detect the appropriate modality to output based on the user's input. If False, you must
        manually call the appropriate ask/plot method.
        
        This allows .ask to be used for plotting, i.e. `ask("show a bar chart")` will result in a plot being generated.
        """

        self._stream_ask: bool = stream_ask
        self._stream_plot: bool = stream_plot

        self._visualization_result: VisualisationResult | None = None
        self._visualization_request: str | None = None

        self._meta: dict[str, Any] = {}

    # def _materialize_data(self, rows_limit: int | None) -> "ExecutionResult":
    #     """Materialize the latest data state by executing pending OPAs if needed."""
    #     new_opas = self._opas[self._opas_processed_count :]
    #     if len(new_opas) > 0:
    #         rows_limit = rows_limit if rows_limit else self._default_rows_limit
    #         stream = self._stream_ask if self._stream_ask is not None else self._default_stream_ask
    #         self._data_result = self._agent.executor.execute(
    #             new_opas,
    #             cache=self._agent.cache.scoped(self._cache_scope),
    #             llm_config=self._agent.llm_config,
    #             sources=self._agent.sources,
    #             rows_limit=rows_limit,
    #             stream=stream,
    #         )
    #         self._meta.update(self._data_result.meta)
    #         self._opas_processed_count += len(new_opas)
    #         self._data_materialized_rows = rows_limit
    #     if self._data_result is None:
    #         raise RuntimeError("_data_result is None after materialization")
    #     return self._data_result

    def _materialize_visualization(self, request: str | None) -> "VisualisationResult":
        """Materialize latest visualization for the given request and current data."""
        data = self._executor.result
        if data is None:
            raise RuntimeError("No result to visualize")
        if self._visualization_result is None or request != self._visualization_request:
            # TODO Cache visualization results as in Executor.execute()?
            stream = self._stream_plot
            self._visualization_result = self._agent.visualizer.visualize(request, data, stream=stream)
            self._visualization_request = request
            self._meta.update(self._visualization_result.meta)
            self._meta["plot_code"] = self._visualization_result.code  # maybe worth to expand as a property later
        if self._visualization_result is None:
            raise RuntimeError("_visualization_result is None after materialization")
        return self._visualization_result

    # def _materialize(self, rows_limit: int | None) -> None:
    #     data_result = self._materialize_data(rows_limit)
    #
    #     if not self._auto_output_modality:
    #         return
    #
    #     # The Executor can provide output modality hints
    #     hints = data_result.meta.get(OutputModalityHints.META_KEY, OutputModalityHints())
    #     if not hints.should_visualize:
    #         return
    #
    #     # Let the Visualizer recommend a plot based on the df if no prompt is provided (None)
    #     self.plot(hints.visualization_prompt)

    @property
    def executor(self) -> Executor:
        return self._executor

    def text(self) -> str:
        """Return the latest textual answer from the executor/LLM."""
        return self._executor.result.text if self._executor.result is not None else ""

    def code(self) -> str | None:
        """Return the latest generated code."""
        return self._executor.result.code if self._executor.result is not None else None

    def meta(self) -> dict[str, Any]:
        """Aggregated metadata from executor/visualizer for this thread."""
        meta = self._executor.result.meta if self._executor.result is not None else {}
        self._meta.update(meta)
        return self._meta

    def df(self) -> DataFrame | None:
        """Return the latest dataframe, materializing data as needed."""
        res = self._executor.result
        if res is None:
            return None
        df = res.df
        # Copy the dataframe to avoid state mutation from outside
        return df.copy() if df is not None else None

    def plot(self, request: str | None = None, stream: bool | None = None) -> "VisualisationResult":
        """Generate or return the latest visualization for the current data.

        Args:
            request: Optional natural-language plotting request.
            stream: Optional stream mode for output.
        """
        if stream is not None:
            self._stream_plot = stream
        return self._materialize_visualization(request)

    def ask(self, query: str, *, rows_limit: int | None = None, stream: bool | None = None) -> Self:
        """Append a new user query to this thread.

        Returns self to allow chaining (e.g., thread.ask("...")).

        Setting rows_limit has no effect in lazy mode.
        """
        # NB. A new Opa is created even if it's identical to the previous one.
        stream = stream if stream is not None else self._stream_ask
        self._executor.add_opa(Opa(query), self._lazy_mode, stream, rows_limit=rows_limit)

        # Invalidate old results so they are not used by repr methods
        self._visualization_result = None

        return self

    def drop(self, n: int = 1) -> None:
        """Remove N last user queries from this thread along with the answer it produced."""
        self._executor.drop_last_opa(n=n)

        print(
            f"Dropped last {n} operation{'s' if n > 1 else ''}. Last remaining operation:"
            f"\n{self._executor.opas[-1].opa.query}"
        )

    def __str__(self) -> str:
        if self._executor.result is not None:
            bundle = self._executor.result._repr_mimebundle_()
            if bundle is not None:
                if (text_markdown := bundle.get("text/markdown")) is not None:
                    return text_markdown  # type: ignore[no-any-return]
                elif (text_plain := bundle.get("text/plain")) is not None:
                    return text_plain  # type: ignore[no-any-return]
        return repr(self)

    def __repr__(self) -> str:
        if self._executor.result is not None:
            return (
                f"Materialized {self.__class__.__name__} with "
                f"{len(self._executor.result.df) if self._executor.result.df is not None else 0} data rows."
            )
        else:
            return f"Unmaterialized {self.__class__.__name__}."

    def _repr_mimebundle_(self, include: Any = None, exclude: Any = None) -> dict[str, Any] | None:
        """Return MIME bundle for rendering in notebooks.

        No materialization is performed in this method. If using lazy mode, you must trigger materialization manually.
        """
        # See docs for the behavior of magic methods https://ipython.readthedocs.io/en/stable/config/integrating.html#custom-methods
        # If None is returned, IPython will fall back to repr()
        if self._executor.result is None:
            return None
        modality_hints = self._executor.result.meta.get(OutputModalityHints.META_KEY, OutputModalityHints())
        plot_bundle: dict[str, Any] | None = None
        if modality_hints.should_visualize and self._visualization_result is not None:
            plot_bundle = self._visualization_result._repr_mimebundle_(include, exclude)
        bundle = self._executor.result._repr_mimebundle_(include, exclude, plot_mimebundle=plot_bundle)
        return bundle
