from typing import Literal

from databao.caches.in_mem_cache import InMemCache
from databao.configs.llm import LLMConfig, LLMConfigDirectory
from databao.core.agent import Agent
from databao.core.cache import Cache
from databao.core.visualizer import Visualizer
from databao.visualizers.vega_chat import VegaChatVisualizer


def new_agent(
    name: str | None = None,
    llm_config: LLMConfig | None = None,
    data_executor: Literal["lighthouse", "react"] | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    rows_limit: int = 1000,
    stream_ask: bool = True,
    stream_plot: bool = False,
    lazy_threads: bool = False,
    auto_output_modality: bool = True,
) -> Agent:
    """This is an entry point for users to create a new agent.
    Agent can't be modified after it's created. Only new data sources can be added.
    """

    llm_config = llm_config if llm_config else LLMConfigDirectory.DEFAULT
    if data_executor is None:
        data_executor = "lighthouse"
    return Agent(
        llm_config,
        name=name or "default_agent",
        executor_name=data_executor,
        visualizer=visualizer or VegaChatVisualizer(llm_config),
        cache=cache or InMemCache(),
        rows_limit=rows_limit,
        stream_ask=stream_ask,
        stream_plot=stream_plot,
        lazy_threads=lazy_threads,
        auto_output_modality=auto_output_modality,
    )
