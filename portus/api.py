from portus.agents.lighthouse.agent import LighthouseAgent
from portus.configs.llm import LLMConfig, LLMConfigDirectory

from .caches.in_mem_cache import InMemCache
from .core import Cache, Executor, Session, Visualizer
from .visualizers.vega_chat import VegaChatVisualizer


def open_session(
    name: str,
    *,
    llm_config: LLMConfig | None = None,
    data_executor: Executor | None = None,
    visualizer: Visualizer | None = None,
    cache: Cache | None = None,
    default_rows_limit: int = 1000,
) -> Session:
    llm_config = llm_config if llm_config else LLMConfigDirectory.DEFAULT
    return Session(
        name,
        llm_config,
        data_executor=data_executor or LighthouseAgent(),
        visualizer=visualizer or VegaChatVisualizer(llm_config),
        cache=cache or InMemCache(),
        default_rows_limit=default_rows_limit,
    )
