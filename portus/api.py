from portus.agents.lighthouse.agent import LighthouseAgent
from portus.configs.llm import DefaultLLMConfig, LLMConfig
from portus.core.executor import AgentExecutor

from .core import Session, Visualizer
from .sessions.in_memory import InMemSession
from .visualizers.dumb import DumbVisualizer


def open_session(
    name: str,
    *,
    llm_config: LLMConfig | None = None,
    data_executor: AgentExecutor | type[AgentExecutor] | None = None,
    visualizer: Visualizer | None = None,
    default_rows_limit: int = 1000,
) -> Session:
    return InMemSession(
        name,
        llm_config if llm_config else DefaultLLMConfig(),
        data_executor=data_executor or LighthouseAgent,
        visualizer=visualizer or DumbVisualizer(),
        default_rows_limit=default_rows_limit,
    )
