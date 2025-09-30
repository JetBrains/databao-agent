import logging
from typing import Union

from portus.agents.lighthouse_agent import LighthouseAgent
from portus.core.llms import LLMConfig
from portus.session import BaseSession
from portus.core.in_mem_session import Session
from portus.agent import Agent
from portus.duckdb.agent import SimpleDuckDBAgenticExecutor
from portus.vizualizer import Visualizer, DumbVisualizer

logger = logging.getLogger(__name__)
# Attach a NullHandler so importing apps without logging config don’t get warnings.
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def open_session(
        name: str,
        *,
        llm: Union[str, LLMConfig] = "claude-sonnet-4-5-20250929",
        visualizer: Visualizer = DumbVisualizer(),
        default_rows_limit: int = 1000
) -> BaseSession:
    return Session(
        name,
        llm if isinstance(llm, LLMConfig) else LLMConfig(name=llm),
        visualizer=visualizer,
        default_rows_limit=default_rows_limit
    )
