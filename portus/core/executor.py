from abc import ABC, abstractmethod
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame
from pydantic import BaseModel, ConfigDict

from portus.configs.llm import LLMConfig

try:
    from duckdb import DuckDBPyConnection
except ImportError:
    DuckDBPyConnection = Any  # type: ignore

from .opa import Opa
from .session import Session


class ExecutionResult(BaseModel):
    text: str
    meta: dict[str, Any]
    code: str | None = None
    df: DataFrame | None = None

    # Pydantic v2 configuration: make the model immutable and allow pandas DataFrame
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class Executor(ABC):
    @abstractmethod
    def execute(
        self, session: Session, opas: list[Opa], llm: BaseChatModel, *, rows_limit: int = 100
    ) -> ExecutionResult:
        pass


class AgentExecutor(Executor):
    """Base class for agents that execute with a DuckDB connection and LLM configuration.

    Stores messages as private state that child classes can access during execution.
    Message history is preserved across multiple execute() calls.
    """

    def __init__(
        self,
        data_connection: DuckDBPyConnection,
        llm_config: LLMConfig,
    ):
        self._data_connection = data_connection
        self._llm_config = llm_config
        self._messages: list[Any] = []  # Store messages as private field for child classes
        self._processed_opa_count: int = 0  # Track how many opas we've already converted
