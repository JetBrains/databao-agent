from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from langchain_core.language_models.chat_models import BaseChatModel
from pandas import DataFrame
from pydantic import BaseModel, ConfigDict

from .session import Session

if TYPE_CHECKING:
    from .pipe import PipeState


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
        self, session: Session, pipe_state: "PipeState", llm: BaseChatModel, *, rows_limit: int = 100
    ) -> tuple[ExecutionResult, "PipeState"]:
        """
        Execute the pipe operations in a stateless manner.

        Args:
            session: The session context
            pipe_state: The current pipe state (input)
            llm: The language model to use
            rows_limit: Maximum number of rows to return

        Returns:
            A tuple of (ExecutionResult, updated PipeState)
        """
        pass
