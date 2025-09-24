from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypedDict, Any, Optional

from pandas import DataFrame
from langchain_core.language_models.chat_models import BaseChatModel
from portus.opa import Opa


@dataclass(frozen=True)
class ExecutionResult:
    text: str
    meta: dict[str, Any]
    code: Optional[str] = None
    df: Optional[DataFrame] = None


class Executor(ABC):
    @abstractmethod
    def execute(
            self,
            opas: list[Opa],
            llm: BaseChatModel,
            dbs: dict[str, Any],
            dfs: dict[str, DataFrame],
            *,
            rows_limit: int = 100
    ) -> ExecutionResult:
        pass
