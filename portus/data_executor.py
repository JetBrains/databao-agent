from abc import ABC, abstractmethod
from dataclasses import dataclass

from pandas import DataFrame
from langchain_core.language_models.chat_models import BaseChatModel


@dataclass(frozen=True)
class DataResult:
    text: str
    df: DataFrame
    meta: dict[str, object]


class DataExecutor(ABC):
    @abstractmethod
    def execute(self, query: str, llm: BaseChatModel, dbs: dict[str, object], dfs: dict[str, DataFrame]) -> DataResult:
        pass
