import abc
from abc import ABC
from typing import Optional

from pandas import DataFrame

from portus.result import Result, LazyResult
from langchain_core.language_models.chat_models import BaseChatModel


class Session(ABC):
    @abc.abstractmethod
    def add_db(self, engine: "Engine", *, name: Optional[str] = None) -> None:
        pass

    @abc.abstractmethod
    def add_df(self, df: DataFrame, *, name: Optional[str] = None) -> None:
        pass

    @abc.abstractmethod
    def ask(self, query: str) -> Result:
        pass


class SessionImpl(Session):
    def __init__(self, llm: BaseChatModel):
        self.__dbs: dict[str, object] = {}
        self.__dfs: dict[str, DataFrame] = {}
        self.__llm = llm

    def add_db(self, connection: "Engine", *, name: Optional[str] = None) -> None:
        conn_name = name or f"sqlalchemy{len(self.__dbs) + 1}_db"
        self.__dbs[conn_name] = connection

    def add_df(self, df: DataFrame, *, name: Optional[str] = None) -> None:
        df_name = name or f"dataframe{len(self.__dbs) + 1}"
        self.__dfs[df_name] = df

    def ask(self, query: str) -> Result:
        return LazyResult(query, self.__llm, self.__dbs, self.__dfs)
