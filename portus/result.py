import abc
from abc import ABC

from pandas import DataFrame
from langchain_core.language_models.chat_models import BaseChatModel

from portus.duck_db import SimpleDuckDBAgenticExecutor


class Result(ABC):
    @abc.abstractmethod
    def df(self) -> DataFrame:
        pass

    @abc.abstractmethod
    def plot(self):
        pass

    @abc.abstractmethod
    def meta(self) -> dict[str, object]:
        pass


class LazyResult(Result):
    def __init__(
            self,
            query: str,
            llm: BaseChatModel,
            dbs: dict[str, object],
            dfs: dict[str, DataFrame]
    ):
        self.__query = query
        self.__llm = llm
        self.__dbs = dict(dbs)
        self.__dfs = dict(dfs)
        self.__data_executor = SimpleDuckDBAgenticExecutor()

        self.__materialized = False
        self.__materialized_text = None
        self.__materialized_df = None
        self.__materialized_meta = None

    def __materialize(self):
        if not self.__materialized:
            result = self.__data_executor.execute(self.__query, self.__llm, self.__dbs, self.__dfs)
            self.__materialized_df = result.df
            self.__materialized_text = result.text
            self.__materialized = True

    def df(self) -> DataFrame:
        self.__materialize()
        return self.__materialized_df

    def plot(self):
        pass

    def meta(self) -> dict[str, object]:
        if self.__materialized:
            return self.__materialized_meta
        raise ValueError("Result is not materialized")

    def __str__(self):
        self.__materialize()
        return self.__materialized_text
