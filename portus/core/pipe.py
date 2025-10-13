import abc
from abc import ABC
from typing import TYPE_CHECKING, Any

from pandas import DataFrame

if TYPE_CHECKING:
    from .executor import Executor


class Pipe(ABC):
    @abc.abstractmethod
    def df(self, *, rows_limit: int | None = None) -> DataFrame | None:
        pass

    @abc.abstractmethod
    def plot(self, request: str = "visualize data", *, rows_limit: int | None = None) -> Any | None:
        pass

    @abc.abstractmethod
    def text(self) -> str:
        pass

    @abc.abstractmethod
    def ask(self, query: str) -> "Pipe":
        pass

    @property
    @abc.abstractmethod
    def meta(self) -> dict[str, Any]:
        pass

    @property
    @abc.abstractmethod
    def code(self) -> str | None:
        pass

    @property
    @abc.abstractmethod
    def executor(self) -> "Executor":
        pass
