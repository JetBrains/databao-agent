import abc
from abc import ABC
from typing import Optional, Any

from pandas import DataFrame


class BasePipe(ABC):
    """Pipe contains Agent and history of messages.
    It returns results of the last execution.
    After calling `ask` method, Pipe is changing its state."""
    @property
    @abc.abstractmethod
    def df(self, *, rows_limit: Optional[int] = None) -> DataFrame | None:
        pass

    @property
    @abc.abstractmethod
    def plot(self, request: str | None = None, *, rows_limit: Optional[int] = None) -> Any:
        pass

    @property
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
    def sql(self) -> str | None:
        pass
