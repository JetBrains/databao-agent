from __future__ import annotations

import abc
from abc import ABC
from typing import TYPE_CHECKING, Any

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from .executor import ExecutionResult
    from .opa import Opa
    from .visualizer import VisualisationResult


class PipeState(BaseModel):
    """Container for pipe stateful variables."""

    data_materialized: bool = False
    data_materialized_rows: int | None = None
    data_result: ExecutionResult | None = None
    visualization_materialized: bool = False
    visualization_result: VisualisationResult | None = None
    opas: list[Opa] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(
        frozen=True,  # Makes the model immutable
        arbitrary_types_allowed=True,  # Allows ExecutionResult, VisualisationResult, Opa
    )


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
    def ask(self, query: str) -> Pipe:
        pass

    @property
    @abc.abstractmethod
    def id(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def state(self) -> PipeState:
        pass

    @property
    @abc.abstractmethod
    def meta(self) -> dict[str, Any]:
        pass

    @property
    @abc.abstractmethod
    def code(self) -> str | None:
        pass
