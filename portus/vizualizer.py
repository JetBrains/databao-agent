from dataclasses import dataclass
from portus.agent import ExecutionResult
from abc import abstractmethod, ABC
from typing import Any, Optional


@dataclass(frozen=True)
class VisualisationResult:
    text: str
    meta: dict[str, Any]
    plot: Optional[Any]
    code: Optional[str]


class Visualizer(ABC):
    @abstractmethod
    def visualize(self, request: str, data: ExecutionResult) -> VisualisationResult:
        pass


class DumbVisualizer(Visualizer):
    def visualize(self, request: str, data: ExecutionResult) -> VisualisationResult:
        return VisualisationResult("", {}, data.df.plot(kind="bar"), "")
