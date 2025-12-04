from dataclasses import dataclass
from typing import TYPE_CHECKING

from langchain_core.messages import HumanMessage

if TYPE_CHECKING:
    from databao.core.opa import Opa


@dataclass
class OpaMetadata:
    opa: "Opa"
    is_materialized: bool = False
    message: HumanMessage | None = None
