from dataclasses import dataclass
from typing import Any

import pandas as pd
from databao_context_engine import DatasourceType
from duckdb import DuckDBPyConnection
from sqlalchemy import Connection, Engine


@dataclass
class DBConnectionConfig:
    # TODO (dce): is it fine to have dependency on DCE type here?
    type: DatasourceType
    content: dict[str, Any]


DBConnectionRuntime = DuckDBPyConnection | Engine | Connection


DBConnection = DBConnectionConfig | DBConnectionRuntime


@dataclass
class DataSource:
    name: str
    context: str


@dataclass
class DFDataSource(DataSource):
    df: pd.DataFrame


@dataclass
class DBDataSource(DataSource):
    db_connection: DBConnection


@dataclass
class Sources:
    dfs: dict[str, DFDataSource]
    dbs: dict[str, DBDataSource]
    additional_context: list[str]
