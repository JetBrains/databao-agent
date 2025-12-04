import duckdb
from sqlalchemy import Connection, Engine

from databao.core.connection_manager import ConnectionManager
from databao.core.data_source import DBDataSource, DFDataSource, Sources
from databao.duckdb.utils import get_db_path, register_sqlalchemy


class DuckDBManager(ConnectionManager):
    def __init__(self) -> None:
        self.duckdb_connection = duckdb.connect(":memory:")
        self._sources: Sources = Sources(dfs={}, dbs={}, additional_context=[])

    def register_db(self, source: DBDataSource) -> None:
        """Register DB in the DuckDB connection."""
        self.sources.dbs[source.name] = source
        connection = source.db_connection
        if isinstance(connection, Connection):
            connection = connection.engine

        if isinstance(connection, duckdb.DuckDBPyConnection):
            path = get_db_path(connection)
            if path is not None:
                connection.close()
                self.duckdb_connection.execute(f"ATTACH '{path}' AS {source.name} (READ_ONLY)")
            else:
                raise RuntimeError("Memory-based DuckDB is not supported.")
        elif isinstance(connection, Engine):
            register_sqlalchemy(self.duckdb_connection, connection, source.name)
        else:
            raise ValueError("Only DuckDB or SQLAlchemy connections are supported.")

    def register_df(self, source: DFDataSource) -> None:
        self.sources.dfs[source.name] = source
        self.duckdb_connection.register(source.name, source.df)

    @property
    def sources(self) -> Sources:
        return self._sources
