from typing import Any

from portus.data_source.data_source import DataSource
from portus.duckdb.utils import is_sqlalchemy_engine, sqlalchemy_to_duckdb_mysql


class DatabaseSource(DataSource):

    def __init__(self, sqlalchemy_engine: Any, name: str):
        if is_sqlalchemy_engine(sqlalchemy_engine):
            self.engine = sqlalchemy_engine
        else:
            raise ValueError(f"Connection type '{type(sqlalchemy_engine)}' is not supported yet")
        self.name = name

    def register(self, connection) -> None:

        url = self.engine.url.render_as_string(hide_password=False)
        dialect = getattr(getattr(self.engine, "dialect", None), "name", "")
        if dialect.startswith("postgres"):
            connection.execute("INSTALL postgres_scanner;")
            connection.execute("LOAD postgres_scanner;")
            connection.execute(f"ATTACH '{url}' AS {self.name} (TYPE POSTGRES);")
        elif dialect.startswith(("mysql", "mariadb")):
            connection.execute("INSTALL mysql;")
            connection.execute("LOAD mysql;")
            mysql_url = sqlalchemy_to_duckdb_mysql(str(url))
            connection.execute(f"ATTACH '{mysql_url}' AS {self.name} (TYPE MYSQL);")
        else:
            raise ValueError(f"Database engine '{self.engine.dialect.name}' is not supported yet")

    def get_context(self) -> dict[str, Any]:
        return {}

    def get_schema(self) -> dict[str, str]:
        pass
