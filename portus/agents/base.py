import pickle
from io import BytesIO
from typing import Any

from portus.configs.llm import LLMConfig
from portus.core import Executor, Session

try:
    from duckdb import DuckDBPyConnection
except ImportError:
    DuckDBPyConnection = Any  # type: ignore


class AgentExecutor(Executor):
    """
    Base class for agents that execute with a DuckDB connection and LLM configuration.
    """

    def _get_data_connection(self, session: Session) -> Any:
        """Get DuckDB connection from session."""
        from duckdb import DuckDBPyConnection

        dbs = session.dbs
        if not dbs:
            raise RuntimeError("No database connection available. Add a database to the session using session.add_db()")

        # Filter for DuckDB connections only
        duckdb_connections = [conn for conn in dbs.values() if isinstance(conn, DuckDBPyConnection)]

        if not duckdb_connections:
            raise RuntimeError(
                "No DuckDB connection found. LighthouseAgent requires a DuckDB connection. "
                "Use portus.sources.attach_postgres() or similar to attach external databases to DuckDB."
            )

        # Use the first DuckDB connection
        return duckdb_connections[0]

    def _get_llm_config(self, session: Session) -> LLMConfig:
        """Get LLM config from session."""
        return session.llm_config

    def _get_messages(self, session: Session, cache_scope: str) -> list[Any]:
        """Retrieve messages from the session cache."""
        try:
            buffer = BytesIO()
            session.cache.scoped(cache_scope).get("messages", buffer)
            buffer.seek(0)
            result: list[Any] = pickle.load(buffer)
            return result
        except (KeyError, EOFError):
            return []

    def _set_messages(self, session: Session, cache_scope: str, messages: list[Any]) -> None:
        """Store messages in the session cache."""
        buffer = BytesIO()
        pickle.dump(messages, buffer)
        buffer.seek(0)
        session.cache.scoped(cache_scope).put("messages", buffer)

    def _get_processed_opa_count(self, session: Session, cache_scope: str) -> int:
        """Retrieve processed OPA count from the session cache."""
        try:
            buffer = BytesIO()
            session.cache.scoped(cache_scope).get("processed_opa_count", buffer)
            buffer.seek(0)
            result: int = pickle.load(buffer)
            return result
        except (KeyError, EOFError):
            return 0

    def _set_processed_opa_count(self, session: Session, cache_scope: str, count: int) -> None:
        """Store processed OPA count in the session cache."""
        buffer = BytesIO()
        pickle.dump(count, buffer)
        buffer.seek(0)
        session.cache.scoped(cache_scope).put("processed_opa_count", buffer)
