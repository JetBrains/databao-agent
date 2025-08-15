from typing import Optional
from sqlalchemy import inspect as sqla_inspect, Engine


class DBEngineMetaFetcher:
    def __init__(self, engine: Engine, target_schema: Optional[str] = None):
        self.__engine = engine
        self.__target_schema = target_schema

    def dialect(self) -> str:
        return getattr(self.__engine.dialect, "name", "unknown")

    def schema(self, table_limit: int, cols_limit: int) -> str:
        inspector = sqla_inspect(self.__engine)
        default_schema = getattr(self.__engine.dialect, "default_schema_name", None)

        # Choose a schema to introspect
        schema_name = self.__target_schema or default_schema

        # Fallback: try to find a likely user schema if default not available
        if not schema_name:
            # noinspection PyBroadException
            try:
                schemas = inspector.get_schema_names()
            except Exception:
                schemas = []
            # Prefer public for Postgres, dbo for SQL Server, else first non-system
            preferred = ["public", "dbo"]
            schema_name = next((s for s in preferred if s in schemas), None) or next(
                (s for s in schemas if not s.startswith("pg_") and not s.startswith("information_schema")), None
            )

        # Collect tables
        # noinspection PyBroadException
        try:
            table_names = inspector.get_table_names(schema=schema_name)[:table_limit]
        except Exception:
            table_names = []

        # Build schema text
        lines = [f"-- Dialect: {self.dialect()}"]
        if schema_name:
            lines.append(f"-- Schema: {schema_name}")
        for t in table_names:
            # noinspection PyBroadException
            try:
                cols = inspector.get_columns(t, schema=schema_name)[:cols_limit]
            except Exception:
                cols = []
            col_parts = []
            for c in cols:
                cname = c.get("name", "unknown")
                ctype = str(c.get("type", ""))
                nullable = c.get("nullable", True)
                col_parts.append(f"{cname} {ctype}{' NULL' if nullable else ' NOT NULL'}")
            if col_parts:
                lines.append(f"TABLE {t} (")
                for i, part in enumerate(col_parts):
                    comma = "," if i < len(col_parts) - 1 else ""
                    lines.append(f"  {part}{comma}")
                lines.append(")")
            else:
                lines.append(f"TABLE {t}")

        return "\n".join(lines).strip()
