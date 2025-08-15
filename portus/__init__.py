import sys
import inspect
import logging

from typing import Optional
from sqlalchemy import Engine

from portus.sql_gen import SqlGen, OneShotSqlGen


def init():
    """
    Monkey-patch an already-imported pandas module in the current execution context
    by adding a `read_ai` function. This function will:
      - Fetch SQL schema information from the provided engine
      - Use a LangChain LLM to generate an SQL query based on the prompt and schema
      - Execute the generated query and return a DataFrame
    The function does NOT import pandas and does NOT return the module.
    It only patches if pandas is already imported in the same context.
    """
    pd_mod = sys.modules.get("pandas")

    # If not found via sys.modules, try to detect a pandas module object in caller globals.
    if pd_mod is None:
        # noinspection PyUnusedLocal
        caller_frame = None
        try:
            caller_frame = inspect.currentframe().f_back
            if caller_frame is not None:
                for val in caller_frame.f_globals.values():
                    if getattr(val, "__name__", None) == "pandas":
                        pd_mod = val
                        break
        finally:
            # Help GC by removing frame references
            del caller_frame

    if pd_mod is None:
        # pandas is not imported in the current context; nothing to patch
        return

    if hasattr(pd_mod, "read_ai"):
        # Already patched; do nothing
        return

    def read_ai(
            prompt: str,
            engine: Engine,
            *,
            sqlgen: SqlGen = OneShotSqlGen(),
            target_schema: Optional[str] = None,
            table_limit: int = 100,
            columns_per_table_limit: int = 50,
            model: Optional[str] = None,
            temperature: float = 0.0,
            llm: Optional[object] = None,
            max_rows: Optional[int] = 1000,
            **kwargs,
    ):
        """
        Parameters:
        - prompt: str. Natural language instruction or question.
        - engine: SQLAlchemy engine/connection (required).
        - target_schema: optional schema name to introspect (defaults to engine default).
        - table_limit: limit number of tables included in the schema prompt.
        - columns_per_table_limit: limit number of columns per table in the schema prompt.
        - model: optional model name for the LLM (depends on provider).
        - temperature: LLM temperature.
        - llm: optional pre-instantiated LangChain chat model. If not provided, a default will be created.
        - max_rows: desired LIMIT to suggest to the LLM if query doesn't include a limit.
        - **kwargs: forwarded to pandas.read_sql for execution (e.g., params=...).

        Returns: pandas.DataFrame with the result of executing the generated SQL.
        """

        sql_query = sqlgen.gen(
            prompt=prompt,
            engine=engine,
            target_schema=target_schema,
            table_limit=table_limit,
            columns_per_table_limit=columns_per_table_limit,
            model=model,
            temperature=temperature,
            llm=llm,
            max_rows=max_rows
        )
        return pd_mod.read_sql(sql_query, engine, **kwargs)

    setattr(pd_mod, "read_ai", read_ai)

    # set up logging
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    handler.setLevel(logging.INFO)

    lib_logger = logging.getLogger("portus")
    lib_logger.setLevel(logging.INFO)
    lib_logger.addHandler(handler)
    lib_logger.propagate = False
