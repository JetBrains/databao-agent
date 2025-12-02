from databao.configs.llm import LLMConfig
from databao.core.cache import Cache
from databao.core.connection_manager import ConnectionManager
from databao.core.executor import Executor
from databao.executors.connection_managers.duckdb_manager import DuckDBManager
from databao.executors.lighthouse.executor import LighthouseExecutor
from databao.executors.react_duckdb.executor import ReactDuckDBExecutor


def get_executor(
    executor_name: str, cache: Cache, llm_config: LLMConfig, conn_manager: ConnectionManager, rows_limit: int
) -> Executor:
    if executor_name == "lighthouse":
        assert isinstance(conn_manager, DuckDBManager)
        executor: Executor = LighthouseExecutor(cache, llm_config, conn_manager=conn_manager, rows_limit=rows_limit)
    elif executor_name == "react":
        assert isinstance(conn_manager, DuckDBManager)
        executor = ReactDuckDBExecutor(cache, llm_config, conn_manager=conn_manager, rows_limit=rows_limit)
    else:
        raise ValueError(f"Unknown executor: {executor_name}")
    return executor


def get_connection_manager(executor_name: str) -> ConnectionManager:
    if executor_name == "lighthouse":
        return DuckDBManager()
    else:
        return DuckDBManager()
