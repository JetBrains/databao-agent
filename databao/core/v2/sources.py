from pathlib import Path
from typing import Any

from databao_context_engine.datasources.types import PreparedDatasource, PreparedConfig
from pandas import DataFrame

from databao.core.data_source import Sources, DBConnectionConfig, DBDataSource, DFDataSource


class SourcesManager:

    def __init__(self, prepared_data_sources: list[PreparedDatasource] | None = None):
        self.__sources: Sources = Sources(dfs={}, dbs={}, additional_context=[])
        self._add_prepared_ds(prepared_data_sources)

    def _add_prepared_ds(self, prepared_data_sources: list[PreparedDatasource] | None):
        if prepared_data_sources is None:
            return
        for ds in prepared_data_sources:
            if isinstance(ds, PreparedConfig):
                type = ds.datasource_type
                content = self._get_config_content(ds)
                name = ds.datasource_name
                self.add_db(DBConnectionConfig(type, content), name=name)
            else:
                ValueError("Only PreparedConfig is supported")

    def add_db(self,
        config: DBConnectionConfig,
        *,
        name: str | None = None,
        context: str | Path | None = None,
    ) -> DBDataSource:
        name = name or f"db{len(self.__sources.dbs) + 1}"
        context_text = self._parse_context_arg(context) or ""

        source = DBDataSource(name=name, context=context_text, db_connection=config)
        self.__sources.dbs[name] = source
        return source

    def add_df(
        self,
        data_frame: DataFrame,
        *,
        name: str | None = None,
        context: str | Path | None = None
    ) -> DFDataSource:
        name = name or f"df{len(self.__sources.dfs) + 1}"

        context_text = self._parse_context_arg(context) or ""

        source = DFDataSource(name=name, context=context_text, df=data_frame)
        self.__sources.dfs[name] = source
        return source

    def add_context(self, context: str | Path) -> None:
        text = self._parse_context_arg(context)
        if text is None:
            raise ValueError("Invalid context provided.")
        self.__sources.additional_context.append(text)

    @staticmethod
    def _get_config_content(ds: PreparedConfig) -> dict[str, Any]:
        return {str(k): v for k, v in ds.config.items()}

    @staticmethod
    def _parse_context_arg(context: str | Path | None) -> str | None:
        if context is None:
            return None
        if isinstance(context, Path):
            return context.read_text()
        return context

    @property
    def sources(self) -> Sources:
        return self.__sources