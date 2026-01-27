from pathlib import Path

from pandas import DataFrame

from databao.core.data_source import Sources, DBConnectionConfig, DBDataSource, DFDataSource


class SourcesManager:

    def __init__(self):
        self.__sources: Sources = Sources(dfs={}, dbs={}, additional_context=[])

    def add_db(
        self,
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
    def _parse_context_arg(context: str | Path | None) -> str | None:
        if context is None:
            return None
        if isinstance(context, Path):
            return context.read_text()
        return context

    @property
    def sources(self) -> Sources:
        return self.__sources