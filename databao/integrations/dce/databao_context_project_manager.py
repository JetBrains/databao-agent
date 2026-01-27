from pathlib import Path
from typing import Any

from databao_context_engine import DatabaoContextProjectManager, DatasourceType, DatasourceConfigFile, \
    BuildContextResult, ChunkEmbeddingMode
from databao_context_engine.datasources.datasource_discovery import discover_datasources, prepare_source, logger
from databao_context_engine.datasources.types import PreparedDatasource


class DatabaoContextProjectManagerApi:
    def __init__(self, delegate: DatabaoContextProjectManager):
        self.delegate = delegate

    def create_datasource_config(
        self,
        datasource_type: DatasourceType,
        datasource_name: str,
        config_content: dict[str, Any]
    ) -> DatasourceConfigFile:
        return self.delegate.create_datasource_config(datasource_type, datasource_name, config_content)

    # TODO (dce): should be implemented on the DCE side
    def get_prepared_datasource_list(self) -> list[PreparedDatasource]:
        result = []
        for discovered_datasource in discover_datasources(project_dir=self.project_dir):
            try:
                prepared_source = prepare_source(discovered_datasource)
            except Exception as e:
                logger.debug(str(e), exc_info=True, stack_info=True)
                logger.info(f"Invalid source at ({discovered_datasource.path}): {str(e)}")
                continue
            result.append(prepared_source)
        return result

    def build_context(self) -> list[BuildContextResult]:
        return self.delegate.build_context(None, ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY)

    @property
    def project_dir(self) -> Path:
        return self.delegate.project_dir