from pathlib import Path
from typing import Any

from databao_context_engine import DatabaoContextProjectManager, DatasourceType, DatasourceConfigFile, \
    BuildContextResult, ChunkEmbeddingMode


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

    def build_context(self) -> list[BuildContextResult]:
        return self.delegate.build_context(None, ChunkEmbeddingMode.EMBEDDABLE_TEXT_ONLY)

    @property
    def project_dir(self) -> Path:
        return self.delegate.project_dir