from pathlib import Path

from databao_context_engine import init_dce_project, DatabaoContextEngine

from databao.integrations.dce.databao_context_project_manager import DatabaoContextProjectManagerApi
from databao.integrations.dce.databao_engine import DatabaoContextEngineApi


class DatabaoApi:
    @staticmethod
    def init_dce_project(project_dir: Path) -> DatabaoContextProjectManagerApi:
        manager = init_dce_project(project_dir)
        return DatabaoContextProjectManagerApi(manager)

    # TODO (dce): implement (check - do we have a context for DCE?)
    @staticmethod
    def get_dce(project_dir: Path) -> DatabaoContextEngineApi:
        engine = DatabaoContextEngine(project_dir)
        return DatabaoContextEngineApi(engine)

