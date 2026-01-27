from pathlib import Path

from databao_context_engine import DatabaoContextEngine, ContextSearchResult


class DatabaoContextEngineApi:
    def __init__(self, delegate: DatabaoContextEngine):
        self._delegate = delegate

    def search_context(self, retrieve_text: str) -> list[ContextSearchResult]:
        return self._delegate.search_context(retrieve_text, None, None, False)

    @property
    def project_dir(self) -> Path:
        return self.delegate.project_dir