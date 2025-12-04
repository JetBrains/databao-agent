from abc import ABC, abstractmethod

from databao.core.data_source import DBDataSource, DFDataSource, Sources


class ConnectionManager(ABC):
    @abstractmethod
    def register_db(self, source: DBDataSource) -> None:
        pass

    @abstractmethod
    def register_df(self, source: DFDataSource) -> None:
        pass

    @property
    @abstractmethod
    def sources(self) -> Sources:
        pass
