from abc import ABC
from abc import abstractmethod
from typing import Dict


class BaseCatalogWrapper(ABC):
    @abstractmethod
    def get_schema(self) -> Dict[str, Dict[str, str]]:
        pass
