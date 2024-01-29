from abc import ABC, abstractmethod
from typing import Dict, Text


class BaseCatalogWrapper(ABC):
    @abstractmethod
    def get_schema(self) -> Dict[Text, Dict[Text, Text]]:
        pass
