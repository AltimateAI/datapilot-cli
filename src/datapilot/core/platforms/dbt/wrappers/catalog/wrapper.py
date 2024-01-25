from abc import ABC
from typing import Dict, Text


class BaseCatalogWrapper(ABC):
    pass

    def get_schema(self) -> Dict[Text, Dict[Text, Text]]:
        pass
