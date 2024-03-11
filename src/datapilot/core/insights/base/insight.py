import logging
from abc import ABC
from abc import abstractmethod
from typing import ClassVar
from typing import Dict
from typing import Optional
from typing import Tuple

from datapilot.core.insights.schema import InsightResponse


class Insight(ABC):
    NAME = "Insight"
    TYPE = "BaseInsight"
    DESCRIPTION = "Base Insight"
    FILES_REQUIRED: ClassVar = []
    ALIAS = "base_insight"

    def __init__(self, config: Optional[Dict] = None, *args, **kwargs):
        self.config = config or {}
        self.args = args
        self.kwargs = kwargs
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def generate(self, *args, **kwargs) -> InsightResponse:
        pass

    @classmethod
    def has_all_required_data(cls, **kwargs) -> Tuple[bool, str]:
        """
        return False
        """
        return False, "Not implemented"
