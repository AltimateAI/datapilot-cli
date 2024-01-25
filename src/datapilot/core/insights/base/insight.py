import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Text, Tuple

from datapilot.core.insights.schema import InsightResponse


class Insight(ABC):
    def __init__(self, config: Optional[Dict] = None, *args, **kwargs):
        self.config = config or {}
        self.args = args
        self.kwargs = kwargs
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def generate(self, *args, **kwargs) -> InsightResponse:
        pass

    @classmethod
    def has_all_required_data(cls, **kwargs) -> Tuple[bool, Text]:
        """
        return False
        """
        return False, "Not implemented"
