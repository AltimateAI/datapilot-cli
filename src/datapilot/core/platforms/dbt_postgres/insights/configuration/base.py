from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

from datapilot.core.insights.schema import Severity
from datapilot.core.platforms.dbt.insights.base import DBTInsight


class DBTConfigurationInsight(DBTInsight):
    NAME = "DBTConfigurationInsight"
    TYPE = "configuration"
    DEFAULT_SEVERITY = Severity.WARNING

    def __init__(self, config: Optional[Dict[str, Any]] = None, *args, **kwargs):
        self.config = config or {}
        super().__init__(*args, **kwargs)

    @abstractmethod
    def generate(self, *args, **kwargs) -> dict:
        pass

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, **kwargs) -> Tuple[bool, str]:
        """
        Check if all required data is available for the insight to run.
        :param has_manifest: A boolean indicating if manifest is available.
        :return: A boolean indicating if all required data is available.
        """
        if not has_manifest:
            return False, "manifest is required for insight to run."
        return True, ""
