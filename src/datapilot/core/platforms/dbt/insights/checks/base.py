from abc import abstractmethod
from typing import ClassVar
from typing import Tuple

from datapilot.core.platforms.dbt.insights.base import DBTInsight


class ChecksInsight(DBTInsight):
    TYPE = "Checks"
    FILES_REQUIRED: ClassVar = ["Manifest"]

    @abstractmethod
    def generate(self, *args, **kwargs) -> dict:
        pass

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        """
        Check if all required data is available for the insight to run.
        :param has_manifest: A boolean indicating if manifest is available.
        :return: A boolean indicating if all required data is available.
        """
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        return True, ""
