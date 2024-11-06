from abc import abstractmethod

from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight


class SqlInsight(ChecksInsight):
    NAME = "SqlInsight"

    @abstractmethod
    def generate(self, *args, **kwargs) -> dict:
        pass
