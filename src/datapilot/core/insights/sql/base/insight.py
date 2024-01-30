from abc import abstractmethod
from typing import Optional

from datapilot.core.insights.base.insight import Insight
from datapilot.schemas.sql import Dialect


class SqlInsight(Insight):
    NAME = "SqlInsight"

    def __init__(self, sql: str, dialect: Optional[Dialect], *args, **kwargs):
        self.sql = sql
        self.dialect = dialect
        super().__init__(*args, **kwargs)

    @abstractmethod
    def generate(self, *args, **kwargs) -> dict:
        pass
