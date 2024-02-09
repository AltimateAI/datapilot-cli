from typing import Optional

from datapilot.core.platforms.dbt.insights import INSIGHTS
from datapilot.core.platforms.dbt_postgres.insights import DBTPostgresLoggedTables


def get_insight_list(dialect: Optional[str] = None):
    if dialect == "postgres":
        return [*INSIGHTS, DBTPostgresLoggedTables]
    return INSIGHTS
