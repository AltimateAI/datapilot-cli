from typing import Optional

from datapilot.core.platforms.dbt.insights import INSIGHTS
from datapilot.core.platforms.dbt_postgres.insights import INSIGHTS as POSTGRES_INSIGHTS
from datapilot.core.platforms.dbt_snowflake.insights import INSIGHTS as SNOWFLAKE_INSIGHTS


def get_insight_list(dialect: Optional[str] = None):
    if dialect == "postgres":
        return [*INSIGHTS, *POSTGRES_INSIGHTS]
    if dialect == "snowflake":
        return [*INSIGHTS, *SNOWFLAKE_INSIGHTS]
    return INSIGHTS
