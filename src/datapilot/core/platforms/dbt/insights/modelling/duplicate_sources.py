from collections import defaultdict
from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTProjectInsightResponse
from datapilot.core.platforms.dbt.utils import get_table_name_from_source
from datapilot.utils.formatting.utils import numbered_list


class DBTDuplicateSources(DBTModellingInsight):
    """
    Check if the DBT project has duplicate sources.
    Ref: https://github.com/dbt-labs/dbt-project-evaluator/blob/main/models/marts/dag/fct_duplicate_sources.sql
    """

    NAME = "Duplicate sources"
    ALIAS = "Duplicate_Sources"
    DESCRIPTION = "Duplicate sources should be avoided."
    REASON_TO_FLAG = (
        "Having multiple source nodes pointing to the same database location can lead to an inaccurate "
        "representation of data lineage and potential confusion in data management."
    )
    FAILURE_MESSAGE = (
        "Duplicate source nodes detected: Multiple source nodes are referencing the same database object. "
        "Database location {source_table} is referenced by:\n {source_nodes_list}"
    )
    RECOMMENDATION = (
        "Consolidate the duplicate source nodes so that each database location has only a single source definition "
        "in your dbt project. This will help maintain clear and accurate data lineage."
    )

    def _build_failure_result(self, source_table: str, source_ids: List[str]) -> DBTInsightResult:
        """
        Build Insight result if a source table has multiple source models defined.
        :param source_table: Name of the source table.
        :param source_ids: List of source IDs which are referencing the source table.
        :return: An instance of DBTInsightResult containing failure message and recommendation and metadata.
        """
        self.logger.debug(f"Building failure result for source table {source_table}")
        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            reason_to_flag=self.REASON_TO_FLAG,
            message=self.FAILURE_MESSAGE.format(source_table=source_table, source_nodes_list=numbered_list(source_ids)),
            recommendation=self.RECOMMENDATION.format(source_table=source_table),
            metadata={
                "source_table": source_table,
                "source_ids": source_ids,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTProjectInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        containing insights about direct source dependencies.
        :return: A list of InsightResponse objects.
        """

        self.logger.debug(f"Generating insights for DBTDuplicateSources for project {self.project_name}")

        source_table_to_id_map = defaultdict(list)
        for source_id, source in self.sources.items():
            table_name = get_table_name_from_source(source)
            source_table_to_id_map[table_name].append(source_id)

        self.logger.debug(f"source_table_to_id_map: {source_table_to_id_map}")
        insight_results = []
        for source_table, source_ids in source_table_to_id_map.items():
            if len(source_ids) > 1:
                insight_results.append(self._build_failure_result(source_table, source_ids))

        if insight_results:
            self.logger.debug("Duplicate source models found")
            return [
                DBTProjectInsightResponse(
                    package_name=self.project_name,
                    insights=insight_results,
                    severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                )
            ]

        self.logger.debug("No duplicate sources found")
        return []
