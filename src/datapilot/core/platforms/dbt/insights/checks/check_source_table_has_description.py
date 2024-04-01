from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceTableHasDescription(ChecksInsight):
    NAME = "Source table has description"
    ALIAS = "check_source_table_has_desc"
    DESCRIPTION = "Ensures that the source table has a description"
    REASON_TO_FLAG = "Missing description for the source table can lead to confusion and inconsistency in analysis. "

    def _build_failure_result(self, source_id: int) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's parent schema is not whitelist or in blacklist.
        """
        failure_message = f"The source:{source_id} does not have a description defined.\n"

        recommendation = "Define the description for the source table to ensure consistency in analysis."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_id": source_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate the insight response for the check. This method is called by the insight runner to generate the insight
        response for the check.
        Ensures that the source table has a description
        """
        insights = []
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self._check_source_table_desc(node_id):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_source_table_desc(self, source_unique_id: str) -> bool:
        source = self.get_node(source_unique_id)
        if source.description is None:
            return False
        return True
