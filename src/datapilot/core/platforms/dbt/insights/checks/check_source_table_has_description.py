from typing import List
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceTableHasDescription(ChecksInsight):
    NAME = "Check Source Table Has Description"
    ALIAS = "check_source_table_has_desc"
    DESCRIPTION = "Ensures that the source table has a description"
    REASON_TO_FLAG = "Missing description for the source table can lead to confusion and inconsistency in analysis. "

    def _build_failure_result(self, source_id: int) -> DBTInsightResult:
        failure_message = "The source table `{source_id}` is missing a description. " "Ensure that the source table has a description."
        recommendation = (
            "Add a description for the source table `{source_id}`. "
            "Ensuring that the source table has a description helps in maintaining data integrity and consistency."
        )
        return DBTInsightResult(
            failure_message=failure_message.format(source_id=source_id),
            recommendation=recommendation.format(source_id=source_id),
            metadata={"source_id": source_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate the insight response for the check. This method is called by the insight runner to generate the insight
        response for the check.
        Ensures that the source table has a description
        """
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self._check_source_table_desc(node_id):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            severity=get_severity(self.DEFAULT_SEVERITY),
                            result=self._build_failure_result(node_id),
                        )
                    )
        return insights

    def _check_source_table_desc(self, model_unique_id: str) -> bool:
        source = self.get_node(model_unique_id)
        if source.description is None:
            return False
        return True

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        return True, ""
