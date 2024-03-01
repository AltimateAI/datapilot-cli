from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceHasLoader(ChecksInsight):
    NAME = "Check Source Has Loader"
    ALIAS = "check_source_has_loader"
    DESCRIPTION = "Check if the source has a loader"
    REASON_TO_FLAG = "Missing loader for the source can lead to confusion and inconsistency in analysis. "

    def _build_failure_result(self, source_id: int) -> DBTInsightResult:
        failure_message = "The source table `{source_id}` is missing a loader. " "Ensure that the source table has a loader."
        recommendation = (
            "Add a loader for the source table `{source_id}`. "
            "Ensuring that the source table has a loader helps in maintaining data integrity and consistency."
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
        Ensures that the source has a loader option
        """
        insights = []
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self._check_source_has_loader(node_id):
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

    def _check_source_has_loader(self, source_unique_id: str) -> bool:
        source = self.get_node(source_unique_id)
        if source.loader is None:
            return False
        return True
