from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceHasLoader(ChecksInsight):
    NAME = "Source has loader"
    ALIAS = "check_source_has_loader"
    DESCRIPTION = "Check if the source has a loader"
    REASON_TO_FLAG = "Missing loader for the source can lead to confusion and inconsistency in analysis. "

    def _build_failure_result(self, source_id: int) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's parent schema is not whitelist or in blacklist.
        """
        failure_message = f"The source:{source_id} does not have a loader defined.\n"

        recommendation = "Define the loader for the source to ensure consistency in analysis."

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
        if not source.loader:
            return False
        return True
