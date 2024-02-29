from typing import List

from datapilot.config.utils import get_test_group_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceHasTestsByGroup(ChecksInsight):
    NAME = "Check Source Has Tests By Group"
    ALIAS = "check_source_has_tests_by_group"
    DESCRIPTION = "Checks that the source has tests with specific groups."
    REASON_TO_FLAG = "Sources should have tests with specific groups for proper validation."

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.test_groups = get_test_group_configuration(self.config)
        insights = []
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self._source_has_tests_by_group(node_id, self.test_groups):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, source_unique_id: str) -> DBTInsightResult:
        failure_message = (
            f"The following sources do not have tests with the specified groups:\n{source_unique_id}. "
            "Ensure that each source has tests with the specified groups for proper validation."
        )
        recommendation = (
            "Add tests with the specified groups for each source listed above. "
            "Having tests with specific groups ensures proper validation and data integrity."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_unique_id": source_unique_id},
        )

    def _source_has_tests_by_group(self, node_id, test_groups: List[str]) -> bool:
        """
        For source, check all dependencies and if node type is test, check if it has the required groups.
        Only return true if all child.group in test_groups
        """
        if node_id not in self.children_map:
            return False
        for child_id in self.children_map[node_id]:
            child = self.get_node(child_id)
            if child.resource_type == AltimateResourceType.test:
                if child.group in test_groups:
                    return True
        return False
