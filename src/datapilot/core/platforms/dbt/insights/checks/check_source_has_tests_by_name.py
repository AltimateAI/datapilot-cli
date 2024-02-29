from typing import List

from datapilot.config.utils import get_source_test_name_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceHasTestsByName(ChecksInsight):
    NAME = "Check Source Has Tests By Name"
    ALIAS = "check_source_has_tests_by_name"
    DESCRIPTION = "Checks that the source has tests with specific names."
    REASON_TO_FLAG = "Sources should have tests with specific names for proper validation."

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.test_names = get_source_test_name_configuration(self.config)
        insights = []
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self._source_has_tests_by_name(node_id, self.test_names):
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
            f"The following sources do not have tests with the specified names:\n{source_unique_id}. "
            "Ensure that each source has tests with the specified names for proper validation."
        )
        recommendation = (
            "Add tests with the specified names for each source listed above. "
            "Having tests with specific names ensures proper validation and data integrity."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_unique_id": source_unique_id},
        )

    def _source_has_tests_by_name(self, node_id, test_names: List[str]) -> bool:
        """
        For source, check all dependencies and if node type is test, check if it has the required names.
        Only return true if all child.name in test_names
        """
        if node_id not in self.children_map:
            return False
        for child_id in self.children_map[node_id]:
            child = self.get_node(child_id)
            if child.resource_type == AltimateResourceType.test:
                if child.name in test_names:
                    return True
        return False
