from typing import List

from datapilot.config.utils import get_source_test_type_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckSourceHasTestsByType(ChecksInsight):
    NAME = "Check Source Has Tests By Type"
    ALIAS = "check_source_has_tests_by_type"
    DESCRIPTION = "Checks that the source has tests with specific types."
    REASON_TO_FLAG = "Sources should have tests with specific types for proper validation."

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.test_types = get_source_test_type_configuration(self.config)
        insights = []
        for source_id, source in self.sources.items():
            if self.should_skip_model(source_id):
                self.logger.debug(f"Skipping source {source_id} as it is not enabled for selected models")
                continue
            if source.resource_type == AltimateResourceType.source:
                if not self._source_has_tests_by_type(source_id, self.test_types):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=source_id,
                            package_name=source.package_name,
                            path=source.original_file_path,
                            original_file_path=source.original_file_path,
                            insight=self._build_failure_result(source_id, self.test_types),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, source_id: str, test_types: List[str]) -> DBTInsightResult:
        failure_message = (
            "The following sources do not have tests with the specified types:\n{missing_tests}. "
            "Ensure that each source has tests with the specified types for proper validation."
        )
        recommendation = (
            "Add tests with the specified types for each source listed above. "
            "Having tests with specific types ensures proper validation and data integrity."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message.format(
                missing_tests=numbered_list(test_types),
            ),
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"missing_tests": test_types, "source_id": source_id},
        )

    def _source_has_tests_by_type(self, node_id, test_types: List[str]) -> bool:
        """
        For source, check all dependencies and if node type is test, check if it has the required types.
        """
        for child_id in self.children_map.get(node_id, []):
            child = self.get_node(child_id)
            if child.resource_type == AltimateResourceType.test:
                if child.test_type not in test_types:
                    return False
        return True
