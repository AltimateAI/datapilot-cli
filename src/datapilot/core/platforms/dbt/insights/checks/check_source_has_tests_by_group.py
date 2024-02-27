from typing import List

from datapilot.config.utils import get_test_group_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckSourceHasTestsByGroup(ChecksInsight):
    NAME = "Check Source Has Tests By Group"
    ALIAS = "check_source_has_tests_by_group"
    DESCRIPTION = "Checks that the source has tests with specific groups."
    REASON_TO_FLAG = "Sources should have tests with specific groups for proper validation."

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.test_groups = get_test_group_configuration(self.config)
        insights = []
        for source_id, source in self.sources.items():
            if self.should_skip_model(source_id):
                self.logger.debug(f"Skipping source {source_id} as it is not enabled for selected models")
                continue
            if source.resource_type == AltimateResourceType.source:
                if not self._source_has_tests_by_group(source_id, self.test_groups):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=source_id,
                            package_name=source.package_name,
                            path=source.original_file_path,
                            original_file_path=source.original_file_path,
                            insight=self._build_failure_result(source_id, self.test_groups),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, source_id: str, test_groups: List[str]) -> DBTInsightResult:
        failure_message = (
            "The following sources do not have tests with the specified groups:\n{missing_tests}. "
            "Ensure that each source has tests with the specified groups for proper validation."
        )
        recommendation = (
            "Add tests with the specified groups for each source listed above. "
            "Having tests with specific groups ensures proper validation and data integrity."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message.format(
                missing_tests=numbered_list(test_groups),
            ),
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"missing_tests": test_groups, "source_id": source_id},
        )

    def _source_has_tests_by_group(self, node_id, test_groups: List[str]) -> bool:
        """
        For source, check all dependencies and if node type is test, check if it has the required groups.
        """
        for child_id in self.children_map.get(node_id, []):
            child = self.get_node(child_id)
            if child.resource_type == AltimateResourceType.test:
                if not child.group or child.group not in test_groups:
                    return False
        return True
