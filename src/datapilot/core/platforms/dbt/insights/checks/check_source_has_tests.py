from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceHasTests(ChecksInsight):
    NAME = "Source has tests"
    ALIAS = "check_source_has_tests"
    DESCRIPTION = "Check if the source has tests"
    REASON_TO_FLAG = "The source table is missing tests. Ensure that the source table has tests."
    TESTS_STR = "tests"

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        source_threshold = self.get_check_config(self.TESTS_STR) or 1
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                source_test_count = self.get_source_test_count(node_id)
                if source_test_count < source_threshold:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, source_test_count, source_threshold),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, source_unique_id: str, source_test_count: int, source_test_count_threshold: int) -> DBTInsightResult:
        failure_message = (
            "The following sources do not have enough tests. Ensure that each source has at least {source_test_count_threshold} tests."
        )
        recommendation = "Add tests for each source listed above. Having tests ensures proper validation and data integrity."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message.format(
                source_test_count_threshold=source_test_count_threshold,
            ),
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_test_count": source_test_count, "source_unique_id": source_unique_id},
        )

    def get_source_test_count(self, node_id: str) -> int:
        """
        Getting test count of sources by checking child nodes of sources that have type test.
        """
        count = 0
        for child_id in self.children_map.get(node_id, []):
            child = self.get_node(child_id)
            if child.resource_type == AltimateResourceType.test:
                count += 1
        return count

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.TESTS_STR: {
                    "type": "integer",
                    "description": "Minimum number of tests required for each source",
                    "default": 0,
                },
            },
            "required": [cls.TESTS_STR],
        }
        return config_schema
