from typing import Dict
from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelHasTestsByGroup(ChecksInsight):
    NAME = "Model has tests by group"
    ALIAS = "check_model_has_tests_by_group"
    DESCRIPTION = "Check if models have a number of tests for specific test groups."
    REASON_TO_FLAG = "Models should have tests with specific groups for proper validation."
    TESTS_LIST_STR = "tests"
    TEST_GROUP_STR = "test_group"
    TEST_COUNT_STR = "min_count"

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.test_list = self.get_check_config(self.TESTS_LIST_STR) or []
        self.test_groups = {
            tuple(test.get(self.TEST_GROUP_STR, [])): test.get(self.TEST_COUNT_STR, 0)
            for test in self.test_list
            if test.get(self.TEST_GROUP_STR)
        }
        if not self.test_groups:
            self.logger.warning(f"No test groups found in the configuration for {self.ALIAS}. Skipping the insight.")
            return []
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                missing_test_groups = self._model_has_tests_by_group(node_id)
                if missing_test_groups:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, missing_test_groups),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, model_unique_id: str, missing_test_groups: List[Dict]) -> DBTInsightResult:
        missing_test_group_str = ""
        for test in missing_test_groups:
            missing_test_group_str += f"Test Group: {test.get(self.TEST_GROUP_STR)}, Min Count: {test.get(self.TEST_COUNT_STR)}, Actual Count: {test.get('actual_count')}\n"

        failure_message = f"The model `{model_unique_id}` does not have enough tests for the following groups:\n{missing_test_group_str}. "
        recommendation = (
            "Add tests with the specified groups for each model listed above. "
            "Having tests with specific groups ensures proper validation and data integrity."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"model_unique_id": model_unique_id, "missing_test_groups": missing_test_groups},
        )

    def _model_has_tests_by_group(self, node_id) -> List[Dict]:
        """
        For model, check all dependencies and if node type is test, check if it has the required groups.
        Only return true if all child.group in test_groups
        """
        test_group_count = {}
        for child_id in self.children_map.get(node_id, []):
            child = self.get_node(child_id)
            if child.resource_type == AltimateResourceType.test:
                for group in self.test_groups:
                    if child.name in group:
                        test_group_count[group] = test_group_count.get(group, 0) + 1
        missing_test_groups = []
        for group, count in self.test_groups.items():
            if test_group_count.get(group, 0) < count:
                missing_test_groups.append(
                    {self.TEST_GROUP_STR: group, self.TEST_COUNT_STR: count, "actual_count": test_group_count.get(group, 0)}
                )

        return missing_test_groups

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.TESTS_LIST_STR: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            cls.TEST_GROUP_STR: {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of tests part of a group. If a test is part of any of the groups, it will be counted.",
                            },
                            cls.TEST_COUNT_STR: {"type": "integer", "description": "The minimum number of tests required", "default": 1},
                        },
                        "required": [cls.TEST_GROUP_STR, cls.TEST_COUNT_STR],
                    },
                    "description": "A list of tests with names and minimum counts required.",
                    "default": [],
                },
            },
            "required": [cls.TESTS_LIST_STR],
        }
        return config_schema
