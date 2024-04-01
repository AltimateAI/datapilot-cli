from typing import Dict
from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelHasTestsByType(ChecksInsight):
    NAME = "Model has tests by type"
    ALIAS = "check_model_has_tests_by_type"
    DESCRIPTION = "Checks that the model has tests with specific types."
    REASON_TO_FLAG = "Models should have tests with specific types for proper validation."
    TESTS_LIST_STR = "tests"
    TEST_TYPE_STR = "test"
    TEST_COUNT_STR = "min_count"

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.test_list = self.get_check_config(self.TESTS_LIST_STR) or []
        self.tests = {
            test.get(self.TEST_TYPE_STR): test.get(self.TEST_COUNT_STR, 0) for test in self.test_list if test.get(self.TEST_TYPE_STR)
        }
        if not self.tests:
            self.logger.warning(f"No tests found in the configuration for {self.ALIAS}. Skipping the insight.")
            return []

        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                missing_tests = self._model_has_tests_by_type(node_id)
                if missing_tests:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, missing_tests),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, model_unique_id: str, missing_tests: List[Dict]) -> DBTInsightResult:
        missing_test_type_str = ""
        for test in missing_tests:
            missing_test_type_str += f"Test type: {test.get(self.TEST_TYPE_STR)}, Min Count: {test.get(self.TEST_COUNT_STR)}, Actual Count: {test.get('actual_count')}\n"

        failure_message = f"The model `{model_unique_id}` does not have enough tests for the following types:\n{missing_test_type_str}. "
        recommendation = (
            "Add tests with the specified names for each model listed above. "
            "Having tests with specific names ensures proper validation and data integrity."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"model_unique_id": model_unique_id, "missing_tests": missing_tests},
        )

    def _model_has_tests_by_type(self, node_id) -> bool:
        """
        For model, check all dependencies and if node type is test, check if it has the required types.
        Only return true if all child.type in test_types
        """
        test_count = {}
        for child_id in self.children_map.get(node_id, []):
            child = self.get_node(child_id)
            if child.resource_type == AltimateResourceType.test:
                self.logger.info(f"child_id: {child_id}, child: {type(child)}")
                test_count[child.test_type] = test_count.get(child.test_type, 0) + 1
        missing_tests = []
        for test_type in self.tests.keys():
            if test_count.get(test_type, 0) < self.tests.get(test_type, 0):
                missing_tests.append(
                    {
                        self.TEST_TYPE_STR: test_type,
                        self.TEST_COUNT_STR: self.tests.get(test_type, 0),
                        "actual_count": test_count.get(test_type, 0),
                    }
                )
        return missing_tests

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
                            cls.TEST_TYPE_STR: {
                                "type": "string",
                                "enum": ["singul", "data"],
                                "description": "The type of the test",
                            },
                            cls.TEST_COUNT_STR: {"type": "integer", "description": "The minimum number of tests required", "default": 1},
                        },
                        "required": [cls.TEST_TYPE_STR, cls.TEST_COUNT_STR],
                    },
                    "description": "A list of tests with names and minimum counts required.",
                    "default": [],
                },
            },
            "required": [cls.TESTS_LIST_STR],
        }
        return config_schema
