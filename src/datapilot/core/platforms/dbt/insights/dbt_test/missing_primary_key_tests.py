from typing import Dict
from typing import List
from typing import Optional

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import GENERIC
from datapilot.core.platforms.dbt.insights.dbt_test.base import DBTTestInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class MissingPrimaryKeyTests(DBTTestInsight):
    """
    This class identifies DBT models that are missing primary key tests.
    Primary key tests are essential for ensuring data integrity in DBT models.
    This class generates insights for each model that lacks proper primary key tests.
    """

    _ALL_TESTS_KEY = "_all_tests"
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    UNIQUE_COMBINATION_OF_COLUMNS = "unique_combination_of_columns"
    NAME = "Missing primary key tests"
    ALIAS = "missing_primary_key_tests"
    DESCRIPTION = "Checks if the model has a primary key test. "
    REASON_TO_FLAG = (
        "dbt tests play a crucial role in asserting data correctness. The absence of primary key tests can increase "
        "the risk of data integrity issues, affecting project reliability and scalability."
    )
    FAILURE_MESSAGE = (
        "dbt model `{model_unique_id}` does not have a primary key test. " "This omission may lead to data integrity challenges."
    )
    RECOMMENDATION = (
        "To address this, apply a uniqueness test and a not-null test to the column representing the model's grain. "
        "For models with unique combinations of columns, consider adding a surrogate key and "
        "applying these tests to that column. You can refer to dbt_utils for a surrogate_key macro"
        " and unique_combination_of_columns test."
    )

    def _build_failure_result(self, model_unique_id: str) -> DBTInsightResult:
        """
        Constructs a failure result for a given model.

        :param model_unique_id: Unique ID of the model being evaluated.
        :return: An instance of DBTInsightResult containing failure details.
        """
        self.logger.debug(f"Building failure result for model {model_unique_id}")
        failure = self.FAILURE_MESSAGE.format(model_unique_id=model_unique_id)
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=self.RECOMMENDATION,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"model_unique_id": model_unique_id},
        )

    def _has_primary_key_test(self, column_tests: Optional[Dict[str, List]]) -> bool:
        """
        Checks if the given column tests include a primary key test.

        :param column_tests: Dictionary of column tests.
        :return: True if primary key test exists, False otherwise.
        """
        self.logger.debug("Checking for primary key tests")
        if not column_tests:
            return False

        if self.UNIQUE_COMBINATION_OF_COLUMNS in column_tests.get(self._ALL_TESTS_KEY, []):
            return True

        column_tests.pop(self._ALL_TESTS_KEY, None)

        for tests in column_tests.values():
            if self.NOT_NULL in tests and self.UNIQUE in tests:
                return True

        return False

    def _get_nodes_which_need_tests(self) -> List[str]:
        return [
            node_id
            for node_id, node in self.nodes.items()
            if self.check_part_of_project(node.package_name) and node.resource_type == AltimateResourceType.model
        ]

    def _get_nodes_with_tests(self, tests) -> Dict[str, Dict[str, List]]:
        nodes_with_tests = {}
        for test in tests.values():
            for node_id in test.depends_on.nodes or []:
                column = test.test_metadata.kwargs.get("column_name")
                key = column if column else self._ALL_TESTS_KEY
                nodes_with_tests.setdefault(node_id, {}).setdefault(key, []).append(test.test_metadata.name)
        return nodes_with_tests

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generates insights for each DBT model in the project.

        :return: A list of DBTModelInsightResponse objects with insights for each model.
        """
        self.logger.debug("Generating insights for DBT models")
        tests = self.manifest.get_tests(GENERIC)

        nodes_which_need_tests = self._get_nodes_which_need_tests()

        nodes_which_have_test = self._get_nodes_with_tests(tests)

        insights = []
        for node_id in nodes_which_need_tests:
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if not self._has_primary_key_test(nodes_which_have_test.get(node_id)):
                node = self.get_node(node_id)
                self.logger.debug(f"Adding insight for model {node_id}")
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

        self.logger.debug("Completed generating insights")
        return insights
