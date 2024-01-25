from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import SINGULAR
from datapilot.core.platforms.dbt.insights.dbt_test.base import DBTTestInsight
from datapilot.core.platforms.dbt.insights.schema import (
    DBTInsightResult, DBTProjectInsightResponse)
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.schemas.constants import CONFIG_METRICS


class DBTTestCoverage(DBTTestInsight):
    """
    This class identifies DBT models with test coverage below a specified threshold.
    It aims to ensure that a minimum percentage of tests are applied to each model to maintain data integrity.
    """

    NAME = "Low Test Coverage in dbt Models"
    ALIAS = "dbt_low_test_coverage"
    DESCRIPTION = (
        "Identifies dbt models in the project with low test coverage, where the test coverage percentage falls below "
        "the minimum required threshold. Adequate test coverage is essential to maintain data integrity"
        " and ensure the accuracy of data transformations."
    )
    REASON_TO_FLAG = (
        "dbt models should have a minimum test coverage percentage to ensure the reliability and accuracy "
        "of data transformations. Low test coverage can lead to data quality issues."
    )
    FAILURE_MESSAGE = (
        "The test coverage {coverage_percent}% is below the minimum threshold"
        " of {min_coverage_percent}%. Insufficient test coverage can impact data integrity and transformation accuracy."
    )
    RECOMMENDATION = (
        "To address this issue, review and increase the number and variety of tests applied to your model to "
        "improve its test coverage. Consider adding different types of tests such as uniqueness, not_null, "
        "and referential integrity tests to ensure data quality and accuracy."
    )
    MIN_COVERAGE_PERCENT = 100
    MIN_COVERAGE_PERCENT_STR = "min_test_coverage_percent"

    def _build_failure_result(self, coverage: float, min_coverage=MIN_COVERAGE_PERCENT) -> DBTInsightResult:
        """
        Constructs a failure result for a given model with low test coverage.
        :param coverage: The calculated test coverage percentage for the model.
        :param min_coverage: The minimum required test coverage percentage.
        :return: An instance of DBTInsightResult containing failure details.
        """
        self.logger.debug(f"CALCULATED COVERAGE: {coverage}")
        failure = self.FAILURE_MESSAGE.format(min_coverage_percent=min_coverage, coverage_percent=coverage)
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=self.RECOMMENDATION,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"min_coverage_percent": min_coverage, "coverage": coverage},
        )

    def _calculate_coverage(self) -> float:
        """
        :return: Test coverage percentage for the model.
        """
        num_models = len(
            [
                node.unique_id
                for node in self.nodes.values()
                if node.resource_type == AltimateResourceType.model and self.check_part_of_project(node.package_name)
            ]
        )

        models_with_tests = set()
        for test in self.tests.values():
            if test.test_type == SINGULAR:
                return 100
            if test.package_name == self.project_name:
                models_with_tests = models_with_tests.union(set(test.depends_on.nodes) if test.depends_on else set())

        return round((len(models_with_tests) / num_models) * 100) if num_models > 0 else 0

    def _get_min_coverage_percent(self) -> int:
        """
        :return: The minimum required test coverage percentage.
        """
        metrics_config = self.config.get(CONFIG_METRICS, {})
        metric_config = metrics_config.get(self.ALIAS, {})
        self.logger.debug(f"METRIC CONFIG: {metric_config}")
        # Return the configured fanout threshold or the default if not specified
        return metric_config.get(self.MIN_COVERAGE_PERCENT_STR, self.MIN_COVERAGE_PERCENT)

    def generate(self, *args, **kwargs) -> List[DBTProjectInsightResponse]:
        """
        Generates insights for each DBT model in the project, focusing on test coverage.

        :return: A list of DBTModelInsightResponse objects with insights for each model.
        """
        self.logger.debug("Generating test coverage insights for DBT models")
        min_coverage = self._get_min_coverage_percent()
        coverage = self._calculate_coverage()

        insights = []
        if coverage < min_coverage:
            insights.append(
                DBTProjectInsightResponse(
                    package_name=self.project_name,
                    insights=[self._build_failure_result(coverage, min_coverage)],
                    severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                )
            )

        self.logger.debug("Completed generating test coverage insights")
        return insights
