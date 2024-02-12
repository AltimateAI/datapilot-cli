from typing import List

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import STAGING
from datapilot.core.platforms.dbt.constants import TABLE
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.utils import classify_model_type


class DBTSnowflakePermanantStagingTables(DBTModellingInsight):
    NAME = "Permanant Staging Tables"
    ALIAS = "snowflake_permanant_staging_tables"
    DESCRIPTION = "This insight checks for the presence of permanant staging tables in the DBT project."
    REASON_TO_FLAG = (
        "Since staging tables typicaly refersh frequently, it is recommended to use transient tables to avoid storage costs."
        " In snowflake, transient tables don't have a failsafe period and hence helps in reducing storage costs."
        " If you are using a permanent staging table, You may incurr additional fail-safe storage costs evertyime you refresh the table."
    )
    FAILURE_MESSAGE = "The stagign table {staging_model} is permanant. Consider using transient tables to avoid fail safestorage costs."
    RECOMMENDATION = (
        "Consider using transient tables to avoid fail safe storage costs. You can use the following configuration in your model:\n"
        "{{ config(materialized='table', transient=True) }}"
    )

    def _build_failure_result(self, staging_model: str) -> DBTInsightResult:
        """
        Constructs a failure result for a given model with low test coverage.
        :param coverage: The calculated test coverage percentage for the model.
        :param min_coverage: The minimum required test coverage percentage.
        :return: An instance of DBTInsightResult containing failure details.
        """
        self.logger.debug(f"Permanant Staging tables found: {staging_model}")
        failure = self.FAILURE_MESSAGE.format(logged_tables=staging_model)
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=self.RECOMMENDATION,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"staging_model": staging_model},
        )

    def _check_logged_tables(self, node: AltimateManifestNode) -> List[str]:
        """
        Checks for logged tables
        """
        regex_configuration = get_regex_configuration(self.config)
        model_type = classify_model_type(node.name, node.original_file_path, regex_configuration)
        self.logger.debug(f"Checking if model {node.name} is a permanant table")
        self.logger.debug(f"Model type: {model_type}")
        if (node.config.transient == False) and (model_type == STAGING) and node.config.materialized == TABLE:  # noqa
            return True
        return False

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generates insights for the DBT project. This insight checks for the presence of permanant staging tables in the DBT project.
        """
        self.logger.debug("Generating test coverage insights for DBT models")

        insights = []
        for node_id, node in self.nodes.items():
            if self._check_logged_tables(node):
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=node,
                        package_name=self.project_name,
                        path=node.path,
                        original_file_path=node.original_file_path,
                        insight=self._build_failure_result(node_id),
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )

        self.logger.debug("Completed generating permanant staging tables insights.")
        return insights
