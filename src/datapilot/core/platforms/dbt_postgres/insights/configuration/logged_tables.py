from typing import List

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import STAGING
from datapilot.core.platforms.dbt.constants import TABLE
from datapilot.core.platforms.dbt.insights.configuration.base import DBTConfigurationInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTProjectInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.utils import classify_model_type


class DBTPostgresLoggedTables(DBTConfigurationInsight):
    """
    This class identifies all models that ar of type unlogged
    """

    NAME = "Logged Tables in dbt Models"
    ALIAS = "staging_logged_tables"
    DESCRIPTION = (
        "Logged tables  are considerably slower than unlogged tables, as they require a write to the WAL and are. "
        "replicated to read replicas. Unlogged tables are not replicated and do not require a write to the WAL, "
        "but are not less safe."
    )
    REASON_TO_FLAG = (
        "Unlogged tables are considerably faster than logged tables, as they do not require a write to the WAL and are not replicated to read replicas."
        " staging tables can be unlogged"
    )
    FAILURE_MESSAGE = (
        "The following staging tables are logged:\n {logged_tables}.\n" "Consider changing them to unlogged tables to improve performance."
    )
    RECOMMENDATION = (
        "To address this issue, consider changing the following staging tables to unlogged tables to improve performance."
        "You can do this by adding the following to your dbt model: \n"
        "{{ config(materialized='table', unlogged=True) }}"
    )

    def _build_failure_result(self, logged_models: List[str]) -> DBTInsightResult:
        """
        Constructs a failure result for a given model with low test coverage.
        :param coverage: The calculated test coverage percentage for the model.
        :param min_coverage: The minimum required test coverage percentage.
        :return: An instance of DBTInsightResult containing failure details.
        """
        self.logger.debug("Logged tables found: {logged_models}")
        failure = self.FAILURE_MESSAGE.format(logged_tables=logged_models)
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=self.RECOMMENDATION,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"logged_models": logged_models},
        )

    def _check_logged_tables(self, node: AltimateManifestNode) -> List[str]:
        """
        Checks for logged tables
        """
        regex_configuration = get_regex_configuration(self.config)
        model_type = classify_model_type(node.name, node.original_file_path, regex_configuration)
        self.logger.debug(f"Checking if model {node.name} is a logged table")
        self.logger.debug(f"Model type: {model_type}")
        if (not node.config.unlogged) and (model_type == STAGING) and node.config.materialized == TABLE:
            return True
        return False

    def generate(self, *args, **kwargs) -> List[DBTProjectInsightResponse]:
        """
        Generates insights for each DBT model in the project, focusing on logged tables
        :return: A list of DBTModelInsightResponse objects with insights for each model.
        """
        self.logger.debug("Generating test coverage insights for DBT models")

        logged_models = []
        for node_id, node in self.nodes.items():
            if self._check_logged_tables(node):
                logged_models.append(node_id)

        insights = []
        if logged_models:
            insights.append(
                DBTProjectInsightResponse(
                    package_name=self.project_name,
                    insights=[self._build_failure_result(logged_models)],
                    severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                )
            )

        self.logger.debug("Completed generating logged tables insights for DBT models.")
        return insights
