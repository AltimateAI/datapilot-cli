from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.governance.base import DBTGovernanceInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateAccess
from datapilot.utils.formatting.utils import numbered_list


class DBTExposureDependentOnPrivateModels(DBTGovernanceInsight):
    """
    DBTExposureDependentOnPrivateModels identifies exposures that are dependent on private models.
    """

    NAME = "Exposures dependent on private models"
    ALIAS = "exposures_dependent_on_private_models"
    DESCRIPTION = "Identify exposures that are dependent on private models. "
    REASON_TO_FLAG = (
        "Exposures illustrate how and where data is consumed in downstream tools. These tools should utilize "
        "data from public, trusted, and contracted sources to ensure data reliability and integrity."
    )
    FAILURE_MESSAGE = (
        "Exposure `{exposure_unique_id}` is dependent on private models, which may not be ideal for "
        "downstream consumption:\n`{private_models}`."
    )
    RECOMMENDATION = (
        "Consider revising the yml file to ensure that the models your exposures depend on are fully "
        "exposed and public. While this rule flags non-public models, it is also recommended to document"
        " and formalize contracts for these public models for best practices."
    )

    def _build_failure_result(self, exposure_unique_id: str, private_models: List[str]) -> DBTInsightResult:
        """
        Build failure result for the insight if a model is a root model with 0 direct parents.

        :param exposure_unique_id: Unique ID of the current model being evaluated.
        :return: An instance of InsightResult containing failure message and recommendation.
        """
        self.logger.debug(f"Building failure result exposure {exposure_unique_id} depends on private models {private_models}")

        failure = self.FAILURE_MESSAGE.format(
            exposure_unique_id=exposure_unique_id,
            private_models=numbered_list(private_models),
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=self.RECOMMENDATION,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"exposure": exposure_unique_id, "private_models": private_models},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the dbt project,
        identifying root models with 0 direct parents.
        :return: A list of InsightResponse objects.
        """
        if len(self.exposures) == 0:
            self.logger.debug(f"No exposures found in project {self.project_name}")
            return []
        insights = []
        for exposure_id, exposure in self.exposures.items():
            if self.should_skip_model(exposure_id):
                self.logger.debug(f"Skipping model {exposure_id} as it is not enabled for selected models")
                continue
            self.logger.debug(f"Checking exposure {exposure_id}")
            private_models = []
            for dependency_id in exposure.depends_on.nodes:
                dependency_node = self.get_node(dependency_id)
                if dependency_node.access == AltimateAccess.private:
                    private_models.append(dependency_id)

            if private_models:
                insight_result = self._build_failure_result(exposure_unique_id=exposure_id, private_models=private_models)
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=exposure_id,
                        package_name=exposure.package_name,
                        path=exposure.original_file_path,
                        original_file_path=exposure.original_file_path,
                        insight=insight_result,
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )

        return insights
