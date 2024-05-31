from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class DBTRootModel(DBTModellingInsight):
    """
    DBTRootModels is used to identify models in a dbt project with 0 direct parents,
    meaning these models cannot be traced back to a declared source or model.
    """

    NAME = "Root model traceability"
    ALIAS = "root_model"
    DESCRIPTION = "Identifies models in a dbt project with 0 direct parents, meaning these models cannot be traced back to a declared source or model."
    REASON_TO_FLAG = (
        "Best Practice is to ensure all models can be traced back to a source or another model in the project. "
        "Root models with no direct parents can lead to challenges in tracking data lineage and understanding"
        " the overall data model."
    )
    FAILURE_MESSAGE = (
        "Model `{current_model_unique_id}` is identified as a root model with no direct parents. "
        "This can hinder traceability and clarity in the data model."
    )
    RECOMMENDATION = (
        "Ensure that model `{current_model_unique_id}` is appropriately linked to a source or another model "
        "within the dbt project. This linkage is crucial for maintaining clear data lineage and project coherence."
    )

    def _build_failure_result(self, current_model_unique_id: str) -> DBTInsightResult:
        """
        Build failure result for the insight if a model is a root model with 0 direct parents.

        :param current_model_unique_id: Unique ID of the current model being evaluated.
        :return: An instance of InsightResult containing failure message and recommendation.
        """
        self.logger.debug(f"Building failure result for root model {current_model_unique_id}")

        failure = self.FAILURE_MESSAGE.format(current_model_unique_id=current_model_unique_id)
        recommendation = self.RECOMMENDATION.format(current_model_unique_id=current_model_unique_id)

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"model": current_model_unique_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying root models with 0 direct parents.
        :return: A list of InsightResponse objects.
        """
        self.logger.debug(f"Generating insights for DBTRootModels for project {self.project_name}")
        insights = []

        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model and not node.depends_on.nodes:
                self.logger.debug(f"Found root model {node_id} with no direct parents")
                insight_result = self._build_failure_result(node.unique_id)
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=node_id,
                        package_name=node.package_name,
                        path=node.path,
                        original_file_path=node.original_file_path,
                        insight=insight_result,
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )

        self.logger.debug(f"Found {len(insights)} root models")
        return insights
