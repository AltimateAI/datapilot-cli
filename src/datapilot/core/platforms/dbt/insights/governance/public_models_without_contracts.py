from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.governance.base import DBTGovernanceInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateAccess
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class DBTPublicModelWithoutContracts(DBTGovernanceInsight):
    """
    DBTUndocumentedPublicModels identifies public models that are not documented.
    """

    NAME = "Public models without contracts"
    ALIAS = "public_models_without_contracts"
    DESCRIPTION = "Identify public models that don't have contracts."
    REASON_TO_FLAG = (
        "Public models are accessible to all downstream consumers, making it crucial to have clear "
        "contracts that specify data types and columns. This ensures consistency and predictability "
        "in data consumption."
    )
    FAILURE_MESSAGE = (
        "Model `{model_unique_id}` is marked as public but does not have a contract. "
        "This can lead to ambiguity regarding data types and columns, impacting downstream consumers."
    )
    RECOMMENDATION = (
        "Enhance the model `{model_unique_id}` by adding clear contract entries for columns along "
        "with their data types. Contracts provide essential documentation and guarantees for downstream consumers."
    )

    def _build_failure_result(
        self,
        model_unique_id: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model is a root model with 0 direct parents.

        :param model_unique_id: Unique ID of the current model being evaluated.
        :return: An instance of InsightResult containing failure message and recommendation.
        """
        self.logger.debug(f"Building failure result model {model_unique_id} is public but not documented.")

        failure = self.FAILURE_MESSAGE.format(
            model_unique_id=model_unique_id,
        )
        recommendation = self.RECOMMENDATION.format(
            model_unique_id=model_unique_id,
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": model_unique_id,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying root models with 0 direct parents.
        """
        self.logger.debug("Generating insights for public models without contracts")
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model and node.access == AltimateAccess.public:
                if (not node.contract) or (not node.contract.enforced):
                    self.logger.debug(f"Found public model {node_id} without contract enforced")
                    insight_result = self._build_failure_result(node_id)
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=insight_result,
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        self.logger.debug("Finished generating insights for public models without contracts")
        return insights
