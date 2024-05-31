from typing import List

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import STAGING
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.utils import classify_model_type
from datapilot.utils.formatting.utils import numbered_list


class DBTStagingModelsDependentOnStagingModels(DBTModellingInsight):
    """
    DBTStagingModelsDependentOnStagingModels identifies staging models in a dbt project that depend on staging models.
    """

    NAME = "Staging models dependency on staging Models"
    ALIAS = "staging_models_on_staging"
    DESCRIPTION = "Staging models should not directly depend on other staging models."
    REASON_TO_FLAG = (
        "Best practice is for staging models to depend on source or raw data models, not on other staging models. "
        "Dependencies among staging models can lead to complicated data flows and hinder data lineage tracking."
    )
    FAILURE_MESSAGE = (
        "Staging model `{current_model_unique_id}` has dependencies on other staging models, "
        "which is against best practices: \n{downstream_dependencies}"
    )
    RECOMMENDATION = (
        "Refactor staging model `{current_model_unique_id}` to ensure it depends on source or raw data models, "
        "not on other staging models. This realignment with best practices promotes clear and effective data flow."
    )

    def _build_failure_result(self, current_model_unique_id: str, downstream_dependencies: List[str]) -> DBTInsightResult:
        failure = self.FAILURE_MESSAGE.format(
            current_model_unique_id=current_model_unique_id,
            downstream_dependencies=numbered_list(downstream_dependencies),
        )
        recommendation = self.RECOMMENDATION.format(current_model_unique_id=current_model_unique_id)

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": current_model_unique_id,
                "downstream_dependencies": downstream_dependencies,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        regex_configuration = get_regex_configuration(self.config)
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if (
                node.resource_type == AltimateResourceType.model
                and classify_model_type(node.name, node.original_file_path, regex_configuration) == STAGING
            ):
                downstream_dependencies = [
                    dependent_node_id
                    for dependent_node_id in node.depends_on.nodes
                    if classify_model_type(
                        self.get_node(dependent_node_id).name,
                        self.get_node(dependent_node_id).original_file_path,
                        regex_configuration,
                    )
                    == STAGING
                ]

                if downstream_dependencies:
                    insight_result = self._build_failure_result(node_id, downstream_dependencies)
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

        return insights
