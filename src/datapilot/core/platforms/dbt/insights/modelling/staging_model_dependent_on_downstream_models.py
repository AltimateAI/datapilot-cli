from typing import ClassVar
from typing import List

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import INTERMEDIATE
from datapilot.core.platforms.dbt.constants import MART
from datapilot.core.platforms.dbt.constants import STAGING
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.utils import classify_model_type
from datapilot.schemas.constants import CONFIG_METRICS
from datapilot.utils.formatting.utils import numbered_list


class DBTStagingModelsDependentOnDownstreamModels(DBTModellingInsight):
    """
    DBTStagingModelsDependentOnDownstream identifies staging models in a dbt project that depend on downstream models.
    """

    NAME = "Staging models dependency check"
    ALIAS = "staging_models_dependency"
    DESCRIPTION = "Staging models should not depend on downstream models."
    REASON_TO_FLAG = (
        "Best practice is for staging models to depend on source or raw data models, not on downstream models. "
        "Dependencies in the wrong direction can lead to complications in data processing and lineage tracing."
    )
    FAILURE_MESSAGE = (
        "Staging model `{current_model_unique_id}` has dependencies on downstream models, "
        "which is against best practices: \n{downstream_dependencies}"
    )
    RECOMMENDATION = (
        "Refactor the staging model `{current_model_unique_id}` to ensure it depends on source or raw data models. "
        "This will align the model with best practices, enhancing data flow clarity and lineage tracing."
    )
    DOWNSTREAM_MODEL_TYPES_STR = "downstream_model_types"
    DOWNSTREAM_MODEL_TYPES: ClassVar[List[str]] = [MART, INTERMEDIATE]

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

    def _get_downstream_models(self) -> List[str]:
        metrics_config = self.config.get(CONFIG_METRICS, {})
        metric_config = metrics_config.get(self.ALIAS, {})

        # Return the configured fanout threshold or the default if not specified
        return metric_config.get(self.DOWNSTREAM_MODEL_TYPES_STR, self.DOWNSTREAM_MODEL_TYPES)

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        downstream_models = self._get_downstream_models()
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
                    in downstream_models
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
