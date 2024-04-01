from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class DBTModelsMultipleSourcesJoined(DBTModellingInsight):
    """
    DBTModelsMultipleSourcesJoined identifies models in a dbt project that reference more than one source.
    """

    NAME = "Multiple sources joined"
    ALIAS = "multiple_sources_joined"
    DESCRIPTION = "Models should not directly join multiple sources."
    REASON_TO_FLAG = (
        "Best practice is to have a single staging model per source and use this staging model as a "
        "dependency for downstream models. Directly joining multiple sources in a single model can "
        "lead to data management complexities and inconsistencies."
    )
    FAILURE_MESSAGE = (
        "Model `{model_id}` directly uses multiple sources, which may complicate data management and lineage tracking. "
        "Detected sources: \n{sources_list}"
    )
    RECOMMENDATION = (
        "Consider refactoring `{model_id}` to reference a single source or "
        "intermediate models that consolidate these sources. This approach simplifies data lineage"
        " and improves maintainability."
    )

    def _build_failure_result(self, model_id: str, source_dependencies: List[str]) -> DBTInsightResult:
        failure = self.FAILURE_MESSAGE.format(
            model_id=model_id,
            sources_list=numbered_list(source_dependencies),
        )
        recommendation = self.RECOMMENDATION.format(model_id=model_id)
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": model_id,
                "source_dependencies": source_dependencies,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.logger.debug(f"Generating insights for DBTModelsMultipleSourcesJoined for project {self.manifest.get_package()}")

        insights = []

        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue

            if node.resource_type == AltimateResourceType.model:
                source_dependencies = [
                    dependent_node_id
                    for dependent_node_id in node.depends_on.nodes
                    if self.get_node(dependent_node_id).resource_type == AltimateResourceType.source
                ]

                if len(source_dependencies) > 1:
                    self.logger.debug(f"Model {node_id} references multiple sources")
                    insight_result = self._build_failure_result(node_id, source_dependencies)
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
