from typing import ClassVar
from typing import List

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import INTERMEDIATE
from datapilot.core.platforms.dbt.constants import MART
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.utils import classify_model_type
from datapilot.utils.formatting.utils import numbered_list


class DBTDownstreamModelsDependentOnSource(DBTModellingInsight):
    """
    DBTDownstreamModelsDependentOnSource identifies downstream models (either marts or intermediate)
    in a dbt project that depend directly on a source node.
    """

    NAME = "Downstream models dependent on source"
    ALIAS = "downstream_source_dependence"
    DESCRIPTION = "Downstream models should not depend directly on source nodes. "
    REASON_TO_FLAG = (
        "Direct dependency of marts or intermediate models on a source node suggests a missing staging model. "
        "Staging models serve as atomic units, maintaining a one-to-one relationship with source data tables, "
        "while providing a consistent format for downstream consumption."
    )
    FAILURE_MESSAGE = (
        "Downstream model `{current_model_unique_id}` of type {model_type} is directly dependent on a source nodes."
        "Direct source dependencies bypass the critical staging layer, leading to potential data consistency issues."
        " Source dependencies: {source_dependencies}"
    )
    RECOMMENDATION = (
        "Introduce or utilize an existing staging model for the source node involved. Refactor the downstream model "
        "`{current_model_unique_id}` to select from this staging layer, ensuring a proper abstraction layer between "
        "raw data and downstream data artifacts."
    )
    MODEL_TYPES: ClassVar[List[str]] = [INTERMEDIATE, MART]

    def _build_failure_result(
        self,
        current_model_unique_id: str,
        source_dependencies: List[str],
        model_type: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a downstream model depends directly on a source node.

        :param current_model_unique_id: Unique ID of the current model being evaluated.
        :param source_dependencies: List of source dependencies for the current model.
        :return: An instance of InsightResult containing failure message and recommendation.
        """
        self.logger.debug(f"Building failure result for model {current_model_unique_id} with direct source dependencies")

        failure = self.FAILURE_MESSAGE.format(
            current_model_unique_id=current_model_unique_id,
            model_type=model_type,
            source_dependencies=numbered_list(source_dependencies),
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=self.RECOMMENDATION.format(current_model_unique_id=current_model_unique_id),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": current_model_unique_id,
                "source_dependencies": source_dependencies,
                "model_type": model_type,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each downstream model in the DBT project,
        identifying those that depend directly on source nodes.
        :return: A list of InsightResponse objects.
        """
        self.logger.debug(f"Generating insights for DBTDownstreamModelsDependentOnSource for project {self.manifest.get_package()}")
        insights = []
        regex_configuration = get_regex_configuration(self.config)
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                model_type = classify_model_type(node.name, node.original_file_path, regex_configuration)
                source_dependencies = [
                    dependent_node_id
                    for dependent_node_id in node.depends_on.nodes
                    if self.get_node(dependent_node_id).resource_type == AltimateResourceType.source
                ]

                if source_dependencies and model_type in self.MODEL_TYPES:
                    self.logger.debug(f"Found downstream model {node_id} of type {model_type} with direct source dependencies")
                    insight_result = self._build_failure_result(node.unique_id, source_dependencies, model_type)
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
        self.logger.debug(
            f"Finished generating insights for DBTDownstreamModelsDependentOnSource. Found  {len(insights)} models with direct source dependencies"
        )
        return insights
