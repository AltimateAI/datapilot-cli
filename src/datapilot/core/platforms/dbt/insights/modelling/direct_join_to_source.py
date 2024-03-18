from typing import Dict
from typing import List
from typing import Optional

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import SOURCE
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class DBTDirectJoinSource(DBTModellingInsight):
    """
    DBTDirectJoinSource is used to ensure that DBT models have a proper mapping
    from source to staging models and to flag any direct dependencies on multiple
    sources without intermediate staging models.
    Ref: https://github.com/dbt-labs/dbt-project-evaluator/blob/main/models/marts/dag/fct_direct_join_to_source.sql
    """

    ALIAS = "source_staging_model_integrity"
    NAME = "Source-Staging Model Integrity"
    DESCRIPTION = "A model should not have direct joins to both sources and other staging models. "
    REASON_TO_FLAG = (
        "Flagged when a model directly joins a source and a model without a staging intermediary. "
        "Direct source-model joins bypass the staging layer, leading to potential inconsistencies in data handling."
    )
    FAILURE_MESSAGE = (
        "Model `{current_model_unique_id}` has direct joins to both sources and other models. "
        "\n### Detected Sources\n{sources}\n\n### Connected Models \n{models}"
    )
    RECOMMENDATION = (
        "Create a dedicated staging model for the source(s) and modify `{current_model_unique_id}` "
        "to depend on this staging model. This ensures consistent initial data processing steps."
    )

    def _build_failure_result(self, current_model_unique_id: str, dependencies: Dict) -> DBTInsightResult:
        """
        Build failure result for the insight if a model is directly joining to a source
        and other models.

        :param current_model_unique_id: Unique ID of the current model being evaluated.
        :param dependencies: A dictionary of dependencies categorized as 'source' and 'model'.
        :return: An instance of InsightResult containing failure message and recommendation and metadata.
        """
        self.logger.debug(f"Found multiple sources and models for {current_model_unique_id}")
        failure = self.FAILURE_MESSAGE.format(
            current_model_unique_id=current_model_unique_id,
            sources=numbered_list(dependencies["source"]),
            models=numbered_list(dependencies["model"]),
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
                "dependencies": dependencies,
            },
        )

    def _check_dependency_on_both_models_and_sources(self, current_node) -> Optional[DBTInsightResult]:
        """
        Check if the current node has dependencies on both models and sources or multiple sources.

        :param current_node: The node representing the current model.
        :return: A list of InsightResult instances with recommendations for each violation.
        """
        self.logger.debug(f"Checking dependencies for model {current_node.unique_id}")
        dependencies = {
            MODEL: [],
            SOURCE: [],
        }
        excluded_nodes = self.config.get("excluded_nodes", [])
        for dependent_unique_id in current_node.depends_on.nodes:
            if dependent_unique_id in excluded_nodes:
                self.logger.debug(f"Skipping dependency {dependent_unique_id} as it is excluded list of the config")
                continue
            dependent_node = self.get_node(dependent_unique_id)
            if dependent_node.resource_type == AltimateResourceType.model:
                self.logger.debug(f"Found dependent model {dependent_unique_id} for model {current_node.unique_id}")
                dependencies[MODEL].append(dependent_node.unique_id)
            elif dependent_node.resource_type == AltimateResourceType.source:
                self.logger.debug(f"Found dependent source {dependent_unique_id} for model {current_node.unique_id}")
                dependencies[SOURCE].append(dependent_node.unique_id)
        if dependencies[MODEL] and dependencies[SOURCE]:
            self.logger.debug(f"Found dependencies on both models and sources for model {current_node.unique_id}")
            return self._build_failure_result(current_node.unique_id, dependencies)
        else:
            self.logger.debug(f"No dependencies on both models and sources for model {current_node.unique_id}")
        return None

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        containing insights about direct source dependencies.
        :return: A list of InsightResponse objects.
        """
        self.logger.debug(f"Generating insights for DBTDirectJoinSource for project {self.project_name}")
        recommendations = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                recommendation = self._check_dependency_on_both_models_and_sources(node)
                if recommendation:
                    self.logger.debug(f"Found recommendation for model {node_id} in DBTDirectJoinSource")
                    recommendations.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.path,
                            original_file_path=node.original_file_path,
                            insight=recommendation,
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return recommendations
