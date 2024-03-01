from typing import List

from datapilot.config.utils import get_max_childs_configuration
from datapilot.config.utils import get_max_parents_configuration
from datapilot.config.utils import get_min_childs_configuration
from datapilot.config.utils import get_min_parents_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelParentsAndChilds(ChecksInsight):
    NAME = "Check Model Parents And Childs"
    ALIAS = "check_model_parents_and_childs"
    DESCRIPTION = "Ensures the model has a specific number (max/min) of parents or/and childs."
    REASON_TO_FLAG = (
        "Models with a specific number of parents or/and childs can lead to confusion and hinder effective data "
        "modeling and analysis. It's important to have consistent model relationships."
    )

    def _build_failure_result(
        self,
        node_id: str,
        min_parents: int,
        max_parents: int,
        min_childs: int,
        max_childs: int,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a column has specific number (max/min) of parents or/and childs.

        :return: An instance of InsightResult containing failure message and recommendation.
        """

        failure_message = f"The model:{node_id} doesn't have the required number of parents or childs.\n Min parents: {min_parents}, Max parents: {max_parents} Min childs: {min_childs}, Max childs: {max_childs}\n"

        recommendation = (
            "Update the model to adhere to have the required number of parents or childs."
            "Models not following the required number of parents or childs can lead to confusion and hinder effective data "
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"min_parents": min_parents, "max_parents": max_parents, "min_childs": min_childs, "max_childs": max_childs},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        ensures that the model has a specific number (max/min) of parents or/and childs.
        The parent and child numbers are defined in the config file.
        The parent and corresponding child information is present in self.children_map
        """
        insights = []
        self.min_parents = get_min_parents_configuration(self.config)
        self.max_parents = get_max_parents_configuration(self.config)
        self.min_childs = get_min_childs_configuration(self.config)
        self.max_childs = get_max_childs_configuration(self.config)

        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")

            if node.resource_type == AltimateResourceType.model:
                if not self._check_model_parents_and_childs(node_id):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(
                                node_id, self.min_parents, self.max_parents, self.min_childs, self.max_childs
                            ),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_model_parents_and_childs(self, model_unique_id: str) -> bool:
        """
        Check if the model has a specific number (max/min) of parents or/and childs.
        """
        children = self.children_map.get(model_unique_id, [])
        node = self.get_node(model_unique_id)
        parents = node.depends_on.nodes
        return self.min_parents <= len(parents) <= self.max_parents and self.min_childs <= len(children) <= self.max_childs
