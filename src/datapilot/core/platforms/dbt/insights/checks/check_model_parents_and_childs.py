from typing import List
from typing import Tuple

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
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a column has specific number (max/min) of parents or/and childs.

        :return: An instance of InsightResult containing failure message and recommendation.
        """

        failure_message = f"The model:{node_id} doesn't have the required number of parents or childs. Min parents: {min_parents}, Max parents: {max_parents}:\n"

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
            metadata={"min_parents": min_parents, "max_parents": max_parents},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        ensures that the model has a specific number (max/min) of parents or/and childs.
        The parent and child numbers are defined in the config file.
        The parent and corresponding child information is present in self.children_map
        """
        insights = []
        self.min_parents = self.config.get("min_parents", 0)
        self.max_parents = self.config.get("max_parents", 0)

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
                            insight=self._build_failure_result(node_id, self.min_parents, self.max_parents),
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
        return self.min_parents <= len(parents) <= self.max_parents and self.min_parents <= len(children) <= self.max_parents

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        return True, ""
