from typing import List
from typing import Optional

from datapilot.config.utils import get_check_config
from datapilot.config.utils import get_insight_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelParentsAndChilds(ChecksInsight):
    NAME = "Model has specific number of parents or/and childs"
    ALIAS = "check_model_parents_and_childs"
    DESCRIPTION = "Ensures the model has a specific number (max/min) of parents or/and childs."
    REASON_TO_FLAG = (
        "Models with a specific number of parents or/and childs can lead to confusion and hinder effective data "
        "modeling and analysis. It's important to have consistent model relationships."
    )
    MIN_PARENTS_STR = "min_parents"
    MAX_PARENTS_STR = "max_parents"
    MIN_CHILDS_STR = "min_childs"
    MAX_CHILDS_STR = "max_childs"

    def _build_failure_result(
        self,
        node_id: str,
        failure_message: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a column has specific number (max/min) of parents or/and childs.

        :return: An instance of InsightResult containing failure message and recommendation.
        """
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
            metadata={
                "min_parents": self.min_parents,
                "max_parents": self.max_parents,
                "min_childs": self.min_childs,
                "max_childs": self.max_childs,
                "model_unique_id": node_id,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        ensures that the model has a specific number (max/min) of parents or/and childs.
        The parent and child numbers are defined in the config file.
        The parent and corresponding child information is present in self.children_map
        """
        insights = []
        self.insight_config = get_insight_configuration(self.config)
        self.min_parents = get_check_config(self.insight_config, self.ALIAS, self.MIN_PARENTS_STR)
        self.max_parents = get_check_config(self.insight_config, self.ALIAS, self.MAX_PARENTS_STR)
        self.min_childs = get_check_config(self.insight_config, self.ALIAS, self.MIN_CHILDS_STR)
        self.max_childs = get_check_config(self.insight_config, self.ALIAS, self.MAX_CHILDS_STR)

        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")

            if node.resource_type == AltimateResourceType.model:
                failure_message = self._check_model_parents_and_childs(node_id)
                if failure_message:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, failure_message),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_model_parents_and_childs(self, model_unique_id: str) -> Optional[str]:
        """
        Check if the model has a specific number (max/min) of parents or/and childs.
        """
        children = self.children_map.get(model_unique_id, [])
        node = self.get_node(model_unique_id)
        parents = node.depends_on.nodes
        message = ""
        if len(parents) < self.min_parents or len(parents) > self.max_parents:
            message += f"The model:{model_unique_id} doesn't have the required number of parents.\n Min parents: {self.min_parents}, Max parents: {self.max_parents}. It has f{len(parents)} parents\n"

        if len(children) < self.min_childs or len(children) > self.max_childs:
            message += f"The model:{model_unique_id} doesn't have the required number of childs.\n Min childs: {self.min_childs}, Max childs: {self.max_childs}. It has f{len(children)} childs\n"

        return message

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.MAX_CHILDS_STR: {
                    "type": "integer",
                    "description": "The maximum number of childs a model can have.",
                },
                cls.MIN_CHILDS_STR: {
                    "type": "integer",
                    "description": "The minimum number of childs a model can have.",
                    "default": "0",
                },
                cls.MAX_PARENTS_STR: {
                    "type": "integer",
                    "description": "The maximum number of parents a model can have.",
                },
                cls.MIN_PARENTS_STR: {
                    "type": "integer",
                    "description": "The minimum number of parents a model can have.",
                    "default": 0,
                },
            },
        }
        config_schema["files_required"] = cls.FILES_REQUIRED
        return config_schema
