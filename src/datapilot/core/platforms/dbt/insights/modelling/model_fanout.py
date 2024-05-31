from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class DBTModelFanout(DBTModellingInsight):
    """
    DBTModelFanout identifies parent models in a dbt project with more than a specified number
    of direct leaf children, indicating a high model fanout.
    """

    NAME = "Model fanout analysis"
    ALIAS = "model_fanout"
    DESCRIPTION = "Identifies parent models with an unusually high number of children. "
    REASON_TO_FLAG = (
        "Flagged to highlight parent models with an unusually high number of leaf children. This can suggest areas "
        "in the data pipeline where complexity is increased and transformations might be optimized."
    )
    FANOUT_THRESHOLD = 3  # Default threshold, can be overridden as needed
    FAILURE_MESSAGE = (
        "Model `{parent_model_unique_id}` has `{leaf_children}` leaf children, "
        "exceeding the fanout threshold of `{fanout_threshold}`. This level of fanout may lead to increased complexity."
    )
    RECOMMENDATION = (
        "Consider reviewing and restructuring `{parent_model_unique_id}` to simplify its dependencies. "
        "Reducing the number of leaf children can lead to a more streamlined and maintainable data pipeline."
    )

    FANOUT_THRESHOLD_STR = "max_fanout"

    def _build_failure_result(
        self,
        parent_model_unique_id: str,
        leaf_children: List[str],
        fanout_threshold: int,
    ) -> DBTInsightResult:
        # Logic to build the failure result
        self.logger.debug(f"Found {len(leaf_children)} leaf children for {parent_model_unique_id}")
        failure_message = self.FAILURE_MESSAGE.format(
            parent_model_unique_id=parent_model_unique_id,
            leaf_children=len(leaf_children),
            fanout_threshold=fanout_threshold,
        )

        recommendation = self.RECOMMENDATION.format(
            parent_model_unique_id=parent_model_unique_id,
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": parent_model_unique_id,
                "leaf_children_count": len(leaf_children),
                "leaf_children": leaf_children,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        fanout_threshold = self.get_check_config(self.FANOUT_THRESHOLD_STR) or self.FANOUT_THRESHOLD
        insights = []
        self.logger.debug(f"Checking for models with fanout greater than {fanout_threshold}")
        for parent, children_set in self.children_map.items():
            if self.should_skip_model(parent):
                self.logger.debug(f"Skipping model {parent} as it is not enabled for selected models")
                continue

            node = self.get_node(parent)
            if node.resource_type != AltimateResourceType.model:
                continue

            leaf_children = [
                child
                for child in children_set
                if len(self.children_map[child]) == 0
                and self.get_node(child).resource_type
                not in [
                    AltimateResourceType.test,
                    AltimateResourceType.analysis,
                    AltimateResourceType.metric,
                ]
            ]

            if len(leaf_children) > fanout_threshold:
                insight_result = self._build_failure_result(parent, leaf_children, fanout_threshold)
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=parent,
                        package_name=node.package_name,
                        path=node.path,
                        original_file_path=node.original_file_path,
                        insight=insight_result,
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )

        self.logger.debug(f"Found {len(insights)} models with high fanout")
        return insights

    @classmethod
    def get_config_schema(cls):
        """
        :return: The configuration schema for the test coverage insight.
        """
        config_schema = super().get_config_schema()

        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.FANOUT_THRESHOLD_STR: {
                    "type": "integer",
                    "description": "The maximum number of direct leaf children a model can have before being flagged.",
                    "default": cls.FANOUT_THRESHOLD,
                },
            },
            "required": [cls.FANOUT_THRESHOLD_STR],
        }
        return config_schema
