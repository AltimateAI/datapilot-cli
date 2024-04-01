from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class DBTSourceFanout(DBTModellingInsight):
    """
    DBTSourceFanout identifies instances where a source is the direct parent of multiple resources in the DAG.
    """

    NAME = "Source fanout analysis"
    ALIAS = "source_fanout"
    DESCRIPTION = "Identifies sources with a high number of direct children."
    REASON_TO_FLAG = (
        "Identifying sources with high fanout can indicate areas where the data model might be overly complex "
        "or dependent on a single source. Such dependencies can introduce risks and "
        "complicate maintenance and scalability."
    )
    SOURCE_FANOUT_THRESHOLD = 1  # Default threshold, can be overridden as needed
    FAILURE_MESSAGE = (
        "Source `{source_unique_id}` has `{children_count}` direct children, "
        "exceeding the fanout threshold of `{fanout_threshold}`. This level of fanout may lead to increased complexity."
    )
    RECOMMENDATION = (
        "Review the source `{source_unique_id}` to identify opportunities to reduce its direct dependencies. "
        "This can help in simplifying the data model and reducing the risk associated with high source reliance."
    )
    SOURCE_FANOUT_THRESHOLD_STR = "max_fanout"

    def _build_failure_result(self, source_unique_id: str, children_count: int, fanout_threshold: int) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(
            source_unique_id=source_unique_id,
            children_count=children_count,
            fanout_threshold=fanout_threshold,
        )

        recommendation = self.RECOMMENDATION.format(source_unique_id=source_unique_id)
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "source": source_unique_id,
                "direct_children_count": children_count,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        fanout_threshold = self.get_check_config(self.SOURCE_FANOUT_THRESHOLD_STR) or self.SOURCE_FANOUT_THRESHOLD
        insights = []

        for node_id, children_set in self.children_map.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            node = self.get_node(node_id)
            if node.resource_type == AltimateResourceType.source:
                if len(children_set) > fanout_threshold:
                    insight_result = self._build_failure_result(
                        source_unique_id=node_id,
                        children_count=len(children_set),
                        fanout_threshold=fanout_threshold,
                    )

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
                cls.SOURCE_FANOUT_THRESHOLD_STR: {
                    "type": "integer",
                    "description": "The maximum number of direct children a source can have before being flagged.",
                    "default": cls.SOURCE_FANOUT_THRESHOLD,
                },
            },
            "required": [cls.SOURCE_FANOUT_THRESHOLD_STR],
        }
        return config_schema
