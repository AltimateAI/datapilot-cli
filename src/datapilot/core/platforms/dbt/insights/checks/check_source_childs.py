from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceChilds(ChecksInsight):
    NAME = "Source has allowed number of children"
    ALIAS = "check_source_childs"
    DESCRIPTION = "Check the source has a specific number (max/min) of childs"
    REASON_TO_FLAG = "The source has a number of childs that is not in the valid range"
    MIN_CHILDS_STR = "min_childs"
    MAX_CHILDS_STR = "max_childs"

    def _build_failure_result(
        self,
        node_id: str,
        min_childs: int,
        max_childs: int,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a source has a specific number (max/min) of childs
        """
        failure_message = f"The source:{node_id} has a number of childs that is not in the valid range:\n"
        failure_message += f"Min childs: {min_childs}\n"
        failure_message += f"Max childs: {max_childs}\n"

        recommendation = "Update the source to adhere to the valid range of childs."
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_unique_id": node_id, "min_childs": min_childs, "max_childs": max_childs},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each source in the DBT project,
        Check the source has a specific number (max/min) of childs
        The min and max number of childs is in the configuration file.
        """
        insights = []
        self.min_childs = self.get_check_config(self.MIN_CHILDS_STR)
        self.max_childs = self.get_check_config(self.MAX_CHILDS_STR)
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self.valid_childs(node_id):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, min_childs=self.min_childs, max_childs=self.max_childs),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def valid_childs(self, source_unique_id: str) -> bool:
        """
        Check if the source has a specific number (max/min) of childs
        """
        source_childs = self.children_map.get(source_unique_id, [])
        if self.min_childs and len(source_childs) < self.min_childs:
            return False
        if self.max_childs and len(source_childs) > self.max_childs:
            return False
        return True

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
            },
        }
        return config_schema
