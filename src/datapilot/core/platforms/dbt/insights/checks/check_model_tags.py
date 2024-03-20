from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelTags(ChecksInsight):
    NAME = "Model only has valid tags"
    ALIAS = "check_model_tags"
    DESCRIPTION = "Ensures that the model has only valid tags from the provided list."
    REASON_TO_FLAG = "The model has tags that are not in the valid tags list"
    TAGS_LIST_STR = "tag_list"

    def _build_failure_result(
        self,
        node_id: str,
        tags: List[str],
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's tags are not in the provided tag list.
        """

        failure_message = f"The model:{node_id}'s tags are not in the provided tag list:\n"

        recommendation = "Update the model's tags to adhere to the provided tag list."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"tags": tags},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        Ensures that the model has only valid tags from the provided list.
        The provided tag list is in the configuration file.
        """
        insights = []
        self.tag_list = self.get_check_config(self.TAGS_LIST_STR)
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                if not self.valid_tag(node.config.tags):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, node.config.tags),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def valid_tag(self, tags: List[str]) -> bool:
        """
        Check if the tags of the model are in the provided tag list.
        """
        if not self.tag_list:
            return True
        return all(tag in self.tag_list for tag in tags)

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.TAGS_LIST_STR: {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of allowed tags for the model. If not provided, all tags are allowed.",
                    "default": [],
                },
            },
        }
