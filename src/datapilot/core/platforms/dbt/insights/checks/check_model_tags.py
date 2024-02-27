from typing import List

from datapilot.config.utils import get_tag_list_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelTags(ChecksInsight):
    NAME = "Check Model Tags"
    ALIAS = "check_model_tags"
    DESCRIPTION = "Ensures that the model has only valid tags from the provided list."
    REASON_TO_FLAG = "The model has tags that are not in the valid tags list"

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
        self.tag_list = get_tag_list_configuration(self.config)
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                if not self.valid_tag(node.config.tags):
                    insights.append(
                        DBTModelInsightResponse(
                            node_id=node_id,
                            result=self._build_failure_result(node_id, node.config.tags),
                            severity=get_severity(self.TYPE, self.config),
                        )
                    )
        return insights

    def valid_tag(self, tags: List[str]) -> bool:
        """
        Check if the tags of the model are in the provided tag list.
        """
        if not self.tag_list:
            return True
        for tag in tags:
            if tag not in self.tag_list:
                return False
        return True
