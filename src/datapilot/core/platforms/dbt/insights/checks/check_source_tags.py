from typing import List
from typing import Tuple

from datapilot.config.utils import get_source_tag_list_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceTags(ChecksInsight):
    NAME = "Check Source Tags"
    ALIAS = "check_source_tags"
    DESCRIPTION = "Ensures that the source has only valid tags from the provided list."
    REASON_TO_FLAG = "The source has tags that are not in the valid tags list"

    def _build_failure_result(
        self,
        node_id: str,
        tags: List[str],
    ) -> DBTInsightResult:
        failure_message = (
            "The source `{source_unique_id}` has the following tags: {tags}. "
            "Ensure that the source has only valid tags from the provided list."
        )
        recommendation = (
            "Remove the following tags from the source `{source_unique_id}`: {tags}. "
            "Ensuring that the source has only valid tags from the provided list helps in maintaining data integrity and consistency."
        )
        return DBTInsightResult(
            failure_message=failure_message.format(source_unique_id=node_id, tags=tags),
            recommendation=recommendation.format(source_unique_id=node_id, tags=tags),
            metadata={"source_unique_id": node_id, "tags": tags},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each source in the DBT project,
        Ensures that the source has only valid tags from the provided list.
        The provided tag list is in the configuration file.
        """
        insights = []
        self.tag_list = get_source_tag_list_configuration(self.config)
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
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
        Check if the tags are valid.
        """
        for tag in tags:
            if tag not in self.tag_list:
                return False
        return True

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        return True, ""
