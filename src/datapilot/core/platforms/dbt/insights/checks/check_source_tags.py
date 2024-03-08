from typing import List

from datapilot.config.utils import get_tag_list_configuration
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
        """
        Build failure result for the insight if a source's tags are not in the provided tag list.
        """

        failure_message = f"The source:{node_id}'s tags are not in the provided tag list:\n"

        recommendation = "Update the source's tags to adhere to the provided tag list."

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
        Generate a list of InsightResponse objects for each source in the DBT project,
        Ensures that the source has only valid tags from the provided list.
        The provided tag list is in the configuration file.
        """
        insights = []
        self.tag_list = get_tag_list_configuration(self.config)
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self.valid_tag(node.tags):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, node.tags),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def valid_tag(self, tags: List[str]) -> bool:
        """
        Check if the tags of the source are in the provided tag list.
        """
        if not self.tag_list:
            return True
        return all(tag in self.tag_list for tag in tags)
