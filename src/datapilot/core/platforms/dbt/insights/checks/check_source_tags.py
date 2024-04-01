from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceTags(ChecksInsight):
    NAME = "Source has tags"
    ALIAS = "check_source_tags"
    DESCRIPTION = "The source has only valid tags from the provided list."
    REASON_TO_FLAG = "The source has tags that are not in the valid tags list"
    TESTS_STR = "tags"

    def _build_failure_result(
        self,
        node_id: str,
        tags: List[str],
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a source's tags are not in the provided tag list.
        """

        failure_message = f"The source:{node_id}'s tags: {tags} are not in the provided tag list: {self.tag_list}\n"

        recommendation = "Update the source's tags to adhere to the provided tag list."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"tags": tags, "source_id": node_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each source in the DBT project,
        Ensures that the source has only valid tags from the provided list.
        The provided tag list is in the configuration file.
        """
        insights = []
        self.tag_list = self.get_check_config(self.TESTS_STR)
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                tag_list = self.valid_tag(node.tags)
                if tag_list:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, tag_list),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def valid_tag(self, tags: List[str]) -> List[str]:
        """
        Check if the tags of the source are in the provided tag list.
        """
        if not self.tag_list:
            return True
        tag_list = []
        for tag in tags:
            if tag not in self.tag_list:
                tag_list.append(tag)
        return tag_list
