from typing import List

from datapilot.config.utils import get_source_max_childs_configuration
from datapilot.config.utils import get_source_min_childs_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceChilds(ChecksInsight):
    NAME = "Check Source Childs"
    ALIAS = "check_source_childs"
    DESCRIPTION = "Check the source has a specific number (max/min) of childs"
    REASON_TO_FLAG = "The source has a number of childs that is not in the valid range"

    def _build_failure_result(
        self,
        node_id: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a source has a number of childs that is not in the valid range.
        """
        failure_message = (
            "The source `{source_unique_id}` has the following number of childs: {number_of_childs}. "
            "Ensure that the source has a specific number (max/min) of childs."
        )
        recommendation = (
            "Ensure that the source `{source_unique_id}` has a specific number (max/min) of childs. "
            "Ensuring that the source has a specific number (max/min) of childs helps in maintaining data integrity and consistency."
        )
        return DBTInsightResult(
            failure_message=failure_message.format(source_unique_id=node_id, number_of_childs=len(self.children_map.get(node_id, []))),
            recommendation=recommendation.format(source_unique_id=node_id),
            metadata={"source_unique_id": node_id, "number_of_childs": len(self.children_map.get(node_id, []))},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each source in the DBT project,
        Check the source has a specific number (max/min) of childs
        The min and max number of childs is in the configuration file.
        """
        insights = []
        self.min_childs = get_source_min_childs_configuration(self.config)
        self.max_childs = get_source_max_childs_configuration(self.config)
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
                            insight=self._build_failure_result(node_id),
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
