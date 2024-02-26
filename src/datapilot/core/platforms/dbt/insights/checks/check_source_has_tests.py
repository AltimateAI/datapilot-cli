from typing import List
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckSourceHasTests(ChecksInsight):
    NAME = "Check Source Has Tests"
    ALIAS = "check_source_has_tests"
    DESCRIPTION = "Check if the source has tests"
    REASON_TO_FLAG = "The source table is missing tests. Ensure that the source table has tests."

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if not self._source_has_tests(node_id):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, self.test_groups),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, source_unique_id: str, test_groups: List[str]) -> DBTInsightResult:
        failure_message = (
            "The source table `{source_unique_id}` is missing the following tests: {test_groups}. "
            "Ensure that the source table has the required tests."
        )
        recommendation = (
            "Add the following tests to the source table `{source_unique_id}`: {test_groups}. "
            "Ensuring that the source table has the required tests helps in maintaining data integrity and consistency."
        )
        return DBTInsightResult(
            failure_message=failure_message.format(source_unique_id=source_unique_id, test_groups=numbered_list(test_groups)),
            recommendation=recommendation.format(source_unique_id=source_unique_id, test_groups=numbered_list(test_groups)),
            metadata={"source_unique_id": source_unique_id, "test_groups": test_groups},
        )

    def _source_has_tests(self, node_id) -> bool:
        source = self.get_node(node_id)
        if not source.test_metadata:
            return False
        return True

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        if not has_catalog:
            return False, "Catalog is required for insight to run."

        return True, ""
