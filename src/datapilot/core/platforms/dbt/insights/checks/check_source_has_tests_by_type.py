from typing import List
from typing import Tuple

from datapilot.config.utils import get_source_test_type_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckSourceHasTestsByType(ChecksInsight):
    NAME = "Check Source Has Tests By Type"
    ALIAS = "check_source_has_tests_by_type"
    DESCRIPTION = "Check if the source has tests with the specified types"
    REASON_TO_FLAG = (
        "The source table is missing tests with the specified types. Ensure that the source table has tests with the specified types."
    )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        self.test_types = get_source_test_type_configuration(self.config)
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                if self._source_has_tests_by_type(node_id, self.test_types):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, self.test_types),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, source_unique_id: str, test_types: List[str]) -> DBTInsightResult:
        failure_message = (
            "The source table `{source_unique_id}` is missing the following tests: {test_types}. "
            "Ensure that the source table has the required tests."
        )
        recommendation = (
            "Add the following tests to the source table `{source_unique_id}`: {test_types}. "
            "Ensuring that the source table has the required tests helps in maintaining data integrity and consistency."
        )
        return DBTInsightResult(
            failure_message=failure_message.format(source_unique_id=source_unique_id, test_types=numbered_list(test_types)),
            recommendation=recommendation.format(source_unique_id=source_unique_id, test_types=numbered_list(test_types)),
            metadata={"source_unique_id": source_unique_id, "test_types": test_types},
        )

    def _source_has_tests_by_type(self, node_id, test_types: List[str]) -> bool:
        source = self.get_node(node_id)
        if source.test_type in test_types:
            return True
        return False

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        if not has_catalog:
            return False, "Catalog is required for insight to run."

        return True, ""
