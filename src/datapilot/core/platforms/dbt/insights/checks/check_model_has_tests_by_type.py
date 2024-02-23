from typing import List
from typing import Tuple

from datapilot.config.utils import get_test_type_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckModelHasTestsByType(ChecksInsight):
    NAME = "Check Model Has Tests By Type"
    ALIAS = "check_model_has_tests_by_type"
    DESCRIPTION = "Checks that the model has tests with specific types."
    REASON_TO_FLAG = "Models should have tests with specific types for proper validation."

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_types = get_test_type_configuration(self.config)

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                if self._model_has_tests_by_type(node, self.test_types):
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

    def _build_failure_result(self, model_unique_id: str, test_types: List[str]) -> DBTInsightResult:
        failure_message = (
            "The following models do not have tests with the specified names:\n{missing_tests}. "
            "Ensure that each model has tests with the specified names for proper validation."
        )
        recommendation = (
            "Add tests with the specified names for each model listed above. "
            "Having tests with specific names ensures proper validation and data integrity."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message.format(
                missing_tests=numbered_list(test_types),
            ),
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"missing_tests": test_types, "model_unique_id": model_unique_id},
        )

    def _model_has_tests_by_type(self, model, test_types: List[str]) -> bool:
        for test_type in test_types:
            if any(test.test_type == test_type for test in self.tests.values()):
                return True
        return False

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        if not has_catalog:
            return False, "Catalog is required for insight to run."

        return True, ""
