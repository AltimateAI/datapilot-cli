from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelHasPropertiesFile(ChecksInsight):
    NAME = "Model has properties file"
    ALIAS = "check_model_has_properties_file"
    DESCRIPTION = "Models should have a properties/schema file (.yml) defined."
    REASON_TO_FLAG = (
        "Missing properties file for a model can lead to inadequate configuration and documentation, "
        "resulting in potential issues in data processing and understanding."
    )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                status_code = self._check_properties_file(node_id)
                if status_code == 1:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, model_unique_id: str) -> DBTInsightResult:
        failure_message = (
            f"The model {model_unique_id} do not have a properties file (.yml) defined."
            "Ensure that each model has a corresponding .yml file for additional configuration and documentation."
        )
        recommendation = (
            "Add a properties file (.yml) for each model listed above. "
            "Having a properties file helps in providing additional configuration and documentation for the model."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"model_unique_id": model_unique_id},
        )

    def _check_properties_file(self, node_id) -> int:
        status_code = 0
        node = self.get_node(node_id)
        if node.resource_type == AltimateResourceType.model and not node.patch_path:
            status_code = 1
        return status_code
