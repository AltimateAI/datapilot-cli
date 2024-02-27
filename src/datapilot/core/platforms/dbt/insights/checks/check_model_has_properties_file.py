from typing import List
from typing import Sequence
from typing import Set
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckModelHasPropertiesFile(ChecksInsight):
    NAME = "Check Model Has Properties File"
    ALIAS = "check_model_has_properties_file"
    DESCRIPTION = (
        "Checks that the model has a properties file (.yml) defined. "
        "Having a properties file helps in providing additional configuration and documentation for the model."
    )
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
                status_code, missing_models = self._check_properties_file(node_id)
                if status_code == 1:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, missing_models),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, model_unique_id: str, missing_models: Sequence[str]) -> DBTInsightResult:
        failure_message = (
            "The following models do not have a properties file (.yml) defined:\n{missing_models}. "
            "Ensure that each model has a corresponding .yml file for additional configuration and documentation."
        )
        recommendation = (
            "Add a properties file (.yml) for each model listed above. "
            "Having a properties file helps in providing additional configuration and documentation for the model."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message.format(
                missing_models=numbered_list(missing_models),
            ),
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"missing_models": missing_models, "model_unique_id": model_unique_id},
        )

    def _check_properties_file(self, node_id) -> Tuple[int, Set[str]]:
        status_code = 0
        missing_models = set()
        node = self.get_node(node_id)
        if node.resource_type == AltimateResourceType.model and not node.patch_path:
            missing_models.add(node.original_file_path)
            status_code = 1
        return status_code, missing_models
