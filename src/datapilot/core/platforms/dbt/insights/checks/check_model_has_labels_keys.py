from typing import List
from typing import Sequence
from typing import Set
from typing import Tuple

from datapilot.config.utils import get_labels_keys_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckModelHasLabelsKeys(ChecksInsight):
    NAME = "Check Model Has Labels Keys"
    ALIAS = "check_model_has_labels_keys"
    DESCRIPTION = (
        "Checks that the model has the specified labels keys as defined in the properties file. "
        "Ensuring that the model has the required labels keys helps in maintaining metadata consistency and understanding."
    )
    REASON_TO_FLAG = (
        "Missing labels keys in the model can lead to inconsistency in metadata management and understanding of the model. "
        "It's important to ensure that the model includes all the required labels keys as per the configuration."
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.labels_keys = get_labels_keys_configuration(self.config)

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                status_code, missing_models = self._check_labels_keys(node_id)
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

    def _build_failure_result(self, model_unique_id: str, missing_keys: Sequence[str]) -> DBTInsightResult:
        failure_message = (
            "The following labels keys are missing in the model `{model_unique_id}`:\n{missing_keys}. "
            "Ensure that the model includes all the required labels keys."
        )
        recommendation = (
            "Add the missing labels keys listed above in the model `{model_unique_id}`. "
            "Ensuring that the model has all the required labels keys helps in maintaining metadata consistency and understanding."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message.format(
                missing_keys=numbered_list(missing_keys),
                model_unique_id=model_unique_id,
            ),
            recommendation=recommendation.format(model_unique_id=model_unique_id),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"missing_keys": missing_keys, "model_unique_id": model_unique_id},
        )

    def _check_labels_keys(self, node_id) -> Tuple[int, Set[str]]:
        status_code = 0
        missing_keys = set(self.labels_keys) - set(self.get_node(node_id).label)
        if missing_keys:
            status_code = 1
        return status_code, missing_keys
