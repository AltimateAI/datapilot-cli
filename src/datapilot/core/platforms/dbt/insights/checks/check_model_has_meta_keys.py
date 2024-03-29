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


class CheckModelHasMetaKeys(ChecksInsight):
    NAME = "Model has all valid keys in metadata"
    ALIAS = "check_model_has_valid_meta_keys"
    DESCRIPTION = "Model always has a list of valid metadata keys."
    REASON_TO_FLAG = (
        "Missing meta keys in the model can lead to inconsistency in metadata management and understanding of the model. "
        "It's important to ensure that the model includes all the required meta keys as per the configuration."
    )
    META_KEYS_STR = "meta_keys"
    ALLOW_EXTRA_KEYS_STR = "allow_extra_keys"

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.meta_keys = self.get_check_config(self.META_KEYS_STR)
        self.allow_extra_keys = self.get_check_config(self.ALLOW_EXTRA_KEYS_STR)
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                status_code, missing_keys, extra_keys = self._check_meta_keys(node_id)
                if status_code == 1:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, missing_keys, extra_keys),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, model_unique_id: str, missing_keys: Sequence[str], extra_keys: Set[str]) -> DBTInsightResult:
        failure_message = (
            f"The following meta keys are missing in the model `{model_unique_id}`:\n{numbered_list(missing_keys)}. "
            "Ensure that the model includes all the required meta keys."
        )
        if not self.allow_extra_keys:
            failure_message += (
                f"The following extra meta keys are present in the model `{model_unique_id}`:\n{numbered_list(extra_keys)}. "
                "Ensure that the model does not include any extra meta keys."
            )
        recommendation = (
            f"Add the missing meta keys listed above in the model `{model_unique_id}`. "
            "Ensuring that the model has all the required meta keys helps in maintaining metadata consistency and understanding."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation.format(model_unique_id=model_unique_id),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"missing_keys": missing_keys, "model_unique_id": model_unique_id, "extra_keys": extra_keys},
        )

    def _check_meta_keys(self, node_id) -> Tuple[int, Set[str], Set[str]]:
        status_code = 0
        model = self.get_node(node_id)
        meta = model.meta.dict() if model.meta else {}
        model_meta_keys = set(meta.keys())
        missing_keys = None
        extra_keys = None
        if model.meta:
            missing_keys = model_meta_keys - set(model.meta.keys())
        if missing_keys:
            status_code = 1
        if not self.allow_extra_keys:
            extra_keys = set(model.meta.keys()) - model_meta_keys
        return status_code, missing_keys, extra_keys

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.META_KEYS_STR: {
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                    "description": "A list of metadata keys that should be present in the model properties files.",
                },
                cls.ALLOW_EXTRA_KEYS_STR: {
                    "type": "boolean",
                    "default": False,
                },
            },
            "required": [cls.META_KEYS_STR],
        }
        return config_schema
