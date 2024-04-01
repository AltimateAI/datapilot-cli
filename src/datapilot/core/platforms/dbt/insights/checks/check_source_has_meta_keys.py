from typing import List
from typing import Set

from datapilot.config.utils import get_insight_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckSourceHasMetaKeys(ChecksInsight):
    NAME = "Source has required metadata keys"
    ALIAS = "check_source_has_meta_keys"
    DESCRIPTION = "Check if the source has required metadata keys"
    REASON_TO_FLAG = "Missing meta keys in the source can lead to inconsistency in metadata management and understanding of the source. It's important to ensure that the source includes all the required meta keys as per the configuration."
    META_KEYS_STR = "meta_keys"
    ALLOW_EXTRA_KEYS_STR = "allow_extra_keys"

    def _build_failure_result(
        self,
        source_id: int,
        missing: Set[str],
        extra: Set[str],
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's parent schema is not whitelist or in blacklist.
        """
        failure_message = ""
        if missing:
            failure_message += f"The source:{source_id} does not have the following meta keys defined: {numbered_list(missing)}\n"
        if extra:
            failure_message += f"The source:{source_id} has the following extra meta keys defined: {numbered_list(extra)}\n"

        recommendation = "Define the meta keys for the source to ensure consistency in analysis."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_id": source_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate the insight response for the check. This method is called by the insight runner to generate the insight
        response for the check.
        Ensures that the source has a list of valid meta keys.
        meta_keys are provided in the configuration file.
        """
        insights = []
        self.insight_config = get_insight_configuration(self.config)
        self.meta_keys = self.get_check_config(self.META_KEYS_STR) or []
        self.allow_extra_keys = self.get_check_config(self.ALLOW_EXTRA_KEYS_STR)
        if not self.meta_keys and not self.allow_extra_keys:
            self.logger.error(f"Meta keys are not provided in the configuration file for the insight: {self.ALIAS}")
            return insights

        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                status_code, missing, extra = self._check_source_has_meta_keys(node_id)
                if status_code:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, missing, extra),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_source_has_meta_keys(self, source_unique_id: str):
        status_code = 0
        model = self.get_node(source_unique_id)
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
                    "description": "A list of metadata keys that should be present in the sources properties.",
                },
                cls.ALLOW_EXTRA_KEYS_STR: {
                    "type": "boolean",
                    "default": False,
                },
            },
            "required": [cls.META_KEYS_STR],
        }
        return config_schema
