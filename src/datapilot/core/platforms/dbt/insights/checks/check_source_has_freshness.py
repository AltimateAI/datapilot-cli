from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckSourceHasFreshness(ChecksInsight):
    NAME = "Source has freshness options"
    ALIAS = "check_source_has_freshness"
    DESCRIPTION = "Ensures that the source has freshness options"
    REASON_TO_FLAG = "Missing freshness options for the source can lead to confusion and inconsistency in analysis. "
    FRESHNESS_STR = "freshness"

    def _build_failure_result(self, source_id: int, missing_keys) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's parent schema is not whitelist or in blacklist.
        """
        missing_keys = ", ".join(missing_keys)
        failure_message = f"The source:{source_id} does not have freshness options defined for the following keys:\n {missing_keys}"

        recommendation = "Define the freshness options for the source to ensure consistency in analysis."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_id": source_id, "missing_keys": missing_keys},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate the insight response for the check. This method is called by the insight runner to generate the insight
        response for the check.
        Ensures that the source has freshness options
        """
        self.freshness_keys = self.get_check_config(self.FRESHNESS_STR) or []
        insights = []
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                missing_keys = self._check_source_has_freshness(node_id)
                if missing_keys:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, missing_keys),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_source_has_freshness(self, source_id: str) -> List[str]:
        source = self.get_node(source_id)
        freshness = source.freshness.dict() if source.freshness else {}

        if not freshness:
            return False

        missing_keys = []
        for key in self.freshness_keys:
            if key not in freshness:
                missing_keys.append(key)

        return missing_keys

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.FRESHNESS_STR: {
                    "type": "array",
                    "description": "The freshness options that should be defined for the source. If not provided, all freshness options are allowed.",
                    "items": {
                        "type": "string",
                        "enum": ["error_after", "warn_after"],
                    },
                },
            },
            "required": [cls.FRESHNESS_STR],
        }
        return config_schema
