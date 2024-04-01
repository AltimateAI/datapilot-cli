import re
from typing import Dict
from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.utils import is_superset_path


class CheckModelNameContract(ChecksInsight):
    NAME = "Valid Mmdel name by folder"
    ALIAS = "model_name_by_folder"
    DESCRIPTION = (
        "Check that model name abides to a contract (similar to check-column-name-contract). A contract consists of a regex pattern."
    )
    REASON_TO_FLAG = "Model naming convention is not as expected"
    DEFAULT_PATTERN_STR = "default_pattern"
    PATTERNS_LIST_STR = "patterns"
    PATTERN_STR = "pattern"
    FOLDER_STR = "folder"

    def _build_failure_result(
        self,
        node_id: str,
        failure: Dict[str, str],
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a column has a different name that doesn't match the contract.

        :return: An instance of InsightResult containing failure message and recommendation.
        """
        model_name = failure.get("model_name")
        model_path = failure.get("model_path")
        expected_pattern = failure.get("pattern")
        failure_message = (
            f"The model:{node_id} with name {model_name} in {model_path} does not match the contract pattern: {expected_pattern}."
        )

        recommendation = (
            "Update the model name to adhere to the contract. "
            "Consistent model naming conventions provide valuable context and aids in data understanding and collaboration."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"model_unique_id": node_id, **failure},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying models with model name that matches a certain regex pattern.
        """
        insights = []
        self.default_pattern = self.get_check_config(self.DEFAULT_PATTERN_STR)
        pattern_configs = self.get_check_config(self.PATTERNS_LIST_STR)
        if not pattern_configs:
            self.logger.debug(f"Model name contract not found in insight config for {self.ALIAS}. Skipping insight.")
            return []
        self.patterns = {
            pattern.get(self.FOLDER_STR): pattern.get(self.PATTERN_STR)
            for pattern in pattern_configs
            if pattern.get(self.PATTERN_STR) and pattern.get(self.FOLDER_STR)
        }
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                failure = self._check_model_name_contract(node_id)
                if failure:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, failure),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_model_name_contract(self, model_unique_id: str) -> bool:
        """
        Check if the model name abides to the contract.
        """
        model_name = self.get_node(model_unique_id).name
        model_path = self.get_node(model_unique_id).original_file_path
        for folder, pattern in self.patterns.items():
            if is_superset_path(folder, model_path):
                if re.match(pattern, model_name, re.IGNORECASE) is None:
                    return {"pattern": pattern, "model_name": model_name, "model_path": model_path}
        return {}

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.DEFAULT_PATTERN_STR: {
                    "type": "string",
                    "description": "The regex pattern to check the model name against",
                    "default": "^[a-z_]+$",
                },
                cls.PATTERNS_LIST_STR: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            cls.PATTERN_STR: {"type": "string", "description": "The regex pattern to check the model name against"},
                            cls.FOLDER_STR: {"type": "string", "description": "The folder to apply the pattern to."},
                        },
                        "required": [cls.PATTERN_STR, cls.FOLDER_STR],
                    },
                    "description": "A list of regex patterns to check the model name against. Each pattern is applied to the folder specified. If no pattern is found for the folder, the default pattern is used.",
                    "default": [],
                },
            },
            "required": [cls.DEFAULT_PATTERN_STR, cls.PATTERNS_LIST_STR],
        }
        config_schema["files_required"] = cls.FILES_REQUIRED
        return config_schema
