import re
from typing import ClassVar
from typing import List
from typing import Sequence
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.wrappers.catalog.wrapper import BaseCatalogWrapper
from datapilot.utils.formatting.utils import numbered_list


class CheckColumnNameContract(ChecksInsight):
    NAME = "Column name follows contract pattern"
    ALIAS = "column_name_contract"
    DESCRIPTION = "Column names should adhere to the contract pattern defined for the data type. "
    REASON_TO_FLAG = (
        "Column names that do not adhere to the contract can lead to confusion and hinder effective data "
        "modeling and analysis. It's important to maintain consistent column naming conventions."
    )
    FAILURE_MESSAGE = (
        "The following columns in the model `{model_unique_id}` do not adhere to the contract:\n{columns}. "
        "Inconsistent column naming conventions can impede understanding and usage of the model."
    )
    RECOMMENDATION = (
        "Update the column names listed above in the model `{model_unique_id}` to adhere to the contract. "
        "Consistent column naming conventions provide valuable context and aids in data understanding and collaboration."
    )
    PATTERN_STR = "pattern"
    DATATYPE_STR = "dtype"
    PATTERNS_LIST_STR = "patterns"
    DEFAULT_PATTERN_STR = "default_pattern"
    FILES_REQUIRED: ClassVar = ["Manifest", "Catalog"]

    def __init__(self, catalog_wrapper: BaseCatalogWrapper, *args, **kwargs):
        self.catalog = catalog_wrapper
        super().__init__(*args, **kwargs)

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        self.default_pattern = self.get_check_config(self.DEFAULT_PATTERN_STR)
        datatype_configs = self.get_check_config(self.PATTERNS_LIST_STR)
        # Patterns : [{"pattern": "^[a-z_]+$", "dtype": "string"}, {"pattern": "^[a-z_]+$", "dtype": "string"}]
        if not datatype_configs:
            self.logger.debug(f"Column name contract not found in insight config for {self.ALIAS}. Skipping insight.")
            return []
        self.patterns = {
            pattern.get(self.DATATYPE_STR).lower(): pattern.get(self.PATTERN_STR)
            for pattern in datatype_configs
            if pattern.get(self.PATTERN_STR) and pattern.get(self.DATATYPE_STR)
        }
        if not self.patterns:
            self.logger.debug(f"Column name contract not found in insight config for {self.ALIAS}")
            return []

        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                columns = self._get_columns_with_contract_violation(node_id)
                if columns:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, columns),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _build_failure_result(self, model_unique_id: str, columns: Sequence[str]) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(
            columns=numbered_list(columns),
            model_unique_id=model_unique_id,
        )
        recommendation = self.RECOMMENDATION.format(model_unique_id=model_unique_id)

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"columns": columns, "model_unique_id": model_unique_id},
        )

    def _get_columns_in_model(self, node_id) -> List[str]:
        if node_id not in self.catalog.get_schema():
            return []
        return self.catalog.get_schema()[node_id].keys()

    def _get_columns_with_contract_violation(self, node_id) -> Sequence[str]:
        columns = []
        for col in self._get_columns_in_model(node_id):
            schema = self.catalog.get_schema()[node_id]
            col_name = col.lower()
            col_type = schema[col]
            if col_type.lower() in self.patterns:
                if re.match(self.patterns[col_type.lower()], col_name, re.IGNORECASE) is None:
                    columns.append(col)
            if self.default_pattern and re.match(self.default_pattern, col_name, re.IGNORECASE) is None:
                columns.append(col)
        return columns

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        if not has_catalog:
            return False, "Catalog is required for insight to run."

        return True, ""

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.DEFAULT_PATTERN_STR: {
                    "type": "string",
                    "description": "The regex pattern to check the column name against if no pattern is found for the data type",
                    "default": "^[a-z_]+$",
                },
                cls.PATTERNS_LIST_STR: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            cls.PATTERN_STR: {"type": "string", "description": "The regex pattern to check the column name against"},
                            cls.DATATYPE_STR: {
                                "type": "string",
                                "description": "The data type for which the pattern is defined",
                            },
                        },
                        "required": [cls.PATTERN_STR, cls.DATATYPE_STR],
                    },
                    "description": "A list of patterns to check the column name against for different data types",
                    "default": [],
                },
            },
            "required": [cls.DEFAULT_PATTERN_STR, cls.PATTERNS_LIST_STR],
        }
        config_schema["files_required"] = cls.FILES_REQUIRED
        return config_schema
