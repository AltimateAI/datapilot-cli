import re
from typing import List
from typing import Sequence
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckColumnNameContract(ChecksInsight):
    NAME = "Check Column Name Contract"
    ALIAS = "check_column_name_contract"
    DESCRIPTION = (
        "Checks that column names abide by a contract, as described in a blog post by Emily Riederer. "
        "A contract consists of a regex pattern and a series of data types. "
        "Ensuring consistent column naming conventions improves data understanding and usage of the dbt project."
    )
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern = kwargs.get("pattern")
        self.dtypes = kwargs.get("dtypes")

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                columns = self._get_columns(node_id)
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

    def _get_columns(self, node_id) -> Sequence[str]:
        columns_with_contract_violation = []
        for col_name, col_node in self.get_node(node_id).columns.items():
            if re.match(self.pattern, col_name, re.IGNORECASE) is None or col_node.description not in self.dtypes:
                columns_with_contract_violation.append(col_name)
        return columns_with_contract_violation

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        if not has_catalog:
            return False, "Catalog is required for insight to run."

        return True, ""