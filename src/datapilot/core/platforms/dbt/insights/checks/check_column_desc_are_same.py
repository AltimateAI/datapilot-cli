from typing import List
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckColumnDescAreSame(ChecksInsight):
    NAME = "Check Column Desc Are Same"
    ALIAS = "check_column_desc_are_same"
    DESCRIPTION = (
        "Checks that models have the same descriptions for the same column names. "
        "Consistent column descriptions improve understanding and usage of the dbt project."
    )
    REASON_TO_FLAG = (
        "Different descriptions for the same column names can lead to confusion and hinder effective data "
        "modeling and analysis. It's important to have consistent column descriptions."
    )
    FAILURE_MESSAGE = (
        "The following columns in the model `{model_unique_id}` have different descriptions:\n{columns}. "
        "Inconsistent descriptions can impede understanding and usage of the model."
    )
    RECOMMENDATION = (
        "Ensure that the descriptions for the columns listed above in the model `{model_unique_id}` are consistent. "
        "Consistent descriptions provide valuable context and aids in data understanding and collaboration."
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _build_failure_result(
        self,
        model_unique_id: str,
        columns: List[str],
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model is a root model with 0 direct parents.

        :param model_unique_id: Unique ID of the current model being evaluated.
        :param columns: List of columns that have different descriptions.
        :return: An instance of InsightResult containing failure message and recommendation.
        """
        self.logger.debug(f"Building failure result for model {model_unique_id} with columns with different descriptions {columns}")
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
            metadata={
                "columns": columns,
                "model_unique_id": model_unique_id,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying models with columns that have different descriptions for the same column name.
        :return: A list of InsightResponse objects.
        """
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                columns = self._get_columns_with_different_desc(node_id)
                if columns:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(
                                node_id,
                                columns,
                            ),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )

        return insights

    def _get_columns_with_different_desc(self, node_id) -> List[str]:
        """
        Get the list of columns that have different descriptions for the same column name.
        :param node_id: The unique ID of the node.
        :return: A list of column names.
        """
        columns_with_different_desc = []
        columns = {}
        for column_name, column_node in self.get_node(node_id).columns.items():
            if column_name in columns:
                if column_node.description != columns[column_name]:
                    columns_with_different_desc.append(column_name)
            else:
                columns[column_name] = column_node.description
        return columns_with_different_desc

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        return True, ""
