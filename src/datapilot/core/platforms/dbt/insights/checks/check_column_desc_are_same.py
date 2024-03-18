from typing import ClassVar
from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckColumnDescAreSame(ChecksInsight):
    NAME = "Column descriptions consistent for same column names"
    ALIAS = "column_descriptions_are_same"
    DESCRIPTION = "Column description for the same column name should be same "
    REASON_TO_FLAG = (
        "Different descriptions for the same column names can lead to confusion and hinder effective data "
        "modeling and analysis. It's important to have consistent column descriptions."
    )
    FILES_REQUIRED: ClassVar = ["Manifest", "Catalog"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_with_different_desc = []
        self.columns = {}
        self.column_node_map = {}

    def _build_failure_result(
        self,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a column has a different description in multiple models or sources.

        :return: An instance of InsightResult containing failure message and recommendation.
        """

        failure_message = "The following models or sources have different descriptions for some columns:\n"
        for col_name in self.columns_with_different_desc:
            failure_message += f"- {self.column_node_map[col_name]} (column: {col_name})\n"

        recommendation = "Ensure that the description for the columns is consistent across all instances."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "columns_with_diff_desc": self.columns_with_different_desc,
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
                self._get_columns_with_different_desc(node_id)

        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            elif node.resource_type == AltimateResourceType.source:
                self._get_columns_with_different_desc(node_id)

        if self.columns_with_different_desc:
            insights.append(
                DBTModelInsightResponse(
                    unique_id=node_id,
                    package_name=node.package_name,
                    path=node.original_file_path,
                    original_file_path=node.original_file_path,
                    insight=self._build_failure_result(),
                    severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                )
            )

        return insights

    def _get_columns_with_different_desc(self, node_id):
        """
        Get the list of models or sources that have different descriptions for the same column name.
        :param node_id: The unique ID of the node.
        """
        for column_name, column_node in self.get_node(node_id).columns.items():
            if column_name in self.column_node_map:
                self.column_node_map[column_name].append(node_id)
            else:
                self.column_node_map[column_name] = [node_id]

            if column_name in self.columns:
                if column_node.description != self.columns[column_name]:
                    if column_name not in self.columns_with_different_desc:
                        self.columns_with_different_desc.append(column_name)
            else:
                self.columns[column_name] = column_node.description
