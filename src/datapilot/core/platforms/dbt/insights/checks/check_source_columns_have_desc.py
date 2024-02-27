from typing import List
from typing import Sequence
from typing import Set
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.wrappers.catalog.wrapper import BaseCatalogWrapper
from datapilot.utils.formatting.utils import numbered_list


class CheckSourceColumnsHaveDescriptions(ChecksInsight):
    NAME = "Check Source Columns Have Descriptions"
    ALIAS = "check_source_columns_have_desc"
    DESCRIPTION = "Ensures that the source has columns with descriptions in the properties file (usually schema.yml)."
    REASON_TO_FLAG = "Missing descriptions for columns in the source can lead to confusion and inconsistency in analysis. "

    def __init__(self, catalog_wrapper: BaseCatalogWrapper, *args, **kwargs):
        self.catalog = catalog_wrapper
        super().__init__(*args, **kwargs)

    def _build_failure_result(self, model_unique_id: str, columns: Sequence[str]) -> DBTInsightResult:
        failure_message = (
            "The following columns in the source `{model_unique_id}` are missing descriptions:\n{columns}. "
            "Ensure that the source includes descriptions for all the columns."
        )
        recommendation = (
            "Add the missing descriptions for the columns listed above in the source `{model_unique_id}`. "
            "Ensuring that the source has descriptions for all the columns helps in maintaining data integrity and consistency."
        )
        return DBTInsightResult(
            failure_message=failure_message.format(model_unique_id=model_unique_id, columns=numbered_list(columns)),
            recommendation=recommendation.format(model_unique_id=model_unique_id),
            metadata={"missing_columns": columns, "model_unique_id": model_unique_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate the insight response for the check. This method is called by the insight runner to generate the insight
        response for the check.
        Ensures that the source has columns with descriptions in the properties file (usually schema.yml).


        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        Returns:
            List[DBTModelInsightResponse]: List of insight responses for the check.

        """
        insights = []
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                missing_columns = self._check_source_columns(node_id)
                if missing_columns:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, missing_columns),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_source_columns(self, node_id) -> Tuple[int, Set[str]]:
        columns_with_missing_descriptions = set()
        for column_name, column_node in self.get_node(node_id).columns.items():
            if not column_node.description:
                columns_with_missing_descriptions.add(column_name)
        return columns_with_missing_descriptions
