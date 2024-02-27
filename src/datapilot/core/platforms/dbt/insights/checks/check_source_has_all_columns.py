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


class CheckSourceHasAllColumns(ChecksInsight):
    NAME = "Check Source Has All Columns"
    ALIAS = "check_source_has_all_columns"
    DESCRIPTION = "Ensures that all columns in the database are also specified in the properties file. (usually schema.yml)."
    REASON_TO_FLAG = "Missing columns in the source can lead to confusion and inconsistency in analysis. "

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
        Ensures that the source has all columns in the properties file (usually schema.yml).
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
                            severity=get_severity(self.DEFAULT_SEVERITY),
                            result=self._build_failure_result(node_id, missing_columns),
                        )
                    )
        return insights

    def _check_source_columns(self, node_id) -> Tuple[int, Set[str]]:
        """
        Check if the source has all columns
        Checking if the source has all columns as defined in the catalog.
        Ensuring that the source has all columns helps in maintaining data integrity and consistency.
        """
        missing_columns = set()
        catalog_columns = [k.lower() for k in self.catalog.get_schema()[node_id].keys()]
        for col_name in self.get_node(node_id).columns.items():
            if col_name not in catalog_columns:
                missing_columns.add(col_name)
        return missing_columns
