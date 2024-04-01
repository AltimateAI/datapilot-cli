from typing import ClassVar
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
    NAME = "Source has all columns"
    ALIAS = "check_source_has_all_columns"
    DESCRIPTION = "Ensures that all columns in the database are also specified in the properties file. (usually schema.yml)."
    REASON_TO_FLAG = "Missing columns in the source can lead to confusion and inconsistency in analysis. "
    FILES_REQUIRED: ClassVar = ["Manifest", "Catalog"]

    def __init__(self, catalog_wrapper: BaseCatalogWrapper, *args, **kwargs):
        self.catalog = catalog_wrapper
        super().__init__(*args, **kwargs)

    def _build_failure_result(self, source_unique_id: str, columns: Sequence[str]) -> DBTInsightResult:
        """
        Build failure result for the insight if a source has missing columns.
        """
        failure_message = f"The source:{source_unique_id} has missing columns:\n"
        failure_message += numbered_list(columns)

        recommendation = "Update the source to include all columns."
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_unique_id": source_unique_id, "columns": columns},
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
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, list(missing_columns)),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
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
        schema = self.catalog.get_schema()
        if node_id not in schema:
            return missing_columns
        catalog_columns = schema[node_id].keys()
        for col_name in self.get_node(node_id).columns.items():
            if col_name not in catalog_columns:
                missing_columns.add(col_name)
        return missing_columns

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        """
        Check if all required data is available for the insight to run.
        :param has_manifest: A boolean indicating if manifest is available.
        :return: A boolean indicating if all required data is available.
        """
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        if not has_catalog:
            return False, "Catalog is required for insight to run."

        return True, ""

    @classmethod
    def requires_catalog(cls) -> bool:
        return True
