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


class CheckModelHasAllColumns(ChecksInsight):
    NAME = "Model has all columns as per catalog"
    ALIAS = "check_model_has_all_columns"
    DESCRIPTION = "Models should have all the columns as per the catalog."
    REASON_TO_FLAG = (
        "Missing columns in the model can lead to data integrity issues and inconsistency in analysis. "
        "It's important to ensure that the model has all the required columns as per the catalog definition."
    )
    FILES_REQUIRED: ClassVar = ["Manifest", "Catalog"]

    def __init__(self, catalog_wrapper: BaseCatalogWrapper, *args, **kwargs):
        self.catalog = catalog_wrapper
        super().__init__(*args, **kwargs)

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                missing_columns = self._check_model_columns(node_id)
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

    def _build_failure_result(self, model_unique_id: str, columns: Sequence[str]) -> DBTInsightResult:
        failure_message = (
            "The following columns in the model `{model_unique_id}` are missing:\n{columns}. "
            "Ensure that the model includes all the required columns."
        )
        recommendation = (
            "Add the missing columns listed above in the model `{model_unique_id}`. "
            "Ensuring that the model has all the required columns helps in maintaining data integrity and consistency."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message.format(
                columns=numbered_list(columns),
                model_unique_id=model_unique_id,
            ),
            recommendation=recommendation.format(model_unique_id=model_unique_id),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"columns": columns, "model_unique_id": model_unique_id},
        )

    def _check_model_columns(self, node_id) -> Tuple[int, Set[str]]:
        missing_columns = set()
        schema = self.catalog.get_schema()
        if node_id not in schema:
            return missing_columns
        catalog_columns = schema[node_id].keys()
        for col_name in self.get_node(node_id).columns.keys():
            if col_name not in catalog_columns:
                missing_columns.add(col_name)
        return missing_columns

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        if not has_catalog:
            return False, "Catalog is required for insight to run."

        return True, ""

    @classmethod
    def requires_catalog(cls) -> bool:
        return True
