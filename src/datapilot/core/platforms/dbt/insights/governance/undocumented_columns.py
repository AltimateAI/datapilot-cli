from typing import ClassVar
from typing import List
from typing import Tuple

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.governance.base import DBTGovernanceInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.wrappers.catalog.wrapper import BaseCatalogWrapper
from datapilot.utils.formatting.utils import numbered_list


class DBTMissingDocumentation(DBTGovernanceInsight):
    """
    DBTDocumentationStaleColumns identifies columns that have been documented but are no longer present in the model.
    """

    NAME = "Missing Documentation"
    ALIAS = "missing_documentation"
    DESCRIPTION = (
        "Detects columns and models in the dbt project that lack documentation. Proper documentation is essential "
        "for understanding data structures and facilitating collaboration and usage of the dbt project."
    )
    REASON_TO_FLAG = (
        "Missing documentation for columns and models can lead to confusion and hinder effective data "
        "modeling and analysis. It's important to document data structures comprehensively."
    )
    FAILURE_MESSAGE = (
        "The following columns in the model `{model_unique_id}` are missing documentation:\n{columns}. "
        "Lack of documentation can impede understanding and usage of the model."
    )
    RECOMMENDATION = (
        "Enhance the documentation for the columns listed above in the model `{model_unique_id}`. "
        "Documentation provides valuable context and aids in data understanding and collaboration."
    )
    FILES_REQUIRED: ClassVar = ["Manifest", "Catalog"]

    def __init__(self, catalog_wrapper: BaseCatalogWrapper, *args, **kwargs):
        self.catalog = catalog_wrapper
        super().__init__(*args, **kwargs)

    def _build_failure_result(
        self,
        model_unique_id: str,
        model_description_is_missing: bool,
        columns: List[str],
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model is a root model with 0 direct parents.

        :param model_unique_id: Unique ID of the current model being evaluated.
        :param columns: List of columns that are documented but no longer present in the model.
        :return: An instance of InsightResult containing failure message and recommendation.
        """
        self.logger.debug(f"Building failure result for model {model_unique_id} with stale columns {columns}")
        failure_message = ""
        if model_description_is_missing:
            failure_message += f"The model {model_unique_id} is missing a description.\n"

        if columns:
            failure_message += self.FAILURE_MESSAGE.format(
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
                "model_description_is_missing": model_description_is_missing,
            },
        )

    def _get_columns_documented(self, node_id) -> List[str]:
        """
        Get the list of columns that are documented for a given node.
        :param node_id: The unique ID of the node.
        :return: A list of column names.
        """
        columns = []
        for column_name, column_node in self.get_node(node_id).columns.items():
            if column_node.description:
                columns.append(column_name.lower())
        return columns

    def _get_columns_in_model(self, node_id) -> List[str]:
        if node_id not in self.catalog.get_schema():
            return []
        return [k.lower() for k in self.catalog.get_schema()[node_id].keys()]

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying root models with 0 direct parents.
        :return: A list of InsightResponse objects.
        """
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                model_description_is_missing = not node.description
                columns_documented = self._get_columns_documented(node_id)
                db_columns = self._get_columns_in_model(node_id)
                columns_missing_documentation = list(set(db_columns) - set(columns_documented))
                if columns_missing_documentation:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(
                                node_id,
                                model_description_is_missing,
                                columns_missing_documentation,
                            ),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )

        return insights

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        """
        return False
        """
        if not has_manifest:
            return False, "manifest is required for insight to run."

        if not has_catalog:
            return False, "catalog is required for insight to run."

        return True, ""
