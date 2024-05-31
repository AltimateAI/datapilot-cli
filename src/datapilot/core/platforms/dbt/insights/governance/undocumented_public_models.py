from typing import List
from typing import Optional

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.governance.base import DBTGovernanceInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateAccess
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


# TODO: Include catalog information to make this better!
class DBTUndocumentedPublicModels(DBTGovernanceInsight):
    """
    DBTUndocumentedPublicModels identifies public models that are not documented.
    """

    NAME = "Undocumented public models"
    ALIAS = "undocumented_public_models"
    DESCRIPTION = "Identify public models that don't have documentation."
    REASON_TO_FLAG = (
        "Public models are accessible to a wide range of data consumers. To promote understanding and usability, "
        "it's essential to document these models comprehensively."
    )
    FAILURE_MESSAGE = (
        "Model `{model_unique_id}` is marked as public but is not documented. "
        "Lack of documentation can lead to confusion for data consumers."
    )
    RECOMMENDATION = (
        "For best practices, ensure that all models with public access are documented adequately. "
        "Documentation enhances data understanding and facilitates collaboration among data consumers."
    )

    def _build_failure_result(
        self,
        model_unique_id: str,
        model_description_is_missing: bool,
        columns: Optional[List[str]] = None,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model is a root model with 0 direct parents.

        :param model_unique_id: Unique ID of the current model being evaluated.
        :param model_description_is_missing: Whether the model description is missing.
        :param columns: List of columns that are missing documentation.
        :return: An instance of InsightResult containing failure message and recommendation.
        """
        self.logger.debug(f"Building failure result model {model_unique_id} is public but not documented.")

        failure = self.FAILURE_MESSAGE.format(
            model_unique_id=model_unique_id,
        )
        failure += "Missing Model documentation." if model_description_is_missing else ""

        failure += f"\n Columns missing documentation: {numbered_list(columns)}" if columns else ""

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure,
            recommendation=self.RECOMMENDATION,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": model_unique_id,
                "columns_without_documentation": columns,
                "model_description_missing": model_description_is_missing,
            },
        )

    def _get_missing_column_documentation(self, node: AltimateManifestNode) -> List[str]:
        columns = []
        for column in node.columns:
            if not column.description:
                columns.append(column.name)
        return columns

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying root models with 0 direct parents.
        """
        self.logger.debug("Generating insights for undocumented public models")
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                if node.access == AltimateAccess.public:
                    missing_model_documentation = not node.description
                    missing_columns = self._get_missing_column_documentation(node)
                    if missing_model_documentation or missing_columns:
                        insights.append(
                            DBTModelInsightResponse(
                                unique_id=node_id,
                                package_name=node.package_name,
                                path=node.original_file_path,
                                original_file_path=node.original_file_path,
                                insight=self._build_failure_result(
                                    node_id,
                                    not missing_model_documentation,
                                    missing_columns,
                                ),
                                severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                            )
                        )

        return insights
