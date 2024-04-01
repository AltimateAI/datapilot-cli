from typing import List
from typing import Optional

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import OTHER
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.insights.structure.base import DBTStructureInsight
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.utils import _check_model_naming_convention
from datapilot.core.platforms.dbt.utils import classify_model_type


class DBTModelNamingConvention(DBTStructureInsight):
    """
    DBTModelNamingConvention identifies models that do not follow the naming convention.
    """

    NAME = "Bad model naming convention"
    ALIAS = "model_naming_convention_check"
    DESCRIPTION = "This rule identifies models that do not follow the naming convention."
    REASON_TO_FLAG = (
        "Inconsistent or unclear naming conventions can lead to confusion and errors in querying the data warehouse. "
        "A well-defined naming convention clarifies the model type and purpose, promoting better understanding "
        "and effective data management. This rule flags models that deviate from established naming standards."
    )
    FAILURE_MESSAGE = (
        "Naming Convention Violation Detected: The model `{model_unique_id}` does not comply with the "
        "established naming convention. It is identified as a `{model_type}` model, but its name does not "
        "reflect the required prefix or convention `{convention}`. Please update the model name to align "
        "with the naming standards."
    )
    RECOMMENDATION = "Please rename the model `{model_unique_id}` to follow the appropriate naming convention. "

    def _build_failure_result(self, model_unique_id: str, model_type: str, convention: Optional[str]) -> DBTInsightResult:
        if model_type != OTHER:
            failure_message = self.FAILURE_MESSAGE.format(
                model_unique_id=model_unique_id,
                model_type=model_type,
                convention=convention,
            )
        else:
            failure_message = (
                f"The model `{model_unique_id}` was not classified as any of the known model types. "
                "The naming conventions for it may not be appropriate"
            )

        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            message=failure_message,
            recommendation=self.RECOMMENDATION.format(model_unique_id=model_unique_id),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": model_unique_id,
                "model_type": model_type,
                "convention": convention,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        regex_configuration = get_regex_configuration(self.config)
        for node in self.nodes.values():
            if self.should_skip_model(node.unique_id):
                self.logger.debug(f"Skipping model {node.unique_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                model_type = classify_model_type(node.name, node.original_file_path, regex_configuration)
                if model_type == OTHER:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node.unique_id,
                            package_name=node.package_name,
                            path=node.path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node.unique_id, model_type, None),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
                    continue
                valid_name, expected_model_type = _check_model_naming_convention(node.name, model_type, regex_configuration.get(MODEL))
                if not valid_name:
                    insight_result = self._build_failure_result(node.unique_id, model_type, expected_model_type)
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node.unique_id,
                            package_name=node.package_name,
                            path=node.path,
                            original_file_path=node.original_file_path,
                            insight=insight_result,
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights
