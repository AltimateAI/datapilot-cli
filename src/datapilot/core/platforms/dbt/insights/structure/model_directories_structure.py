from typing import List
from typing import Optional

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import OTHER
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.insights.structure.base import DBTStructureInsight
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.utils import _check_model_folder_convention
from datapilot.core.platforms.dbt.utils import classify_model_type


class DBTModelDirectoryStructure(DBTStructureInsight):
    """
    DBTModelDirectoryStructure checks if models are placed in the correct directories.
    """

    NAME = "Bad model directory structure"
    ALIAS = "model_directory_structure"
    DESCRIPTION = "This rule identifies models that are not placed in their correct directories. "
    REASON_TO_FLAG = (
        "Placing models in the correct directories is vital for maintaining a structured and "
        "efficient data warehouse. Incorrectly placed models can lead to confusion, hinder "
        "discoverability, and complicate maintenance and scaling of the dbt project."
    )
    FAILURE_MESSAGE = (
        "Incorrect Directory Placement Detected: The model `{model_unique_id}` is incorrectly "
        "placed in the current directory. As a `{model_type}` model, it should be located in "
        "the `{convention}` directory."
    )
    RECOMMENDATION = (
        "To resolve this issue, please move the model `{model_unique_id}` to the `{convention}` "
        "directory. This change will align the model's location with the established directory "
        "structure, improving organization and ease of access in your dbt project."
    )

    def _build_failure_result(self, model_unique_id: str, model_type: str, convention: Optional[str]) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(
            model_unique_id=model_unique_id,
            model_type=model_type,
            convention=convention,
        )
        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            message=failure_message,
            recommendation=self.RECOMMENDATION.format(model_unique_id=model_unique_id, convention=convention),
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
                    continue

                valid_convention, message = _check_model_folder_convention(
                    model_type,
                    node.original_file_path,
                    regex_configuration,
                    node=node,
                    sources=self.sources,
                )
                if not valid_convention:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node.unique_id,
                            package_name=node.package_name,
                            path=node.path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(
                                model_unique_id=node.unique_id,
                                model_type=model_type,
                                convention=message,
                            ),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights
