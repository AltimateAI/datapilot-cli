from typing import List
from typing import Optional

from datapilot.config.utils import get_regex_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.insights.structure.base import DBTStructureInsight
from datapilot.core.platforms.dbt.utils import _check_source_folder_convention


class DBTSourceDirectoryStructure(DBTStructureInsight):
    """
    DBTSourcesDirectoryStructure checks if sources are placed in the correct directories.
    """

    NAME = "Bad source directory structure"
    ALIAS = "source_directory_structure"
    DESCRIPTION = "This rule identifies sources that are not placed in their correct directories. "
    REASON_TO_FLAG = (
        "Sources need to be organized in the correct directories to ensure an efficient and "
        "maintainable data architecture. Proper directory structure facilitates easy navigation, "
        "improves readability, and aids in managing the data sources effectively."
    )
    FAILURE_MESSAGE = (
        "Inappropriate Directory Placement Detected: The source file for {source_id} is currently "
        "placed in an incorrect directory. This can lead to organizational issues and hinder "
        "efficient source management."
    )
    RECOMMENDATION = (
        "To address this issue, please move the source file for {source_id} to the appropriate "
        "directory. The recommended directory structure is {convention}, which aligns with best "
        "practices for organizing source files in dbt projects."
    )

    def _build_failure_result(self, model_unique_id: str, convention: Optional[str]) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(
            source_id=model_unique_id,
        )
        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            message=failure_message,
            recommendation=self.RECOMMENDATION.format(source_id=model_unique_id, convention=convention),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "source_id": model_unique_id,
                "convention": convention,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        regex_configuration = get_regex_configuration(self.config)
        for source_id, source in self.sources.items():
            if self.should_skip_model(source_id):
                self.logger.debug(f"Skipping model {source_id} as it is not enabled for selected models")
                continue
            valid_convention, expected_directory = _check_source_folder_convention(
                source_name=source.source_name,
                folder_path=source.original_file_path,
                patterns=regex_configuration,
            )
            if not valid_convention:
                insight = self._build_failure_result(
                    model_unique_id=source_id,
                    convention=expected_directory,
                )
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=source.unique_id,
                        package_name=source.package_name,
                        path=source.path,
                        original_file_path=source.original_file_path,
                        insight=insight,
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )

        return insights
