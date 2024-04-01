from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckMacroHasDesc(ChecksInsight):
    NAME = "Macro has documentation"
    ALIAS = "check_macro_has_desc"
    DESCRIPTION = "Macros should be documented."
    REASON_TO_FLAG = "Undocumented macros can cause misunderstandings and inefficiencies in data modeling and analysis, as they make it difficult to understand their purpose and usage. Clear descriptions are vital for accuracy and streamlined workflow."

    def _build_failure_result(
        self,
        node_id: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a macro doesn't have a description.

        :return: An instance of InsightResult containing failure message and recommendation.
        """

        failure_message = f"The macro `{node_id}` does not have a description."
        recommendation = "Add a description to the macro to help in understanding the purpose of the macro."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"macro_unique_id": node_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying macros that don't have descriptions.
        :return: A list of InsightResponse objects.
        """

        insights = []
        for macro_id, macro in self.macros.items():
            if self.should_skip_model(macro_id):
                self.logger.debug(f"Skipping model {macro_id} as it is not enabled for selected models")
                continue
            if macro.resource_type == AltimateResourceType.macro:
                if not macro.description:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=macro_id,
                            package_name=macro.package_name,
                            original_file_path=macro.original_file_path,
                            path=macro.original_file_path,
                            insight=self._build_failure_result(macro_id),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )

        return insights
