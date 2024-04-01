from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse


class DBTUnusedSources(DBTModellingInsight):
    """
    DBTUnusedSources identifies sources in a dbt project that are not being referenced by any models.
    """

    NAME = "Unused sources detection"
    ALIAS = "unused_sources"
    DESCRIPTION = "Detects sources in the dbt project that are not being referenced by any models."
    REASON_TO_FLAG = (
        "Unused sources, either defined in YML but not used in any model or leftover from deprecated models, "
        "represent unnecessary complexity in the project. It's important to keep the dbt project lean and relevant."
    )
    FAILURE_MESSAGE = "Source `{source_unique_id}` is not being referenced by any model, indicating it is unused."
    RECOMMENDATION = (
        "Review the source `{source_unique_id}`. Consider removing it or integrating it into the project "
        "if it's needed. Keeping only relevant sources in the project reduces complexity and improves maintainability."
    )

    def _build_failure_result(self, source_unique_id: str) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(source_unique_id=source_unique_id)
        recommendation = self.RECOMMENDATION.format(source_unique_id=source_unique_id)

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source": source_unique_id},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for source_id, source in self.sources.items():
            if self.should_skip_model(source_id):
                self.logger.debug(f"Skipping model {source_id} as it is not enabled for selected models")
                continue
            if source_id not in self.children_map.keys():
                insight_result = self._build_failure_result(source_id)
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=source_id,
                        package_name=source.package_name,
                        path=source.path,
                        original_file_path=source.original_file_path,
                        insight=insight_result,
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )

        return insights
