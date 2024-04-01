from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import SOURCE
from datapilot.core.platforms.dbt.insights.performance.base import DBTPerformanceInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class DBTExposureParentMaterialization(DBTPerformanceInsight):
    """
    Checks if the dbt model has hard coded references to other models.
    """

    NAME = "Exposure parent materialization check"
    ALIAS = "exposure_parent_bad_materialization"
    DESCRIPTION = "Exposures should depend on transformed data models or metrics, not raw untransformed sources. "
    REASON_TO_FLAG = (
        "Exposures should depend on transformed data models or metrics, not raw untransformed sources. "
        "Moreover, parent models of exposures, being heavily used in downstream systems, "
        "should be materialized efficiently to ensure performance when queried."
    )
    FAILURE_MESSAGE = (
        "Exposure `{exposure_unique_id}` has parent models with suboptimal materialization types. "
        "This could impact performance and clarity in downstream systems."
    )
    RECOMMENDATION = (
        "Review the parent models of exposure `{exposure_unique_id}`. If using sources, "
        "consider transforming the raw data into a model first. If parent models are views or ephemerals,"
        " evaluate materializing them as tables to enhance query performance."
    )

    def _build_failure_result(
        self,
        exposure_unique_id: str,
        source_parents: List[str],
        bad_materializations: List[str],
    ) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(
            exposure_unique_id=exposure_unique_id,
        )

        failure_message += f" It has some source models as it's parents:\n {numbered_list(source_parents)}" if source_parents else ""

        failure_message += (
            f" The following parent models are not materialized as table " f"or incremental :\n {numbered_list(bad_materializations)}"
            if bad_materializations
            else ""
        )

        recommendation = self.RECOMMENDATION.format(
            exposure_unique_id=exposure_unique_id,
        )

        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "exposure_unique_id": exposure_unique_id,
                "source_parents": source_parents,
                "bad_materialization_parents": bad_materializations,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []

        for exposure_id, exposure in self.exposures.items():
            if self.should_skip_model(exposure_id):
                self.logger.debug(f"Skipping model {exposure_id} as it is not enabled for selected models")
                continue
            bad_materializations = []
            source_parents = []
            for parent_model in exposure.depends_on.nodes:
                if parent_model.split(".")[0] == SOURCE:
                    source_parents.append(parent_model)
                else:
                    node = self.nodes.get(parent_model)
                    materialization = node.config.materialized if node.config else "not defined"
                    if node and node.resource_type == AltimateResourceType.model and materialization not in ["table", "incremental"]:
                        bad_materializations.append(parent_model)

            if source_parents or bad_materializations:
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=exposure_id,
                        package_name=exposure.package_name,
                        path=exposure.path,
                        original_file_path=exposure.original_file_path,
                        insight=self._build_failure_result(
                            exposure_unique_id=exposure.unique_id,
                            source_parents=source_parents,
                            bad_materializations=bad_materializations,
                        ),
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )

        return insights
