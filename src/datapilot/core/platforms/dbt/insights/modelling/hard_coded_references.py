from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import SQL
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.utils import get_hard_coded_references
from datapilot.utils.formatting.utils import numbered_list


class DBTHardCodedReferences(DBTModellingInsight):
    """
    Checks if the dbt model has hard coded references to other models.
    """

    NAME = "Hard coded references"
    ALIAS = "hard_coded_references"
    DESCRIPTION = "Models should not have hard-coded references to tables"
    REASON_TO_FLAG = (
        "Hard-coded references in SQL prevent easy identification and tracking of data lineage, "
        "and can lead to issues in maintainability and scalability of the data models."
    )
    SOURCE_FANOUT_THRESHOLD = 1  # Default threshold, can be overridden as needed
    FAILURE_MESSAGE = (
        "Model `{model_unique_id}` contains hard-coded references, which may obscure data lineage. "
        "Detected hard-coded references: \n{hard_coded_references}"
    )
    RECOMMENDATION = (
        "Replace hard-coded references in `{model_unique_id}` with dbt sources or model references to "
        "improve clarity and maintainability of data lineage."
    )

    def _build_failure_result(self, model_unique_id: str, hard_coded_references: List[str]) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(
            model_unique_id=model_unique_id,
            hard_coded_references=numbered_list(hard_coded_references),
        )
        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            message=failure_message,
            recommendation=self.RECOMMENDATION.format(model_unique_id=model_unique_id),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": model_unique_id,
                "hard_coded_references": hard_coded_references,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []

        for node in self.nodes.values():
            if self.should_skip_model(node.unique_id):
                self.logger.debug(f"Skipping model {node.unique_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                raw_code = node.raw_code
                if (not raw_code) or node.language != SQL:
                    continue
                hard_coded_references = get_hard_coded_references(raw_code)
                if hard_coded_references:
                    insight_result = self._build_failure_result(
                        model_unique_id=node.unique_id,
                        hard_coded_references=hard_coded_references,
                    )
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
