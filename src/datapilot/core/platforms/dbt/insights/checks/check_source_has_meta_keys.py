from typing import List
from typing import Set
from typing import Tuple

from datapilot.config.utils import get_source_meta_keys_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.utils.formatting.utils import numbered_list


class CheckSourceHasMetaKeys(ChecksInsight):
    NAME = "Check Source Has Meta Keys"
    ALIAS = "check_source_has_meta_keys"
    DESCRIPTION = "Check if the source has meta keys"
    REASON_TO_FLAG = "The source table is missing a description. Ensure that the source table has a description."

    def _build_failure_result(
        self,
        source_id: int,
        diff: Set[str],
    ) -> DBTInsightResult:
        failure_message = (
            "The source table `{source_id}` is missing the following meta keys: {diff}. "
            "Ensure that the source table has the required meta keys."
        )
        recommendation = (
            "Add the following meta keys to the source table `{source_id}`: {diff}. "
            "Ensuring that the source table has the required meta keys helps in maintaining data integrity and consistency."
        )
        return DBTInsightResult(
            failure_message=failure_message.format(source_id=source_id, diff=numbered_list(diff)),
            recommendation=recommendation.format(source_id=source_id, diff=numbered_list(diff)),
            metadata={"source_id": source_id, "diff": diff},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate the insight response for the check. This method is called by the insight runner to generate the insight
        response for the check.
        Ensures that the source has a list of valid meta keys.
        meta_keys are provided in the configuration file.
        """
        meta_keys = get_source_meta_keys_configuration(self.config)
        insights = []
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                diff = self._check_source_has_meta_keys(node_id, meta_keys)
                if diff:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            severity=get_severity(self.DEFAULT_SEVERITY),
                            result=self._build_failure_result(node_id, diff),
                        )
                    )
        return insights

    def _check_source_has_meta_keys(self, model_unique_id: str, meta_keys: List[str]):
        source = self.get_node(model_unique_id)
        source_meta = set(source.meta.keys())
        diff = set(meta_keys).difference(source_meta)
        return diff

    @classmethod
    def has_all_required_data(cls, has_manifest: bool, has_catalog: bool, **kwargs) -> Tuple[bool, str]:
        if not has_manifest:
            return False, "Manifest is required for insight to run."

        return True, ""
