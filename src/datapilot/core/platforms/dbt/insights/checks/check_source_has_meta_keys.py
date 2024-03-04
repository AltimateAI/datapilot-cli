from typing import List
from typing import Set

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
        """
        Build failure result for the insight if a model's parent schema is not whitelist or in blacklist.
        """
        failure_message = f"The source:{source_id} does not have the following meta keys defined: {numbered_list(diff)}\n"

        recommendation = "Define the meta keys for the source to ensure consistency in analysis."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"source_id": source_id},
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
        for node_id, node in self.sources.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping source {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.source:
                diff = self._check_source_has_meta_keys(node_id, meta_keys)
                if diff:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            original_file_path=node.original_file_path,
                            path=node.original_file_path,
                            insight=self._build_failure_result(node_id, diff),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_source_has_meta_keys(self, source_unique_id: str, meta_keys: List[str]):
        source = self.get_node(source_unique_id)
        if len(source.meta) > 0:
            source_meta = set(source.meta.keys())
            diff = set(meta_keys).difference(source_meta)
            return diff
        return set(meta_keys)
