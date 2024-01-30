from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.performance.base import DBTPerformanceInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTProjectInsightResponse
from datapilot.schemas.constants import CONFIG_METRICS
from datapilot.utils.formatting.utils import numbered_list


class DBTChainViewLinking(DBTPerformanceInsight):
    """
    Checks if the dbt model has a chain of views in it
    """

    CHAIN_LENGTH_STR = "chain_length"
    NAME = "Chain View Linking"
    ALIAS = "chain_view_linking"
    CHAIN_LENGTH = 4  # Default chain length, can be adjusted as needed
    DESCRIPTION = (
        "Analyzes the dbt project to identify long chains of non-physically-materialized models (views and ephemerals)."
        " Such long chains can result in increased runtime for models built on top of them due to extended computation"
        " and memory usage."
    )
    REASON_TO_FLAG = (
        "Long runtime can occur for a model when it is built on top of a long chain of 'non-physically-materialized'"
        " models. Identifying these chains is crucial to optimize performance and reduce computation overhead."
    )
    FAILURE_MESSAGE = (
        "Detected {number_of_chains} chains of views/ephemeral models in your dbt project that are at least {"
        "chain_length} models long. Chains of concern: \n{chain_views}"
    )
    RECOMMENDATION = (
        "Consider altering the materialization strategy of some key upstream models to 'table' or 'incremental'. "
        "This change can reduce computation time, minimize in-memory data processing, and "
        "prevent excessive nesting of views."
    )

    def _build_failure_result(
        self,
        chain_views: List[List[str]],
        chain_length: int = CHAIN_LENGTH,
    ) -> DBTInsightResult:
        chains = [" -> ".join(chain_view[::-1]) for chain_view in chain_views]
        failure_message = self.FAILURE_MESSAGE.format(
            number_of_chains=len(chains),
            chain_length=chain_length,
            chain_views=numbered_list(chains),
        )

        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            message=failure_message,
            recommendation=self.RECOMMENDATION,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "chains": chains,
            },
        )

    def _get_chain_length(self):
        metrics_config = self.config.get(CONFIG_METRICS, {})
        metric_config = metrics_config.get(self.ALIAS, {})
        # Return the configured fanout threshold or the default if not specified
        return metric_config.get(self.CHAIN_LENGTH_STR, self.CHAIN_LENGTH)

    def generate(self, *args, **kwargs) -> List[DBTProjectInsightResponse]:
        chain_length = self._get_chain_length()
        chain_views = self.find_long_chains(chain_length)

        if chain_views:
            insight_result = self._build_failure_result(chain_views)

            return [
                DBTProjectInsightResponse(
                    package_name=self.project_name,
                    insights=[insight_result],
                    severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                )
            ]
        return []
