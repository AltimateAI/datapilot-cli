import re
from typing import List

from datapilot.config.utils import get_insight_configuration
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelNameContract(ChecksInsight):
    NAME = "Check Model Name Contract"
    ALIAS = "check_model_name_contract"
    DESCRIPTION = (
        "Check that model name abides to a contract (similar to check-column-name-contract). A contract consists of a regex pattern."
    )
    REASON_TO_FLAG = "Model naming convention is not as expected"

    def _build_failure_result(
        self,
        node_id: str,
        pattern: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a column has a different name that doesn't match the contract.

        :return: An instance of InsightResult containing failure message and recommendation.
        """

        failure_message = f"The model:{node_id} doesn't follow the required pattern : {pattern}:\n"

        recommendation = (
            "Update the model name to adhere to the contract. "
            "Consistent model naming conventions provide valuable context and aids in data understanding and collaboration."
        )

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"pattern": pattern},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        identifying models with model name that matches a certain regex pattern.
        """
        insights = []
        self.insight_config = get_insight_configuration(self.config)
        self.pattern = self.insight_config["check_model_name_contract"]["pattern"]
        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            if node.resource_type == AltimateResourceType.model:
                if not self._check_model_name_contract(node_id):
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=node_id,
                            package_name=node.package_name,
                            path=node.original_file_path,
                            original_file_path=node.original_file_path,
                            insight=self._build_failure_result(node_id, self.pattern),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights

    def _check_model_name_contract(self, model_unique_id: str) -> bool:
        """
        Check if the model name abides to the contract.
        """
        model_name = self.get_node(model_unique_id).name
        if re.match(self.pattern, model_name, re.IGNORECASE) is None:
            return False
        return True
