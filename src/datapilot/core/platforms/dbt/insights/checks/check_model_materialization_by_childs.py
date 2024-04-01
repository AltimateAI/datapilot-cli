from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.constants import VIEW
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse


class CheckModelMaterializationByChilds(ChecksInsight):
    NAME = "Model materialization by children"
    ALIAS = "check_model_materialization_by_childs"
    DESCRIPTION = "Fewer children than threshold ideally should be view or ephemeral, more or equal should be table or incremental."
    REASON_TO_FLAG = "The model is flagged due to inappropriate materialization: models with child counts above the threshold require robust and efficient data processing, hence they should be materialized as tables or incrementals for optimized query performance and data management."
    THRESHOLD_CHILDS_STR = "threshold_childs"

    def _build_failure_result_view_materialization(
        self,
        node_id: str,
        nr_childs: int,
        threshold_childs: int,
        model_materialization: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's materialization is view and has less child models than the threshold.
        """

        failure_message = f"The model:{node_id} has {nr_childs} childs, but the materialization is {model_materialization}.\n"

        recommendation = "Consider changing the materialization to table or incremental."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"threshold_childs": threshold_childs, "nr_childs": nr_childs, "model_materialization": model_materialization},
        )

    def _build_failure_result_not_view_materialization(
        self,
        node_id: str,
        nr_childs: int,
        threshold_childs: int,
        model_materialization: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's materialization is not view and has more or equal child models than the threshold.
        """

        failure_message = f"The model:{node_id} has {nr_childs} childs, but the materialization is {model_materialization}.\n"

        recommendation = "Consider changing the materialization to view or ephemeral."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"threshold_childs": threshold_childs, "nr_childs": nr_childs, "model_materialization": model_materialization},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        Checks the model materialization by a given threshold of child models.
        All models with less child models then the treshold should be materialized as views (or ephemerals),
        all the rest as tables or incrementals.
        threshold_childs: Threshold from which onwards the materialization should be changed.
        threshold_childs will be taken from the config file.
        """
        insights = []
        threshold_childs = self.get_check_config(self.THRESHOLD_CHILDS_STR)
        if not threshold_childs:
            self.logger.info(f"Threshold childs are not provided in the configuration file for the insight {self.ALIAS}")
            return insights

        for node_id, node in self.nodes.items():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            nr_childs = len(self.children_map.get(node_id, []))
            model_materialization = node.config.materialized

            if nr_childs > threshold_childs and model_materialization == VIEW:
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=node_id,
                        package_name=node.package_name,
                        path=node.original_file_path,
                        original_file_path=node.original_file_path,
                        insight=self._build_failure_result_view_materialization(
                            node_id, nr_childs, threshold_childs, model_materialization
                        ),
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )
            elif nr_childs <= threshold_childs and model_materialization != VIEW:
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=node_id,
                        package_name=node.package_name,
                        path=node.original_file_path,
                        original_file_path=node.original_file_path,
                        insight=self._build_failure_result_not_view_materialization(
                            node_id, nr_childs, threshold_childs, model_materialization
                        ),
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )
        return insights

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.THRESHOLD_CHILDS_STR: {
                    "type": "integer",
                    "description": "Threshold from which onwards the materialization should be changed.",
                    "default": 5,
                },
            },
        }
        return config_schema
