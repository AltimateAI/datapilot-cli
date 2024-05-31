from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.checks.base import ChecksInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType


class CheckModelParentsDatabase(ChecksInsight):
    NAME = "Check model parents database"
    ALIAS = "check_model_parents_database"
    DESCRIPTION = "Ensures the parent models or sources are from certain database."
    REASON_TO_FLAG = "The model has a different database as parent model or source."
    WHITELIST_STR = "whitelist"
    BLACKLIST_STR = "blacklist"

    def _build_failure_result(
        self,
        node_id: str,
        parent_database: str,
    ) -> DBTInsightResult:
        """
        Build failure result for the insight if a model's parent database is not whitelist or in blacklist.
        """

        failure_message = f"The model:{node_id}'s parent model's database is not in whitelist or blacklisted:\n"

        recommendation = "Update the parent model's database to adhere to the whitelist or remove the model from the blacklist."

        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"parent_database": parent_database},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generate a list of InsightResponse objects for each model in the DBT project,
        ensures the parent models or sources are from certain database.
        The whitelist and blacklist of databases are defined in the config file.
        """
        insights = []

        self.whitelist = self.get_check_config(self.WHITELIST_STR)
        self.blacklist = self.get_check_config(self.BLACKLIST_STR) or []

        for node_id in self.nodes.keys():
            if self.should_skip_model(node_id):
                self.logger.debug(f"Skipping model {node_id} as it is not enabled for selected models")
                continue
            parent_database = self._check_model_parents_database(node_id)
            if parent_database:
                insights.append(
                    DBTModelInsightResponse(
                        unique_id=node_id,
                        package_name=self.nodes[node_id].package_name,
                        path=self.nodes[node_id].original_file_path,
                        original_file_path=self.nodes[node_id].original_file_path,
                        insight=self._build_failure_result(node_id, parent_database),
                        severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                    )
                )
        return insights

    def _check_model_parents_database(self, model_unique_id: str) -> bool:
        """
        Check if the parent models or sources are from certain database.
        """
        model = self.get_node(model_unique_id)
        if model.resource_type == AltimateResourceType.model:
            for parent in getattr(model.depends_on, "nodes", []):
                parent_model = self.get_node(parent)
                if not parent_model:
                    continue

                if parent_model.resource_type not in [AltimateResourceType.model, AltimateResourceType.source]:
                    continue

                if self.whitelist and (parent_model.database not in self.whitelist):
                    return parent_model.database

                if self.blacklist and (parent_model.database in self.blacklist):
                    return parent_model.database
        return None

    @classmethod
    def get_config_schema(cls):
        config_schema = super().get_config_schema()
        config_schema["config"] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                cls.WHITELIST_STR: {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of databases that are allowed as parent models or sources.",
                },
                cls.BLACKLIST_STR: {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of databases that are not allowed as parent models or sources.",
                },
            },
        }
        return config_schema
