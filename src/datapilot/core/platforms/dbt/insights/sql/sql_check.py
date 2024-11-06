import inspect
from typing import List

from sqlglot import parse_one
from sqlglot.optimizer.eliminate_ctes import eliminate_ctes
from sqlglot.optimizer.eliminate_joins import eliminate_joins
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.normalize import normalize
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.unnest_subqueries import unnest_subqueries

from datapilot.core.insights.sql.base.insight import SqlInsight
from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse

RULES = (
    pushdown_projections,
    normalize,
    unnest_subqueries,
    eliminate_subqueries,
    eliminate_joins,
    eliminate_ctes,
)


class SqlCheck(SqlInsight):
    """
    This class identifies DBT models with SQL optimization issues.
    """

    NAME = "sql optimization issues"
    ALIAS = "check_sql_optimization"
    DESCRIPTION = "Checks if the model has SQL optimization issues. "
    REASON_TO_FLAG = "The query can be optimized."
    FAILURE_MESSAGE = "The query for model `{model_unique_id}` has optimization opportunities:\n{rule_name}. "
    RECOMMENDATION = "Please adapt the query of the model `{model_unique_id}` as in following example:\n{optimized_sql}"

    def _build_failure_result(self, model_unique_id: str, rule_name: str, optimized_sql: str) -> DBTInsightResult:
        """
        Constructs a failure result for a given model with sql optimization issues.
        :param model_unique_id: The unique id of the dbt model.
        :param rule_name: The rule that generated this failure result.
        :param optimized_sql: The optimized sql.
        :return: An instance of DBTInsightResult containing failure details.
        """
        failure_message = self.FAILURE_MESSAGE.format(model_unique_id=model_unique_id, rule_name=rule_name)
        recommendation = self.RECOMMENDATION.format(model_unique_id=model_unique_id, optimized_sql=optimized_sql)
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={"model_unique_id": model_unique_id, "rule_name": rule_name},
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        """
        Generates insights for each DBT model in the project, focusing on sql optimization issues.

        :return: A list of DBTModelInsightResponse objects with insights for each model.
        """
        self.logger.debug("Generating sql insights for DBT models")
        insights = []

        possible_kwargs = {
            "db": None,
            "catalog": None,
            "dialect": self.adapter_type,
            "isolate_tables": True,  # needed for other optimizations to perform well
            "quote_identifiers": False,
            **kwargs,
        }
        for node_id, node in self.nodes.items():
            try:
                compiled_query = node.compiled_code
                if compiled_query:
                    parsed_query = parse_one(compiled_query, dialect=self.adapter_type)
                    qualified = qualify(parsed_query, **possible_kwargs)
                    changed = qualified.copy()
                    for rule in RULES:
                        original = changed.copy()
                        rule_params = inspect.getfullargspec(rule).args
                        rule_kwargs = {param: possible_kwargs[param] for param in rule_params if param in possible_kwargs}
                        changed = rule(changed, **rule_kwargs)
                        if changed.sql() != original.sql():
                            insights.append(
                                DBTModelInsightResponse(
                                    unique_id=node_id,
                                    package_name=node.package_name,
                                    path=node.original_file_path,
                                    original_file_path=node.original_file_path,
                                    insight=self._build_failure_result(node_id, rule.__name__, changed.sql()),
                                    severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                                )
                            )
            except Exception as e:
                self.logger.error(e)
        return insights
