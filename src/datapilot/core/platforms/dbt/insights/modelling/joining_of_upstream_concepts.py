from typing import List

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.modelling.base import DBTModellingInsight
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse


class DBTRejoiningOfUpstreamConcepts(DBTModellingInsight):
    """
    DBTRejoiningOfUpstreamConcepts identifies cases in the dbt project where a parent model's direct child
    is also the direct child of another one of the parent's direct children, with the condition that the intermediate
    model has no other downstream dependencies.
    """

    NAME = "Rejoining of upstream Concepts"
    ALIAS = "rejoining_upstream_concepts"
    DESCRIPTION = (
        "Detects scenarios where a parent's direct child is also a direct child of another one " "of the parent's direct children."
    )
    REASON_TO_FLAG = (
        "Flagged to identify cases where a parent model has a direct child that is also a direct child "
        "of another one of the parent's direct children. Such patterns can suggest loops or redundancies in the DAG."
    )
    FAILURE_MESSAGE = (
        "Model `{child}` has a rejoining upstream concept with parent model `{parent_model}` "
        "and downstream child: `{downstream_child}`. This may indicate a loop or redundancy in the DAG."
    )
    RECOMMENDATION = (
        "Review and potentially refactor the model relationships in `{child}`,"
        " `{parent_model}`, and `{downstream_child}` to simplify the DAG and "
        "avoid unnecessary complexity or potential loops."
    )

    def _build_failure_result(self, child: str, parent_model: str, children_list: List[str]) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(child=child, parent_model=parent_model, downstream_child=children_list[0])

        recommendation = self.RECOMMENDATION.format(child=child, parent_model=parent_model, downstream_child=children_list[0])
        return DBTInsightResult(
            type=self.TYPE,
            name=self.NAME,
            message=failure_message,
            recommendation=recommendation,
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": parent_model,
                "children": children_list,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for parent_model, children in self.children_map.items():
            for child in children:
                child_child_is_also_parent_child = any(
                    dwn_stream_child in self.children_map[child] for dwn_stream_child in self.children_map[parent_model]
                )
                if child_child_is_also_parent_child and len(self.children_map[child]) == 1:
                    insight_result = self._build_failure_result(
                        child=child,
                        parent_model=parent_model,
                        children_list=list(self.children_map[child]),
                    )
                    child_node = self.get_node(child)
                    if self.should_skip_model(child_node.unique_id):
                        self.logger.debug(f"Skipping model {child_node.unique_id} as it is not enabled for selected models")
                        continue
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=child_node.unique_id,
                            package_name=child_node.package_name,
                            path=child_node.path,
                            original_file_path=child_node.original_file_path,
                            insight=insight_result,
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )

        return insights
