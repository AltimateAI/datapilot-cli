from typing import Dict
from typing import List

from datapilot.core.insights.schema import InsightResult
from datapilot.core.insights.schema import Severity
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.insights.schema import DBTProjectInsightResponse
from datapilot.utils.formatting.utils import color_based_on_severity


def gen_table(insight: InsightResult, severity: Severity) -> Dict[str, str]:
    return {
        "name": insight.name,
        "type": insight.type,
        "level": color_based_on_severity(severity),
        "message": insight.message,
        "recommendation": insight.recommendation,
        "reason_to_flag": insight.reason_to_flag,
    }


def generate_model_insights_table(model_insights: Dict[str, List[DBTModelInsightResponse]]):
    results = {}

    for model_id, insights in model_insights.items():
        for insight in insights:
            if model_id not in results:
                results[model_id] = {
                    "package_name": insight.package_name,
                    "unique_id": insight.unique_id,
                    "path": insight.original_file_path,
                    "table": [],
                }

            results[model_id]["table"].append(gen_table(insight.insight, insight.severity))
    return results


def generate_project_insights_table(project_insights: List[DBTProjectInsightResponse]):
    results = []

    for project_insight in project_insights:
        for insight in project_insight.insights:
            results.append(gen_table(insight, project_insight.severity))
    return results
