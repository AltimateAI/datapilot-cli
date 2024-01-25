from typing import List, Optional, Text

from datapilot.core.insights.schema import InsightResponse, InsightResult
from datapilot.core.platforms.dbt.constants import MODEL, PROJECT

# from src.utils.formatting.utils import get_severity_color, reset_color, bold, underline


class DBTInsightResult(InsightResult):
    pass


class DBTInsightResponse(InsightResponse):
    pass


class DBTModelInsightResponse(DBTInsightResponse):
    unique_id: Text
    package_name: Text
    path: Text
    original_file_path: Text
    insight_level: Text = MODEL

    # def get_report(self, do_format=True) -> str:
    #     divider = "-" * 40
    #     report_lines = [
    #         f"{bold('Package Name:', do_format)} {self.package_name}",
    #         f"{bold('Unique ID:', do_format)} {self.unique_id}",
    #         f"{bold('File Path:', do_format)} {self.original_file_path}",
    #         f"{underline('Insight Details:', do_format)}",
    #         f"  {bold('Name:', do_format)} {self.insight.name}",
    #         f"  {bold('Severity:', do_format)} {get_severity_color(self.severity)}{self.severity.value}{reset_color(do_format) }",
    #         f"  {bold('Message:', do_format)} {self.insight.message}",
    #         f"  {bold('Recommendation:', do_format)} {self.insight.recommendation}",
    #         f"  {bold('Reason to Flag:', do_format)} {self.insight.reason_to_flag}",
    #         divider,
    #     ]
    #     return "\n".join(report_lines)


class DBTProjectInsightResponse(DBTInsightResponse):
    package_name: Text
    insight_level: Text = PROJECT
    insights: List[DBTInsightResult]
    insight: Optional[DBTInsightResult] = None
    #
    # def get_report(self, do_format=True) -> str:
    #     divider = "-" * 40
    #     severity_color = get_severity_color(self.severity)
    #     report_lines = [
    #         f"Package Name: {self.package_name}",
    #         f"Insight Level: {self.insight_level}",
    #         divider,
    #     ]
    #
    #     for insight in self.insights:
    #         report_lines.extend(
    #             [
    #                 f"Insight Name: {insight.name}",
    #                 f"Type: {insight.type}",
    #                 f"Severity: {severity_color}{self.severity.value}{reset_color()}",
    #                 f"Message: {insight.message}",
    #                 f"Recommendation: {insight.recommendation}",
    #                 f"Reason to Flag: {insight.reason_to_flag}",
    #                 divider,
    #             ]
    #         )
    #
    #     return "\n".join(report_lines)
