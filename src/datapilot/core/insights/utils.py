from typing import Dict
from typing import Optional

from datapilot.core.insights.schema import Severity
from datapilot.schemas.constants import CONFIG_METRICS
from datapilot.schemas.constants import CONFIG_SEVERITY


def get_severity(
    config: Optional[Dict],
    alias: str,
    default_severity: Severity,
):
    if config is None:
        return default_severity

    insights = config.get(CONFIG_METRICS, {})
    metric = insights.get(alias, {})
    severity = metric.get(CONFIG_SEVERITY, default_severity)
    return severity
