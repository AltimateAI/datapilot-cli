from enum import Enum
from typing import Dict

from datapilot.schemas.base import AltimateBaseModel


class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class InsightResult(AltimateBaseModel):
    name: str
    type: str
    message: str
    recommendation: str
    reason_to_flag: str
    metadata: Dict


class InsightResponse(AltimateBaseModel):
    insight: InsightResult
    severity: Severity = Severity.ERROR
