from enum import Enum
from typing import Dict

from pydantic import BaseModel


class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class InsightResult(BaseModel):
    name: str
    type: str
    message: str
    recommendation: str
    reason_to_flag: str
    metadata: Dict


class InsightResponse(BaseModel):
    insight: InsightResult
    severity: Severity = Severity.ERROR
