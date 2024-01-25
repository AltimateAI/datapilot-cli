from enum import Enum
from typing import Dict, Text

from pydantic import BaseModel


class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class InsightResult(BaseModel):
    name: Text
    type: Text
    message: Text
    recommendation: Text
    reason_to_flag: Text
    metadata: Dict


class InsightResponse(BaseModel):
    insight: InsightResult
    severity: Severity = Severity.ERROR
