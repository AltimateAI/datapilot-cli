from enum import Enum
from typing import Dict

from pydantic import BaseModel
from pydantic import ConfigDict


class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class InsightResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    type: str
    message: str
    recommendation: str
    reason_to_flag: str
    metadata: Dict


class InsightResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    insight: InsightResult
    severity: Severity = Severity.ERROR
