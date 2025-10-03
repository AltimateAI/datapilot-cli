from dataclasses import dataclass
from enum import Enum
from typing import Dict


class Severity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass
class InsightResult:
    name: str
    type: str
    message: str
    recommendation: str
    reason_to_flag: str
    metadata: Dict


@dataclass
class InsightResponse:
    insight: InsightResult
    severity: Severity = Severity.ERROR
