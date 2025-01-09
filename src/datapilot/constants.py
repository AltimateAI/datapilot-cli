from enum import Enum


class Extra(str, Enum):
    allow = "allow"
    forbid = "forbid"
    ignore = "ignore"
