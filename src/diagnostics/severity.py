# diagnostics/severity.py
from enum import Enum


class IssueSeverity(str, Enum):
    INFO = 0
    WARNING = 1
    ERROR = 2
