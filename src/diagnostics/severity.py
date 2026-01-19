# diagnostics/severity.py
from enum import Enum


class TraceSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
