from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .severity import IssueSeverity
from .absence_reasons import DataAbsenceReason


@dataclass(frozen=True)
class DataIssue:
    """
    Represents a processing/data problem.

    Issues form a recursive causal tree:
    - derivation failed
        -> missing runoff
        -> invalid concentration
    """

    reason: DataAbsenceReason
    details: str = ""
    severity: IssueSeverity = IssueSeverity.WARNING

    # metadata: dict | None = None

    # -------------------------
    # helpers
    # -------------------------

    def identity(self) -> tuple:

        return (
            self.reason,
            self.details,
            self.severity,
        )

    # -------------------------
    # serialization
    # -------------------------

    def to_dict(self) -> dict:

        return {
            "reason": self.reason.name,
            "details": self.details,
            "severity": self.severity.name,
            # "metadata": self.metadata,
        }
