from dataclasses import dataclass
from .absence_reasons import DataAbsenceReason
from .severity import TraceSeverity


@dataclass(frozen=True)
class DataIssue:
    reason: DataAbsenceReason
    source: str
    details: str
    causes: tuple["DataIssue", ...] = ()

@dataclass(frozen=True)
class DataTrace:
    issues: tuple[DataIssue, ...]
    severity: TraceSeverity
    category: str
    level: str

    def identity(self) -> tuple:
        return (
            self.category,
            self.level,
            tuple(
                (i.reason, i.source, i.details)
                for i in self.issues
            ),
        )

# service functions
def serialize_issue(issue: DataIssue) -> dict:
    return {
        "reason": issue.reason,
        "source": issue.source,
        "details": issue.details,
        "causes": [serialize_issue(c) for c in issue.causes],
    }


def serialize_trace(trace: DataTrace) -> dict:
    return {
        "severity": trace.severity,
        "category": trace.category,
        "level": trace.level,
        "issues": [serialize_issue(i) for i in trace.issues],
    }
