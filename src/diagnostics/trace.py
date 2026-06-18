from dataclasses import dataclass, field
from typing import Optional

from .severity import IssueSeverity
from .issue import DataIssue


@dataclass
class DataTrace:
    source: str
    details: str = ""
    variable: Optional[str] = None

    owner_id: Optional[int] = None
    owner_class: Optional[str] = None
    dataset: Optional[str] = None

    success: bool = True

    traces: list["DataTrace"] = field(default_factory=list)

    issues: list[DataIssue] = field(default_factory=list)

    # metadata: dict | None = None

    @property
    def failed(self) -> bool:
        return not self.success

    # -------------------------
    # helpers
    # -------------------------

    def identity(self) -> tuple:

        return (
            self.source,
            self.details,
            self.variable,
            self.success,
            self.owner_id,
            self.owner_class,
            self.dataset,
        )

    # -------------------------
    # serialization
    # -------------------------

    def to_dict(self) -> dict:

        return {
            "variable": self.variable,
            "source": self.source,
            "details": self.details,
            "owner_class": self.owner_class,
            "owner_id": self.owner_id,
            "dataset": self.dataset,
            "success": self.success,

            "traces": [
                p.to_dict()
                for p in self.traces
            ],

            "issues": [
                i.to_dict()
                for i in self.issues
            ],

            # "metadata": self.metadata,
        }


def create_trace(
    *,
    source: str,
    owner,
    details: str = None,
    variable: str = None,
    dataset: str = None,
    success: bool = True,
    traces: list[DataTrace] | None = None,
    issues: list[DataIssue] | None = None,
):
    return DataTrace(
        source=source,
        variable=variable,
        owner_id=getattr(owner, "id", None),
        owner_class=type(owner).__name__,
        dataset=dataset,
        details=details,
        success=success,
        traces=traces if traces is not None else [],
        issues=issues if issues is not None else [],
    )