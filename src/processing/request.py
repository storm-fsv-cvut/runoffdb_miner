from dataclasses import dataclass, field
from typing import List
from src.filters.run_filter import RunFilter
from src.processing.policy import DataPolicy


@dataclass
class ProcessingOptions:
    diagnostics_level: str = "standard"
    # "none" | "standard" | "full"


@dataclass(frozen=True)
class ProcessingRequest:
    selection: RunFilter
    variables: list[str]
    policy: DataPolicy

    options: ProcessingOptions = field(default_factory=ProcessingOptions)