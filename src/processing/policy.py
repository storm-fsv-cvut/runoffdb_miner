from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Literal


MissingStrategy = Literal[
    "discard_run",
    "discard_row",
    "keep_partial",
    "fill_nodata"
]

class ResolutionMode(Enum):
    DIRECT_ONLY = auto()
    DERIVED_ONLY = auto()
    PREFER_DIRECT = auto()
    PREFER_DERIVED = auto()

@dataclass(frozen=True)
class VariableRequestOptions:
    resolution_mode: ResolutionMode | None = None
    required: bool | None = None
    allow_interpolation: bool | None = None
    allow_unit_conversion: bool | None = None

@dataclass(frozen=True)
class DataPolicy:
    """
    Defines how the processing engine handles missing or incomplete data.
    This is NOT about data selection — only about execution semantics.
    """

    resolution_mode: ResolutionMode = ResolutionMode.PREFER_DIRECT

    skip_runs_missing_records: bool = False

    # transformation controls
    allow_interpolation: bool = True

    allow_unit_conversion: bool = True

    variable_overrides: dict[str, VariableRequestOptions] = field(default_factory=dict)

    def resolution_for(self, variable):

        override = self.variable_overrides.get(variable)

        if override and override.resolution_mode:
            return override.resolution_mode

        return self.resolution_mode