from dataclasses import dataclass, field
from typing import List, Optional, Literal


MissingStrategy = Literal[
    "discard_run",
    "discard_row",
    "keep_partial",
    "fill_nodata"
]

@dataclass(frozen=True)
class VariablePolicyOverride:
    resolution_order: tuple[str, ...] | None = None

    required: bool | None = None

    allow_interpolation: bool | None = None

    allow_unit_conversion: bool | None = None

@dataclass(frozen=True)
class DataPolicy:
    """
    Defines how the processing engine handles missing or incomplete data.
    This is NOT about data selection — only about execution semantics.
    """

    resolution_order: tuple[
        str,
        ...
    ] = (
        "direct",
        "derived"
    )

    skip_runs_missing_records: bool = False

    # transformation controls
    allow_interpolation: bool = True

    allow_unit_conversion: bool = True

    variable_overrides: dict[str, VariablePolicyOverride] = field(
        default_factory=dict
    )

