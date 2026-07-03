from dataclasses import dataclass
from typing import Callable, Any, Union

from src.entities.run import Run

HeaderType = Union[
    dict[str, str],
    Callable[[dict, str], str]
]


@dataclass(frozen=True)
class RunColumn:
    header: HeaderType
    getter: Callable[['Run', dict], Any]


@dataclass(frozen=True)
class IntervalColumn:
    header: HeaderType
    getter: callable  # (run, df_row, ctx, state) -> value


def build_variable_header(
    key: str,
    registry,
    lang: str,
) -> str:
    """
    Build localized variable header including unit symbol.

    Resolves:
        - localized base label from VariableDefinition.base_labels
        - default unit symbol from registry units

    Fallback strategy:
        - if variable missing -> return key
        - if label missing for lang -> fallback to 'en'
        - if no unit -> return label only
    """
    try:
        var_def = registry[key]
    except KeyError:
        return key  # unknown variable

    # ---- resolve label ----
    label = None
    if var_def.base_labels:
        label = (
            var_def.base_labels.get(lang)
            or var_def.base_labels.get("en")
        )

    if not label:
        label = key

    # ---- resolve unit ----
    unit_symbol = None
    if var_def.default_unit_id is not None:
        unit = registry._units.get(var_def.default_unit_id)
        if unit:
            unit_symbol = getattr(unit, "unit", None)

    if unit_symbol:
        return f"{label} [{unit_symbol}]"

    return label

def resolve_header_of_column(column, registry, lang):
    if callable(column.header):
        return column.header(registry, lang)

    if isinstance(column.header, dict):
        return column.header.get(lang) or column.header.get("en") or ""
