import pandas as pd
from ..setup.unit_ids import UNITS_CONVERSION

class UnitConversionError(Exception):
    pass


def convert_dataframe_units(
    *,
    df: pd.DataFrame,
    source_unit_id: int,
    target_unit_id: int,
    value_column: str,
    output_column: str | None = None,
) -> pd.DataFrame:
    """
    Convert values in df from source_unit_id to target_unit_id.
    """
    if source_unit_id == target_unit_id:
        return df

    try:
        multiplier = UNITS_CONVERSION[source_unit_id][target_unit_id]
    except KeyError:
        raise UnitConversionError(
            f"no conversion defined from unit {source_unit_id} to {target_unit_id}"
        )

    out = df.copy()
    col = output_column or value_column
    out[col] = out[value_column] * multiplier
    return out
