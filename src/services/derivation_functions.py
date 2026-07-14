import pandas as pd


def calculate_sediment_flux(
    df: pd.DataFrame,
    surface_runoff_column: str,
    sediment_concentration_column: str,
    target_col: str,
) -> None:
    df[target_col] = (df[surface_runoff_column].fillna(0) * df[sediment_concentration_column].fillna(0)).replace(0, pd.NA)
