import pandas as pd
from ..services.integration import integrate_series

def derive_rainfall_total(df: pd.DataFrame) -> None:
    integrate_series(df, "rainfall_intensity", "rainfall_total", time_unit="hours", shift_source=True)

def derive_discharge(df: pd.DataFrame) -> None:
    integrate_series(df, "surface_runoff", "discharge", time_unit="minutes", shift_source=True)

def derive_sediment_flux(df: pd.DataFrame) -> None:
    (df["surface_runoff"].fillna(0) * df["sediment_concentration"].fillna(0)).replace(0, pd.NA)

def derive_sediment_yield(df: pd.DataFrame) -> None:
    integrate_series(df, "sediment_flux", "sediment_yield", time_unit="minutes", shift_source=True)