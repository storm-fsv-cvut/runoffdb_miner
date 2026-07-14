from typing import Literal
import pandas as pd

def integrate_series(
    df: pd.DataFrame,
    source_col: str,
    target_col: str,
    *,
    time_unit: Literal["seconds", "minutes", "hours"] = "minutes",
    method: Literal["trapezoid", "left", "right"] = "trapezoid",
    initial_zero: bool = False,
) -> None:
    """
    Integrate a time series into cumulative values.

    Methods:
        trapezoid - trapezoidal rule
        left      - left Riemann sum
        right     - right Riemann sum
    """

    if source_col not in df.columns or df.index.empty:
        df[target_col] = pd.NA
        return

    factor = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
    }[time_unit]

    dt = (
        df.index.to_series()
        .diff()
        .dt.total_seconds()
        .fillna(0)
        / factor
    )

    source = pd.to_numeric(df[source_col], errors="coerce").fillna(0)

    if method == "trapezoid":
        interval_values = (source.shift(1) + source) / 2
        interval_values.iloc[0] = 0
        interval_values = interval_values.fillna(0)

    elif method == "left":
        interval_values = source.shift(1).fillna(0)

    elif method == "right":
        interval_values = source

    else:
        raise ValueError(f"Unknown integration method '{method}'")

    if initial_zero and len(interval_values) > 1:
        interval_values.iloc[1] = 0

    df[target_col] = (interval_values * dt).cumsum()
