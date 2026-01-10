from .interpolation import get_value_in_time
import pandas as pd

def integrate_by_minutes(df, series_name, start_time = None, end_time = None, zero_time = None, extrapolate = None, interpolate=True):
    return integrate_by_time(df, series_name, start_time, end_time, zero_time, extrapolate, interpolate, 'minutes')


def integrate_by_time(df, series_name, start_time=None, end_time=None, zero_time=None, extrapolate=None, interpolate=True, time_unit='minutes'):
    import numpy as np

    # ensure dataframe is timedelta-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    time_conversion_factor = {
        'seconds': 1,
        'minutes': 60,
        'hours': 3600
    }

    if time_unit not in time_conversion_factor:
        raise ValueError("Invalid time unit. Allowed values are 'seconds', 'minutes', 'hours'.")

    conversion_factor = time_conversion_factor[time_unit]

    if start_time is None:
        start_time = zero_time if zero_time is not None else df.index[0]
    if end_time is None:
        end_time = df.index[-1]

    output_value = 0
    prev_time = None
    prev_value = None
    found_valid_value = False  # Track if any valid value was used

    for time in df.index:
        value = get_value_in_time(df, time, series_name, zero_time, extrapolate)

        if pd.isna(value):
            if prev_time is None:
                continue  # skip initial NaNs entirely now
            else:
                continue  # skip NaNs elsewhere too

        if prev_time is not None:
            found_valid_value = True

            if prev_time >= start_time and time <= end_time:
                if time == prev_time:
                    print(f"\nError: two consequent times are equal at {time}. Skipping.")
                    continue
                time_diff = time - prev_time
                if interpolate:
                    output_value += (value + prev_value) / 2 * time_diff.total_seconds() / conversion_factor
                else:
                    output_value += prev_value * time_diff.total_seconds() / conversion_factor

            elif prev_time < start_time and time > start_time:
                time_diff = time - start_time
                if interpolate:
                    output_value += (value + prev_value) / 2 * time_diff.total_seconds() / conversion_factor
                else:
                    output_value += prev_value * time_diff.total_seconds() / conversion_factor

            elif prev_time < end_time and time > end_time:
                time_diff = end_time - prev_time
                if interpolate:
                    output_value += (value + prev_value) / 2 * time_diff.total_seconds() / conversion_factor
                else:
                    output_value += prev_value * time_diff.total_seconds() / conversion_factor

        prev_time = time
        prev_value = value

    return output_value if found_valid_value else np.nan

def integrate_series(
    df: pd.DataFrame,
    source_col: str,
    target_col: str,
    time_unit: str = "minutes",
    shift_source: bool = True,
) -> None:
    if source_col not in df or df.index.empty:
        df[target_col] = pd.NA
        return

    factor = {"seconds": 1, "minutes": 60, "hours": 3600}[time_unit]

    delta_seconds = (
        df.index.to_series()
        .diff()
        .dt.total_seconds()
        .fillna(0)
    )

    source = df[source_col]
    if shift_source:
        source = source.shift(1).fillna(0)

    df[target_col] = (source * (delta_seconds / factor)).cumsum()
