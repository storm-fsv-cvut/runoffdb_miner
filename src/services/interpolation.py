from ..exceptions import RequestedTimeDeltaValueMissing

import pandas as pd
from typing import Literal


def interpolate_dataframe(df: pd.DataFrame, methods: dict[str, Literal["linear", "ffill"]]) -> pd.DataFrame:
    """
    Interpolate DataFrame columns according to methods.
    :param df: input DataFrame with TimedeltaIndex
    :param methods: dict[column_name] -> interpolation method
    :return: interpolated DataFrame
    """
    result = df.copy()
    for col, method in methods.items():
        if col in result:
            if method == "linear":
                result[col] = result[col].interpolate(method="linear", limit_direction="both")
            elif method == "ffill":
                result[col] = result[col].ffill()
    return result

def get_value_in_time(
    df: pd.DataFrame, timedelta, series_name, zero_time=None,
    interpolate=True, extrapolate=None, fallback="previous"
):
    """
    Returns interpolated or extrapolated value of data series at the specified timedelta.

    :param df: DataFrame with a TimedeltaIndex.
    :param timedelta: the time point at which to get the value.
    :param series_name: column name of the series to be interpolated/extrapolated.
    :param zero_time: presumed time of start of the series (value = 0).
    :param interpolate: whether to interpolate values if exact match not found.
    :param extrapolate: range of extrapolation specified as a multiplier of the last interval.
    :param fallback: behavior when interpolate=False and requested time is between values.
                     Options:
                        - "previous" (default): return the last valid value before timedelta
                        - "error": raise RequestedTimeDeltaValueMissing
                        - None: return None
    :returns: interpolated/extrapolated/last-valid value based on specified inputs, otherwise None.
    """
    import pandas as pd

    # ensure dataframe is time-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    try:
        first_time = df.index[0]
        last_time = df.index[-1]
    except IndexError:
        return None

    # exact match
    if timedelta in df.index:
        return df.loc[timedelta, series_name]

    # before first value
    if timedelta < first_time:
        if zero_time:
            # print(" - extrapolating to zero\n")
            return (df.loc[first_time, series_name] / (first_time - zero_time).total_seconds()) * \
                   (timedelta - zero_time).total_seconds()
        else:
            print(f"Requested time is before the first record in '{series_name}' and extrapolation to zero was not requested.\n")
            return None

    # after last value
    elif timedelta > last_time:
        if extrapolate:
            if df[series_name].size > 1:
                # returns last recorded value
                if extrapolate == -1:
                    return df.iloc[-1][series_name]
                # check duration of last step
                last_step_duration = df.index[-1] - df.index[-2]
                if (timedelta - df.index[-1]) < extrapolate * last_step_duration:
                    # print(" - extrapolating after series end\n")
                    v1, v2 = df.iloc[-2][series_name], df.iloc[-1][series_name]
                    t1, t2 = df.index[-2], df.index[-1]
                    return v2 + (timedelta - t2).total_seconds() * ((v2 - v1) / (t2 - t1).total_seconds())
            else:
                print(f"Data series '{series_name}' doesn't have enough values for extrapolation.\n")
                return None
        else:
            raise RequestedTimeDeltaValueMissing(
                series_name, timedelta,
                f"Requested timedelta is after last record in the data series and extrapolation was not requested.\n"
            )

    # within series range
    if interpolate:
        # linear interpolation
        df_with_requested_time = df.reindex(df.index.union([timedelta]))
        interpolated_series = df_with_requested_time[series_name].interpolate(method='time')
        return interpolated_series.loc[timedelta]

    # fallback behavior when interpolate=False
    prev_times = df.index[df.index < timedelta]
    if prev_times.empty:
        print(f"No earlier values found in '{series_name}' for the requested timedelta.\n")
        return None

    if fallback == "previous":
        last_valid_time = prev_times[-1]
        # print(f" - using previous value from {last_valid_time}\n")
        return df.loc[last_valid_time, series_name]
    elif fallback == "error":
        raise RequestedTimeDeltaValueMissing(
            series_name, timedelta,
            f"Requested timedelta is between index values of data series and interpolation was not requested.\n"
        )
    else:
        # print(f"Interpolation disabled and fallback=None — returning None for '{series_name}'.\n")
        return None
