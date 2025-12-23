# -*- coding: utf-8 -*-

from ..exceptions import RequestedTimeDeltaValueMissing
from datetime import timedelta


def as_list(v):
    if v is None:
        return None
    return v if isinstance(v, (list, tuple, set)) else [v]

def remove_last_zero_row(df):
    """
    Removes last row of a dataframe if value is equal to 0
    :param df: pandas Dataframe to be processes
    :return: copy of a Dataframe with removed last row or the original Dataframe if last row's value != 0
    """
    # Check if the last row value is equal to 0
    if df.iloc[-1]['value'] == 0:
        # Remove the last row
        df = df.copy().drop(df.index[-1])
    return df

def czech_date(datetime):
    return f"{datetime.strftime('%d.').strip('0')}{datetime.strftime('%m.').strip('0')}{datetime.strftime('%Y')}"


def format_timedelta_hms(td, **kwargs):
    return str(td).split(' ')[2]

def format_timedelta_s(timedelta):
    return timedelta.total_seconds()

def format_timedelta_min(timedelta):
    total_seconds = timedelta.total_seconds()
    minutes = int((total_seconds % 3600) / 60)
    return f"{minutes:.1f}"

def format_timedelta(td):
    """Format a timedelta object by removing '0 days'."""
    from pandas import Timedelta

    if isinstance(td, Timedelta):
        td_str = str(td)
        return td_str.replace('0 days ', '') if '0 days' in td_str else td_str
    elif isinstance(td, timedelta):
        return str(td)
    else:
        raise ValueError(f"input value '{td}' is not a TimeDelta instance. ({type(td)})")


def interpolate_texture(original_texture, new_limits, cum_mass_col_name, return_cumulative = True, return_int = True, smallest_content = 1):
    import pandas as pd

    # ensure original_texture is a pandas DataFrame
    if not isinstance(original_texture, pd.DataFrame):
        raise TypeError("original_texture parameter value must be a pandas DataFrame")

    # get column names from the input DataFrame
    particle_size_col = original_texture.index.name

    # extract original limits and cumulative contents from the DataFrame
    original_limits = original_texture.index.to_list()
    original_contents = original_texture[cum_mass_col_name].to_list()

    # insert artificial first datapoint with the smallest content to allow for interpolation of smaller particles content
    original_limits.insert(0, 0)
    original_contents.insert(0, smallest_content)

    # sort the new limits
    new_limits = sorted(new_limits)

    cumul_contents = []


    for nl in new_limits:
        i = 0
        for ol, content in zip(original_limits, original_contents):
            if i == 0:
                prev_ol = ol
                prev_content = content
            else:
                if nl > prev_ol and nl <= ol:
                    new_value = prev_content + ((content - prev_content) / (ol - prev_ol)) * (nl - prev_ol)
                    cumul_contents.append(new_value)
                prev_ol = ol
                prev_content = content
            i += 1

    # round the content values to integers if requested
    if return_int:
        cumul_contents = [round(val) for val in cumul_contents]

    # recalculate cumulative values to net values if requested
    if not return_cumulative:
        net_contents = [cumul_contents[0]]
        for j in range(1, len(cumul_contents)):
            net_contents.append(cumul_contents[j] - cumul_contents[j - 1])
        output_contents = net_contents
    else:
        output_contents = cumul_contents

    # create the output DataFrame with the same column names as the input DataFrame

    output_df = pd.DataFrame({
        particle_size_col: new_limits,
        cum_mass_col_name: output_contents
    })

    # set particle_size as the index
    output_df.set_index(particle_size_col, inplace=True)

    return output_df



def get_zero_time(dataframe, series_name):
    """
    Finds the point in time when the dataframe intersects the x-axis (searches for timestamp where value == 0)

    :param dataframe: time indexed dataframe
    :return: the timestamp of zero value or None if impossible to be interpolated
    """

    import pandas as pd

    # Ensure dataframe is time-indexed
    if not isinstance(dataframe.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")


    if dataframe.index.size > 1:
        # check the direction of the first interval and break if extrapolation is not possible
        if (dataframe[series_name].iloc[1] - dataframe[series_name].iloc[0]) == 0:
            print("Zero time value couldn't be extrapolated because the value in first interval is constant.\n")
            return None
        elif (dataframe[series_name].iloc[1] - dataframe[series_name].iloc[0]) < 0:
            print("Zero time value couldn't be extrapolated because the value in first interval is decreasing.\n")
            return None
        else:
            # extrapolate the zero value time from the first two points in dataseries
            t0 = dataframe[series_name].index[0]-((dataframe[series_name].iloc[0]*(dataframe[series_name].index[1]-dataframe[series_name].index[0]))/(dataframe[series_name].iloc[1]-dataframe[series_name].iloc[0]))
            # if the zero time should negative (meaning that there was a value before the experiment start) it is set to 0
            if t0 < pd.Timedelta(seconds=0):
                t0 = pd.Timedelta(seconds=0)
            return t0
    else:
        print ("Zero time value couldn't be extrapolated because the timeline doesn't have enough datapoints.\n")
        return None

def integrate_by_minutes(df, series_name, start_time = None, end_time = None, zero_time = None, extrapolate = None, interpolate=True):
    return integrate_by_time(df, series_name, start_time, end_time, zero_time, extrapolate, interpolate, 'minutes')


def integrate_by_time(df, series_name, start_time=None, end_time=None, zero_time=None, extrapolate=None, interpolate=True, time_unit='minutes'):
    import pandas as pd
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

def integrate_data_series(df, series_name_in, series_name_out, interpolate=True, time_unit='minutes'):
    """
    Calculates discreet integral for all points of given 'series_name_in' from dataframe 'df' and stores the values in new series 'series_name_out'

    :param df:
    :param series_name_in: column name to process
    :param series_name_out: column name for the output integrated series
    :param interpolate: whether to interpolate between points in time series, if False stepwise integration is performed (value considered constant in each time interval)
    :param time_unit: unit of time to use for integration ('minutes', 'hours', 'seconds')
    :return:
    """

    import pandas as pd

    # Ensure dataframe is time-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        print(type(df.index))
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    output_values = []

    for time in df.index:
        integral_value = integrate_by_time(df, series_name_in,  pd.Timedelta(seconds=0), time, interpolate=interpolate, time_unit=time_unit)

        # Store the integrated value
        output_values.append(integral_value)
    # Add the integrated values as a new column to the DataFrame
    df[series_name_out] = output_values

    return df

def get_value_in_time(
    df, timedelta, series_name, zero_time=None,
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

def integrate_flow(df, value_col, duration_col, placement="start",
                   interpolate=True, time_unit='minutes'):
    import pandas as pd

    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("Index must be TimedeltaIndex.")

    time_conv = {'seconds': 1, 'minutes': 60, 'hours': 3600}
    if time_unit not in time_conv:
        raise ValueError(f"Invalid time_unit: {time_unit}")

    # compute instantaneous flow rates (L/s)
    flow_rate = df[value_col] / df[duration_col]
    times = df.index
    dur = pd.to_timedelta(df[duration_col], unit='s')

    if placement == "start":
        new_times = pd.Index(times)
        new_flow = pd.Series(flow_rate.values, index=new_times)

    elif placement == "sample_mid":
        new_times = pd.Index(times + dur / 2)
        new_flow = pd.Series(flow_rate.values, index=new_times)
        new_times = pd.Index([times[0]]).append(new_times)
        new_flow = pd.concat([pd.Series([0.0], index=[times[0]]), new_flow])

    elif placement == "sample_end":
        new_times = pd.Index(times + dur)
        new_flow = pd.Series(flow_rate.values, index=new_times)
        new_times = pd.Index([times[0]]).append(new_times)
        new_flow = pd.concat([pd.Series([0.0], index=[times[0]]), new_flow])

    elif placement == "interval_mid":
        t_series = times.to_series()
        mid_times = t_series + (t_series.shift(-1) - t_series) / 2
        mid_times = mid_times.iloc[:-1]
        new_times = pd.Index(mid_times)
        new_flow = pd.Series(flow_rate.iloc[:-1].values, index=new_times)
        new_times = pd.Index([times[0]]).append(new_times)
        new_flow = pd.concat([pd.Series([0.0], index=[times[0]]), new_flow])

    else:
        raise ValueError(f"Unknown placement: {placement}")

    adj_df = pd.DataFrame({'flow_rate': new_flow.values}, index=new_times).sort_index()

    # integrate to discharge
    discharge = []      # per-interval volume
    cum_discharge = []  # running total
    total = 0
    prev_t, prev_f = None, None
    for t, f in adj_df['flow_rate'].items():
        if prev_t is not None:
            dt = (t - prev_t).total_seconds() / time_conv[time_unit]
            if interpolate:
                disch = 0.5 * (prev_f + f) * dt
            else:
                disch = prev_f * dt
        else:
            disch = 0
        total += disch
        discharge.append(disch)
        cum_discharge.append(total)

        prev_t, prev_f = t, f

    adj_df['discharge'] = discharge
    adj_df['cum_discharge'] = cum_discharge
    return adj_df