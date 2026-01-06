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
