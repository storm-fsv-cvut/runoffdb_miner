from ..exceptions import RequestedTimeDeltaValueMissing

from src.diagnostics.report_collector import ReportCollector
from src.diagnostics.issue import DataIssue
from src.diagnostics.trace import DataTrace, create_trace
from src.diagnostics.severity import IssueSeverity
from src.diagnostics.absence_reasons import DataAbsenceReason

import pandas as pd
from typing import Literal

def interpolate_dataframe(
    df: pd.DataFrame,
    methods: dict[str, Literal["linear", "ffill"]],
) -> tuple[pd.DataFrame | None, DataTrace]:
    """
    Interpolate DataFrame columns according to requested methods.

    Returns:
        interpolated dataframe and trace describing the operation.
    """

    trace = create_trace(
        source="interpolate_dataframe",
        dataset="hydro_sediment_data",
    )

    if df is None or df.empty:
        trace.success = False
        trace.details = "hydro-sediment dataframe assembly failed"
        trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.INVALID_VALUES,
                details="obtained empty dataframe for interpolation",
                severity=IssueSeverity.WARNING,
            )
        )

        return None, trace

    result = df.copy()

    for col, method in methods.items():

        if col not in result.columns:
            pass
        #     trace.issues.append(
        #         DataIssue(
        #             reason=DataAbsenceReason.NOT_REQUESTED,
        #             details=f"column '{col}' not present for interpolation",
        #             severity=IssueSeverity.INFO,
        #         )
        #     )
            continue

        before = result[col].notna().sum()

        try:

            if method == "linear":
                numeric = pd.to_numeric(
                    result[col],
                    errors="coerce",
                )

                # values lost during conversion
                lost = (result[col].notna() & numeric.isna()).sum()

                if lost:
                    trace.issues.append(
                        DataIssue(
                            reason=DataAbsenceReason.INVALID_VALUES,
                            details=f"column '{col}' contained {lost} non-numeric values converted to NaN",
                            severity=IssueSeverity.WARNING,
                        )
                    )

                result[col] = numeric.interpolate(
                    method="linear",
                    limit_direction="forward",
                )

            elif method == "ffill":
                result[col] = result[col].ffill()

            else:

                trace.issues.append(
                    DataIssue(
                        reason=DataAbsenceReason.INVALID_VALUES,
                        details=f"interpolation method '{method}' for '{col}' was not specified",
                        severity=IssueSeverity.ERROR,
                    )
                )

                trace.success = False
                continue

            after = result[col].notna().sum()

            if before > 0 and after == 0:

                trace.success = False

                trace.issues.append(
                    DataIssue(
                        reason=DataAbsenceReason.INTERPOLATION_FAILED,
                        details=f"column '{col}' lost all values during interpolation",
                        severity=IssueSeverity.ERROR,
                    )
                )

            elif after > before:
                trace.traces.append(
                    DataTrace(
                        source="interpolation",
                        variable=col,
                        details=f"{method}: filled {after - before} values",
                        success=True,
                    )
                )

        except Exception as e:
            trace.success = False
            trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.INTERPOLATION_FAILED,
                    details=(
                        f"interpolation failed for '{col}': {e}"
                    ),
                    severity=IssueSeverity.ERROR,
                )
            )

    return result, trace


def get_value_in_time(
        df: pd.DataFrame,
        timedelta,
        series_name,
        zero_time=None,
        interpolate=True,
        extrapolate=None,
        fallback="previous",
        *,
        return_trace: bool = False
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
        issue = DataIssue(
            reason=DataAbsenceReason.INVALID_RECORD_TYPE,
            source="get_value_in_time",
            details=f"data series '{series_name}' is not is not TimedeltaIndex",
        )
        return (None, (issue,)) if return_trace else None

    try:
        first_time = df.index[0]
        last_time = df.index[-1]
    except IndexError:
        issue = DataIssue(
            reason=DataAbsenceReason.INTERPOLATION_NO_DATA,
            source="get_value_in_time",
            details=f"data series '{series_name}' is empty",
        )
        return (None, (issue, )) if return_trace else None

    # exact match
    if timedelta in df.index:
        return (df.loc[timedelta, series_name], None) if return_trace else df.loc[timedelta, series_name]

    # before first value
    if timedelta < first_time:
        if zero_time:
            v = (df.loc[first_time, series_name] / (first_time - zero_time).total_seconds()) * \
                (timedelta - zero_time).total_seconds()
            return (v, None) if return_trace else v
        else:
            issue = DataIssue(
                reason=DataAbsenceReason.BEFORE_FIRST_VALUE,
                source="get_value_in_time",
                details=f"requested timedelta {timedelta} before first value in '{series_name}'",
            )
            return (None, (issue, )) if return_trace else None

    # after last value
    if timedelta > last_time:
        if extrapolate:
            if df[series_name].size > 1:
                v1, v2 = df.iloc[-2][series_name], df.iloc[-1][series_name]
                t1, t2 = df.index[-2], df.index[-1]
                if (timedelta - t2) < extrapolate * (t2 - t1):
                    v = v2 + (timedelta - t2).total_seconds() * ((v2 - v1) / (t2 - t1).total_seconds())
                    return (v, None) if return_trace else v
                elif extrapolate == -1:
                    return (df.iloc[-1][series_name], None) if return_trace else df.iloc[-1][series_name]
            issue = DataIssue(
                reason=DataAbsenceReason.EXTRAPOLATION_FAILED,
                source="get_value_in_time",
                details=f"not enough data to extrapolate '{series_name}' at {timedelta}",
            )
            return (None, (issue, )) if return_trace else None
        else:
            issue = DataIssue(
                reason=DataAbsenceReason.AFTER_LAST_VALUE,
                source="get_value_in_time",
                details=f"requested timedelta after last row in '{series_name}' and extrapolation not allowed",
            )
            return (None, (issue, )) if return_trace else None

    # within series range
    if interpolate:
        df_with_requested_time = df.reindex(df.index.union([timedelta]))
        interpolated_series = df_with_requested_time[series_name].interpolate(method='time')
        value = interpolated_series.loc[timedelta]
        if pd.isna(value):
            issue = DataIssue(
                reason=DataAbsenceReason.INTERPOLATION_NO_DATA,
                source="get_value_in_time",
                details=f"interpolation failed for '{series_name}' at {timedelta}",
            )
            return (value, (issue, )) if return_trace else value

    # fallback behavior when interpolate=False
    prev_times = df.index[df.index < timedelta]
    if prev_times.empty:
        issue = DataIssue(
            reason=DataAbsenceReason.INTERPOLATION_NO_DATA,
            source="get_value_in_time",
            details=f"can not interpolate value: requested timedelta {timedelta} before first row in '{series_name}'",
        )
        return (None, (issue, )) if return_trace else None

    if fallback == "previous":
        last_valid_time = prev_times[-1]
        value = df.loc[last_valid_time, series_name]
        return (value, None) if return_trace else value

    elif fallback == "error":
        issue = DataIssue(
            reason=DataAbsenceReason.INVALID_REQUEST,
            source="get_value_in_time",
            details=f"requested timedelta {timedelta} is between rows of '{series_name}' and interpolation not allowed",
        )
        return (None, (issue,)) if return_trace else None
    # else:
    #     trace = DataReport(
    #         reason=DataAbsenceReason.INTERPOLATION_NO_DATA,
    #         source="get_value_in_time",
    #         details=f"fallback=None returned None for '{series_name}' at {timedelta}",
    #     )
    #     return (None, trace) if return_trace else None