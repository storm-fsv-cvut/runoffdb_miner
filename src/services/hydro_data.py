from __future__ import annotations

from typing import Dict, List, Optional

from src.setup.unit_ids import *
from src.services.interpolation import *

from src.services.interpolation import interpolate_dataframe
from src.services.integration import integrate_series
from src.services.record_resolution import *

from src.exceptions import RecordSetNotComplete, DataframeEmptyError, DataframeNotTimeIndexed
from src.entities.run import Run

# defaults

DEFAULT_UNITS = {
    "runoff": RUNOFF_RATE_LMIN_UNIT_ID,
    "sediment_concentration": SS_CONCENTRATION_GL_UNIT_ID,
    "rainfall_intensity": RAINFALL_INTENSITY_MMH_UNIT_ID,
    "sediment_flux": SEDIMENT_FLUX_GMIN_UNIT_ID,
}

DEFAULT_LABELS = {
    "runoff": "runoff",
    "sediment_concentration": "sediment_concentration",
    "rainfall_intensity": "rainfall_intensity",
    "rainfall_total": "rainfall_total",
    "discharge": "discharge",
    "sediment_flux": "sediment_flux",
    "sediment_yield": "sediment_yield",
}

DEFAULT_INTERPOLATIONS = {
    "runoff": "linear",
    "sediment_concentration": "linear",
    "rainfall_intensity": "ffill",
    "sediment_flux": "linear",
}


def get_best_hydro_data(
        *,
        run: "Run",
        labels_map: Optional[Dict[str, str]] = None,
        request_map: Optional[Dict[str, bool]] = None,
        interpolation_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Assemble hydrological data for a run into a single Timedelta-indexed DataFrame.

    Semantics:
    - request_map[key] == True -> must exist, else raise RecordSetNotComplete
    - all request_map values False -> export whatever exists
    """

    labels_map = labels_map or {}
    request_map = request_map or {}
    interpolation_map = interpolation_map or {}

    labels = {k: labels_map.get(k, v) for k, v in DEFAULT_LABELS.items()}
    requested = {k: request_map.get(k, False) for k in DEFAULT_LABELS}
    interpolations = {k: interpolation_map.get(k, v) for k, v in DEFAULT_INTERPOLATIONS.items()}

    # dependency graph
    dependencies = {
        "rainfall_intensity": [{"record": "rainfall_intensity"}],
        "rainfall_total": [{"derived_from": ["rainfall_intensity"]}],
        "runoff": [{"record": "runoff"}],
        "sediment_concentration": [{"record": "sediment_concentration"}],
        "discharge": [{"derived_from": ["runoff"]}],
        "sediment_flux": [
            {"record": "sediment_flux"},
            {"derived_from": ["runoff", "sediment_concentration"]},
        ],
        "sediment_yield": [{"derived_from": ["sediment_flux"]}],
    }

    # resolve present records
    present_records, derived_sources = _resolve_present_records(run=run, dependencies=dependencies)

    # collect timelines
    dataframes: List[pd.DataFrame] = []
    missing_requested: List[str] = []

    for key, record in present_records.items():
        if record is None:
            if requested[key]:
                missing_requested.append(key)
            continue

        if not record.is_timeline:
            continue

        df = _get_record_timeline(run=run, record=record, value_label=key, target_unit_id=DEFAULT_UNITS.get(key))
        if df.empty and requested[key]:
            missing_requested.append(key)
            continue

        if not df.empty:
            dataframes.append(df)

    if missing_requested:
        raise RecordSetNotComplete(
            [k for k, v in requested.items() if v],
            missing_requested,
        )

    if not dataframes:
        return pd.DataFrame(columns=list(labels.values()))

    # --- merge timelines ---
    merged = pd.concat(dataframes, axis=1, join="outer")
    merged.index = pd.to_timedelta(merged.index)
    merged.sort_index(inplace=True)

    # ensure all keys exist
    for key in DEFAULT_LABELS:
        if key not in merged:
            merged[key] = pd.NA

    # --- interpolate ---
    merged = interpolate_dataframe(merged, interpolations)

    # --- derived quantities ---
    for key, sources in derived_sources.items():
        # check if all sources exist and have at least one non-NA value
        if all(src in merged.columns and merged[src].notna().any() for src in sources):
            if key == "rainfall_total":
                integrate_series(merged, "rainfall_intensity", "rainfall_total", time_unit="hours", shift_source=True)
            elif key == "discharge":
                integrate_series(merged, "runoff", "discharge", time_unit="minutes", shift_source=True)
            elif key == "sediment_flux":
                merged["sediment_flux"] = (
                        merged["runoff"].fillna(0)
                        * merged["sediment_concentration"].fillna(0)
                ).replace(0, pd.NA)
            elif key == "sediment_yield":
                integrate_series(merged, "sediment_flux", "sediment_yield", time_unit="minutes", shift_source=False)

    # rename columns
    merged = merged.rename(columns=labels)

    return merged


def _resolve_present_records(*, run, dependencies, units=DEFAULT_UNITS):
    present_records = {}
    derived_sources = {}

    for key, configs in dependencies.items():
        record = None
        derived = None

        for cfg in configs:
            if "record" in cfg:
                record = get_best_record_of_unit(run=run, unit_id=units.get(key))
                if record:
                    break
            elif "derived_from" in cfg:
                derived = cfg["derived_from"]

        present_records[key] = record
        if derived:
            derived_sources[key] = derived

    return present_records, derived_sources

def get_best_rainfall_record(
    *,
    run,
    view_order=None,
):
    """
    Returns the best available rainfall intensity record for a run.

    Preference order:
    1. Dedicated rainfall intensity record explicitly assigned to the run
    2. Best available rainfall intensity record matching known unit / phenomenon combinations
    """

    if run.rain_intensity_recid is not None:
        return run.runoffdb.load_record_by_id(run.rain_intensity_recid)

    # fallback: search for any suitable rainfall intensity record
    return get_best_record_of_unit(
        run=run,
        unit_id=[RAINFALL_INTENSITY_MMH_UNIT_ID, RAINFALL_INTENSITY_MMMIN_UNIT_ID],
        phenomenon_id=RAINFALL_PHEN_ID,
        view_order=view_order,
    )

def get_rainfall_intensity_dataframe(
    *,
    run,
    target_unit_id: Optional[int] = None,
    series_label: str = "rain_intensity",
) -> Optional[pd.DataFrame]:
    """
    Return rainfall intensity timeline DataFrame for a run, or None if invalid.

    Validity rules:
    - must be a timeline
    - must contain at least two rows
    - if exactly two rows, last value must be zero
    """

    rec_id = getattr(run, "rain_intensity_recid", None)
    if rec_id is None:
        return None

    record = get_best_rainfall_record(run=run)
    if record is None:
        return None

    try:
        df = _get_record_timeline(
            run=run,
            record=record,
            value_label=series_label,
            target_unit_id=target_unit_id,
        )
    except DataframeEmptyError:
        raise

    if df is None or df.empty:
        return None

    if len(df.index) < 2:
        return None

    if len(df.index) == 2:
        if df[series_label].iloc[-1] != 0:
            return None

    return df

def get_rainfall_intensity_value(
    *,
    run,
    target_unit_id: Optional[int] = None,
) -> Optional[object]:
    """
    Return representative rainfall intensity value.

    Returns:
    - float → constant rainfall
    - "interrupted"
    - "variable"
    - None → unavailable / invalid
    """

    try:
        df = get_rainfall_intensity_dataframe(
            run=run,
            target_unit_id=target_unit_id,
            series_label="rain_intensity",
        )
    except DataframeEmptyError:
        return None

    if df is None:
        return None

    values = df["rain_intensity"]

    if len(values) == 2:
        if values.iloc[-1] != 0:
            return None
        return values.iloc[0]

    # variable / interrupted rainfall
    zero_count = (values == 0).sum()

    if zero_count > 1:
        return "interrupted"

    return "variable"

def get_best_sediment_concentration_record(
        *,
        run,
        view_order=None
        ):
    return get_best_record_of_unit(
        run=run,
        unit_id=[SS_CONCENTRATION_MGL_UNIT_ID, SS_CONCENTRATION_GL_UNIT_ID],
        phenomenon_id=SEDIMENT_QUANTITY_PHEN_ID,
        view_order=view_order)


def get_best_runoff_record(
        *,
        run,
        view_order=None
        ):
    return get_best_record_of_unit(
        run=run,
        unit_id=RUNOFF_RATE_LMIN_UNIT_ID,
        phenomenon_id=SURFACE_RUNOFF_PHEN_ID,
        view_order=view_order)

def _get_record_timeline(*, run, record, value_label, target_unit_id):
    """
    Fetches data for a specific record, optionally converts it to target unit, and logs its status.
    :return: DataFrame with the data for the record.
    """
    if target_unit_id and record.unit_id != target_unit_id:
        return get_record_data(
            target_unit_id=target_unit_id,
            value_label=value_label,
            demand_timeline=True,
        )

    return record.get_data(
        value_label=value_label,
        demand_timeline=True,
    )


def _adjust_end_time(merged_data, kwargs):
    """
    Adjusts the end time based on the last valid index of runoff or sediment concentration.
    :return: Adjusted dataframe.
    """
    runoff_label = kwargs.get("runoff")
    sed_conc_label = kwargs.get("sediment_concentration")

    end_time = min(
        merged_data[runoff_label].last_valid_index(),
        merged_data[sed_conc_label].last_valid_index()
    )
    merged_data = merged_data.loc[:end_time]
    return merged_data


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