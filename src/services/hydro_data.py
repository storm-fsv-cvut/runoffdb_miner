from __future__ import annotations

from ..setup.unit_ids import *
from .interpolation import *
from .integration import *

from typing import Dict, List, Optional

from src.services.interpolation import interpolate_dataframe
from src.services.integration import integrate_series

from src.exceptions import RecordSetNotComplete, DataframeEmptyError, DataframeNotTimeIndexed

def get_best_hydro_data(
    *,
    run,
    labels_map: Optional[Dict[str, str]] = None,
    request_map: Optional[Dict[str, bool]] = None,
    interpolation_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Assemble best available hydrological data for a run into a single
    Timedelta-indexed DataFrame.

    Semantics:
    - If request_map[key] == True and data cannot be produced -> raise RecordSetNotComplete
    - If all request_map values are False -> export whatever exists
    """

    # defaults

    default_units = {
        "runoff": RUNOFF_RATE_LMIN_UNIT_ID,
        "sediment_concentration": SS_CONCENTRATION_GL_UNIT_ID,
        "rainfall_intensity": RAINFALL_INTENSITY_MMH_UNIT_ID,
        "sediment_flux": SEDIMENT_FLUX_GMIN_UNIT_ID,
    }

    default_labels = {
        "runoff": "runoff",
        "sediment_concentration": "sediment_concentration",
        "rainfall_intensity": "rainfall_intensity",
        "rainfall_total": "rainfall_total",
        "discharge": "discharge",
        "sediment_flux": "sediment_flux",
        "sediment_yield": "sediment_yield",
    }

    default_interpolations = {
        "runoff": "linear",
        "sediment_concentration": "linear",
        "rainfall_intensity": "ffill",
        "sediment_flux": "linear",
    }

    labels_map = labels_map or {}
    request_map = request_map or {}
    interpolation_map = interpolation_map or {}

    labels = {k: labels_map.get(k, v) for k, v in default_labels.items()}
    requested = {k: request_map.get(k, False) for k in default_labels}
    interpolations = {k: interpolation_map.get(k, v) for k, v in default_interpolations.items()}

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

    # resolve records
    present_records, derived_sources = _resolve_present_records(
        run=run,
        dependencies=dependencies,
        requested=requested,
        default_units=default_units,
    )

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

        try:
            df = _get_record_dataframe(
                run=run,
                record=record,
                key=key,
                target_unit_id=default_units.get(key),
            )
        except DataframeEmptyError:
            if requested[key]:
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

    # merge timelines
    merged = pd.concat(dataframes, axis=1, join="outer")
    merged.index = pd.to_timedelta(merged.index)
    merged.sort_index(inplace=True)

    # ensure all keys exist
    for key in default_labels:
        if key not in merged:
            merged[key] = pd.NA

    # interpolate
    merged = interpolate_dataframe(merged, interpolations)

    # derive quantities
    if merged["rainfall_intensity"].notna().any():
        integrate_series(
            merged,
            "rainfall_intensity",
            "rainfall_total",
            time_unit="hours",
        )

    if merged["runoff"].notna().any():
        integrate_series(
            merged,
            "runoff",
            "discharge",
            time_unit="minutes",
        )

    if merged["sediment_flux"].isna().all():
        if all(col in merged for col in ("runoff", "sediment_concentration")):
            merged["sediment_flux"] = merged["runoff"] * merged["sediment_concentration"]

    if merged["sediment_flux"].notna().any():
        integrate_series(
            merged,
            "sediment_flux",
            "sediment_yield",
            time_unit="minutes",
        )

    # ------------------------------------------------------------------
    # rename columns for export
    # ------------------------------------------------------------------

    merged = merged.rename(columns=labels)

    return merged

def _resolve_present_records(*, run, dependencies, requested, default_units):
    present_records = {}
    derived_sources = {}

    for key, configs in dependencies.items():
        record = None
        derived = None

        for cfg in configs:
            if "record" in cfg:
                record = run.get_best_record_of_unit(default_units.get(key))
                if record:
                    break
            elif "derived_from" in cfg:
                derived = cfg["derived_from"]

        present_records[key] = record
        if derived:
            derived_sources[key] = derived

    return present_records, derived_sources


def _get_record_dataframe(*, run, record, key, target_unit_id):
    if target_unit_id and record.unit_id != target_unit_id:
        return record.get_data_in_unit(
            target_unit_id=target_unit_id,
            value_name=key,
            output_column_label=key,
            demand_timeline=True,
        )

    return record.get_data(
        key,
        demand_timeline=True,
    )


def _get_record_data(record, label, log_label, demand_timeline=False, target_unit_id=None):
    """
    Fetches data for a specific record, optionally converts it to target unit, and logs its status.
    :return: DataFrame with the data for the record.
    """
    import pandas as pd

    try:
        if target_unit_id is not None and target_unit_id != record.unit_id:
            df = record.get_data_in_unit(
                target_unit_id=target_unit_id,
                value_name=label,
                output_column_label=label,
                demand_timeline=demand_timeline
            )
            if df is not None and not df.empty:
                self.runoffdb.log(self.id,
                                  f"{log_label} (record #{record.id}) data converted to unit: {self.runoffdb.units[target_unit_id].unit} (original unit {record.unit.unit})")
        else:
            df = record.get_data(label, demand_timeline=demand_timeline)
            if df is not None and not df.empty:
                self.runoffdb.log(self.id, f"{log_label} (record #{record.id}) data unit: {record.unit.unit}")

        return df

    except DataframeEmptyError:
        # self.runoffdb.log(self.id, f"{log_label} DataFrame of record #{record.id} is empty")
        return pd.DataFrame({label: []})
    except DataframeNotTimeIndexed:
        # self.runoffdb.log(self.id, f"{log_label} DataFrame of record #{record.id} is not timeline")
        return pd.DataFrame({label: []})
    except Exception as e:
        # self.runoffdb.log(self.id, f"{log_label} record not available:\n{e}")
        return pd.DataFrame({label: []})


def _adjust_end_time(self, merged_data, kwargs):
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