from __future__ import annotations

from typing import Dict, List, Optional
from dataclasses import dataclass

from src.services.interpolation import *

from src.services.interpolation import interpolate_dataframe
from src.services.integration import integrate_series
from src.services.record_resolution import *

from src.diagnostics.trace import DataTrace, DataIssue
from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.severity import TraceSeverity

from src.entities.run import Run
from src.setup.variables_registry import *


@dataclass(frozen=True)
class RecordDep:
    key: str
    record: object | None
    kind: str = "record"

@dataclass(frozen=True)
class DerivedDep:
    key: str
    sources: tuple[str, ...]
    kind: str = "derived"

def get_best_hydro_data(
    *,
    run: "Run",
    labels_map: Optional[Dict[str, str]] = None,
    request_map: Optional[Dict[str, bool]] = None,
    interpolation_map: Optional[Dict[str, str]] = None,
    return_trace: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, "DataTrace | None"]:

    print("\n=== get_best_hydro_data START ===")
    print(f"Run ID: {run.id}")
    print(f"Request map: {request_map}")

    labels_map = labels_map or {}
    request_map = request_map or {}
    interpolation_map = interpolation_map or {}

    registry = run.runoffdb.variable_registry.subregistry_by_group(VariableGroup.HYDRO_SEDIMENT)

    default_units = registry.default_units()
    default_labels = registry.default_labels()
    default_interpolations = registry.default_interpolations()

    labels = {k: labels_map.get(k, v) for k, v in default_labels.items()}
    interpolations = {
        k: interpolation_map.get(k, v)
        for k, v in default_interpolations.items()
    }

    issues: list[DataIssue] = []
    trace_severity = TraceSeverity.INFO

    merged = pd.DataFrame()
    resolved_cache: dict[str, bool] = {}

    # ------------------------------------------------------------------
    # Recursive resolver
    # ------------------------------------------------------------------
    def resolve_variable(key: str, required: bool) -> bool:

        print(f"\nResolving variable: {key} (required={required})")

        if key in resolved_cache:
            print(f"  -> already resolved: {resolved_cache[key]}")
            return resolved_cache[key]

        var_def = registry[key]

        # --------------------------------------------------------------
        # 1. Try direct record
        # --------------------------------------------------------------
        record = None
        if any("record" in d for d in var_def.dependencies):
            print(f"  -> trying direct record for '{key}', allowed_units: {var_def.allowed_unit_ids}")

            try:
                record = get_best_record_of_unit(
                    owner=run,
                    unit_id=var_def.allowed_unit_ids,
                    phenomenon_id=var_def.phenomenon_id,
                )
            except Exception as e:
                print(f"  !! record lookup exception: {e}")

            if record:
                print(f"  -> found record ID {record.id}")

                if not record.is_timeline:
                    print("  !! record not timeline type")
                else:
                    df, sub_issues = get_record_data(
                        record=record,
                        value_label=key,
                        target_unit_id=default_units.get(key),
                        return_trace=True,
                    )

                    if df is not None and not df.empty:
                        print(f"  -> record data loaded, rows: {len(df)}")
                        series = df[key] if key in df else df.iloc[:, 0]
                        merged[key] = series
                        resolved_cache[key] = True
                        return True
                    else:
                        print("  !! record has no usable data")
            else:
                print("  -> no direct record found")

        # --------------------------------------------------------------
        # 2. Try derivation
        # --------------------------------------------------------------
        if var_def.derivation_func:
            print(f"  -> attempting derivation of '{key}'")

            dependency_keys = []
            for dep in var_def.dependencies:
                dependency_keys.extend(dep.get("derived_from", []))

            print(f"     dependencies: {dependency_keys}")

            for dep_key in dependency_keys:
                ok = resolve_variable(dep_key, required)
                if not ok:
                    print(f"     !! dependency '{dep_key}' failed")
                    resolved_cache[key] = False
                    return False

            try:
                print(f"     -> executing derivation_func for '{key}'")
                var_def.derivation_func(merged)
            except Exception as e:
                print(f"     !! derivation failed: {e}")
                resolved_cache[key] = False
                return False

            if key in merged and merged[key].notna().any():
                print(f"     -> derivation succeeded for '{key}'")
                resolved_cache[key] = True
                return True
            else:
                print(f"     !! derivation produced no usable data")

        # --------------------------------------------------------------
        # 3. Not resolvable
        # --------------------------------------------------------------
        print(f"  -> '{key}' NOT resolvable")
        resolved_cache[key] = False
        return False

    # ------------------------------------------------------------------
    # 1. Resolve required variables
    # ------------------------------------------------------------------
    for key, is_required in request_map.items():
        if not is_required:
            continue

        print(f"\n--- Resolving mandatory variable '{key}' ---")

        available = resolve_variable(key, required=True)

        if not available:
            print(f"!! Mandatory variable '{key}' unavailable. Aborting.")

            issues.append(DataIssue(
                reason=DataAbsenceReason.NO_RECORD,
                source="get_best_hydro_data",
                details=f"mandatory variable '{key}' unavailable",
            ))
            trace_severity = TraceSeverity.ERROR

            if return_trace:
                trace = DataTrace(
                    issues=tuple(issues),
                    category="data_issues",
                    level="hydro_sediment_record_set",
                    severity=trace_severity,
                )
                return pd.DataFrame(), trace

            return pd.DataFrame()

    # ------------------------------------------------------------------
    # 2. Resolve optional variables
    # ------------------------------------------------------------------
    print("\n--- Resolving optional variables ---")

    for key in registry.keys():
        if key in resolved_cache:
            continue
        resolve_variable(key, required=False)

    # ------------------------------------------------------------------
    # 3. Final checks
    # ------------------------------------------------------------------
    print("\nMerged columns:", list(merged.columns))

    if merged.empty:
        print("!! merged DataFrame is EMPTY")
        issues.append(DataIssue(
            reason=DataAbsenceReason.INVALID_VALUES,
            source="get_best_hydro_data",
            details="no resolvable variables available",
        ))

        if return_trace:
            trace = DataTrace(
                issues=tuple(issues),
                category="data_issues",
                level="hydro_sediment_record_set",
                severity=trace_severity,
            )
            return merged, trace

        return merged

    merged.index = pd.to_timedelta(merged.index)
    merged.sort_index(inplace=True)

    merged, interp_issues = interpolate_dataframe(
        merged,
        interpolations,
        return_trace=True,
    )

    print("=== get_best_hydro_data END ===\n")

    if return_trace:
        if issues:
            trace = DataTrace(
                issues=tuple(issues),
                category="data_issues",
                level="hydro_sediment_record_set",
                severity=trace_severity,
            )
        else:
            trace = None

        return merged, trace

    return merged




def get_best_rainfall_intensity_record(
    *,
    run,
    view_order=None,
    return_trace: bool = False,
):
    """
    Returns the best available rainfall intensity record for a run.

    Preference order:
    1. Dedicated rainfall intensity record explicitly assigned to the run
    2. Best available rainfall intensity record matching known unit / phenomenon combinations
    """

    issues: list[DataIssue] = []

    # get the best record for the unit or units list
    return resolve_dedicated_or_generic_record(
        owner=run,
        dedicated_recid_attr="rain_intensity_recid",
        unit_id=RAINFALL_INTENSITY_UNITS,
        phenomenon_id=RAINFALL_PHEN_ID,
        view_order=view_order,
        return_trace=return_trace,
    )

def get_rainfall_intensity_value(
    *,
    run: Run,
    target_unit_id: Optional[int] = None,
    return_trace: bool = False,
) -> Optional[object]:
    """
    Return representative rainfall intensity value.

    Returns:
    - float → constant rainfall
    - "interrupted"
    - "variable"
    - None → unavailable / invalid
    """

    issues: list[DataIssue] = []

    record, sub_issues = get_best_rainfall_intensity_record(
        run=run,
        return_trace=return_trace,
    )

    if not record:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            source="get_rainfall_intensity_value",
            details="No rainfall intensity record found for run",
            causes=sub_issues
        ))
        return (None, tuple(issues)) if return_trace else None

    df, sub_issues = get_record_data(
        record=record,
        target_unit_id=target_unit_id,
        value_label="rain_intensity",
        return_trace=return_trace)

    if df is None or df.empty:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_DATA_IN_RECORD,
            source="get_rainfall_intensity_value",
            details=f"record ID {record.id} (requested as '{'rain_intensity'}') has no data",
            causes=sub_issues,
        ))
        return (None, tuple(issues)) if return_trace else None

    values = df["rain_intensity"]

    if sub_issues:
        issues.extend(sub_issues)

    if len(values) == 2:
        # the last value of a propper rainfall timeline is 0 (zero)
        if values.iloc[-1] != 0:
            issues.append(DataIssue(
                reason=DataAbsenceReason.INVALID_VALUES,
                source="get_rainfall_intensity_value",
                details=f"rainfall intensity record #{record.id} has non-zero last value",
            ))
            return (None, tuple(issues)) if return_trace else None

        return (values.iloc[0], None) if return_trace else values.iloc[0]

    # variable / interrupted rainfall
    zero_count = (values == 0).sum()

    if zero_count > 1:
        issues.append(DataIssue(
            reason=DataAbsenceReason.INVALID_VALUES,
            source="get_rainfall_intensity_value",
            details=f"rainfall intensity record #{record.id} has more non-zero values (rainfall was interrupted)",
        ))
        return ("interrupted", tuple(issues)) if return_trace else "interrupted"

    else:
        issues.append(DataIssue(
            reason=DataAbsenceReason.INVALID_VALUES,
            source="get_rainfall_intensity_value",
            details=f"rainfall intensity record #{record.id} has more non-zero values (rainfall was interrupted)",
        ))
        return ("variable", tuple(issues)) if return_trace else "variable"

def get_best_sediment_concentration_record(
        *,
        run,
        view_order=None
        ):

    return get_best_record_of_unit(
        owner=run,
        unit_id=SS_CONCENTRATION_UNITS,
        phenomenon_id=SEDIMENT_QUANTITY_PHEN_ID,
        view_order=view_order)


def get_best_runoff_record(
        *,
        run,
        view_order=None
        ):

    return get_best_record_of_unit(
        owner=run,
        unit_id=RUNOFF_RATE_UNITS,
        phenomenon_id=SURFACE_RUNOFF_PHEN_ID,
        view_order=view_order)

def get_initial_moisture_value(
    *,
    run: "Run",
    multi_value: bool = False,
    return_trace: bool = False,
):
    issues: list[DataIssue] = []

    record, sub_issues = resolve_dedicated_or_generic_record(
        owner=run,
        dedicated_recid_attr="initmoist_recid",
        unit_id=SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,
        return_trace=return_trace,
    )
    if not record:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            source="get_initial_moisture_value",
            details="No initial soil moisture record found",
            causes=sub_issues
        ))
        return (None, tuple(issues)) if return_trace else None

    if sub_issues:
        issues.extend(list(sub_issues))

    value, sub_issues = get_record_scalar_value(
        record=record,
        target_unit_id=SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,
        value_label="initial_moisture",
        multi_value=multi_value,
        return_trace=return_trace,
    )
    if sub_issues:
        issues.extend(list(sub_issues))

    return (value, tuple(issues)) if return_trace else value


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

