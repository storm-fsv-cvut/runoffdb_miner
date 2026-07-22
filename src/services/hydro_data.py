from __future__ import annotations

from typing import Dict, List
from dataclasses import dataclass

from src.services.interpolation import *

from src.services.interpolation import interpolate_dataframe
from src.services.integration import integrate_series
from src.services.record_resolution import *

from src.diagnostics.absence_reasons import DataAbsenceReason

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

def get_hydro_sediment_timeline(
    *,
    run: "Run",
    variables: list[str] | None = None,
    interpolation_map: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, DataTrace]:

    interpolation_map = interpolation_map or {}

    registry = run.runoffdb.variable_registry.subregistry_by_group(
        VariableGroup.HYDRO_SEDIMENT
    )

    default_units = registry.default_units()
    default_interpolations = registry.default_interpolations()

    interpolations = {
        k: interpolation_map.get(k, v)
        for k, v in default_interpolations.items()
    }

    root_trace = create_trace(
        source="get_hydro_sediment_timeline",
        owner=run,
        dataset="hydro_sediment_data",
        details=f"run_id={run.id}",
    )

    resolve_trace = create_trace(
        source="resolve_timeline_variables",
        owner=run,
    )

    assembly_trace = create_trace(
        source="assemble_timeline",
        owner=run,
    )

    derivation_trace = create_trace(
        source="derive_variables",
        owner=run,
    )

    resolved_series: dict[str, pd.Series] = {}
    derived_cache: dict[str, bool] = {}

    if variables is None:
        variables = list(registry.keys())

    # =====================================================
    # record variables
    # =====================================================

    def resolve_record_variable(key: str) -> bool:

        if key in resolved_series:
            return True

        var_def = registry[key]

        if not any("record" in d for d in var_def.dependencies):
            return False

        record, record_trace = get_best_record_of_unit(
            owner=run,
            unit_id=var_def.allowed_unit_ids,
            phenomenon_id=var_def.phenomenon_id,
        )

        if not record:
            resolve_trace.traces.append(record_trace)
            return False

        if not record.is_timeline:

            record_trace.success = False
            record_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.INVALID_VALUES,
                    details=f"record for '{key}' is not timeline data",
                    severity=IssueSeverity.WARNING,
                )
            )

            resolve_trace.traces.append(record_trace)
            return False

        df, data_trace = get_record_data(
            record=record,
            value_label=key,
            target_unit_id=default_units.get(key),
        )

        # append the data trace to the last (and should be the only one) trace of the record traces
        if record_trace.traces:
            record_trace.traces[-1].traces.append(data_trace)
        else:
            record_trace.traces.append(data_trace)

        if df is None or df.empty:
            record_trace.success = False
            resolve_trace.traces.append(record_trace)
            return False

        series = df[key] if key in df.columns else df.iloc[:, 0]
        series.name = key

        resolved_series[key] = series

        resolve_trace.traces.append(record_trace)

        return True

    # =====================================================
    # derived variables
    # =====================================================

    def derive_variable(key: str) -> bool:

        if key in merged.columns or key in resolved_series.keys():
            return True

        if key not in registry:
            derived_cache[key] = False
            return False

        var_def = registry[key]

        if not var_def.derivation_func:
            derived_cache[key] = False
            return False

        dependency_keys = []

        for dep in var_def.dependencies:
            dependency_keys.extend(dep.get("derived_from", []))

        unavailable_dependencies = []

        if len(dependency_keys) == 1:
            all_dep_string = f"'{dependency_keys[0]}'"
        else:
            all_dep_string = f"{', '.join(dependency_keys)}"

        for dep_key in dependency_keys:
            if not derive_variable(dep_key):
                derived_cache[key] = False
                unavailable_dependencies.append(dep_key)

        if unavailable_dependencies:
            trace = create_trace(
                source="derivation",
                owner=run,
                variable=key,
                success=False,
                details=f"needed {'dependencies' if len(dependency_keys)>1 else 'dependency'}: {all_dep_string}"
            )

            if len(unavailable_dependencies) == 1:
                dep_string = f"dependency '{unavailable_dependencies[0]}'"
            else:
                dep_string = f"dependencies {', '.join(unavailable_dependencies)}"

            trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.MISSING_REQUIRED_INPUT,
                    details=f"{dep_string} missing for derived variable '{key}'",
                    severity=IssueSeverity.WARNING,
                )
            )

            derivation_trace.traces.append(trace)

            return False

        trace = create_trace(
            source="derivation",
            owner=run,
            variable=key,
            details=var_def.derivation_description
        )

        try:
            var_def.derivation_func(merged)
            derivation_trace.traces.append(trace)
            derived_cache[key] = True

            return True

        except Exception as e:

            trace.success = False
            trace.details = str(e)

            trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.UNKNOWN,
                    details=str(e),
                    severity=IssueSeverity.ERROR,
                )
            )

            derivation_trace.traces.append(trace)

            derived_cache[key] = False

            return False

    # =====================================================
    # resolve timeline variables
    # =====================================================

    for key in variables:
        if key not in registry:
            continue

        resolve_record_variable(key)

    if not resolved_series:

        root_trace.success = False

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.NO_RECORD_IN_SET_AVAILABLE,
                details="no resolvable timeline variables",
                severity=IssueSeverity.WARNING,
            )
        )

        root_trace.traces.append(resolve_trace)

        return pd.DataFrame(), root_trace

    # =====================================================
    # assemble common timeline
    # =====================================================

    merged = pd.concat(
        resolved_series.values(),
        axis=1,
    )

    merged.index = pd.to_timedelta(merged.index)
    merged.sort_index(inplace=True)

    merged, interpolation_trace = interpolate_dataframe(
        merged,
        interpolations,
    )

    assembly_trace.traces.append(interpolation_trace)

    if merged is None:

        assembly_trace.success = False

        root_trace.success = False
        root_trace.traces.extend([
            resolve_trace,
            assembly_trace,
        ])

        return pd.DataFrame(), root_trace

    # =====================================================
    # derive remaining variables
    # =====================================================

    for key in variables:

        if key not in registry:
            continue

        derive_variable(key)

    # =====================================================
    # assemble final provenance tree
    # =====================================================

    root_trace.traces.append(resolve_trace)
    root_trace.traces.append(assembly_trace)

    if derivation_trace.traces or derivation_trace.issues:
        root_trace.traces.append(derivation_trace)

    return merged, root_trace

def get_best_rainfall_intensity_record(
    *,
    run: "Run",
    view_order: list[int] | None = None,
) -> tuple["Record | None", DataTrace]:
    """
    Returns the best available rainfall intensity record.

    Resolution order:
        1. run.rain_intensity_recid
        2. generic rainfall intensity record
    """

    var_def = (
        run.runoffdb
        .variable_registry["rainfall_intensity"]
    )

    return resolve_dedicated_or_generic_record(
        owner=run,
        dedicated_recid_attr="rain_intensity_recid",
        unit_id=var_def.allowed_unit_ids,
        phenomenon_id=var_def.phenomenon_id,
        view_order=view_order,
    )


def get_rainfall_intensity_value(
    *,
    run,
    target_unit_id: int | None = None,
) -> tuple[float | str | None, DataTrace]:

    root_trace = create_trace(
        source="get_rainfall_intensity_value",
        owner=run,
        variable="rainfall_intensity",
    )

    # --------------------------------------------------
    # resolve record
    # --------------------------------------------------

    record, record_trace = get_best_rainfall_intensity_record(run=run,)

    if not record:
        root_trace.success = False
        root_trace.details = "no rainfall intensity record found"

        if record_trace:
            root_trace.traces.append(record_trace)

        return None, root_trace

    current_trace = record_trace

    # --------------------------------------------------
    # load / convert data
    # --------------------------------------------------

    df, data_trace = get_record_data(
        record=record,
        value_label="rainfall_intensity",
        target_unit_id=target_unit_id,
    )

    if data_trace:
        # append the data trace to the last (and should be the only one) trace of the record traces
        if record_trace.traces:
            record_trace.traces[-1].traces.append(data_trace)
        else:
            record_trace.traces.append(data_trace)
        current_trace = data_trace

    if df is None or df.empty:

        root_trace.success = False

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                details=f"record #{record.id} returned no usable data",
                severity=IssueSeverity.ERROR
            )
        )

        root_trace.traces.append(current_trace)

        return None, root_trace

    # --------------------------------------------------
    # extract values
    # --------------------------------------------------

    values = df["rainfall_intensity"].dropna()

    if values.empty:

        root_trace.success = False

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                details=f"record #{record.id} contains only NaN values",
                severity=IssueSeverity.ERROR
            )
        )

        root_trace.traces.append(current_trace)

        return None, root_trace

    # --------------------------------------------------
    # classify rainfall regime
    # --------------------------------------------------

    if len(values) == 2:

        if values.iloc[-1] != 0:

            root_trace.success = False

            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.INVALID_VALUES,
                    details=f"record #{record.id} invalid terminal value "
                        "of rainfall timeline",
                    severity=IssueSeverity.WARNING
                )
            )

            root_trace.traces.append(current_trace)

            return None, root_trace

        root_trace.details = "rainfall regime classified as 'constant'"
        # root_trace.metadata = {
        #     "record_id": record.id,
        #     "rain_type": "constant",
        # }

        root_trace.traces.append(current_trace)

        return values.iloc[0], root_trace

    # --------------------------------------------------
    # variable / interrupted
    # --------------------------------------------------

    zero_count = int((values == 0).sum())

    rain_type = (
        "interrupted"
        if zero_count > 1
        else "variable"
    )

    root_trace.details = (
        f"rainfall regime classified as '{rain_type}'"
    )
    #
    # root_trace.metadata = {
    #     "record_id": record.id,
    #     "rain_type": rain_type,
    #     "zero_count": zero_count,
    # }

    root_trace.traces.append(current_trace)

    return rain_type, root_trace


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
):
    root_trace = DataTrace(
        variable="initial_moisture",
        source="get_initial_moisture_value",
        owner_class=type(run).__name__,
        owner_id=getattr(run, "id", None),
        success=True
    )

    record, rec_trace = resolve_dedicated_or_generic_record(
        owner=run,
        dedicated_recid_attr="initmoist_recid",
        unit_id=SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,
    )
    if not record:
        root_trace.success = False
        root_trace.details = "no initial moisture record found"
        root_trace.traces.append(rec_trace)
        return None, root_trace

    value, sv_trace = get_record_scalar_value(
        record=record,
        target_unit_id=SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,
        value_label="initial_moisture",
        multi_value=multi_value,
    )

    if sv_trace:
        rec_trace.traces.append(sv_trace)

    root_trace.traces.append(rec_trace)

    return value, root_trace


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

