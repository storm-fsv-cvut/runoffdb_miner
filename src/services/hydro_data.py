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

# dependency relations
DEFAULT_DEPENDENCIES = {
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

# def get_best_hydro_data(
#     *,
#     run: "Run",
#     labels_map: Optional[Dict[str, str]] = None,
#     request_map: Optional[Dict[str, bool]] = None,
#     interpolation_map: Optional[Dict[str, str]] = None,
#     return_trace: bool = False,
# ) -> pd.DataFrame | tuple[pd.DataFrame, "DataTrace | None"]:
#     """
#     Assemble hydrological data for a run into a single Timedelta-indexed DataFrame.
#
#     Semantics of TraceSeverity:
#     - if any request_map[key] == True -> must exist -> ERROR
#     - all request_map values False -> export whatever exists -> WARNING/INFO
#     """
#     labels_map = labels_map or {}
#     request_map = request_map or {}
#     interpolation_map = interpolation_map or {}
#
#     labels = {k: labels_map.get(k, v) for k, v in DEFAULT_LABELS.items()}
#     requested = {k: request_map.get(k, False) for k in DEFAULT_LABELS}
#     interpolations = {k: interpolation_map.get(k, v) for k, v in DEFAULT_INTERPOLATIONS.items()}
#
#     trace: DataTrace | None = None
#     trace_severity = TraceSeverity.INFO
#     issues: list[DataIssue] = []
#
#     # --- resolve present records ---
#     present_records, derived_sources = _resolve_present_records(
#         run=run,
#         dependencies=DEFAULT_DEPENDENCIES,
#     )
#
#     dataframes: List[pd.DataFrame] = []
#     missing_requested: List[str] = []
#
#
#     # --- load timelines with explicit tracing ---
#     for key, record in present_records.items():
#
#         # 1. record missing entirely
#         if record is None:
#             issues.append(DataIssue(
#                             reason=DataAbsenceReason.NO_RECORD,
#                             source="get_best_hydro_data",
#                             details=f"record '{key}' not present",
#                             )
#                         )
#             if requested.get(key):
#                 missing_requested.append(key)
#                 trace_severity = TraceSeverity.ERROR
#
#             continue
#
#         # 2. record exists but is not a timeline
#         if not record.is_timeline:
#             issues.append(DataIssue(
#                     reason=DataAbsenceReason.INVALID_RECORD_TYPE,
#                     source="get_best_hydro_data",
#                     details=f"record ID {record.id} (requested as '{key}') is not timeline type",
#                     ))
#             if return_trace:
#                 if requested.get(key):
#                     trace_severity = TraceSeverity.ERROR
#
#             continue
#
#         # 3. attempt to load record timeline
#         df, sub_issues = get_record_data(
#             run=run,
#             record=record,
#             value_label=key,
#             target_unit_id=DEFAULT_UNITS.get(key),
#             return_trace=True,
#         )
#
#         # 4. timeline loaded but empty
#         if df.empty:
#             if return_trace:
#                 issues.append(DataIssue(
#                         reason=DataAbsenceReason.NO_DATA_IN_RECORD,
#                         source="get_best_hydro_data",
#                         details=f"record ID {record.id} (requested as '{key}') has no data",
#                         causes=sub_issues
#                         )
#                     )
#
#             if requested.get(key):
#                 missing_requested.append(key)
#                 trace_severity = TraceSeverity.ERROR
#             continue
#
#         # 5. timeline exists and has data
#         dataframes.append(df)
#
#     # --- handle no usable records at all ---
#     if not dataframes:
#         if return_trace and trace is None:
#             if len(missing_requested) > 0:
#                 trace_severity = TraceSeverity.ERROR
#             else:
#                 trace_severity = TraceSeverity.INFO
#
#             issue = (DataIssue(
#                 reason=DataAbsenceReason.NO_RECORD,
#                 source="get_best_hydro_data",
#                 details=f"no usable hydro/sediment data acquired",
#                 causes=tuple(issues)
#                 ),)
#
#             trace = DataTrace(
#                             issues=issue,
#                             category="missing_records",
#                             level="hydro_sediment_record_set",
#                             severity=trace_severity,
#                             )
#
#         empty = pd.DataFrame(columns=list(labels.values()))
#         return (empty, trace) if return_trace else empty
#
#     # --- merge timelines ---
#     merged = pd.concat(dataframes, axis=1, join="outer")
#     merged.index = pd.to_timedelta(merged.index)
#     merged.sort_index(inplace=True)
#
#     # ensure all keys exist
#     for key in DEFAULT_LABELS:
#         if key not in merged:
#             merged[key] = pd.NA
#
#     # --- interpolate ---
#     merged, interp_issues = interpolate_dataframe(
#         merged,
#         interpolations,
#         return_trace=True,
#     )
#
#     interpolation_issues = []
#     for key, iss in interp_issues.items():
#         if requested.get(key):
#             trace_severity = TraceSeverity.ERROR
#         else:
#             trace_severity = TraceSeverity.WARNING
#
#         interpolation_issues.append(DataIssue(
#                 reason=DataAbsenceReason.INTERPOLATION_FAILED,
#                 source="get_best_hydro_data",
#                 details=f"'{key}' value timeline corrupted during interpolation",
#                 causes=tuple(iss)
#                 ))
#
#         trace = DataTrace(
#                         issues=iss,
#                         category="interpolation_failed",
#                         level="hydro_sediment_record_set",
#                         severity=trace_severity,
#                         )
#
#     # --- derived quantities ---
#     for key, sources in derived_sources.items():
#         if not all(src in merged.columns and merged[src].notna().any() for src in sources):
#             if return_trace:
#                 if requested.get(key):
#                     severity = TraceSeverity.ERROR
#                 else:
#                     severity = TraceSeverity.INFO
#                 trace = DataTrace(
#                     issues=(DataIssue(
#                         reason=DataAbsenceReason.MISSING_REQUIRED_INPUT,
#                         source="get_best_hydro_data",
#                         details=f"missing sources for required derived field '{key}'",
#                         ),
#                     ),
#                     category="missing_data",
#                     level="hydro_sediment_records_derivation",
#                     severity=severity,
#                 )
#             continue
#
#         # compute derived fields
#         if key == "rainfall_total":
#             integrate_series(
#                 merged,
#                 "rainfall_intensity",
#                 "rainfall_total",
#                 time_unit="hours",
#                 shift_source=True,
#             )
#         elif key == "discharge":
#             integrate_series(
#                 merged,
#                 "runoff",
#                 "discharge",
#                 time_unit="minutes",
#                 shift_source=True,
#             )
#         elif key == "sediment_flux":
#             merged["sediment_flux"] = (
#                 merged["runoff"].fillna(0)
#                 * merged["sediment_concentration"].fillna(0)
#             ).replace(0, pd.NA)
#
#         elif key == "sediment_yield":
#             integrate_series(
#                 merged,
#                 "sediment_flux",
#                 "sediment_yield",
#                 time_unit="minutes",
#                 shift_source=True,
#             )
#
#     # --- final check for completely NA result ---
#     if merged.notna().sum().sum() == 0 and return_trace:
#         trace = DataTrace(
#                         issues=(DataIssue(
#                             reason=DataAbsenceReason.INVALID_VALUES,
#                             source="get_best_hydro_data",
#                             details="result dataframe contains only NA values",
#                             causes=tuple(issues)
#                             ),),
#                         category="invalid_data",
#                         level="hydro_sediment_records_set",
#                         severity=trace_severity,
#                         )
#
#     # rename columns
#     merged = merged.rename(columns=labels)
#
#     return (merged, trace) if return_trace else merged

def get_best_hydro_data(
    *,
    run: "Run",
    labels_map: Optional[Dict[str, str]] = None,
    request_map: Optional[Dict[str, bool]] = None,
    interpolation_map: Optional[Dict[str, str]] = None,
    return_trace: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, "DataTrace | None"]:
    """
    Assemble hydrological data for a run into a single Timedelta-indexed DataFrame.

    Semantics:
    - empty DataFrame == function succeeded, no usable data
    - None is never returned
    """

    # ------------------------------------------------------------------
    # 0. Normalize inputs
    # ------------------------------------------------------------------
    labels_map = labels_map or {}
    request_map = request_map or {}
    interpolation_map = interpolation_map or {}

    labels = {k: labels_map.get(k, v) for k, v in DEFAULT_LABELS.items()}
    required = {k: request_map.get(k, False) for k in DEFAULT_LABELS}
    interpolations = {k: interpolation_map.get(k, v) for k, v in DEFAULT_INTERPOLATIONS.items()}

    issues: list[DataIssue] = []
    trace_severity = TraceSeverity.INFO
    trace: DataTrace | None = None

    # ------------------------------------------------------------------
    # 1. Resolve availability (pure, no diagnostics inside resolver)
    # ------------------------------------------------------------------
    resolved = _resolve_present_records(
        run=run,
        dependencies=DEFAULT_DEPENDENCIES,
    )

    # ------------------------------------------------------------------
    # 2. Resolution diagnostics
    # ------------------------------------------------------------------
    for key, deps in resolved.items():
        record_dep = deps["record"]
        derived_dep = deps["derived"]

        # requested key with no direct record and no derivation path
        if record_dep and record_dep.record is None and derived_dep is None:
            issues.append(
                DataIssue(
                    reason=DataAbsenceReason.NO_RECORD,
                    source="get_best_hydro_data",
                    details=f"no direct or derived data available for '{key}'",
                )
            )
            if required.get(key):
                trace_severity = TraceSeverity.ERROR

    # ------------------------------------------------------------------
    # 3. Load DIRECT timeline records
    # ------------------------------------------------------------------
    dataframes: list[pd.DataFrame] = []

    for key, deps in resolved.items():
        record_dep = deps["record"]
        if record_dep is None:
            continue

        record = record_dep.record

        # 3a. missing record
        if record is None:
            continue  # already diagnosed in step 2

        # 3b. invalid record type
        if not record.is_timeline:
            issues.append(
                DataIssue(
                    reason=DataAbsenceReason.INVALID_RECORD_TYPE,
                    source="get_best_hydro_data",
                    details=f"record ID {record.id} (requested as '{key}') is not timeline type",
                )
            )
            if required.get(key):
                trace_severity = TraceSeverity.ERROR
            continue

        # 3c. load data
        df, sub_issues = get_record_data(
            record=record,
            value_label=key,
            target_unit_id=DEFAULT_UNITS.get(key),
            return_trace=True,
        )

        # 3d. empty data
        if df is None or df.empty:
            issues.append(
                DataIssue(
                    reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                    source="get_best_hydro_data",
                    details=f"record ID {record.id} ('{key}') contains no usable data",
                    causes=sub_issues if sub_issues else None,
                )
            )
            if required.get(key):
                trace_severity = TraceSeverity.ERROR
            continue

        dataframes.append(df)

    # ------------------------------------------------------------------
    # 4. No usable direct records at all
    # ------------------------------------------------------------------
    if not dataframes:
        empty = pd.DataFrame(columns=list(labels.values()))

        if return_trace:
            trace = DataTrace(
                issues=tuple(issues) if issues else (
                    DataIssue(
                        reason=DataAbsenceReason.NO_RECORD,
                        source="get_best_hydro_data",
                        details="no usable hydro/sediment records acquired",
                        causes=tuple(issues)
                    ),
                ),
                category="missing_records",
                level="hydro_sediment_record_set",
                severity=trace_severity,
            )

        return (empty, trace) if return_trace else empty

    # ------------------------------------------------------------------
    # 5. Merge timelines
    # ------------------------------------------------------------------
    merged = pd.concat(dataframes, axis=1, join="outer")
    merged.index = pd.to_timedelta(merged.index)
    merged.sort_index(inplace=True)

    for key in DEFAULT_LABELS:
        if key not in merged:
            merged[key] = pd.NA

    # ------------------------------------------------------------------
    # 6. Interpolation
    # ------------------------------------------------------------------
    merged, interp_issues = interpolate_dataframe(
        merged,
        interpolations,
        return_trace=True,
    )

    for key, iss in interp_issues.items():
        if not iss:
            continue

        issues.append(
            DataIssue(
                reason=DataAbsenceReason.INTERPOLATION_FAILED,
                source="get_best_hydro_data",
                details=f"interpolation failed for '{key}'",
                causes=tuple(iss),
            )
        )

        trace_severity = (
            TraceSeverity.ERROR if required.get(key)
            else TraceSeverity.WARNING
        )

    # ------------------------------------------------------------------
    # 7. Derived quantities
    # ------------------------------------------------------------------
    for key, deps in resolved.items():
        derived_dep = deps["derived"]
        if derived_dep is None:
            continue

        missing_sources = [
            src for src in derived_dep.sources
            if src not in merged or not merged[src].notna().any()
        ]

        if missing_sources:
            issues.append(
                DataIssue(
                    reason=DataAbsenceReason.MISSING_REQUIRED_INPUT,
                    source="get_best_hydro_data",
                    details=f"cannot derive '{key}'; missing source data",
                    causes=tuple(
                        DataIssue(
                            reason=DataAbsenceReason.NO_RECORD,
                            source="get_best_hydro_data",
                            details=f"source '{src}' unavailable",
                        )
                        for src in missing_sources
                    ),
                )
            )
            if required.get(key):
                trace_severity = TraceSeverity.ERROR
            continue

        # --- compute derived ---
        if key == "rainfall_total":
            integrate_series(
                merged,
                "rainfall_intensity",
                "rainfall_total",
                time_unit="hours",
                shift_source=True,
            )

        elif key == "discharge":
            integrate_series(
                merged,
                "runoff",
                "discharge",
                time_unit="minutes",
                shift_source=True,
            )

        elif key == "sediment_flux":
            merged["sediment_flux"] = (
                merged["runoff"].fillna(0)
                * merged["sediment_concentration"].fillna(0)
            ).replace(0, pd.NA)

        elif key == "sediment_yield":
            integrate_series(
                merged,
                "sediment_flux",
                "sediment_yield",
                time_unit="minutes",
                shift_source=True,
            )

    # ------------------------------------------------------------------
    # 8. Final sanity check
    # ------------------------------------------------------------------
    if merged.notna().sum().sum() == 0:
        issues.append(
            DataIssue(
                reason=DataAbsenceReason.INVALID_VALUES,
                source="get_best_hydro_data",
                details="result dataframe contains only NA values",
            )
        )

    # ------------------------------------------------------------------
    # 9. Finalize trace
    # ------------------------------------------------------------------
    if return_trace and issues:
        trace = DataTrace(
            issues=tuple(issues),
            category="data_issues",
            level="hydro_sediment_record_set",
            severity=trace_severity,
        )

    merged = merged.rename(columns=labels)
    return (merged, trace) if return_trace else merged


def _resolve_present_records(*, run, dependencies, units=DEFAULT_UNITS):
    """
    Resolve available direct and derived data dependencies for a given run.

    This helper inspects the dependency specification for each requested output
    key and determines:
      - whether a suitable *direct record* exists on the run (unit-aware),
      - whether the key can alternatively be produced as a *derived value*
        from other source keys.

    The function does not load any data, perform validation, or generate issues.
    It only resolves *availability* and *relationships* so that higher-level
    logic (e.g. `get_best_hydro_data`) can:
      - prefer direct records when present,
      - fall back to derived computations when possible,
      - emit structured `DataTrace` information when neither is viable.

    Parameters
    ----------
    run : Run
        Simulation run instance against which records are resolved.

    dependencies : dict[str, list[dict]]
        Mapping of output keys to dependency configurations.
        Each configuration dictionary may define:
          - {"record": ...}          direct record request
          - {"derived_from": [...]}  derived dependency on other keys

        Multiple configurations per key are allowed; the resolver records
        at most one direct and one derived dependency per key.

    units : dict[str, int], optional
        Mapping of output keys to preferred unit IDs used when selecting
        the best matching direct record. Defaults to DEFAULT_UNITS.

    Returns
    -------
    dict[str, dict[str, RecordDep | DerivedDep | None]]
        A mapping keyed by output field name. Each value contains:
          - "record":  RecordDep instance or None
          - "derived": DerivedDep instance or None

        Presence of an entry does not imply validity or usability; it only
        reflects what *could* be attempted later.

    Notes
    -----
    - Resolution is intentionally permissive: missing records, incompatible
      types, empty timelines, and missing derived inputs are handled later.
    - This function is a pure resolver and should remain free of side effects
      such as logging, tracing, or data access beyond record lookup.
    """

    resolved: dict[str, dict[str, RecordDep | DerivedDep | None]] = {}

    for key, configs in dependencies.items():
        record_dep: RecordDep | None = None
        derived_dep: DerivedDep | None = None

        # ---- resolve record ONCE per key ----
        if any("record" in cfg for cfg in configs):
            record = get_best_record_of_unit(
                owner=run,
                unit_id=units.get(key),
            )
            record_dep = RecordDep(
                key=key,
                record=record,
            )

        # ---- resolve derived dependencies ----
        for cfg in configs:
            if "derived_from" in cfg:
                derived_dep = DerivedDep(
                    key=key,
                    sources=tuple(cfg["derived_from"]),
                )
                break  # only one derived dep per key is allowed

        resolved[key] = {
            "record": record_dep,
            "derived": derived_dep,
        }

    return resolved

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
        issues.extend(sub_issues)

    value, sub_issues = get_record_scalar_value(
        record=record,
        target_unit_id=SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,
        value_label="initial_moisture",
        multi_value=multi_value,
        return_trace=return_trace,
    )
    if sub_issues:
        issues.extend(sub_issues)

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

