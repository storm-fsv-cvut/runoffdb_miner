from .unit_conversion import convert_dataframe_units, UnitConversionError
from src.setup.unit_ids import *
from src.setup.entity_ids import *

from src.diagnostics.trace import DataIssue
from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.trace import DataTrace, DataIssue
from ..utilities.utilities import remove_last_zero_row

DEFAULT_RECORD_TYPE_PRIORITY = [
    2,  # edited data
    1,  # raw data
    3,  # homogenized edited data
    4,  # homogenized raw data
    6,  # derived data
    7,  # estimated
    8,  # rough estimate
    5,  # set value
]
# services/record_resolution.py

def get_best_record_of_unit(
    *,
    run,
    unit_id,
    phenomenon_id=None,
    view_order=None,
    related_value_x_unit_id=None,
    related_value_y_unit_id=None,
    related_value_z_unit_id=None,
):
    """
    Returns the best record according to type priority and quality index.
    """

    view_order = view_order or DEFAULT_RECORD_TYPE_PRIORITY

    for record_type in view_order:
        records = run.get_records(
            unit_id,
            phenomenon_id,
            record_type,
            related_value_x_unit_id,
            related_value_y_unit_id,
            related_value_z_unit_id,
        )

        if not records:
            continue

        # quality index priority: lowest first, None last
        for qi in run.runoffdb.get_all_quality_indexes() + [None]:
            matches = [r for r in records if r.quality_index_id == qi]
            if matches:
                return matches[0]

    return None


def get_record_data(
    *,
    run,
    record,
    value_label,
    related_x_label=None,
    related_y_label=None,
    related_z_label=None,
    target_unit_id=None,
    remove_last_zero=False,
    return_trace: bool = False,
):
    """
    Load record data, enforce timeline, convert units if needed.

    Returns:
    - DataFrame
    - (DataFrame, DataTrace) if return_trace=True
    """
    import pandas as pd

    issue: DataIssue | None = None

    df = pd.DataFrame()

    try:
        df = record.get_data(
            value_label=value_label,
            related_x_label=related_x_label,
            related_y_label=related_y_label,
            related_z_label=related_z_label,
        )

        if df.empty:
            issue = DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                source="get_record_data",
                details=f"no data found for record {record.id} (requested as '{value_label}')",
            )
            return (df, issue) if return_trace else df


        # unit conversion
        if target_unit_id and record.unit_id != target_unit_id:
            before_non_na = df[value_label].notna().sum()
            try:
                df = convert_dataframe_units(
                    df=df,
                    source_unit_id=record.unit_id,
                    target_unit_id=target_unit_id,
                    value_column=value_label,
                    output_column=value_label,
                )
            except UnitConversionError as e:
                issue = DataIssue(
                    reason=DataAbsenceReason.UNIT_CONVERSION_FAILED,
                    source="get_record_data",
                    details=f"unit conversion failed: {str(e)}",
                )

            after_non_na = df[value_label].notna().sum()
            if after_non_na == 0 and before_non_na > 0:
                issue = DataIssue(
                    reason=DataAbsenceReason.UNIT_CONVERSION_FAILED,
                    source="get_record_data",
                    details=f"units conversion {record.unit_id} -> {target_unit_id} removed all values",
                )

    except Exception as exc:
        issue = DataIssue(
            reason=DataAbsenceReason.UNKNOWN,
            source="get_record_data",
            details=str(exc),
        )

    else:
        if remove_last_zero:
            df = remove_last_zero_row(df)

    return (df, issue) if return_trace else df


def get_best_initial_moisture_record(*, run, view_order=None):
    """
    Returns the best initial soil moisture record for a run.
    Preference:
      1) dedicated record assigned to the run
      2) fallback via get_best_record_of_unit
    """

    if run.initmoist_recid:
        return run.runoffdb.load_record_by_id(run.initmoist_recid)

    return get_best_record_of_unit(
        run=run,
        unit_id=SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,
        phenomenon_id=SOIL_MOISTURE_PHEN_ID,
        view_order=view_order,
    )

def get_initial_moisture_value(
    *,
    run,
    view_order=None,
    multi_value=False,
):
    """
    Returns initial soil moisture value(s) for a run.

    - If a single value exists → returns scalar
    - If multiple values exist:
        - multi_value=True  → returns list
        - multi_value=False → returns mean
    """

    record = get_best_initial_moisture_record(
        run=run,
        view_order=view_order,
    )

    if record is None:
        return None
    data = record.get_data(value_label="initial_moisture")

    if data is None:
        return None

    values = data["initial_moisture"]

    if len(values.index) == 1:
        return values.iloc[0]

    if multi_value:
        return values.tolist()

    return values.mean()

def get_best_surface_cover_record(*, run, return_trace: bool = False):
    """
    Resolves the best surface cover record for a run.

    Resolution order:
    1. dedicated surface cover record assigned to the run
    2. best available generic surface cover record
    """

    issues: list[DataIssue] = ()

    # 1. dedicated record
    rec_id = run.surface_cover_recid
    if rec_id:
        record = run.runoffdb.load_record_by_id(rec_id)
        if record:
            return (record, None) if return_trace else record
        else:
            issues.append(DataIssue(
                            reason=DataAbsenceReason.RECORD_NOT_ASSIGNED,
                            source="get_best_surface_cover_record",
                            details=f"dedicated surface cover record not set",
                        )
                    )
            return (None, issues) if return_trace else None
    else:
        # 2. fallback: try to get any surface cover record
        record = get_best_record_of_unit(
            run=run,
            unit_id=SURFACE_COVER_PERC_UNIT_ID,
        )

    if record:
        issues.append(DataIssue(
                reason=DataAbsenceReason.DEDICATION_MISSING,
                source="get_best_surface_cover_record",
                details=f"surface cover record found, but is not set as 'dedicated'",
            )
        )
        return (record, issues) if return_trace else record

    # fallback also failed
    issues.append(DataIssue(
                    reason=DataAbsenceReason.NO_RECORD,
                    source="get_best_surface_cover_record",
                    details=f"no surface cover record found",
                    )
                )

    return (None, tuple(issues)) if return_trace else None

def get_surface_cover_value(
    *,
    run: "Run",
    multi_value: bool = False,
    return_trace: bool = False,
):
    """
    Returns surface cover value for a run.

    Semantics:
    - if record contains a single value -> return that value
    - if multiple values:
        - multi_value=True  -> return list
        - multi_value=False -> return mean
    - if no record:
        - return 0 for no-cover crop
        - otherwise None
    """
    issues: list[DataIssue] = []
    record, sub_issues = get_best_surface_cover_record(run=run, return_trace=return_trace)

    if record:
        df = get_record_data(run=run, record=record, value_label="surface_cover")

        if len(df.index) == 1:
            return (df["surface_cover"].iloc[0], sub_issues) if return_trace else df["surface_cover"].iloc[0]

        if multi_value:
            if sub_issues:
                return df["surface_cover"].tolist(), sub_issues
            else:
                return df["surface_cover"].tolist()

        # the record has more values than one but single value was requested
        sub_issues.add(DataIssue(
                reason=DataAbsenceReason.DERIVED_MEAN,
                source="get_surface_cover_value",
                details=f"surface cover record has more than 1 value and single value was requested - mean value returned",
            )
        )
        return (df["surface_cover"].mean(), tuple(sub_issues)) if return_trace else df["surface_cover"].mean()

    elif run.crop:
        if run.crop.crop_type_id == NO_COVER_CROP_TYPE_ID:
            issue = DataIssue(
                reason=DataAbsenceReason.IMPLICIT_VALUE,
                source="days_since_last_operation",
                details=f"days since last operation implicitly assumed = 0 for 'cultivated fallow'",
            )
            return (0, (issue,)) if return_trace else 0

    return None

def get_crop_height_value(
    *,
    run: "Run",
    multi_value: bool = False,
    return_trace: bool = False,
):
    """
    Returns crop height value for a run.

    Resolution:
    - uses best available crop height record
    - returns mean value if multiple data points exist
    """

    record = get_best_record_of_unit(
        run=run,
        unit_id=CROP_HEIGHT_CM_UNIT_ID,
    )

    if record is None:
        return None

    df = record.get_data(value_label="crop_height")

    if df is None or df.empty:
        return None

    return df["crop_height"].mean()

def get_plant_density_value(
    *,
    run: "Run",
    multi_value: bool = False,
    return_trace: bool = False,
):
    """
    Returns plant density value for a run.

    Resolution:
    - returns mean value if multiple data points exist
    """

    record = get_best_record_of_unit(
        run=run,
        unit_id=CROP_DENSITY_M_2_UNIT_ID,
    )

    if record is None:
        return None

    df = record.get_data(value_label="plant_density")

    if df is None or df.empty:
        return None

    return df["plant_density"].mean()
