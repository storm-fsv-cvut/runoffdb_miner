from src.diagnostics.trace import DataTrace, DataIssue
from src.diagnostics.absence_reasons import DataAbsenceReason

from src.setup.entity_ids import *
from src.services.record_resolution import *

def get_plant_density_value(
    *,
    run: "Run",
    multi_value: bool = False,
    return_trace: bool = False,
):

    issues: list[DataIssue] = []

    # for the cultivated fallow always return None
    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
        issue = (DataIssue(
            reason=DataAbsenceReason.INVALID_REQUEST,
            source="get_plant_density_value",
            details=f"plant density irrelevant for 'cultivated fallow'",
        ),)
        return (None, issue) if return_trace else None

    record = get_best_record_of_unit(
        owner=run,
        unit_id=CROP_DENSITY_M_2_UNIT_ID,
    )

    if not record:
        issue = DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            source="get_plant_density_value",
            details=(
                f"no plant density record found"
            ),
        )
        return (None, (issue, )) if return_trace else None

    value, sub_issues = get_record_scalar_value(
        record=record,
        value_label="crop_density",
        multi_value=multi_value,
        return_trace=return_trace,
    )

    if sub_issues:
        issues.extend(sub_issues)

    return (value, tuple(issues)) if return_trace else value


def get_crop_height_value(
    *,
    run: "Run",
    multi_value: bool = False,
    return_trace: bool = False,
):
    issues: list[DataIssue] = []

    # for the cultivated fallow always return None
    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
        issue = (DataIssue(
            reason=DataAbsenceReason.INVALID_REQUEST,
            source="get_crop_height_value",
            details=f"crop height irrelevant for 'cultivated fallow'",
        ),)
        return (None, issue) if return_trace else None

    record = get_best_record_of_unit(
        owner=run,
        unit_id=CROP_HEIGHT_UNITS,
    )

    if not record:
        issue = DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            source="get_crop_height_value",
            details=f"no crop height record found",
        )
        return (None, (issue,)) if return_trace else None

    value, sub_issues = get_record_scalar_value(
        record=record,
        target_unit_id=CROP_HEIGHT_CM_UNIT_ID,
        value_label="crop_height",
        multi_value=multi_value,
        return_trace=return_trace,
    )

    if sub_issues:
        issues.extend(sub_issues)

    return (value, tuple(issues)) if return_trace else value


def get_surface_cover_value(
    *,
    run: "Run",
    multi_value: bool = False,
    return_trace: bool = False,
):
    issues: list[DataIssue] = []

    # for the cultivated fallow always return None
    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
        issues.append(DataIssue(
            reason=DataAbsenceReason.INVALID_REQUEST,
            source="get_surface_cover_value",
            details=f"surface cover irrelevant for 'cultivated fallow'",
        ))
        return (0, tuple(issues)) if return_trace else 0

    # get the best record for the unit or units list
    record, rec_issues = resolve_dedicated_or_generic_record(
        owner=run,
        dedicated_recid_attr="surface_cover_recid",
        unit_id=SURFACE_COVER_PERC_UNIT_ID,
        return_trace=return_trace,
    )

    if not record:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            source="get_surface_cover_value",
            details=f"no surface cover record found",
            causes=rec_issues
            )
        )
        return (None, tuple(issues)) if return_trace else None

    if rec_issues:
        issues.extend(rec_issues)

    value, sub_issues = get_record_scalar_value(
        record=record,
        target_unit_id=SURFACE_COVER_PERC_UNIT_ID,
        value_label="surface_cover",
        multi_value=multi_value,
        return_trace=return_trace,
    )

    if sub_issues:
        issues.extend(sub_issues)

    return (value, tuple(issues)) if return_trace else value

