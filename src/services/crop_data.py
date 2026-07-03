from src.diagnostics.report_collector import ReportCollector

from src.setup.entity_ids import *
from src.services.record_resolution import *

def get_plant_density_value(
    *,
    run: "Run",
    multi_value: bool = False,
) -> tuple[float | None, DataTrace]:

    root_trace = create_trace(
            variable="plant_density",
            source="get_plant_density_value",
            owner=run,
        )

    # for the cultivated fallow always return None
    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
        root_trace.details = f"plant density irrelevant for 'cultivated fallow'"

        return None, root_trace

    record, rec_trace = get_best_record_of_unit(
        owner=run,
        unit_id=CROP_DENSITY_M_2_UNIT_ID,
    )

    if not record:
        root_trace.success = False
        root_trace.traces.append(rec_trace)
        root_trace.details = "no plant density record found"

        return None, root_trace

    value, sv_trace = get_record_scalar_value(
        record=record,
        target_unit_id=CROP_DENSITY_M_2_UNIT_ID,
        value_label="crop_density",
        multi_value=multi_value,
    )
    if sv_trace:
        rec_trace.traces.append(sv_trace)

    root_trace.traces.append(sv_trace)

    return value, root_trace


def get_crop_height_value(
    *,
    run: "Run",
    multi_value: bool = False,
) -> tuple[float | None, DataTrace]:

    # for the cultivated fallow always return None
    root_trace = create_trace(
        variable="crop_height",
        source="get_crop_height_value",
        owner=run,
    )
    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
        root_trace.details = f"crop height irrelevant for 'cultivated fallow'"
        return None, root_trace

    record, rec_trace = get_best_record_of_unit(
        owner=run,
        unit_id=CROP_HEIGHT_UNITS,
    )

    if not record:
        root_trace.success = False
        root_trace.traces.append(rec_trace)
        root_trace.details = "no crop height record found"

        return None, root_trace


    value, sv_trace = get_record_scalar_value(
        record=record,
        target_unit_id=CROP_HEIGHT_CM_UNIT_ID,
        value_label="crop_height",
        multi_value=multi_value,
    )
    if sv_trace:
        rec_trace.traces.append(sv_trace)

    root_trace.traces.append(sv_trace)

    return value, root_trace


def get_surface_cover_value(
    *,
    run: "Run",
    multi_value: bool = False,
) -> tuple[float | None, DataTrace]:

    # for the cultivated fallow always return 0
    root_trace = create_trace(
        variable="surface_cover",
        source="get_surface_cover_value",
        owner=run,
    )
    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
        root_trace.details = f"surface cover assumed 0 'cultivated fallow'"
        root_trace.issues.append(DataIssue(
            reason=DataAbsenceReason.IMPLICIT_VALUE,
            severity=IssueSeverity.INFO
        ))
        return 0, root_trace

    # get the best record for the unit or units list
    record, rec_trace = resolve_dedicated_or_generic_record(
        owner=run,
        dedicated_recid_attr="surface_cover_recid",
        unit_id=SURFACE_COVER_PERC_UNIT_ID,
    )

    if not record:
        root_trace.success = False,
        root_trace.details = "no surface cover record found"
        root_trace.traces.append(rec_trace)
        return None, root_trace

    value, sv_trace = get_record_scalar_value(
        record=record,
        target_unit_id=SURFACE_COVER_PERC_UNIT_ID,
        value_label="surface_cover",
        multi_value=multi_value,
    )

    if sv_trace:
        rec_trace.traces.append(sv_trace)

    root_trace.traces.append(rec_trace)

    return value, root_trace

