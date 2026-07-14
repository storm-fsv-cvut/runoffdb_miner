from typing import Iterable, Optional
import pandas as pd

from src.setup.unit_ids import *
from src.services.record_resolution import get_best_record_of_unit, get_record_data, get_record_scalar_value, record_matches_units
from src.entities.record import *
from src.entities.soil_sample import SoilSample

from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.trace import DataTrace, create_trace
from src.diagnostics.issue import DataIssue
from src.diagnostics.severity import IssueSeverity


def get_best_soil_dedicated_record(
    *,
    run: "Run",
    dedicated_ss_attr: str,
    dedicated_rec_attr: str,
    allowed_units: int | list[int],
    phenomenon_id: int,
):

    root_trace = create_trace(
        source="get_best_soil_dedicated_record",
        owner=run,
        variable=f"{dedicated_ss_attr}-{dedicated_rec_attr}",
    )

    if not isinstance(allowed_units, (list, tuple, set)):
        allowed_units = [allowed_units]

    # --------------------------------------------------
    # dedicated soil sample
    # --------------------------------------------------

    ss_id = getattr(run, dedicated_ss_attr, None)

    if ss_id is not None:

        ss = run.runoffdb.get_soil_samples_by_id(ss_id)

        if not isinstance(ss, SoilSample):

            root_trace.success = False
            root_trace.details = "soil record resolution failed",
            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.REFERENCED_ENTITY_NOT_FOUND,
                    details=f"soil sample #{ss_id} referenced by {dedicated_ss_attr} was not found",
                    severity=IssueSeverity.ERROR
                )
            )

            return None, root_trace

        rec_id = getattr(ss, dedicated_rec_attr, None)

        if rec_id:
            record = run.runoffdb.load_record_by_id(rec_id)

            if not record:
                root_trace.success = False
                root_trace.details = "soil record resolution failed",
                root_trace.issues.append(
                    DataIssue(
                        reason=DataAbsenceReason.REFERENCED_RECORD_NOT_FOUND,
                        severity=IssueSeverity.ERROR,
                        details=f"record #{rec_id} referenced by {dedicated_rec_attr} was not found"
                    )
                )

                return None, root_trace

            if record.phenomenon_id != phenomenon_id:
                root_trace.success = False
                root_trace.details = "soil record resolution failed",
                root_trace.issues.append(
                    DataIssue(
                        reason=DataAbsenceReason.INVALID_RECORD_TYPE,
                        details=f"record {record.id} has phenomenon {record.phenomenon_id}, expected {phenomenon_id}"
                        )
                    )

                return None, root_trace

            if not record_matches_units(
                record,
                allowed_unit_ids=allowed_units,
            ):

                root_trace.success = False
                root_trace.details = "soil record resolution failed",
                root_trace.issues.append(
                    DataIssue(
                        reason=DataAbsenceReason.INCOMPATIBLE_UNIT_SET,
                        details=f"record #{record.id} has incompatible units",
                    )
                )

                return None, root_trace

            root_trace.details = "dedicated soil record selected"
            return record, root_trace

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.RECORD_NOT_ASSIGNED,
                details=f"SoilSample {ss_id} has no {dedicated_rec_attr}",
                severity=IssueSeverity.INFO
            )
        )

    # --------------------------------------------------
    # generic fallback
    # --------------------------------------------------

    samples = run.runoffdb.get_soil_samples_of_run(run)

    for ss in samples:

        record, child_trace = get_best_record_of_unit(
            owner=ss,
            unit_id=allowed_units,
            phenomenon_id=phenomenon_id,
        )

        if child_trace:
            root_trace.traces.append(child_trace)

        if not record:
            continue

        if not record_matches_units(record, allowed_unit_ids=allowed_units):
            continue

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.DEDICATION_MISSING,
                details="soil property record found on non-dedicated soil sample",
                severity=IssueSeverity.INFO
                ),
            )

        root_trace.details = "generic soil record selected"

        return record, root_trace

    # --------------------------------------------------
    # nothing found
    # --------------------------------------------------

    root_trace.success = False
    root_trace.details = "soil record resolution failed",
    root_trace.issues.append(
        DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            details="no suitable soil record found",
            severity=IssueSeverity.INFO
        )
    )

    return None, root_trace


def get_best_soil_texture_record(
    *,
    run: "Run",
):
    return get_best_soil_dedicated_record(
        run=run,
        dedicated_ss_attr="texture_ss_id",
        dedicated_rec_attr="texture_record_id",
        allowed_units=[
            CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID,
            PARTICLE_SIZE_THRESHOLD_MM_UNIT_ID,
        ],
        phenomenon_id=PARTICLE_SIZE_DISTRIBUTION_PHEN_ID,
    )


def get_best_soil_texture_data(
    *,
    run,
    x_label: str = "cumulative_mass_content",
    y_label: str = "particle_size",
    order_by: Optional[str] = "particle_size",
    limits: Optional[Iterable[float]] = None,
    return_int: bool = False,
    return_cumulative: bool = False,
):

    root_trace = create_trace(
        source="get_best_soil_texture_data",
        owner=run,
        variable="soil_texture",
        details="soil texture resolution",
    )

    order_by = order_by or DB_2ND_DIM_VALUE_COLUMN

    # --------------------------------------------------
    # resolve record
    # --------------------------------------------------

    texture_record, record_trace = get_best_soil_texture_record(
        run=run,
    )

    root_trace.traces.append(record_trace)


    if texture_record is None:

        root_trace.success = False
        root_trace.details = "no soil texture record found"

        return None, root_trace

    # --------------------------------------------------
    # load data
    # --------------------------------------------------

    df, data_trace = get_record_data(
        record=texture_record,
        target_unit_id=CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID,
        value_label=x_label,
        related_x_label=y_label,
        order_by=order_by,
    )

    record_trace.traces.append(data_trace)

    if df is None or df.empty:

        root_trace.success = False
        root_trace.details="soil texture resolution failed",
        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                details=f"soil texture record #{texture_record.id} contains no data",
                severity=IssueSeverity.ERROR
            )
        )

        return None, root_trace

    # --------------------------------------------------
    # interpolation
    # --------------------------------------------------
    if limits:

        df, interpolation_trace = interpolate_texture(
            original_texture=df,
            new_limits=limits,
            cum_mass_col_name=x_label,
            return_int=return_int,
            return_cumulative=return_cumulative,
        )

        data_trace.traces.append(interpolation_trace)

    return df, root_trace


def get_best_bulk_density_value(
    *,
    run: "Run",
    target_unit_id: int | None = None,
    multi_value: bool = False,
):

    root_trace = create_trace(
        source="get_best_bulk_density_value",
        owner=run,
        variable="bulk_density",
        details="bulk density resolution",
    )

    record, rec_trace = get_best_soil_dedicated_record(
        run=run,
        dedicated_ss_attr="bulkd_ss_id",
        dedicated_rec_attr="bulk_density_id",
        allowed_units=BULK_DENSITY_UNITS,
        phenomenon_id=PHYSICAL_SOIL_PROPERTIES_PHEN_ID,
    )

    root_trace.traces.append(rec_trace)

    if not record:
        root_trace.success = False
        root_trace.details = "no bulk density record found"
        return None, root_trace


    value, value_trace = get_record_scalar_value(
        record=record,
        target_unit_id=target_unit_id,
        value_label="bulk_density",
        multi_value=multi_value,
    )

    rec_trace.traces.append(value_trace)

    if value is None:
        root_trace.success = False

    return value, root_trace

def interpolate_texture(
    *,
    original_texture,
    new_limits,
    cum_mass_col_name,
    return_cumulative=True,
    return_int=True,
    smallest_content=1,
):

    import pandas as pd

    root_trace = create_trace(
        source="interpolate_texture",
        variable="soil_texture",
    )

    # --------------------------------------------------
    # validate input
    # --------------------------------------------------

    if not isinstance(original_texture, pd.DataFrame):

        root_trace.success = False
        root_trace.details = "texture interpolation failed",
        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.INVALID_VALUES,
                details="input texture is not a pandas DataFrame",
                severity=IssueSeverity.ERROR
            )
        )

        return None, root_trace

    # --------------------------------------------------
    # interpolation
    # --------------------------------------------------

    particle_size_col = original_texture.index.name

    original_limits = original_texture.index.to_list()
    original_contents = original_texture[cum_mass_col_name].to_list()

    original_limits.insert(0, 0)
    original_contents.insert(0, smallest_content)

    new_limits = sorted(new_limits)

    cumul_contents = []

    for nl in new_limits:

        prev_ol = None
        prev_content = None

        for ol, content in zip(
            original_limits,
            original_contents,
        ):

            if prev_ol is not None:

                if prev_ol < nl <= ol:

                    new_value = (
                        prev_content + ((content - prev_content) / (ol - prev_ol)) * (nl - prev_ol)
                    )

                    cumul_contents.append(new_value)
                    break

            prev_ol = ol
            prev_content = content

    if return_int:
        cumul_contents = [round(v) for v in cumul_contents ]

    if not return_cumulative:

        output_contents = [cumul_contents[0]]

        for i in range(1, len(cumul_contents)):
            output_contents.append(cumul_contents[i] - cumul_contents[i - 1])

    else:
        output_contents = cumul_contents

    output_df = pd.DataFrame(
        {
            particle_size_col: new_limits,
            cum_mass_col_name: output_contents,
        }
    )

    output_df.set_index(
        particle_size_col,
        inplace=True,
    )

    root_trace.details = (
        f"interpolated into {len(new_limits)} particle size limits [{', '.join([str(l) for l in new_limits])}]"
    )

    # trace.metadata = {
    #     "return_cumulative": return_cumulative,
    #     "return_int": return_int,
    #     "smallest_content": smallest_content,
    # }

    return output_df, root_trace
