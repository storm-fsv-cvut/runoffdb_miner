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
    particle_size_col="particle_size",
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
        root_trace.details = "texture interpolation failed"

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.INVALID_VALUES,
                details="input texture is not a pandas DataFrame",
                severity=IssueSeverity.ERROR,
            )
        )

        return None, root_trace

    # --------------------------------------------------
    # prepare dataframe
    # --------------------------------------------------

    df = original_texture.copy()

    if df.index.name != particle_size_col:

        if particle_size_col not in df.columns:

            root_trace.success = False
            root_trace.details = "texture interpolation failed"

            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.INVALID_VALUES,
                    details=f"particle size column '{particle_size_col}' not found",
                    severity=IssueSeverity.ERROR,
                )
            )

            return None, root_trace

        df.set_index(particle_size_col, inplace=True)

    df.sort_index(inplace=True)

    # --------------------------------------------------
    # report and remove missing values
    # --------------------------------------------------

    missing = df[df[cum_mass_col_name].isna()]

    for particle_size in missing.index:

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                details=f"missing cumulative mass content for particle size {particle_size}",
                severity=IssueSeverity.WARNING,
            )
        )

    df = df.dropna(subset=[cum_mass_col_name])

    if len(df) < 2:

        root_trace.success = False
        root_trace.details = "texture interpolation failed"

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.MISSING_DATA,
                details="at least two valid texture points are required",
                severity=IssueSeverity.ERROR,
            )
        )

        return None, root_trace

    # --------------------------------------------------
    # validate cumulative curve
    # --------------------------------------------------

    previous = None

    for particle_size, value in df[cum_mass_col_name].items():

        if not (0 <= value <= 100):

            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.INVALID_VALUES,
                    details=(
                        f"cumulative mass content {value} at particle size "
                        f"{particle_size} is outside the expected range <0,100>"
                    ),
                    severity=IssueSeverity.WARNING,
                )
            )

        if previous is not None and value < previous:

            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.INVALID_VALUES,
                    details=(
                        f"cumulative mass content decreases from "
                        f"{previous} to {value}"
                    ),
                    severity=IssueSeverity.WARNING,
                )
            )

        previous = value

    # --------------------------------------------------
    # interpolation
    # --------------------------------------------------

    original_limits = [0.0] + df.index.to_list()
    original_contents = [smallest_content] + df[cum_mass_col_name].to_list()

    new_limits = sorted(new_limits)

    min_limit = original_limits[1]
    max_limit = original_limits[-1]

    cumul_contents = []

    for nl in new_limits:

        if nl < min_limit or nl > max_limit:

            cumul_contents.append(pd.NA)

            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.NO_DATA_IN_RANGE,
                    details=(
                        f"cannot interpolate particle size {nl}; "
                        f"available range is {min_limit}–{max_limit}"
                    ),
                    severity=IssueSeverity.WARNING,
                )
            )

            continue

        interpolated = pd.NA

        for prev_limit, limit, prev_content, content in zip(
            original_limits[:-1],
            original_limits[1:],
            original_contents[:-1],
            original_contents[1:],
        ):

            if prev_limit < nl <= limit:

                interpolated = (
                    prev_content
                    + (content - prev_content)
                    * (nl - prev_limit)
                    / (limit - prev_limit)
                )

                break

        cumul_contents.append(interpolated)

    # --------------------------------------------------
    # cumulative -> interval
    # --------------------------------------------------

    if return_cumulative:

        output_contents = cumul_contents

    else:

        output_contents = []

        previous = 0

        for value in cumul_contents:

            if pd.isna(value):

                output_contents.append(pd.NA)

            else:

                output_contents.append(value - previous)
                previous = value

    # --------------------------------------------------
    # rounding
    # --------------------------------------------------

    if return_int:

        output_contents = [
            round(v) if not pd.isna(v) else pd.NA
            for v in output_contents
        ]

    # --------------------------------------------------
    # output
    # --------------------------------------------------

    output_df = pd.DataFrame(
        {
            particle_size_col: new_limits,
            cum_mass_col_name: output_contents,
        }
    ).set_index(particle_size_col)

    root_trace.details = (
        f"interpolated into {len(new_limits)} particle size limits "
        f"[{', '.join(map(str, new_limits))}]"
    )

    return output_df, root_trace
