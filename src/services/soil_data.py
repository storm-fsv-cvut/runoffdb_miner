from typing import Iterable, Optional
import pandas as pd

from src.setup.unit_ids import *
from src.services.record_resolution import get_best_record_of_unit, get_record_data, get_record_scalar_value, record_matches_units
from src.entities.record import *
from src.entities.soil_sample import SoilSample

from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.trace import DataIssue

# public API

def get_best_soil_dedicated_record(
    *,
    run: "Run",
    dedicated_ss_attr: str,
    dedicated_rec_attr: str,
    allowed_units: int | list[int],
    phenomenon_id: int,
    return_trace: bool = False,
):
    """
    Resolve the best soil-related record for a run using a dedicated soil sample,
    with fallback to any assigned soil sample.

    Resolution order:
    1. Dedicated soil sample on run (e.g. run.texture_ss)
       1.a sample reference invalid
       1.b dedicated record not assigned
       1.c record missing / wrong phenomenon / incompatible units
       1.d valid dedicated record returned
    2. Fallback: any soil sample assigned to run
       2.a no suitable record found
       2.b generic record found (reported as non-dedicated)
    """

    issues: list[DataIssue] = []

    # normalize allowed units
    if not isinstance(allowed_units, (list, tuple, set)):
        allowed_units = [allowed_units]

    # --------------------------------------------------
    # 1. dedicated soil sample path
    # --------------------------------------------------
    ss = getattr(run, dedicated_ss_attr, None)

    if ss is not None:

        # 1.a invalid soil sample reference
        if not isinstance(ss, SoilSample):
            issues.append(DataIssue(
                reason=DataAbsenceReason.DATABASE_RECORD_INVALID,
                source="get_best_soil_dedicated_record",
                details=(
                    f"run.{dedicated_ss_attr} references a non-existent SoilSample ID {ss}"
                ),
            ))
            return (None, tuple(issues)) if return_trace else None

        # 1.b dedicated record ID on soil sample
        rec_id = getattr(ss, dedicated_rec_attr, None)
        if not rec_id:
            issues.append(DataIssue(
                reason=DataAbsenceReason.RECORD_NOT_ASSIGNED,
                source="get_best_soil_dedicated_record",
                details=(
                    f"dedicated SoilSample '{dedicated_ss_attr}' does not have appropriate dedicated record assigned"
                    f"(attribute {dedicated_rec_attr})"
                ),
            ))
        else:
            record = run.runoffdb.load_record_by_id(rec_id)

            # 1.c.1 record missing
            if not record:
                issues.append(DataIssue(
                    reason=DataAbsenceReason.RECORD_NOT_FOUND,
                    source="get_best_soil_dedicated_record",
                    details=(
                        f"SoilSample.{dedicated_rec_attr}={rec_id} "
                        f"but record not found"
                    ),
                ))
                return (None, tuple(issues)) if return_trace else None

            # 1.c.2 wrong phenomenon
            if record.phenomenon_id != phenomenon_id:
                issues.append(DataIssue(
                    reason=DataAbsenceReason.INVALID_RECORD_TYPE,
                    source="get_best_soil_dedicated_record",
                    details=(
                        f"record ID {record.id} has phenomenon "
                        f"{record.phenomenon_id}, expected {phenomenon_id}"
                    ),
                ))
                return (None, tuple(issues)) if return_trace else None

            # 1.c.3 incompatible units
            if not record_matches_units(
                record,
                allowed_unit_ids=allowed_units,
            ):
                issues.append(DataIssue(
                    reason=DataAbsenceReason.INCOMPATIBLE_UNIT_SET,
                    source="get_best_soil_dedicated_record",
                    details=(
                        f"record ID {record.id} has incompatible units "
                        f"(unit_id={record.unit_id}, "
                        f"x={record.related_value_x_unit_id}, "
                        f"y={record.related_value_y_unit_id}, "
                        f"z={record.related_value_z_unit_id})"
                    ),
                ))
                return (None, tuple(issues)) if return_trace else None

            # 1.d success
            return (record, None) if return_trace else record

    # --------------------------------------------------
    # 2. generic fallback via all soil samples
    # --------------------------------------------------
    samples = run.runoffdb.get_soil_samples_of_run(run)

    for ss in samples:
        record = ss.get_best_record_of_unit(
            unit_id=allowed_units,
            phenomenon_id=phenomenon_id,
        )

        if not record:
            continue

        if not record_matches_units(
            record,
            allowed_unit_ids=allowed_units,
        ):
            continue

        issues.append(DataIssue(
            reason=DataAbsenceReason.DEDICATION_MISSING,
            source="get_best_soil_dedicated_record",
            details=(
                "soil property record found on non-dedicated soil sample"
            ),
        ))

        return (record, tuple(issues)) if return_trace else record

    # --------------------------------------------------
    # 3. nothing found
    # --------------------------------------------------
    return (None, tuple(issues)) if return_trace else None


def get_best_soil_texture_record(
    *,
    run: "Run",
    return_trace: bool = False,
):
    return get_best_soil_dedicated_record(
        run=run,
        dedicated_ss_attr="texture_ss",
        dedicated_rec_attr="texture_recid",
        allowed_units=[
            CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID,
            PARTICLE_SIZE_THRESHOLD_MM_UNIT_ID,
        ],
        phenomenon_id=PHYSICAL_SOIL_PROPERTIES_PHEN_ID,
        return_trace=return_trace,
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
    return_trace: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Returns best available soil texture data for a run.

    - prefers dedicated texture sample
    - falls back to best generic texture record
    - optionally interpolates to given limits
    """

    order_by = order_by or DB_2ND_DIM_VALUE_COLUMN
    issues: list[DataIssue] = []

    texture_record, sub_issues = get_best_soil_texture_record(run=run, return_trace=return_trace)


    if texture_record is None:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            source="get_best_soil_texture_data",
            details="No soil texture record found for run and related soil samples",
            causes=sub_issues
        ))
        return (None, tuple(issues)) if return_trace else None

    if sub_issues:
        issues.extend(sub_issues)

    df = texture_record.get_data(
        value_label=x_label,
        related_x=y_label,
        index_column=y_label,
        order_by=order_by,
    )

    if df is None or df.empty:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_DATA_IN_RECORD,
            source="get_best_soil_texture_data",
            details=f"soil texture record ID {texture_record.id} contains no data",
        ))
        return (None, tuple(issues)) if return_trace else None

    if limits:
        df = interpolate_texture(
            df,
            limits,
            x_label,
            return_int=return_int,
            return_cumulative=return_cumulative,
        )

    return df


def get_best_bulk_density_value(
    *,
    run: "Run",
    target_unit_id = None,
    multi_value: bool = False,
    return_trace: bool = False,
):
    issues: list[DataIssue] = []

    record, sub_issues = get_best_soil_dedicated_record(
        run=run,
        dedicated_ss_attr="bulkd_ss_id",
        dedicated_rec_attr="bulk_density_id",
        allowed_units=BULK_DENSITY_UNITS,
        phenomenon_id=PHYSICAL_SOIL_PROPERTIES_PHEN_ID,
        return_trace=return_trace,
    )
    if not record:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            source="get_best_bulk_density_value",
            details="No bulk density record found for run and related soil samples",
            causes=sub_issues
        ))
        return (None, tuple(issues)) if return_trace else None

    if sub_issues:
        issues.extend(sub_issues)

    value, sub_issues = get_record_scalar_value(
        record=record,
        target_unit_id=target_unit_id,
        value_label="bulk_density",
        source="get_best_bulk_density_value",
        multi_value=multi_value,
        return_trace=return_trace,
    )

    if sub_issues:
        issues.extend(sub_issues)

    return (value, tuple(issues)) if return_trace else value

def interpolate_texture(original_texture, new_limits, cum_mass_col_name, return_cumulative=True, return_int=True, smallest_content=1):
    import pandas as pd

    # ensure original_texture is a pandas DataFrame
    if not isinstance(original_texture, pd.DataFrame):
        raise TypeError("original_texture parameter value must be a pandas DataFrame")

    # get column names from the input DataFrame
    particle_size_col = original_texture.index.name

    # extract original limits and cumulative contents from the DataFrame
    original_limits = original_texture.index.to_list()
    original_contents = original_texture[cum_mass_col_name].to_list()

    # insert artificial first datapoint with the smallest content to allow for interpolation of smaller particles content
    original_limits.insert(0, 0)
    original_contents.insert(0, smallest_content)

    # sort the new limits
    new_limits = sorted(new_limits)

    cumul_contents = []


    for nl in new_limits:
        i = 0
        for ol, content in zip(original_limits, original_contents):
            if i == 0:
                prev_ol = ol
                prev_content = content
            else:
                if nl > prev_ol and nl <= ol:
                    new_value = prev_content + ((content - prev_content) / (ol - prev_ol)) * (nl - prev_ol)
                    cumul_contents.append(new_value)
                prev_ol = ol
                prev_content = content
            i += 1

    # round the content values to integers if requested
    if return_int:
        cumul_contents = [round(val) for val in cumul_contents]

    # recalculate cumulative values to net values if requested
    if not return_cumulative:
        net_contents = [cumul_contents[0]]
        for j in range(1, len(cumul_contents)):
            net_contents.append(cumul_contents[j] - cumul_contents[j - 1])
        output_contents = net_contents
    else:
        output_contents = cumul_contents

    # create the output DataFrame with the same column names as the input DataFrame

    output_df = pd.DataFrame({
        particle_size_col: new_limits,
        cum_mass_col_name: output_contents
    })

    # set particle_size as the index
    output_df.set_index(particle_size_col, inplace=True)

    return output_df
