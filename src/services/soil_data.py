from typing import Iterable, Optional
import pandas as pd

from src.setup.unit_ids import *
from src.services.record_resolution import get_best_record_of_unit, get_record_data
from src.entities.record import *


# public API
def get_best_bulk_density_record(*, run) -> Optional["Record"]:
    """
    Resolve the best available bulk density record for a run.

    Resolution order:
    1. Dedicated bulk density soil sample record (if assigned)
    2. Best available bulk density record by unit / phenomenon
    """

    # 1. dedicated bulk density soil sample
    bulkd_ss = getattr(run, "bulkd_ss", None)

    if bulkd_ss is not None:
        record_id = getattr(bulkd_ss, "bulk_density_id", None)
        if record_id is not None:
            record = run.runoffdb.load_record_by_id(record_id)
            if record is not None:
                return record
            return None

        return None

    # 2. fallback: any bulk density record
    return get_best_record_of_unit(
        run=run,
        unit_id=[BULK_DENSITY_GCM_UNIT_ID, BULK_DENSITY_KGM_UNIT_ID],
        phenomenon_id=PHYSICAL_SOIL_PROPERTIES_PHEN_ID,
    )

def get_best_bulk_density_value(
    *,
    run,
    target_unit_id: Optional[int] = None,
    label: str = "bulk_density",
    return_trace: bool = False
) -> Optional[float]:
    """
    Return mean bulk density value for the run, if available.
    """

    record = get_best_bulk_density_record(run=run)
    if record is None:
        return None

    df = get_record_data(
        run=run,
        record=record,
        value_label=label,
        target_unit_id=target_unit_id,
    )

    if df is None or df.empty:
        return None

    return df[label].mean()


def get_best_soil_texture_record(*, run):
    """
    Resolves the best soil texture record for a run.

    Resolution order:
    1. dedicated soil texture sample record
    2. best available generic texture record
    """

    # 1. dedicated texture soil sample
    ss = getattr(run, "texture_ss", None)
    if ss and ss.texture_record_id:
        record = run.runoffdb.load_record_by_id(ss.texture_record_id)
        if record:
            return record

    # 2. fallback: generic texture record
    try:
        return get_best_record_of_unit(run=run, unit_id=CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID, phenomenon_id=PHYSICAL_SOIL_PROPERTIES_PHEN_ID)
    except Exception:
        return None


def get_best_soil_texture_data(
    *,
    run,
    x_label: str = "cumulative_mass_content",
    y_label: str = "particle_size",
    order_by: Optional[str] = None,
    limits: Optional[Iterable[float]] = None,
    return_int: bool = False,
    return_cumulative: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Returns best available soil texture data for a run.

    - prefers dedicated texture sample
    - falls back to best generic texture record
    - optionally interpolates to given limits
    """

    order_by = order_by or DB_2ND_DIM_VALUE_COLUMN

    texture_record = get_best_soil_texture_record(run=run)
    if texture_record is None:
        return None

    df = texture_record.get_data(
        value_label=x_label,
        related_x=y_label,
        index_column=y_label,
        order_by=order_by,
    )

    if df is None or df.empty:
        return None

    if limits:
        df = interpolate_texture(
            df,
            limits,
            x_label,
            return_int=return_int,
            return_cumulative=return_cumulative,
        )

    return df


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
