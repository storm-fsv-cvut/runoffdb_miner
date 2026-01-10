from .unit_conversion import convert_dataframe_units
from src.setup.unit_ids import *
from src.setup.entity_ids import *
from src.exceptions import DataframeEmptyError

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
    target_unit_id,
    demand_timeline=False
):
    """
    Load record data, enforce timeline, convert units if needed.
    """
    df = record.get_data(
        value_label=value_label,
        demand_timeline=demand_timeline,
    )

    if df is None or df.empty:
        return df

    if target_unit_id and record.unit_id != target_unit_id:
        df = convert_dataframe_units(
            df=df,
            source_unit_id=record.unit_id,
            target_unit_id=target_unit_id,
            value_column=value_label,
            output_column=value_label,
        )

    return df


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
    try:
        data = record.get_data(value_label="initial_moisture")
    except DataframeEmptyError:
        return None

    if data is None:
        return None

    values = data["initial_moisture"]

    if len(values.index) == 1:
        return values.iloc[0]

    if multi_value:
        return values.tolist()

    return values.mean()

def get_best_surface_cover_record(*, run):
    """
    Resolves the best surface cover record for a run.

    Resolution order:
    1. dedicated surface cover record assigned to the run
    2. best available generic surface cover record
    """

    # 1. dedicated record
    rec_id = getattr(run, "surface_cover_recid", None)
    if rec_id:
        record = run.runoffdb.load_record_by_id(rec_id)
        if record:
            return record

    # 2. fallback: generic surface cover record
    return get_best_record_of_unit(
        run=run,
        unit_id=SURFACE_COVER_PERC_UNIT_ID,
    )

def get_surface_cover_value(
    *,
    run,
    multi_value: bool = False,
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

    record = get_best_surface_cover_record(run=run)

    if record:
        try:
            df = record.get_data(value_label="surface_cover")
        except DataframeEmptyError:
            return None

        if len(df.index) == 1:
            return df["surface_cover"].iloc[0]

        if multi_value:
            return df["surface_cover"].tolist()

        return df["surface_cover"].mean()

    # fallback: derive from crop type
    crop = getattr(run, "crop", None)
    if crop and crop.crop_type_id == NO_COVER_CROP_TYPE_ID:
        return 0

    return None

def get_crop_height_value(*, run):
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

    try:
        df = record.get_data(value_label="crop_height")
    except DataframeEmptyError:
        return None

    if df is None or df.empty:
        return None

    return df["crop_height"].mean()

def get_plant_density_value(*, run):
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

    try:
        df = record.get_data(value_label="plant_density")
    except DataframeEmptyError:
        return None

    if df is None or df.empty:
        return None

    return df["plant_density"].mean()
