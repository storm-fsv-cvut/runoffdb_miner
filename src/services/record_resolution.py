from .unit_conversion import convert_dataframe_units, UnitConversionError
from src.setup.unit_ids import *
from src.setup.entity_ids import *

from src.entities.data_owners import RecordOwner
from src.entities.record import Record
from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.issue import DataIssue
from src.diagnostics.trace import DataTrace, create_trace
from src.diagnostics.severity import IssueSeverity
from src.utilities.utilities import remove_last_zero_row
from src.services.record_type_priorities import DEFAULT_RECORD_TYPE_PRIORITY


def get_best_record_of_unit(
    *,
    owner: RecordOwner,
    unit_id: int | list[int] | None = None,
    phenomenon_id: int | None = None,
    view_order: list | None = None,
) -> tuple[Record | None, DataTrace]:

    view_order = view_order or DEFAULT_RECORD_TYPE_PRIORITY

    units = owner.runoffdb.units
    phenomena = owner.runoffdb.phenomena

    # get units names and units for the trace
    if isinstance(unit_id, list) or isinstance(unit_id, tuple):
        units_string = "/ ".join([str(units[uid]) for uid in unit_id])
    else:
        units_string = str(units[unit_id])

    #
    if phenomenon_id:
        phenomenon_string = f" ({phenomena[phenomenon_id]})"
    else:
        phenomenon_string = f" (phenomenon not specified)"

    trace = create_trace(
        source="get_best_record_of_unit",
        owner=owner,
        details=f"requested: {units_string}{phenomenon_string}",
    )

    for record_type in view_order:
        records = owner.get_records(
            unit_id,
            phenomenon_id,
            record_type,
        )

        if not records:
            continue

        for qi in owner.runoffdb.get_all_quality_indexes() + [None]:

            matches = [r for r in records if r.quality_index_id == qi]

            if not matches:
                continue

            record = matches[0]

            trace.details += f", selected record #{record.id} ({owner.runoffdb.record_types[record_type].name_en}, quality={qi})"

            if qi is None:
                trace.issues.append(
                    DataIssue(
                        reason=DataAbsenceReason.INVALID_VALUES,
                        details=f"record #{record.id} has NULL quality index",
                        severity=IssueSeverity.WARNING,
                    )
                )

            return record, trace

    trace.success = False
    trace.issues.append(
        DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            details="no matching record found",
            severity=IssueSeverity.INFO,
        )
    )

    return None, trace


def resolve_dedicated_or_generic_record(
    *,
    owner: "RecordOwner",
    dedicated_recid_attr: str | None,
    unit_id: int | list[int],
    phenomenon_id: int | None = None,
    view_order: list[int] | None = None,
) -> tuple["Record | None", DataTrace]:

    root_trace = create_trace(
        source="resolve_dedicated_or_generic_record",
        owner=owner,
        details=f"dedicated_recid_attr='{dedicated_recid_attr}', unit_id={unit_id}, phenomenon_id={phenomenon_id}",
    )

    # -------------------------
    # 1. dedicated record
    # -------------------------
    if dedicated_recid_attr:

        rec_id = getattr(owner, dedicated_recid_attr, None)

        if rec_id:

            record = owner.runoffdb.load_record_by_id(rec_id)

            if record:
                root_trace.details = f"dedicated record found #{rec_id}"
                return record, root_trace

            load_trace = create_trace(
                source=f"runoffdb.load_record_by_id",
                owner=owner,
                details=f"loading record #{rec_id} failed"
            )
            load_trace.success = False
            load_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.REFERENCED_RECORD_NOT_FOUND,
                    details=f"dedicated record assigned {rec_id} but not found",
                    severity=IssueSeverity.ERROR
                )
            )

            root_trace.success = False
            root_trace.traces.append(load_trace)
            return None, root_trace

        # not assigned
        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.RECORD_NOT_ASSIGNED,
                details=f"{dedicated_recid_attr} not assigned",
                severity=IssueSeverity.INFO,
            )
        )

    # -------------------------
    # 2. fallback resolution
    # -------------------------
    record, rec_trace = get_best_record_of_unit(
        owner=owner,
        unit_id=unit_id,
        phenomenon_id=phenomenon_id,
        view_order=view_order,
    )

    root_trace.details = f"generic fallback resolution: unit_id={unit_id}, phenomenon_id={phenomenon_id}"

    root_trace.success = True
    root_trace.traces.append(rec_trace)

    if record:
        return record, root_trace

    # -------------------------
    # 3. failure
    # -------------------------
    root_trace.success = False
    root_trace.issues.append(
        DataIssue(
            reason=DataAbsenceReason.NO_RECORD,
            details="neither dedicated nor generic record found",
            severity=IssueSeverity.INFO
        )
    )

    return None, root_trace

def get_record_data(
    *,
    record,
    value_label,
    related_x_label=None,
    related_y_label=None,
    related_z_label=None,
    target_unit_id=None,
    order_by=None,
    remove_last_zero=False,
):
    """
    Load record data and optionally:
        - convert units
        - remove trailing zero

    Returns:
        tuple[pd.DataFrame | None, DataTrace]
    """

    root_trace = create_trace(source="get_record_data",
                              owner=record,
                              variable=f"record {record.id} as '{value_label}'",
                              )

    # --------------------------------------------------
    # load dataframe
    # --------------------------------------------------

    df, load_trace = load_dataframe(
        record=record,
        value_label=value_label,
        related_x_label=related_x_label,
        related_y_label=related_y_label,
        related_z_label=related_z_label,
        order_by=order_by,
    )

    if df is None:
        root_trace.success = False
        root_trace.details = "record data retrieval pipeline failed"
        root_trace.traces.append(load_trace)

        return df, root_trace

    current_trace = load_trace

    # --------------------------------------------------
    # unit conversion
    # --------------------------------------------------

    if (
        target_unit_id is not None
        and record.unit_id != target_unit_id
    ):

        df, conversion_trace = convert_units(
            df=df,
            value_label=value_label,
            source_unit_id=record.unit_id,
            target_unit_id=target_unit_id,
        )

        # append the last step_trace
        conversion_trace.traces.append(current_trace)
        # and make this the last step
        current_trace = conversion_trace

        if not conversion_trace.success:
            root_trace.success = False
            root_trace.details = "record data retrieval pipeline failed"
            root_trace.traces.append(conversion_trace)

            return None, root_trace

    # --------------------------------------------------
    # trailing zero removal
    # --------------------------------------------------

    if remove_last_zero:

        df, trim_trace = remove_trailing_zero(df=df)
        # append the last step_trace
        trim_trace.traces.append(current_trace)
        # and make this the last step
        current_trace = trim_trace

    # --------------------------------------------------
    # attach deepest successful step
    # --------------------------------------------------
    root_trace.details = "record data retrieval pipeline successful"
    root_trace.traces.append(current_trace)

    return df, root_trace

def load_dataframe(
    *,
    record,
    value_label,
    related_x_label=None,
    related_y_label=None,
    related_z_label=None,
    order_by=None,
):
    root_trace = create_trace(
        source="load_dataframe",
        variable=f"record {record.id} as '{value_label}'",
        owner=record,
    )

    try:

        df = record.get_data(
            value_label=value_label,
            related_x_label=related_x_label,
            related_y_label=related_y_label,
            related_z_label=related_z_label,
            order_by=order_by,
        )

    except Exception as exc:

        root_trace.success = False
        root_trace.details = f"loading data of record #{record.id} failed",

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.UNKNOWN,
                details=f"record.get_data() raised exception:\n{exc}",
                severity=IssueSeverity.ERROR,
            )
        )

        return None, root_trace

    if df.empty:

        root_trace.success = False
        root_trace.details = f"loading data of record #{record.id} failed",

        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                details="record.get_data() returned empty dataframe",
                severity=IssueSeverity.WARNING,
            )
        )

        return None, root_trace

    root_trace.details = f"loaded data of record #{record.id} as '{value_label}'"

    return df, root_trace


def convert_units(
    *,
    df,
    value_label,
    source_unit_id,
    target_unit_id,
):

    before_non_na = (
        df[value_label]
        .notna()
        .sum()
    )

    try:

        converted_df = convert_dataframe_units(
            df=df,
            source_unit_id=source_unit_id,
            target_unit_id=target_unit_id,
            value_column=value_label,
            output_column=value_label,
        )

    except UnitConversionError as exc:

        trace = DataTrace(
            source="convert_units",
            details=f"unit conversion #{source_unit_id} → #{target_unit_id} failed",
            success=False,
        )

        trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.UNIT_CONVERSION_FAILED,
                details=str(exc),
                severity=IssueSeverity.ERROR,
            )
        )

        return None, trace

    after_non_na = (
        converted_df[value_label]
        .notna()
        .sum()
    )

    if before_non_na > 0 and after_non_na == 0:

        trace = DataTrace(
            source="convert_units",
            details=f"{source_unit_id} → {target_unit_id}",
            success=False,
        )

        trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.UNIT_CONVERSION_FAILED,
                details="conversion removed all values",
                severity=IssueSeverity.ERROR,
            )
        )

        return None, trace

    return (
        converted_df,
        DataTrace(
            source="convert_units",
            details=f"units converted #{source_unit_id} → #{target_unit_id}",
            success=True,
        ),
    )

def remove_trailing_zero(
    *,
    df,
):

    before_rows = len(df)
    trimmed_df = remove_last_zero_row(df)
    removed_rows = before_rows - len(trimmed_df)

    return (
        trimmed_df,
        DataTrace(
            source="remove_trailing_zero",
            details=f"removed_rows = {removed_rows}",
            success=True,
            # metadata={
            #     "removed_rows": removed_rows,
            # },
        ),
    )

def get_record_scalar_value(
    *,
    record: "Record",
    value_label: str,
    target_unit_id: int | None = None,
    multi_value: bool = False,
) -> tuple[float | list[float] | None, DataTrace]:
    """
    Resolve a scalar value from the best available record.
    """

    df, trace = get_record_data(
        record=record,
        value_label=value_label,
        related_x_label=None,
        related_y_label=None,
        related_z_label=None,
        target_unit_id=target_unit_id,
    )

    root_trace = DataTrace(
        source="get_record_scalar_value",
        variable=f"{value_label}",
        details=f"record #{record.id}",
        success=True,
        traces=[trace],
    )

    # --------------------------------------------------
    # no data case
    # --------------------------------------------------
    if df is None or df.empty:

        root_trace.success = False
        root_trace.issues.append(
            DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                details=f"no usable data for record {record.id} ({value_label})",
                severity=IssueSeverity.WARNING,
            )
        )

        return None, root_trace

    values = df[value_label]

    # --------------------------------------------------
    # single value → scalar
    # --------------------------------------------------
    if len(values) == 1:
        value = values.iloc[0]

        root_trace.details += " - single value found"
        return value, root_trace

    # --------------------------------------------------
    # multi-value allowed → list
    # --------------------------------------------------
    if multi_value:
        root_trace.details += f" - multiple values found ({len(values)})"
        # root_trace.metadata = {
        #     **(root_trace.metadata or {}),
        #     "n_values": len(values),
        # }

        return values.tolist(), root_trace

    # --------------------------------------------------
    # multi-value collapsed → mean aggregation
    # --------------------------------------------------
    root_trace.success = True  # still valid derivation step

    root_trace.issues.append(
        DataIssue(
            reason=DataAbsenceReason.DERIVED_MEAN,
            details=f"collapsed {len(values)} values using mean",
            severity=IssueSeverity.INFO,
        )
    )

    return values.mean(), root_trace

def record_matches_units(
    record,
    *,
    allowed_unit_ids: int | list[int],
    include_related: bool = True,
) -> bool:
    """
    Check whether a record matches allowed units.

    A match occurs if any of the record's unit attributes
    equals one of the allowed unit IDs.

    Checked attributes:
    - record.unit_id
    - record.related_value_x_unit_id
    - record.related_value_y_unit_id
    - record.related_value_z_unit_id

    Parameters
    ----------
    record
        Record instance to be checked.
    allowed_unit_ids
        Single unit ID or list of acceptable unit IDs.
    include_related
        Whether to consider related X/Y/Z unit IDs.

    Returns
    -------
    bool
        True if record matches at least one allowed unit.
    """

    if record is None:
        return False

    if not isinstance(allowed_unit_ids, (list, tuple, set)):
        allowed = {allowed_unit_ids}
    else:
        allowed = set(allowed_unit_ids)

    # main unit
    if getattr(record, "unit_id", None) in allowed:
        return True

    if not include_related:
        return False

    # related units
    for attr in (
        "related_value_x_unit_id",
        "related_value_y_unit_id",
        "related_value_z_unit_id",
    ):
        if getattr(record, attr, None) in allowed:
            return True

    return False

def iter_record_candidates(owner, unit_id, phenomenon_id, view_order):
    for record_type in view_order:
        records = owner.get_records(unit_id, phenomenon_id, record_type)
        if records:
            yield record_type, records

def pick_by_quality_index(records, quality_indexes):
    for qi in quality_indexes + [None]:
        matches = [r for r in records if r.quality_index_id == qi]
        if matches:
            return matches[0], qi
    return None, None
