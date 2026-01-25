from .unit_conversion import convert_dataframe_units, UnitConversionError
from src.setup.unit_ids import *
from src.setup.entity_ids import *

from src.entities.data_owners import RecordOwner
from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.trace import DataIssue
from src.utilities.utilities import remove_last_zero_row
from src.services.record_type_priorities import DEFAULT_RECORD_TYPE_PRIORITY


def get_best_record_of_unit(
    *,
    owner: RecordOwner,
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
        records = owner.get_records(
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
        for qi in owner.runoffdb.get_all_quality_indexes() + [None]:
            matches = [r for r in records if r.quality_index_id == qi]
            if matches:
                return matches[0]

    return None

def resolve_dedicated_or_generic_record(
    *,
    owner: RecordOwner,
    dedicated_recid_attr: str | None,
    unit_id: int | list[int],
    phenomenon_id: int | None = None,
    view_order: list[int] | None = None,
    return_trace: bool = False,
):
    """
    Resolve a record with optional dedicated override.

    Resolution order:
    1. dedicated record referenced by run (if attribute provided)
    2. best available generic record

    Returns:
    - record
    - (record, tuple[DataIssue]) if return_trace=True
    """

    issues: list[DataIssue] = []

    # 1. dedicated record path
    if dedicated_recid_attr:
        rec_id = getattr(owner, dedicated_recid_attr, None)
        if rec_id:
            record = owner.runoffdb.load_record_by_id(rec_id)
            if record:
                return (record, None) if return_trace else record
            else:
                issues.append(DataIssue(
                    reason=DataAbsenceReason.RECORD_NOT_FOUND,
                    source="resolve_dedicated_or_generic_record",
                    details=(
                        f"run.{dedicated_recid_attr}={rec_id} "
                        "but record was not found - database inconsistency"
                    ),
                ))
                return (None, tuple(issues)) if return_trace else None
        else:
            issues.append(DataIssue(
                reason=DataAbsenceReason.RECORD_NOT_ASSIGNED,
                source="resolve_dedicated_or_generic_record",
                details=(
                    f"'{dedicated_recid_attr}' dedicated record not set"
                ),
            ))
    # 2. generic fallback
    record = get_best_record_of_unit(
        owner=owner,
        unit_id=unit_id,
        phenomenon_id=phenomenon_id,
        view_order=view_order,
    )

    if record:
        if dedicated_recid_attr:
            issues.append(DataIssue(
                reason=DataAbsenceReason.DEDICATION_MISSING,
                source="resolve_dedicated_or_generic_record",
                details=(
                    f"generic record used because run.{dedicated_recid_attr} is not set"
                ),
            ))
        return (record, tuple(issues) if issues else None) if return_trace else record

    # 3. nothing found
    is_list = True if isinstance(unit_id, list) else False
    details = f"no record found for unit ID{'s (' if is_list else ' = '}{', '.join([str(i) for i in unit_id]) + ')' if is_list else unit_id}"
    details += f" and phenomenon ID = {phenomenon_id}" if phenomenon_id else ''
    issues.append(DataIssue(
        reason=DataAbsenceReason.NO_RECORD,
        source="resolve_dedicated_or_generic_record",
        details=details
        ),
    )

    return (None, tuple(issues)) if return_trace else None

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
    return_trace: bool = False,
):
    """
    Load record data, enforce timeline, convert units if needed.

    Returns:
    - DataFrame
    - (DataFrame, DataTrace) if return_trace=True
    """
    import pandas as pd

    issues: list[DataIssue] = []

    df = pd.DataFrame()

    try:
        df = record.get_data(
            value_label=value_label,
            related_x_label=related_x_label,
            related_y_label=related_y_label,
            related_z_label=related_z_label,
            order_by=order_by,
        )

        if df.empty:
            issues.append(DataIssue(
                reason=DataAbsenceReason.NO_DATA_IN_RECORD,
                source="get_record_data",
                details=f"record ID {record.id} returned no data",
            ))
            return (df, tuple(issues)) if return_trace else df


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
                issues.append(DataIssue(
                    reason=DataAbsenceReason.UNIT_CONVERSION_FAILED,
                    source="get_record_data",
                    details=f"unit conversion failed: {str(e)}",
                ))
                return (None, tuple(issues)) if return_trace else None

            after_non_na = df[value_label].notna().sum()
            if after_non_na == 0 and before_non_na > 0:
                issues.append(DataIssue(
                    reason=DataAbsenceReason.UNIT_CONVERSION_FAILED,
                    source="get_record_data",
                    details=f"units conversion {record.unit_id} -> {target_unit_id} removed all values",
                ))

    except Exception as exc:
        issues.append(DataIssue(
            reason=DataAbsenceReason.UNKNOWN,
            source="get_record_data",
            details=str(exc),
        ))

    else:
        if remove_last_zero:
            df = remove_last_zero_row(df)

    return (df, tuple(issues)) if return_trace else df

def get_record_scalar_value(
    *,
    record: "Record",
    value_label: str,
    target_unit_id: int | None = None,
    multi_value: bool = False,
    return_trace: bool = False,
):
    """
    Resolve a scalar value from the best available record.

    Supports:
    - multiple source units
    - phenomenon filtering
    - optional unit conversion
    """
    issues: list[DataIssue] = []

    # load data
    df, sub_issues = get_record_data(
        record=record,
        value_label=value_label,
        related_x_label=None,
        related_y_label=None,
        related_z_label=None,
        target_unit_id=target_unit_id,
        return_trace=True,
    )
    if df is None or df.empty:
        issues.append(DataIssue(
            reason=DataAbsenceReason.NO_DATA_IN_RECORD,
            source="get_record_scalar_value",
            details=f"no usable data gained for record {record.id} (requested as '{value_label}')",
            causes=sub_issues
        ))
        return (None, tuple(issues)) if return_trace else None

    if sub_issues:
        issues.append(sub_issues)

    values = df[value_label]

    # if data contain only single value return in
    if len(values.index) == 1:
        return (
            values.iloc[0],
            tuple(issues) if issues else None,
        ) if return_trace else values.iloc[0]

    # if multi-value was requested
    if multi_value:
        return (
            values.tolist(),
            tuple(issues) if issues else None,
        ) if return_trace else values.tolist()

    # single value requested but data have more values
    issues.append(DataIssue(
        reason=DataAbsenceReason.DERIVED_MEAN,
        source="get_record_scalar_value",
        details=(
            f"record ID {record.id} contains multiple values but single value was requested - "
            "mean value was returned"
        ),
    ))

    return (
        values.mean(),
        tuple(issues),
    ) if return_trace else values.mean()

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