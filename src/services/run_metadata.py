from src.diagnostics.trace import DataTrace, DataIssue
from src.diagnostics.absence_reasons import DataAbsenceReason

def get_crop_name(run: "Run", ctx, return_trace: bool = False) -> tuple[str | None, DataIssue | None]:
    # missing crop_id
    if not run.crop_id:
        issue = (DataIssue(
            reason=DataAbsenceReason.MISSING_ENTITY_PROPERTY,
            source="get_crop_name",
            details="run has no crop assigned",
        ), )
        return None, issue if return_trace else None

    # entity not loaded
    if run.crop is None:
        issue = (DataIssue(
            reason=DataAbsenceReason.DATABASE_RECORD_INVALID,
            source="get_crop_name",
            details=f"run.crop_id={run.crop_id} but the Crop entity was not loaded",
        ), )
        return None, issue if return_trace else None

    # name missing in requested language
    name = run.crop.name.get(ctx["lang"])
    if not name:
        issue = (DataIssue(
            reason=DataAbsenceReason.MISSING_PROPERTY_TRANSLATION,
            source="get_crop_name",
            details=f"crop ID {run.crop_id} does not have a name in language '{ctx['lang']}'",
        ), )
        return None, issue if return_trace else None

    # everything ok
    return (name, None) if return_trace else name

def get_crop_condition(run: "Run", ctx, return_trace: bool = False) -> tuple[str | None, DataIssue | None]:
    # missing crop_id
    if not run.crop_id:
        issue = (DataIssue(
            reason=DataAbsenceReason.MISSING_ENTITY_PROPERTY,
            source="get_crop_condition",
            details="run has no crop assigned",
        ), )
        return None, issue if return_trace else None

    # crop condition is not empty but missing in requested language

    if any(run.crop_condition.values()) and not run.crop_condition.get(ctx["lang"]):
        issue = (DataIssue(
            reason=DataAbsenceReason.MISSING_PROPERTY_TRANSLATION,
            source="get_crop_condition",
            details=f"crop condition description missing in language '{ctx['lang']}'",
        ), )
        return None, issue if return_trace else None

    cond = run.crop_condition.get(ctx["lang"])
    return (cond, None) if return_trace else cond
