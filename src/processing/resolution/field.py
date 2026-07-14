from src.diagnostics.trace import DataTrace
from src.diagnostics.issue import DataIssue
from src.diagnostics.severity import IssueSeverity
from src.diagnostics.absence_reasons import DataAbsenceReason

def resolve_translated_field(
    *,
    owner,
    field_name: str,
    values: dict[str, str | None],
    lang: str,
    source: str,
    mandatory: bool = False,
):
    """
    Generic translation resolver with trace/issue support.
    """

    issue = None
    value = values.get(lang)

    # detect fallback languages
    if not value:
        available_langs = [
            l for l, v in values.items() if v
        ]

        if available_langs:
            issue = DataIssue(
                severity=IssueSeverity.WARNING,
                reason=DataAbsenceReason.MISSING_PROPERTY_TRANSLATION,
                details=f"No {field_name} in requested language ({lang}) but exists in ({', '.join(available_langs)})",
            )
        elif mandatory:
            issue = DataIssue(
                severity=IssueSeverity.ERROR,
                reason=DataAbsenceReason.MISSING_PROPERTY,
                details=f"No values found in any language for mandatory property {field_name}",
            )

        if not issue:
            return None, None

        return None, DataTrace(
            source=source,
            variable=f"{type(owner).__name__.lower()}_{field_name}",
            success=False if mandatory else True,
            owner_id=owner.id,
            owner_class=type(owner).__name__,
            details=f"{field_name} translation request in '{lang}'",
            issues=[issue],
        )

    return value, None
