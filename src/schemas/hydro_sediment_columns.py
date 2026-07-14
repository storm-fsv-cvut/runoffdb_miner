from pandas import Timedelta

from src.schemas.column_schemas import build_variable_header
from src.schemas.column_schemas import IntervalColumn, RunColumn
from src.utilities.utilities import format_timedelta

from src.services.hydro_data import *

HYDRO_SEDIMENT_INTERVALS = [

    IntervalColumn(
        header={"cz": "interval", "en": "interval #"},
        getter=lambda run, row, ctx: ctx["run_ctx"]["interval_ctx"]["i"],
    ),


    IntervalColumn(
        header={"cz": "délka intervalu", "en": "interval duration"},
        getter=lambda run, row, ctx: _get_interval_duration(ctx)
    ),


    IntervalColumn(
        header={"cz": "t1", "en": "t1"},
        getter=lambda run, row, ctx: format_timedelta(ctx["run_ctx"]["interval_ctx"]["index"]),
    ),


    IntervalColumn(
        header={"cz": "t2", "en": "t2"},
        getter=lambda run, row, ctx: _get_relative_time(run=run, ctx=ctx,),
    ),


    IntervalColumn(
        header=lambda registry, lang:
            build_variable_header(
                key="rainfall_total",
                registry=registry,
                lang=lang,
            ),
        getter=lambda run, row, ctx: row.get("rainfall_total"),
    ),


    IntervalColumn(
        header=lambda registry, lang:
            build_variable_header(
                key="surface_runoff",
                registry=registry,
                lang=lang,
            ),
        getter=lambda run, row, ctx: row.get("surface_runoff"),
    ),


    IntervalColumn(
        header=lambda registry, lang:
            build_variable_header(
                key="discharge",
                registry=registry,
                lang=lang,
            ),
        getter=lambda run, row, ctx: row.get("discharge"),
    ),


    IntervalColumn(
        header=lambda registry, lang:
            build_variable_header(
                key="sediment_concentration",
                registry=registry,
                lang=lang,
            ),
        getter=lambda run, row, ctx: row.get("sediment_concentration"),
    ),


    IntervalColumn(
        header=lambda registry, lang:
            build_variable_header(
                key="sediment_flux",
                registry=registry,
                lang=lang,
            ),
        getter=lambda run, row, ctx: row.get("sediment_flux"),
    ),


    IntervalColumn(
        header=lambda registry, lang:
            build_variable_header(
                key="sediment_yield",
                registry=registry,
                lang=lang,
            ),
        getter=lambda run, row, ctx: row.get("sediment_yield"),
    ),
]


def _get_interval_duration(ctx):
    interval_ctx = ctx["run_ctx"]["interval_ctx"]

    index = interval_ctx["index"]
    prev_index = interval_ctx["prev_index"]

    if prev_index is None:
        return None

    delta = index - prev_index

    if delta < Timedelta(0):
        return None

    return format_timedelta(delta)


def _get_relative_time(*, run, ctx,):
    interval_ctx = ctx["run_ctx"]["interval_ctx"]

    index = interval_ctx["index"]

    if run.ttr is None:
        return None

    delta = index - run.ttr

    if delta < Timedelta(0):
        return None

    return format_timedelta(delta)


# -------------------------
# schema-compatible getters
# -------------------------
def _series_value_getter(*, series_key: str, source: str):
    def getter(run, ctx, return_trace: bool = False):
        hydro_df = ctx.get("hydro_df")

        if hydro_df is None:
            issue = DataIssue(
                reason=DataAbsenceReason.NO_RECORD,
                source=source,
                details="runoff-sediment data set not available for given context",
            )
            return (None, (issue,)) if return_trace else None

        if series_key not in hydro_df.columns:
            issue = DataIssue(
                reason=DataAbsenceReason.NO_RECORD,
                source=source,
                details=f"series '{series_key}' not present in runoff-sediment data set",
            )
            return (None, (issue,)) if return_trace else None

        value, issues = get_value_in_time(
            hydro_df,
            ctx["time"],
            series_key,
            interpolate=ctx.get("interpolate", True),
            extrapolate=ctx.get("extrapolate"),
            return_trace=True,
        )

        return (value, issues) if return_trace else value

    return getter

rainfall_total_getter = _series_value_getter(
    series_key="rainfall_total",
    source="rainfall_total_getter",
)

runoff_getter = _series_value_getter(
    series_key="runoff",
    source="runoff_getter",
)

discharge_getter = _series_value_getter(
    series_key="discharge",
    source="discharge_getter",
)

sediment_conc_getter = _series_value_getter(
    series_key="sediment_concentration",
    source="sediment_conc_getter",
)

sediment_flux_getter = _series_value_getter(
    series_key="sediment_flux",
    source="sediment_flux_getter",
)

sediment_yield_getter = _series_value_getter(
    series_key="sediment_yield",
    source="sediment_yield_getter",
)

# getter for SLR ratio (crop / fallow)
def slr_ratio_getter(run, ctx, return_trace: bool = False):
    sed_crop = ctx.get("sed_crop")
    sed_fallow = ctx.get("sed_fallow")

    if sed_crop is None:
        issue = DataIssue(
            reason=DataAbsenceReason.MISSING_REQUIRED_INPUT,
            source="slr_ratio_getter",
            details="missing crop sediment yield for SLR calculation",
            severity=IssueSeverity.ERROR
        )
        return (None, (issue, )) if return_trace else None

    if sed_fallow in (None, 0):
        issue = DataIssue(
            reason=DataAbsenceReason.MISSING_REQUIRED_INPUT,
            source="slr_ratio_getter",
            details="missing or zero fallow sediment yield for SLR calculation",
            severity=IssueSeverity.ERROR
        )
        return (None, (issue, )) if return_trace else None

    return (sed_crop / sed_fallow, None) if return_trace else sed_crop / sed_fallow


def slr_average_getter(run, ctx, return_trace: bool = False):
    acc = ctx.get("slr_acc")

    if not acc:
        issue = (DataIssue(
            reason=DataAbsenceReason.PROCESSING_ERROR,
            source="slr_average_getter",
            details="SLRAccumulator not available in context",
        ),)
        return (None, issue) if return_trace else None

    value = acc.mean_if_complete()
    if value is None:
        issue = (DataIssue(
            reason=DataAbsenceReason.PROCESSING_ERROR,
            source="slr_average_getter",
            details="combined SLR could not be computed",
        ),)
        return (None, issue) if return_trace else None

    return (value, None) if return_trace else value



SLR_COLUMNS = [
    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="rainfall_total",
            registry=registry,
            lang=lang,
        ),
        getter=rainfall_total_getter,
    ),

    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="discharge",
            registry=registry,
            lang=lang,
        ),
        getter=discharge_getter
    ),

    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="sediment_yield",
            registry=registry,
            lang=lang,
        ),        getter=sediment_yield_getter
    ),

    RunColumn(
        header={"en": "SLR", "cz": "SLR"},
        getter=slr_ratio_getter
    ),

    RunColumn(
        header={"en": "SLR averaged", "cz": "SLR kombi"},
        getter=slr_average_getter
    ),
]