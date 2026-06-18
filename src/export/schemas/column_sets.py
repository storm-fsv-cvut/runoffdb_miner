from src.export.schemas.column_schemas import RunColumn, IntervalColumn
from src.utilities.utilities import czech_date, format_timedelta

from src.export.schemas.column_schemas import RunColumn
from src.utilities.utilities import czech_date
from src.services.soil_data import *
from src.services.hydro_data import *
from src.services.record_resolution import *
from src.services.crop_data import *
from src.services.run_metadata import *

from pandas import Timedelta

WRB_FRACTION_LIMITS = [0.002, 0.063, 2]

def build_variable_header(
    key: str,
    registry,
    lang: str,
) -> str:
    """
    Build localized variable header including unit symbol.

    Resolves:
        - localized base label from VariableDefinition.base_labels
        - default unit symbol from registry units

    Fallback strategy:
        - if variable missing -> return key
        - if label missing for lang -> fallback to 'en'
        - if no unit -> return label only
    """
    try:
        var_def = registry[key]
    except KeyError:
        return key  # unknown variable

    # ---- resolve label ----
    label = None
    if var_def.base_labels:
        label = (
            var_def.base_labels.get(lang)
            or var_def.base_labels.get("en")
        )

    if not label:
        label = key

    # ---- resolve unit ----
    unit_symbol = None
    if var_def.default_unit_id is not None:
        unit = registry._units.get(var_def.default_unit_id)
        if unit:
            unit_symbol = getattr(unit, "unit", None)

    if unit_symbol:
        return f"{label} [{unit_symbol}]"

    return label

def resolve_header_of_column(column, registry, lang):
    if callable(column.header):
        return column.header(registry, lang)

    if isinstance(column.header, dict):
        return column.header.get(lang) or column.header.get("en") or ""


def _soil_texture_value(
    limit: float,
):
    def getter(run, ctx):

        root_trace = create_trace(
            source="_soil_texture_value",
            owner=run,
            variable=f"soil_texture_fraction_{limit}",
            details=f"soil texture fraction at limit {limit}",
        )

        df, texture_trace = get_best_soil_texture_data(
            run=run,
            x_label="cumulative_mass_content",
            y_label="particle_size",
            limits=[limit],
        )

        root_trace.traces.append(texture_trace)

        if df is None or df.empty:

            root_trace.success = False
            root_trace.details = "no soil texture data available"

            return None, root_trace

        try:
            value = df.loc[limit, "cumulative_mass_content"]

        except KeyError:

            root_trace.success = False

            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.DATA_NOT_AVAILABLE,
                    source="_soil_texture_value",
                    details=(
                        f"interpolated texture does not "
                        f"contain limit {limit}"
                    ),
                )
            )

            return None, root_trace

        return value, root_trace

    return getter



RUN_LEVEL_COLUMNS: list[RunColumn] = [

    RunColumn(
        header={"cz": "ID simulace", "en": "run ID"},
        getter=lambda r, ctx: (r.id, None)
    ),

    RunColumn(
        header={"cz": "ID sekvence", "en": "sequence ID"},
        getter=lambda r, ctx: (r.sequence_id, None)
    ),

    RunColumn(
        header={"cz": "ID lokality", "en": "locality ID"},
        getter=lambda r, ctx: (r.locality.id, None)
    ),

    RunColumn(
        header={"cz": "lokalita", "en": "locality"},
        getter=lambda r, ctx: (r.locality.name, None),
    ),

    RunColumn(
        header={"cz": "datum", "en": "date"},
        getter=lambda r, ctx: (czech_date(r.datetime), None),
    ),

    RunColumn(
        header={"cz": "ID simulátoru", "en": "simulator ID"},
        getter=lambda r, ctx: (r.simulator.id, None),
    ),

    RunColumn(
        header={"cz": "simulátor", "en": "simulator"},
        getter=lambda r, ctx: r.simulator.get_name(ctx["lang"])
    ),

    RunColumn(
        header={"cz": "ID plochy", "en": "plot ID"},
        getter=lambda r, ctx: (r.plot_id, None),
    ),

    RunColumn(
        header={"cz": "název plochy", "en": "plot name"},
        getter=lambda r, ctx: (r.plot.name, None),
    ),

    RunColumn(
        header={"cz": "délka plochy [m]", "en": "plot length [m]"},
        getter=lambda r, ctx: (r.plot.plot_length, None),
    ),

    RunColumn(
        header={"cz": "šířka plochy [m]", "en": "plot width [m]"},
        getter=lambda r, ctx: (r.plot.plot_width, None),
    ),

    RunColumn(
        header={"cz": "sklon plochy [%]", "en": "plot slope [%]"},
        getter=lambda r, ctx: (r.plot.plot_slope, None),
    ),

    RunColumn(
        header={"cz": "poznámky k ploše", "en": "plot notes"},
        getter=lambda r, ctx: r.plot.get_note(lang=ctx["lang"]),
    ),

    RunColumn(
        header={"cz": "ID plodiny", "en": "crop ID"},
        getter=lambda r, ctx: (r.crop_id, None),
    ),


    RunColumn(
        header={"cz": "název plodiny", "en": "crop name"},
        getter=lambda r, ctx: r.crop.get_name(lang=ctx["lang"]),
    ),

    RunColumn(
        header={"cz": "stav plodiny", "en": "crop condition"},
        getter=lambda r, ctx: r.get_crop_condition(lang=ctx["lang"]),
    ),

    RunColumn(
        header={"cz": "dnů od zasetí", "en": "days since seeding"},
        getter=lambda r, ctx: r.plot.days_since_seeding(r.datetime, main_crop_only=True)
    ),

    RunColumn(
        header={"cz": "ochranná opatření", "en": "soil protection measures"},
        getter=lambda r, ctx: r.plot.get_protection_measures_names(lang=ctx["lang"])
    ),

    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="crop_height",
            registry=registry,
            lang=lang,
        ),
        getter=lambda r, ctx:
            get_crop_height_value(run=r, multi_value=False),
    ),

    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="crop_density",
            registry=registry,
            lang=lang,
        ),
        getter=lambda r, ctx: get_plant_density_value(run=r, multi_value=False,)
    ),

    RunColumn(
        header={"cz": "BBCH", "en": "BBCH"},
        getter=lambda r, ctx: (r.bbch, None),
    ),

    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="surface_cover",
            registry=registry,
            lang=lang,
        ),
        getter=lambda r, ctx: get_surface_cover_value(run=r, multi_value=False),
    ),

    RunColumn(
        header={"cz": "počáteční stav", "en": "initial cond."},
        getter=lambda r, ctx: r.run_type.get_name(lang=ctx["lang"]),
    ),

    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="soil_moisture",
            registry=registry,
            lang=lang,
        ),
        getter=lambda r, ctx:
            get_initial_moisture_value(run=r, multi_value=False),
    ),

    # --- soil texture fractions ---
    RunColumn(
        header={"cz": "<0, 0.002mm>", "en": "<0, 0.002mm>"},
        getter=_soil_texture_value(0.002),
    ),

    RunColumn(
        header={"cz": "<0.002, 0.063mm>", "en": "<0.002, 0.063mm>"},
        getter=_soil_texture_value(0.063),
    ),

    RunColumn(
        header={"cz": "<0.063, 2mm>", "en": "<0.063, 2mm>"},
        getter=_soil_texture_value(2),
    ),

    RunColumn(
        header={"cz": "objemová hmotnost [g/cm3]", "en": "bulk density [g.cm-3]"},
        getter=lambda r, ctx:
            get_best_bulk_density_value(run=r, target_unit_id=BULK_DENSITY_GCM_UNIT_ID),
    ),

    RunColumn(
        header={"cz": "TTR", "en": "time to runoff"},
        getter=lambda r, ctx: (r.ttr, None),
    ),

    RunColumn(
        header=lambda registry, lang: build_variable_header(
            key="rainfall_intensity",
            registry=registry,
            lang=lang,
        ),
        getter=lambda r, ctx: get_rainfall_intensity_value(run=r),
    ),
]


INTERVAL_LEVEL_COLUMNS: list[IntervalColumn] = [

    IntervalColumn(
        header={"cz": "interval", "en": "interval #"},
        getter=lambda run, row, ctx, st: (st["i"], None),
    ),

    IntervalColumn(
        header={"cz": "délka intervalu", "en": "interval duration"},
        getter=lambda run, row, ctx, st: (
            (format_timedelta(st["index"] - st["prev_index"]), None)
            if st["prev_index"] is not None and (delta := st["index"] - st["prev_index"]) >= Timedelta(0)
            else (ctx["no_data_value"], None)
        ),
    ),

    IntervalColumn(
        header={"cz": "t1", "en": "t1"},
        getter=lambda run, row, ctx, st:
            (format_timedelta(st["index"]), None),
    ),

    IntervalColumn(
        header={"cz": "t2", "en": "t2"},
        getter=lambda run, row, ctx, st:
            (format_timedelta(st["index"] - run.ttr), None)
            if run.ttr is not None and (delta := st["index"] - run.ttr) >= Timedelta(0)
            else (None, None),
    ),

    IntervalColumn(
        header=lambda registry, lang: build_variable_header(
            key="rainfall_total",
            registry=registry,
            lang=lang,
        ),
        getter=lambda run, row, ctx, st:
            (row.get("rainfall_total"), None)
    ),

    IntervalColumn(
        header=lambda registry, lang:
            build_variable_header(
                key="surface_runoff",
                registry=registry,
                lang=lang,
            ),
        getter=lambda run, row, ctx, st:
            (row.get("surface_runoff"), None)
    ),

    IntervalColumn(
        header=lambda registry, lang: build_variable_header(
            key="discharge",
            registry=registry,
            lang=lang,
        ),        getter=lambda run, row, ctx, st:
            (row.get("discharge"), None)
    ),

    IntervalColumn(
        header=lambda registry, lang: build_variable_header(
            key="sediment_concentration",
            registry=registry,
            lang=lang,
        ),
        getter=lambda run, row, ctx, st:
            (row.get("sediment_concentration"), None)
    ),

    IntervalColumn(
        header=lambda registry, lang: build_variable_header(
            key="sediment_flux",
            registry=registry,
            lang=lang,
        ),
        getter=lambda run, row, ctx, st:
            (row.get("sediment_flux"), None)
    ),

    IntervalColumn(
        header=lambda registry, lang: build_variable_header(
            key="sediment_yield",
            registry=registry,
            lang=lang,
        ),
        getter=lambda run, row, ctx, st:
            (row.get("sediment_yield"), None)
    ),
]

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
        )
        return (None, (issue, )) if return_trace else None

    if sed_fallow in (None, 0):
        issue = DataIssue(
            reason=DataAbsenceReason.MISSING_REQUIRED_INPUT,
            source="slr_ratio_getter",
            details="missing or zero fallow sediment yield for SLR calculation",
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