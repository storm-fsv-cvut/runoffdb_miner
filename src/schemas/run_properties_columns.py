from src.schemas.column_schemas import RunColumn, build_variable_header
from src.utilities.utilities import czech_date
from src.services.soil_data import *
from src.services.hydro_data import *
from src.services.crop_data import *
from src.services.run_metadata import *

WRB_FRACTION_LIMITS = [0.002, 0.063, 2]



def _soil_texture_value(limit: float):

    def getter(run, ctx):

        root_trace = create_trace(
            source="_soil_texture_value",
            owner=run,
            variable=f"soil_texture_fraction_{limit}",
            details=f"soil texture fraction at limit {limit}",
        )

        df, texture_trace = get_cached_soil_texture(
            run=run,
            ctx=ctx,
        )

        root_trace.traces.append(texture_trace)

        if df is None or df.empty:
            root_trace.success = False
            root_trace.details = "no soil texture data available"

            return None, root_trace

        try:
            value = df.loc[
                limit,
                "cumulative_mass_content",
            ]

        except KeyError:

            root_trace.success = False

            root_trace.issues.append(
                DataIssue(
                    reason=DataAbsenceReason.DATA_NOT_AVAILABLE,
                    details=f"interpolated texture does not contain limit {limit}",
                    severity=IssueSeverity.ERROR,
                )
            )

            return None, root_trace

        return value, root_trace

    return getter



RUN_PROPERTIES: list[RunColumn] = [

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
        getter=lambda r, ctx: get_rainfall_intensity_value(run=r, target_unit_id=RAINFALL_INTENSITY_MMH_UNIT_ID),
    ),
]

def get_cached_soil_texture(
    *,
    run: "Run",
    ctx: dict,
):
    """
    Resolve soil texture once per run and cache it in run_ctx.

    The cached object contains both:
        - interpolated dataframe
        - provenance trace

    Returns:
        tuple[pd.DataFrame | None, DataTrace]
    """

    run_ctx = ctx.setdefault(
        "run_ctx",
        {},
    )

    if "soil_texture" not in run_ctx:

        run_ctx["soil_texture"] = get_best_soil_texture_data(
            run=run,
            x_label="cumulative_mass_content",
            y_label="particle_size",
            limits=WRB_FRACTION_LIMITS,
        )

    return run_ctx["soil_texture"]