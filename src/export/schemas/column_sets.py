from src.export.schemas.column_schemas import RunColumn, IntervalColumn
from src.utilities.utilities import czech_date, format_timedelta

from src.export.schemas.column_schemas import RunColumn
from src.utilities.utilities import czech_date
from src.exceptions import DataframeEmptyError


WRB_FRACTION_LIMITS = [0.002, 0.063, 2]


def _soil_texture_value(limit):
    def getter(run, ctx):
        try:
            df = run.get_best_soil_texture_data(
                "cumulative_mass_content",
                "particle_size",
                index_column="particle_size",
                order_by="particle_size",
                limits=WRB_FRACTION_LIMITS,
            )
        except DataframeEmptyError:
            return ctx["no_data_value"]

        if df is None or limit not in df.index:
            return ctx["no_data_value"]

        return df.loc[limit, "cumulative_mass_content"]

    return getter


RUN_INFO_COLUMNS: list[RunColumn] = [

    RunColumn(
        header={"cz": "ID sekvence", "en": "sequence ID"},
        getter=lambda r, ctx: r.sequence_id,
    ),

    RunColumn(
        header={"cz": "ID simulace", "en": "run ID"},
        getter=lambda r, ctx: r.id,
    ),

    RunColumn(
        header={"cz": "ID lokality", "en": "locality ID"},
        getter=lambda r, ctx: r.locality.id,
    ),

    RunColumn(
        header={"cz": "lokalita", "en": "locality"},
        getter=lambda r, ctx: r.locality.name,
    ),

    RunColumn(
        header={"cz": "datum", "en": "date"},
        getter=lambda r, ctx: czech_date(r.datetime),
    ),

    RunColumn(
        header={"cz": "ID plochy", "en": "plot ID"},
        getter=lambda r, ctx: r.plot_id,
    ),

    RunColumn(
        header={"cz": "název plochy", "en": "plot name"},
        getter=lambda r, ctx: r.plot.name,
    ),

    RunColumn(
        header={"cz": "délka plochy [m]", "en": "plot length [m]"},
        getter=lambda r, ctx: r.plot.plot_length,
    ),

    RunColumn(
        header={"cz": "šířka plochy [m]", "en": "plot width [m]"},
        getter=lambda r, ctx: r.plot.plot_width,
    ),

    RunColumn(
        header={"cz": "sklon plochy [%]", "en": "plot slope [%]"},
        getter=lambda r, ctx: r.plot.plot_slope,
    ),

    RunColumn(
        header={"cz": "poznámky k ploše", "en": "plot notes"},
        getter=lambda r, ctx: r.plot.get_note(
            lang=ctx["lang"],
            no_data_value=ctx["no_data_value"],
        ),
    ),

    RunColumn(
        header={"cz": "dnů od zasetí", "en": "days since seeding"},
        getter=lambda r, ctx:
            r.plot.days_since_seeding(
                r.datetime, main_crop_only=True
            ) or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "ochranná opatření", "en": "soil protection measures"},
        getter=lambda r, ctx:
            r.plot.get_protection_measures_names(ctx["lang"])
            or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "ID simulátoru", "en": "simulator ID"},
        getter=lambda r, ctx: r.simulator.id,
    ),

    RunColumn(
        header={"cz": "simulátor", "en": "simulator"},
        getter=lambda r, ctx: r.simulator.name[ctx["lang"]],
    ),

    RunColumn(
        header={"cz": "ID plodiny", "en": "crop ID"},
        getter=lambda r, ctx: r.crop_id or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "plodina", "en": "crop"},
        getter=lambda r, ctx:
            r.crop.name[ctx["lang"]] if r.crop else ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "stav plodiny", "en": "crop condition"},
        getter=lambda r, ctx:
            f"\"{r.crop_condition[ctx['lang']]}\""
            if r.crop_condition and r.crop_condition.get(ctx["lang"])
            else ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "výška plodiny [cm]", "en": "crop height [cm]"},
        getter=lambda r, ctx:
            r.get_crop_height_value() or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "počet rostlin [1/m2]", "en": "plant density [pcs.m^2]"},
        getter=lambda r, ctx:
            r.get_plant_density_value() or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "BBCH", "en": "BBCH"},
        getter=lambda r, ctx: r.bbch or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "zakrytí povrchu [%]", "en": "surface cover [%]"},
        getter=lambda r, ctx:
            r.get_surface_cover_value() or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "počáteční stav", "en": "initial cond."},
        getter=lambda r, ctx: r.run_type.name[ctx["lang"]],
    ),

    RunColumn(
        header={"cz": "počáteční vlhkost", "en": "init. moisture"},
        getter=lambda r, ctx:
            r.get_initial_moisture_value() or ctx["no_data_value"],
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
            r.get_best_bulk_density_value(27) or ctx["no_data_value"],
    ),

    RunColumn(
        header={"cz": "TTR", "en": "time to runoff"},
        getter=lambda r, ctx: r.ttr,
    ),
]


INTERVAL_COLUMNS: list[IntervalColumn] = [

    IntervalColumn(
        header={"cz": "interval", "en": "interval #"},
        getter=lambda run, row, ctx, st: st["i"],
    ),

    IntervalColumn(
        header={"cz": "délka intervalu", "en": "interval duration"},
        getter=lambda run, row, ctx, st:
            format_timedelta(st["index"] - st["prev_index"])
            if st["prev_index"] is not None else ctx["no_data_value"],
    ),

    IntervalColumn(
        header={"cz": "t1", "en": "t1"},
        getter=lambda run, row, ctx, st:
            format_timedelta(st["index"]),
    ),

    IntervalColumn(
        header={"cz": "t2", "en": "t2"},
        getter=lambda run, row, ctx, st:
            format_timedelta(st["index"] - run.ttr)
            if run.ttr is not None else ctx["no_data_value"],
    ),

    IntervalColumn(
        header={"cz": "srážkový úhrn [mm]", "en": "rainfall total [mm]"},
        getter=lambda run, row, ctx, st:
            row[ctx["labels"]["rainfall_total"]],
    ),

    IntervalColumn(
        header={"cz": "průtok [l/min]", "en": "flow rate [l.min-1]"},
        getter=lambda run, row, ctx, st:
            row[ctx["labels"]["runoff"]],
    ),

    IntervalColumn(
        header={"cz": "celkový odtok [l]", "en": "total discharge [l]"},
        getter=lambda run, row, ctx, st:
            row[ctx["labels"]["discharge"]],
    ),

    IntervalColumn(
        header={"cz": "koncentrace sedimentu [g/l]", "en": "SS concentration [g.l-1]"},
        getter=lambda run, row, ctx, st:
            row[ctx["labels"]["sediment_concentration"]],
    ),

    IntervalColumn(
        header={"cz": "tok sedimentu [g/min]", "en": "SS flux [g.min-1]"},
        getter=lambda run, row, ctx, st:
            row[ctx["labels"]["sediment_flux"]],
    ),

    IntervalColumn(
        header={"cz": "ztráta půdy [g]", "en": "sediment yield [g]"},
        getter=lambda run, row, ctx, st:
            row[ctx["labels"]["sediment_yield"]],
    ),
]