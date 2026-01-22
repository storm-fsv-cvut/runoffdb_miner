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


def _soil_texture_value(limit):
    def getter(run, ctx):
        df = get_best_soil_texture_data(
            run=run,
            x_label="cumulative_mass_content",
            y_label="particle_size",
            limits=WRB_FRACTION_LIMITS,
        )

        if df is None or limit not in df.index:
            return ctx["no_data_value"]

        return df.loc[limit, "cumulative_mass_content"]

    return getter


RUN_INFO_COLUMNS: list[RunColumn] = [

    RunColumn(
        header={"cz": "ID sekvence", "en": "sequence ID"},
        getter=lambda r, ctx: (r.sequence_id, None)
    ),

    RunColumn(
        header={"cz": "ID simulace", "en": "run ID"},
        getter=lambda r, ctx: (r.id, None),
    ),

    RunColumn(
        header={"cz": "ID lokality", "en": "locality ID"},
        getter=lambda r, ctx: (r.locality.id, None),
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
        getter=lambda r, ctx: (r.simulator.name[ctx["lang"]], None),
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
        getter=lambda r, ctx: (r.plot.get_note(lang=ctx["lang"]), None),
    ),

    RunColumn(
        header={"cz": "ID plodiny", "en": "crop ID"},
        getter=lambda r, ctx: (r.crop_id, None),
    ),

    RunColumn(
        header={"cz": "název plodiny", "en": "crop name"},
        getter=lambda r, ctx: get_crop_name(run=r, ctx=ctx, return_trace=True),
    ),

    RunColumn(
        header={"cz": "stav plodiny", "en": "crop condition"},
        getter=lambda r, ctx: get_crop_condition(run=r, ctx=ctx, return_trace=True),
    ),

    RunColumn(
        header={"cz": "dnů od zasetí", "en": "days since seeding"},
        getter=lambda r, ctx: r.plot.days_since_seeding(r.datetime, main_crop_only=True, return_trace=True)
    ),

    RunColumn(
        header={"cz": "ochranná opatření", "en": "soil protection measures"},
        getter=lambda r, ctx: r.plot.get_protection_measures_names(lang=ctx["lang"], return_trace=True)
    ),

    RunColumn(
        header={"cz": "výška plodiny [cm]", "en": "crop height [cm]"},
        getter=lambda r, ctx:
            get_crop_height_value(run=r, return_trace=True, multi_value=False),
    ),

    RunColumn(
        header={"cz": "počet rostlin [1/m2]", "en": "plant density [pcs.m^2]"},
        getter=lambda r, ctx:
            get_plant_density_value(run=r, return_trace=True, multi_value=False),
    ),

    RunColumn(
        header={"cz": "BBCH", "en": "BBCH"},
        getter=lambda r, ctx: (r.bbch, None),
    ),

    RunColumn(
        header={"cz": "zakrytí povrchu [%]", "en": "surface cover [%]"},
        getter=lambda r, ctx: get_surface_cover_value(run=r, multi_value=False, return_trace=True),
    ),

    RunColumn(
        header={"cz": "počáteční stav", "en": "initial cond."},
        getter=lambda r, ctx: get_run_type_name(run=r, ctx=ctx, return_trace=True),
    ),

    RunColumn(
        header={"cz": "počáteční vlhkost [%V]", "en": "init. moisture [%V]"},
        getter=lambda r, ctx:
            get_initial_moisture_value(run=r, multi_value=False, return_trace=True),
    ),
    #
    # # --- soil texture fractions ---
    # RunColumn(
    #     header={"cz": "<0, 0.002mm>", "en": "<0, 0.002mm>"},
    #     getter=_soil_texture_value(0.002),
    # ),
    #
    # RunColumn(
    #     header={"cz": "<0.002, 0.063mm>", "en": "<0.002, 0.063mm>"},
    #     getter=_soil_texture_value(0.063),
    # ),
    #
    # RunColumn(
    #     header={"cz": "<0.063, 2mm>", "en": "<0.063, 2mm>"},
    #     getter=_soil_texture_value(2),
    # ),

    RunColumn(
        header={"cz": "objemová hmotnost [g/cm3]", "en": "bulk density [g.cm-3]"},
        getter=lambda r, ctx:
            get_best_bulk_density_value(run=r, target_unit_id=BULK_DENSITY_GCM_UNIT_ID, return_trace=True),
    ),

    RunColumn(
        header={"cz": "TTR", "en": "time to runoff"},
        getter=lambda r, ctx: (r.ttr, None),
    ),

    RunColumn(
        header={"cz": "intenzita srážky [mm/h]", "en": "rainfall intensity [mm.hour-1]"},
        getter=lambda r, ctx:
            get_rainfall_intensity_value(run=r, target_unit_id=RAINFALL_INTENSITY_MMH_UNIT_ID, return_trace=True),
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
            if st["prev_index"] is not None and (delta := st["index"] - st["prev_index"]) >= Timedelta(0)
            else ctx["no_data_value"]
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
            if run.ttr is not None and (delta := st["index"] - run.ttr) >= Timedelta(0)
            else ctx["no_data_value"],
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

# -------------------------
# schema-compatible getters
# -------------------------

def rainfall_total_getter(run, ctx):
    hydro_df = ctx.get("hydro_df")
    label = ctx.get("rainfall_total_label", "rainfall_total")
    if hydro_df is not None and label in hydro_df.columns:
        return get_value_in_time(
            hydro_df,
            ctx["time"],
            label,
            interpolate=ctx.get("interpolate", True),
            extrapolate=ctx.get("extrapolate", 2)
        )
    return ctx["no_data_value"]


def runoff_getter(run, ctx):
    hydro_df = ctx.get("hydro_df")
    label = ctx.get("runoff_label", "runoff")
    if hydro_df is not None and label in hydro_df.columns:
        return get_value_in_time(
            hydro_df,
            ctx["time"],
            label,
            interpolate=ctx.get("interpolate", True),
            extrapolate=ctx.get("extrapolate", 2)
        )
    return ctx["no_data_value"]


def discharge_getter(run, ctx):
    hydro_df = ctx.get("hydro_df")
    label = ctx.get("discharge_label", "discharge")
    if hydro_df is not None and label in hydro_df.columns:
        return get_value_in_time(
            hydro_df,
            ctx["time"],
            label,
            interpolate=ctx.get("interpolate", True),
            extrapolate=ctx.get("extrapolate", 2)
        )
    return ctx["no_data_value"]


def sediment_conc_getter(run, ctx):
    hydro_df = ctx.get("hydro_df")
    label = ctx.get("sediment_conc_label", "sediment_concentration")
    if hydro_df is not None and label in hydro_df.columns:
        return get_value_in_time(
            hydro_df,
            ctx["time"],
            label,
            interpolate=ctx.get("interpolate", True),
            extrapolate=ctx.get("extrapolate", 2)
        )
    return ctx["no_data_value"]


def sediment_flux_getter(run, ctx):
    hydro_df = ctx.get("hydro_df")
    label = ctx.get("sed_flux_label", "sediment_flux")
    if hydro_df is not None and label in hydro_df.columns:
        return get_value_in_time(
            hydro_df,
            ctx["time"],
            label,
            interpolate=ctx.get("interpolate", True),
            extrapolate=ctx.get("extrapolate", 2)
        )
    return ctx["no_data_value"]


def sediment_yield_getter(run, ctx):
    hydro_df = ctx.get("hydro_df")
    label = ctx.get("sed_yield_label", "sediment_yield")
    if hydro_df is not None and label in hydro_df.columns:
        return get_value_in_time(
            hydro_df,
            ctx["time"],
            label,
            interpolate=ctx.get("interpolate", True),
            extrapolate=ctx.get("extrapolate", 2)
        )
    return ctx["no_data_value"]

# getter for SLR ratio (crop / fallow)
def slr_ratio_getter(run, ctx):
    """
    SLR ratio of crop / fallow.
    Expects ctx to have:
      - "sed_crop" : float
      - "sed_fallow" : float
      - "no_data_value" : fallback
    """
    sed_crop = float(ctx.get("sed_crop", 0) or 0)
    sed_fallow = float(ctx.get("sed_fallow", 0) or 0)
    if sed_fallow > 0:
        return sed_crop / sed_fallow
    return ctx["no_data_value"]


def slr_average_getter(run, ctx):
    """
    Combined SLR across sequence.
    Expects ctx to have:
      - "slr_acc" : object with .combined_slr() method
      - "no_data_value"
    """
    acc = ctx.get("slr_acc")
    if acc:
        val = acc.combined_slr()
        return val if val is not None else ctx["no_data_value"]
    return ctx["no_data_value"]


SLR_COLUMNS = [
    RunColumn(header={"en": "rainfall total", "cz": "srážkový úhrn"}, getter=rainfall_total_getter),
    RunColumn(header={"en": "runoff", "cz": "průtok"}, getter=runoff_getter),
    RunColumn(header={"en": "discharge", "cz": "celkový odtok"}, getter=discharge_getter),
    RunColumn(header={"en": "sediment concentration", "cz": "koncentrace sedimentu"}, getter=sediment_conc_getter),
    RunColumn(header={"en": "sediment flux", "cz": "tok sedimentu"}, getter=sediment_flux_getter),
    RunColumn(header={"en": "sediment yield", "cz": "ztráta půdy"}, getter=sediment_yield_getter),
    RunColumn(header={"en": "SLR ratio", "cz": "SLR"}, getter=slr_ratio_getter),
    RunColumn(header={"en": "SLR averaged", "cz": "SLR průměr"}, getter=slr_average_getter),
]