# from pipeline import Miner
from src.setup.entity_ids import *
from src.export.schemas.column_sets import *
from .filesystem import *
from .writers import *
from ..diagnostics.collector import TraceCollector
import os
from ..utilities.utilities import czech_date
from ..utilities.plotters import *

class SLRAccumulator:
    """
    Tracks crop vs fallow sediment yields to compute combined SLR.
    """
    def __init__(self):
        self.dry_crop = None
        self.verywet_crop = None
        self.dry_fallow = None
        self.verywet_fallow = None

    def add_run_pair(self, run, sed_crop, sed_fallow):
        if run.run_type_id == DRY_RUN_TYPE_ID:
            self.dry_crop = sed_crop
            self.dry_fallow = sed_fallow
        else:
            self.verywet_crop = sed_crop
            self.verywet_fallow = sed_fallow

    def mean_if_complete(self):
        """
        Returns mean SLR across dry and very wet variants
        only if BOTH variants are fully available.
        """
        # check completeness
        if (
                self.dry_crop is None or
                self.dry_fallow is None or
                self.verywet_crop is None or
                self.verywet_fallow is None
        ):
            return None

        # avoid division by zero explicitly
        if self.dry_fallow == 0 or self.verywet_fallow == 0:
            return None

        return (self.dry_crop + self.verywet_crop) / (self.dry_fallow + self.verywet_fallow)

def calculate_SLR(miner, output_dir, lang="en", no_data_value="", in_time=None, interpolate=True, output_trace_path=None):
    """
    Export SLR values for all runs in schema-driven fashion.
    The function uses SLR_COLUMNS (RunColumn objects) to define headers and computations.
    """
    import pandas as pd
    from datetime import datetime
    import os

    if not miner.runs:
        print("\n\033[91mNo runs available within given limits.\033[00m\n")
        return

    the_time = pd.Timedelta(in_time) if in_time is not None else pd.Timedelta("0:30:00")

    if interpolate:
        # settings for interpolated values
        interpolate = True
        extrapolate = 2
        interpolations = {
            "rainfall_total": "linear",
            "runoff": "linear",
            "sediment_concentration": "linear",
            "sediment_flux": "linear",
            "sediment_yield": "linear"
        }

        plot_types = {
            "rainfall_total": "line",
            "runoff": "line",
            "sediment_concentration": "line",
            "sediment_flux": "line",
            "sediment_yield": "line"
        }
    else:
        # settings for stepwise values
        interpolate = False
        extrapolate = -1
        interpolations = {
            "rainfall_total": "linear",
            "runoff": "ffill",
            "sediment_concentration": "ffill",
            "sediment_flux": "ffill",
            "sediment_yield": "ffill"
        }

        plot_types = {
            "rainfall_total": "line",
            "runoff": "step",
            "sediment_concentration": "step",
            "sediment_flux": "step",
            "sediment_yield": "step"
        }


    ensure_directory(output_dir)
    output_file = os.path.join(output_dir, f"_slr_{datetime.now().strftime('%Y%m%d')}.csv")

    collector = TraceCollector()

    # setup CSV and headers
    run_headers = [c.header[lang] for c in RUN_INFO_COLUMNS]
    slr_headers = [c.header[lang] for c in SLR_COLUMNS]

    try:
        with open(output_file, "w", encoding="utf-8") as output_csv:
            write_row_to_csv(output_csv, run_headers + slr_headers)

            ## process sequences
            # runs in sequence should come in pairs dry-very wet
            for seq_id, run_list in miner.sequences.items():
                # the run pair has one mutual SLR accumulator
                print(f"\n### Sequence {seq_id} ###")
                slr_accumulator = SLRAccumulator()

                for r_idx, run in enumerate(run_list, start=1):
                    # SLR calculation must start with crop simulations - fallow runs are excluded from the main loop
                    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
                        continue

                    # fetch reference (fallow) run
                    frun = run.get_reference_run()

                    # SLR value can't be calculated if no reference fallow was found
                    if frun is None:
                        print(f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]}\t-> \033[91mno reference fallow found\033[00m\n")
                        continue

                    print(f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]}\t-> fallow #{frun.id}")

                    ## each iteration of the CROP runs loop 2 rows are created
                    # the rows are written only if all needed data are found
                    # crop run values
                    run_line = []
                    # fallow run values
                    f_run_line = []
                    ctx = {"lang": lang, "no_data_value": no_data_value}

                    # get basic info for the crop run
                    for col in RUN_INFO_COLUMNS:
                        # try:
                        val, issues = col.getter(run, ctx)
                        f_val, f_issues = col.getter(frun, ctx)
                        # except:
                        #     print(col.header[lang])
                        # replace None value with export-specific no_data_value
                        if val is None:
                            val = ctx["no_data_value"]

                        if f_val is None:
                            f_val = ctx["no_data_value"]
                        run_line.append(val)
                        f_run_line.append((f_val))

                        if issues:
                            trace = DataTrace(
                                issues=issues,
                                category="run_metadata",
                                level="run_info_columns",
                                severity=TraceSeverity.WARNING,
                            )
                            collector.add(
                                run_id=run.id,
                                dataset="run_info",
                                trace=trace)

                        if f_issues:
                            trace = DataTrace(
                                issues=f_issues,
                                category="fallow_run_metadata",
                                level="run_info_columns",
                                severity=TraceSeverity.WARNING,
                            )
                            collector.add(
                                run_id=run.id,
                                dataset="run_info",
                                trace=trace)


                    # fetch hydrodata
                    labels = {
                        "rainfall_intensity": "rainfall intensity [mm.h-1]",
                        "rainfall_total": "rainfall total [mm]",
                        "runoff": "runoff [l.min-1]",
                        "sediment_concentration": "sediment concentration [g.l-1]",
                        "discharge": "discharge [l]",
                        "sediment_flux": "sediment flux [g.min-1]",
                        "sediment_yield": "sediment yield [g]",
                    }

                    request = {k: False for k in labels}
                    # sediment yield is required for SLR calculation
                    request['sediment_yield'] = True

                    crop_hydrodata, trace = get_best_hydro_data(
                        run=run,
                        request_map=request,
                        labels_map=labels,
                        interpolation_map=interpolations,
                        return_trace=True,
                    )
                    # collect the trace if any
                    if trace:
                        collector.add(
                            run_id=run.id,
                            dataset="crop_runoff_sediment_data",
                            trace=trace,
                        )

                    # skip line if no runoff-sediment data were retrieved
                    if crop_hydrodata is None or crop_hydrodata.empty:
                        continue

                    fallow_hydrodata, trace = get_best_hydro_data(
                        run=frun,
                        request_map=request,
                        labels_map=labels,
                        interpolation_map=interpolations,
                        return_trace=True,
                    )
                    # collect the trace if any
                    if trace:
                        collector.add(
                            run_id=run.id,
                            dataset="fallow_runoff_sediment_data",
                            trace=trace,
                        )
                    # SLR value can't be calculated if no reference fallow hydrodata were fetched
                    if fallow_hydrodata is None or fallow_hydrodata.empty:
                        continue

                    # fill missing values safely
                    crop_hydrodata = crop_hydrodata.infer_objects(copy=False)
                    fallow_hydrodata = fallow_hydrodata.infer_objects(copy=False)

                    # --- here the records should be OK, let's get to values
                    issues = []
                    # get sediment yield values in specified time
                    sedyield_crop, sub_issues = get_value_in_time(
                        crop_hydrodata,
                        the_time,
                        "sediment_yield",
                        interpolate=interpolate,
                        extrapolate=extrapolate,
                        return_trace=True
                    )

                    # print(f"sedyield_crop: {sedyield_crop}")
                    if pd.isna(sedyield_crop):
                        issues.append(DataIssue(
                            reason=DataAbsenceReason.DATA_NOT_AVAILABLE,
                            source="calculate_SLR",
                            details=f"get_value_in_time returned None for crop sediment yield",
                            causes=sub_issues if sub_issues else ()
                        ))
                        trace = DataTrace(
                            issues=tuple(issues),
                            category="missing_records",
                            level="SLR calculation",
                            severity=TraceSeverity.ERROR,
                        )
                        collector.add(
                            run_id=run.id,
                            dataset="crop_runoff_sediment_data",
                            trace=trace,
                        )
                        continue


                    sedyield_fallow, sub_issues = get_value_in_time(
                        fallow_hydrodata,
                        the_time,
                        "sediment_yield",
                        interpolate=interpolate,
                        extrapolate=extrapolate,
                        return_trace=True
                    )

                    # print(f"sedyield_fallow: {sedyield_fallow}")
                    if pd.isna(sedyield_fallow):
                        issues.append(DataIssue(
                            reason=DataAbsenceReason.DATA_NOT_AVAILABLE,
                            source="calculate_SLR",
                            details=f"get_value_in_time returned None for fallow sediment yield",
                            causes=sub_issues if sub_issues else ()
                        ))
                        trace = DataTrace(
                            issues=tuple(issues),
                            category="missing_records",
                            level="SLR calculation",
                            severity=TraceSeverity.ERROR,
                        )
                        collector.add(
                            run_id=run.id,
                            dataset="fallow_runoff_sediment_data",
                            trace=trace,
                        )
                        continue

                    # update accumulator
                    slr_accumulator.add_run_pair(run, sedyield_crop, sedyield_fallow)

                    # --- CROP ROW (no SLR values) ---
                    crop_ctx = {
                        "lang": lang,
                        "no_data_value": no_data_value,
                        "hydro_df": crop_hydrodata,
                        "time": the_time,
                        "interpolate": interpolate,
                        "extrapolate": 1,
                        "sed_crop": sedyield_crop,
                        "sed_fallow": sedyield_fallow,
                        "slr_acc": slr_accumulator,
                    }

                    crop_slr_row = []

                    for col in SLR_COLUMNS:
                        value, _ = col.getter(run, crop_ctx, return_trace=True)

                        # SLR values should NOT appear on crop rows
                        if col.getter in (slr_ratio_getter, slr_average_getter):
                            value = None

                        crop_slr_row.append(value if value is not None else no_data_value)

                    write_row_to_csv(output_csv, run_line + crop_slr_row)

                    # --- FALLOW ROW (contains SLR values) ---
                    fallow_ctx = {
                        "lang": lang,
                        "no_data_value": no_data_value,
                        "hydro_df": fallow_hydrodata,
                        "time": the_time,
                        "interpolate": interpolate,
                        "extrapolate": 1,
                        "sed_crop": sedyield_crop,
                        "sed_fallow": sedyield_fallow,
                        "slr_acc": slr_accumulator,
                    }

                    fallow_slr_row = []

                    for col in SLR_COLUMNS:
                        value, _ = col.getter(frun, fallow_ctx, return_trace=True)
                        fallow_slr_row.append(value if value is not None else no_data_value)

                    write_row_to_csv(output_csv, f_run_line + fallow_slr_row)


                    # optional: save plots and CSVs for hydrodata
                    if crop_hydrodata is not None:
                        plot_title = f"\n#{run.id} - {czech_date(run.datetime)} >{run.locality.name}< {run.crop.name[lang]} @[{run.plot_id}] - {run.run_type.name[lang]} - {run.ttr}"
                        plot_file = os.path.join(output_dir, f"{run.id}_{run.datetime.strftime('%Y-%m-%d')}_{run.locality.name}_{run.crop.name[lang]}.png")
                        points = {"sediment_yield": [(the_time, sedyield_crop)]}

                        plot_hydro_data(crop_hydrodata, plot_file, plot_types, extra_points=points, plot_title=plot_title)
                        crop_hydrodata.to_csv(os.path.join(output_dir, f"{run.id}.csv"),
                                              index=True, sep=",", decimal=".")

                    if fallow_hydrodata is not None:
                        plot_title = f"\n#{frun.id} - {czech_date(frun.datetime)} >{frun.locality.name}< {frun.crop.name[lang]} @[{frun.plot_id}] - {frun.run_type.name[lang]} - {frun.ttr}"
                        plot_file = os.path.join(output_dir, f"{frun.id}_{frun.datetime.strftime('%Y-%m-%d')}_{frun.locality.name}_{frun.crop.name[lang]}.png")
                        points = {"sediment_yield": [(the_time, sedyield_fallow)]}
                        plot_hydro_data(fallow_hydrodata, plot_file, plot_types, extra_points=points, plot_title=plot_title)
                        fallow_hydrodata.to_csv(os.path.join(output_dir, f"{frun.id}.csv"),
                                                index=True, sep=",", decimal=".")

        print(f"\nSLR CSV exported to: {output_file}")

        # do something with the collected traces
        if output_trace_path:
            collector.dump_json_by_run(output_trace_path)

    except PermissionError:
        print(f"\033[91mspecified output file '{output_file}' is being used by another application\033[00m\n")
        return