# from pipeline import Miner
from .filesystem import *
from .writers import *
from ..diagnostics.collector import TraceCollector
from ..utilities.utilities import czech_date


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

def build_interpolation_config(interpolate: bool):
    if interpolate:
        return {
            "interpolate": True,
            "extrapolate": 2,
            "interpolations": {
                "rainfall_total": "linear",
                "runoff": "linear",
                "sediment_concentration": "linear",
                "sediment_flux": "linear",
                "sediment_yield": "linear",
            },
            "plot_types": {
                "rainfall_total": "line",
                "runoff": "line",
                "sediment_concentration": "line",
                "sediment_flux": "line",
                "sediment_yield": "line",
            },
        }
    else:
        return {
            "interpolate": False,
            "extrapolate": -1,
            "interpolations": {
                "rainfall_total": "linear",
                "runoff": "ffill",
                "sediment_concentration": "ffill",
                "sediment_flux": "ffill",
                "sediment_yield": "ffill",
            },
            "plot_types": {
                "rainfall_total": "line",
                "runoff": "step",
                "sediment_concentration": "step",
                "sediment_flux": "step",
                "sediment_yield": "step",
            },
        }

def fetch_hydro_sediment_df(run,
                            request,
                            # labels,
                            interpolations,
                            collector,
                            dataset_name):
    hydro_df, trace = get_hydro_sediment_timeline(
        run=run,
        request_map=request,
        # labels_map=labels,
        interpolation_map=interpolations,
        return_trace=True,
    )

    if trace:
        collector.add(
            run_id=run.id,
            dataset=dataset_name,
            trace=trace,
        )

    if hydro_df is None or hydro_df.empty:
        return None

    return hydro_df.infer_objects(copy=False)

def resolve_sediment_yield(
    hydro_df,
    the_time,
    interpolate,
    extrapolate,
    run_id,
    dataset,
    collector,
):
    value, sub_issues = get_value_in_time(
        hydro_df,
        the_time,
        "sediment_yield",
        interpolate=interpolate,
        extrapolate=extrapolate,
        return_trace=True,
    )

    if pd.isna(value):
        trace = DataReport(
            issues=(
                DataIssue(
                    reason=DataAbsenceReason.DATA_NOT_AVAILABLE,
                    source="calculate_SLR",
                    details="get_value_in_time returned None for sediment yield",
                    causes=sub_issues if sub_issues else (),
                ),
            ),
            category="missing_records",
            level="SLR calculation",
            severity=IssueSeverity.ERROR,
        )
        collector.add(run_id=run_id, dataset=dataset, trace=trace)
        return None

    return value

def build_slr_row(run, ctx, columns, hide_slr=False):
    row = []
    for col in columns:
        value, _ = col.getter(run, ctx, return_trace=True)

        if hide_slr and col.getter in (slr_ratio_getter, slr_average_getter):
            value = None

        row.append(value if value is not None else ctx["no_data_value"])

    return row

def calculate_SLR(
    miner,
    output_dir,
    lang="en",
    no_data_value="",
    in_time=None,
    interpolate=True,
    output_trace_path=None,
):
    import pandas as pd
    from datetime import datetime
    import os

    if not miner.runs:
        print("\nNo runs available within given limits.\n")
        return

    the_time = pd.Timedelta(in_time) if in_time else pd.Timedelta("0:30:00")

    config = build_interpolation_config(interpolate)
    # get the runoff and sediment load variables sub-registry from runoffdb
    var_registry = miner.runoffdb.variable_registry.subregistry_by_group(VariableGroup.HYDRO_SEDIMENT)
    # build the labels
    labels = var_registry.default_labels()
    # sediment yield is essential for SLR calculation
    request = {"sediment_yield": True}

    ensure_directory(output_dir)
    output_file = os.path.join(
        output_dir,
        f"_slr_{datetime.now().strftime('%Y%m%d')}.csv",
    )

    collector = TraceCollector()

    run_headers = [resolve_header_of_column(c, var_registry, lang) for c in RUN_PROPERTIES]
    slr_headers = [resolve_header_of_column(c, var_registry, lang) for c in SLR_COLUMNS]

    try:
        with open(output_file, "w", encoding="utf-8") as output_csv:
            write_row_to_csv(output_csv, run_headers + slr_headers)
            num_seq = len(miner.sequences)

            # runs in sequence should come in pairs dry-very wet
            for seq_id, run_list in miner.sequences.items():
                # the run pair has one mutual SLR accumulator
                slr_accumulator = SLRAccumulator()

                print(f"\n### Sequence {seq_id} ###")

                for run in run_list:
                    # SLR calculation must start with crop simulations - fallow runs are excluded from the main loop
                    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
                        continue

                    # fetch reference (fallow) run
                    frun = run.get_reference_run()
                    if frun is None:
                        print(f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]}\t-> \033[91mno reference fallow found\033[00m\n")
                        continue

                    crop_df = fetch_hydro_sediment_df(
                        run,
                        request,
                        # labels,
                        config["interpolations"],
                        collector,
                        "crop_runoff_sediment_data",
                    )

                    fallow_df = fetch_hydro_sediment_df(
                        frun,
                        request,
                        # labels,
                        config["interpolations"],
                        collector,
                        "fallow_runoff_sediment_data",
                    )

                    if crop_df is None or crop_df.empty:
                        print(f"\t\tcrop_df empty\n")
                        continue

                    if fallow_df is None or fallow_df.empty:
                        print(f"\t\tfallow_df empty\n")
                        continue

                    sed_crop = resolve_sediment_yield(
                        crop_df,
                        the_time,
                        config["interpolate"],
                        config["extrapolate"],
                        run.id,
                        "crop_runoff_sediment_data",
                        collector,
                    )

                    sed_fallow = resolve_sediment_yield(
                        fallow_df,
                        the_time,
                        config["interpolate"],
                        config["extrapolate"],
                        run.id,
                        "fallow_runoff_sediment_data",
                        collector,
                    )

                    if sed_crop is None or sed_fallow is None:
                        continue

                    slr_accumulator.add_run_pair(run, sed_crop, sed_fallow)

                    # only after all essential data are available the rows are being assembled
                    print(
                        f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]}\t-> fallow #{frun.id}")

                    ## each iteration of the CROP runs loop 2 rows are created
                    # the rows are written only if all needed data are found
                    # crop run values
                    crop_row = []
                    # fallow run values
                    fallow_row = []
                    ctx = {"lang": lang, "no_data_value": no_data_value}


                    # get basic info for the crop run
                    for col in RUN_PROPERTIES:
                        # try:
                        val, issues = col.getter(run, ctx)
                        f_val, f_issues = col.getter(frun, ctx)
                        # except:
                        #     print(col.header[lang])
                        # replace None value with exports-specific no_data_value
                        if val is None:
                            val = ctx["no_data_value"]

                        if f_val is None:
                            f_val = ctx["no_data_value"]
                        crop_row.append(val)
                        fallow_row.append(f_val)

                        if issues:
                            trace = DataReport(
                                issues=issues,
                                category="run_metadata",
                                level="run_info_columns",
                                severity=IssueSeverity.WARNING,
                            )
                            collector.add(
                                run_id=run.id,
                                dataset="run_info",
                                trace=trace)

                        if f_issues:
                            trace = DataReport(
                                issues=f_issues,
                                category="fallow_run_metadata",
                                level="run_info_columns",
                                severity=IssueSeverity.WARNING,
                            )
                            collector.add(
                                run_id=run.id,
                                dataset="run_info",
                                trace=trace)


                    slr_ctx = {
                        "lang": lang,
                        "no_data_value": no_data_value,
                        "time": the_time,
                        "interpolate": config["interpolate"],
                        "extrapolate": 1,
                        "sed_crop": sed_crop,
                        "sed_fallow": sed_fallow,
                        "slr_acc": slr_accumulator,
                    }

                    crop_row.extend(build_slr_row(
                        run,
                        {**slr_ctx, "hydro_df": crop_df},
                        SLR_COLUMNS,
                        hide_slr=True,
                        )
                    )

                    fallow_row.extend(build_slr_row(
                        frun,
                        {**slr_ctx, "hydro_df": fallow_df},
                        SLR_COLUMNS,
                        )
                    )

                    write_row_to_csv(output_csv, crop_row)
                    write_row_to_csv(output_csv, fallow_row)

        if output_trace_path:
            collector.dump_json_by_run(output_trace_path)

        print(f"\nSLR CSV exported to: {output_file}")
    except PermissionError:
        print(f"\033[91mspecified output file '{output_file}' is being used by another application\033[00m\n")
        return