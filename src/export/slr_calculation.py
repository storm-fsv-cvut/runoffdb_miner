# from pipeline import Miner
from src.setup.entity_ids import *
from src.export.schemas.column_sets import *
from .filesystem import *
from .writers import *
import os
from ..utilities.utilities import czech_date
from ..utilities.plotters import *

class SLRAccumulator:
    """
    Tracks crop vs fallow sediment yields to compute combined SLR.
    """
    def __init__(self):
        self.dry_crop = 0
        self.verywet_crop = 0
        self.dry_fallow = 0
        self.verywet_fallow = 0

    def add(self, run, sed_crop, sed_fallow):
        if run.run_type_id == DRY_RUN_TYPE_ID:
            self.dry_crop = sed_crop
            self.dry_fallow = sed_fallow
        else:
            self.verywet_crop = sed_crop
            self.verywet_fallow = sed_fallow

    def combined_slr(self):
        try:
            total_crop = self.dry_crop + self.verywet_crop
            total_fallow = self.dry_fallow + self.verywet_fallow
            return total_crop / total_fallow if total_fallow else None
        except Exception:
            return None

def calculate_SLR(self, output_dir, lang="en", no_data_value="", in_time=None, interpolate=True):
    """
    Export SLR values for all runs in schema-driven fashion.
    The function uses SLR_COLUMNS (RunColumn objects) to define headers and computations.
    """
    import pandas as pd
    from datetime import datetime
    import os

    the_time = pd.Timedelta(in_time) if in_time is not None else pd.Timedelta("0:30:00")

    ensure_directory(output_dir)
    output_file = os.path.join(output_dir, f"_slr_{datetime.now().strftime('%Y%m%d')}.csv")

    if not self.runs:
        print("\n\033[91mNo runs available within given limits.\033[00m\n")
        return

    # ------------------------
    # Setup CSV and headers
    # ------------------------
    run_headers = [c.header[lang] for c in RUN_INFO_COLUMNS]
    slr_headers = [c.header[lang] for c in SLR_COLUMNS]

    try:
        with open(output_file, "w", encoding="utf-8") as output_csv:
            write_row_to_csv(output_csv, run_headers + slr_headers)

            # ------------------------
            # Process sequences
            # ------------------------
            for seq_id, run_list in self.sequences.items():
                print(f"\n### Sequence {seq_id} ###")
                slr_acc = SLRAccumulator()

                for r_idx, run in enumerate(run_list, start=1):
                    if run.crop_id == CULTIVATED_FALLOW_CROP_ID:
                        continue

                    print(f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]}")

                    # get run-level info
                    run_line = [col.getter(run, {"lang": lang, "no_data_value": no_data_value})
                                for col in RUN_INFO_COLUMNS]

                    # fetch hydrodata
                    labels_map = {
                        "rainfall_intensity": "rainfall_intensity",
                        "rainfall_total": "rainfall_total",
                        "runoff": "runoff",
                        "sediment_concentration": "sediment_concentration",
                        "discharge": "discharge",
                        "sediment_flux": "sediment_flux",
                        "sediment_yield": "sediment_yield",
                    }
                    request_map = {k: True for k in labels_map}

                    # try:
                    crop_hydrodata = get_best_hydro_data(
                        run=run,
                        request_map=request_map,
                        labels_map=labels_map
                    )
                    # except RecordSetNotComplete as e:
                    #     print(f"\033[91mHydro-sediment record set of crop run #{run.id} incomplete.\033[00m")
                    #     write_row_to_csv(output_csv, run_line + [no_data_value] * len(SLR_COLUMNS))
                    #     continue

                    # fill missing values safely
                    if crop_hydrodata is not None:
                        crop_hydrodata = crop_hydrodata.infer_objects(copy=False)
                        crop_hydrodata.fillna(no_data_value, inplace=True)

                    # fetch reference (fallow) run
                    frun = run.get_reference_run()
                    sed_crop = get_value_in_time(crop_hydrodata, the_time, "sediment_yield", interpolate=interpolate) if crop_hydrodata is not None else 0
                    sed_fallow = 0
                    fallow_hydrodata = None

                    if frun:
                        fallow_hydrodata = get_best_hydro_data(
                            run=frun,
                            request_map=request_map,
                            labels_map=labels_map
                        )

                        if fallow_hydrodata is not None:
                            fallow_hydrodata = fallow_hydrodata.infer_objects(copy=False)
                            fallow_hydrodata.fillna(no_data_value, inplace=True)
                            sed_fallow = get_value_in_time(fallow_hydrodata, the_time, "sediment_yield", interpolate=interpolate) or 0

                    # update SLR accumulator
                    slr_acc.add(run, sed_crop, sed_fallow)

                    # build context for SLR_COLUMNS getters
                    ctx = {
                        "lang": lang,
                        "no_data_value": no_data_value,
                        "time": the_time,
                        "interpolate": interpolate,
                        "sed_crop": sed_crop,
                        "sed_fallow": sed_fallow,
                        "slr_acc": slr_acc,
                        "hydro_df": crop_hydrodata,  # optional, still passed via ctx
                    }

                    # compute schema-driven SLR row
                    slr_row = [col.getter(run, ctx) for col in SLR_COLUMNS]

                    # write CSV row
                    write_row_to_csv(output_csv, run_line + slr_row)

                    # optional: save plots and CSVs for hydrodata
                    if crop_hydrodata is not None:
                        plot_file = os.path.join(output_dir, f"{run.id}_{run.datetime.strftime('%Y-%m-%d')}_{run.locality.name}_{run.crop.name[lang]}.png")
                        run.plot_hydro_data(crop_hydrodata, plot_file, {}, extra_points={})
                        crop_hydrodata.to_csv(os.path.join(output_dir, f"{run.id}.csv"),
                                              index=True, sep=",", decimal=".")

                    if fallow_hydrodata is not None:
                        plot_file = os.path.join(output_dir, f"{frun.id}_{frun.datetime.strftime('%Y-%m-%d')}_{frun.locality.name}_{frun.crop.name[lang]}.png")
                        frun.plot_hydro_data(fallow_hydrodata, plot_file, {}, extra_points={})
                        fallow_hydrodata.to_csv(os.path.join(output_dir, f"{frun.id}.csv"),
                                                index=True, sep=",", decimal=".")
        print(f"\nSLR CSV exported to: {output_file}")

    except PermissionError:
        print(f"\033[91mspecified output file '{output_file}' is being used by another application\033[00m\n")
        return