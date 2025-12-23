# from pipeline import Miner
from ..miner import Miner
from ..run_filter import RunFilter
from .filesystem import *
from .writers import *
import os
from ..utilities.utilities import czech_date
from ..utilities.plotters import *

from ..exceptions import RecordSetNotComplete


def calculate_SLR(miner: Miner, output_dir=None,
                  lang="en",
                  no_data_value="",
                  in_time=None,
                  interpolate=True):
    import pandas as pd

    # default calculation time is 0:30:00 from the run start but other can be specified

    the_time = pd.Timedelta(in_time) if in_time is not None else pd.Timedelta("0:30:00")

    try:
        os.mkdir(output_dir) if not os.path.isdir(output_dir) else None
    except OSError as error:
        print("Selected directory for the dump does not exist and it's not possible to create it.")
        print("Dump failed.")
        return

    output_file = os.path.join(output_dir, f"_slr_{datetime.now().strftime('%Y%m%d')}.csv")

    if not self.runs:
        print("\n\033[91mNo runs available within given limits.\033[00m\n")
        return

    column_headers = {
        "cz": ["t2", "srážková intenzita [mm/min]", "srážkový úhrn [mm]", "průtok [l/min]", "celkový odtok [l]",
               "koncentrace sedimentu [g/l]", "tok sedimentu[g/min]", "ztráta půdy [g]", "SLR", "SLR průměr"
               ],
        "en": ["t2", "rainfall intensity [mm.min-1]", "rainfall total [mm]", "flow rate [l.min-1]",
               "total discharge [l]",
               "SS concentration [g.l-1]", "SS flux [g.min-1]", "sediment yield[g]", "SLR", "SLR averaged"
               ]
    }
    # separators for the export to CSV
    local_seps = {"celld": {"cz": ";", "en": ","}, "decd": {"cz": ",", "en": "."}}
    #
    # # hydrodata request and labels definition
    # rain_int_label = "rainfall_intensity"
    # rain_tot_label = "rainfall_total"
    # runoff_label = "runoff"
    # discharge_label = "discharge"
    # sed_conc_label = "sediment_concentration"
    # sed_flux_label = "sediment_flux"
    # sed_yield_label = "sediment_yield"

    rain_int_label = "rainfall intensity [mm.hour-1]"
    rain_tot_label = "rainfall total [mm]"
    runoff_label = "runoff [l.s-1]"
    discharge_label = "discharge [l]"
    sed_conc_label = "sediment concentration [g.l-1]"
    sed_flux_label = "sediment flux [g.min-1]"
    sed_yield_label = "sediment yield [g]"

    labels = {
        "rainfall_intensity": rain_int_label,
        "rainfall_total": rain_tot_label,
        "runoff": runoff_label,
        "sediment_concentration": sed_conc_label,
        "discharge": discharge_label,
        "sediment_flux": sed_flux_label,
        "sediment_yield": sed_yield_label
    }
    # all requests are False to get all runs
    request = {"runoff": True,
               "sediment_flux": True}

    if interpolate:
        # settings for interpolated values
        interpolate = True
        extrapolate = 2
        interpolations = {
            rain_tot_label: "linear",
            runoff_label: "linear",
            sed_conc_label: "linear",
            sed_flux_label: "linear"
        }

        plots = {
            rain_tot_label: "line",
            runoff_label: "line",
            sed_conc_label: "line",
            sed_flux_label: "line"
        }
    else:
        # settings for stepwise values
        extrapolate = -1
        interpolations = {
            rain_tot_label: "linear",
            runoff_label: "ffill",
            sed_conc_label: "ffill",
            sed_flux_label: "ffill"
        }

        plots = {
            rain_tot_label: "line",
            runoff_label: "step",
            sed_conc_label: "step",
            sed_flux_label: "step"
        }

    # self.runoffdb.load_runs(date_from=date_from, date_to=date_to)

    self.runoffdb.log_file_path = os.path.join(output_dir, "_log.txt")

    # open the output file for writing
    try:
        output_csv = open(output_file, "w", encoding="utf-8")
    except PermissionError:
        print(f"\033[91mspecified output file '{output_file}' is being used by another application\033[00m")
        return
    # get the run properties column headers from the first run
    values, headers = list(self.runs.values())[0].get_info_array(lang=lang)
    # extend with export specific column headers
    headers.extend(column_headers[lang])

    writeRowToCSV(output_csv, headers)

    no_fallow = 0
    one_fallow = 0
    more_fallows = 0

    updates = []

    for seq_id, run_list in self.sequences.items():
        print(f"\n### {seq_id} ###")
        for run in run_list:
            print(f"\t{run.id} - {run.run_type.name[lang]}")

    for seq_id, run_list in self.sequences.items():
        print(f"\n### {seq_id} ###")

        dry_crop_sedyield = 0
        verywet_crop_sedyield = 0
        dry_fallow_sedyield = 0
        verywet_fallow_sedyield = 0

        r = 0
        for run in run_list:
            r += 1
            if run.crop_id != CULTIVATED_FALLOW_CROP_ID:
                print(
                    f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
                # start the line with the general info
                line = run.get_info_array(no_data_value=no_data_value, lang=lang)[0]

                try:
                    crop_hydrodata = run.get_best_hydro_data(request_map=request, labels_map=labels,
                                                             interpolation_map=interpolations)
                    # if crop_hydrodata is None or crop_hydrodata[sed_yield_label].empty:
                    #     continue
                except RecordSetNotComplete as e:
                    print(f"\n\033[91mHydro-sediment record set of crop run #{run.id} is not complete\033[00m")
                    print(
                        f"\033[91mmissing record{'s' if len(e.missing_records) > 1 else ''}: {', '.join(e.missing_records)}\033[00m")
                    line.extend(6 * no_data_value)
                else:
                    print("crop hydrodata fetched")

                    line.append(format_timedelta(the_time - run.ttr))
                    line.append(run.get_rainfall_intensity_value(RAINFALL_INTENSITY_MMH_UNIT_ID) or no_data_value)
                    line.append(get_value_in_time(crop_hydrodata,
                                                  the_time,
                                                  rain_tot_label,
                                                  interpolate=interpolate,
                                                  extrapolate=extrapolate))
                    line.append(get_value_in_time(crop_hydrodata,
                                                  the_time,
                                                  runoff_label,
                                                  interpolate=interpolate,
                                                  extrapolate=extrapolate))
                    line.append(get_value_in_time(crop_hydrodata,
                                                  the_time,
                                                  discharge_label,
                                                  interpolate=interpolate,
                                                  extrapolate=extrapolate))
                    line.append(get_value_in_time(crop_hydrodata,
                                                  the_time,
                                                  sed_conc_label,
                                                  interpolate=interpolate,
                                                  extrapolate=extrapolate))
                    print(f"crop_hydrodata.columns: {crop_hydrodata.columns}")
                    sedflux = get_value_in_time(crop_hydrodata,
                                                the_time,
                                                sed_flux_label,
                                                interpolate=interpolate,
                                                extrapolate=extrapolate)
                    line.append(sedflux)
                    if crop_hydrodata is None or crop_hydrodata[sed_yield_label].empty:
                        sedyield = 0
                    else:
                        sedyield = get_value_in_time(crop_hydrodata,
                                                     the_time,
                                                     sed_yield_label,
                                                     interpolate=interpolate,
                                                     extrapolate=extrapolate) or 0

                        if run.run_type_id == DRY_RUN_TYPE_ID:
                            dry_crop_sedyield = sedyield
                        else:
                            verywet_crop_sedyield = sedyield

                line.append(sedyield)

                # print(crop_hydrodata.columns.tolist())

                points = {sed_flux_label: [(the_time, sedflux)]}
                plots_filename = f"{run.id}_{run.datetime.strftime('%Y-%m-%d')}_{run.locality.name}_{run.crop.name[lang]}_{run.run_type.name[lang]}"
                plot_title = f"\n#{run.id} - {czech_date(run.datetime)} >{run.locality.name}< {run.crop.name[lang]} @[{run.plot_id}] - {run.run_type.name[lang]} - {run.ttr}"

                run.plot_hydro_data(crop_hydrodata, os.path.join(output_dir, plots_filename + ".png"), plots,
                                    extra_points=points, plot_title=plot_title)

                crop_hydrodata.to_csv(os.path.join(output_dir, plots_filename + ".csv"),
                                      index=True,
                                      sep=local_seps["celld"][lang],
                                      decimal=local_seps["decd"][lang])
                # print(f"rainfall total: {crop_hydrodata[rain_tot_label]}")

                # try to get the reference run
                frun = run.get_reference_run()

                if frun is not None:
                    print(
                        f"\t\033[1;33m#{frun.id} - {czech_date(frun.datetime)} - {frun.locality.name} - {frun.crop.name[lang]} - [{frun.plot_id}] {frun.run_type.name[lang]} - {frun.ttr}\033[00m")

                    line2 = frun.get_info_array(no_data_value=no_data_value, lang=lang)[0]

                    try:
                        fallow_hydrodata = frun.get_best_hydro_data(request_map=request, labels_map=labels,
                                                                    interpolation_map=interpolations)
                    except RecordSetNotComplete as e:
                        print(f"\n\n\033[91mHydro-sediment record set of fallow run #{frun.id} is not complete\033[00m")
                        print(
                            f"\033[91mmissing record{'s' if len(e.missing_records) > 1 else ''}: {', '.join(e.missing_records)}\033[00m")
                        # continue
                    else:
                        print("fallow hydrodata fetched")

                    line2.append(format_timedelta(the_time - frun.ttr))

                    line2.append(frun.get_rainfall_intensity_value(RAINFALL_INTENSITY_MMH_UNIT_ID) or no_data_value)
                    line2.append(get_value_in_time(fallow_hydrodata,
                                                   the_time,
                                                   rain_tot_label,
                                                   interpolate=interpolate,
                                                   extrapolate=extrapolate))
                    line2.append(get_value_in_time(fallow_hydrodata,
                                                   the_time,
                                                   runoff_label,
                                                   interpolate=interpolate,
                                                   extrapolate=extrapolate))
                    line2.append(get_value_in_time(fallow_hydrodata,
                                                   the_time,
                                                   discharge_label,
                                                   interpolate=interpolate,
                                                   extrapolate=extrapolate))
                    line2.append(get_value_in_time(fallow_hydrodata,
                                                   the_time,
                                                   sed_conc_label,
                                                   interpolate=interpolate,
                                                   extrapolate=extrapolate))

                    sedflux = get_value_in_time(fallow_hydrodata,
                                                the_time,
                                                sed_flux_label,
                                                interpolate=interpolate,
                                                extrapolate=extrapolate)
                    line2.append(sedflux)

                    if fallow_hydrodata is None or fallow_hydrodata[sed_yield_label].empty:
                        sedyield2 = 0
                    else:
                        sedyield2 = get_value_in_time(fallow_hydrodata,
                                                      the_time,
                                                      sed_yield_label,
                                                      interpolate=interpolate,
                                                      extrapolate=extrapolate) or 0

                        if frun.run_type_id == DRY_RUN_TYPE_ID:
                            dry_fallow_sedyield = sedyield2
                        else:
                            verywet_fallow_sedyield = sedyield2
                    line2.append(sedyield2)

                    points = {sed_flux_label: [(the_time, sedflux)]}
                    plots_filename = f"{frun.id}_{frun.datetime.strftime('%Y-%m-%d')}_{frun.locality.name}_{frun.crop.name[lang]}_{frun.run_type.name[lang]}"
                    plot_title = f"\n#{frun.id} - {czech_date(frun.datetime)} >{frun.locality.name}< {frun.crop.name[lang]} @[{frun.plot_id}] - {frun.run_type.name[lang]} - {frun.ttr}"

                    run.plot_hydro_data(fallow_hydrodata, os.path.join(output_dir, plots_filename + ".png"), plots,
                                        extra_points=points, plot_title=plot_title)

                    fallow_hydrodata.to_csv(os.path.join(output_dir, plots_filename + ".csv"),
                                            index=True,
                                            sep=local_seps["celld"][lang],
                                            decimal=local_seps["decd"][lang])

                    line.append(sedyield / sedyield2)

                    # at the last run in sequence save the 'combined SLR value'
                    if r == len(run_list):
                        line.append((dry_crop_sedyield + verywet_crop_sedyield) / (
                                    dry_fallow_sedyield + verywet_fallow_sedyield))

                    writeRowToCSV(output_csv, line)
                    writeRowToCSV(output_csv, line2)

            else:
                # print(f"\n\033[96m==> #{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}\033[00m")
                pass

    output_csv.close()

    print(f"\n\n\033[91mno fallow: {no_fallow}\033[00m")
    print(f"\033[92mone fallow: {one_fallow}\033[00m")
    print(f"\033[93mmore fallows: {more_fallows}\033[00m")

    # print(";\n".join(updates))
    return