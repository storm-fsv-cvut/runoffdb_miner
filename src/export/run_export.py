from .filesystem import *
from .writers import *
from ..utilities.plotters import *

def dump_run_to_dir(run, parent_dir, lang, no_data_value):
    run_dir = make_run_dir(parent_dir, run)
    if not run_dir:
        return

    write_run_metadata_json(run, run_dir, lang)
    write_all_run_records_to_csv(run, run_dir, lang, no_data_value)

    write_hydro_sediment_data_to_csv(run, run_dir, lang, no_data_value)
    return run_dir

def write_hydro_sediment_data_to_csv(run, output_path, lang, no_data_value, lables=None, request=None):
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
    request = {key: False for key in labels}

    hydro_data = run.get_best_hydro_data(request_map=request,
                                         labels_map=labels)


    print(hydro_data)
    # print(hydro_data.index)
    print(hydro_data.columns.tolist())
    plot_hydro_data(hydro_data, os.path.join(os.path.dirname(output_path), "runoff.png"), [rain_int_label, rain_tot_label, runoff_label])

    local_seps = {"celld": {"cz": ";", "en": ","}, "decd": {"cz": ",", "en": "."}}
    # hydro_data.to_csv(output_path),
    #                index=index,
    #                sep=local_seps["celld"][lang],
    #                decimal=local_seps["decd"][lang],
    #                header=column_headers)

    # miner.runoffdb.save_log(run_log_path)

