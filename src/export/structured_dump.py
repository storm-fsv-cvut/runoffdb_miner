# from pipeline import Miner
from ..miner import Miner
from ..run_filter import RunFilter
from .filesystem import *
from .writers import *
from .run_export import *
import os
from ..utilities.utilities import czech_date
from ..utilities.plotters import *


def generate_structured_dump(miner: Miner, root_path, lang="en", no_data_value="NA"):
    import pandas as pd

    if not ensure_directory(root_path):
        return

    # get all dates when any simulation occurred
    all_days = miner.get_simulation_days()

    for day in all_days:
        # get runs of the day
        day_runs = miner.get_runs(RunFilter(date_from=day, date_to=day))
        # no runs on day with simulations is a result of unfinished/messed-up entry in DB (run group without any run)
        if day_runs is not None:
            # crete directory for the day
            day_dir = make_day_dir(root_path, day)
            if not day_dir:
                # if the folder does not exist, nothing can be writen
                continue

            for run in day_runs:

                print("\n " +80 *"-")
                print \
                    (f"#{run.id} - {czech_date(day)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]}")
                print(80 * "-")

                run_dir = dump_run_to_dir(run, day_dir, lang, no_data_value)

                write_hydro_sediment_data_to_csv(run, run_dir, )

    return
