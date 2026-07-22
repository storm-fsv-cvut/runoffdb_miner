# -*- coding: utf-8 -*-
import os
import json
import math
import locale
import re

from collections import defaultdict
from contextlib import contextmanager

from src.entities.type_entities import *
from src.entities.runoffdb import RunoffDB
from src.utilities.plotters import *
from src.filters.run_filter import RunFilter
from src.services.hydro_data import *

lang = "en"


def plots_overview(runoffdb):
    print(f"\n\nPLOTS OVERVIEW ============================")
    runoffdb.show_plots()
    return


def simulators_overview(runoffdb, lang="en"):
    print(f"\n\nSIMULATORS OVERVIEW ============================")
    runoffdb.show_simulators(lang=lang)
    return


def methodics_overview(runoffdb, lang="en"):
    print(f"\n\nMETHODICS OVERVIEW ============================")
    runoffdb.show_methodics(lang=lang)


def localities_overview(runoffdb):
    print(f"\n\nLOCALITIES OVERVIEW ============================")
    runoffdb.show_localities()


def agrotechnologies_overview(runoffdb):
    print(f"\n\nAGROTECHNOLOGIES OVERVIEW ============================")
    runoffdb.show_agrotechnologies()


def export_methodics(runoffdb, output_path, lang="en"):
    export = runoffdb.export_methodics(lang)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(export, f, ensure_ascii=False, indent=4)

    print(f"All methodics successfully exported to '{output_path}'")
    return


def show_run_notes(runoffdb, lang="en"):
    print(f"\n\nRUN NOTES OVERVIEW ============================")

    if not runoffdb.runs:
        print("\n\033[91mNo runs available within given limits.\033[00m\n")
        return

    for run in list(runoffdb.runs.values()):
        print(
            f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
        print(run.get_notes(lang))

    return

def find_fallow(runoffdb):
    """
    Searches for matching reference fallow runs for all crop-runs.
    Fallow fits if the date, locality, run type and simulator are equal and the plot has equal dimensions.
    """

    if not runoffdb.runs:
        print("\n\033[91mNo runs available within given limits.\033[00m\n")
        return

    runs = list(runoffdb.runs.values())

    no_fallow = []
    one_fallow_recorded = []
    one_fallow_found = []
    more_fallows = []

    updates = []
    for run in runs:
        # only for non-fallow runs ...
        if run.crop_id != 1:
            print \
                (f"\n\033[0;32m#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - [{run.plot_id}] {run.plot.plot_length:.0f}x{run.plot.plot_width:.0f}m - {run.run_type.name[lang]} - {run.ttr}\033[32m")

            # try to get the reference run from Run
            frun = run.get_reference_run()
            if frun is not None:
                one_fallow_recorded.append(run.id)
                print(
                    f"\t\033[0;33m#{frun.id} - {czech_date(frun.datetime)} - {frun.locality.name} - {frun.crop.name[lang]} - [{frun.plot_id}] {run.plot.plot_length:.0f}x{run.plot.plot_width:.0f}m - {frun.run_type.name[lang]} - {frun.ttr}\033[33m")
            else:
                fallows = run.find_fellow_fallow()
                if len(fallows) == 0:
                    print("\t\033[91mno fellow fallow found\033[00m")
                    no_fallow.append(run.id)
                    # rdb.log(run.id, f"no fallow found")

                elif len(fallows) == 1:
                    frun = fallows[0]
                    print(
                        f"\t\033[1;33m#{frun.id} - {czech_date(frun.datetime)} - {frun.locality.name} - {frun.crop.name[lang]} - [{frun.plot_id}] {run.plot.plot_length:.0f}x{run.plot.plot_width:.0f}m - {frun.run_type.name[lang]} - {frun.ttr}\033[1;33m")

                    # print(f"\t\033[92mmy fellow fallow is #{frun.id}\033[00m")
                    # rdb.log(run.id, f"single fallow #{fallows[0].id}")
                    updates.append(f"update `run` set `reference_run_id` = {fallows[0].id} where `id` = {run.id}")

                    one_fallow_found.append(run.id)
                else:
                    print \
                        (f"\t\033[38;5;208mfallows with matching properties: {', '.join([str(f.id) for f in fallows])}\033[0m")
                    more_fallows.append(run.id)
                    # rdb.log(run.id, f"multiple matching fallows found: {', '.join([str(f.id) for f in fallows])}")

        else:
            # print(f"\n\033[96m==> #{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}\033[00m")
            pass

    print(f"\n\n\033[92mone fallow stored in DB: {len(one_fallow_recorded)}\033[00m")
    print(f"\033[93mone fallow found (but not stored in DB): {len(one_fallow_found)}\033[00m")

    print(f"\n\033[91mno fallow recorded nor found: {len(no_fallow)}\033[00m")
    print(f"\033[93mmore fallows found: {len(more_fallows)}\033[00m")

    if len(no_fallow) > 0:
        print("\nVegetation runs missing reference fallow:\n" + ", ".join([str(f) for f in no_fallow]))
    if len(more_fallows) > 0:
        print("\nVegetation runs with more matching fallows:\n" + ", ".join([str(f) for f in more_fallows]))
    if len(updates) > 0:
        print("\n" + ";\n".join(updates) + ";\n")
    return

