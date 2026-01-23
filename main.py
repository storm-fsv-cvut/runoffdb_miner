# -*- coding: utf-8 -*-
"""
@author: Jan Devátý
"""

from src.miner import Miner
from src.logging.logger import RunLogger
from src.run_filter import RunFilter
from src.export.structured_dump import generate_structured_dump
from src.export.interval_export import generate_interval_values_csv, generate_intervals_csv_by_simulator
from src.export.slr_calculation import calculate_SLR
from src.project_structure import project_tree
from datetime import datetime


if __name__ == '__main__':
    start_time = datetime.now()
    # lang = "cz"
    lang = "en"

    filter_all = RunFilter(date_from=datetime.fromisoformat("1993-01-01"),
                       date_to=None,
                       simulators=None,
                       localities=None,
                       crops=None,
                       with_runoff_only=False)

    filter_MDS = RunFilter(date_from=datetime.fromisoformat("2025-01-01"),
                       date_to=datetime.fromisoformat("2025-12-31"),
                       simulators=None, # [7, 10],
                       localities=None,
                       crops=None,
                       with_runoff_only=False)

    filter_Puclice = RunFilter(localities=8)

    with Miner(filter_Puclice) as miner:

        # miner.agrotechnologies_overview()
        generate_interval_values_csv(miner,
                                     f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}.csv",
                                     lang=lang,
                                     no_data_value="",
                                     output_trace_path=f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}_trace.json")
        #
        # generate_intervals_csv_by_simulator(miner,
        #                                     f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}",
        #                                     lang=lang,
        #                                     no_data_value="")
        # miner.simulators_overview(lang)
        # miner.plots_overview()
        # miner.methodics_overview(lang)

        # miner.compare_discharge_calculation_methods(log_file=f"d:/Downloads/records_overview_{datetime.now().strftime('%Y%m%d')}_log.txt")
        # miner.compare_discharge_calculation_methods(output_dir=f"d:/Downloads/discharge_calculation_comparison",
        #                                             log_file=f"d:/Downloads/discharge_calculation_comparison/_log_{datetime.now().strftime('%Y%m%d')}.txt")

        # calculate_SLR(miner, output_dir=f"d:/Downloads/SLR_{datetime.now().strftime('%Y%m%d')}_step",
        #                     interpolate=False, lang="cz")

        # miner.find_fallow()

        # miner.show_methodics(lang=lang)
        # miner.export_methodics("d:/Downloads/methods.json")
        # miner.show_run_notes(lang=lang)
        # miner.generate_soilpulse_csv("d:/Downloads/runoffdb_excerpt.csv", date_from="2018-01-01", date_to="2022-12-31")

        # miner.generate_overview_html(f"d:/Downloads/runoffdb_overview_{datetime.now().strftime('%Y%m%d')}_{lang}.html",
        #                              date_from=datetime.fromisoformat("1990-01-01"),
        #                              date_to=datetime.fromisoformat("2100-01-01"),
        #                              lang=lang,
        #                              no_data_value="NA"
        #                              )



        # miner.generate_cumulative_values_csv("d:/Downloads/runoff_sediment_cumulative.csv",

        # generate_structured_dump(miner, f"D:/downloads/runoffdb_dump_{datetime.now().strftime('%Y%m%d')}_{lang}",
        #                                   lang=lang)

        # miner.generate_structured_dump(f"D:/downloads/runoffdb_dump_{datetime.now().strftime('%Y%m%d')}_{lang}",
        #                                   date_from="2019-06-03",
        #                                   date_to="2020-07-01",
        #                                   lang=lang)


        # miner.generate_euro_table("d:/Downloads/euro_export.csv")
        pass

    duration = datetime.now() - start_time
    print(f"{80*'_'}\nfinished at {datetime.now().strftime('%H:%M:%S')}")
    print(f"total processing time {duration}")

    print(f"\n\nProject structure\n--------------------")
    project_tree("D:/Dokumenty/RUNOFFDB/runoffdb_miner", files_only=True)

