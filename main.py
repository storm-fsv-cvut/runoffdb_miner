# -*- coding: utf-8 -*-
"""
@author: Jan Devátý
"""
import os
# from src.exports.testing import *
from datetime import datetime
from src.entities.runoffdb import RunoffDB

from src.processing.request import ProcessingRequest
from src.filters.run_filter import RunFilter
from src.processing .policy import DataPolicy

from src.exports.templates import *
from src.utilities.utilities import czech_date
from src.project_structure import project_tree




if __name__ == '__main__':
    start_time = datetime.now()
    # lang = "cz"
    lang = "en"

    filter_all = RunFilter(date_from=datetime.fromisoformat("1993-01-01"),
                           date_to=None,
                           simulators=None,
                           localities=None,
                           crops=None)

    filter_MDS = RunFilter(date_from=datetime.fromisoformat("2025-01-01"),
                           date_to=datetime.fromisoformat("2025-12-31"),
                           simulators=[7, 10],
                           localities=None,
                           crops=None)

    filter_Puclice = RunFilter(localities=[8])
    filter_2018 = RunFilter(date_from=datetime.fromisoformat("2018-01-01"),
                            date_to=datetime.fromisoformat("2018-12-31"))

    filter_2018_Risuty = RunFilter(date_from=datetime.fromisoformat("2018-05-10"),
                                   date_to=datetime.fromisoformat("2018-05-25"),
                                   localities=[1])

    policy = DataPolicy()

    request = ProcessingRequest(
        selection=filter_2018_Risuty,
        variables=[],
        policy=policy
    )

    with RunoffDB() as runoffdb:
        interval_export(runoffdb=runoffdb,
                        request=request,
                        output_path=f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}.csv",
                        lang=lang,
                        no_data_value="",
                        output_trace_path=f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}_report.json")

        # dir_name = f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}"
        # trace_path = os.path.join(dir_name, f"runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}_trace.json")
        # interval_export_by_simulator(runoffdb=runoffdb,
        #                              request=request,
        #                              output_dir=dir_name,
        #                              lang=lang,
        #                              no_data_value="",
        #                              output_trace_path=trace_path)
        pass

    # project_tree("D:/Dokumenty/RUNOFFDB/runoffdb_miner", files_only=False)
    #
    # with Miner(filter_2018) as miner:
    #     # test_this(miner)
    #     # miner.agrotechnologies_overview()
    #     test_interval_export_csv(miner,
    #                              f"d:/Downloads/test_export_{lang}.csv",
    #                              lang=lang,
    #                              no_data_value="",
    #                              output_trace_path=f"d:/Downloads/test_export_{lang}_trace.json")
    #     #
    #     # generate_interval_values_csv(miner,
    #     #                              f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}.csv",
    #     #                              lang=lang,
    #     #                              no_data_value="",
    #     #                              output_trace_path=f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}_trace.json")
    #
    #     # generate_intervals_csv_by_simulator(miner,
    #     #                                     f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}",
    #     #                                     lang=lang,
    #     #                                     no_data_value="",
    #     #                                     output_trace_path=f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}_trace.json")
    #
    #     # miner.simulators_overview(lang)
    #     # miner.plots_overview()
    #     # miner.methodics_overview(lang)
    #
    #     # miner.compare_discharge_calculation_methods(log_file=f"d:/Downloads/records_overview_{datetime.now().strftime('%Y%m%d')}_log.txt")
    #     # miner.compare_discharge_calculation_methods(output_dir=f"d:/Downloads/discharge_calculation_comparison",
    #     #                                             log_file=f"d:/Downloads/discharge_calculation_comparison/_log_{datetime.now().strftime('%Y%m%d')}.txt")
    #     #
    #     # output_dir_path = f"d:/Downloads/SLR_{datetime.now().strftime('%Y%m%d')}_step"
    #     # calculate_SLR(miner, output_dir=output_dir_path,
    #     #                     interpolate=False,
    #     #                     lang="cz",
    #     #                     output_trace_path=os.path.join(output_dir_path, "_data_trace.json")
    #     #               )
    #
    #     # output_dir_path = f"d:/Downloads/SLR_{datetime.now().strftime('%Y%m%d')}"
    #     # calculate_SLR(miner, output_dir=output_dir_path,
    #     #                     interpolate=True,
    #     #                     lang="cz",
    #     #                     output_trace_path=os.path.join(output_dir_path, "_data_trace.json")
    #     #               )
    #     # miner.find_fallow()
    #
    #     # miner.show_methodics(lang=lang)
    #     # miner.export_methodics("d:/Downloads/methods.json")
    #     # miner.show_run_notes(lang=lang)
    #     # miner.generate_soilpulse_csv("d:/Downloads/runoffdb_excerpt.csv", date_from="2018-01-01", date_to="2022-12-31")
    #
    #     # miner.generate_overview_html(f"d:/Downloads/runoffdb_overview_{datetime.now().strftime('%Y%m%d')}_{lang}.html",
    #     #                              date_from=datetime.fromisoformat("1990-01-01"),
    #     #                              date_to=datetime.fromisoformat("2100-01-01"),
    #     #                              lang=lang,
    #     #                              no_data_value="NA"
    #     #                              )
    #
    #
    #
    #     # miner.generate_cumulative_values_csv("d:/Downloads/runoff_sediment_cumulative.csv",
    #
    #     # generate_structured_dump(miner, f"D:/downloads/runoffdb_dump_{datetime.now().strftime('%Y%m%d')}_{lang}",
    #     #                                   lang=lang)
    #
    #     # miner.generate_structured_dump(f"D:/downloads/runoffdb_dump_{datetime.now().strftime('%Y%m%d')}_{lang}",
    #     #                                   date_from="2019-06-03",
    #     #                                   date_to="2020-07-01",
    #     #                                   lang=lang)
    #
    #
    #     # miner.generate_euro_table("d:/Downloads/euro_export.csv")
    #     pass

    duration = datetime.now() - start_time
    print(f"{80*'_'}\nfinished at {datetime.now().strftime('%H:%M:%S')}")
    print(f"total processing time {duration}")

