# -*- coding: utf-8 -*-
"""
@author: Jan Devátý
"""
import datetime

from src.db_access import DBconnector
from src.entities import *
from src.miner import Miner
import time
from sshtunnel import SSHTunnelForwarder


if __name__ == '__main__':
    start_time = datetime.now()
    lang = "cz"
    print()

    with Miner() as miner:
        # miner.generate_soilpulse_csv("d:/Downloads/runoffdb_excerpt.csv", date_from="2018-01-01", date_to="2022-12-31")

        # miner.generate_overview_html(f"d:/Downloads/runoffdb_overview_{datetime.now().strftime('%Y%m%d')}_{lang}.html",
        #                              date_from="1990-01-01",
        #                              lang=lang,
        #                              no_data_value="NA",
        #                              )

        #
        # kwargs = {"must_have_rainfall": False,
        #           "must_have_runoff": False,
        #           "must_have_sediment": False}
        #
        # miner.generate_interval_values_csv(f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}.csv",
        #                                    date_from="1990-01-01",
        #                                    lang=lang,
        #                                    **kwargs)

        miner.generate_interval_values_csv(f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_{lang}.csv",
                                           date_from="1990-01-01",
                                           lang=lang,
                                           log_file=f"d:/Downloads/runoff_sediment_intervals_{datetime.now().strftime('%Y%m%d')}_log.txt")

        # miner.generate_cumulative_values_csv("d:/Downloads/runoff_sediment_cumulative.csv",

        # miner.generate_structured_dump(f"D:/downloads/runoffdb_dump_{datetime.now().strftime('%Y%m%d')}_{lang}",
        #                                   date_from="1993-01-01",
        #                                   lang=lang)

        # miner.generate_structured_dump("D:\\downloads\\runoffdb_dump_{datetime.now().strftime('%Y%m%d')_{lang}",
        #                                   date_from="2019-06-03",
        #                                   date_to="2019-07-01",
        #                                   lang=lang)

        # set the date limits for all miner actions
        # miner.date_from = "2000-01-01"
        # miner.date_from = "2020-04-08"
        # miner.date_to = "2023-12-31"
        # miner.generate_euro_table("d:/Downloads/euro_export.csv")

    duration = datetime.now() - start_time
    print(f"\n{80*'_'}\ntotal processing time: {duration}")



