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
    start_time = datetime.datetime.now()
    miner = Miner()

    # set the date limits for all miner actions
    # miner.date_from = "2020-06-18 00:00:00"
    # miner.date_to = "2020-08-30 23:59:00"
    # miner.load_runs(limit=None)


    # runs_without_measurements = []
    #     for run in miner.runs:
    #         run.load_measurements()
    #         run.load_plot()
    #
    #         run.show_details()
    #         if not run.measurements:
    #             runs_without_measurements.append(run.id)
    #
    #     print("\nTotal number of runs: {}".format(len(miner.runs)))
    #     print("\nRuns with no measurements: {}".format(runs_without_measurements))

    miner.generate_euro_table("d:/Downloads/euro_export.csv")
    # miner.generate_interval_values_csv("d:/Downloads/runoff_sediment.csv", "d:/Downloads/log.txt")
    # miner.generate_cumulative_values_csv("d:/Downloads/runoff_sediment_cumulative.csv", plots_dir="d:/Downloads/cum_plots")
    # miner.generate_html_overview("d:/Downloads/overview.html")

    duration = datetime.datetime.now() - start_time
    print(f"Total processing time: {duration}")



