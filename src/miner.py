import time
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import math
import locale
import re
import pandas as pd
from pint import UnitRegistry

from src.db_access import DBconnector
from .entities import *


lang = "en"

# set locale for proper numbers formating
locale.setlocale(locale.LC_NUMERIC, lang)

# output files line delimiter
lined = "\n"
# output files cell delimiter
celld = ";"


class Miner:
    def __init__(self, date_from = None, date_to = None, simulators = None, localities = None, crops = None):
        # limits for data loads
        self.date_from = date_from
        self.date_to = date_to

        # record type priorities
        self.runoff_types_view_order = [2, 3, 4, 1, 6, 7, 5]
        self.ss_types_view_order = [2, 3, 4, 1, 6, 7, 5]
        # 1 - raw data
        # 2 - edited data
        # 3 - homogenized edited data
        # 4 - homogenized raw data
        # 5 - set value
        # 6 - derived data
        # 7 - estimated from similar conditions
        # 8 - rough estimate

    #
    # def load_runs(self, limit = None, dateFrom = None, dateTo = None):
    #     dbcon = self.dbc.connect()
    #     # use global date limits if not specified
    #     dateFrom = dateFrom if dateFrom is not None else self.date_from
    #     dateTo = dateTo if dateTo is not None else self.date_to
    #
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #
    #         # start of the query
    #         query = f"SELECT {runs_table}.`id` AS run_id, " \
    #                 f"{runs_table}.`runoff_start` AS ttr, " \
    #                 f"{runs_table}.`init_moisture_id` AS initmoist_recid, " \
    #                 f"{runs_table}.`surface_cover_id` AS surface_cover_recid, " \
    #                 f"{runs_table}.`rain_intensity_id` AS rainfall_recid, " \
    #                 f"{runs_table}.`soil_sample_bulk_id` AS bulkd_ss_id, " \
    #                 f"{runs_table}.`soil_sample_texture_id` AS texture_ss_id, "\
    #                 f"{runs_table}.`soil_sample_corg_id` AS corg_ss_id, "\
    #                 f"{run_groups_table}.`sequence_id` AS sequence_id, " \
    #                 f"{run_groups_table}.`datetime` AS datetime, " \
    #                 f"{sequences_table}.`simulator_id` AS simulator_id, " \
    #                 f"{runs_table}.`run_group_id` AS run_group_id, " \
    #                 f"{run_groups_table}.`run_type_id` AS run_type_id, " \
    #                 f"{plots_table}.`locality_id` AS locality_id, " \
    #                 f"{plots_table}.`id` AS plot_id, " \
    #                 f"{plots_table}.`crop_id` AS crop_id, " \
    #                 f"{crops_table}.`crop_type_id` AS crop_type_id "\
    #                 f"FROM {runs_table} " \
    #                 f"JOIN {run_groups_table} ON {runs_table}.`run_group_id` = {run_groups_table}.`id` " \
    #                 f"JOIN {sequences_table} ON {run_groups_table}.`sequence_id` = {sequences_table}.`id` " \
    #                 f"JOIN {plots_table} ON {runs_table}.`plot_id` = {plots_table}.`id` " \
    #                 f"JOIN {crops_table} ON {plots_table}.`crop_id` = {crops_table}.`id` "\
    #                 f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) "
    #         if dateFrom is not None:
    #             query += f" AND {run_groups_table}.`datetime` > '{dateFrom}'"
    #         if dateTo is not None:
    #             query += f" AND {run_groups_table}.`datetime` < '{dateTo}'"
    #
    #         # additional conditions
    #         # query += f"AND `` = "
    #
    #         # end of the query
    #         query += " ORDER BY `datetime` ASC"
    #
    #         if limit:
    #             query += f" LIMIT {limit}"
    #         # execute the query and fetch the results
    #         thecursor.execute(query)
    #
    #         results = thecursor.fetchall()
    #
    #         run_dict = {}
    #         if thecursor.rowcount > 0:
    #             for r in results:
    #                 new_run = Run(**r)
    #                 new_run.load_measurements()
    #                 new_run.plot = self.plots.get(new_run.plot_id)
    #                 # new_run.show_details()
    #                 run_dict.update({new_run.id: new_run})
    #             dbcon.close()
    #             self.runs = run_dict
    #             return run_dict
    #
    #         dbcon.close()
    #         return None
    #     return None
    #
    # def load_plots(self, dateFrom = None, dateTo = None):
    #     dbcon = self.dbc.connect()
    #
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #
    #         # start of the query
    #         query = f"SELECT * FROM {plots_table} "
    #         if dateFrom is not None:
    #             query += f" AND `established` > '{dateFrom}'"
    #         if dateTo is not None:
    #             query += f" AND `established` < '{dateTo}'"
    #
    #         # end of the query
    #         query += " ORDER BY `id` ASC"
    #
    #         # execute the query and fetch the results
    #         thecursor.execute(query)
    #
    #         results = thecursor.fetchall()
    #
    #         plot_dict = {}
    #         if thecursor.rowcount > 0:
    #             for r in results:
    #                 new_plot = Plot(r)
    #                 plot_dict.update({new_plot.id: new_plot})
    #             dbcon.close()
    #             return plot_dict
    #
    #         dbcon.close()
    #         return None
    #     return None
    #
    # def load_samples(self):
    #     dbcon = self.dbc.connect()
    #
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #
    #         query = f"SELECT * FROM {soil_samples_table} ORDER BY `id` ASC"
    #
    #         thecursor.execute(query)
    #
    #         results = thecursor.fetchall()
    #
    #         samples_dict = {}
    #         if thecursor.rowcount > 0:
    #             for r in results:
    #                 new_sample = SoilSample(r)
    #                 samples_dict.update({new_sample.id: new_sample})
    #             dbcon.close()
    #             return samples_dict
    #
    #         dbcon.close()
    #         return None
    #     return None
    #
    # def load_simulators(self, lang):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor()
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT `id`, `name_{lang}` "
    #                           "FROM `simulator`")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             simulators = {}
    #             for r in results:
    #                 simulators.update({r[0]: r[1]})
    #             self.simulators = simulators
    #         dbcon.close()
    #         return simulators
    #     return False
    #
    # def load_localities(self):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor()
    #         # execute the query and fetch the results
    #         thecursor.execute("SELECT `id`, `name`, `lat`, `lng` "
    #                           "FROM `locality`")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             localities = {}
    #             for r in results:
    #                 localities.update({r[0]: {"name": r[1], "lat": r[2], "long": r[3]}})
    #         dbcon.close()
    #         return localities
    #     return False
    #
    # def load_run_types(self, lang):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor()
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT `id`, `name_{lang}` FROM {run_types_table}")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             run_types_names = {}
    #             for r in results:
    #                 run_types_names.update({r[0]: r[1]})
    #         dbcon.close()
    #         return run_types_names
    #     return None
    #
    # def load_crop_types(self, lang):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor()
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT `id`, `name_{lang}` FROM {crop_types_table}")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             crop_types_names = {}
    #             for r in results:
    #                 crop_types_names.update({r[0]: r[1]})
    #         dbcon.close()
    #         return crop_types_names
    #     return None
    #
    # def load_protection_measures(self, lang):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor()
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT `id`, `name_{lang}` FROM {protection_measures_table}")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             protection_measures_names = {}
    #             for r in results:
    #                 protection_measures_names.update({r[0]: r[1]})
    #         dbcon.close()
    #         return protection_measures_names
    #     return None
    #
    # def load_units(self):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT * FROM {units_table}")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             units = {}
    #             for r in results:
    #                 new_unit = Unit(**r)
    #                 units.update({new_unit.id: new_unit})
    #         dbcon.close()
    #         return units
    #     return None
    #
    # def load_projects(self):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT * FROM {projects_table}")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             projects = {}
    #             for r in results:
    #                 new_project = Project(**r)
    #                 projects.update({new_project.id: new_project})
    #         dbcon.close()
    #         return projects
    #     return None
    #
    # def load_crops(self):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT * FROM {crops_table}")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             crops = {}
    #             for r in results:
    #                 new_crop = Crop(r)
    #                 crops.update({new_crop.id: new_crop})
    #         dbcon.close()
    #         return crops
    #     return None
    #
    # def load_agrotechnologies(self):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #         # execute the query and fetch the results
    #         thecursor.execute(f"SELECT * FROM {agrotechnologies_table}")
    #         results = thecursor.fetchall()
    #         if thecursor.rowcount > 0:
    #             agrotechnologies = {}
    #             for r in results:
    #                 new_agt = Agrotechnology(**r)
    #                 agrotechnologies.update({new_agt.id: new_agt})
    #         dbcon.close()
    #         return agrotechnologies
    #     return None
    #
    # def load_record(self, record_id):
    #     dbcon = self.dbc.connect()
    #
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #
    #         # start of the query
    #         query = f"SELECT * FROM {records_table} WHERE `id` = {record_id}"
    #         # execute the query and fetch the results
    #         thecursor.execute(query)
    #
    #         results = thecursor.fetchone()
    #         if thecursor.rowcount > 0:
    #             dbcon.close()
    #             return Record(**results)
    #         else:
    #             dbcon.close()
    #             return None
    #     return None
    #
    # def get_simulation_days(self, dateFrom = None, dateTo = None):
    #     dbcon = self.dbc.connect()
    #     # use instances date limits if not specified
    #     dateFrom = dateFrom if dateFrom is not None else self.date_from
    #     dateTo = dateTo if dateTo is not None else self.date_from
    #
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #
    #         # start of the query
    #         query = f"SELECT DISTINCT {run_groups_table}.`datetime` AS datetime, " \
    #                 f"{run_groups_table}.`sequence_id` AS sequence_id, " \
    #                 f"{runs_table}.`id` AS run_id " \
    #                 f"FROM {runs_table} " \
    #                 f"JOIN {run_groups_table} ON {runs_table}.`run_group_id` = {run_groups_table}.`id` " \
    #                 f"JOIN {sequences_table} ON {run_groups_table}.`sequence_id` = {sequences_table}.`id` " \
    #                 f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) "
    #         if dateFrom is not None:
    #             query += f" AND {run_groups_table}.`datetime` > '{dateFrom}'"
    #         if dateTo is not None:
    #             query += f" AND {run_groups_table}.`datetime` < '{dateTo}'"
    #
    #         # additional conditions
    #         # query += f"AND `` = "
    #
    #         # end of the query
    #         query += " ORDER BY `datetime` ASC"
    #
    #         # execute the query and fetch the results
    #         thecursor.execute(query)
    #
    #         results = thecursor.fetchall()
    #
    #         sim_days = []
    #         if thecursor.rowcount > 0:
    #             for r in results:
    #                 sim_days.append(r['datetime'])
    #
    #             dbcon.close()
    #             return sim_days
    #
    #         dbcon.close()
    #         return None
    #     return None
    #
    # def load_sequence_ids_by_date(self, datetime):
    #     dbcon = self.dbc.connect()
    #
    #     if dbcon:
    #         thecursor = dbcon.cursor(dictionary=True)
    #
    #         # start of the query
    #         query = f"SELECT {runs_table}.`id` AS run_id, " \
    #                 f"{run_groups_table}.`id` AS group_id, " \
    #                 f"{run_groups_table}.`sequence_id` AS sequence_id " \
    #                 f"FROM {runs_table} "\
    #                 f"JOIN {run_groups_table} ON {runs_table}.`run_group_id` = {run_groups_table}.`id` " \
    #                 f"JOIN {sequences_table} ON {run_groups_table}.`sequence_id` = {sequences_table}.`id` " \
    #                 f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) " \
    #                 f"AND {run_groups_table}.`datetime` = '{datetime}'"
    #
    #         # execute the query and fetch the results
    #         thecursor.execute(query)
    #         results = thecursor.fetchall()
    #
    #         if thecursor.rowcount > 0:
    #             dbcon.close()
    #             return results
    #
    #         dbcon.close()
    #         return None
    #     return None

    def repair_psd(self):
        """
        Repairs swapped values of particle size limit and particle mass content for particle size distribution records
        :return: None
        """
        dbcon = self.dbc.connect()

        if dbcon:
            thecursor = dbcon.cursor(dictionary=True)

            # start of the query
            query = f"SELECT `id` FROM {RunoffDB.records_table} WHERE `unit_id` = 19 and `related_value_xunit_id` = 20"
            print(query)

            thecursor.execute(query)
            results = thecursor.fetchall()
            for r in results:
                cursor2 = dbcon.cursor(dictionary=True)
                query = f"\nUPDATE {RunoffDB.records_table} SET `related_value_xunit_id` = 19, `unit_id` = 20 WHERE `id`= {r.get('id')};"
                print(query)
                cursor2.execute(query)
                # dbcon.commit()

                query = f"SELECT `id`, `value`, `related_value_x` FROM `data` WHERE `record_id`= {r.get('id')}"
                cursor2.execute(query)
                results2 = cursor2.fetchall()
                for res in results2:

                    query = f"UPDATE `data` SET `value` = {res.get('related_value_x')}, `related_value_x` = {res.get('value')}" \
                            f" WHERE `id` = {res.get('id')};"
                    print(query)
                    cursor3 = dbcon.cursor()
                    cursor3.execute(query)
                    # dbcon.commit()

            dbcon.close()
            return None
        return

    def generate_structured_dump(self, root_path, date_from = None, date_to = None, lang="en"):
        # create runoffDB connection instance
        rdb = RunoffDB()
        # get dates when some simulation occurred
        all_days = rdb.get_simulation_days(date_from, date_to)
        print("\n")

        for day_start in all_days:
            day_end = day_start.replace(hour=23, minute=59, second=59)
            # load runs of the day
            day_runs = rdb.load_runs(date_from=day_start, date_to=day_end)
            # no runs on day with simulations is a result of unfinished/messed-up record in DB (run group without any run)
            if day_runs is not None:
                day_dir = os.path.join(root_path, day_start.strftime('%Y-%m-%d'))
                try:
                    os.mkdir(day_dir)
                except OSError as error:
                   pass

                for rid, run in day_runs.items():
                    sim_dir_name = sanitize_path(f"{run.id}-{rdb.localities[run.locality_id].name}-{rdb.crops[run.crop_id].name[lang]}-{run.plot_id}-{rdb.run_types[run.run_type_id].name[lang]}")
                    sim_dir = os.path.join(day_dir, sim_dir_name)
                    print("\n"+80*"-")
                    print(f"#{run.id} - {day_start.strftime('%d. %m. %Y')} - {rdb.localities[run.locality_id].name} - {rdb.crops[run.crop_id].name[lang]} - {run.plot_id} - {rdb.run_types[run.run_type_id].name[lang]}")
                    print(80 * "-")
                    try:
                        os.mkdir(sim_dir)
                    except OSError as error:
                        pass
                    run.save_metadata(os.path.join(sim_dir, sim_dir_name+".json"))

                    # loop through all phenomena and if measurement exists go through it's records
                    for phid in rdb.get_all_phenomena_ids():
                        msrmnts = run.get_measurements(phid)
                        if msrmnts is not None:
                            for ms in msrmnts:
                                # loop through units and if record exists export it
                                for uid in rdb.get_all_units_ids():
                                    rcrds = ms.get_records(uid)
                                    if rcrds is not None:
                                        recids = []
                                        for rec in rcrds:
                                            recids.append(rec.id)
                                            if rec.record_type_id != 99:
                                                # the dataframe is TimeDelta indexed if is_timeline attribute is True
                                                index_column = "time" if rec.is_timeline else None
                                                index = True if rec.is_timeline else False
                                                # column_headers = ["time"] if rec.is_timeline else []
                                                column_headers = []
                                                data_df = rec.get_data("value", index_column=index_column)
                                                if data_df is not None:
                                                    rec_filename = sanitize_path(f"{rec.id}-{rec.unit.name[lang]}-[{rec.unit.unit}]")
                                                    column_headers.append(f"{rec.unit.name[lang]} [{rec.unit.unit}]")
                                                    column_headers.append(f"{rec.unit_rel_x.name[lang]} [{rec.unit_rel_x.unit}]") if rec.related_value_x_unit_id is not None else None
                                                    column_headers.append(f"{rec.unit_rel_y.name[lang]} [{rec.unit_rel_y.unit}]") if rec.related_value_y_unit_id is not None else None
                                                    column_headers.append(f"{rec.unit_rel_z.name[lang]} [{rec.unit_rel_z.unit}]") if rec.related_value_z_unit_id is not None else None

                                                    # format the TimeDelta index to desired format (get rid of the '0 days')
                                                    if pd.api.types.is_timedelta64_dtype(data_df.index):
                                                        data_df.index = data_df.index.map(lambda
                                                                                    x: f"{x.components.hours:02}:{x.components.minutes:02}:{x.components.seconds:02}")

                                                    print(f"#{rec.id}: {column_headers} ({rdb.record_types[rec.record_type_id].name[lang]})")

                                                    # if ms.phenomenon_id == 16:
                                                    #     print(data_df)

                                                    # try:
                                                    data_df.to_csv(os.path.join(sim_dir, rec_filename+".csv"),
                                                               index=index,
                                                               sep=";",
                                                               decimal=",",
                                                               header=column_headers)
                                                    # except ValueError:
                                                    #     print(data_df)
                                                else:
                                                    print(f"record {rec.id} ({rec.unit.name[lang]} [{rec.unit.unit}]) gains no data on load")

                                        # print(f"{phid} - {len(ms.records)} ({', '.join([str(rid) for rid in recids])})")



            else:
                print(f"\t{day_start.strftime('%Y-%m-%d')} skipped")
        # load runs matching input conditions
        # runs = rdb.load_runs(date_from=self.date_from, date_to=self.date_to)

    def generate_html_overview(self, output_path):
        cumulatives_headers1 = ["lokalita", "simID", "datum", "plot ID", "simulator ID", "plodina", "poc_stav",
                                      "poc_vlhkost", "canopy_cover", "BBCH", "intenzita", "TTR", "odtok_l",
                                      "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g"]
        cumulatives_headers2 = ["", "", "", "", "", "", "", "", "", "", "", "", "t1=10min", "", "t1=20min", "", "t1=30min"]

        output_html = open(output_path, "w")
        writeHTMLheader(output_html)

        for sim_datetime in self.get_simulation_days(dateFrom=None, dateTo=None):
            print(sim_datetime.strftime("%d.").lstrip("0") + " " + sim_datetime.strftime("%m.").lstrip(
                "0") + " " + sim_datetime.strftime("%Y"))
            print(f"\t{len(self.load_sequence_ids_by_date(sim_datetime))}")

        for run in self.runs:
            output_html.write(f"<h2>#{run.id} - {run.datetime.strftime('%d. %m. %Y')} - {run.crop_name}, {self.run_types[run.run_type_id]} </h2>\n")
            output_html.write("<table>\n")
            writeRowToHTML(output_html, cumulatives_headers2, True)
            writeRowToHTML(output_html, cumulatives_headers1, True)

            output_html.write("</table>")
        output_html.write("</body>\n</html>")
        output_html.close()
        return


    def generate_interval_values_csv(self, output_path, date_from=None, date_to=None, lang="en", no_data_value = "NA", log_file=None):
        rdb = RunoffDB()
        # the runs may not be loaded yet ...
        if rdb.runs is None:
            rdb.load_runs(date_from = date_from, date_to = date_to)

        if rdb.runs:
            flowrates_headers = {"cz": ["simID", "ID lokality", "lokalita", "datum", "plot ID", "ID simulatoru", "simulator", "ID plodiny", "plodina", "poc_stav",
                                 "poc_vlhkost", "canopy_cover", "BBCH", "intenzita", "TTR", "interval",
                                 "delka_intervalu", "t1", "t2", "prutok_l_min", "konc_sed_g_l", "ztrata_pudy_g_min"],
                                 "en": ["run ID", "locality ID", "locality", "date", "plot ID", "simulator ID", "simulator", "crop ID", "crop", "initial cond.",
                                    "init. moisture", "canopy cover", "BBCH", "rain intensity", "time to runoff", "interval #",
                                    "interval duration", "t1", "t2", "discharge [l.min-1]", "SS concentration [g.l-1]", "SS flux [g.min-1]"]}

            output_csv = open(output_path, "w")
            writeRowToCSV(output_csv, flowrates_headers[lang])

            for run in rdb.runs.values():
                print()
                # single row to be filled and written to the output files
                # one line represents one time interval of a measured time series within a run
                line = []

                print(
                    f"#{run.id} - {run.datetime.strftime('%d. %m. %Y')} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]}")

                # gather all the info and values common for the whole simulation run ===================================
                line.append(run.id)
                line.append(run.locality.id)
                line.append(run.locality.name)
                line.append(run.datetime.strftime('%d. %m. %Y'))
                line.append(run.plot_id)
                line.append(run.simulator.id)
                line.append(run.simulator.name[lang])
                line.append(run.crop_id if run.crop_id is not None else no_data_value)
                line.append(run.crop.name[lang] if run.crop.name[lang] is not None else no_data_value)
                line.append(run.run_type.name[lang])
                line.append(run.get_initial_moisture_value() if run.get_initial_moisture_value() is not None else no_data_value)
                line.append(run.get_surface_cover_value() if run.get_surface_cover_value() is not None else no_data_value)
                line.append(run.bbch if run.bbch is not None else no_data_value)
                line.append(run.get_rainfall_intensity_value() if run.get_rainfall_intensity_value() is not None else no_data_value)
                line.append(run.ttr)

                runoff_rec = run.get_best_runoff_record()
                sedconc_rec = run.get_best_sediment_concentration_record()

                if runoff_rec is not None and sedconc_rec is not None:
                    # gather or calculate values of the measure records
                    i = 1
                    prev_time = None
                    runoff_data = runoff_rec.get_data("runoff")
                    # print(runoff_data)
                    for time in runoff_data.index:
                        line_int = []
                        line_int.append(i)
                        line_int.append(time-prev_time) if prev_time is not None else line_int.append(no_data_value)
                        prev_time = time
                        line_int.append(time)
                        line_int.append(time-run.ttr)
                        i += 1
                        writeRowToCSV(output_csv, line+line_int)
            output_csv.close()
        else:
            print("No runs available within given limits.")

        return

    def generate_euro_table(self, output_path):
        # create runoffDB connection instance
        rdb = RunoffDB(output_na_value="NA")
        rdb.show_localities()
        # load runs matching input conditions
        runs = rdb.load_runs(date_from=self.date_from, date_to=self.date_to)

        if runs:
            lines = []
            catchThem = []
            for run in runs.values():
                # run.show_details()
                print(f"\n\nrun ID {run.id} ({czech_date(run.datetime)}, {rdb.localities.get(run.locality_id).name})")
                headers = ["ID"]
                notes = [""]
                poznamky = [""]
                line = [run.id]

                # headers.append("run title")
                # notes.append("")
                # poznamky.append("tohle je tu jenom teď pro nás, abysme se orientovali a snadno mohli odhality chyby")
                # line.append(f"{czech_date(run.datetime)} - {self.localities.get(run.locality_id)['name']}")

                headers.append("Contributor name")
                notes.append("")
                poznamky.append("Co myslíš, že by mělo bejt tady? Jména odpovědnejch lidí? institucí? nebo všude my jakožto contributor do týhle iniciativy?")
                if run.get_project_ids() is not None:
                    contributors = []
                    for prid in run.get_project_ids():
                        contributors.append(Project.project_leaders.get(prid))
                    if run.locality_id == 8:
                        contributors.append("Beitlerová H.")
                    line.append(f"CTU in Prague ({', '.join([name for name in contributors])})")

                elif run.locality_id == 2:
                    line.append(f"CTU in Prague (Kavka P.)")
                elif run.locality_id == 5:
                    line.append(f"CTU in Prague (Kavka P., Krása J.)")
                else:
                    line.append(f"CTU in Prague")


                headers.append("Published?")
                notes.append("")
                poznamky.append("Tady nevim, jestli jako byly publikovaný ty samotný data ... nebo jde i o publikace z těch dat vycházející?")

                headers.append("DOI")
                notes.append("")
                poznamky.append("")

                if run.locality_id == 10:
                    line.append("y")
                    line.append("https://doi.org/10.3390/app11104427")
                elif run.locality_id == 2:
                    line.append("y")
                    line.append("https://doi.org/10.3390/w14030327")
                else:
                    line.append("n")
                    line.append("NA")


                headers.append("Coordinates  lat (deg)")
                notes.append("")
                poznamky.append("")
                line.append(rdb.localities.get(run.plot.locality_id).lat)

                headers.append("Coordinates  long (deg)")
                notes.append("")
                poznamky.append("")
                line.append(rdb.localities.get(run.plot.locality_id).lng)

                headers.append("Soil Type (WRB)")
                notes.append("")
                poznamky.append("")
                line.append("")

                headers.append("Soil Texture clay (%)")
                headers.append("Soil Texture silt (%)")
                headers.append("Soil Texture sand (%)")
                notes.extend(["", "", ""])
                poznamky.extend(["", "", ""])

                if run.texture_ss_id is not None:
                    # print(f"run {run.id} has texture sample {run.texture_ss_id}")
                    # print(f"sample {run.texture_ss_id} has texture record set {self.samples.get(run.texture_ss_id).texture_record_id}")
                    # load the record from DB
                    if rdb.samples.get(run.texture_ss_id).texture_record_id is not None:
                        tex_rec = rdb.load_record(rdb.samples.get(run.texture_ss_id).texture_record_id)
                        # print(f"unit of the record is '{self.units.get(tex_rec.unit_id).name_en}' with dimension [{self.units.get(tex_rec.unit_id).unit}]")
                        # print(f"related X unit of the record is '{self.units.get(tex_rec.related_value_x_unit_id).name_en}' with dimension [{self.units.get(tex_rec.related_value_x_unit_id).unit}]")
                        tex_rec.load_data("cumulative_mass_content", "particle_size", index_column="particle_size", order_by="particle_size")
                        # print(tex_rec.data)
                        # upper size limits for clay/silt/sand
                        WRB_fraction_limits = [0.002, 0.063, 2]
                        interpolated = interpolate_texture(tex_rec.data, WRB_fraction_limits, "cumulative_mass_content", return_int=False, return_cumulative=False)
                        # print("\n"+interpolated.to_string())
                        line.append(interpolated.loc[WRB_fraction_limits[0], 'cumulative_mass_content'])
                        line.append(interpolated.loc[WRB_fraction_limits[1], 'cumulative_mass_content'])
                        line.append(interpolated.loc[WRB_fraction_limits[2], 'cumulative_mass_content'])
                    else:
                        print("Soil sample assigned as texture sample doesn't have texture record assigned!")
                        line.extend(["NA", "NA", "NA"])
                else:
                    line.extend(["NA", "NA", "NA"])

                headers.append("Soil Texture coarse fractions (%)")
                notes.append("")
                poznamky.append("Tohle se u nás nikdy nezaznamenávalo, nebo se pletu?")
                line.append("NA")

                headers.append("Soil texture system")
                notes.append("")
                poznamky.append("")
                line.append("WRB")

                headers.append("SOC (g/kg)")
                notes.append("")
                poznamky.append("")
                if run.corg_ss_id is not None:
                    if rdb.samples.get(run.corg_ss_id).corg_id is not None:
                        corg_rec = rdb.load_record(rdb.samples.get(run.corg_ss_id).corg_id)
                        corg_data = corg_rec.load_data("C_org")
                        # for a (undesired!) case when the assigned bulk density record consists of multiple values
                        line.append(corg_data["C_org"].mean())
                    else:
                        print("Soil sample assigned as organic carbon sample doesn't have organic carbon record assigned!")
                        line.append("NA")
                else:
                    line.append("NA")

                headers.append("SOM (g/kg)")
                notes.append("")
                poznamky.append("Tohle jsme nikdy neurčovali, nebo se pletu? Tušim, že je na to nějakej jednoduchej přepočet z Corg, ale to určitě neni, to co by chtěli")
                line.append("")

                # load the record from DB
                headers.append("BD (g/cm3)")
                poznamky.append("")
                notes.append("")
                if run.bulkd_ss_id is not None:
                    if rdb.samples.get(run.bulkd_ss_id).bulk_density_id is not None:
                        bd_rec = rdb.load_record(rdb.samples.get(run.bulkd_ss_id).bulk_density_id)
                        bd_rec.load_data("bulk_density")
                        bd_data = bd_rec.get_data_in_unit(27, "bulk_density")
                        # for a (undesired!) case when the assigned bulk density record consists of multiple values
                        line.append(bd_data["bulk_density"].mean())
                    else:
                        print("Soil sample assigned as bulk density sample doesn't have bulk density record assigned!")
                        line.append("NA")
                else:
                    line.append("NA")

                headers.append("Landuse")
                notes.append("")
                poznamky.append("")
                # categories defined by crop ID list from DB `crops` table
                crops_landuse = {"Bare": [1, 2, 16],
                                 "Grassland": [23, 32],
                                 "Crop_vineyard": [33]}
                if run.crop_id is None:
                    landuse = "NA"
                else:
                    landuse = None
                    for category, crop_list in crops_landuse.items():
                        if run.crop_id in crop_list:
                            landuse = category

                    # cropland crops are all remaining not listed here
                    if landuse is None:
                        landuse = "Cropland"

                line.append(landuse)


                headers.append("Land Cover")
                notes.append("")
                poznamky.append("")
                if run.crop_id is None:
                    line.append("NA")
                else:
                    line.append(rdb.crops.get(run.crop_id).name[lang])

                headers.append("Remarks (land cover)")
                notes.append("")
                poznamky.append("")
                if run.crop_id == 1:
                    line.append("'Reference cultivated fallow' - prepared with rotary tiller and compacted with roller")
                else:
                    line.append("")

                headers.append("Disturbance?")
                notes.append("How is this meant to be filled for a rainfall simulator data? The last disturbance before the experiment?")
                poznamky.append("")
                # categories defined by crop ID list from DB `crops` table
                if run.crop_id in [23, 32]:
                    line.append("")
                else:
                    line.append("Tillage")

                headers.append("Remarks (Disturbance)")
                notes.append("")
                poznamky.append("")
                if run.crop_id == 1:
                    line.append("Cultivated just before the simulation run")
                else:
                    line.append("")

                # two columns at once
                headers.append("Land Management?")
                notes.append("")
                poznamky.append("")
                headers.append("Remarks (Land Management)")
                notes.append("")
                poznamky.append("")

                if run.plot.agrotechnology_id is None:
                    print(f"Plot #{run.plot_id} has no agrotechnology assigned.")
                    line.append("")
                    line.append("")
                else:
                    agrt = rdb.agrotechnologies.get(run.plot.agrotechnology_id)
                    # grassland was cut for hay - other cases
                    if run.crop_id in [22, 23]:
                        if agrt.is_hay_cut():
                            line.append("Hay cut")
                            line.append("")
                        elif agrt.is_mulch_cut():
                            line.append("Mulch cut")
                            line.append("")
                        else:
                            line.append("")
                            line.append("")
                    else:
                        if agrt.get_maximum_disturbance_level() == 1:
                            line.append("No tillage")
                            line.append("Direct seeding without topsoil preparation")
                        elif agrt.get_maximum_disturbance_level() == 2:
                            line.append("Minimum tillage")
                            line.append("Direct seeding with limited topsoil disturbance")
                        elif agrt.get_maximum_disturbance_level() == 3:
                            line.append("Conservational tillage")
                            line.append("Topsoil disturbance without flipping")
                        elif agrt.get_maximum_disturbance_level() == 4:
                            line.append("Conventional tillage")
                            line.append("Topsoil disturbance including topsoil flipping")
                        else:
                            line.append("")
                            line.append("")


                headers.append("Practices targeting erosion")
                notes.append("")
                poznamky.append("")
                if run.plot.protection_measure_id is not None:
                    line.append(rdb.protection_measures.get(run.plot.protection_measure_id).name[lang])
                else:
                    line.append("")

                headers.append("Remarks (Practices)")
                notes.append("")
                poznamky.append("")
                line.append("")

                headers.append("total monitoring period (days/months/years)")
                notes.append("How is this meant for a particular bounded experimental plot? The plots are destroyed after each season and are not rebuilt in exactly same spots ...")
                poznamky.append("Zatim jsem sem dal rozdíl data poslední simulace na daný ploše a data založení plochy, záporný hodnoty nutno opravit v DB (zjevně špatně zadaný datum založení plochy)")
                plot_established = run.plot.established
                plot_last_used = run.plot.get_last_run_datetime()
                plot_lasted = (plot_last_used.date() - plot_established).days
                line.append(f"{1 if plot_lasted == 0 else plot_lasted} {'day' if plot_lasted == 0 else 'days'}")


                headers.append("time-step (days/months/years)")
                notes.append("How is this meant for rainfall simulator plots? The time-step of sampling within each simulation? Or a time-step between simulations?")
                poznamky.append("")
                line.append("")

                headers.append("beginning monitoring")
                notes.append("For a particular experimental plot? Or for the whole locality?")
                poznamky.append("teď je tady uvedenej den založení plochy")
                line.append(plot_established.strftime('%d.%m.%Y'))

                headers.append("end monitoring")
                notes.append("For a particular experimental plot? Or for the whole locality?")
                poznamky.append("a tady je datum posledního experimentu na daný ploše")
                line.append(plot_last_used.strftime('%d.%m.%Y'))

                headers.append("plot number/name")
                notes.append("")
                poznamky.append("")
                line.append(f"{run.plot.id}/{run.plot.name if run.plot.name not in ('', None) else '-'}")

                headers.append("Setup/Method")
                notes.append("How detailed should this description be?")
                poznamky.append("")
                line.append(f"artificial rainfall simulator experiment with '{rdb.simulators.get(run.simulator_id).name[lang]}' simulator setup.")

                headers.append("Bounded/Open")
                notes.append("")
                poznamky.append("")
                line.append("Bounded")

                headers.append("Slope (°, degrees)")
                notes.append("")
                poznamky.append("")
                line.append(math.atan(run.plot.plot_slope/100)/math.pi*180)

                headers.append("Scale (micro/slope/headwater catchment)")
                notes.append("")
                poznamky.append("")
                line.append("micro")

                headers.append("Plot Size (m2)")
                notes.append("")
                poznamky.append("")
                plot_area = run.plot.plot_length*run.plot.plot_width
                line.append(plot_area)

                headers.append("Plot Length (m)")
                notes.append("")
                poznamky.append("")
                line.append(run.plot.plot_length)

                headers.append("Plot Width (m)")
                notes.append("")
                poznamky.append("")
                line.append(run.plot.plot_width)

                headers.append("Bare Soil (%)")
                notes.append("")
                poznamky.append("")
                # for cultivated fallow presume 0 surface cover
                if rdb.crops.get(run.plot.crop_id).crop_type_id == 10:
                    line.append(100)
                else:
                    if run.surface_cover_recid is not None:
                        surcov_value = run.get_surface_cover_value()
                        line.append(100-surcov_value)
                    else:
                        line.append("NA")

                headers.append("Vegetation cover (%)")
                notes.append("")
                poznamky.append("")
                veg_cover = "NA"
                measurements = run.get_measurements()
                if measurements is not None:
                    for msrmnt in measurements:
                        for rcrd in msrmnt.records:
                            if rcrd.unit_id == 7:
                                veg_cover_rec = rdb.load_record(rcrd.id)
                                veg_cover_data = veg_cover_rec.load_data("vegetation_cover")
                                # for a (undesired!) case when the assigned bulk density record consists of multiple values
                                veg_cover = veg_cover_data["vegetation_cover"].mean()
                line.append(veg_cover)

                headers.append("Stone cover (%)")
                notes.append("")
                poznamky.append("")
                stone_cover = "NA"
                if measurements is not None:
                    for msrmnt in measurements:
                        for rcrd in msrmnt.records:
                            if rcrd.unit_id == 9:
                                stone_cover_rec = rdb.load_record(rcrd.id)
                                stone_cover_data = stone_cover_rec.load_data("stone_cover")
                                # for a (undesired!) case when the assigned bulk density record consists of multiple values
                                stone_cover = stone_cover_data["stone_cover"].mean()
                line.append(stone_cover)

                headers.append("Rainfall (mm/h)")
                notes.append("")
                poznamky.append("")
                if run.rain_intensity_recid is not None:
                    intensity_rec = rdb.load_record(run.rain_intensity_recid)
                    intensity_rec.load_data("rain_intensity")
                    intensity_data = intensity_rec.get_data("rain_intensity")
                    # regular intensity series has exactly 2 rows, any other number is some exception or non-standard rainfall
                    x = "*" if len(intensity_data.index) > 2 else ""
                    line.append(str(round(intensity_data["rain_intensity"].max(), 1))+x)

                else:
                    # ignore the whole simulation run if rainfall is not available
                    # continue
                    line.append("NA")

                headers.append("Rainfall (mm)")
                notes.append("")
                poznamky.append("")
                rainfall_rectype = ""
                rainfall_mm = None
                if run.rain_intensity_recid is not None:
                    intensity_rec = rdb.load_record(run.rain_intensity_recid)
                    intensity_data = intensity_rec.load_data("rain_intensity")
                    rainfall_mm = round(integrate_series(intensity_data, "rain_intensity", interpolate=False, time_unit='hours'), 0)

                    line.append(rainfall_mm)
                    if intensity_rec.record_type_id in [7, 8]:
                        rainfall_rectype = "Estimated"
                    elif intensity_rec.record_type_id == 5:
                        rainfall_rectype = "Set"
                    else:
                        rainfall_rectype = "Measured"

                else:
                    line.append("NA")

                headers.append("EI30 (mm/h)")
                notes.append("")
                poznamky.append("Jak se tohle počítá? Máme to někde?")
                line.append("")

                headers.append("Measured/Estimated Rainfall")
                notes.append("The value was set on the simulator and keeps within +-10% of the nominal value.")
                poznamky.append("")
                line.append(rainfall_rectype)

                headers.append("Runoff (mm)")
                notes.append("")
                poznamky.append("")

                # prepare column labels for dataframes
                runoff_label = "runoff_rate"
                sed_conc_label = f"sediment_concentration"
                sed_flux_label = f"sediment_flux"

                # initiate with NA values that will be used if no valid data is found
                runoff_mm = "NA"

                # search for surface runoff rate record
                runoff_data = None
                # go through the record type priority list and find the first matching Record
                for record_type in self.runoff_types_view_order:
                    # get the best surface runoff measurement Record
                    found_records = run.get_records(1, 1, record_type)
                    if found_records is not None:
                        if len(found_records) > 1:
                            print(f"\tMultiple runoff records of type {record_type} were found for run #{run.id}.\n"
                                  f"\tFirst of them will be used for processing (record id {found_records[0].id}).")
                        runoff_record = found_records[0]
                        # get runoff data in [l.min-1]
                        runoff_data = runoff_record.get_data_in_unit(1, runoff_label)
                        # if runoff data exist break the search cycle
                        if runoff_data is not None:
                            # print(f"runoff best record of run {run.id} is {runoff_record.id} (unit: {runoff_record.unit_id}, record type: {runoff_record.record_type_id})")
                            # if runoff dataframe has some data
                            if not runoff_data.empty:
                                try:
                                    runoff_l = integrate_series_minutes(runoff_data, runoff_label,
                                                                        zero_time=get_zero_timestamp(runoff_data, runoff_label))
                                    runoff_mm = runoff_l/plot_area
                                except ValueError as e:
                                    catchThem.append(runoff_record.id)
                                    print(f"Integration by time failed on total runoff calculation - data frame index is not TimeDelta")
                                    runoff_data = None
                            break

                line.append(runoff_mm)

                headers.append("Runoff coefficient")
                notes.append("")
                poznamky.append("")
                if rainfall_mm not in (None, "NA") and runoff_mm not in (None, "NA"):
                    line.append(runoff_mm/rainfall_mm)
                else:
                    line.append("NA ")
                #
                # headers.append("Soil Erosion (g)")
                # notes.append("")
                # poznamky.append("")

                headers.append("Soil Erosion (Mg/ha)")
                notes.append("")
                poznamky.append("")

                # search for sediment concentration records
                sediment_data = None
                # go through the record type priority list and find the first matching Record
                for record_type in self.ss_types_view_order:
                    # get the best sediment concentration measurement Record(s)
                    found_records = run.get_records(2, [2, 3], record_type_id = record_type)
                    if found_records is not None:
                        if len(found_records) > 1:
                            print(f"\tMultiple sediment concentration records of type {record_type} were found for run #{run.id}.\n"
                                  f"\tFirst of them will be used for processing (record id {found_records[0].id}).")
                        ss_record = found_records[0]
                        # get the sediment concentration data in [g.l-1]
                        sediment_data = ss_record.get_data_in_unit(3, sed_conc_label)
                        # if sediment data exist break the search cycle
                        if sediment_data is not None:
                            try:
                                get_zero_timestamp(sediment_data, sed_conc_label)
                            except ValueError as e:
                                catchThem.append(ss_record.id)
                                print(
                                    f"Integration by time failed on total sedtest calculation - data frame index is not TimeDelta")
                                sediment_data = None
                            # print(f"runoff best record of run {run.id} is {runoff_record.id} (unit: {runoff_record.unit_id}, record type: {runoff_record.record_type_id})")
                            break

                # initiate with NA values that will be used if no valid data is found
                soilloss_g = "NA"
                soilloss_Mg_ha = "NA"
                # if both runoff and sediment concentration data are found
                if runoff_data is not None and sediment_data is not None:
                    # if both dataframes have some data
                    if not runoff_data.empty and not sediment_data.empty:
                        # a common zero time is added (if possible) to force the integration from very start
                        # and to allow for cross-interpolation if the sediment series starts later than the runoff series
                        t0 = get_zero_timestamp(runoff_data, runoff_label)
                        if t0:
                            runoff_data.loc[pd.Timedelta(t0)] = 0
                            runoff_data = pd.concat([runoff_data.tail(1), runoff_data.head(len(runoff_data) - 1)])
                            runoff_data.sort_index()
                            # if the zero time from runoff series is before the first value of sediment series (should be)
                            if t0 < sediment_data.index[0]:
                                # New row to add
                                sediment_data.loc[pd.Timedelta(t0)] = 0
                                sediment_data = pd.concat([sediment_data.tail(1), sediment_data.head(len(sediment_data) - 1)])
                                sediment_data.sort_index()
                        else:  # assign the runoff start time as t0
                            t0 = run.ttr
                        print(f"runoff data (record #{runoff_record.id}):\n{runoff_data}\n")
                        print(f"sediment data (record #{ss_record.id}):\n{sediment_data}\n")

                        # merge the two dataframes into one with common 'time' index
                        merged_data = pd.concat([runoff_data, sediment_data], axis=1, join='outer')
                        # re-order the rows by time
                        try:
                            merged_data.sort_index(inplace=True)
                        except TypeError as e:
                            print(f"Incompatible indexes in input dataframes - runoff or sediment record is not a timeline")
                            catchThem.append(ss_record.id)

                        # cross-interpolate if the timepoints are not the same in the two series' and some values are missing
                        merged_data[runoff_label] = merged_data[runoff_label].interpolate(method='linear')
                        merged_data[sed_conc_label] = merged_data[sed_conc_label].interpolate(method='linear')
                        # replace possible NaN at the very beginning of time series with 0
                        # (situation when runoff has started but no sediment concentration data are available yet)
                        merged_data[sed_conc_label] = merged_data[sed_conc_label].fillna(0)
                        # calculate the sediment flux [g.min-1]
                        print(f"merged runoff and sediment concentration data:\n{merged_data}\n\n")
                        merged_data[sed_flux_label] = merged_data[runoff_label] * merged_data[sed_conc_label]
                        # write the cumulative values at the end of series
                        try:
                            soilloss_g = integrate_series_minutes(merged_data, sed_flux_label, zero_time=t0, extrapolate=1)
                        except ValueError as e:
                            print(f"Integration by time failed on soil loss calculation - data frame index is not TimeDelta")
                            soilloss_Mg_ha = "NA"
                        else:
                            soilloss_Mg_ha = soilloss_g/1000000/plot_area*10000
                else:
                    soilloss_g = "NA"
                    soilloss_Mg_ha = "NA"
                    # ignore the whole simulation run if runoff or sediment is not available
                    continue

                # line.append(soilloss_g)
                line.append(soilloss_Mg_ha)

                headers.append("Sediments (texture)")
                notes.append("")
                poznamky.append("")
                line.append("NA")

                headers.append("Sediments (%OM/%SOC)")
                notes.append("")
                poznamky.append("")
                line.append("NA")

                headers.append("Sediments (Nutrients g/kg)")
                notes.append("")
                poznamky.append("")
                line.append("NA")

                headers.append("Extra info")
                if run.locality_id == 10:
                    line.append("performed on disturbed soil sample container")
                else:
                    line.append("")

                lines.append(line)

            print(headers)
            print(lines)
            # write everything to output table
            output_csv = open(output_path, "w")
            # writeRowToCSV(output_csv, poznamky)
            writeRowToCSV(output_csv, notes)
            writeRowToCSV(output_csv, headers)

            for line in lines:
                writeRowToCSV(output_csv, line)

            output_csv.close()
            # print record IDs with
            if len(catchThem) > 0:
                print("following records don't have correct TimeDelta index:\n"+", ".join([str(c) for c in catchThem]))
        else:
            print("No runs available within given limits.")

        return


    def generate_cumulative_values_csv(self, output_path, logfile_path = None, plots_dir = None):
        """

        :param output_path: path of the output file
        :param logfile_path:
        :param plots_dir: directory path for plots
        :return:
        """
        # create runoffDB connection instance
        rdb = RunoffDB()
        # load runs matching input conditions
        runs = rdb.load_runs(date_from=self.date_from, date_to=self.date_to)

        if runs:
            velocities_filename = "velocities.csv"

            cumulatives_headers1 = {"cz": ["lokalita", "simID", "datum", "plot ID", "simulator", "plodina", "typ_plodiny", "poc_stav",
                                          "poc_vlhkost", "canopy_cover", "BBCH", "intenzita", "TTR",
                                           "odtok_l", "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g",
                                           "povrchova rychlost [m.s-1]"],
                                    "en": ["locality", "run ID", "date", "plot ID", "simulator", "crop", "crop type", "initial cond.",
                                           "init. moisture", "canopy cover", "BBCH", "rain intensity", "time to runoff",
                                           "cum. discharge [l]", "cum. soil loss [g]", "cum. discharge [l]", "cum. soil loss [g]", "cum. discharge [l]", "cum. soil loss [g]",
                                           "povrchova rychlost [m.s-1]"]}

            cumulatives_headers2 = ["", "", "", "", "", "", "", "", "", "", "", "", "", "t1=10min", "", "t1=20min", "", "t1=30min"]

            # open the file for writing, overwrite if exists, write file headers
            output_csv = open(output_path, "w")
            writeRowToCSV(output_csv, cumulatives_headers2)
            writeRowToCSV(output_csv, cumulatives_headers1[lang])

            # just for the counter
            i = 1
            num_runs = len(runs)
            for run in runs.values():
                # show the counter
                print(f"{i}/{num_runs}")
                i += 1

                run_title = f"#{run.id} - {czech_date(run.datetime)} - {rdb.localities[run.locality_id]['name']} - {run.get_crop_name(lang)} [{run.plot_id}], {rdb.run_types[run.run_type_id]} {{{run.ttr}}}"
                print(run_title)
                # run.show_details()

                # one row to be filled and written to the output files
                line = []
                # search for surface runoff rate record
                runoff_record = None
                runoff_data = None
                # go through the record type priority list and find the first matching Record
                for record_type in self.runoff_types_view_order:
                    # get the best surface runoff measurement Record
                    found_records = run.get_records(1, 1, record_type)
                    if found_records:
                        if len(found_records) > 1:
                            print(f"\tMultiple runoff records of type {record_type} were found for run #{run.id}.\n"
                                  f"\tFirst of them will be used for processing (record id {found_records[0].id}).")
                        runoff_record = found_records[0]
                        # get runoff data in [l.min-1]
                        runoff_data = runoff_record.get_data()
                        # if runoff data exist break the search cycle
                        if runoff_data is not None:
                            # print(f"runoff best record of run {run.id} is {runoff_record.id} (unit: {runoff_record.unit_id}, record type: {runoff_record.record_type_id})")
                            break

                # search for sediment concentration records
                ss_record = None
                sediment_data = None
                # go through the record type priority list and find the first matching Record
                for record_type in self.ss_types_view_order:
                    # get the best sediment concentration measurement Record(s)
                    found_records = run.get_records(2, [2, 3], record_type_id = record_type)
                    # print(found_records)
                    if found_records:
                        if len(found_records) > 1:
                            print(f"\tMultiple sediment concentration records of type {record_type} were found for run #{run.id}.\n"
                                  f"\tFirst of them will be used for processing (record id {found_records[0].id}).")
                        ss_record = found_records[0]
                        # print(f"sediment load best record of run {run.id} is {ss_record.id} (unit: {ss_record.unit_id}, record type: {ss_record.record_type_id})")
                        # get the sediment concentration data in [g.l-1]
                        sediment_data = ss_record.get_data_in_unit(3)
                        # if sediment data exist break the search cycle
                        if sediment_data is not None:
                            # print(f"runoff best record of run {run.id} is {runoff_record.id} (unit: {runoff_record.unit_id}, record type: {runoff_record.record_type_id})")
                            break

                # search for the surface flow velocity records
                run.get_terminal_velocity_value()

                # if both data are found
                if runoff_data is not None and sediment_data is not None:
                    # if both dataframes have some data
                    if not runoff_data.empty and not sediment_data.empty:
                        # check if the directory exists and if not create it
                        if plots_dir:
                            # check if the directory exists and if not create it
                            if not os.path.isdir(plots_dir):
                                os.mkdir(plots_dir)
                        # gather all the info and values ========================================
                        line.append(rdb.localities[run.locality_id]['name'])
                        line.append(run.id)
                        line.append(czech_date(run.datetime))
                        line.append(run.plot_id)
                        line.append(rdb.simulators[run.simulator_id])
                        crop_name = run.get_crop_name(lang)
                        if crop_name:
                            line.append(crop_name)
                        else:
                            line.append("NA")
                        if run.crop_type_id:
                            line.append(rdb.crop_types[run.crop_type_id])
                        else:
                            line.append("NA")
                        line.append(rdb.run_types[run.run_type_id])
                        if run.initmoist_recid:
                            line.append(run.get_initial_moisture_value())
                        else:
                            line.append("NA")
                        if run.surface_cover_recid:
                            line.append(run.get_surface_cover_value())
                        else:
                            line.append("NA")
                        if run.bbch:
                            line.append(run.bbch)
                        else:
                            line.append("NA")
                        if run.rain_intensity_recid:
                            line.append(run.get_rainfall_intensity())
                        else:
                            line.append("NA")
                        line.append(run.ttr)


                        runoff_label = f"runoff [{rdb.units.get(runoff_record.unit_id).unit}]"
                        sed_conc_label = f"sed. conc. [{rdb.units.get(3).unit}]"
                        sed_flux_label = f"sed. flux [{rdb.units.get(25).unit}]"
                        tot_runoff_label = "total runoff"
                        sed_mass_label = "sediment mass"

                        # a common zero time is added (if possible) to force the integration from very start
                        # and to allow for cross-interpolation if the sediment series starts later than the runoff series
                        t0 = get_zero_timestamp(runoff_data)
                        if t0:
                            runoff_data.loc[pd.Timedelta(t0)] = 0
                            runoff_data = pd.concat([runoff_data.tail(1), runoff_data.head(len(runoff_data) - 1)])
                            runoff_data.sort_index()
                            # if the zero time from runoff series is before the first value of sediment series (should be)
                            if t0 < sediment_data.index[0]:
                                # New row to add
                                sediment_data.loc[pd.Timedelta(t0)] = 0
                                sediment_data = pd.concat([sediment_data.tail(1), sediment_data.head(len(sediment_data) - 1)])
                                sediment_data.sort_index()
                        else: # assign the runoff start time as t0
                            t0 = run.ttr
                        # print(f"runoff data:\n{runoff_data}\n")
                        # print(f"sediment data:\n{sediment_data}\n\n")

                        # merge the two dataframes into one with common 'time' index
                        merged_data = pd.concat([runoff_data, sediment_data], axis=1, join='outer', keys=[runoff_label, sed_conc_label])
                        # re-order the rows by time
                        merged_data.reset_index(inplace=True)
                        merged_data['time'] = pd.to_timedelta(merged_data['time'])
                        merged_data.sort_values(by='time', inplace=True)
                        # set the time index back
                        merged_data.set_index('time', inplace=True)

                        # cross-interpolate if the timepoints are not the same in the two series' and some values are missing
                        merged_data[runoff_label] = merged_data[runoff_label].interpolate(method='linear')
                        merged_data[sed_conc_label] = merged_data[sed_conc_label].interpolate(method='linear')
                        # replace possible NaN at the very beginning of time series with 0
                        # (situation when runoff has started but no sediment concentration data are available yet)
                        merged_data[sed_conc_label] = merged_data[sed_conc_label].fillna(0)

                        # calculate the sediment flux [g.min-1]
                        merged_data[(sed_flux_label, '[g.min-1]')] = merged_data[runoff_label] * merged_data[sed_conc_label]

                        # calculate cumulative runoff series
                        integrate_data_series(merged_data, runoff_label, (tot_runoff_label, '[l]'))
                        # calculate cumulative sediment flux series
                        integrate_data_series(merged_data, sed_flux_label, (sed_mass_label, '[g]'))
                        print(f"merged data with total runoff and sediment:\n{merged_data}\n")

                        # write the cumulative values in desired times
                        line.append(integrate_series_minutes(merged_data, t0, pd.Timedelta(minutes=10), runoff_label, zero_time=t0, extrapolate=2))
                        line.append(integrate_series_minutes(merged_data, t0, pd.Timedelta(minutes=10), sed_flux_label, zero_time=t0, extrapolate=2))

                        line.append(integrate_series_minutes(merged_data, t0, pd.Timedelta(minutes=20), runoff_label, zero_time=t0, extrapolate=2))
                        line.append(integrate_series_minutes(merged_data, t0, pd.Timedelta(minutes=20), sed_flux_label, zero_time=t0, extrapolate=2))

                        line.append(integrate_series_minutes(merged_data, t0, pd.Timedelta(minutes=30), runoff_label, zero_time=t0, extrapolate=2))
                        line.append(integrate_series_minutes(merged_data, t0, pd.Timedelta(minutes=30), sed_flux_label, zero_time=t0, extrapolate=2))

                        # write the row to output
                        writeRowToCSV(output_csv, line)

                        if plots_dir:
                            plot_series_to_file(merged_data, [runoff_label, tot_runoff_label], os.path.join(plots_dir, f"{run.id}_runoff"), run_title, "time [min]", ["runoff rate [l.min-1]", "total runoff [l]"], True)
                            merged_data.index = merged_data.index.map(lambda x: format_timedelta_index(x))
                            merged_data.to_csv(os.path.join(plots_dir, f"{run.id}_runoff_sediment.csv"), sep=celld, decimal= ",")

                    else:
                        print(f"One or both data series of run {run.id} is empty!\n"
                              f"... which really shouldn't happen as the Record.get_data() returns None when the Record.data is empty dataframe ...")

            # close the files if were opened
            output_csv.close()
        else:
            print("No runs available within given limits.")

        return

def sanitize_path(path_str):
    # Replace invalid characters for both Windows and Unix-like systems
    sanitized = re.sub(r'[<>:"/\\|?* ]', '_', path_str)
    return sanitized
def interpolate_texture(original_texture, new_limits, cum_mass_col_name, return_cumulative = True, return_int = True, smallest_content = 1):
    # ensure original_texture is a pandas DataFrame
    if not isinstance(original_texture, pd.DataFrame):
        raise TypeError("original_texture parameter value must be a pandas DataFrame")

    # get column names from the input DataFrame
    particle_size_col = original_texture.index.name

    # extract original limits and cumulative contents from the DataFrame
    original_limits = original_texture.index.to_list()
    original_contents = original_texture[cum_mass_col_name].to_list()

    # insert artificial first datapoint with the smallest content to allow for interpolation of smaller particles content
    original_limits.insert(0, 0)
    original_contents.insert(0, smallest_content)

    # sort the new limits
    new_limits = sorted(new_limits)

    cumul_contents = []


    for nl in new_limits:
        i = 0
        for ol, content in zip(original_limits, original_contents):
            if i == 0:
                prev_ol = ol
                prev_content = content
            else:
                if nl > prev_ol and nl <= ol:
                    new_value = prev_content + ((content - prev_content) / (ol - prev_ol)) * (nl - prev_ol)
                    cumul_contents.append(new_value)
                prev_ol = ol
                prev_content = content
            i += 1

    # round the content values to integers if requested
    if return_int:
        cumul_contents = [round(val) for val in cumul_contents]

    # recalculate cumulative values to net values if requested
    if not return_cumulative:
        net_contents = [cumul_contents[0]]
        for j in range(1, len(cumul_contents)):
            net_contents.append(cumul_contents[j] - cumul_contents[j - 1])
        output_contents = net_contents
    else:
        output_contents = cumul_contents

    # create the output DataFrame with the same column names as the input DataFrame

    output_df = pd.DataFrame({
        particle_size_col: new_limits,
        cum_mass_col_name: output_contents
    })

    # set particle_size as the index
    output_df.set_index(particle_size_col, inplace=True)

    return output_df

def get_zero_timestamp(dataframe, series_name):
    """
    Finds the point in time when the dataframe intersects the x-axis (searches for timestamp where value == 0)

    :param dataframe: time indexed dataframe
    :return: the timestamp of zero value or None if impossible to be interpolated
    """

    # Ensure dataframe is time-indexed
    if not isinstance(dataframe.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")


    if dataframe.index.size > 1:
        # check the direction of the first interval and break if extrapolation is not possible
        if (dataframe[series_name].iloc[1] - dataframe[series_name].iloc[0]) == 0:
            print("Zero time value couldn't be extrapolated because the value in first interval is constant.\n");
            return None
        elif (dataframe[series_name].iloc[1] - dataframe[series_name].iloc[0]) < 0:
            print("Zero time value couldn't be extrapolated because the value in first interval is decreasing.\n")
            return None
        else:
            # extrapolate the zero value time from the first two points in dataseries
            t0 = dataframe[series_name].index[0]-((dataframe[series_name].iloc[0]*(dataframe[series_name].index[1]-dataframe[series_name].index[0]))/(dataframe[series_name].iloc[1]-dataframe[series_name].iloc[0]))
            # if the zero time should negative (meaning that there was a value before the experiment start) it is set to 0
            if t0 < pd.Timedelta(seconds=0):
                t0 = pd.Timedelta(seconds=0)
            return t0
    else:
        print ("Zero time value couldn't be extrapolated because the timeline doesn't have enough datapoints.\n")
        return None

def integrate_series_minutes(df, series_name, start_time = None, end_time = None, zero_time = None, extrapolate = None, interpolate=True):
    return integrate_series(df, series_name, start_time, end_time, zero_time, extrapolate, interpolate, 'minutes')

def integrate_series(df, series_name, start_time = None, end_time = None, zero_time = None, extrapolate = None, interpolate=True, time_unit='minutes'):
    """
    Calculate discrete time integral of selected 'series_name' from dataframe 'df' between 'start_time' and 'end_time'
    'zero_time' (if set) or first datapoint is used if start_time is None
    Last datapoint is used if end_time is None
    Values between datapoints in source dataframe are linear interpolated if interpolate is True
    Constant value between datapoints is assumed if interpolate is False
    When end_time is after last datapoint in series and extrapolate is True then the en value is linear extrapolated from last interval's times and values
    Assumes variable time steps in the index.

    :param df: Input pandas DataFrame with timedelta index.
    :param series_name: Series name to integrate.
    :param start_time: Start timedelta for integration.
    :param end_time: End timedelta for integration.
    :param zero_time: optional starting time for integration
    :param extrapolate: whether extrapolate after last point of time series
    :param interpolate: whether to interpolate between points in time series, if False stepwise integration is performed (value considered constant in each time interval)
    :param time_unit: unit of time to use for integration ('minutes', 'hours', 'seconds')

    :returns: discrete time integral values.
    """
    if series_name == "rain_intensiity":
        print("integrating rain_intensiity")
    # ensure dataframe is time-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    time_conversion_factor = {
        'seconds': 1,
        'minutes': 60,
        'hours': 3600
    }

    if time_unit not in time_conversion_factor:
        raise ValueError("Invalid time unit. Allowed values are 'seconds', 'minutes', 'hours'.")

    conversion_factor = time_conversion_factor[time_unit]

    # set default start_time and end_time if None
    if start_time is None:
        start_time = zero_time if zero_time is not None else df.index[0]
    if end_time is None:
        end_time = df.index[-1]

    output_value = 0
    prev_time = None  # initialize prev_time with the first time index in the DataFrame

    for time in df.index:
        # value = df.loc[time, series_name]
        value = get_value_in_time(df, time, series_name, zero_time, extrapolate)
        if prev_time is not None:
            if prev_time >= start_time and time <= end_time:
                # for the case when two consequent times are equal = error in time data series
                if time == prev_time:
                    print(f"\nThere seems to be an error in your data - two consequent times are equal")
                    continue
                time_diff = time - prev_time
                if interpolate:
                    output_value += (value + prev_value) / 2 * time_diff.total_seconds() / conversion_factor
                else:
                    output_value += prev_value * time_diff.total_seconds() / conversion_factor
            elif prev_time < start_time and time > start_time:
                time_diff = time - start_time
                if interpolate:
                    output_value += (value + prev_value) / 2 * time_diff.total_seconds() / conversion_factor
                else:
                    output_value += prev_value * time_diff.total_seconds() / conversion_factor
            elif prev_time < end_time and time > end_time:
                time_diff = end_time - prev_time
                if interpolate:
                    output_value += (value + prev_value) / 2 * time_diff.total_seconds() / conversion_factor
                else:
                    output_value += prev_value * time_diff.total_seconds() / conversion_factor
        prev_time = time
        prev_value = value

    return output_value

def integrate_data_series(df, series_name_in, series_name_out):
    """
    Calculates discreet integral for all points of given 'series_name_in' from dataframe 'df' and stores the values in new series 'series_name_out'

    :param df:
    :param series_name_in:
    :param series_name_out:
    :return:
    """
    # Ensure dataframe is time-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    output_values = []

    for time in df.index:
        integral_value = integrate_series_minutes(df, pd.Timedelta(seconds=0), time, series_name_in)

        # Store the integrated value
        output_values.append(integral_value)
    # Add the integrated values as a new column to the DataFrame
    df[series_name_out] = output_values

    return df

def get_value_in_time(df, timedelta, series_name, zero_time=None, extrapolate=None):
    """
    Returns interpolated value of dataseries in time specified as timedelta

    :param timedelta:
    :param series_name: column index name of the series to be interpolated
    :param zero_time: presumed time of start of the series (value = 0)
    :param extrapolate: range of extrapolation specified as multiplication of last complete interval length
    :returns: interpolated/extrapolated value if available based on specified inputs otherwise False
    """

    # Ensure dataframe is time-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    first_time = df.index[0]
    last_time = df.index[-1]

    # if timedelta is before the first value
    if timedelta < first_time:
        # if the zero time was specified
        if zero_time:
            print(" - extrapolating to zero\n\n")
            return (df.loc[first_time, series_name] / (first_time - zero_time).total_seconds()) * (timedelta - zero_time).total_seconds()
        else:
            print(f"Requested time is before the first record in '{series_name}' data series and extrapolation to zero was not requested.\n")
            return None

    # if time is after the last value
    elif timedelta > last_time:
        if extrapolate:
            if df[series_name].size > 1:
                # duration of last step in series
                last_step_duration = df.index[-1] - df.index[-2]

                # if the desired timedelta is within 'extrapolate' times last interval duration from series end
                if (timedelta - df.index[-1]) < extrapolate * last_step_duration:
                    print(" - extrapolating after series end\n")

                    v1 = df.loc[df.index[-2], series_name]
                    v2 = df.loc[df.index[-1], series_name]
                    t1 = df.index[-2]
                    t2 = df.index[-1]
                    t3 = timedelta

                    if (t3 - t2) < extrapolate * (t2 - t1):
                        return v2 + (t3 - t2).total_seconds() * ((v2 - v1) / (t2 - t1).total_seconds())

            else:
                print(
                    f"Data series '{series_name}' doesn't have enough values for extrapolation.\n")
                return None
        else:  # or return False if extrapolation not intended
            print(f"Requested timedelta is after last record of '{series_name}' data series and extrapolation was not requested.\n")
            return None

    else:
        # print(f"this is the dataframe inside the 'get_value_in_time():\n{df}")
        return df[series_name].loc[timedelta]


# def plot_series_to_file(df, series_names, file_path, title=None, xlabel=None, ylabel=None, legend=True):
#     """
#     Plot multiple series from a DataFrame and save the plot to a PNG image file.
#
#     :param df (DataFrame): Input DataFrame containing the series to be plotted.
#     :param series_names (list of str): List of series names to be plotted.
#     :param file_path (str): File path to save the plot as a PNG image.
#     :param title (str, optional): Title of the plot.
#     :param xlabel (str, optional): Label for the x-axis.
#     :param ylabel (str, optional): Label for the y-axis.
#     :param legend (bool, optional): Whether to display the legend. Default is True.
#     """
#
#     # clear existing plot
#     plt.clf()
#
#     # plot each series
#     for series_name in series_names:
#         plt.plot(df.index, df[series_name], label=series_name)
#
#     # set title and labels
#     if title:
#         plt.title(title)
#     if xlabel:
#         plt.xlabel(xlabel)
#     if ylabel:
#         plt.ylabel(ylabel)
#
#     # add legend if specified
#     if legend:
#         plt.legend()
#
#     # save the plot to a PNG file
#     plt.savefig(file_path)

def plot_series_to_file(df, series_names, file_path, title=None, xlabel = None, ylabels=None, legend=True):
    """
    Plot multiple series from a DataFrame and save the plot to a PNG image file.

    :param df: DataFrame, Input DataFrame containing the series to be plotted.
    :param series_names: list of str, List of series names to be plotted.
    :param file_path: str, File path to save the plot as a PNG image.
    :param title: str, optional, Title of the plot.
    :param xlabel: str, optional, Label for the x-axis.
    :param ylabels: list of str, optional, Labels for the y-axes
    :param legend: bool, optional, Whether to display the legend. Default is True.
    """

    # Clear existing plot
    plt.clf()

    # Plot each series with separate y-axis
    fig, ax1 = plt.subplots()
    if xlabel:
        ax1.set_xlabel(xlabel)
    ax1.set_ylabel(ylabels[0] or series_names[0], color='tab:blue')
    ax1.plot(df.index, df[series_names[0]], color='tab:blue', label=ylabels[0] or series_names[0])

    i = 1
    for series_name in series_names[1:]:
        ax2 = ax1.twinx()
        ax2.set_ylabel(series_name if ylabels[i] is None else ylabels[i], color='tab:red')
        ax2.plot(df.index, df[series_name], color='tab:red', label=series_name)

    # Set title
    if title:
        plt.title(title)

    # Remove border around the plot
    # ax1.spines['top'].set_visible(False)
    # ax1.spines['right'].set_visible(False)
    # ax1.spines['bottom'].set_visible(False)
    # ax1.spines['left'].set_visible(False)
    # ax1.tick_params(axis='both', which='both', length=0)  # Remove tick marks

    # Set axes properties
    plt.axhline(0, color='black', linewidth=1)
    plt.axvline(0, color='black', linewidth=1)

    # Create a custom timedelta formatter
    timedelta_formatter = ticker.FuncFormatter(lambda x, pos: format_timedelta_min(pd.Timedelta(x)))

    # Set the formatter for the x-axis
    ax1.xaxis.set_major_formatter(timedelta_formatter)
    ax1.tick_params(axis='x', rotation=90)  # Rotate x-axis labels

    # Set labels
    if xlabel:
        plt.xlabel(xlabel)

    # Add legend if specified
    if legend:
        plt.legend()

    # Save the plot to a PNG file
    plt.savefig(file_path)

def format_timedelta_index(td, **kwargs):
    return str(td).split(' ')[2]

def format_timedelta_hms(timedelta):
    total_seconds = timedelta.total_seconds()
    hours = int(total_seconds / 3600)
    minutes = int((total_seconds % 3600) / 60)
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def format_timedelta_min(timedelta):
    total_seconds = timedelta.total_seconds()
    minutes = int((total_seconds % 3600) / 60)

    return f"{minutes:.1f}"

def writeRowToCSV(fileref, towrite):
    linestring = ""
    i = 0
    for item in towrite:
        if isinstance(item, float):
            linestring += locale.format_string('%.3f', item)
        else:
            linestring += f"{item}"

        if i < len(towrite)-1:
            linestring += celld
        else:
            linestring += lined
        i += 1
    fileref.write(linestring)
    return

def writeRowToHTML(fileref, towrite, is_header = False):
    linestring = "<tr>\n"
    for item in towrite:
        if is_header:
            linestring += f"<th>{item}</th>\n"

        else:
            if isinstance(item, float):
                linestring += f"<td>{item:.3f}</td>\n"
            else:
                linestring += f"<td>{item}</td>\n"
    linestring += "</tr>\n"
    fileref.write(linestring)
    return

def writeHTMLheader(fileref):
    towrite = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\" />\n"\
    "<html xmlns = \"http://www.w3.org/1999/xhtml\" lang = \"cs\" xml: lang = \"cs\" />\n"\
    "<head> <meta http - equiv = \"Content-Type\" content = \"text/html; charset=utf8\" />\n"\
    "<title> Runoff + sediment integration </title>\n"\
    "<style>\n"\
    "html {\n\tbackground: # ddd;\n}\n\t" \
    "body {\n\tbackground: white;\n\twidth: 1200px;\n\tmargin: 0 auto;\n\tpadding: 5px 20px;\n\tfont: 'Arial';\n}\n\t" \
    "p {\n\tfont-size: 14px;\n\tmargin: 0.2 em 0 0 0;\n}\n\t" \
    "p.conditions {\n\tmargin: 0 0 0.2em 0;\n\tcolor: #a71;\n}\n\t" \
    "p.locality {\n\tfont-size: 1.1em;\n\tfont-weight: bold;\n\tcolor: #555;\n}\n\t" \
    "p.note {\n\tcolor: #ff2222;\n\tfont-weight: bold;\n\tmargin: 0 0 0.5em 0;\n}\n\t" \
    "span.ids {\n\tfont-size: 0.8em;\n\tmargin: 2px;\n\tcolor: #555;\n\tmargin: 0 20px 0 0;\n}\n\t" \
    "span.mainid {\n\tcolor: black;\n\tfont-weight: bold;\n}\n\t" \
    "h1 {\n\tfont-size: 1.5em;\n\tmargin: 1em 0 0.2em 0;\n}\n\t" \
    "h2 {\n\tfont-size: 1em;\n\tmargin: 1em 0 0.2em 0;\n}\n\t" \
    "table {\n\tborder-collapse: collapse;\n}\n\t" \
    "td, th {\n\ttext-align: center;\n\tborder: 1px solid #ddd;\n\tpadding: 2px 4px;\n\tfont-size: 0.8em;\n}\n\t" \
    "table {\n\tmargin: 0.5em 0 1em 0;\n}\n\t" \
    "</style>\n" \
    "</head>\n"\
    "<body>"
    fileref.write(towrite)
    return

def czech_date(datetime):
    return f"{datetime.strftime('%d.').strip('0')} {datetime.strftime('%m.').strip('0')} {datetime.strftime('%Y')}"
def uka(data, depth = 0, ind = "."):
    """
    Prints out the structure of JSON-like container recursively
    Each level is indented by 'depth times ind' character sequence

    :param data: the list/dict structure to be shown
    :param depth: current depth of showKeyValueStructure recursion
    :param ind: string used for one level of indentation

    :return: nothing
    """
    tt = ind * depth
    depth += 1
    # if the currently passed json is a dictionary
    if isinstance(data, dict):
        if len(data) == 0:
            print(f"{tt}{data}: {{}}")
        else:
            for k, v in data.items():
                if isinstance(v, dict):
                    if len(v) == 0:
                        print(f"{tt}{k}: {{}}")
                    else:
                        print(f"{tt}{k}:")
                        uka(v, depth, ind)
                elif isinstance(v, list):
                    if len(v) == 0:
                        #
                        print(f"{tt}{k}: []")
                    else:
                        print(f"{tt}{k}:")
                        uka(v, depth, ind)
                else:
                    print(f"{tt}{k}: {v}")

    elif isinstance(data, list):
        if len(data) == 0:
            print(f"{tt}: []")
        else:
            i = 0
            for v in data:
                if len(v) == 0:
                    print(f"{tt}{i}: []")
                else:
                    print(f"{tt}{i}:")
                    uka(v, depth, ind)
                i += 1
    else:
        print(f"{tt}: {data}")

    return