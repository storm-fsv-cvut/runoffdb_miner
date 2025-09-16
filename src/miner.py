# -*- coding: utf-8 -*-

from datetime import timedelta
import os
import math
import locale
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates
import scipy
# from pint import UnitRegistry

from .entities import *
from .entities import interpolate_texture, integrate_by_time, integrate_by_minutes, integrate_data_series, get_zero_time, get_value_in_time
from .entities import czech_date


lang = "en"

# set locale for proper numbers formating
locale.setlocale(locale.LC_NUMERIC, lang)

# output files line delimiter
lined = "\n"
# output files cell delimiter
celld = ";"


class Miner:
    def __init__(self, date_from=None, date_to=None, simulators=None, localities=None, crops=None):
        # limits for data loads
        self.date_from = date_from
        self.date_to = date_to
        self.simulators = simulators
        self.localities = localities
        self.crops = crops

        # record type priorities
        self.runoff_types_view_order = [2, 1, 3, 4, 6, 7, 8, 5]
        self.ss_types_view_order = [2, 1, 3, 4, 6, 7, 8, 5]
        # 1 - raw data
        # 2 - edited data
        # 3 - homogenized edited data
        # 4 - homogenized raw data
        # 5 - set value
        # 6 - derived data
        # 7 - estimated from similar conditions
        # 8 - rough estimate

        self.runoffdb = RunoffDB()
        self.runoffdb.load_runs(date_from=date_from, date_to=date_to, simulators=simulators, localities=localities, crops=crops)

        if not self.runoffdb.runs:
            print("\n\033[91mMiner initialization error: no runs were fetched within provided limits.\033[00m\n")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
       return

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

    def generate_structured_dump(self, root_path, date_from=None, date_to=None, lang="en", no_data_value="NA"):

        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        # if the date limits differ from Miner assigned date limits
        if date_from < self.date_from or date_to > self.date_to:
            self.runoffdb.load_runs(date_from=date_from, date_to=date_to)


        try:
            os.mkdir(root_path) if not os.path.isdir(root_path) else None
        except OSError as error:
            print("Selected directory for the dump does not exist and it's not possible to create it.")
            print("Dump failed.")
            return

        # create RunoffDB instance
        with self.runoffdb as rdb:
            # get all dates when any simulation occurred
            all_days = rdb.get_simulation_days(date_from, date_to)
            print("\n")

            for day_start in all_days:
                day_end = day_start.replace(hour=23, minute=59, second=59)
                # load runs of the day
                day_runs = rdb.get_runs(date_from=day_start, date_to=day_end)
                # no runs on day with simulations is a result of unfinished/messed-up entry in DB (run group without any run)
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
                        print(f"#{run.id} - {czech_date(day_start)} - {rdb.localities[run.locality_id].name} - {rdb.crops[run.crop_id].name[lang]} - {run.plot_id} - {rdb.run_types[run.run_type_id].name[lang]}")
                        print(80 * "-")
                        try:
                            os.mkdir(sim_dir)
                        except OSError as error:
                            pass

                        # collect and save run metadata
                        with open(os.path.join(sim_dir, sim_dir_name+".json"), "w") as f:
                            json.dump(run.get_metadata(), f, ensure_ascii=False, indent=4)

                        rdb.clear_log()
                        run_log_path = os.path.join(sim_dir, "log.txt")

                        # loop through all phenomena and if measurement exists go through it's records
                        for phid in rdb.get_all_phenomena_ids():
                            msrmnts = run.get_measurements(phid)
                            if msrmnts is not None:
                                for ms in msrmnts:
                                    # loop through units and if record exists export it
                                    for uid in rdb.get_all_units_ids():
                                        rcrds = ms.get_records(unit_id=uid)
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
                                                    rec_filename = sanitize_path(f"{rec.id}-{rec.unit.name[lang]}-[{rec.unit.unit}]")

                                                    try:
                                                        data_df = rec.get_data("value", index_column=index_column)
                                                    except DataframeEmptyError as e:
                                                        print(f"\t{e.message}")
                                                        print(f"rec_filename: {rec_filename}")
                                                        print(f"sim_dir: {sim_dir}")
                                                        with open(os.path.join(sim_dir, rec_filename+".csv"), "w") as f:
                                                            f.write(e.message)
                                                    else:
                                                        if data_df is not None:

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
                                                            local_seps = {"celld": {"cz": ";", "en": ","}, "decd": {"cz": ",", "en": "."}}
                                                            data_df.to_csv(os.path.join(sim_dir, rec_filename+".csv"),
                                                                       index=index,
                                                                       sep=local_seps["celld"][lang],
                                                                       decimal=local_seps["decd"][lang],
                                                                       header=column_headers)
                                                            # except ValueError:
                                                            #     print(data_df)
                                                        else:
                                                            print(f"record {rec.id} ({rec.unit.name[lang]} [{rec.unit.unit}]) gains no data on load")
                                                            with open(os.path.join(sim_dir, rec_filename+".csv", "w")) as f:
                                                                f.write(f"record {rec.id} ({rec.unit.name[lang]} [{rec.unit.unit}]) gains no data on load")
                                            # print(f"{phid} - {len(ms.records)} ({', '.join([str(rid) for rid in recids])})")

                                            # get DataFrame with all hydro-sediment data

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

                                            try:
                                                hydro_data = run.get_best_hydro_data(request_map=request,
                                                                                     labels_map=labels)
                                            except RecordSetNotComplete as e:
                                                # if any of needed records is not available skip the run and log why
                                                rdb.log(run.id,
                                                        f"Following essential hydro-sediment records are not available: {', '.join([r for r in e.missing_records])}. "
                                                        f"\n\t=> Run was excluded from the export.")
                                            else:
                                                # hydro_data.fillna(no_data_value, inplace=True)
                                                # in case all hydro-sediment data are empty
                                                if hydro_data.empty:
                                                    rdb.log(run.id, f"\n\t=> Run has no hydro/sediment data.")

                                                # print(hydro_data[rain_int_label])
                                                # print(hydro_data[rain_tot_label])
                                                # print(hydro_data[runoff_label])
                                                # print(hydro_data[sed_conc_label])
                                                # print(hydro_data[sed_flux_label])
                                                # print(hydro_data[sed_yield_label])

                                                print(hydro_data)
                                                # print(hydro_data.index)
                                                print(hydro_data.columns.tolist())
                                                run.plot_hydro_data(hydro_data, os.path.join(sim_dir, "runoff.png"), [rain_int_label, rain_tot_label, runoff_label])
                        rdb.save_log(run_log_path)
                else:
                    print(f"\t{day_start.strftime('%Y-%m-%d')} skipped")
        return

    def generate_html_overview(self, output_path, date_from=None, date_to=None):
        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        # if the date limits differ from Miner assigned date limits
        if date_from < self.date_from or date_to > self.date_to:
            self.runoffdb.load_runs(date_from=date_from, date_to=date_to)

        cumulatives_headers1 = ["lokalita", "simID", "datum", "plot ID", "simulator ID", "plodina", "poc_stav",
                                      "poc_vlhkost", "canopy_cover", "BBCH", "intenzita", "TTR", "odtok_l",
                                      "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g"]
        cumulatives_headers2 = ["", "", "", "", "", "", "", "", "", "", "", "", "t1=10min", "", "t1=20min", "", "t1=30min"]

        output_html = open(output_path, "w")
        writeHTMLheader(output_html)

        with self.runoffdb as rdb:
            # get dates when some simulation occurred
            all_days = rdb.get_simulation_days(date_from, date_to)
            print("\n")

            for day_start in all_days:
                day_end = day_start.replace(hour=23, minute=59, second=59)
                # load runs of the day
                day_runs = rdb.get_runs(date_from=day_start, date_to=day_end)

                for run in day_runs:
                    output_html.write(f"<h2>#{run.id} - {run.datetime.strftime('%d. %m. %Y')} - {run.crop_name}, {self.run_types[run.run_type_id]} </h2>\n")
                    output_html.write("<table>\n")
                    writeRowToHTML(output_html, cumulatives_headers2, True)
                    writeRowToHTML(output_html, cumulatives_headers1, True)

                    output_html.write("</table>")
            output_html.write("</body>\n</html>")
            output_html.close()
        return


    def generate_interval_values_csv(self, output_path, date_from=None, date_to=None, lang="en", no_data_value="NA", log_file=None):
        """
        Generates runoff and sediment export for all simulation runs fitting into given limits.
        Each row in the output file represents a time interval in merged data of precipitation intensity, runoff rate, sediment concentration

        :param output_path:
        :param date_from:
        :param date_to:
        :param lang:
        :param no_data_value:
        :param interpolate_zero_time:
        :param log_file:
        :return:
        """
        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        with self.runoffdb as rdb:
            rdb.log_file_path = log_file
            # the runs may not be loaded yet ...
            rdb.load_runs(date_from=date_from, date_to=date_to)
            if not rdb.runs:
                print("\n\033[91mNo runs available within given limits.\033[00m\n")
                return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to)

            headers = {"cz": ["ID sekvence", "ID simulace", "ID lokality", "lokalita", "datum", "ID plochy", "název plochy", "délka plochy [m]",
                            "šířka plochy [m]", "sklon plochy [%]", "poznámky k ploše", "dnů od zasetí", "ochranné opatření", "ID simulátoru", "simulátor",
                              "ID plodiny", "plodina", "stav plodiny", "výška plodiny [cm]", "počet rostlin [1/m2]", "BBCH",  "zakrytí povrchu [%]",
                            "počáteční stav", "počáteční vlhkost","<0, 0.002mm>", "<0.002, 0.063mm>", "<0.063, 2mm>",
                              "objemová hmotnost [g/cm3]", "intenzita srážky [mm/h]",
                              "TTR", "interval", "délka intervalu", "t1", "t2", "srážkový úhrn [mm]",
                              "průtok [l/min]", "celkový odtok [l]", "koncentrace sedimentu [g/l]", "tok sedimentu[g/min]",
                              "ztráta půdy [g]"],
                     "en": ["sequence ID", "run ID", "locality ID", "locality", "date", "plot ID", "plot name", "plot length [m]",
                            "plot width [m]", "plot slope [%]", "plot notes", "days since seeding", "soil protection measure",
                            "simulator ID", "simulator", "crop ID", "crop", "crop condition", "crop height [cm]", "plant density [pcs.m^2]", "BBCH",  "surface cover [%]",

                            "initial cond.", "init. moisture","<0, 0.002mm>", "<0.002, 0.063mm>", "<0.063, 2mm>",
                            "bulk density [g.cm-3]", "rain intensity [mm.h-1]",
                            "time to runoff", "interval #", "interval duration", "t1", "t2", "rainfall total [mm]",
                            "flow rate [l.min-1]", "total discharge [l]", "SS concentration [g.l-1]", "SS flux [g.min-1]",
                            "sediment yield[g]"]}

            # open the output file for writing
            output_csv = open(output_path, "w", encoding="utf-8")
            # and write the column headers
            writeRowToCSV(output_csv, headers[lang])

            # list of records that have issues - database content debugging
            invalid_record_ids = []

            num_runs_tot = 0
            num_runs_exported = 0
            num_runs_empty = 0

            for run in runs:
                num_runs_tot += 1
                # single row to be filled and written to the output files
                # one line represents one time interval of a measured time series within a run
                line = []

                print(f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")

                # gather all the info and values common for the whole simulation run ===================================
                # id of sequence the run belungs to
                line.append(run.sequence_id)
                # id of the run
                line.append(run.id)
                # id of locality where the run took place
                line.append(run.locality.id)
                # name of the locality where the run took place
                line.append(run.locality.name)
                # date of the run
                line.append(czech_date(run.datetime))
                # id of the plot where the run was executed
                line.append(run.plot_id)
                # name of the plot where the run was executed
                line.append(run.plot.name)
                # length in meters of the plot where the run was executed
                line.append(run.plot.plot_length)
                line.append(run.plot.plot_width)
                line.append(run.plot.plot_slope)
                line.append(run.plot.note[lang].replace(";", ".") if run.plot.note[lang] else no_data_value)
                line.append(run.plot.days_since_seeding(run.datetime) or no_data_value)
                line.append(run.plot.get_protection_measures_names(lang))
                line.append(run.simulator.id)
                line.append(run.simulator.name[lang])
                line.append(run.crop_id if run.crop_id else no_data_value)
                line.append(run.crop.name[lang] if run.crop else no_data_value)
                line.append(f"\"{run.crop_condition[lang]}\"" if run.crop_condition[lang] else no_data_value)
                line.append(run.get_crop_height_value() or no_data_value)
                line.append(run.get_plant_density_value() or no_data_value)
                line.append(run.bbch or no_data_value)
                line.append(run.get_surface_cover_value() or no_data_value)
                line.append(run.run_type.name[lang])
                line.append(run.get_initial_moisture_value() or no_data_value)

                # get and transform soil texture data
                WRB_fraction_limits = [0.002, 0.063, 2]
                try:
                    texture_data = run.get_best_soil_texture_data("cumulative_mass_content", "particle_size", index_column="particle_size",
                                             order_by="particle_size", limits=WRB_fraction_limits)
                except DataframeEmptyError as dee:
                    line.extend(len(WRB_fraction_limits) * [no_data_value])
                    rdb.log(run.id, f"soil texture data missing: {dee.message}")

                else:
                    if texture_data is not None:
                        for i, cl in enumerate(WRB_fraction_limits):
                            line.append(texture_data.loc[WRB_fraction_limits[i], 'cumulative_mass_content'])
                    else:
                        line.extend(len(WRB_fraction_limits) * [no_data_value])

                # get bulk density data
                try:
                    line.append(run.get_best_bulk_density_value(27) or no_data_value)
                except DataframeEmptyError as dee:
                    line.append(no_data_value)
                    rdb.log(run.id, f"bulk density data missing: {dee.message}")

                try:
                    line.append(run.get_rainfall_intensity_value(6) or no_data_value)
                except DataframeEmptyError as dee:
                    line.append(no_data_value)
                    rdb.log(run.id, f"rainfall intensity data missing: {dee.message}")

                line.append(run.ttr)

                # get DataFrame with all hydro-sediment data

                rain_int_label = "rainfall_intensity"
                rain_tot_label = "rainfall_total"
                runoff_label = "runoff"
                discharge_label = "discharge"
                sed_conc_label = "sediment_concentration"
                sed_flux_label = "sediment_flux"
                sed_yield_label = "sediment_yield"

                request = {
                    "rainfall_intensity": True,
                    "rainfall_total": True,
                    "runoff": True,
                    "sediment_concentration": True,
                    "discharge": True,
                    "sediment_flux": True,
                    "sediment_yield": True
                }

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

                try:
                    hydro_data = run.get_best_hydro_data(request_map=request, labels_map=labels)
                except RecordSetNotComplete as e:
                    # if any of needed records is not available skip the run and log why
                    rdb.log(run.id, f"Following essential hydro-sediment records are not available: {', '.join([r for r in e.missing_records])}. "
                                    f"\n\t=> Run was excluded from the export.")
                    continue
                else:
                    hydro_data.fillna(no_data_value, inplace=True)
                    # in case all hydro-sediment data are empty
                    if hydro_data.empty:
                        no_data_row = [no_data_value] * (len(hydro_data.columns) + 4)  # +4 for index fields
                        writeRowToCSV(output_csv, line + no_data_row)
                        num_runs_empty += 1
                        num_runs_exported += 1
                        rdb.log(run.id, f"\n\t=> Run exported with no hydro/sediment data.")
                        continue

                # print(hydro_data[rain_int_label])
                # print(hydro_data[rain_tot_label])
                # print(hydro_data[runoff_label])
                # print(hydro_data[sed_conc_label])
                # print(hydro_data[sed_flux_label])
                # print(hydro_data[sed_yield_label])

                i = 1
                prev_index = None
                for index, row in hydro_data.iterrows():
                    line_int = []

                    t1 = format_timedelta(index)
                    # skip time intervals before runoff appeared
                    if run.ttr > index:
                        continue
                    t2 = format_timedelta(index - run.ttr)
                    int_dur = format_timedelta(index-prev_index) if prev_index is not None else no_data_value

                    line_int.append(i)
                    line_int.append(int_dur)
                    line_int.append(t1)
                    line_int.append(t2)
                    line_int.append(row[rain_tot_label])
                    line_int.append(row[runoff_label])
                    line_int.append((row[discharge_label]))
                    line_int.append(row[sed_conc_label])
                    line_int.append(row[sed_flux_label])
                    line_int.append(row[sed_yield_label])

                    # line_int.append(runoff_record.record_type.name[lang])
                    # line_int.append(runoff_record.quality_index.name[lang] if runoff_record.quality_index is not None else no_data_value)
                    # line_int.append(sed_conc_record.record_type.name[lang])
                    # line_int.append(sed_conc_record.quality_index.name[lang] if sed_conc_record.quality_index is not None else no_data_value)
                    prev_index = index
                    i += 1
                    # print(line_int)
                    writeRowToCSV(output_csv, line+line_int)
                rdb.log(run.id, f"\n\t=> Run successfully exported.")
                num_runs_exported += 1

            output_csv.close()

            print(f"\nexported {num_runs_exported} runs of {num_runs_tot}")
            print(f"{num_runs_empty} of which is empty") if num_runs_empty > 0 else None
            #
            # if len(invalid_record_ids) > 0:
            #     print(f"\n\nUPDATE `record` set `is_timeline` = 1 WHERE `id` IN ({', '.join([str(rid) for rid in invalid_record_ids])})")

        return


    def generate_cumulative_values_csv(self, output_path, date_from=None, date_to=None, lang="en", no_data_value="NA", logfile_path=None, plots_dir=None):
        """

        :param output_path: path of the output file
        :param logfile_path:
        :param plots_dir: directory path for plots
        :return:
        """
        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        # if the date limits differ from Miner assigned date limits
        if date_from < self.date_from or date_to > self.date_to:
            self.runoffdb.load_runs(date_from=date_from, date_to=date_to)

        with self.runoffdb as rdb:
            rdb.log_file_path = logfile_path
            # the runs may not be loaded yet ...
            if rdb.runs is None:
                rdb.load_runs(date_from=date_from, date_to=date_to)
                if not rdb.runs:
                    print("\n\033[91mNo runs available within given limits.\033[00m\n")
                    return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to)

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
            output_csv = open(output_path, "w", encoding="utf-8")
            writeRowToCSV(output_csv, cumulatives_headers2)
            writeRowToCSV(output_csv, cumulatives_headers1[lang])

            # just for the counter
            i = 1
            num_runs = len(rdb.runs)
            for run in runs:
                # show the counter
                print(f"{i}/{num_runs}")
                i += 1

                run_title = f"#{run.id} - {czech_date(run.datetime)} - {rdb.localities[run.locality_id]['name']} - {run.get_crop_name(lang)} [{run.plot_id}], {rdb.run_types[run.run_type_id]} {{{run.ttr}}}"
                print(run_title)
                # run.show_details()

                # one row to be filled and written to the output files
                line = []

                # get best runoff, sediment concentration and rainfall data
                runoff_record = run.get_best_runoff_record()
                sed_conc_record = run.get_best_sediment_concentration_record()
                rainfall_record = run.get_best_rainfall_record()

                # if both records were found
                if runoff_record is not None and sed_conc_record is not None and rainfall_record is not None:
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
                        line.append(run.locality.name)
                        line.append(run.id)
                        line.append(czech_date(run.datetime))
                        line.append(run.plot_id)
                        line.append(run.simulator.id)
                        line.append(run.crop.name[lang] or no_data_value)
                        line.append(run.crop.crop_type.name[lang] or no_data_value)
                        line.append(run.run_type.name[lang])
                        line.append(run.get_initial_moisture_value() or no_data_value)
                        line.append(run.get_surface_cover_value() or no_data_value)
                        line.append(run.bbch or no_data_value)
                        line.append(run.get_rainfall_intensity_value() or no_data_value)
                        line.append(run.ttr)


                        runoff_label = f"runoff [{rdb.units.get(runoff_record.unit_id).unit}]"
                        sed_conc_label = f"sed. conc. [{rdb.units.get(3).unit}]"
                        sed_flux_label = f"sed. flux [{rdb.units.get(25).unit}]"
                        tot_runoff_label = "total runoff"
                        sed_mass_label = "sediment mass"

                        # a common zero time is added (if possible) to force the integration from very start
                        # and to allow for cross-interpolation if the sediment series starts later than the runoff series
                        t0 = get_zero_time(runoff_data)
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
                        line.append(integrate_by_minutes(merged_data, t0, pd.Timedelta(minutes=10), runoff_label, zero_time=t0, extrapolate=2))
                        line.append(integrate_by_minutes(merged_data, t0, pd.Timedelta(minutes=10), sed_flux_label, zero_time=t0, extrapolate=2))

                        line.append(integrate_by_minutes(merged_data, t0, pd.Timedelta(minutes=20), runoff_label, zero_time=t0, extrapolate=2))
                        line.append(integrate_by_minutes(merged_data, t0, pd.Timedelta(minutes=20), sed_flux_label, zero_time=t0, extrapolate=2))

                        line.append(integrate_by_minutes(merged_data, t0, pd.Timedelta(minutes=30), runoff_label, zero_time=t0, extrapolate=2))
                        line.append(integrate_by_minutes(merged_data, t0, pd.Timedelta(minutes=30), sed_flux_label, zero_time=t0, extrapolate=2))

                        # write the row to output
                        writeRowToCSV(output_csv, line)

                        if plots_dir:
                            plot_series_to_file(merged_data, [runoff_label, tot_runoff_label], os.path.join(plots_dir, f"{run.id}_runoff"), run_title, "time [min]", ["runoff rate [l.min-1]", "total runoff [l]"], True)
                            merged_data.index = merged_data.index.map(lambda x: format_timedelta_hms(x))
                            merged_data.to_csv(os.path.join(plots_dir, f"{run.id}_runoff_sediment.csv"), sep=celld, decimal= ",")

                    else:
                        print(f"One or both data series of run {run.id} is empty!\n"
                              f"... which really shouldn't happen as the Record.get_data() returns None when the Record.data is empty dataframe ...")

            # close the files if were opened
            output_csv.close()
        return
    def generate_soilpulse_csv(self, output_path, date_from=None, date_to=None, lang="en", no_data_value="NA", logfile_path=None):
        """

        :param output_path: path of the output file
        :param logfile_path:
        :param plots_dir: directory path for plots
        :return:
        """
        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        # if the date limits differ from Miner assigned date limits
        if date_from < self.date_from or date_to > self.date_to:
            self.runoffdb.load_runs(date_from=date_from, date_to=date_to)

        with self.runoffdb as rdb:
            rdb.log_file_path = logfile_path
            # the runs may not be loaded yet ...
            if rdb.runs is None:
                rdb.load_runs(date_from=date_from, date_to=date_to)
                if not rdb.runs:
                    print("\n\033[91mNo runs available within given limits.\033[00m\n")
                    return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to)

            velocities_filename = "velocities.csv"

            headers = {"cz": ["lokalita", "lat", "long", "simID", "datum", "plot ID", "simulator", "plodina", "typ_plodiny", "poc_stav",
                              "poc_vlhkost", "canopy_cover", "BBCH", "intenzita_srazky_mm_h-1", "TTR", "objemova_hmotnost_g_cm-3",
                               "celkovy_cas_s", "srazkovy_uhrn_mm", "celkovy_odtok_l", "celkova_ztrata_pudy_g"],
                        "en": ["locality", "latitude", "longitude", "run ID", "date", "plot ID", "simulator", "crop", "crop type", "initial cond.",
                               "init. moisture", "canopy cover", "BBCH", "rain intensity [mm.h^-1]", "time to runoff", "bulk density [g.cm^-3]",
                                   "total time [s]", "total rainfall [mm]", "total discharge [l]", "total soil loss [g]"]}

            # open the file for writing, overwrite if exists, write file headers
            output_csv = open(output_path, "w", encoding="utf-8")
            writeRowToCSV(output_csv, headers[lang])

            # just for the counter
            i = 1
            num_runs = len(runs)
            for run in runs:
                # show the counter
                print(f"{i}/{num_runs}")
                i += 1

                run_title = f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} [{run.plot_id}], {run.run_type.name[lang]} {{{run.ttr}}}"
                print(run_title)
                # run.show_details()

                # one row to be filled and written to the output files
                line = []

                # get best runoff, sediment concentration and rainfall data
                runoff_record = run.get_best_runoff_record()
                sed_conc_record = run.get_best_sediment_concentration_record()
                rainfall_record = run.get_best_rainfall_record()

                # if all necessary records were found
                if runoff_record is not None and sed_conc_record is not None and rainfall_record is not None:
                    # prepare column labels for dataframes
                    runoff_label = "runoff_rate"
                    discharge_label = "total_discharge"
                    sed_conc_label = "sediment_concentration"
                    sed_flux_label = "sediment_flux"
                    sed_yield_label = "sediment_yield"

                    rain_int_label = "rainfall_intensity"
                    rain_tot_label = "rainfall_total"

                    # get rainfall intensity data in mm.min^-1
                    rainfall_data = rainfall_record.get_data_in_unit(28, rain_int_label)
                    # get runoff data in [l.min-1]
                    runoff_data = runoff_record.get_data_in_unit(1, runoff_label)
                    # get the sediment concentration data in [g.l-1]
                    sediment_data = sed_conc_record.get_data_in_unit(3, sed_conc_label)

                    # if all needed dataframes are not empty
                    if not runoff_data.empty and not sediment_data.empty and not rainfall_data.empty:
                        line.append(run.locality.name)
                        line.append(run.locality.lat)
                        line.append(run.locality.lng)
                        line.append(run.id)
                        line.append(run.datetime)
                        line.append(run.plot_id)
                        line.append(run.simulator.name[lang])
                        line.append(run.crop.name[lang] or no_data_value)
                        line.append(run.crop.crop_type.name[lang] or no_data_value)
                        line.append(run.run_type.name[lang])
                        line.append(run.get_initial_moisture_value() or no_data_value)
                        line.append(run.get_surface_cover_value() or no_data_value)
                        line.append(run.bbch or no_data_value)
                        line.append(run.get_rainfall_intensity_value(6) or no_data_value)
                        line.append(run.ttr)
                        line.append(run.get_bulk_density_value(27) or no_data_value)

                        # merge the dataframes into one with common 'time' index
                        merged_data = pd.concat([runoff_data, sediment_data, rainfall_data], axis=1, join='outer')
                        # re-order the rows by time
                        try:
                            merged_data.sort_index(inplace=True)
                        except TypeError as e:
                            print(
                                f"Incompatible indexes in input dataframes - one or more of input dataframes is not a timeline")

                        # Limit the index to end at the last time point of the runoff or sediment data
                        end_time = min(merged_data[runoff_label].last_valid_index(),
                                       merged_data[sed_conc_label].last_valid_index())
                        merged_data = merged_data.loc[:end_time]

                        # cross-interpolate for the runoff and sediment concentration values
                        merged_data[runoff_label] = merged_data[runoff_label].interpolate(method='linear')
                        merged_data[sed_conc_label] = merged_data[sed_conc_label].interpolate(method='linear')

                        # forward-fill the rainfall intensity data and fill any remaining sediment concentration gaps
                        merged_data[rain_int_label] = merged_data[rain_int_label].ffill()
                        merged_data[sed_conc_label] = merged_data[sed_conc_label].fillna(0)
                        # calculate the sediment flux [g.min-1]
                        merged_data[sed_flux_label] = merged_data[runoff_label] * merged_data[sed_conc_label]
                        # print(f"runoff + sediment concentration rainfall + sediment flux:\n{merged_data}\n\n")
                        # integrate rainfall data stepwise to [mm]
                        integrate_data_series(merged_data, rain_int_label, rain_tot_label,
                                              interpolate=False, time_unit='minutes')
                        # integrate runoff data linearly to [l]
                        integrate_data_series(merged_data, runoff_label, discharge_label, interpolate=True,
                                              time_unit='minutes')

                        # integrate sediment flux to get total sediment yield in [g]
                        integrate_data_series(merged_data, sed_flux_label, sed_yield_label, interpolate=True,
                                              time_unit='minutes')

                        # print(merged_data[runoff_label])
                        # print(merged_data[sed_conc_label])
                        # print(merged_data[sed_flux_label])
                        # print(merged_data[sed_yield_label])
                        line.append(format_timedelta_s(end_time))
                        line.append(merged_data[rain_tot_label][end_time])
                        line.append(merged_data[discharge_label][end_time])
                        line.append(merged_data[sed_yield_label][end_time])

                        writeRowToCSV(output_csv, line)
                    else:
                        print(f"One or both data series of run {run.id} is empty!\n"
                              f"... which really shouldn't happen as the Record.get_data() returns None when the Record.data is empty dataframe ...")

            # close the file
            output_csv.close()

        return

    def generate_euro_table(self, output_path, date_from=None, date_to=None, logfile_path=None):

        # if the date limits differ from Miner assigned date limits
        if date_from < self.date_from or date_to > self.date_to:
            self.runoffdb.load_runs(date_from=date_from, date_to=date_to)

        with self.runoffdb as rdb:
            rdb.log_file_path = logfile_path
            # the runs may not be loaded yet ...
            if rdb.runs is None:
                rdb.load_runs()
                if not rdb.runs:
                    print("\n\033[91mNo runs available within given limits.\033[00m\n")
                    return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to)

            lines = []
            catchThem = []

            for run in runs:
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
                        print("Assigned texture soil sample doesn't have texture record assigned!")
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
                    # get the total by integrating the intensity timeline
                    rainfall_mm = round(integrate_by_time(intensity_data, "rain_intensity", interpolate=False, time_unit='hours'), 0)

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
                                    runoff_l = integrate_by_minutes(runoff_data, runoff_label,
                                                                        zero_time=get_zero_time(runoff_data, runoff_label))
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
                    found_records = run.get_records([2, 3], 2, record_type_id=record_type)
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
                                get_zero_time(sediment_data, sed_conc_label)
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
                        t0 = get_zero_time(runoff_data, runoff_label)
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
                            soilloss_g = integrate_by_minutes(merged_data, sed_flux_label, zero_time=t0, extrapolate=1)
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
            output_csv = open(output_path, "w", encoding="utf-8")
            # writeRowToCSV(output_csv, poznamky)
            writeRowToCSV(output_csv, notes)
            writeRowToCSV(output_csv, headers)

            for line in lines:
                writeRowToCSV(output_csv, line)

            output_csv.close()
            # print record IDs with
            if len(catchThem) > 0:
                print("following records don't have correct TimeDelta index:\n"+", ".join([str(c) for c in catchThem]))

        return

    def generate_overview_html(self, output_path, date_from=None, date_to=None, lang="en", no_data_value="NA", log_file=None, **kwargs):
        """
        Generates HTML overview of all simulations and all measurements, all assigned records
        Each row in the output file represents a time interval in merged data of precipitation intensity, runoff rate, sediment concentration

        :param output_path:
        :param date_from:
        :param date_to:
        :param lang:
        :param no_data_value:
        :param log_file:
        :return:
        """
        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        # if the date limits differ from Miner assigned date limits
        if date_from < self.date_from or date_to > self.date_to:
            self.runoffdb.load_runs(date_from=date_from, date_to=date_to)

        with self.runoffdb as rdb:
            rdb.log_file_path = log_file
            # the runs may not be loaded yet ...
            if rdb.runs is None:
                rdb.load_runs(date_from=date_from, date_to=date_to)
                if not rdb.runs:
                    print("\n\033[91mNo runs available within given limits.\033[00m\n")
                    return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to)

            # construct the headers
            headers = {"cz": ["ID simulace",
                              "ID sekvence",
                              "další simulace ve skupině",
                              "ID lokality",
                              "lokalita",
                              "datum",
                              "čas spuštění simulátoru",
                              "ID simulátoru",
                              "simulátor",
                              "ID plochy",
                              "název plochy",
                              "délka plochy [m]",
                              "ID plodiny",
                              "plodina",
                              "povrchový odtok začal",
                              "začátek p.o.",
                              "přiřazená srážka",
                              "přiřazená počáteční vlhkost",
                              "přiřazený surface cover",
                              "hodnota bbch",
                              "půdní vzorek objemová hmotnost",
                              "typ přiřazení objemová hmotnost",
                              "půdní vzorek zrnitost",
                              "typ přiřazení zrnitost",
                              "půdní vzorek Corg",
                              "typ přiřazení Corg",
                              ],
                     "en": ["run ID",
                            "sequence ID",
                            "locality ID",
                            "other runs in group"
                            "locality",
                            "date",
                            "time of simulator start"
                            "simulator ID",
                            "simulator",
                            "plot ID",
                            "plot name",
                            "plot length [m]",
                            "crop ID",
                            "crop",
                            "surface runoff initiated",
                            "time to runoff",
                            "dedicated precipitation record",
                            "dedicated initial moisture record",
                            "dedicated surface cover record",
                            "bbch value",
                            "soil sample bulk density",
                            "assignment type bulk density",
                            "soil sample texture",
                            "assignment type texture",
                            "soil sample Corg",
                            "assignment type Corg"]}


            for uid, unit in rdb.units.items():
                # append the unit name to appropriate header language
                for ll in headers.keys():
                    headers[ll].append(f"({uid}) {unit.name[ll]} [{unit.unit}]")

            # open the output file for writing
            output_html = open(output_path, "w", encoding="utf-8")
            # write the HTML headers
            writeHTMLheader(output_html, html_title="runs overview", lang=lang)

            # start the table
            output_html.write("<div>\n<table>\n")
            # and write the column headers
            writeRowToHTML(output_html, headers[lang], is_header=True)

            for run in runs:
                print()
                # single row to be filled and written to the output files
                # one line represents one time interval of a measured time series within a run
                line = []

                print(f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
                run_row = [run.id, run.sequence_id, len(run.load_group_brothers()),
                             run.locality_id,
                             run.locality.name,
                             czech_date(run.datetime),
                             run.datetime.strftime('%H:%M'),
                             run.simulator_id,
                             run.simulator.name[lang],
                             run.plot_id,
                             run.plot.name,
                             f"{run.plot.plot_length:.1f}",
                             run.crop_id,
                             run.crop.name[lang]
                             ]
                run_row.append("1" if run.ttr else "0")
                run_row.append(format_timedelta(run.ttr) if run.ttr else no_data_value)
                run_row.append(run.rain_intensity_recid or no_data_value)
                run_row.append(run.initmoist_recid or no_data_value)
                run_row.append(run.surface_cover_recid or no_data_value)
                run_row.append(run.bbch or no_data_value)
                run_row.append(run.bulkd_ss_id or no_data_value)
                run_row.append(run.bulkd_ss_asstype.description[lang] if run.bulkd_ss_asstype else "")
                run_row.append(run.texture_ss_id or no_data_value)
                run_row.append(run.texture_ss_asstype.description[lang] if run.texture_ss_asstype else "")
                run_row.append(run.corg_ss_id or no_data_value)
                run_row.append(run.corg_ss_asstype.description[lang] if run.corg_ss_asstype else "")

                run_records = run.get_records()
                # print(f"run_records: {', '.join([str(r) for r in run_records]) if run_records else 'None'}")

                # check records for every unit in units table
                for uid, unit in rdb.units.items():
                    if run_records:
                        rr = []
                        for r in run_records:
                            if r.unit_id == uid:
                                rr.append(r)
                        # run_row.append(len(rr))
                        # print(f"{uid} rr: {', '.join([str(kr) for kr in rr]) if rr else 'None'}")
                        run_row.append(", ".join([str(rrr.id) for rrr in rr]))

                    else:
                        run_row.append("")
               # for ph in rdb.phenomena.values():

                writeRowToHTML(output_html, run_row)

            output_html.write("</table>\n</div>\n")
            writeHTMLfooter(output_html)

        return

    def compare_discharge_calculation_methods(self, output_dir=None,
                             date_from=None,
                             date_to=None,
                             lang="en",
                             simulators=None,
                             localities=None,
                             crops=None,
                             log_file=None):
        """

        """

        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        results = []
        with self.runoffdb as rdb:
            rdb.log_file_path = log_file
            # the runs may not be loaded yet ...
            rdb.load_runs(date_from=date_from, date_to=date_to)
            if not rdb.runs:
                print("\n\033[91mNo runs available within given limits.\033[00m\n")
                return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to, simulators=simulators, localities=localities, crops=crops)

            print("\n\nSimulations with raw discharge data records\n"+80*"=")

            raw_runs = [run for run in runs if run.get_records(unit_id=5, related_value_x_unit_id=15, record_type_id=1)]

            if len(raw_runs) > 0:
                print("following runs have the raw discharge data stored in db:")

                for run in raw_runs:
                    run.show_records()
                    print(f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
                    # print(f"\trecord #{run.get_best_record_of_unit(unit_id=5, related_value_x_unit_id=15).id}")
                    raw_record = run.get_best_record_of_unit(unit_id=5, related_value_x_unit_id=15)
                    print(f"raw_record.id: {raw_record.id}")
                    raw_data = raw_record.get_data("sample_volume_l", "sampling_duration_s")
                    # print(raw_data)

                    run_results = {"run": run, "raw_data":raw_data}
                    integrated_data = {}
                    for case in ["start", "sample_mid", "sample_end", "interval_mid"]:
                        cumulative = integrate_flow(raw_data, 'sample_volume_l', 'sampling_duration_s', placement=case, interpolate=True,
                                               time_unit='seconds')
                        integrated_data[case] = cumulative
                        # print(f"\nflow value placement '{case}'\n{cumulative}:")

                    run_results["integrated_data"] = integrated_data
                    results.append(run_results)

                print(f"\n>>> {len(raw_runs)} simulations with raw discharge data in total")
            else:
                print("\tNo runs with raw data found in db.")

            # draw the results to plot
            if len(results) > 0:
                print("\nplotting and summarizing results ...")
                overview_rows = []
                for run_results in results:
                    run = run_results["run"]
                    file_name = f"{run.id}_{run.datetime.strftime('%Y%m%d')}_{re.sub(r'[.]', '_', run.locality.name)}_{run.crop.name[lang]}_{run.run_type.name[lang]}.png"
                    file_path = os.path.join(output_dir, file_name)

                    plot_title = f"{run.simulator.name[lang]}\n#{run.id} - {czech_date(run.datetime)} '{run.locality.name}' ({run.crop.name[lang]}, {run.run_type.name[lang]})"
                    plot_flow_comparison(run_results["raw_data"],
                                         run_results["integrated_data"],
                                         "sample_volume_l", "sampling_duration_s",
                                         title=plot_title,
                                         output=file_path)

                    # --- Extract final discharge values for overview ---
                    integrated_data = run_results["integrated_data"]

                    # Get "start" case DataFrame
                    start_df = integrated_data.get("start")
                    if start_df is None:
                        continue

                    # Common time: start of last sampling (in "start" case)
                    last_start_time = start_df.index[-1]

                    # Interval start value = last value in "start" discharge
                    interval_start_val = start_df.loc[last_start_time, "cum_discharge"]

                    # Get "sample_mid" case DataFrame
                    mid_df = integrated_data.get("sample_mid")
                    if mid_df is None:
                        continue

                    # Interpolate sample_mid discharge to last_start_time
                    sample_mid_val = mid_df["cum_discharge"].reindex(
                        mid_df.index.union([last_start_time])
                    ).interpolate(method="time").loc[last_start_time]

                    # Get "sample_end" case DataFrame
                    end_df = integrated_data.get("sample_end")
                    if end_df is None:
                        continue

                    # Interpolate sample_end discharge to last_start_time
                    sample_end_val = end_df["cum_discharge"].reindex(
                        end_df.index.union([last_start_time])
                    ).interpolate(method="time").loc[last_start_time]

                    # calculate differences relative to interval_start value

                    overview_rows.append({
                        "run_id": run.id,
                        "interval_start": interval_start_val,
                        "sample_mid": sample_mid_val,
                        "sample_end": sample_end_val,
                        "sample_mid_diff": sample_mid_val/interval_start_val-1,
                        "sample_end_diff": sample_end_val/interval_start_val-1
                    })
                print(f"\trun discharge plots saved")

                # Save overview CSV
                overview_df = pd.DataFrame(overview_rows)
                overview_path = os.path.join(output_dir, "_overview.csv")
                overview_df.to_csv(overview_path, index=False)
                print(f"\tresults overview saved to: {overview_path}")

                plot_frequency_analysis(overview_df, os.path.join(output_dir, "_diff_frequency.png"))

                print(" ... successful.")
        return

    def calculate_SLR(self, output_file=None,
                             date_from=None,
                             date_to=None,
                             lang="en",
                             simulators=None,
                             localities=None,
                             crops=None,
                             log_file=None):

        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        self.runoffdb.load_runs(date_from=date_from, date_to=date_to)

        with self.runoffdb as rdb:
            rdb.log_file_path = log_file
            # the runs may not be loaded yet ...
            rdb.load_runs(date_from=date_from, date_to=date_to)
            if not rdb.runs:
                print("\n\033[91mNo runs available within given limits.\033[00m\n")
                return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to, simulators=simulators, localities=localities,
                                crops=crops)
            no_fallow = 0
            one_fallow = 0
            more_fallows = 0

            for run in runs:
                # only for non-fallow runs ...
                if run.crop_id != 1:
                    print(f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")

                    fallows = run.find_fellow_fallow()
                    if len(fallows) == 0:
                        print("\t\033[91mno fellow fallow found\033[00m")
                        no_fallow += 1
                        rdb.log(run.id, f"no fallow found")

                    elif len(fallows) == 1:
                        print(f"\t\033[92mmy fellow fallow is #{fallows[0].id}\033[00m")
                        rdb.log(run.id, f"single fallow #{fallows[0].id}")

                        try:
                            crop_hydrodata = run.get_best_hydro_data()
                        except RecordSetNotComplete:
                            print(f"\n\n\033[91mHydro-sediment record set of crop run is not complete\033[00m")
                            pass
                        else:
                            print("crop hydrodata ready")
                            print(crop_hydrodata)

                        try:
                            fallow_hydrodata = run.get_best_hydro_data()
                        except RecordSetNotComplete:
                            print(f"\n\n\033[91mHydro-sediment record set of fallow run is not complete\033[00m")
                            pass
                        else:
                            print("fallow hydrodata ready")
                            print(fallow_hydrodata)
                        one_fallow += 1
                    else:
                        print(f"\t\033[93mseems like I have to choose between {', '.join([str(f.id) for f in fallows])}\033[00m")
                        more_fallows += 1
                        rdb.log(run.id, f"multiple matching fallows found: {', '.join([str(f.id) for f in fallows])}")

                else:
                    print(f"\n\033[96m==> #{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}\033[00m")

            print(f"\n\n\033[91mno fallow: {no_fallow}\033[00m")
            print(f"\033[92mone fallow: {one_fallow}\033[00m")
            print(f"\033[93mmore fallows: {more_fallows}\033[00m")
        return

    def show_plots_overview(self):
        with RunoffDB as rdb:
            print(f"")
            for id, plot in rdb.plots.items():
                print(f"\n{id} - {plot.name}")
                print(f"\testablished {czech_date(plot.established)}")
                print(f"\tprotection measures :") if plot.protection_measures else None

                for measure in plot.protection_measures:
                    print(f"\t\t{measure.id} - {measure.name['cz']}")
        return

    def show_methodics(self, lang="en"):
        print("\n\n")
        for meth in self.runoffdb.methodics.values():
            meth.show_details(lang)
    def export_methodics(self, output_path, lang="en"):
        export = self.runoffdb.export_methodics(lang)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export, f, ensure_ascii=False, indent=4)

        print(f"All methodics successfully exported to '{output_path}'")
        return

    def show_run_notes(self, date_from=None, date_to=None, lang="en", log_file=None):
        # use Miner date limits if no limits provided
        date_from = date_from or self.date_from
        date_to = date_to or self.date_to

        with self.runoffdb as rdb:
            rdb.load_runs(date_from=date_from, date_to=date_to)
            rdb.log_file_path = log_file
            # the runs may not be loaded yet ...
            if rdb.runs is None:
                rdb.load_runs(date_from=date_from, date_to=date_to)
                if not rdb.runs:
                    print("\n\033[91mNo runs available within given limits.\033[00m\n")
                    return

            runs = rdb.get_runs(date_from=date_from, date_to=date_to)

            for run in runs:
                print(
                    f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
                print(run.get_notes(lang))

        return

def sanitize_path(path_str):
    # Replace invalid characters for both Windows and Unix-like systems
    sanitized = re.sub(r'[<>:"/\\|?* ]', '_', path_str)
    return sanitized
#
# def interpolate_texture(original_texture, new_limits, cum_mass_col_name, return_cumulative = True, return_int = True, smallest_content = 1):
#     # ensure original_texture is a pandas DataFrame
#     if not isinstance(original_texture, pd.DataFrame):
#         raise TypeError("original_texture parameter value must be a pandas DataFrame")
#
#     # get column names from the input DataFrame
#     particle_size_col = original_texture.index.name
#
#     # extract original limits and cumulative contents from the DataFrame
#     original_limits = original_texture.index.to_list()
#     original_contents = original_texture[cum_mass_col_name].to_list()
#
#     # insert artificial first datapoint with the smallest content to allow for interpolation of smaller particles content
#     original_limits.insert(0, 0)
#     original_contents.insert(0, smallest_content)
#
#     # sort the new limits
#     new_limits = sorted(new_limits)
#
#     cumul_contents = []
#
#
#     for nl in new_limits:
#         i = 0
#         for ol, content in zip(original_limits, original_contents):
#             if i == 0:
#                 prev_ol = ol
#                 prev_content = content
#             else:
#                 if nl > prev_ol and nl <= ol:
#                     new_value = prev_content + ((content - prev_content) / (ol - prev_ol)) * (nl - prev_ol)
#                     cumul_contents.append(new_value)
#                 prev_ol = ol
#                 prev_content = content
#             i += 1
#
#     # round the content values to integers if requested
#     if return_int:
#         cumul_contents = [round(val) for val in cumul_contents]
#
#     # recalculate cumulative values to net values if requested
#     if not return_cumulative:
#         net_contents = [cumul_contents[0]]
#         for j in range(1, len(cumul_contents)):
#             net_contents.append(cumul_contents[j] - cumul_contents[j - 1])
#         output_contents = net_contents
#     else:
#         output_contents = cumul_contents
#
#     # create the output DataFrame with the same column names as the input DataFrame
#
#     output_df = pd.DataFrame({
#         particle_size_col: new_limits,
#         cum_mass_col_name: output_contents
#     })
#
#     # set particle_size as the index
#     output_df.set_index(particle_size_col, inplace=True)
#
#     return output_df

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

def plot_flow_comparison(df_original, integrated_results, value_col, duration_col, title=None, output=None):
    """
    Plot sample volumes (bars), flow rates (lines), and cumulative discharges (lines) for multiple placement cases.

    Parameters
    ----------
    df_original : pandas.DataFrame
        Original data with TimedeltaIndex (sampling START times), containing raw sample volumes and durations.
    integrated_results : dict
        {case_label: integrated_df}, where integrated_df has 'flow_rate' and 'cum_discharge' columns
        and an index with the derived timestamps for that case.
    value_col : str
        Column name for sample volume (L).
    duration_col : str
        Column name for sampling duration (s).
    title : str, optional
        Plot title.
    output : str, optional
        Path to save the figure instead of showing it.
    """
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Reference datetime so TimedeltaIndex can be plotted with hh:mm:ss
    zero_time = pd.Timestamp("2000-01-01")
    original_times = zero_time + df_original.index               # sampling START
    case_times = {case: zero_time + df.index for case, df in integrated_results.items()}

    # --- Left Y axis: sample volumes as bars starting at sampling start ---
    ax1.bar(
        original_times,
        df_original[value_col],
        width=pd.to_timedelta(df_original[duration_col], unit='s'),  # full duration
        align='edge',                                                # left edge at start time
        color='lightblue',
        label='sample volume (l)'
    )
    ax1.set_xlabel("time from start (hh:mm:ss)")
    ax1.set_ylabel("sample volume (l)", color='black')
    ax1.tick_params(axis='y', labelcolor='black')

    # X axis formatting
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())

    # --- Right Y axis #1: flow rates ---
    ax2 = ax1.twinx()
    flow_colors = {'start': 'orange', 'sample_mid': 'red', 'interval_mid': 'purple'}
    for case, df in integrated_results.items():
        ax2.plot(
            case_times[case],
            df['flow_rate'],
            color=flow_colors.get(case, None),
            marker='o',
            label=f"flow rate ({case})",
            zorder=3
        )
    ax2.set_ylabel("flow rate (l/s)")
    ax2.tick_params(axis='y', labelcolor='black')

    # --- Right Y axis #2: cumulative discharge (same linestyle ":" for all) ---
    ax3 = ax1.twinx()
    ax3.spines["right"].set_position(("outward", 60))
    for case, df in integrated_results.items():
        ax3.plot(
            case_times[case],
            df['cum_discharge'],
            linestyle=':',
            color=flow_colors.get(case, None),
            marker='s',
            label=f"cumulative discharge ({case})",
            zorder=2
        )
    ax3.set_ylabel("cumulative discharge (l)")
    ax3.tick_params(axis='y', labelcolor='black')

    # Scale both right-side axes independently (keeps both readable)
    flow_min = min(d['flow_rate'].min() for d in integrated_results.values())
    flow_max = max(d['flow_rate'].max() for d in integrated_results.values())
    ax2.set_ylim(flow_min * 0.9, flow_max * 1.1)

    cd_min = min(d['cum_discharge'].min() for d in integrated_results.values())
    cd_max = max(d['cum_discharge'].max() for d in integrated_results.values())
    ax3.set_ylim(cd_min * 0.9 if cd_min != 0 else 0, cd_max * 1.1)

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    lines3, labels3 = ax3.get_legend_handles_labels()
    ax1.legend(lines1 + lines2 + lines3,
               labels1 + labels2 + labels3,
               loc='lower right')

    if title:
        ax1.set_title(title)

    plt.tight_layout()
    if output:
        plt.savefig(output, dpi=200)
        plt.close(fig)
    else:
        plt.show()


def plot_frequency_analysis(overview_df, output_path=None):
    """
    draw frequency analysis of the 'sample_mid_diff' and 'sample_end_diff' columns

    parameters
    ----------
    overview_df : pd.DataFrame
        dataframe with 'sample_mid_diff' and 'sample_end_diff' columns
    output_dir : str or None
        if provided, saves the plot as 'overview_diff_frequency.png' in this directory
    """

    plt.figure(figsize=(8, 6))

    # histogram with density (normalized frequencies)
    overview_df["sample_mid_diff"].plot(
        kind="hist", bins=15, alpha=0.5, density=True,
        label="sample_mid_diff"
    )
    overview_df["sample_end_diff"].plot(
        kind="hist", bins=15, alpha=0.5, density=True,
        label="sample_end_diff"
    )

    # overlay kde (smooth density curve)
    overview_df["sample_mid_diff"].plot(
        kind="kde", lw=2, label="sample_mid_diff KDE"
    )
    overview_df["sample_end_diff"].plot(
        kind="kde", lw=2, label="sample_end_diff KDE"
    )

    plt.xlabel("relative difference (1 - case / interval_start)")
    plt.ylabel("frequency (density)")
    plt.title("Frequency analysis of final discharge differences")
    plt.legend()

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300)
        print(f"\tfrequency analysis plot saved to: {output_path}")
    else:
        plt.show()

def format_timedelta_hms(td, **kwargs):
    return str(td).split(' ')[2]

def format_timedelta_s(timedelta):
    return timedelta.total_seconds()

def format_timedelta_min(timedelta):
    total_seconds = timedelta.total_seconds()
    minutes = int((total_seconds % 3600) / 60)

    return f"{minutes:.1f}"

def writeRowToCSV(fileref, towrite, lined="\n", celld=";"):
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

def writeHTMLheader(fileref, html_title=None, lang=None):
    towrite = "<!DOCTYPE html>\n"\
    f"<html xmlns = \"http://www.w3.org/1999/xhtml\" lang = \"{lang or 'en'}\">\n"\
    "<head> <meta http - equiv = \"Content-Type\" content = \"text/html; charset=utf8\" />\n"\
    f"<title> {html_title or ''}</title>\n"\
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

def writeHTMLfooter(fileref):
    towrite = "\n</body>\n<footer>\n</footer>\n</html>"
    fileref.write(towrite)
    return

def format_timedelta(td):
    """Format a timedelta object by removing '0 days'."""
    if isinstance(td, pd.Timedelta):
        td_str = str(td)
        return td_str.replace('0 days ', '') if '0 days' in td_str else td_str
    elif isinstance(td, timedelta):
        return str(td)
    else:
        raise ValueError(f"input value '{td}' is not a TimeDelta instance. ({type(td)})")

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