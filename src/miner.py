# -*- coding: utf-8 -*-
import os
import json
import math
import locale
import re

from collections import defaultdict
from contextlib import contextmanager

from .entities.type_entities import *
from .entities.runoffdb import RunoffDB
from .utilities.plotters import *
from src.filters.run_filter import RunFilter
from .logging.logger import *
from .services.hydro_data import *


lang = "en"

# set locale for proper numbers formating
locale.setlocale(locale.LC_NUMERIC, lang)

# output files line delimiter
lined = "\n"
# output files cell delimiter
celld = ";"



class Miner:
    def __init__(self, filter: RunFilter, logger: RunLogger = None):
        self.runoffdb = RunoffDB()
        self.filter = filter
        self.runs: dict[int, Run] = {}
        self.sequences: dict[int, [Run]] = {}
        self.logger = logger

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

        self.load()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
       return

    def __str__(self):
        return f"miner with filter:\n{self.filter}"

    @contextmanager
    def scoped_runs(self, query: RunFilter):
        """
        Temporarily narrows the Miner run set to those matching query.
        Needed for structured sub-categorization within dataset already filtered by init query
        Restores previous state afterwards.
        """
        previous_runs = self.runs
        try:
            self.runs = {r.id: r for r in previous_runs.values() if query.matches(r)}
            yield
        finally:
            self.runs = previous_runs

    def load(self):
        self.runs = self.runoffdb.load_runs(self.filter)
        self.sequences = self.runs_by_sequence()

        return

    def runs_by_sequence(self) -> dict[int, list[Run]]:
        """
        Transforms runs dictionary to dictionary keyed by sequence {sequence_id: [run, run]
        Runs in sequence are ordered by run type (dry, very wet, wet)
        :return:
        """
        sequences = defaultdict(list)
        for run in self.runs.values():
            sequences[run.sequence_id].append(run)

        for seq_id, run_list in sequences.items():
            # in-place sort of each list
            run_list.sort(key=lambda r: r.run_type_id)
        return sequences

    def get_runs(self, query: RunFilter | None = None):
        """
        Returns runs from the Miner dataset matching the given RunQuery.
        """
        if query is None:
            return list(self.runs.values())

        return [r for r in self.runs.values() if query.matches(r)]

    def fetch_run_by_id(self, run_id: int):
        """
        Fetches a run from the database and returns it.
        Does NOT add it to the Miner run set.
        """
        runs = self.runoffdb.load_runs(RunFilter(run_id=run_id))
        return runs.get(run_id)

    def generate_html_overview(self, output_path, date_from=None, date_to=None, lang="en"):

        self.runoffdb.load_runs(self.filter)

        cumulatives_headers1 = ["lokalita", "simID", "datum", "plot ID", "simulator ID", "plodina", "poc_stav",
                                      "poc_vlhkost", "canopy_cover", "BBCH", "intenzita", "TTR", "odtok_l",
                                      "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g", "odtok_l", "ztrata_pudy_g"]
        cumulatives_headers2 = ["", "", "", "", "", "", "", "", "", "", "", "", "t1=10min", "", "t1=20min", "", "t1=30min"]

        output_html = open(output_path, "w")
        writeHTMLheader(output_html)

        # get dates when some simulation occurred
        all_days = self.runoffdb.get_simulation_days(self.filter.date_from, self.filter.date_to)
        print("\n")

        for day in all_days:
            # load runs of the day
            day_runs = self.get_runs(RunFilter(date_from=day, date_to=day))

            for run in day_runs:
                output_html.write(f"<h2>#{run.id} - {run.datetime.strftime('%d. %m. %Y')} - {run.crop.name[lang]}, {run.run_type.name[lang]} </h2>\n")
                output_html.write("<table>\n")
                writeRowToHTML(output_html, cumulatives_headers2, True)
                writeRowToHTML(output_html, cumulatives_headers1, True)

                output_html.write("</table>")
        output_html.write("</body>\n</html>")
        output_html.close()
        return




    def generate_cumulative_values_csv(self, output_path, lang="en", no_data_value="NA", logfile_path=None, plots_dir=None):
        """

        :param output_path: path of the output file
        :param logfile_path:
        :param plots_dir: directory path for plots
        :return:
        """
        import pandas as pd

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return
        else:
            self.runoffdb.log_file_path = logfile_path

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
            num_runs = len(self.runs)
            for run in list(self.runs.values()):
                # show the counter
                print(f"{i}/{num_runs}")
                i += 1

                run_title = f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name[lang]} - {run.crop.name(lang)} [{run.plot_id}], {self.runoffdb.run_types[run.run_type_id]} {{{run.ttr}}}"
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


                        runoff_label = f"runoff [{self.runoffdb.units.get(runoff_record.unit_id).unit}]"
                        sed_conc_label = f"sed. conc. [{self.runoffdb.units.get(3).unit}]"
                        sed_flux_label = f"sed. flux [{self.runoffdb.units.get(25).unit}]"
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

    def generate_soilpulse_csv(self, output_path, lang="en", no_data_value="NA", logfile_path=None):
        """

        :param output_path: path of the output file
        :param lang: language of the export
        :param no_data_value: directory path for plots
        :param logfile_path:
        :return:
        """

        import pandas as pd

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return

        with self.runoffdb as rdb:
            self.runoffdb.log_file_path = logfile_path

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

            runs = list(self.runs.values())
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
                        line.append(run.get_best_bulk_density_value(27) or no_data_value)

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
        import pandas as pd

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return

        else:
            self.runoffdb.log_file_path = logfile_path

            runs = list(self.runs.values())

            lines = []
            catchThem = []

            for run in runs:
                # run.show_details()
                print(f"\n\nrun ID {run.id} ({czech_date(run.datetime)}, {run.locality.name[lang]})")
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
                run_project_ids = self.runoffdb.get_project_ids(run)
                if run_project_ids:
                    contributors = []
                    for prid in run_project_ids:
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
                line.append(run.plot.locality.lat)

                headers.append("Coordinates  long (deg)")
                notes.append("")
                poznamky.append("")
                line.append(run.plot.locality.lng)

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
                    if self.runoffdb.samples.get(run.texture_ss_id).texture_record_id is not None:
                        tex_rec = self.runoffdb.load_record(run.texture_ss.texture_record_id)
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
                    if self.runoffdb.samples.get(run.corg_ss_id).corg_id is not None:
                        corg_rec = self.runoffdb.load_record(self.runoffdb.samples.get(run.corg_ss_id).corg_id)
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
                    if self.runoffdb.samples.get(run.bulkd_ss_id).bulk_density_id is not None:
                        bd_rec = self.runoffdb.load_record(self.runoffdb.samples.get(run.bulkd_ss_id).bulk_density_id)
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
                    line.append(run.crop.name[lang])

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
                    agrt = run.plot.agrotechnology_id
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
                    line.append(", ".join([pm.name[lang] for pm in run.plot.protection_measures]))
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
                line.append(f"artificial rainfall simulator experiment with '{run.simulator.name[lang]}' simulator setup.")

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
                if run.crop.crop_type_id == 10:
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
                                veg_cover_data = rcrd.load_data("vegetation_cover")
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
                                stone_cover_data = rcrd.load_data("stone_cover")
                                # for a (undesired!) case when the assigned bulk density record consists of multiple values
                                stone_cover = stone_cover_data["stone_cover"].mean()
                line.append(stone_cover)

                headers.append("Rainfall (mm/h)")
                notes.append("")
                poznamky.append("")
                if run.rain_intensity_recid is not None:
                    intensity_rec = self.runoffdb.load_record(run.rain_intensity_recid)
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
                    intensity_rec = self.runoffdb.load_record(run.rain_intensity_recid)
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
                runoff_record = get_best_runoff_record(run=run)
                if runoff_record is None:
                    continue
                # get runoff data in [l.min-1]
                runoff_data = runoff_record.get_data_in_unit(RUNOFF_RATE_LMIN_UNIT_ID, runoff_label)
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
                ss_record = get_best_sediment_concentration_record(run=run)
                # get the sediment concentration data in [g.l-1]
                sediment_data = ss_record.get_data_in_unit(SS_CONCENTRATION_GL_UNIT_ID, sed_conc_label)
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

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return

        self.runoffdb.log_file_path = log_file

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


        for uid, unit in self.runoffdb.units.items():
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

        runs = list(self.runs.values())
        for run in runs:
            print()
            # single row to be filled and written to the output files
            # one line represents one time interval of a measured time series within a run
            line = []

            print(f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
            run_row = [run.id, run.sequence_id, len(run.brothers),
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
            for uid, unit in self.runoffdb.units.items():
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

    def compare_discharge_calculation_methods(self, output_dir=None, lang="en", log_file=None):
        """

        """
        import pandas as pd

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return

        results = []
        with self.runoffdb as rdb:
            self.runoffdb.log_file_path = log_file
            runs = list(self.runs.values())

            print("\n\nSimulations with raw discharge data records\n"+80*"=")

            raw_runs = [run for run in runs if run.get_records(unit_id=5, related_value_x_unit_id=15, record_type_id=1)]

            if len(raw_runs) > 0:
                print("following runs have the raw discharge data stored in db:")

                for run in raw_runs:
                    print(f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
                    run.show_records()
                    # print(f"\trecord #{run.get_best_record_of_unit(unit_id=5, related_value_x_unit_id=15).id}")
                    raw_record = run.get_best_record_of_unit(unit_id=5, related_value_x_unit_id=15)
                    print(f"raw_record.id: {raw_record.id}")
                    raw_data = raw_record.get_data("sample_volume_l", "sampling_duration_s")
                    # print(raw_data)

                    run_results = {"run": run, "raw_data": raw_data}
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

    def calculate_SLR(self, output_dir=None,
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
            "en": ["t2", "rainfall intensity [mm.min-1]", "rainfall total [mm]", "flow rate [l.min-1]", "total discharge [l]",
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
                    print(f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
                    # start the line with the general info
                    line = run.get_info_array(no_data_value=no_data_value, lang=lang)[0]

                    try:
                        crop_hydrodata = run.get_best_hydro_data(request_map=request, labels_map=labels, interpolation_map=interpolations)
                        # if crop_hydrodata is None or crop_hydrodata[sed_yield_label].empty:
                        #     continue
                    except RecordSetNotComplete as e:
                        print(f"\n\033[91mHydro-sediment record set of crop run #{run.id} is not complete\033[00m")
                        print(f"\033[91mmissing record{'s' if len(e.missing_records) > 1 else ''}: {', '.join(e.missing_records)}\033[00m")
                        line.extend(6*no_data_value)
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

                    run.plot_hydro_data(crop_hydrodata, os.path.join(output_dir, plots_filename+".png"), plots, extra_points=points, plot_title=plot_title)

                    crop_hydrodata.to_csv(os.path.join(output_dir, plots_filename + ".csv"),
                                   index=True,
                                   sep=local_seps["celld"][lang],
                                   decimal=local_seps["decd"][lang])
                    # print(f"rainfall total: {crop_hydrodata[rain_tot_label]}")

                    # try to get the reference run
                    frun = run.get_reference_run()

                    if frun is not None:
                        print(f"\t\033[1;33m#{frun.id} - {czech_date(frun.datetime)} - {frun.locality.name} - {frun.crop.name[lang]} - [{frun.plot_id}] {frun.run_type.name[lang]} - {frun.ttr}\033[00m")

                        line2 = frun.get_info_array(no_data_value=no_data_value, lang=lang)[0]

                        try:
                            fallow_hydrodata = frun.get_best_hydro_data(request_map=request, labels_map=labels, interpolation_map=interpolations)
                        except RecordSetNotComplete as e:
                            print(f"\n\n\033[91mHydro-sediment record set of fallow run #{frun.id} is not complete\033[00m")
                            print(f"\033[91mmissing record{'s' if len(e.missing_records) > 1 else ''}: {', '.join(e.missing_records)}\033[00m")
                            # continue
                        else:
                            print("fallow hydrodata fetched")

                        line2.append(format_timedelta(the_time-frun.ttr))

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

                        run.plot_hydro_data(fallow_hydrodata, os.path.join(output_dir, plots_filename+".png"), plots, extra_points=points, plot_title=plot_title)

                        fallow_hydrodata.to_csv(os.path.join(output_dir, plots_filename + ".csv"),
                                              index=True,
                                              sep=local_seps["celld"][lang],
                                              decimal=local_seps["decd"][lang])


                        line.append(sedyield/sedyield2)

                        # at the last run in sequence save the 'combined SLR value'
                        if r == len(run_list):
                            line.append((dry_crop_sedyield + verywet_crop_sedyield) / (dry_fallow_sedyield + verywet_fallow_sedyield))

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


    def find_fallow(self):

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return

        runs = list(self.runs.values())

        no_fallow = []
        one_fallow_recorded = []
        one_fallow_found = []
        more_fallows = []

        updates = []
        for run in runs:
            # only for non-fallow runs ...
            if run.crop_id != 1:
                print(f"\n\033[0;32m#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - [{run.plot_id}] {run.plot.plot_length:.0f}x{run.plot.plot_width:.0f}m - {run.run_type.name[lang]} - {run.ttr}\033[32m")

                # try to get the reference run
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
                        print(f"\t\033[38;5;208mfallows with matching properties: {', '.join([str(f.id) for f in fallows])}\033[0m")
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

    def repair_record_relations(self):

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return

        runs = list(self.runs.values())

        updates = []
        for run in runs:
            print(f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")

            # try getting the sediment flux record
            sedflux_record = run.get_best_record_of_unit(23)
            if sedflux_record:
                print(f"sediment flux record id: {sedflux_record.id}")
                if sedflux_record.source_ids:
                    print(f"\tsource records:")
                    for sr in sedflux_record.source_ids:
                        print(f"\t\t{sr}")
                        # try getting the raw runoff record
                        runoff_volume_record = run.get_best_record_of_unit(5)
                        if runoff_volume_record:
                            print(f"\t\t\t{runoff_volume_record.id}")
                        else:
                            print(f"\t\t\tno raw runoff record")
                        # print(f"\t{sr.id} - {sr.unit.name_cz}")
            else:
                print(f"no sediment flux record found")

    def plots_overview(self):
        print(f"\n\nPLOTS OVERVIEW ============================")
        self.runoffdb.show_plots()
        return

    def simulators_overview(self, lang="en"):
        print(f"\n\nSIMULATORS OVERVIEW ============================")
        self.runoffdb.show_simulators(lang=lang)
        return

    def methodics_overview(self, lang="en"):
        print(f"\n\nMETHODICS OVERVIEW ============================")
        self.runoffdb.show_methodics(lang=lang)

    def localities_overview(self):
        print(f"\n\nLOCALITIES OVERVIEW ============================")
        self.runoffdb.show_localities()

    def agrotechnologies_overview(self):
        print(f"\n\nAGROTECHNOLOGIES OVERVIEW ============================")
        self.runoffdb.show_agrotechnologies()

    def export_methodics(self, output_path, lang="en"):
        export = self.runoffdb.export_methodics(lang)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export, f, ensure_ascii=False, indent=4)

        print(f"All methodics successfully exported to '{output_path}'")
        return

    def show_run_notes(self, lang="en"):
        print(f"\n\nRUN NOTES OVERVIEW ============================")

        if not self.runs:
            print("\n\033[91mNo runs available within given limits.\033[00m\n")
            return

        for run in list(self.runs.values()):
            print(
                f"\n#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name[lang]} - {run.plot_id} - {run.run_type.name[lang]} - {run.ttr}")
            print(run.get_notes(lang))

        return




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