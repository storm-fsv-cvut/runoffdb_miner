import pandas as pd
import mysql.connector
import numpy as np
from pint import UnitRegistry
# import pint_pandas

from src.db_access import DBconnector

# multipliers for different units to convert between each other
multipliers = {1: {1: 1}, 2: {2: 1, 3: 0.001}, 3: {2: 1000, 3: 1}, 18: {27: 0.001}, 27: {18: 1000}}


def remove_last_zero_row(df):
    """
    Removes last row of a dataframe if value is equal to 0
    :param df: pandas Dataframe to be processes
    :return: copy of a Dataframe with removed last row or the original Dataframe if last row's value != 0
    """
    # Check if the last row value is equal to 0
    if df.iloc[-1]['value'] == 0:
        # Remove the last row
        df = df.drop(df.index[-1])
def czech_date(datetime):
    return f"{datetime.strftime('%d.').strip('0')} {datetime.strftime('%m.').strip('0')} {datetime.strftime('%Y')}"

class RunoffDB:
    agrotechnologies_table = "`agrotechnology`"
    assignmenttypes_table = "`assignment_type`"
    crops_table = "`crop`"
    crop_types_table = "`crop_type`"
    data_table = "`data`"
    localities_table = "`locality`"
    measurements_table = "`measurement`"
    measurement_run_table = "`measurement_run`"
    measurement_soil_sample_table = "`measurement_soil_sample`"
    models_table = "`model`"
    operations_table = "`operation`"
    operation_intensities_table = "`operation_intensity`"
    operation_types_table = "`operation_type`"
    organizations_table = "`organization`"
    phenomena_table = "`phenomenon`"
    plots_table = "`plot`"
    projects_table = "`project`"
    projectlinks_table = "`sequence_project`"
    protection_measures_table = "`protection_measure`"
    records_table = "`record`"
    record_types_table = "`record_type`"
    record_record_table = "`record_record`"
    runs_table = "`run`"
    run_groups_table = "`run_group`"
    run_types_table = "`run_type`"
    sequences_table = "`sequence`"
    simulators_table = "`simulator`"
    soil_samples_table = "`soil_sample`"
    surfacecond_table = "`surface_condition`"
    tillageseq_table = "`tillage_sequence`"
    units_table = "`unit`"
    vegetationcond_table = "`vegetation_condition`"

    # record type priorities
    default_view_order = [2, 3, 4, 1, 6, 7, 5]
    # 1 - raw data
    # 2 - edited data
    # 3 - homogenized edited data
    # 4 - homogenized raw data
    # 5 - set value
    # 6 - derived data
    # 7 - estimated from similar conditions
    # 8 - rough estimate

    def __init__(self, output_na_value = None):
        self.dbcon = DBconnector().pool.get_connection()
        self.na_value = output_na_value

        # on initiation load all the entities that are used all the time
        self.simulators = self.load_simulators()
        self.localities = self.load_localities()
        self.run_types = self.load_run_types()
        self.units = self.load_units()
        self.crop_types = self.load_crop_types()
        self.plots = self.load_plots()
        self.samples = self.load_samples()
        self.crops = self.load_crops()
        self.agrotechnologies = self.load_agrotechnologies()
        self.protection_measures = self.load_protection_measures()
        self.projects = self.load_projects()

        # do not load the runs as they might be limited by filters
        self.runs = None



    def load_runs(self, limit = None, date_from = None, date_to = None, simulators = None, localities = None, crops = None):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # start of the query
            query = f"SELECT {self.runs_table}.`id` AS run_id, " \
                    f"{self.runs_table}.`runoff_start` AS ttr, " \
                    f"{self.runs_table}.`init_moisture_id` AS initmoist_recid, " \
                    f"{self.runs_table}.`surface_cover_id` AS surface_cover_recid, " \
                    f"{self.runs_table}.`rain_intensity_id` AS rainfall_recid, " \
                    f"{self.runs_table}.`soil_sample_bulk_id` AS bulkd_ss_id, " \
                    f"{self.runs_table}.`soil_sample_texture_id` AS texture_ss_id, " \
                    f"{self.runs_table}.`soil_sample_corg_id` AS corg_ss_id, " \
                    f"{self.runs_table}.`crop_bbch` AS bbch, " \
                    f"{self.run_groups_table}.`sequence_id` AS sequence_id, " \
                    f"{self.run_groups_table}.`datetime` AS datetime, " \
                    f"{self.sequences_table}.`simulator_id` AS simulator_id, " \
                    f"{self.runs_table}.`run_group_id` AS run_group_id, " \
                    f"{self.run_groups_table}.`run_type_id` AS run_type_id, " \
                    f"{self.plots_table}.`locality_id` AS locality_id, " \
                    f"{self.plots_table}.`id` AS plot_id, " \
                    f"{self.plots_table}.`crop_id` AS crop_id, " \
                    f"{self.crops_table}.`crop_type_id` AS crop_type_id " \
                    f"FROM {self.runs_table} " \
                    f"JOIN {self.run_groups_table} ON {self.runs_table}.`run_group_id` = {self.run_groups_table}.`id` " \
                    f"JOIN {self.sequences_table} ON {self.run_groups_table}.`sequence_id` = {self.sequences_table}.`id` " \
                    f"JOIN {self.plots_table} ON {self.runs_table}.`plot_id` = {self.plots_table}.`id` " \
                    f"JOIN {self.crops_table} ON {self.plots_table}.`crop_id` = {self.crops_table}.`id` " \
                    f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) "
            if date_from is not None:
                query += f" AND {self.run_groups_table}.`datetime` > '{date_from}'"
            if date_to is not None:
                query += f" AND {self.run_groups_table}.`datetime` < '{date_to}'"

            # additional conditions
            # query += f"AND `` = "

            # end of the query
            query += " ORDER BY `datetime` ASC"

            if limit:
                query += f" LIMIT {limit}"
            # execute the query and fetch the results
            thecursor.execute(query)

            results = thecursor.fetchall()

            run_dict = {}
            if thecursor.rowcount > 0:
                for r in results:
                    new = Run(self, **r)
                    new.plot = self.plots.get(new.plot_id)
                    # new_run.show_details()
                    run_dict.update({new.id: new})
                thecursor.close()
                self.runs = run_dict
                return run_dict
            return None

    def load_plots(self, id = None):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # start of the query
            query = f"SELECT * FROM {self.plots_table}"
            if id is not None:
                query += f" WHERE `id` = {id}"

            query += " ORDER BY `id` ASC"

            # execute the query and fetch the results
            thecursor.execute(query)
            results = thecursor.fetchall()

            plot_dict = {}
            if thecursor.rowcount > 0:
                for r in results:
                    new = Plot(self, **r)
                    plot_dict.update({new.id: new})
                thecursor.close()
                print(f"{len(plot_dict)} plots successfully loaded")
                return plot_dict
            return None

    def load_samples(self, id = None):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            query = f"SELECT * FROM {self.soil_samples_table}"
            if id is not None:
                query += f" WHERE `id` = {id}"
            query += " ORDER BY `id` ASC"

            thecursor.execute(query)
            results = thecursor.fetchall()

            samples_dict = {}
            if thecursor.rowcount > 0:
                for r in results:
                    new = SoilSample(self, **r)
                    samples_dict.update({new.id: new})
                thecursor.close()
                print(f"{len(samples_dict)} soil samples successfully loaded")

                return samples_dict

            return None

    def load_simulators(self, id = None):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            query = f"SELECT * FROM {self.simulators_table}"
            if id is not None:
                query += f" WHERE `id` = {id}"
            query += " ORDER BY `id` ASC"

            thecursor.execute(query)
            results = thecursor.fetchall()
            if thecursor.rowcount > 0:
                simulators = {}
                for r in results:
                    new = Simulator(self, **r)
                    simulators.update({new.id: new})
                print(f"{len(simulators)} simulators successfully loaded")
            thecursor.close()
        return simulators

    def load_localities(self, id = None):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            query = f"SELECT * FROM {self.localities_table}"
            if id is not None:
                query += f" WHERE `id` = {id}"
            query += " ORDER BY `id` ASC"
            thecursor.execute(query)
            results = thecursor.fetchall()
            if thecursor.rowcount > 0:
                localities = {}
                for r in results:
                    new = Locality(self, **r)
                    localities.update({new.id: new})
            print(f"{len(localities)} localities successfully loaded")
            thecursor.close()
        return localities

    def load_run_types(self):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            query = f"SELECT * FROM {self.record_types_table}"

            thecursor.execute(query)
            results = thecursor.fetchall()

            if thecursor.rowcount > 0:
                run_types = {}
                for r in results:
                    new = RunType(**r)
                    run_types.update({new.id: new})
                print(f"run types successfully loaded")
            thecursor.close()
        return run_types


    def load_crop_types(self):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            query = f"SELECT * FROM {self.crop_types_table}"

            thecursor.execute(query)
            results = thecursor.fetchall()
            crop_types = {}
            if thecursor.rowcount > 0:
                for r in results:
                    new = CropType(**r)
                    crop_types.update({new.id: new})
            print(f"crop types successfully loaded")
            thecursor.close()
        return crop_types

    def load_protection_measures(self):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            query = f"SELECT * FROM {self.protection_measures_table}"

            thecursor.execute(query)
            results = thecursor.fetchall()
            out_dict = {}
            if thecursor.rowcount > 0:
                for r in results:
                    new = ProtectionMeasure(**r)
                    out_dict.update({new.id: new})
            print(f"{len(out_dict)} soil protection measures successfully loaded")
            thecursor.close()
        return out_dict

    def load_units(self):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # execute the query and fetch the results
            thecursor.execute(f"SELECT * FROM {self.units_table}")
            results = thecursor.fetchall()
            if thecursor.rowcount > 0:
                units = {}
                for r in results:
                    new_unit = Unit(**r)
                    units.update({new_unit.id: new_unit})
            print(f"{len(units)} units successfully loaded")
            thecursor.close()
        return units

    def load_projects(self):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # execute the query and fetch the results
            thecursor.execute(f"SELECT * FROM {self.projects_table}")
            results = thecursor.fetchall()
            if thecursor.rowcount > 0:
                projects = {}
                for r in results:
                    new_project = Project(**r)
                    projects.update({new_project.id: new_project})
            print(f"projects successfully loaded")
            thecursor.close()
        return projects

    def load_crops(self):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # execute the query and fetch the results
            thecursor.execute(f"SELECT * FROM {self.crops_table}")
            results = thecursor.fetchall()
            if thecursor.rowcount > 0:
                crops = {}
                for r in results:
                    new_crop = Crop(**r)
                    crops.update({new_crop.id: new_crop})
            print(f"{len(crops)} crops successfully loaded")
            thecursor.close()
        return crops

    def load_agrotechnologies(self):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # execute the query and fetch the results
            thecursor.execute(f"SELECT * FROM {self.agrotechnologies_table}")
            results = thecursor.fetchall()
            if thecursor.rowcount > 0:
                agrotechnologies = {}
                for r in results:
                    new_agt = Agrotechnology(self, **r)
                    agrotechnologies.update({new_agt.id: new_agt})
            print(f"{len(agrotechnologies)} agrotechnologies successfully loaded")
            thecursor.close()
        return agrotechnologies

    def load_record(self, record_id):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # start of the query
            query = f"SELECT * FROM {self.records_table} WHERE `id` = {record_id}"
            # execute the query and fetch the results
            thecursor.execute(query)

            results = thecursor.fetchone()
            thecursor.close()
            if len(results) > 0:
                return Record(self, **results)

        return None

    def get_simulation_days(self, dateFrom = None, dateTo = None):
        # use instances date limits if not specified
        dateFrom = dateFrom if dateFrom is not None else self.date_from
        dateTo = dateTo if dateTo is not None else self.date_from

        with self.dbcon.cursor(dictionary=True) as thecursor:
            # start of the query
            query = f"SELECT DISTINCT {self.run_groups_table}.`datetime` AS datetime, " \
                    f"{self.run_groups_table}.`sequence_id` AS sequence_id, " \
                    f"{self.runs_table}.`id` AS run_id " \
                    f"FROM {self.runs_table} " \
                    f"JOIN {self.run_groups_table} ON {self.runs_table}.`run_group_id` = {self.run_groups_table}.`id` " \
                    f"JOIN {self.sequences_table} ON {self.run_groups_table}.`sequence_id` = {self.sequences_table}.`id` " \
                    f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) "
            if dateFrom is not None:
                query += f" AND {self.run_groups_table}.`datetime` > '{dateFrom}'"
            if dateTo is not None:
                query += f" AND {self.run_groups_table}.`datetime` < '{dateTo}'"

            # additional conditions
            # query += f"AND `` = "

            # end of the query
            query += " ORDER BY `datetime` ASC"

            # execute the query and fetch the results
            thecursor.execute(query)

            results = thecursor.fetchall()
            thecursor.close()
            sim_days = []
            if len(results) > 0:
                for r in results:
                    sim_days.append(r['datetime'])

                return sim_days
            return None

    def show_agrotechnologies(self):
        for id, agt in self.agrotechnologies.items():
            print(f"\n#{agt.id} - {agt.name_en}")
            for date, operation in agt.operation_sequence.items():
                print(f"\t{date}: {operation.name_en}")

    def show_localities(self):
        print("\n")
        for loc in self.localities.values():
            print(str(loc))
        return

    def load_sequence_ids_by_date(self, datetime):
        with self.dbcon.cursor(dictionary=True) as thecursor:
            # start of the query
            query = f"SELECT {self.runs_table}.`id` AS run_id, " \
                    f"{self.run_groups_table}.`id` AS group_id, " \
                    f"{self.run_groups_table}.`sequence_id` AS sequence_id " \
                    f"FROM {self.runs_table} " \
                    f"JOIN {self.run_groups_table} ON {self.runs_table}.`run_group_id` = {self.run_groups_table}.`id` " \
                    f"JOIN {self.sequences_table} ON {self.run_groups_table}.`sequence_id` = {self.sequences_table}.`id` " \
                    f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) " \
                    f"AND {self.run_groups_table}.`datetime` = '{datetime}'"

            # execute the query and fetch the results
            thecursor.execute(query)
            results = thecursor.fetchall()
            thecursor.close()

            if len(results) > 0:
                return results
            return None

class Run:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["run_id"]
        self.sequence_id = kwargs["sequence_id"]
        self.run_group_id = kwargs["run_group_id"]
        self.datetime = kwargs["datetime"]
        self.simulator_id = kwargs["simulator_id"]
        self.run_type_id = kwargs["run_type_id"]
        self.ttr = kwargs["ttr"]
        self.measurements = None
        self.brothers = None
        self.plot_id = kwargs["plot_id"]
        self.plot = None
        self.locality_id = kwargs["locality_id"]
        self.crop_id = kwargs["crop_id"]
        self.crop_type_id = kwargs["crop_type_id"]
        self.crop_name = None
        self.initmoist_recid = kwargs["initmoist_recid"]
        self.surface_cover_recid = kwargs["surface_cover_recid"]
        self.bbch = kwargs["bbch"]
        self.rain_intensity_recid = kwargs["rainfall_recid"]

        self.bulkd_ss_id = kwargs["bulkd_ss_id"]
        self.texture_ss_id = kwargs["texture_ss_id"]
        self.corg_ss_id = kwargs["corg_ss_id"]

        self.note = None


    def show_details(self, indent = "", measurement_details=False):
        print(indent + "\n" + 40 * "-")
        print(indent+f"run_id: {self.id}")
        print(indent+40*"-")
        indent += "\t"
        print(indent+f"sequence #{self.sequence_id}")
        print(indent+f"date: {self.datetime.strftime('%d. %m. %Y')}")
        print(indent+f"start time: {self.datetime.strftime('%H:%M')}")
        print(indent+f"time to runoff: {self.ttr}")
        print(indent+f"simulator #{self.simulator_id}")
        print(indent+f"plot #{self.plot_id}")
        print(indent+f"locality #{self.locality_id}")
        print(indent+f"crop #{self.crop_id}")
        print(indent + f"crop type #{self.crop_type_id}")
        if self.brothers:
            print(indent+f"run group brothers: {self.brothers}")
        if measurement_details:
            print(indent + f"measurements:")
            if self.measurements:
                for meas in self.measurements:
                    meas.show_details(indent)

        print("\n"+ indent + f"soil texture sample: {self.texture_ss_id}")
        print(indent + f"soil bulk density sample: {self.bulkd_ss_id}")
        print(indent + f"soil Corg sample: {self.corg_ss_id}")

        print("\n" + indent + f"rain intensity record: {self.rain_intensity_recid}")
        print(indent + f"initial moisture record: {self.initmoist_recid}")
        print(indent + f"surface cover record: {self.surface_cover_recid}")
        print("\n")

    def load_group_brothers(self):
        with self.runoffdb.dbcon.cursor() as thecursor:
            # execute the query and fetch the results
            query = f"SELECT `run`.`id` FROM {RunoffDB.runs_table} WHERE `run_group_id` = {self.rungroup_id}"

            thecursor.execute(query)
            results = thecursor.fetchall()
            thecursor.close()

            if len(results) > 0:
                brothers = []
                for r in results:
                    if r[0] != self.id:
                        brothers.append(r[0])
                return brothers
            return None

    # def load_measurements(self, phenomenon_id = None):
    #     dbcon = self.dbc.connect()
    #     if dbcon:
    #         thecursor = dbcon.cursor()
    #         # compose the query
    #         query = "SELECT `measurement`.`id`, `phenomenon`.`name_cz` FROM `measurement` "\
    #             "JOIN `phenomenon` ON `phenomenon`.`id` = `measurement`.`phenomenon_id`"\
    #             "JOIN `measurement_run` ON `measurement_run`.`measurement_id` = `measurement`.`id`"\
    #             f" WHERE `run_id` = {self.id}"
    #         if phenomenon_id:
    #             query += f" AND `phenomenon_id` = {phenomenon_id}"
    #         # execute the query and fetch the results
    #         thecursor.execute(query)
    #         results = thecursor.fetchall()
    #
    #         measurements = {}
    #         if thecursor.rowcount > 0:
    #             for m in results:
    #                 measurements.update({m[0]:m[1]})
    #             dbcon.close()
    #             return measurements
    #         dbcon.close()
    #     return None


    def get_initial_moisture_value(self):
        if self.initmoist_recid:
            initmoist_rec = self.get_records(self.initmoist_recid)
            initmoist_rec.load_data("initial_moisture")
            initmoist_data = initmoist_rec.get_data("initial_moisture")

            if len(initmoist_data.index) > 1:
                print(f"Initial moisture record {initmoist_rec.id} of run {self.id} has more then one data points, mean value is returned.")
            return initmoist_data.mean()

        else:
            print(f"run {self.id} doesn't have initial moisture record ID assigned.")
            return None

    def get_surface_cover_value(self, multi_value=False):
        if self.surface_cover_recid:
            surface_cover_rec = self.runoffdb.load_record(self.surface_cover_recid)
            surface_cover_rec.load_data("surface_cover")
            surface_cover_data = surface_cover_rec.get_data("surface_cover")

            if len(surface_cover_data.index) == 1:
                return surface_cover_data["surface_cover"].mean()
            else:
                if multi_value:
                    return surface_cover_data["surface_cover"].toList()
                else:
                    print(f"Surface cover record {surface_cover_rec.id} of run {self.id} has more then one value!")
                    print(f"Mean value of all {len(surface_cover_data.index)} data points was returned.")
                    return surface_cover_data["surface_cover"].mean()
        else:
            print(f"Run {self.id} doesn't have surface cover record ID assigned.")
            return self.runoffdb.na_value

    def get_rainfall_intensity_timeline(self):
        if self.rain_intensity_recid:
            intensity_rec = self.load_record(self.rain_intensity_recid)
            intensity_rec.load_data("rain_intensity")
            intensity_data = intensity_rec.get_data("rain_intensity")
            # regular intensity series has exactly 2 rows, any other number is some exception or non-standard rainfall
            if len(intensity_data.index) == 1:
                print(f"Rainfall intensity record {intensity_rec.id} of run {self.id} contains only one data point. Proper rainfall intensity must have at least two data points.")
                return None
            elif len(intensity_data.index) == 2:
                if intensity_data["rain_intensity"].iloc[-1] != 0:
                    print(f"Rainfall intensity record {intensity_rec.id} of run {self.id} doesn't end with zero value!")
                    return None
                return intensity_data["rain_intensity"].iloc[0]
            else:
                # interrupted or variable intensity rainfall
                numzeros = 0
                for datapoint in intensity_data:
                    if datapoint[0] == 0:
                        numzeros += 1
                if numzeros > 1:
                    return "interrupted"
                else:
                    return "variable"
        else:
            print(f"run {self.id} doesn't have rainfall intensity record ID assigned.")
            return None

    def get_total_rainfall(self):
        raise NotImplementedError("Run object method 'get_total_rainfall()' is not implemented yet")

    def get_best_runoff_value_l(self):
        raise NotImplementedError("Run object method 'get_best_runoff_value_l()' is not implemented yet")

    def get_best_runoff_value_mm(self):
        raise NotImplementedError("Run object method 'get_best_runoff_value_mm()' is not implemented yet")

    def get_best_sediment_yield_value_g(self):
        raise NotImplementedError("Run object method 'get_best_sediment_yield_value_g()' is not implemented yet")

    def get_best_sediment_yield_value_tha(self):
        raise NotImplementedError("Run object method 'get_best_sediment_yield_value_tha()' is not implemented yet")

    def get_records(self, phenomenon_id = None, unit_id = None, record_type_id = None):
        out = []
        # get the measurements related to Run instance, pass on the argument
        measurements = self.get_measurements(phenomenon_id)
        # if any measurements like that exist
        if measurements:
            for meas in measurements:
                # load the records of measurement, pass on the arguments
                # records of all unit IDs are in the obtained list if unit_is a list
                recs = meas.get_records(unit_id, record_type_id)
                # if any records like that exist
                if recs:
                    out.extend(recs)
                else:
                    return None
            else:
                return out
        else:
            return None


    def load_measurements(self):
        msrmsnts = None
        with self.runoffdb.dbcon.cursor(dictionary=True) as thecursor:
            query = f"SELECT * FROM {RunoffDB.measurements_table} " \
                    f"JOIN {RunoffDB.measurement_run_table} ON {RunoffDB.measurement_run_table}.`measurement_id` = {RunoffDB.measurements_table}.`id` " \
                    f"WHERE {RunoffDB.measurement_run_table}.`run_id` = {self.id}"
            thecursor.execute(query)
            results = thecursor.fetchall()
            thecursor.close()

            if len(results) == 0:
                # print(f"\tNo measurement found for run {self.id}")
                return {}
            else:
                msrmsnts = {}
                for res in results:
                    new_measurement = Measurement(self.runoffdb, **res)
                    msrmsnts.update({new_measurement.id: new_measurement})
        return msrmsnts

    def get_measurements(self, phenomenon_id = None):
        if self.measurements:
            out = []
            for meas in self.measurements.values():
                if phenomenon_id is None:
                    out.append(meas)
                # if the phenomenon id is limited by the argument
                else:
                    if meas.phenomenon_id == phenomenon_id:
                        out.append(meas)
            if len(out) == 0:
                return None
            else:
                return out
        else:
            # the measurements are not loaded yet, try loading them
            self.load_measurements()
            if self.measurements:
                return self.get_measurements(phenomenon_id)
            else:
                return None

    def get_project_ids(self):
        ids = None
        with self.runoffdb.dbcon.cursor() as thecursor:
            # execute the query and fetch the results
            query = f"SELECT `project_id` FROM `sequence_project` WHERE `sequence_id` = {self.sequence_id}"

            thecursor.execute(query)
            results = thecursor.fetchall()
            thecursor.close()

            if len(results)> 0:
                ids = []
                for r in results:
                    ids.append(r[0])
        return ids

    def get_terminal_velocity_value(self, time=None, record_type=None):
        found_records = self.get_records(5, [15], record_type_id=record_type)
        if found_records:
            if len(found_records) > 0:
                # check if found records have same type -
                first = found_records[0].record_type_id
                for rec in found_records:
                    if rec.record_type_id != first:
                        print("Records of more types were found. Specify record type for unambiguous results.")

                for rec in found_records:
                    data = rec.load_data("plot_x", indexColumn="plot_x")

                    print(data[data['plot_x'] == data['plot_x'].max()])

                    print(data)
        else:
            return None
        return

class Measurement:
    def __init__(self, dbcon, **kwargs):
        self.dbcon = dbcon

        self.id = kwargs.get("id")
        self.phenomenon_id = kwargs.get("phenomenon_id")
        self.plot_id = kwargs.get("plot_id")
        self.locality_id = kwargs.get("locality_id")
        self.date = kwargs.get("date")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.note_cz = kwargs.get("note_cz")
        self.note_en = kwargs.get("note_en")
        self.user_id = kwargs.get("user_id")

        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.note = {"cz": self.note_cz, "en": self.note_en}
        self.records = None

    def show_details(self, indent = "", t = "- "):
        indent += t
        print(indent+f"measurement_id: {self.id}")
        indent += t
        print(indent+f"phenomenon_id: {self.phenomenon_id}")
        if self.date:
            print(indent+f"date: {czech_date(self.date)}")
        else:
            print(indent+f"date: not set")
        if self.plot_id:
            print(indent+f"plot_id: {self.plot_id}")
        else:
            print(indent+f"plot_id: not set")
        if self.locality_id:
            print(indent+f"locality_id: {self.locality_id}")
        else:
            print(indent+f"locality_id: not set")
        print(indent+f"user_id: {self.user_id}")

        if self.records:
            print(indent+f"records:")
            for rec in self.records:
                rec.show_details(indent)


    def load_records(self):
        with self.dbc.connect() as dbcon:
            if dbcon:
                thecursor = dbcon.cursor(dictionary=True)

                query = f"SELECT * FROM {RunoffDB.records_table} WHERE `measurement_id` = {self.id}"
                # print(query)
                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount == 0:
                    # print(f"\tNo record found for measurement {self.id}")
                    return []
                else:
                    rcrds = []
                    for r in results:
                        new = Record(**r)
                        rcrds.append(new)
                    self.records = rcrds
                    return rcrds
            else:
                return None

    def get_records(self, unit_id = None, record_type_id = None):
        if self.records:
            out = []
            for rec in self.records:
                if not unit_id:
                    out.append(rec)
                else:
                    if isinstance(unit_id, list):
                        for uid in unit_id:
                            if rec.unit_id == uid:
                                if not record_type_id:
                                    out.append(rec)
                                else:
                                    if rec.record_type_id == record_type_id:
                                        out.append(rec)
                    else:
                        if rec.unit_id == unit_id:
                            if not record_type_id:
                                out.append(rec)
                            else:
                                if rec.record_type_id == record_type_id:
                                    out.append(rec)

            if len(out) == 0:
                # add = "" if not record_type_id  else f" and record type id {record_type_id}"
                # print(f"measurement {self.id} has no record of unit id {unit_id}{add}.")
                return None
            else:
                return out
        else:
            # the records were not loaded yet, try loading them
            self.load_records()
            if self.records:
                self.get_records(unit_id, record_type_id)
            else:
                return None

class Record:

    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.measurement_id = kwargs.get("measurement_id")
        self.record_type_id = kwargs.get("record_type_id")
        self.unit_id = kwargs.get("unit_id")
        self.note_cz = kwargs.get("note_cz")
        self.note_en = kwargs.get("note_en")
        self.related_value_x_unit_id = kwargs.get("related_value_xunit_id")
        self.related_value_y_unit_id = kwargs.get("related_value_yunit_id")
        self.related_value_z_unit_id = kwargs.get("related_value_zunit_id")
        self.quality_index_id = kwargs.get("quality_index_id")
        self.is_timeline = kwargs.get("is_timeline")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.note = {"cz": self.note_cz, "en": self.note_en}

        self.unit = self.load_unit()

        self.data = None


        if multipliers.get(self.unit_id):
            self.multiplier_to_SI = multipliers.get(self.unit_id)
        else:
            self.multiplier_to_SI = None

    def show_details(self, indent="", t=". "):
        indent += t
        print(indent+f"record_id: {self.id}")
        indent += t
        print(indent+f"record_type_id: {self.record_type_id}")
        print(indent+f"unit_id: {self.unit_id}")
        if self.related_value_x_unit_id:
            print(indent+f"related_value_x_unit_id: {self.related_value_x_unit_id}")
        if self.related_value_y_unit_id:
            print(indent+f"related_value_y_unit_id: {self.related_value_y_unit_id}")
        if self.related_value_z_unit_id:
            print(indent+f"related_value_z_unit_id: {self.related_value_z_unit_id}")
        if self.quality_index_id:
            print(indent+f"quality_index_id: {self.quality_index_id}")
        print(indent+f"is_timeline: {self.is_timeline}")

        return

    def load_data(self, value_name, related_x = None, related_y = None, related_z = None, index_column = None, order_by=None):
        more = ""

        if related_x is not None:
            if not self.related_value_x_unit_id:
                print(f"Related value X is not defined for record id {self.id}")
            else:
                more += ", `related_value_x`"
                if related_x != True:
                    more += f" AS {related_x} "
        if related_y is not None:
            if not self.related_value_y_unit_id:
                print(f"Related value Y is not defined for record id {self.id}")
            else:
                more += ", `related_value_y`"
                if related_y != True:
                    more += f" AS {related_y} "
        if related_z is not None:
            if not self.related_value_z_unit_id:
                print(f"Related value Z is not defined for record id {self.id}")
            else:
                more += ", `related_value_z`"
                if related_z != True:
                    more += f" AS {related_z} "

        if order_by is not None:
            order_by = f" ORDER BY `{order_by}` ASC"
        elif self.is_timeline:
            order_by = f" ORDER BY `time` ASC"
        else:
            order_by = ""

        query = f"SELECT `time`, `value` AS {value_name} {more} FROM {RunoffDB.data_table} WHERE `record_id` = {self.id}{order_by}"
        result_dataFrame = pd.read_sql(query, self.runoffdb.dbcon)

        if self.is_timeline and index_column is None:
            result_dataFrame.set_index('time', inplace=True)
        elif index_column is not None:
            result_dataFrame.set_index(index_column, inplace=True)

        self.data = result_dataFrame
        return result_dataFrame


    def get_data(self, value_name, index_column = None, remove_last_zero=False):
        """
        Return self.data if there were any data loaded.
        Tries loading the data if self.data is None.
        :param remove_last_zero: Whether to remove last row if the value in the row is equal to 0
        :return: pandas Dataframe object with data loaded from DB or None if the self.data is empty
        """
        if self.data is not None:
            if remove_last_zero:
                return remove_last_zero_row(self.data.copy())
            else:
                return self.data
        else:
            # the data were not loaded yet
            self.load_data(value_name=value_name, index_column=index_column)
            if self.data is not None:
                if self.data.empty:
                    return None
                else:
                    if remove_last_zero:
                        return remove_last_zero_row(self.data.copy())
                    else:
                        return self.data
            else:
                return None

    def get_data_in_unit(self, target_unit_id, value_name, remove_last_zero=False, output_column_label=None):
        """
        Return self.data with 'value' multiplied by the unit's multiplier to SI (if there were any data loaded).
        Tries loading the data if self.data is None.

        :return: pandas Dataframe object with data loaded from DB or None if the self.data is empty, values in SI units
        """
        if self.get_data(value_name) is not None:
            # if not self.data.empty:
            if target_unit_id == self.unit_id:
                return self.data
            output_column_label = output_column_label if output_column_label is not None else value_name

            multiply_by = multipliers.get(self.unit_id).get(target_unit_id)
            if not multiply_by:
                print(f"Unit id {self.unit_id} doesn't have multiplier defined for conversion to unit id {target_unit_id}!")
                return None
            elif multiply_by != 1:
                if remove_last_zero:
                    data_out = remove_last_zero_row(self.data)
                else:
                    data_out = self.data.copy()
                data_out[output_column_label] = data_out[value_name] * multiply_by
                return data_out
            else:
                return self.data
        else:
            return None

    # def get_value_in_time(self, timedelta, zero_time=False, extrapolate=False, timekey = 'time', valuekey = 'value'):
    #     """
    #     Returns interpolated value of dataseries in time specified as timedelta
    #
    #     :param timedelta:
    #     :param zero_time: presumed time of start of the series (value = 0)
    #     :param extrapolate: range of extrapolation specified as multipliction of last complete interval length
    #     :returns: interpolated/extrapolated value if available based on specified inputs otherwise False
    #     """
    #     return get_value_in_time(self.get_data(), timedelta, zero_time, extrapolate, timekey, valuekey)

    def load_unit(self):
        if self.unit_id:
            with self.runoffdb.dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {RunoffDB.units_table} WHERE `id` = {self.unit_id}"
                thecursor.execute(query)
                results = thecursor.fetchall()
                thecursor.close

                if len(results) == 1:
                    unit = Unit(**results[0])
                    return unit
                elif len(results) == 0:
                    print(f"No unit with ID {self.unit_id} found.")
                    return None
                else:
                    print(f"More then one unit found for ID {self.unit_id} ... this really shouldn't happen.")
                    return None
        else:
            print(f"Record {self.id} doesn't have unit ID assigned.")
            return None



class Locality:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.organization_id = kwargs.get("organization_id")
        self.name = kwargs.get("name")
        self.lat = kwargs.get("lat")
        self.lng = kwargs.get("lng")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.wrb_soil_class_id = kwargs.get("wrb_soil_class_id")

        self.description = {"cz": self.description_cz, "en": self.description_en}

    def __str__(self):
        outstring = f"#{self.id} - {self.name}"
        if self.description_cz is not None:
            outstring += f" ({self.description_cz})"
        outstring += f"[{self.lat}N, {self.lng}E]"
        loc_plots = []
        first_plot_date = None
        last_experiment_date = None
        for pl in self.runoffdb.plots.values():
            if pl.locality_id == self.id:
                loc_plots.append(pl.id)
                if len(pl.get_runs_on_plot()) > 0:
                    # print(f"runs on plot: {', '.join([str(rid) for rid in pl.get_runs_on_plot()])}")
                    first_plot_date = pl.established if first_plot_date is None else min(pl.established, first_plot_date)
                    last_experiment_date = pl.get_last_run_datetime() if last_experiment_date is None else max(pl.get_last_run_datetime(), last_experiment_date)
        if first_plot_date is not None and last_experiment_date is not None:
            outstring += f"\n\t{czech_date(first_plot_date)} - {czech_date(last_experiment_date)}"
        elif first_plot_date is not None and last_experiment_date is None:
            outstring += f"\n\t{czech_date(first_plot_date)} - /"
        else:
            outstring += f"\n\tno plot was ever established on the locality"

        outstring += f"\n\tplots: {', '.join([str(plid) for plid in loc_plots])}"
        outstring += "\n"
        return outstring

class SoilSample:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb
        self.id = kwargs.get("id")
        self.processed_at_id = kwargs.get("processed_at_id")
        # optional direct reference to plot where the sample was collected
        self.plot_id = kwargs.get("plot_id")
        self.wrb_soil_class_id = kwargs.get("wrb_soil_class_id")
        self.locality_id = kwargs.get("locality_id")
        # optional direct reference to run directly related to the sample
        self.run_id = kwargs.get("run_id")
        # direct reference to important records
        self.corg_id = kwargs.get("corg_id")
        self.bulk_density_id = kwargs.get("bulk_density_id")
        self.texture_record_id = kwargs.get("texture_record_id")
        self.moisture_id = kwargs.get("moisture_id")

        self.date_sampled = kwargs.get("date_sampled")
        self.date_processed = kwargs.get("date_processed")

        # verbal description of sampling location (e.g. relative to experimental plot)
        self.sample_location = kwargs.get("sample_location")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.sample_depth_m = kwargs.get("sample_depth_m")
        self.raw_data_path = kwargs.get("raw_data_path")
        self.deleted = kwargs.get("deleted")
        self.user_id = kwargs.get("user_id")

        self.description = {"cz": self.description_cz, "en": self.description_en}


class Plot:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb
        self.id = kwargs.get("id")
        self.locality_id = kwargs.get("locality_id")
        self.soil_origin_locality_id = kwargs.get("soil_origin_locality_id")
        self.name = kwargs.get("name")
        self.crop_id = kwargs.get("crop_id")
        self.agrotechnology_id = kwargs.get("agrotechnology_id")
        self.established = kwargs.get("established")
        self.plot_width = kwargs.get("plot_width")
        self.plot_length = kwargs.get("plot_length")
        self.plot_slope = kwargs.get("plot_slope")
        self.protection_measure_id = kwargs.get("protection_measure_id")


    def get_last_run_datetime(self):
        with self.runoffdb.dbcon.cursor() as thecursor:
            query = f"SELECT max(`datetime`) FROM `run_group` JOIN `run` ON `run`.`run_group_id` = `run_group`.`id` " \
                    f"WHERE `run`.`plot_id` = {self.id}"
            # execute the query and fetch the results
            thecursor.execute(query)
            results = thecursor.fetchone()
            thecursor.close()

            if len(results) > 0:
                return results[0]

        return None

    def get_runs_on_plot(self):
        with self.runoffdb.dbcon.cursor() as thecursor:
            query = f"SELECT `id` FROM `run` WHERE `plot_id` = {self.id}"
            # execute the query and fetch the results
            thecursor.execute(query)
            results = thecursor.fetchall()
            thecursor.close()

            if len(results) > 0:
                run_list = []
                for res in results:
                    run_list.append(res[0])
                return run_list
        return []


class Crop:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.crop_type_id = kwargs.get("crop_type_id")
        self.crop_er_type_id = kwargs.get("croper_type_id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.variety = kwargs.get("variety")
        self.is_catch_crop = kwargs.get("is_catch_crop")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class Agrotechnology:

    operations = None

    @classmethod
    def load_all_operations(cls, runoffdb):
        print("loading tillage operations to establish agrotechnologies ...\n")
        with runoffdb.dbcon.cursor(dictionary=True) as thecursor:
            # execute the query and fetch the results
            thecursor.execute(f"SELECT * FROM {RunoffDB.operations_table}")
            results = thecursor.fetchall()
            thecursor.close()
            if len(results) > 0:
                operations = {}
                for r in results:
                    new = Operation(**r)
                    operations.update({new.id: new})
            Agrotechnology.operations = operations
            print(f"{len(operations)} agrotechnical operations successfully loaded")

        return operations



    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        # the first Agrotechnology instance induces the agrotechnology DB load of all operations
        if self.operations is None:
            Agrotechnology.load_all_operations(runoffdb)

        self.id = kwargs.get("id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.note_cz = kwargs.get("note_cz")
        self.note_en = kwargs.get("note_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.note = {"cz": self.note_cz, "en": self.note_en}

        self.operation_sequence = self.load_operation_sequence()

    def load_operation_sequence(self):
        dbcon = DBconnector().connect()
        if dbcon:
            thecursor = dbcon.cursor(dictionary=True)

            query = f"SELECT `operation_id`, `date` FROM {RunoffDB.tillageseq_table} WHERE `agrotechnology_id` = {self.id}"
            # print(query)
            thecursor.execute(query)
            results = thecursor.fetchall()

            if thecursor.rowcount == 0:
                print(f"\tno tillage sequence entry found for agrotechnology ID {self.id}")
                dbcon.close()
                return {}
            else:
                sequence = {}
                for r in results:
                    sequence.update({r["date"]: Agrotechnology.operations.get(r["operation_id"])})
                dbcon.close()
                return sequence
        else:
            return None

    def get_maximum_disturbance_level(self):
        if self.operation_sequence is None or self.operation_sequence == {}:
            return None
        return max([op.operation_intensity_id for op in self.operation_sequence.values()])


    def is_hay_cut(self):
        for op in self.operation_sequence.values():
            if op.id == 9:
                return True
        return False

    def is_mulch_cut(self):
        for op in self.operation_sequence.values():
            if op.id == 12:
                return True
        return False

class Operation:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.operation_intensity_id = kwargs.get("operation_intensity_id")
        self.operation_depth_m = kwargs.get("operation_depth_m")
        self.operation_type_id = kwargs.get("operation_type_id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.machinery_type_cz = kwargs.get("machinery_type_cz")
        self.machinery_type_en = kwargs.get("machinery_type_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.machinery_type = {"cz": self.machinery_type_cz, "en": self.machinery_type_en}

class Unit:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.unit = kwargs.get("unit")
        self.decimals = kwargs.get("decimals")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class Project:
    project_leaders = {1: "Dostál T.",
                       2: "Krása J., Stašek J., Roudnická A.",
                       3: "Devátý J.",
                       4: "Kavka P., Kubínová R.",
                       5: "Krása J.",
                       6: "Dostál T.",
                       7: "Kavka P.",
                       8: "Neumann M., Mistr M."}

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.project_name = kwargs.get("project_name")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.funding_agency = kwargs.get("funding_agency")
        self.project_code = kwargs.get("project_code")

        self.description = {"cz": self.description_cz, "en": self.description_en}

class Simulator:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.organization_id = kwargs["organization_id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.reference = kwargs.get("reference")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class ProtectionMeasure:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}
class RunType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class CropType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}
