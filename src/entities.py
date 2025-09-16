# -*- coding: utf-8 -*-
import os.path
import os

import pandas as pd
from datetime import datetime, time, date
import numpy as np
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

from src.db_access import DBconnector
from src.exceptions import DataframeEmptyError, RecordSetNotComplete, DataframeNotTimeIndexed

# multipliers for different units to convert between each other
multipliers = {1: {1: 1},
               2: {2: 1, 3: 0.001},
               3: {2: 1000, 3: 1},
               6: {6: 1, 32: 1/60},
               18: {18: 1, 27: 0.001},
               27: {18: 1000, 27: 1},
               32: {6: 60, 32: 1}}

class RunoffDB:
    agrotechnologies_table = "`agrotechnology`"
    assignmenttypes_table = "`assignment_type`"
    crops_table = "`crop`"
    crop_types_table = "`crop_type`"
    data_table = "`data`"
    instruments_table = "`instrument`"
    localities_table = "`locality`"
    measurements_table = "`measurement`"
    measurement_run_table = "`measurement_run`"
    measurement_soil_sample_table = "`measurement_soil_sample`"
    methodics_table = "`methodics`"
    methodics_processing_step_table = "`methodics_processing_step`"
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
    plot_protection_measures_table = "`plot_protection_measure`"
    processing_step_table = "`processing_step`"
    processing_step_instrument_table = "`processing_step_instrument`"
    quality_index_table = "quality_index"
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
    default_view_order = [2, 1, 3, 4, 6, 7, 8, 5]
    # 1 - raw data
    # 2 - edited data
    # 3 - homogenized edited data
    # 4 - homogenized raw data
    # 5 - set value
    # 6 - derived data
    # 7 - estimated from similar conditions
    # 8 - rough estimate

    def __init__(self, output_na_value=None, log_file_path=None):
        print(80*"=")
        print("RunoffDB initialization ... ")
        print(80*"="+"\n")
        self.dbcon = DBconnector()
        self.na_value = output_na_value

        # on initiation load all the entities that are used all the time
        self.run_types = self.load_run_types()
        self.crop_types = self.load_crop_types()
        self.operation_intensities = self.load_operation_intensities()
        self.operation_types = self.load_operation_types()
        self.organizations = self.load_organizations()
        self.simulators = self.load_simulators()
        self.localities = self.load_localities()
        self.agrotechnologies = self.load_agrotechnologies()
        self.units = self.load_units()
        self.crops = self.load_crops()
        self.protection_measures = self.load_protection_measures()
        self.plots = self.load_plots()
        self.samples = self.load_samples()
        self.methodics = self.load_methodics()
        self.projects = self.load_projects()
        self.phenomena = self.load_phenomena()
        self.record_types = self.load_record_types()
        self.quality_index = self.load_quality_index()
        self.assignment_types = self.load_assignment_types()

        # do not load the runs as they might be limited by filters
        self.runs = None

        self.log_file_path = log_file_path
        self.run_log = {}
        print("\n... everything is ready.")
        print(80*"="+"\n")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # write the log
        self.save_log()

    def get_connection(self):
        return self.dbcon.pool.get_connection()

    def log(self, run_id, text_to_log):
        """
        Creates run id indexed log
        :param run_id: run ID as key
        :param text_to_log: text to append to the key
        :return: None
        """
        if run_id not in self.run_log.keys():
            self.run_log[run_id] = set()

        self.run_log[run_id].add(text_to_log)
        return

    def save_log(self, output_path=None):
        output_path = output_path or self.log_file_path
        if len(self.run_log) == 0 and output_path:
            print(f"\nnothing to logg ...")
            return
        if len(self.run_log) > 0 and not output_path:
            print(f"\nlogg file path not defined - can't save the log with {len(self.run_log)} entries")
            return

        if output_path:
            # delete the file if already exists
            if os.path.isfile(output_path):
                os.remove(output_path)
            with open(output_path, "a") as f:
                for run_id, logs in self.run_log.items():
                    run = self.runs.get(run_id)
                    f.write(f"#{run.id} - {czech_date(run.datetime)} - {run.locality.name} - {run.crop.name['en']} - {run.run_type.name['en']}\n")
                    for log in logs:
                        f.write(f"\t{log} \n")
                    f.write("\n")
            print(f"\nlog file saved to {output_path}")
        return

    def clear_log(self):
        self.run_log = {}

    def load_runs(self, limit=None, date_from=None, date_to=None, simulators=None, localities=None, crops=None):
        """
        Loads Run objects from database filtered to match given limitations. Loads all simulation runs if no filtres provided.
        :param limit: number of runs to load
        :param date_from: load runs older than ('YYYY-MM-DD' format)
        :param date_to: load runs younger than ('YYYY-MM-DD' format)
        :param simulators: load runs performed with given simulator/s (list of simulator IDs)
        :param localities: load runs performed on given locality/localities (list of locality IDs)
        :param crops: load runs performed on a plot with given crop/s (list of crop IDs)
        :return:
        """
        # convert everything to lists if not already

        if simulators is not None and not isinstance(simulators, list):
            simulators = [simulators]
        if localities is not None and not isinstance(localities, list):
            localities = [localities]
        if crops is not None and not isinstance(crops, list):
            crops = [crops]

        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # start of the query
                query = f"SELECT {self.runs_table}.`id` AS run_id, " \
                        f"{self.runs_table}.`runoff_start` AS ttr, " \
                        f"{self.runs_table}.`init_moisture_id` AS initmoist_recid, " \
                        f"{self.runs_table}.`surface_cover_id` AS surface_cover_recid, " \
                        f"{self.runs_table}.`rain_intensity_id` AS rainfall_recid, " \
                        f"{self.runs_table}.`soil_sample_bulk_id` AS bulkd_ss_id, " \
                        f"{self.runs_table}.`bulk_assignment_type_id` AS bulkd_ss_asstype, " \
                        f"{self.runs_table}.`soil_sample_texture_id` AS texture_ss_id, " \
                        f"{self.runs_table}.`texture_assignment_type_id` AS texture_ss_asstype, " \
                        f"{self.runs_table}.`soil_sample_corg_id` AS corg_ss_id, " \
                        f"{self.runs_table}.`corg_assignment_type_id` AS corg_ss_asstype, " \
                        f"{self.runs_table}.`crop_bbch` AS bbch, " \
                        f"{self.runs_table}.`note_cz`, " \
                        f"{self.runs_table}.`note_en`, " \
                        f"{self.runs_table}.`crop_condition_cz`, " \
                        f"{self.runs_table}.`crop_condition_en`, " \
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
                    query += f" AND {self.run_groups_table}.`datetime` >= '{date_from}'"
                if date_to is not None:
                    query += f" AND {self.run_groups_table}.`datetime` <= '{date_to}'"
                if simulators is not None:
                    query += f" AND {self.sequences_table}.`simulator_id` IN ({', '.join([str(s) for s in simulators])})"
                if localities is not None:
                    query += f" AND {self.plots_table}.`locality_id` IN ({', '.join([str(s) for s in localities])})"
                if crops is not None:
                    query += f" AND {self.runs_table}.`crop_id` IN ({', '.join([str(s) for s in crops])})"
                # additional conditions
                # query += f"AND `` = "

                # end of the query
                query += " ORDER BY `datetime` ASC"

                if limit:
                    query += f" LIMIT {limit}"
                # print(query)
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

    def get_runs(self, date_from=None, date_to=None, run_types=None, simulators=None, localities=None, crops=None, plots=None):
        """
        Returns filtered list of runs from own list of runs

        :param date_from:
        :param date_to:
        :param run_types:
        :param simulators:
        :param localities:
        :param crops:
        :param plots:
        :return:
        """

        # convert everything to lists if not already
        if run_types is not None and not isinstance(run_types, list):
            run_types = [run_types]
        if simulators is not None and not isinstance(simulators, list):
            simulators = [simulators]
        if localities is not None and not isinstance(localities, list):
            localities = [localities]
        if crops is not None and not isinstance(crops, list):
            crops = [crops]
        if plots is not None and not isinstance(plots, list):
            plots = [plots]

        result = self.runs.values()

        # filter by datetime range
        if date_from is not None:
            result = [r for r in result if r.datetime >= datetime.combine(date.fromisoformat(date_from), time(0, 0, 0))]
        if date_to is not None:
            result = [r for r in result if r.datetime <= datetime.combine(date.fromisoformat(date_to), time(23, 59, 59))]

        # filter by categorical ids
        if run_types is not None:
            result = [r for r in result if r.run_type_id in run_types]
        if simulators is not None:
            result = [r for r in result if r.simulator_id in simulators]
        if localities is not None:
            result = [r for r in result if r.locality_id in localities]
        if crops is not None:
            result = [r for r in result if r.crop_id in crops]
        if plots is not None:
            result = [r for r in result if r.plot_id in plots]

        return result

    def load_plots(self, id=None):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
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
                    print(f"plots loaded ({len(plot_dict)})")

                    return plot_dict
                return None

    def load_samples(self, id=None):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
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
                    print(f"soil samples loaded ({len(samples_dict)})")

                    return samples_dict

                return None

    def load_simulators(self, id=None):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
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
                    print(f"simulators loaded ({len(simulators)})")
                thecursor.close()
            return simulators

    def load_organizations(self, id=None):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.organizations_table}"
                if id is not None:
                    query += f" WHERE `id` = {id}"
                query += " ORDER BY `id` ASC"

                thecursor.execute(query)
                results = thecursor.fetchall()
                if thecursor.rowcount > 0:
                    organizations = {}
                    for r in results:
                        new = Organization(self, **r)
                        organizations.update({new.id: new})
                    print(f"organizations loaded ({len(organizations)})")
                thecursor.close()
            return organizations

    def load_localities(self, id=None):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
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
                print(f"localities loaded ({len(localities)})")
                thecursor.close()
            return localities

    def load_run_types(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.run_types_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    run_types = {}
                    for r in results:
                        new = RunType(**r)
                        run_types.update({new.id: new})
                    print(f"run types loaded")
                thecursor.close()
            return run_types


    def load_crop_types(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.crop_types_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()
                crop_types = {}
                if thecursor.rowcount > 0:
                    for r in results:
                        new = CropType(**r)
                        crop_types.update({new.id: new})
                print(f"crop types loaded")
                thecursor.close()
            return crop_types

    def load_operation_types(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.operation_types_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    op_types = {}
                    for r in results:
                        new = OperationType(**r)
                        op_types.update({new.id: new})
                    print(f"operation types loaded")
                thecursor.close()
            return op_types

    def load_operation_intensities(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.operation_intensities_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    op_ints = {}
                    for r in results:
                        new = OperationIntensity(**r)
                        op_ints.update({new.id: new})
                    print(f"operation intensities loaded")
                thecursor.close()
            return op_ints

    def load_protection_measures(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.protection_measures_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()
                out_dict = {}
                if thecursor.rowcount > 0:
                    for r in results:
                        new = ProtectionMeasure(**r)
                        out_dict.update({new.id: new})
                print(f"soil protection measures loaded ({len(out_dict)})")
                thecursor.close()
            return out_dict

    def load_units(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {self.units_table}")
                results = thecursor.fetchall()
                if thecursor.rowcount > 0:
                    units = {}
                    for r in results:
                        new_unit = Unit(**r)
                        units.update({new_unit.id: new_unit})
                print(f"units loaded ({len(units)})")
                thecursor.close()
            return units

    def load_projects(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {self.projects_table}")
                results = thecursor.fetchall()
                if thecursor.rowcount > 0:
                    projects = {}
                    for r in results:
                        new_project = Project(**r)
                        projects.update({new_project.id: new_project})
                print(f"projects loaded")
                thecursor.close()
            return projects

    def load_crops(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {self.crops_table}")
                results = thecursor.fetchall()
                if thecursor.rowcount > 0:
                    crops = {}
                    for r in results:
                        new_crop = Crop(self, **r)
                        crops.update({new_crop.id: new_crop})
                print(f"crops loaded ({len(crops)})")
                thecursor.close()
            return crops

    def load_agrotechnologies(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {self.agrotechnologies_table}")
                results = thecursor.fetchall()
                if thecursor.rowcount > 0:
                    agrotechnologies = {}
                    for r in results:
                        new_agt = Agrotechnology(self, **r)
                        agrotechnologies.update({new_agt.id: new_agt})
                print(f"agrotechnologies loaded ({len(agrotechnologies)})")
                thecursor.close()
            return agrotechnologies

    def load_phenomena(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.phenomena_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    phenomena = {}
                    for r in results:
                        new = Phenomenon(**r)
                        phenomena.update({new.id: new})
                    print(f"phenomena loaded")
                thecursor.close()
            return phenomena

    def load_record_types(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.record_types_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    record_types = {}
                    for r in results:
                        new = RecordType(**r)
                        record_types.update({new.id: new})
                    print(f"record types loaded")
                thecursor.close()
            return record_types

    def load_quality_index(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.quality_index_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    quality_indices = {}
                    for r in results:
                        new = QualityIndex(**r)
                        quality_indices.update({new.id: new})
                    print(f"quality indexes loaded")
                thecursor.close()
            return quality_indices

    def load_assignment_types(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {self.assignmenttypes_table}"

                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    ats = {}
                    for r in results:
                        new = AssignmentType(**r)
                        ats.update({new.id: new})
                    print(f"assignment types loaded")
                thecursor.close()
            return ats

    def load_methodics(self):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {self.methodics_table}")
                results = thecursor.fetchall()
                if thecursor.rowcount > 0:
                    methodics = {}
                    for r in results:
                        new = Method(self, **r)
                        methodics.update({new.id: new})
                print(f"methodics loaded ({len(methodics)})")
                thecursor.close()
            return methodics

    def load_record_by_id(self, record_id):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True)as thecursor:
                # start of the query
                query = f"SELECT {self.records_table}.*, {self.measurements_table}.`phenomenon_id` AS phenomenon_id " \
                        f"FROM {self.records_table} " \
                        f"JOIN {self.measurements_table} ON {self.measurements_table}.`id` = {self.records_table}.`measurement_id` " \
                        f"WHERE {self.records_table}.`id` = {record_id}"
                # execute the query and fetch the results
                thecursor.execute(query)
                results = thecursor.fetchone()
                thecursor.close()

                if len(results) > 0:
                    return Record(self, **results)
                else:
                    return None

    def get_simulation_days(self, date_from=None, date_to=None):
        """
        Returns list of datetime objects when a simulation was carried on.
        Time component of the datetime is set to 0:00:00.
        :param date_from: filter simulation days older than date_from
        :param date_to: filter simulation days younger than date_to
        :return:
        """

        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # start of the query
                query = f"SELECT DISTINCT DATE({self.run_groups_table}.`datetime`) AS datetime " \
                        f"FROM {self.runs_table} " \
                        f"JOIN {self.run_groups_table} ON {self.runs_table}.`run_group_id` = {self.run_groups_table}.`id` " \
                        f"JOIN {self.sequences_table} ON {self.run_groups_table}.`sequence_id` = {self.sequences_table}.`id` " \
                        f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) "
                if date_from is not None:
                    query += f" AND {self.run_groups_table}.`datetime` >= '{date_from}'"
                if date_to is not None:
                    query += f" AND {self.run_groups_table}.`datetime` <= '{date_to}'"

                # end of the query
                query += " ORDER BY `datetime` ASC"

                # execute the query and fetch the results
                thecursor.execute(query)

                results = thecursor.fetchall()
                thecursor.close()
                sim_days = []
                if len(results) > 0:
                    for r in results:
                        # create start of the day (00:00:00)
                        the_day = r['datetime']
                        the_day = datetime.combine(the_day, time(0, 0, 0))
                        # the_day.replace(hour=0, minute=0, second=0)
                        sim_days.append(the_day)
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

    def show_run_records(self, lang="en", indent=0):
        """

        """
        for run in self.runs.values():
            run.show_records(lang=lang, indent=indent)
        return

    def load_sequence_ids_by_date(self, datetime):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
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

    def get_all_phenomena_ids(self):
        return [k for k in self.phenomena.keys()]

    def get_all_units_ids(self):
        return [k for k in self.units.keys()]

    def get_all_quality_indexes(self):
        return [k for k in self.quality_index.keys()]

    def find_orphan_records(self):
        return
    def find_orphan_measurements(self):
        return

    def export_methodics(self, lang="en"):
        export = {}
        for meth in self.methodics.values():
            export.update(meth.export_to_json(lang))
        return export

class Run:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["run_id"]
        self.sequence_id = kwargs["sequence_id"]
        self.run_group_id = kwargs["run_group_id"]
        self.datetime = kwargs["datetime"]
        self.simulator_id = kwargs["simulator_id"]
        self.simulator = runoffdb.simulators[self.simulator_id]
        self.run_type_id = kwargs["run_type_id"]
        self.run_type = self.runoffdb.run_types[self.run_type_id]
        self.ttr = kwargs["ttr"]
        self.measurements = None
        self.brothers = None
        self.plot_id = kwargs["plot_id"]
        self.plot = runoffdb.plots[self.plot_id]
        self.locality_id = kwargs["locality_id"]
        self.locality = self.runoffdb.localities[self.locality_id]
        self.crop_id = kwargs["crop_id"]
        self.crop = runoffdb.crops[self.crop_id]
        self.crop_type_id = kwargs["crop_type_id"]
        self.crop_name = None

        self.initmoist_recid = kwargs["initmoist_recid"]
        self.surface_cover_recid = kwargs["surface_cover_recid"]
        self.bbch = kwargs["bbch"]
        self.rain_intensity_recid = kwargs["rainfall_recid"]

        self.bulkd_ss_id = kwargs["bulkd_ss_id"]
        self.bulkd_ss = self.runoffdb.samples[self.bulkd_ss_id] if self.bulkd_ss_id is not None else None
        self.bulkd_ss_asstype = self.runoffdb.assignment_types[kwargs["bulkd_ss_asstype"]] if kwargs.get("bulkd_ss_asstype") else None
        self.texture_ss_id = kwargs["texture_ss_id"]
        self.texture_ss = self.runoffdb.samples[self.texture_ss_id] if self.texture_ss_id is not None else None
        self.texture_ss_asstype = self.runoffdb.assignment_types[kwargs["texture_ss_asstype"]] if kwargs.get("texture_ss_asstype") else None
        self.corg_ss_id = kwargs["corg_ss_id"]
        self.corg_ss = self.runoffdb.samples[self.corg_ss_id] if self.corg_ss_id is not None else None
        self.corg_ss_asstype = self.runoffdb.assignment_types[kwargs["corg_ss_asstype"]] if kwargs.get("corg_ss_asstype") else None

        self.note = {"en": kwargs["note_en"], "cz": kwargs["note_cz"]}
        self.crop_condition = {"en": kwargs["crop_condition_en"], "cz": kwargs["crop_condition_cz"]}

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

        print(f"\n{indent}soil texture sample: {self.texture_ss_id}")
        print(f"{indent}soil bulk density sample: {self.bulkd_ss_id}")
        print(f"{indent}soil Corg sample: {self.corg_ss_id}")

        print(f"\n{indent}rain intensity record: {self.rain_intensity_recid}")
        print(f"{indent}initial moisture record: {self.initmoist_recid}")
        print(f"{indent}surface cover record: {self.surface_cover_recid}")
        print("\n")

    def show_records(self, lang="en", indent=0):
        print("\n"+indent*"\t"+f"#{self.id} - {czech_date(self.datetime)} - {self.locality.name}")

        all_records = self.get_records()
        if all_records:
            for rec in all_records:
                # rec.show_units(lang=lang, indent=indent+1)
                rec.show_details()
        else:
            print(f"\tno records at all")
        return

    def load_group_brothers(self):
        """
        Returns a list of IDs of simulation runs from the same group
        :return:
        """
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor() as thecursor:
                # execute the query and fetch the results
                query = f"SELECT `run`.`id` FROM {RunoffDB.runs_table} WHERE `run_group_id` = {self.run_group_id}"

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

    def get_initial_moisture_value(self, multi_value=False):
        if self.initmoist_recid:
            initmoist_rec = self.runoffdb.load_record_by_id(self.initmoist_recid)
            initmoist_rec.load_data("initial_moisture")
            initmoist_data = initmoist_rec.get_data("initial_moisture")

            if len(initmoist_data.index) == 1:
                return initmoist_data["initial_moisture"].mean()
            else:
                self.runoffdb.log(self.id, f"dedicated initial moisture record {initmoist_rec.id} contains more than one value")
                if multi_value:
                    return initmoist_data["initial_moisture"].toList()
                else:
                    print(f"Initial moisture record {initmoist_rec.id} of run {self.id} has more then one value!")
                    print(f"Mean value of all {len(initmoist_data.index)} data points was returned.")
                    return initmoist_data["initial_moisture"].mean()
        else:
            self.runoffdb.log(self.id, "initial moisture dedicated record not assigned")
            print(f"\trun #{self.id} doesn't have dedicated initial moisture record ID assigned.")
            return None


    def get_surface_cover_value(self, multi_value=False):
        """
        Returns a value of surface cover assigned to the run.
        If the assigned record holds more data points either mean is returned or a list of values.

        :param multi_value: if False returns mean of all values if more than one data point belongs to the record or list of values if True
        :return: value of surface cover or list of all values in record
        """
        # first check if the surface cover record is assigned to run
        surface_cover_rec = None

        if not self.surface_cover_recid:
            self.runoffdb.log(self.id,f"surface cover dedicated record not assigned")
            print(f"\trun #{self.id} doesn't have dedicated surface cover record assigned")
            surface_cover_rec = self.get_best_record_of_unit(unit_id=10)

        else:
            surface_cover_rec = self.runoffdb.load_record_by_id(self.surface_cover_recid)

        if surface_cover_rec:
            surface_cover_rec.load_data("surface_cover")
            surface_cover_data = surface_cover_rec.get_data("surface_cover")

            if len(surface_cover_data.index) == 1:
                return surface_cover_data["surface_cover"].mean()
            else:
                self.runoffdb.log(self.id, f"dedicated surface cover record {surface_cover_rec.id} contains more than one value")
                if multi_value:
                    return surface_cover_data["surface_cover"].toList()
                else:
                    print(f"Surface cover record {surface_cover_rec.id} of run {self.id} has more then one value!")
                    print(f"Mean value of all {len(surface_cover_data.index)} data points was returned.")
                    return surface_cover_data["surface_cover"].mean()

        else:
            # check crop type for "without cover" if no surface cover assigned to run
            if self.crop.crop_type_id == 10:
                self.runoffdb.log(self.id, f"no record of surface cover found - value derived from crop type")
                print(f"\trun #{self.id} has no surface cover record - value derived from crop type")
                return 0
            else:
                self.runoffdb.log(self.id, f"no record of surface cover found")
                print(f"\trun #{self.id} has no surface cover record")
                return None


    def get_crop_height_value(self):
        crop_height_rec = self.get_best_record_of_unit(31)
        if crop_height_rec is not None:
            try:
                crop_height_data = crop_height_rec.get_data("crop_height")
            except DataframeEmptyError:
                self.runoffdb.log(self.id, f"crop heigh record #{crop_height_rec.id} returned empty dataframe")
                print(f"\tcrop heigh dataframe of record {crop_height_rec.id} is empty")
                return None
            else:
                if crop_height_data is not None:
                    return crop_height_data["crop_height"].mean()
                else:
                    self.runoffdb.log(self.id, f"plant density data of record {crop_height_rec.id} is None")
                    print(f"\tplant density data of record {crop_height_rec.id} is None")
                    return None
        else:
            self.runoffdb.log(self.id, f"no crop height record found")
            print(f"\trun #{self.id} has no crop height record")
            return None


    def get_plant_density_value(self):
        plant_density_rec = self.get_best_record_of_unit(30)
        if plant_density_rec is not None:
            try:
                plant_density_data = plant_density_rec.get_data("plant_density")
            except DataframeEmptyError:
                self.runoffdb.log(self.id, f"plant density record #{plant_density_rec.id} returned empty dataframe")
                print(f"\tplant density dataframe of record {plant_density_rec.id} is empty")
                return None
            else:
                if plant_density_data is not None:
                    return plant_density_data["plant_density"].mean()
                else:
                    self.runoffdb.log(self.id, f"plant density data of record {plant_density_rec.id} is None")
                    print(f"\tplant density data of record {plant_density_rec.id} is None")
                    return None
        else:
            self.runoffdb.log(self.id, f"no plant density record found")
            print(f"\trun #{self.id} has no plant density record")
        return

    def get_rainfall_intensity_timeline(self, target_unit_id=None, series_label="rainfall_intensity"):
        if self.rain_intensity_recid is not None:
            intensity_rec = self.runoffdb.load_record_by_id(self.rain_intensity_recid)
            intensity_data = intensity_rec.get_data_in_unit(target_unit_id, series_label, demand_timeline=True)
            # constant intensity series has exactly 2 rows, any other number is some exception or non-standard rainfall
            if len(intensity_data.index) == 1:
                self.runoffdb.log(self.id, f"rainfall intensity series of record {intensity_rec.id} contains only one data point")
                print(f"Rainfall intensity record {intensity_rec.id} of run {self.id} contains only one data point. Proper rainfall intensity must have at least two data points.")
                return None
            elif len(intensity_data.index) == 2:
                if intensity_data["rain_intensity"].iloc[-1] != 0:
                    self.runoffdb.log(self.id, f"rainfall intensity timeline record {intensity_rec} not ending with 0")
                    print(f"Rainfall intensity record {self.rain_intensity_recid} of run {self.id} doesn't end with zero value!")
                    return None
                else:
                    return intensity_data
        else:
            self.runoffdb.log(self.id, f"dedicated rainfall intensity record not assigned")
            print(f"\trun #{self.id} doesn't have dedicated rainfall intensity record ID assigned")
            return None

    def get_rainfall_intensity_value(self, target_unit_id = None):
        intensity_data = self.get_rainfall_intensity_timeline(target_unit_id, "rain_intensity")
        if intensity_data is not None:
            # regular intensity series has exactly 2 rows, any other number is some exception or non-standard rainfall
            if len(intensity_data.index) == 1:
                return None
            elif len(intensity_data.index) == 2:
                if intensity_data["rain_intensity"].iloc[-1] != 0:
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
            return None


    def get_total_rainfall(self):
        raise NotImplementedError("Run object method 'get_total_rainfall()' is not implemented yet")

    def get_best_record_of_unit(self, unit_id, phenomenon_id=None, view_order=None,
                                related_value_x_unit_id=None,
                                related_value_y_unit_id=None,
                                related_value_z_unit_id=None):
        """
        Returns record of specified unit/units that is 'best' according to specified (or default) record types view order
        Record with best (lowest) quality index is return if more than one record is retrieved from the DB.
        If more than one record is available with best type and q.i. the first record is returned.
        :param unit_id:
        :param phenomenon_id:
        :param view_order:
        :return:
        """

        # use the default view order if not specified
        view_order = view_order if view_order is not None else self.runoffdb.default_view_order
        # search for records of unit
        # go through the record type priority list and find the first matching Record
        for record_type in view_order:
            # get records of current type
            found_records = self.get_records(unit_id, phenomenon_id, record_type, related_value_x_unit_id, related_value_y_unit_id, related_value_z_unit_id)
            if found_records is not None:
                if found_records is not None:
                    for qi in self.runoffdb.get_all_quality_indexes() + [None]:
                        # list records of this quality
                        rq = []
                        for rec in found_records:
                            if rec.quality_index_id == qi:
                                # print(
                                #     f"\t{rec.unit.name['en']} record of {rec.phenomenon.name['en']}, type '{rec.record_type.name['en']}' and quality index {qi} found: {rec.id}")
                                rq.append(rec)
                        # as soon as any record is found return it
                        if len(rq) > 0:
                            if len(rq) > 1:
                                print(
                                    f"\tMultiple records of unit {unit_id}, type '{self.runoffdb.record_types[record_type].name['en']}' and quality index {qi} were found for run #{self.id}:\n"
                                    f"\t{', '.join([str(r.id) for r in rq])} - first of them will be used for processing (record id {rq[0].id}).\n")

                                self.runoffdb.log(self.id, f"multiple records of type '{self.runoffdb.record_types[record_type].name['en']}' and quality index {qi} were found for unit #{unit_id}: "
                                    f"{', '.join([str(r.id) for r in rq])}")
                            return rq[0]

        return None

    def get_best_rainfall_record(self, view_order=None):
        if self.rain_intensity_recid is not None:
            return self.runoffdb.load_record_by_id(self.rain_intensity_recid)
        else:
            self.runoffdb.log(self.id, f"dedicated rainfall intensity record not assigned")
            print(f"\trun #{self.id} doesn't have dedicated rainfall intensity record assigned")
            # check if there's another rainfall intensity record that was not assigned as dedicated
            return self.get_best_record_of_unit([6, 28], 3, view_order)

    def get_best_runoff_record(self, view_order=None):
        return self.get_best_record_of_unit(1, 1, view_order)

    def get_best_runoff_value_l(self):
        raise NotImplementedError("Run object method 'get_best_runoff_value_l()' is not implemented yet")

    def get_best_runoff_value_mm(self):
        raise NotImplementedError("Run object method 'get_best_runoff_value_mm()' is not implemented yet")

    def get_best_sediment_concentration_record(self, view_order=None):
        return self.get_best_record_of_unit([2, 3], 2, view_order)

    def get_best_sediment_yield_value_g(self):
        raise NotImplementedError("Run object method 'get_best_sediment_yield_value_g()' is not implemented yet")

    def get_best_sediment_yield_value_tha(self):
        raise NotImplementedError("Run object method 'get_best_sediment_yield_value_tha()' is not implemented yet")

    def get_best_soil_texture_record(self):
        # try getting dedicated soil texture record
        if self.texture_ss is not None:
            # print(f"run {run.id} has texture sample {run.texture_ss_id}")
            # print(f"sample {run.texture_ss_id} has texture record set {self.samples.get(run.texture_ss_id).texture_record_id}")
            # load the record from DB
            if self.texture_ss.texture_record_id is not None:
                tex_rec = self.runoffdb.load_record_by_id(self.texture_ss.texture_record_id)
                if tex_rec is not None:
                    # print(f"unit of the record is '{self.units.get(tex_rec.unit_id).name_en}' with dimension [{self.units.get(tex_rec.unit_id).unit}]")
                    # print(f"related X unit of the record is '{self.units.get(tex_rec.related_value_x_unit_id).name_en}' with dimension [{self.units.get(tex_rec.related_value_x_unit_id).unit}]")
                    return tex_rec
                else:
                    self.runoffdb.log(self.id,
                                      f"\tdedicated texture record #{self.texture_ss.texture_record_id} of texture soil sample #{self.texture_ss.id} doesn't have dedicated texture record assigned.")
                    print(f"\tdedicated texture soil sample of run #{self.id} returns None as texture record ID {self.texture_ss.texture_record_id}.\n"
                          f"Check your data consistency in the database.")
                    return None
            else:
                self.runoffdb.log(self.id, f"\tdedicated texture soil sample #{self.texture_ss.id} doesn't have dedicated texture record assigned.")
                print(f"\tdedicated texture soil sample of run #{self.id} doesn't have dedicated texture record assigned.")
                return None
        else:
            self.runoffdb.log(self.id, f"dedicated texture soil sample not assigned")
            print(f"\trun #{self.id} doesn't have dedicated texture soil sample assigned")

        # try getting any record with texture
        return self.get_best_record_of_unit(9, 20)

    def get_best_soil_texture_data(self, x_label="cumulative_mass_content", y_label="particle_size", index_column=None,
                                             order_by=None, limits=None, return_int=False, return_cumulative=False):
        # set default values for index column and row order
        index_column = index_column or y_label
        order_by = order_by or y_label

        texture_rec = self.get_best_soil_texture_record()
        if texture_rec is not None:
            try:
                texture_data = texture_rec.load_data(x_label, y_label, index_column=index_column, order_by=order_by)
            except DataframeEmptyError:
                self.runoffdb.log(self.id, f"soil texture record #{texture_rec.id} returned empty dataframe")
                raise
            else:
                if texture_data is not None and limits:
                    texture_data = interpolate_texture(texture_data,
                                                               limits,
                                                               "cumulative_mass_content",
                                                               return_int=return_int,
                                                               return_cumulative=return_cumulative)
                return texture_data

        return None


    def get_best_bulk_density_redord(self):
        # try getting dedicated bulk density record
        if self.bulkd_ss is not None:
            # print(f"run {run.id} has texture sample {run.texture_ss_id}")
            # load the record from DB
            if self.bulkd_ss.bulk_density_id is not None:
                bulkd_rec = self.runoffdb.load_record_by_id(self.bulkd_ss.bulk_density_id)
                if bulkd_rec is not None:
                    return bulkd_rec
                else:
                    self.runoffdb.log(self.id,
                                      f"\tdedicated bulk density record #{self.bulkd_ss.bulk_density_id } of bulk density soil sample #{self.bulkd_ss.id} doesn't have dedicated bulk density record assigned.")

                    print(f"\tassigned bulk density soil sample of run #{self.id} returns None as bulk density record ID {self.bulkd_ss.bulk_density_id}.\n"
                          f"Check your data consistency in the database.")
                    return None
            else:
                self.runoffdb.log(self.id, f"\tdedicated bulk density soil sample #{self.bulkd_ss.id} doesn't have dedicated bulk density record assigned.")

                print(f"\tdedicated bulk density soil sample of run #{self.id} doesn't have dedicated bulk density record assigned.")
                return None
        else:
            self.runoffdb.log(self.id, f"dedicated bulk density soil sample not assigned")
            print(f"\trun #{self.id} doesn't have dedicated bulk density soil sample assigned")

        # try getting any record with bulk density
        return self.get_best_record_of_unit(9, [18, 27])

    def get_best_bulk_density_value(self, target_unit_id=None, label="bulk_density"):
        bulkd_record = self.get_best_bulk_density_redord()
        if bulkd_record:
            try:
                bulk_data = bulkd_record.get_data_in_unit(target_unit_id, label)
            except DataframeEmptyError:
                self.runoffdb.log(self.id, f"soil bulk density record #{bulkd_record.id} returned empty dataframe")
                raise
            else:
                if bulk_data is not None:
                    return bulk_data["bulk_density"].mean()
        return None

    def get_best_hydro_data(self, labels_map=None, request_map=None, interpolation_map=None):
        """
        Combines hydrological data records of a run into one TimeDelta indexed dataframe with cross-interpolated time points.
        :param labels_map: Dictionary of labels that should be assigned to records in the output dataframe
        :param request_map: Dictionary of True/False that state if the record is essential for the output -
        If essential and not found The RecordSetNotComlete exception is thrown
        :param interpolation_map: Dictionary specifying interpolation method
        :param kwargs: Dictionary with labels for requested records.
        :return: pandas DataFrame with requested data.
        :except: RecordSetNotComplete if requested record is not found for the run
        """

        # default units
        default_units = {
            "runoff": 1, # in l.min-1
            "sediment_concentration": 3, # in g.l-1
            "rainfall_intensity": 6,# in mm.hour-1
            "sediment_flux": 23, # in g.min-1
            }

        # default labels map
        default_lables = {
            "runoff": "runoff [l.s-1]",
            "sediment_concentration": "sediment concentration [g.l-1]",
            "rainfall_intensity": "rainfall intensity [mm.hour-1]",
            "rainfall_total": "rainfall total [mm]",
            "discharge": "discharge [l]",
            "sediment_flux": "sediment flux [g.min-1]",
            "sediment_yield": "sediment yield [g]"
        }

        # default interpolation map
        default_interpolations = {
                "runoff": "linear",
                "sediment_concentration": "linear",
                "rainfall_intensity": "ffill",
            }


        # get complete set of labels by adopting user input
        if not labels_map:
            labels = default_lables.copy()
        else:
            labels = {key: labels_map.get(key, def_label) for key, def_label in default_lables.items()}

        # default request flag for all is False - no runs will throw the RecordSetNotComplete exception
        if not request_map:
            requested = {key: False for key in default_lables.keys()}
        else:
            requested = {key: request_map.get(key, False) for key in default_lables.keys()}

        # get complete set of interpolations by adopting user input
        if not interpolation_map:
            interpolations = default_interpolations.copy()
        else:
            interpolations = {key: interpolation_map.get(key, def_inter) for key, def_inter in
                              default_interpolations.items()}

        # known dependencies and record-fetching functions
        dependencies = {
            "rainfall_intensity": {"record": self.get_best_rainfall_record, "derived_from": []},
            "rainfall_total": {"derived_from": ["rainfall_intensity"]},
            "runoff": {"record": self.get_best_runoff_record, "derived_from": []},
            "sediment_concentration": {"record": self.get_best_sediment_concentration_record, "derived_from": []},
            "discharge": {"derived_from": ["runoff"]},
            "sediment_flux": {"derived_from": ["runoff", "sediment_concentration"]},
            "sediment_yield": {"derived_from": ["runoff", "sediment_concentration"]},
        }

        requested_keys = [k for k, v in requested.items() if v]
        present_records, missing_records = self._resolve_present_records(requested_keys, dependencies)

        # print(f" --> missing records: {missing_records}")
        if missing_records:
            raise RecordSetNotComplete(requested_keys, missing_records)

        # Collect data to DataFrames
        dataframes_to_merge = []
        empty_dataframes = []

        # print(f"present records: {present_records}")
        # Try loading the data for each requested record
        for key, rec in present_records.items():
            if rec:
                try:
                    unit_id = default_units.get(key)
                    df = self._get_record_data(rec, key, labels[key], demand_timeline=True, target_unit_id=unit_id)
                except DataframeEmptyError:
                    empty_dataframes.append(key)
                else:
                    dataframes_to_merge.append(df)
            else:
                # if record is None (means it doesn't exist)
                self.runoffdb.log(self.id, f"{labels[key]} record not available")
                dataframes_to_merge.append(pd.DataFrame({labels[key]: []}))

        # raise exception if any dataframe is empty
        # print(f" --> empty records: {empty_dataframes}")
        if empty_dataframes:
            raise RecordSetNotComplete(requested_keys, empty_dataframes)

        # merge dataframes into a single dataframe
        if len(dataframes_to_merge) > 1:
            merged_data = pd.concat(dataframes_to_merge, axis=1, join='outer')
        elif len(dataframes_to_merge) == 1:
            merged_data = dataframes_to_merge[0]
        else:
            # create an empty DataFrame with expected columns and no rows
            merged_data = pd.DataFrame()

        # add missing keys, so that the column count is constant
        for key, label in labels.items():
            if label not in merged_data.columns:
                merged_data[label] = pd.NA

        # reorder columns according to labels_map
        final_columns = [labels[k] for k in labels]
        merged_data = merged_data.reindex(columns=final_columns)

        # ensure merged index is a TimedeltaIndex
        merged_data.index = pd.to_timedelta(merged_data.index, errors='raise')

        # sort by time index
        try:
            merged_data.sort_index(inplace=True)
        except TypeError:
            print("Incompatible indexes in input dataframes - one of the input dataframes is not a timeline")

        # Adjust the merged data for missing or available labels
        # self._adjust_end_time(merged_data, kwargs)

        merged_data = self._interpolate_data(merged_data, interpolations)

        # Calculate derived fields like sediment_flux, rainfall_total, etc.
        # if "sediment_flux" in requested_keys and all(labels_map.get(col) in df for col in ["runoff", "sediment_concentration"]):
        #     df[labels_map.get("sediment_flux")] = df[labels_map.get("runoff")] * df[labels_map.get("sediment_concentration")]

        if labels.get("rainfall_intensity") in merged_data and not merged_data[labels.get("rainfall_intensity")].empty:
            integrate_data_series(merged_data, labels.get("rainfall_intensity"), labels.get("rainfall_total"), interpolate=True, time_unit='hours')

        if labels.get("runoff") in merged_data and not merged_data[labels.get("runoff")].empty:
            integrate_data_series(merged_data, labels.get("runoff"), labels.get("discharge"), interpolate=True, time_unit='minutes')

        if all(labels.get(col) in merged_data for col in ["runoff", "sediment_concentration"]):
            merged_data[labels["sediment_flux"]] = (merged_data[labels["runoff"]] * merged_data[labels["sediment_concentration"]]
            )

        if "sediment_flux" in merged_data and not merged_data[labels.get("sediment_flux")].empty:
            integrate_data_series(merged_data, labels.get("sediment_flux"), labels.get("sediment_yield"), interpolate=True, time_unit='minutes')

        return merged_data

    def _resolve_present_records(self, requested_keys, dependencies):
        """
        Resolves the presence of all base records regardless of requested_keys.
        Records are added to present_records if successfully loaded.
        Only records that are requested or are dependencies and cannot be loaded
        are added to missing_records.

        :return: (present_records, missing_records) as dict and list.
        """
        present_records = {}
        missing_records = []

        for key, config in dependencies.items():
            if "record" not in config:
                continue  # skip derived fields

            try:
                record = config["record"]()
                present_records[key] = record

                # if it's explicitly needed and not present, mark missing
                if record is None and (
                        key in requested_keys or
                        any(key in dependencies[req].get("derived_from", []) for req in requested_keys)
                ):
                    missing_records.append(key)

            except Exception:
                present_records[key] = None
                if (
                        key in requested_keys or
                        any(key in dependencies[req].get("derived_from", []) for req in requested_keys)
                ):
                    missing_records.append(key)

        return present_records, missing_records

    def _get_record_data(self, record, label, log_label, demand_timeline=False, target_unit_id=None):
        """
        Fetches data for a specific record, optionally converts it to target unit, and logs its status.
        :return: DataFrame with the data for the record.
        """
        try:
            if target_unit_id is not None and target_unit_id != record.unit_id:
                df = record.get_data_in_unit(
                    target_unit_id=target_unit_id,
                    value_name=label,
                    output_column_label=label,
                    demand_timeline=demand_timeline
                )
                if df is not None and not df.empty:
                    self.runoffdb.log(self.id, f"{log_label} (record #{record.id}) data converted to unit: {self.runoffdb.units[target_unit_id].unit} (original unit {record.unit.unit})")
            else:
                df = record.get_data(label, demand_timeline=demand_timeline)
                if df is not None and not df.empty:
                    self.runoffdb.log(self.id, f"{log_label} (record #{record.id}) data unit: {record.unit.unit}")

            return df

        except DataframeEmptyError:
            self.runoffdb.log(self.id, f"{log_label} DataFrame of record #{record.id} is empty")
            return pd.DataFrame({label: []})
        except DataframeNotTimeIndexed:
            self.runoffdb.log(self.id, f"{log_label} DataFrame of record #{record.id} is not timeline")
            return pd.DataFrame({label: []})
        except Exception as e:
            self.runoffdb.log(self.id, f"{log_label} record not available ({e})")
            return pd.DataFrame({label: []})

    def _adjust_end_time(self, merged_data, kwargs):
        """
        Adjusts the end time based on the last valid index of runoff or sediment concentration.
        :return: Adjusted dataframe.
        """
        runoff_label = kwargs.get("runoff")
        sed_conc_label = kwargs.get("sediment_concentration")

        end_time = min(
            merged_data[runoff_label].last_valid_index(),
            merged_data[sed_conc_label].last_valid_index()
        )
        merged_data = merged_data.loc[:end_time]
        return merged_data

    def _interpolate_data(self, df, interpolation_map):
        """
        Interpolates missing data in the dataframe according to the provided method map.
        :param df: The dataframe to interpolate.
        :param interpolation_map: Dict of column -> method (e.g., {'col1': 'linear', 'col2': 'ffill'})
        :return: Interpolated dataframe.
        """
        for column, method in interpolation_map.items():
            if column in df:
                if method == "ffill":
                    df[column] = df[column].ffill()
                elif method == "bfill":
                    df[column] = df[column].bfill()
                elif method in ("linear", "quadratic", "cubic", "spline", "polynomial"):
                    df[column] = df[column].interpolate(method=method)
                elif callable(method):
                    df[column] = method(df[column])
                else:
                    raise ValueError(f"Unsupported interpolation method: {method} for column {column}")
        return df

    def get_records(self, unit_id=None,
                    phenomenon_id=None,
                    record_type_id=None,
                    related_value_x_unit_id=None,
                    related_value_y_unit_id=None,
                    related_value_z_unit_id=None,
                    exclude_missing_records=False):

        out = []
        # get the measurements related to Run instance, pass on the argument
        measurements = self.get_measurements(phenomenon_id)
        # if any measurements like that exist
        if measurements:
            for meas in measurements:
                # load the records of measurement, pass on the arguments
                # records of all unit IDs are in the obtained list if unit is a list
                recs = meas.get_records(unit_id, record_type_id, related_value_x_unit_id, related_value_y_unit_id, related_value_z_unit_id, exclude_missing_records)

                # if any records like that exist
                if recs:
                    out.extend(recs)

            # return None if the out list is empty
            return out or None
        else:
            return None

    def plot_hydro_data(self, df, output_path, series_to_plot=None):
        """
        Plot selected hydro-sediment data from get_best_hydro_data() and save to file.

        Each data series gets its own Y-axis (stacked if needed).
        Uses the timedelta index (in minutes) as X-axis.

        Chart types per key:
            rainfall_intensity: bar
            rainfall_total: line
            runoff: point
            discharge: line
            sediment_flux: line
            sediment_yield: line

        :param output_path: File path to save the plot (e.g., "output.png")
        :param series_to_plot: Optional list of column names to include (default is all recognized ones)
        """
        default_chart_types = {
            'rainfall_intensity': 'bar',
            'rainfall_total': 'line',
            'runoff': 'point',
            'discharge': 'line',
            'sediment_flux': 'line',
            'sediment_yield': 'line'
        }

        if df is None or df.empty:
            print("No hydro-sediment data available to plot.")
            return

        if not isinstance(df.index, pd.TimedeltaIndex):
            raise ValueError("DataFrame index must be a TimedeltaIndex.")

        available_columns = [c for c in default_chart_types if c in df.columns]

        if series_to_plot is None:
            columns = available_columns
        else:
            columns = [col for col in series_to_plot if col in available_columns]

        if not columns:
            print("No valid columns selected for plotting.")
            return

        fig, ax1 = plt.subplots(figsize=(14, 6))
        base_ax = ax1
        axes = [base_ax]
        lines = []

        # Create additional Y-axes if needed
        for i in range(1, len(columns)):
            new_ax = base_ax.twinx()
            new_ax.spines.right.set_position(("axes", 1 + 0.1 * i))
            axes.append(new_ax)

        x_values = df.index.total_seconds() / 60  # X in minutes
        color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

        for i, col in enumerate(columns):
            ax = axes[i]
            color = color_cycle[i % len(color_cycle)]
            chart_type = default_chart_types.get(col, 'line')

            if chart_type == 'bar':
                bar_width = (x_values[1] - x_values[0]) if len(x_values) > 1 else 1
                line = ax.bar(x_values, df[col], width=bar_width, label=col, color=color, alpha=0.6)
            elif chart_type == 'point':
                line, = ax.plot(x_values, df[col], 'o', label=col, color=color)
            else:  # line
                line, = ax.plot(x_values, df[col], label=col, color=color)

            ax.set_ylabel(col, color=color)
            ax.tick_params(axis='y', labelcolor=color)
            lines.append(line)

        base_ax.set_xlabel("Time [minutes]")
        base_ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        base_ax.grid(True, which='both', axis='both', linestyle='--', alpha=0.4)

        # Create a single combined legend
        labels = [l.get_label() if hasattr(l, 'get_label') else l[0].get_label() for l in lines]
        fig.legend(lines, labels, loc='upper right', bbox_to_anchor=(1, 1), bbox_transform=fig.transFigure)

        plt.title("Hydro-Sediment Time Series")
        plt.tight_layout()
        fig.savefig(output_path)
        plt.close(fig)

    def load_measurements(self):
        msrmsnts = None
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {RunoffDB.measurements_table} " \
                        f"JOIN {RunoffDB.measurement_run_table} ON {RunoffDB.measurement_run_table}.`measurement_id` = {RunoffDB.measurements_table}.`id` " \
                        f"WHERE {RunoffDB.measurement_run_table}.`run_id` = {self.id}"
                # print(query)
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

    def get_measurements(self, phenomenon_id=None):
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
            self.measurements = self.load_measurements()
            if self.measurements:
                return self.get_measurements(phenomenon_id)
            else:
                return None

    def get_project_ids(self):
        ids = None
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor() as thecursor:
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
        found_records = self.get_records([15], 5, record_type_id=record_type)
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

    def get_metadata(self, lang="en"):
        meta = {}
        meta.update({"run ID": self.id})
        meta.update({"sequence ID": self.sequence_id})
        meta.update({"run group ID": self.run_group_id})
        meta.update({"simulator": self.simulator.get_metadata()})
        meta.update({"run type": self.runoffdb.run_types[self.run_type_id].name[lang]})
        meta.update({"date": self.datetime.strftime('%Y-%m-%d')})
        meta.update({"start time": self.datetime.strftime('%H:%M:%S')})
        meta.update({"locality": self.runoffdb.localities[self.locality_id].name})
        meta.update({"time to runoff": str(self.ttr)})
        meta.update({"crop": self.crop.get_metadata(lang)})
        meta.update({"plot": self.plot.get_metadata(lang)})
        if self.plot.agrotechnology is not None:
            meta.update({"agrotechnology": self.plot.agrotechnology.get_metadata(lang)})
        else:
            meta.update({"agrotechnology": "NA"})

        if len(self.get_measurements_metadata(lang)) > 0:
            meta.update({"measurements": self.get_measurements_metadata(lang)})
        else:
            meta.update({"measurements": "no measurements found"})

        return meta

    def get_measurements_metadata(self, lang="en"):
        meta = []
        msrmnts = self.get_measurements()
        if msrmnts is not None:
            for ms in msrmnts:
                meta.append(ms.get_metadata(lang))
        return meta

    def get_notes(self, lang="en"):
        notes = {}
        if self.plot.get_notes(lang=lang):
            notes["plot"] = self.plot.get_notes(lang=lang)
        notes["sequence"] = "NA"
        if self.get_measurements():
            mnotes = [m.get_notes(lang) for m in self.get_measurements() if m.get_notes(lang)]
            if mnotes:
                notes["measurements"] = mnotes

        return notes

    def find_fellow_fallow(self):
        """
        Attempts to find the reference fallow simulation for the Run
        (another run executed at the same day at the same location with same simulator)
        :return:
        """
        fallow_crop_id = 1

        return self.runoffdb.get_runs(date_from=self.datetime.strftime("%Y-%m-%d"),
                               date_to=self.datetime.strftime("%Y-%m-%d"),
                               localities=self.locality_id,
                               simulators=self.simulator_id,
                               crops=fallow_crop_id,
                               run_types=self.run_type_id
                                )

class Measurement:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.phenomenon_id = kwargs.get("phenomenon_id")
        self.phenomenon = runoffdb.phenomena[self.phenomenon_id]
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
        """
        TODO: THIS METHOD NEEDS TO BE ADJUSTED AFTER THE TARGET-SOURCE BUG IS REPAIRED IN THE DATABASE
        :return:
        """
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"""
                    SELECT r.*,
                           m.`phenomenon_id` AS phenomenon_id,
                           GROUP_CONCAT(rr.record_target) AS source_ids
                    FROM {self.runoffdb.records_table} r
                    JOIN {self.runoffdb.measurements_table} m
                         ON m.`id` = r.`measurement_id`
                    LEFT JOIN {self.runoffdb.record_record_table} rr
                         ON rr.record_source = r.`id`
                    WHERE r.`measurement_id` = {self.id}
                    GROUP BY r.`id`
                """
                thecursor.execute(query)
                results = thecursor.fetchall()
                thecursor.close()

                if len(results) == 0:
                    return []
                else:
                    rcrds = []
                    for r in results:
                        # parse source_ids into a Python list
                        if r["source_ids"] is None:
                            r["source_ids"] = []
                        else:
                            r["source_ids"] = [int(x) for x in r["source_ids"].split(",") if x]

                        new = Record(self.runoffdb, **r)
                        rcrds.append(new)
                    self.records = rcrds
                    return rcrds

    def get_records(self,
                    unit_id=None,
                    record_type_id=None,
                    related_value_x_unit_id=None,
                    related_value_y_unit_id=None,
                    related_value_z_unit_id=None,
                    exclude_missing_records=False):
        """
        The core method to obtain records.

        :param unit_id: main unit of the record to be found
        :param record_type_id: the record type to be found
        :param related_value_x_unit_id: related value X unit id
        :param related_value_y_unit_id: related value Y unit id
        :param related_value_z_unit_id: related value Z unit id
        :return:
        """
        if self.records is not None:
            out = []
            # print(f"requested unit_id: {unit_id}, record_type: {record_type_id}")
            for rec in self.records:
                # print(f"meas_id: {self.id}, rec_id: {rec.id}, unit_id: {rec.unit_id}")
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
                                if exclude_missing_records and rec.record_type_id == 99:
                                    continue
                                else:
                                    out.append(rec)
                            else:
                                if rec.record_type_id == record_type_id:
                                    out.append(rec)

            # filter by the related values units
            if related_value_x_unit_id is not None:
                out = [o for o in out if o.related_value_x_unit_id == related_value_x_unit_id]
            if related_value_y_unit_id is not None:
                out = [o for o in out if o.related_value_y_unit_id == related_value_y_unit_id]
            if related_value_z_unit_id is not None:
                out = [o for o in out if o.related_value_z_unit_id == related_value_z_unit_id]

            # return None if the resulting list should be empty
            return out or None
        else:
            # the records were not loaded yet, try loading them
            self.records = self.load_records()
            if self.records:
                return self.get_records(unit_id, record_type_id, related_value_x_unit_id, related_value_y_unit_id, related_value_z_unit_id)
            else:
                return None

    def get_metadata(self, lang="en"):
        meta = {"measurement ID": self.id,
                "phenomenon": self.phenomenon.name[lang]}
        meta.update({"date": self.date.strftime('%Y-%m-%d')}) if self.date is not None else None
        meta.update({"description": self.description[lang]}) if self.description[lang] is not None else None
        meta.update({"note": self.note[lang]}) if self.note[lang] is not None else None



        if self.get_records() is not None:
            rmeta = []
            for rec in self.get_records():
                rmeta.append(rec.get_metadata())
            if len(rmeta) > 0:
                meta.update({"records": rmeta})
            else:
                meta.update({"records": "no records found"})
        else:
            meta.update({"records": "no records found"})
        return meta

    def get_notes(self, lang="en"):
        notes = {}
        if self.note[lang]:
            notes[self.id] = self.note[lang]
            if self.get_records():
                rnotes = {}
                for r in self.get_records():
                    if r.get_notes(lang):
                        rnotes[r.id] = r.get_notes(lang)

                if len(rnotes) > 0:
                    notes["records"] = rnotes
        return notes

class Record:

    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.measurement_id = kwargs.get("measurement_id")
        self.record_type_id = kwargs.get("record_type_id")
        self.record_type = self.runoffdb.record_types[self.record_type_id]
        self.unit_id = kwargs.get("unit_id")
        self.unit = self.runoffdb.units[self.unit_id]
        self.phenomenon_id = kwargs.get("phenomenon_id")
        self.phenomenon = self.runoffdb.phenomena[self.phenomenon_id]
        self.related_value_x_unit_id = kwargs.get("related_value_xunit_id")
        self.unit_rel_x = self.runoffdb.units[self.related_value_x_unit_id] if self.related_value_x_unit_id is not None else None
        self.related_value_y_unit_id = kwargs.get("related_value_yunit_id")
        self.unit_rel_y = self.runoffdb.units[self.related_value_y_unit_id] if self.related_value_y_unit_id is not None else None
        self.related_value_z_unit_id = kwargs.get("related_value_zunit_id")
        self.unit_rel_z = self.runoffdb.units[self.related_value_z_unit_id] if self.related_value_z_unit_id is not None else None
        self.quality_index_id = kwargs.get("quality_index_id")
        self.quality_index = self.runoffdb.quality_index[self.quality_index_id ] if self.quality_index_id is not None else None
        self.is_timeline = kwargs.get("is_timeline")
        self.source_ids = kwargs.get("source_ids")
        self.methodics_id = kwargs.get("methodics_id")
        self.methodics = self.runoffdb.methodics[self.methodics_id ] if self.methodics_id is not None else None

        self.note_cz = kwargs.get("note_cz")
        self.note_en = kwargs.get("note_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.note = {"cz": self.note_cz, "en": self.note_en}

        self.data = None


        if multipliers.get(self.unit_id):
            self.multiplier_to_SI = multipliers.get(self.unit_id)
        else:
            self.multiplier_to_SI = None

    def __str__(self):
        string = f"record #{self.id}: unit #{self.unit_id}, record type {self.record_type_id}, quality index {self.quality_index_id}"
        string += f", related value x unit #{self.related_value_x_unit_id}" if self.related_value_x_unit_id else ""
        string += f", related value y unit #{self.related_value_y_unit_id}" if self.related_value_y_unit_id else ""
        string += f", related value z unit #{self.related_value_z_unit_id}" if self.related_value_z_unit_id else ""

        return string

    def show_details(self, indent="", t=". ", lang="en"):
        indent += t
        print("\n"+indent+f"record_id: {self.id}")
        indent += t
        print(indent+f"record_type_id: {self.record_type_id} ({self.runoffdb.record_types[self.record_type_id].name[lang]})")
        print(indent+f"unit_id: {self.unit_id} ({self.unit.name[lang]} [{self.unit.unit}])")
        if self.related_value_x_unit_id:
            print(indent+f"related_value_x_unit_id: {self.related_value_x_unit_id} ({self.unit_rel_x.name[lang]} [{self.unit_rel_x.unit}])")
        if self.related_value_y_unit_id:
            print(indent+f"related_value_y_unit_id: {self.related_value_y_unit_id} ({self.unit_rel_y.name[lang]} [{self.unit_rel_y.unit}])")
        if self.related_value_z_unit_id:
            print(indent+f"related_value_z_unit_id: {self.related_value_z_unit_id} ({self.unit_rel_z.name[lang]} [{self.unit_rel_z.unit}])")
        if self.quality_index_id:
            print(indent+f"quality_index_id: {self.quality_index_id} ({self.runoffdb.quality_index[self.quality_index_id].name[lang]})")
        print(indent+f"is_timeline: {'yes' if self.is_timeline else 'no'}")

        if self.source_ids:
            print(indent+f"source records: {', '.join([str(sr) for sr in self.source_ids])}")

        return

    def show_units(self, lang="en", indent=0):
        con = '-' if self.is_timeline else u"\u00B7"
        print(indent*"\t"+f"{self.unit_id} {con} {self.unit.name[lang]} [{self.unit.unit}]")
        print(indent*"\t"+f"\tx: {self.related_value_x_unit_id} - {self.unit_rel_x.name[lang]} [{self.unit_rel_x.unit}]") if self.related_value_x_unit_id else None
        print(indent*"\t"+f"\ty: {self.related_value_y_unit_id} - {self.unit_rel_y.name[lang]} [{self.unit_rel_y.unit}]") if self.related_value_y_unit_id else None
        print(indent*"\t"+f"\tz: {self.related_value_z_unit_id} - {self.unit_rel_z.name[lang]} [{self.unit_rel_z.unit}]") if self.related_value_z_unit_id else None
        return

    def load_data(self, value_name, related_x=None, related_y=None, related_z=None, index_column=None, order_by=None):
        more = ""

        if self.related_value_x_unit_id is not None:
            more += ", `related_value_x`"
            if related_x is not None:
                more += f" AS {related_x} "
            else:
                more += f" AS rel_value_x"

        if self.related_value_y_unit_id is not None:
            more += ", `related_value_y`"
            if related_y is not None:
                more += f" AS {related_y} "
            else:
                more += f" AS rel_value_y"

        if self.related_value_z_unit_id is not None:
            more += ", `related_value_z`"
            if related_z is not None:
                more += f" AS {related_z} "
            else:
                more += f" AS rel_value_z"

        if order_by is not None:
            order_by = f" ORDER BY `{order_by}` ASC"
        elif self.is_timeline:
            order_by = f" ORDER BY `time` ASC"
        else:
            order_by = ""

        select_time = "`time`, " if self.is_timeline else ""
        query = f"SELECT {select_time}`value` AS {value_name} {more} FROM {RunoffDB.data_table} WHERE `record_id` = {self.id}{order_by}"



        with self.runoffdb.get_connection() as dbcon:
            result_dataFrame = pd.read_sql(query, dbcon)
            if self.is_timeline and index_column is None:
                result_dataFrame.set_index('time', inplace=True)
            elif index_column is not None:
                result_dataFrame.set_index(index_column, inplace=True)

            if not result_dataFrame.empty:
                self.data = result_dataFrame
            else:
                raise DataframeEmptyError(value_name, self.id)
            return result_dataFrame

    def get_data(self, value_name="value", related_x="rel_value_x", related_y="rel_value_y", related_z="rel_value_z", index_column=None, remove_last_zero=False, demand_timeline=False):
        """
        Return self.data if there were any data loaded.
        Tries loading the data if self.data is None.
        :param value_name: the label to be assigned to the column in output dataframe
        :param index_column: label of column to be used as index column
        :param remove_last_zero: Whether to remove last row if the value in the row is equal to 0
        :param demand_timeline: raises DataframeNotTimeIndexed exception if demand_timeline=True but the obtained data are not
        :return: pandas Dataframe object with data loaded from DB or None if the self.data is empty
        """
        if self.data is not None:
            if remove_last_zero:
                return remove_last_zero_row(self.data)
            else:
                if not self.data.empty:
                    if demand_timeline and not isinstance(self.data.index, pd.TimedeltaIndex):
                        raise DataframeNotTimeIndexed(self.id)
                    else:
                        return self.data
        else:
            # the data may not be loaded yet
            try:
                self.load_data(value_name=value_name, related_x=related_x, related_y=related_y, related_z=related_z, index_column=index_column)
            except DataframeEmptyError:
                raise

            return self.get_data(value_name, related_x, related_y, related_z, index_column, remove_last_zero, demand_timeline)

    def get_data_in_unit(self, target_unit_id, value_name="value", related_x="rel_value_x", related_y="rel_value_y", related_z="rel_value_z", index_column=None, remove_last_zero=False, demand_timeline=False, output_column_label=None):
        """
        Return self.data with 'value' multiplied by the unit's multiplier to SI (if there were any data loaded).
        Tries loading the data if self.data is None.

        :return: pandas Dataframe object with data loaded from DB or None if the self.data is empty, values in SI units
        """
        try:
            self.get_data(value_name, related_x, related_y, related_z,  index_column, remove_last_zero, demand_timeline)
        except DataframeEmptyError:
            raise

        if self.data is not None:
            # if not self.data.empty:
            if target_unit_id == self.unit_id:
                return self.data
            if target_unit_id is None:
                print(f"Unit ID for data retrieval was not specified - returning in original unit: {self.unit_id} ({self.unit.unit})!")
                return self.data
            output_column_label = output_column_label or value_name

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

    # def get_source_records(self):
    #     return self.runoffdb.get_record_source_records(self)

    def get_metadata(self, lang="en"):
        meta = {"record ID": self.id,
                "value name": self.unit.name[lang],
                "value unit": self.unit.unit
                }
        meta.update({"related value x name": self.unit_rel_x.name[lang],
                     "related value x unit": self.unit_rel_x.unit}) if self.related_value_x_unit_id is not None else None
        meta.update({"related value y name": self.unit_rel_y.name[lang],
             "related value y unit": self.unit_rel_y.unit}) if self.related_value_y_unit_id is not None else None
        meta.update({"related value z name": self.unit_rel_z.name[lang],
             "related value z unit": self.unit_rel_z.unit}) if self.related_value_z_unit_id is not None else None
        meta.update({"quality index": self.runoffdb.quality_index[self.quality_index_id].name[lang]}) if self.quality_index_id is not None else None

        meta.update({"record type": self.runoffdb.record_types[self.record_type_id].name[lang]})
        meta.update({"source records": [r for r in self.get_source_records()]}) if self.get_source_records() is not None else None

        return meta

    def get_notes(self, lang="en"):
        notes = {self.id: self.note[lang]}
        return notes

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

    # def load_unit(self):
    #     if self.unit_id:
    #         with self.runoffdb.dbcon.cursor(dictionary=True) as thecursor:
    #             query = f"SELECT * FROM {RunoffDB.units_table} WHERE `id` = {self.unit_id}"
    #             thecursor.execute(query)
    #             results = thecursor.fetchall()
    #             thecursor.close
    #
    #             if len(results) == 1:
    #                 unit = Unit(**results[0])
    #                 return unit
    #             elif len(results) == 0:
    #                 print(f"No unit with ID {self.unit_id} found.")
    #                 return None
    #             else:
    #                 print(f"More then one unit found for ID {self.unit_id} ... this really shouldn't happen.")
    #                 return None
    #     else:
    #         print(f"Record {self.id} doesn't have unit ID assigned.")
    #         return None



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
        self.crop = self.runoffdb.crops[self.crop_id] if self.crop_id is not None else None
        self.agrotechnology_id = kwargs.get("agrotechnology_id")
        self.agrotechnology = runoffdb.agrotechnologies[self.agrotechnology_id] if self.agrotechnology_id is not None else None
        self.established = kwargs.get("established")
        self.plot_width = kwargs.get("plot_width")
        self.plot_length = kwargs.get("plot_length")
        self.plot_slope = kwargs.get("plot_slope")
        self.protection_measure_ids, self.protection_measures = self.get_protection_measures()

        self.note = {"cz": kwargs.get("note_cz"), "en": kwargs.get("note_en")}

    def get_protection_measures(self):
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT `protection_measure_id` FROM {self.runoffdb.plot_protection_measures_table} WHERE `plot_id` = {self.id}"
                thecursor.execute(query)
                results = thecursor.fetchall()

                ids = []
                measures = []
                if thecursor.rowcount > 0:
                    for r in results:
                        ids.append(r["protection_measure_id"])
                        measures.append(self.runoffdb.protection_measures[r["protection_measure_id"]])
                    thecursor.close()
            return ids, measures

    def get_protection_measures_names(self, lang):
        return ", ".join([m.name[lang] for m in self.protection_measures]) if self.protection_measures else ""

    def get_last_run_datetime(self):
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor() as thecursor:
                query = f"SELECT max(`datetime`) FROM `run_group` JOIN `run` ON `run`.`run_group_id` = `run_group`.`id` " \
                        f"WHERE `run`.`plot_id` = {self.id}"
                # execute the query and fetch the results
                thecursor.execute(query)
                results = thecursor.fetchone()
                thecursor.close()

                if len(results) > 0:
                    return results[0]

            return None

    def days_since_seeding(self, datetime):
        # for the cultivated fallow always return 0 since it is assumed prepared right before the simulation
        if self.crop_id == 1:
            return None
        else:
            if self.agrotechnology is not None:
                return self.agrotechnology.days_since_seeding(datetime)
            else:
                print(f"\tplot {self.id} has no agrotechnology assigned")
                return None

    def days_since_last_operation(self):
        # for the cultivated fallow always return 0 since it is assumed prepared right before the simulation
        if self.crop_id == 1:
            return 0
        else:
            if self.agrotechnology is not None:
                return self.agrotechnology.days_since_last_operation(date)
            else:
                print(f"\tplot {self.id} has no agrotechnology assigned")
                return None

    def get_runs_on_plot(self):
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor() as thecursor:
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

    def get_metadata(self, lang="en"):
        meta = {"plot ID": self.id,
                "name": self.name,
                "established": self.established.strftime('%Y-%m-%d'),
                "plot length": self.plot_length,
                "plot length unit": "m",
                "plot width": self.plot_width,
                "plot width unit": "m",
                "slope steepness": self.plot_slope,
                "slope steepness unit": "%"}
        if self.soil_origin_locality_id is not None and self.soil_origin_locality_id != self.locality_id:
            meta.update({"soil origin locality": self.runoffdb.localities[self.soil_origin_locality_id].name})
        return meta

    def get_notes(self, lang="en"):
        return self.note[lang]
class Crop:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.crop_type_id = kwargs.get("crop_type_id")
        self.crop_type = self.runoffdb.crop_types[self.crop_type_id] if self.crop_type_id is not None else None
        self.crop_er_type_id = kwargs.get("croper_type_id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.variety = kwargs.get("variety")
        self.is_catch_crop = kwargs.get("is_catch_crop")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_metadata(self, lang="en"):
        meta = {"crop ID": self.id,
                "name": self.name[lang]
                }
        meta.update({"description": self.description[lang]}) if self.description[lang] is not None else None
        meta.update({"crop type": self.crop_type.name[lang]}) if self.crop_type is not None else None
        meta.update({"variety": self.variety}) if self.variety is not None else None
        meta.update({"is catch crop": "True" if self.is_catch_crop == 1 else "False"}) if self.is_catch_crop is not None else None
        return meta

class Agrotechnology:

    operations = None

    @classmethod
    def load_all_operations(cls, runoffdb):
        print("loading tillage operations to establish agrotechnologies ...\n")
        with runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {RunoffDB.operations_table}")
                results = thecursor.fetchall()
                thecursor.close()
                if len(results) > 0:
                    operations = {}
                    for r in results:
                        new = Operation(runoffdb, **r)
                        operations.update({new.id: new})
                Agrotechnology.operations = operations
                print(f"agrotechnical operations loaded ({len(operations)})")

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
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:

                query = f"SELECT `operation_id`, `date` FROM {RunoffDB.tillageseq_table} WHERE `agrotechnology_id` = {self.id}"
                # print(query)
                thecursor.execute(query)
                results = thecursor.fetchall()
                thecursor.close()

                if len(results) == 0:
                    print(f"\tno tillage sequence entry found for agrotechnology ID {self.id}")
                    return {}
                else:
                    sequence = {}
                    for r in results:
                        sequence.update({r["date"]: Agrotechnology.operations.get(r["operation_id"])})
                    return sequence

    def get_maximum_disturbance_level(self):
        if self.operation_sequence is None or self.operation_sequence == {}:
            return None
        return max([op.operation_intensity_id for op in self.operation_sequence.values()])

    def get_maximum_disturbance_depth(self):
        if self.operation_sequence is None or self.operation_sequence == {}:
            return None
        return max([op.operation_depth_m for op in self.operation_sequence.values()])


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

    def days_since_seeding(self, input_datetime):
        # check if the input is a datetime or a date
        if isinstance(input_datetime, datetime):
            # extract just the date if it's a datetime
            the_date = input_datetime.date()
        elif isinstance(input_datetime, date):
            the_date = input_datetime
        else:
            raise ValueError("The input must be a date or datetime object.")

        # sort the operation_sequence by date in reverse order (most recent first)
        for operation_date in sorted(self.operation_sequence.keys(), reverse=True):
            if operation_date <= the_date:
                # Check if the operation type is "seeding"
                if self.operation_sequence[operation_date].operation_type_id == 3:
                    # Return the number of days since the last seeding
                    return (the_date - operation_date).days
        # If no "seeding" operation was found, return None or a suitable value (e.g. -1)
        return None

    def days_since_last_operation(self, input_datetime):
        # check if the input is a datetime or a date
        if isinstance(input_datetime, datetime):
            # extract just the date if it's a datetime
            the_date = input_datetime.date()
        elif isinstance(input_datetime, date):
            the_date = input_datetime
        else:
            raise ValueError("The input must be a date or datetime object.")
        # Sort the operation_sequence by date in reverse order (most recent first)
        for operation_date in sorted(self.operation_sequence.keys(), reverse=True):
            if operation_date <= the_date:
                # Return the number of days since the last seeding
                return (the_date - operation_date).days
        # If no "seeding" operation was found, return None or a suitable value (e.g. -1)
        return None

    def get_metadata(self, lang="en"):
        meta = {"name": self.name[lang]}
        if self.description[lang] is not None:
            meta.update({"description": self.description[lang]})
        if self.note[lang] is not None:
            meta.update({"note": self.note[lang]})
        if self.operation_sequence is not None:
            if len(self.operation_sequence) > 0:
                sequence = {}
                for date, operation in self.operation_sequence.items():
                    sequence.update({date.strftime('%Y-%m-%d'): operation.get_metadata(lang)})
                meta.update({"operation sequence": sequence})
                meta.update({"maximum disturbance depth": self.get_maximum_disturbance_depth()})
                meta.update({"maximum disturbance intensity": self.runoffdb.operation_intensities[self.get_maximum_disturbance_level()].description[lang]})
        return meta
class Operation:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

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

    def get_metadata(self, lang="en"):
        meta = {"name": self.name[lang]}
        if self.description[lang] is not None:
            meta.update({"description": self.description[lang]})
        if self.machinery_type[lang] is not None:
            meta.update({"machinery type": self.machinery_type[lang]})
        meta.update({"operation type": self.runoffdb.operation_types[self.operation_type_id].description[lang],
                "operation depth": self.operation_depth_m,
                "operation depth unit": "m",
                "operation intensity": self.runoffdb.operation_intensities[self.operation_intensity_id].description[lang]})
        return meta

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

        self.organization = self.runoffdb.organizations[self.organization_id]
    def get_metadata(self, lang="en"):
        meta = {"name": self.name[lang]}
        if self.description is not None :
            meta.update({"description": self.description[lang],
                         "organization": self.organization.get_metadata()})
        return meta

class Organization:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name = kwargs["name"]
        self.contact_person = kwargs["contact_person"]
        self.contact_number = kwargs.get("contact_number")
        self.contact_email = kwargs.get("contact_email")
        self.name_code = kwargs.get("name_code")

    def get_metadata(self, lang="en"):
        meta = {"name": self.name}
        if self.contact_person is not None:
            if self.contact_person != "":
                meta.update({"contact person": self.contact_person})
                # add the contact info if contact person is in DB
                if self.contact_email is not None:
                    if self.contact_email != "":
                        meta.update({"contact email": self.contact_email})
                    else:
                        meta.update({"contact email": "NA"})

                if self.contact_number is not None:
                    if self.contact_number != "":
                        meta.update({"contact number": self.contact_number})
                    else:
                        meta.update({"contact number": "NA"})
            else:
                meta.update({"contact person": "NA"})
        else:
            meta.update({"contact person": "NA"})

        return meta

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

class OperationType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

class OperationIntensity:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

class Phenomenon:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.model_parameter_set_id = kwargs.get("model_parameter_set_id")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class RecordType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.model_parameter_set_id = kwargs.get("model_parameter_set_id")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class QualityIndex:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class AssignmentType:
    def __init__(self, **kwargs):

        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

class Method:

    processing_steps = None
    instruments = None

    @classmethod
    def load_all_processing_steps(cls, runoffdb):
        with runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {RunoffDB.processing_step_table}")
                results = thecursor.fetchall()
                thecursor.close()
                if len(results) > 0:
                    steps = {}
                    for r in results:
                        new = ProcessingStep(runoffdb, **r)
                        steps.update({new.id: new})
                Method.processing_steps = steps
            return steps

    @classmethod
    def load_all_instruments(cls, runoffdb):
        with runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # execute the query and fetch the results
                thecursor.execute(f"SELECT * FROM {RunoffDB.instruments_table}")
                results = thecursor.fetchall()
                thecursor.close()
                if len(results) > 0:
                    instruments = {}
                    for r in results:
                        new = Instrument(runoffdb, **r)
                        instruments.update({new.id: new})
                Method.instruments = instruments
            return instruments

    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        # the first Method instance induces the DB load of all processing steps and instruments
        if self.instruments is None:
            Method.load_all_instruments(runoffdb)
        if self.processing_steps is None:
            Method.load_all_processing_steps(runoffdb)

        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.processing_steps_sequence = self.get_processing_steps_sequence() # ordered list of processing steps included in the method
        # self.instruments_map = {} # mapping local indexes to instrument class instances

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_processing_steps_sequence(self):
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:

                query = f"SELECT `processing_step_id`, `sort` FROM {RunoffDB.methodics_processing_step_table} WHERE `methodics_id` = {self.id} ORDER BY `sort` ASC"
                # print(query)
                thecursor.execute(query)
                results = thecursor.fetchall()
                thecursor.close()

                if len(results) == 0:
                    print(f"\tno processing steps found for methodics ID {self.id}")
                    return []
                else:
                    sequence = []
                    for r in results:
                        sequence.append(Method.processing_steps.get(r["processing_step_id"]))
                    return sequence

    def show_details(self, lang="en", indent=""):
        print(f"Methodics {self.id} - {self.name[lang]}")
        print(f"{self.description[lang]}")
        print(f"\nprocessing steps:")
        for i, prs in enumerate(self.processing_steps_sequence, start=1):
            print(f"\t{i}")
            prs.show_details(lang, indent)

    def export_to_json(self, lang="en", include_ids=False):
        export = {}
        if include_ids:
            export["id"] = self.id
        export["name"] = self.name[lang]
        export["description"] = self.description[lang] if self.description[lang] else "NA"
        steps = {}
        for i, ps in enumerate(self.processing_steps_sequence, start=1):
            steps.update({i:ps.export_to_json(lang, include_ids)})
        # export["processing steps"] = [ps.export_to_json(lang, include_ids) for ps in self.processing_steps_sequence]
        export["processing steps"] = steps
        return export

class ProcessingStep:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.link = kwargs.get("link")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

        self.instruments = self.get_instruments()

    def get_instruments(self):
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:

                query = f"SELECT `instrument_id` FROM {RunoffDB.processing_step_instrument_table} WHERE `processing_step_id` = {self.id}"
                # print(query)
                thecursor.execute(query)
                results = thecursor.fetchall()
                thecursor.close()

                if len(results) == 0:
                    return []
                else:
                    instruments = []
                    for r in results:
                        instruments.append(Method.instruments.get(r["instrument_id"]))
                    return instruments

    def show_details(self, lang="en", indent=""):
        indent += "\t"
        print(f"{indent}{self.name[lang]}")
        print(f"{indent}{self.description[lang]}") if self.description[lang] else None
        if self.instruments:
            print(f"\n{indent}instruments used:")
            for instr in self.instruments:
                instr.show_details(lang, indent)
        print("\n")
    def export_to_json(self, lang="en", include_ids=False):
        export = {}
        if include_ids:
            export["id"] = self.id
        export["name"] = self.name[lang]
        export["description"] = self.description[lang] if self.description[lang] else "NA"
        export["instruments"] = [instr.export_to_json(lang, include_ids) for instr in self.instruments]
        return export

class Instrument:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.link = kwargs.get("link")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def show_details(self, lang="en", indent=""):
        indent += "\t"
        print(f"{indent}{self.name[lang]} - {self.description[lang]}")

    def export_to_json(self, lang="en", include_ids=False):
        export = {}
        if include_ids:
            export["id"] = self.id
        export["name"] = self.name[lang]
        export["description"] = self.description[lang] if self.description[lang] else "NA"

        return export
def remove_last_zero_row(df):
    """
    Removes last row of a dataframe if value is equal to 0
    :param df: pandas Dataframe to be processes
    :return: copy of a Dataframe with removed last row or the original Dataframe if last row's value != 0
    """
    # Check if the last row value is equal to 0
    if df.iloc[-1]['value'] == 0:
        # Remove the last row
        df = df.copy().drop(df.index[-1])
    return df

def czech_date(datetime):
    return f"{datetime.strftime('%d.').strip('0')}{datetime.strftime('%m.').strip('0')}{datetime.strftime('%Y')}"

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



def get_zero_time(dataframe, series_name):
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

def integrate_by_minutes(df, series_name, start_time = None, end_time = None, zero_time = None, extrapolate = None, interpolate=True):
    return integrate_by_time(df, series_name, start_time, end_time, zero_time, extrapolate, interpolate, 'minutes')


def integrate_by_time(df, series_name, start_time=None, end_time=None, zero_time=None, extrapolate=None, interpolate=True, time_unit='minutes'):
    # ensure dataframe is timedelta-indexed
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

    if start_time is None:
        start_time = zero_time if zero_time is not None else df.index[0]
    if end_time is None:
        end_time = df.index[-1]

    output_value = 0
    prev_time = None
    prev_value = None
    found_valid_value = False  # Track if any valid value was used

    for time in df.index:
        value = get_value_in_time(df, time, series_name, zero_time, extrapolate)

        if pd.isna(value):
            if prev_time is None:
                continue  # skip initial NaNs entirely now
            else:
                continue  # skip NaNs elsewhere too

        if prev_time is not None:
            found_valid_value = True

            if prev_time >= start_time and time <= end_time:
                if time == prev_time:
                    print(f"\nError: two consequent times are equal at {time}. Skipping.")
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

    return output_value if found_valid_value else np.nan

def integrate_data_series(df, series_name_in, series_name_out, interpolate=True, time_unit='minutes'):
    """
    Calculates discreet integral for all points of given 'series_name_in' from dataframe 'df' and stores the values in new series 'series_name_out'

    :param df:
    :param series_name_in: column name to process
    :param series_name_out: column name for the output integrated series
    :param interpolate: whether to interpolate between points in time series, if False stepwise integration is performed (value considered constant in each time interval)
    :param time_unit: unit of time to use for integration ('minutes', 'hours', 'seconds')
    :return:
    """
    # Ensure dataframe is time-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        print(type(df.index))
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    output_values = []

    for time in df.index:
        integral_value = integrate_by_time(df, series_name_in,  pd.Timedelta(seconds=0), time, interpolate=interpolate, time_unit=time_unit)

        # Store the integrated value
        output_values.append(integral_value)
    # Add the integrated values as a new column to the DataFrame
    df[series_name_out] = output_values

    return df

def get_value_in_time(df, timedelta, series_name, zero_time=None, interpolate=True, extrapolate=None):
    """
    Returns interpolated value of dataseries in time specified as timedelta

    :param timedelta: the time point at which to get the value
    :param series_name: column name of the series to be interpolated
    :param zero_time: presumed time of start of the series (value = 0)
    :param extrapolate: range of extrapolation specified as a multiplier of the last interval
    :param interpolate: whether to interpolate values if exact match not found
    :returns: interpolated/extrapolated value if available based on specified inputs, otherwise None
    """

    # ensure dataframe is time-indexed
    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("DataFrame index must be of type TimedeltaIndex.")

    first_time = df.index[0]
    last_time = df.index[-1]

    # if timedelta is in the index, return the exact value directly
    if timedelta in df.index:
        return df.loc[timedelta, series_name]

    # if timedelta is before the first value
    if timedelta < first_time:
        # if the zero time was specified, extrapolate to zero
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
                print(f"Data series '{series_name}' doesn't have enough values for extrapolation.\n")
                return None
        else:
            print(f"Requested timedelta is after last record of '{series_name}' data series and extrapolation was not requested.\n")
            return None

    # interpolate the output value if `interpolate=True`
    if interpolate:
        # reindex the dataframe to include the requested timedelta
        df_with_requested_time = df.reindex(df.index.union([timedelta]))
        # use linear interpolation
        interpolated_series = df_with_requested_time[series_name].interpolate(method='time')
        # retrieve the interpolated value
        interpolated_value = interpolated_series.loc[timedelta]
        return interpolated_value
    else:
        return None

def integrate_flow(df, value_col, duration_col, placement="start",
                   interpolate=True, time_unit='minutes'):
    import pandas as pd

    if not isinstance(df.index, pd.TimedeltaIndex):
        raise ValueError("Index must be TimedeltaIndex.")

    time_conv = {'seconds': 1, 'minutes': 60, 'hours': 3600}
    if time_unit not in time_conv:
        raise ValueError(f"Invalid time_unit: {time_unit}")

    # compute instantaneous flow rates (L/s)
    flow_rate = df[value_col] / df[duration_col]
    times = df.index
    dur = pd.to_timedelta(df[duration_col], unit='s')

    if placement == "start":
        new_times = pd.Index(times)
        new_flow = pd.Series(flow_rate.values, index=new_times)

    elif placement == "sample_mid":
        new_times = pd.Index(times + dur / 2)
        new_flow = pd.Series(flow_rate.values, index=new_times)
        new_times = pd.Index([times[0]]).append(new_times)
        new_flow = pd.concat([pd.Series([0.0], index=[times[0]]), new_flow])

    elif placement == "sample_end":
        new_times = pd.Index(times + dur)
        new_flow = pd.Series(flow_rate.values, index=new_times)
        new_times = pd.Index([times[0]]).append(new_times)
        new_flow = pd.concat([pd.Series([0.0], index=[times[0]]), new_flow])

    elif placement == "interval_mid":
        t_series = times.to_series()
        mid_times = t_series + (t_series.shift(-1) - t_series) / 2
        mid_times = mid_times.iloc[:-1]
        new_times = pd.Index(mid_times)
        new_flow = pd.Series(flow_rate.iloc[:-1].values, index=new_times)
        new_times = pd.Index([times[0]]).append(new_times)
        new_flow = pd.concat([pd.Series([0.0], index=[times[0]]), new_flow])

    else:
        raise ValueError(f"Unknown placement: {placement}")

    adj_df = pd.DataFrame({'flow_rate': new_flow.values}, index=new_times).sort_index()

    # integrate to discharge
    discharge = []      # per-interval volume
    cum_discharge = []  # running total
    total = 0
    prev_t, prev_f = None, None
    for t, f in adj_df['flow_rate'].items():
        if prev_t is not None:
            dt = (t - prev_t).total_seconds() / time_conv[time_unit]
            if interpolate:
                disch = 0.5 * (prev_f + f) * dt
            else:
                disch = prev_f * dt
        else:
            disch = 0
        total += disch
        discharge.append(disch)
        cum_discharge.append(total)

        prev_t, prev_f = t, f

    adj_df['discharge'] = discharge
    adj_df['cum_discharge'] = cum_discharge
    return adj_df