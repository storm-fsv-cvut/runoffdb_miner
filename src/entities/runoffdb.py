# -*- coding: utf-8 -*-
from ..run_filter import RunFilter
from ..db_access import DBconnector

from ..entities.run import Run
from ..entities.record import Record
from ..entities.soil_sample import SoilSample
from ..entities.type_entities import *
import os

class RunoffDB:

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

        self._run_cache: dict[int, Run] = {}

        print("\n... everything is ready.")
        print(80*"="+"\n")


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # write the log
        self.save_log()

    def get_connection(self):
        return self.dbcon.pool.get_connection()

    def load_runs(self, query: RunFilter) -> dict[int, 'Run']:

        simulators = as_list(query.simulators)
        localities = as_list(query.localities)
        crops = as_list(query.crops)
        run_id = as_list(query.run_id)

        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as cur:

                sql = f"""
                    SELECT
                        r.`id` AS run_id,
                        r.`runoff_start` AS ttr,
                        r.`init_moisture_id` AS initmoist_recid,
                        r.`surface_cover_id` AS surface_cover_recid,
                        r.`rain_intensity_id` AS rainfall_recid,
                        r.`soil_sample_bulk_id` AS bulkd_ss_id,
                        r.`bulk_assignment_type_id` AS bulkd_ss_asstype,
                        r.`soil_sample_texture_id` AS texture_ss_id,
                        r.`texture_assignment_type_id` AS texture_ss_asstype,
                        r.`soil_sample_corg_id` AS corg_ss_id,
                        r.`corg_assignment_type_id` AS corg_ss_asstype,
                        r.`crop_bbch` AS bbch,
                        r.`note_cz`,
                        r.`note_en`,
                        r.`crop_condition_cz`,
                        r.`crop_condition_en`,
                        r.`reference_run_id`,
                        r.`run_group_id` AS run_group_id,
                        rg.`sequence_id`,
                        rg.`datetime`,
                        rg.`run_type_id`,
                        s.`simulator_id`,
                        p.`locality_id`,
                        p.`id` AS plot_id,
                        p.`crop_id`,
                        c.`crop_type_id`
                    FROM {runs_table} r
                    JOIN {run_groups_table} rg ON r.`run_group_id` = rg.`id`
                    JOIN {sequences_table} s ON rg.`sequence_id` = s.`id`
                    JOIN {plots_table} p ON r.`plot_id` = p.`id`
                    JOIN {crops_table} c ON p.`crop_id` = c.`id`
                    WHERE (s.`deleted` = 0 OR s.`deleted` IS NULL)
                """

                if query.with_runoff_only:
                    sql += " AND r.`runoff_start` IS NOT NULL"

                if run_id:
                    sql += f" AND r.`id` IN ({', '.join(map(str, run_id))})"
                else:
                    if query.date_from:
                        sql += f" AND rg.`datetime` >= '{query.date_from}'"
                    if query.date_to:
                        sql += f" AND rg.`datetime` <= '{query.date_to}'"
                    if simulators:
                        sql += f" AND s.`simulator_id` IN ({', '.join(map(str, simulators))})"
                    if localities:
                        sql += f" AND p.`locality_id` IN ({', '.join(map(str, localities))})"
                    if crops:
                        sql += f" AND p.`crop_id` IN ({', '.join(map(str, crops))})"

                sql += " ORDER BY rg.`datetime` ASC"

                if query.limit:
                    sql += f" LIMIT {query.limit}"

                cur.execute(sql)
                rows = cur.fetchall()

        runs: dict[int, Run] = {}
        for row in rows:
            rid = row["run_id"]
            if rid in self._run_cache:
                run = self._run_cache[rid]
            else:
                run = Run(self, **row)
                self._run_cache[rid] = run

            runs[rid] = run

        return runs


    def load_plots(self, id=None):
        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # start of the query
                query = f"SELECT * FROM {plots_table}"
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
                query = f"SELECT * FROM {soil_samples_table}"
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
                query = f"SELECT * FROM {simulators_table}"
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
                query = f"SELECT * FROM {organizations_table}"
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
                query = f"SELECT * FROM {localities_table}"
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
                query = f"SELECT * FROM {run_types_table}"

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
                query = f"SELECT * FROM {crop_types_table}"

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
                query = f"SELECT * FROM {operation_types_table}"

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
                query = f"SELECT * FROM {operation_intensities_table}"

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
                query = f"SELECT * FROM {protection_measures_table}"

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
                thecursor.execute(f"SELECT * FROM {units_table}")
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
                thecursor.execute(f"SELECT * FROM {projects_table}")
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
                thecursor.execute(f"SELECT * FROM {crops_table}")
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
                thecursor.execute(f"SELECT * FROM {agrotechnologies_table}")
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
                query = f"SELECT * FROM {phenomena_table}"

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
                query = f"SELECT * FROM {record_types_table}"

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
                query = f"SELECT * FROM {quality_index_table}"

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
                query = f"SELECT * FROM {assignmenttypes_table}"

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
                thecursor.execute(f"SELECT * FROM {methodics_table}")
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
                query = f"SELECT {records_table}.*, {measurements_table}.`phenomenon_id` AS phenomenon_id " \
                        f"FROM {records_table} " \
                        f"JOIN {measurements_table} ON {measurements_table}.`id` = {records_table}.`measurement_id` " \
                        f"WHERE {records_table}.`id` = {record_id}"
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
        from datetime import datetime, time

        with self.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                # start of the query
                query = f"SELECT DISTINCT DATE({run_groups_table}.`datetime`) AS datetime " \
                        f"FROM {runs_table} " \
                        f"JOIN {run_groups_table} ON {runs_table}.`run_group_id` = {run_groups_table}.`id` " \
                        f"JOIN {sequences_table} ON {run_groups_table}.`sequence_id` = {sequences_table}.`id` " \
                        f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) "
                if date_from is not None:
                    query += f" AND {run_groups_table}.`datetime` >= '{date_from}'"
                if date_to is not None:
                    query += f" AND {run_groups_table}.`datetime` <= '{date_to}'"

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

    def show_plots(self, lang="en"):
        for plot in self.plots.values():
            plot.show_properties(lang=lang)
        return

    def show_simulators(self, lang="en"):
        for simulator in self.simulators.values():
            simulator.show_properties(lang=lang)

        return

    def show_methodics(self, lang="en"):
        for meth in self.methodics.values():
            meth.show_details(lang)

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
                query = f"SELECT {runs_table}.`id` AS run_id, " \
                        f"{run_groups_table}.`id` AS group_id, " \
                        f"{run_groups_table}.`sequence_id` AS sequence_id " \
                        f"FROM {runs_table} " \
                        f"JOIN {run_groups_table} ON {runs_table}.`run_group_id` = {run_groups_table}.`id` " \
                        f"JOIN {sequences_table} ON {run_groups_table}.`sequence_id` = {sequences_table}.`id` " \
                        f"WHERE `runoff_start` IS NOT NULL AND (`deleted` = 0 OR `deleted` IS NULL) " \
                        f"AND {run_groups_table}.`datetime` = '{datetime}'"

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
