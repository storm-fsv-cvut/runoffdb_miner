# -*- coding: utf-8 -*-
from sqlalchemy import create_engine

from ..run_filter import RunFilter

from ..entities.run import Run
from ..entities.record import Record
from ..entities.measurement import Measurement
from ..entities.soil_sample import SoilSample
from ..entities.type_entities import *
import os

from sqlalchemy import text
from typing import Iterable, Mapping, Any

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

        host = os.environ["RUNOFFDB_HOST"]
        db = os.environ["RUNOFFDB_NAME"]
        user = os.environ["RUNOFFDB_USER"]
        pwd = os.environ["RUNOFFDB_PASSWORD"]

        try:
            self.engine = create_engine(
                f"mysql+pymysql://{user}:{pwd}@{host}/{db}",
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True
            )
        except ConnectionRefusedError:
            print(f"\033[91mConnection to database is not available.\nMake sure your database server is running.\033[00m\n")

        # cache for special entities
        self._run_cache: dict[int, Run] = {}
        self._soil_samples_cache: dict[int: SoilSample] = {}

        # on initiation load all the entities that are used all the time
        self.run_types = self.load_run_types()
        self.crop_types = self.load_crop_types()
        self.agrooperations = self.load_agrooperations()
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
        self.instruments = self.load_instruments()
        self.processing_steps = self.load_processing_steps()
        self.methodics = self.load_methodics()
        self.projects = self.load_projects()
        self.phenomena = self.load_phenomena()
        self.record_types = self.load_record_types()
        self.quality_index = self.load_quality_index()
        self.assignment_types = self.load_assignment_types()


        print("\n... everything is ready.")
        print(80*"="+"\n")


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # write the log
        # self.save_log()
        return


    def get_connection(self):
        return self.engine.connect()

    def fetch_all(self, sql: str, **params) -> list[dict[str, Any]]:
        """
        Execute a SELECT query and return all rows as list of dicts.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params)
            return [dict(row._mapping) for row in result]

    def fetch_one(self, sql: str, **params) -> dict[str, Any] | None:
        """
        Execute a SELECT query and return a single row as a dict or None.
        SQLAlchemy 1.4+ / 2.0 compatible.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params).first()
            if result is None:
                return None
            return dict(result._mapping)

    def load_runs(self, query: RunFilter) -> dict[int, 'Run']:

        simulators = as_list(query.simulators)
        localities = as_list(query.localities)
        crops = as_list(query.crops)
        run_id = as_list(query.run_id)

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

        results = self.fetch_all(sql)


        runs: dict[int, Run] = {}
        for row in results:
            rid = row["run_id"]
            if rid in self._run_cache:
                run = self._run_cache[rid]
            else:
                run = Run(self, **row)
                self._run_cache[rid] = run

            runs[rid] = run

        return runs

    def get_group_brothers_ids(self, run: Run):
        """
        Returns a list of IDs of simulation runs from the same group
        :return:
        """
        query = f"SELECT `run`.`id` FROM {runs_table} WHERE `run_group_id` = {run.id}"
        results = self.fetch_all(query)

        brothers = []
        if len(results) > 0:
            for r in results:
                if r['id'] != run.id:
                    brothers.append(r['id'])
            return brothers

    def get_project_ids(self, run: Run):
        query = f"SELECT `project_id` FROM `sequence_project` WHERE `sequence_id` = {run.sequence_id}"
        results = self.fetch_all(query)

        ids = []
        if len(results) > 0:
            for r in results:
                ids.append(r['id'])
        return ids

    def load_plots(self):

        results = self.fetch_all(f"SELECT * FROM {plots_table}")

        plot_dict = {}
        for r in results:
            new = Plot(self, **r)
            plot_dict.update({new.id: new})

        print(f"plots loaded ({len(plot_dict)})")
        return plot_dict

    def load_samples(self):
        query = f"SELECT * FROM {soil_samples_table}"
        query += " ORDER BY `id` ASC"

        results = self.fetch_all(query)

        samples: dict[int, SoilSample] = {}
        for row in results:
            ssid = row["id"]
            if ssid in self._soil_samples_cache:
                sample = self._soil_samples_cache[ssid]
            else:
                sample = SoilSample(self, **row)
                self._soil_samples_cache[ssid] = sample

            samples[ssid] = sample

        return samples


    def load_simulators(self):
        query = f"SELECT * FROM {simulators_table} ORDER BY `id` ASC"
        results = self.fetch_all(query)
        simulators = {}
        for r in results:
            new = Simulator(self, **r)
            simulators.update({new.id: new})
        print(f"simulators loaded ({len(simulators)})")
        return simulators

    def load_organizations(self):
        query = f"SELECT * FROM {organizations_table}"
        query += " ORDER BY `id` ASC"
        results = self.fetch_all(query)

        organizations = {}
        for r in results:
            new = Organization(self, **r)
            organizations.update({new.id: new})
        print(f"organizations loaded ({len(organizations)})")
        return organizations

    def load_localities(self):
        query = f"SELECT * FROM {localities_table}"

        query += " ORDER BY `id` ASC"
        results = self.fetch_all(query)

        localities = {}
        for r in results:
            new = Locality(self, **r)
            localities.update({new.id: new})
        print(f"localities loaded ({len(localities)})")
        return localities

    def load_run_types(self):
        """
        Loads all run types from the database into a dictionary keyed by run type ID.
        """
        results = self.fetch_all(f"SELECT * FROM {run_types_table}")

        run_types = {}
        for r in results:
            new = RunType(**r)
            run_types[new.id] = new

        if run_types:
            print("run types loaded")

        return run_types


    def load_crop_types(self):

        results = self.fetch_all(f"SELECT * FROM {crop_types_table}")

        crop_types = {}

        for r in results:
            new = CropType(**r)
            crop_types.update({new.id: new})

        print(f"crop types loaded")
        return crop_types

    def load_operation_types(self):
        results = self.fetch_all(f"SELECT * FROM {operation_types_table}")

        op_types = {}
        for r in results:
            new = OperationType(**r)
            op_types.update({new.id: new})
        print(f"operation types loaded")
        return op_types

    def load_operation_intensities(self):
        results = self.fetch_all(f"SELECT * FROM {operation_intensities_table}")

        op_ints = {}
        for r in results:
            new = OperationIntensity(**r)
            op_ints.update({new.id: new})
        print(f"operation intensities loaded")
        return op_ints

    def load_agrooperations(self):
        print("\nloading tillage operations to establish agrotechnologies ...")

        results = self.fetch_all(f"SELECT * FROM {operations_table}")
        operations = {}
        for r in results:
            new = Operation(self, **r)
            operations.update({new.id: new})
        Agrotechnology.operations = operations
        print(f"agrotechnical operations loaded ({len(operations)})")

        return operations

    def get_operation_sequence(self, agrotechnology_id):
        query = f"SELECT `operation_id`, `date` FROM {tillageseq_table} WHERE `agrotechnology_id` = {agrotechnology_id} ORDER BY `date` ASC"
        results = self.fetch_all(query)

        if len(results) == 0:
            print(f"\tno tillage sequence entry found for agrotechnology ID {agrotechnology_id}")
            return {}
        else:
            sequence = {}
            for r in results:
                sequence.update({r["date"]: self.agrooperations.get(r["operation_id"])})
            return sequence

    def load_processing_steps(self):
        results = self.fetch_all(f"SELECT * FROM {processing_step_table}")

        steps = {}
        for r in results:
            new = ProcessingStep(self, **r)
            steps.update({new.id: new})
        return steps

    def load_instruments(self):
        results = self.fetch_all(f"SELECT * FROM {instruments_table}")

        instruments = {}
        for r in results:
            new = Instrument(self, **r)
            instruments.update({new.id: new})
        return instruments

    def get_processing_steps_sequence(self, methodics_id):
        query = f"SELECT `processing_step_id`, `sort` FROM {methodics_processing_step_table} WHERE `methodics_id` = {methodics_id}"
        results = self.fetch_all(query)

        if len(results) == 0:
            print(f"\tno processing steps found for methodics ID {methodics_id}")
            return []
        else:
            sequence = []
            for r in results:
                sequence.append(self.processing_steps.get(r["processing_step_id"]))
            return sequence

    def get_instruments(self, step_id):
        query = f"SELECT `instrument_id` FROM {processing_step_instrument_table} WHERE `processing_step_id` = {step_id}"
        results = self.fetch_all(query)

        instruments = []
        for r in results:
            instruments.append(self.instruments.get(r["instrument_id"]))
        return instruments


    def load_protection_measures(self):
        results = self.fetch_all(f"SELECT * FROM {protection_measures_table}")

        out_dict = {}
        for r in results:
            new = ProtectionMeasure(**r)
            out_dict.update({new.id: new})
        print(f"soil protection measures loaded ({len(out_dict)})")
        return out_dict

    def get_protection_measures(self, plot_id):
        query = f"SELECT `protection_measure_id` FROM {plot_protection_measures_table} WHERE `plot_id` = {plot_id}"
        results = self.fetch_all(query)

        ids = []
        measures = []
        for r in results:
            ids.append(r["protection_measure_id"])
            measures.append(self.protection_measures[r["protection_measure_id"]])

        return ids, measures

    def load_units(self):
        results = self.fetch_all(f"SELECT * FROM {units_table}")
        units = {}
        for r in results:
            new_unit = Unit(**r)
            units.update({new_unit.id: new_unit})
        print(f"units loaded ({len(units)})")
        return units

    def load_projects(self):
        results = self.fetch_all(f"SELECT * FROM {projects_table}")
        projects = {}
        for r in results:
            new_project = Project(**r)
            projects.update({new_project.id: new_project})
        print(f"projects loaded")
        return projects

    def load_crops(self):
        results = self.fetch_all(f"SELECT * FROM {crops_table}")
        crops = {}
        for r in results:
            new_crop = Crop(self, **r)
            crops.update({new_crop.id: new_crop})
        print(f"crops loaded ({len(crops)})")
        return crops

    def load_agrotechnologies(self):
        results = self.fetch_all(f"SELECT * FROM {agrotechnologies_table}")
        agrotechnologies = {}
        for r in results:
            new_agt = Agrotechnology(self, **r)
            agrotechnologies.update({new_agt.id: new_agt})
        print(f"agrotechnologies loaded ({len(agrotechnologies)})")
        return agrotechnologies

    def load_phenomena(self):
        results = self.fetch_all(f"SELECT * FROM {phenomena_table}")
        phenomena = {}
        for r in results:
            new = Phenomenon(**r)
            phenomena.update({new.id: new})
        print(f"phenomena loaded")
        return phenomena

    def load_record_types(self):
        results = self.fetch_all(f"SELECT * FROM {record_types_table}")
        record_types = {}
        for r in results:
            new = RecordType(**r)
            record_types.update({new.id: new})
        print(f"record types loaded")
        return record_types

    def load_quality_index(self):
        results = self.fetch_all(f"SELECT * FROM {quality_index_table}")
        quality_indices = {}
        for r in results:
            new = QualityIndex(**r)
            quality_indices.update({new.id: new})
        print(f"quality indexes loaded")
        return quality_indices

    def load_assignment_types(self):
        results = self.fetch_all(f"SELECT * FROM {assignmenttypes_table}")

        ats = {}
        for r in results:
            new = AssignmentType(**r)
            ats.update({new.id: new})
        print(f"assignment types loaded")
        return ats

    def load_methodics(self):
        results = self.fetch_all(f"SELECT * FROM {methodics_table}")
        methodics = {}
        for r in results:
            new = Method(self, **r)
            methodics.update({new.id: new})
        print(f"methodics loaded ({len(methodics)})")
        return methodics

    def load_measurements_of_run(self, run: Run):
        msrmsnts = {}
        query = f"SELECT * FROM {measurements_table} " \
                f"JOIN {measurement_run_table} ON {measurement_run_table}.`measurement_id` = {measurements_table}.`id` " \
                f"WHERE {measurement_run_table}.`run_id` = {run.id}"
        results = self.fetch_all(query)

        for res in results:
            new_measurement = Measurement(self, **res)
            msrmsnts.update({new_measurement.id: new_measurement})
        return msrmsnts

    def load_record_by_id(self, record_id):
        query = f"SELECT {records_table}.*, {measurements_table}.`phenomenon_id` AS phenomenon_id " \
                f"FROM {records_table} " \
                f"JOIN {measurements_table} ON {measurements_table}.`id` = {records_table}.`measurement_id` " \
                f"WHERE {records_table}.`id` = {record_id}"

        results = self.fetch_one(query)

        if len(results) > 0:
            return Record(self, **results)
        else:
            return None


    def load_records_of_measurement(self, measurement: Measurement):
        """
        TODO: THIS METHOD NEEDS TO BE ADJUSTED AFTER THE TARGET-SOURCE BUG IS REPAIRED IN THE DATABASE
        :return:
        """

        query = f"""
            SELECT r.*,
                   m.`phenomenon_id` AS phenomenon_id,
                   GROUP_CONCAT(rr.record_target) AS source_ids
            FROM {records_table} r
            JOIN {measurements_table} m
                 ON m.`id` = r.`measurement_id`
            LEFT JOIN {record_record_table} rr
                 ON rr.record_source = r.`id`
            WHERE r.`measurement_id` = {measurement.id}
            GROUP BY r.`id`
        """
        results = self.fetch_all(query)

        if len(results) == 0:
            return []
        else:
            rcrds = []
            for r in results:
                # parse source_ids into a list
                if r["source_ids"] is None:
                    r["source_ids"] = []
                else:
                    r["source_ids"] = [int(x) for x in r["source_ids"].split(",") if x]

                new = Record(self, **r)
                rcrds.append(new)
            return rcrds

    def get_simulation_days(self, date_from=None, date_to=None):
        """
        Returns list of datetime objects when a simulation was carried on.
        Time component of the datetime is set to 0:00:00.
        :param date_from: filter simulation days older than date_from
        :param date_to: filter simulation days younger than date_to
        :return:
        """
        from datetime import datetime, time

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

        results = self.fetch_all(query)

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
