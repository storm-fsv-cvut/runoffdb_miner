# -*- coding: utf-8 -*-
from ..setup.entity_ids import *
from ..setup.unit_ids import *
from ..setup.table_names import *

from ..utilities.utilities import *
from ..run_filter import RunFilter
from ..entities.measurement import Measurement

from src.services.record_resolution import get_best_record_of_unit

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
        self.brothers = self.runoffdb.get_group_brothers_ids(self)
        self.plot_id = kwargs["plot_id"]
        self.plot = runoffdb.plots[self.plot_id]
        self.locality_id = kwargs["locality_id"]
        self.locality = self.runoffdb.localities[self.locality_id]
        self.crop_id = kwargs["crop_id"]
        self.crop = runoffdb.crops[self.crop_id]
        self.crop_type_id = kwargs["crop_type_id"]
        self.reference_run_id = kwargs["reference_run_id"]

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


    def get_reference_run(self):
        if self.reference_run_id is None:
            return None
        else:
            # this should never happen
            ref_runs = self.runoffdb.load_runs(RunFilter(run_id=self.reference_run_id))
            if len(ref_runs) > 1:
                print(f"\n\033[91mRun #{self.id} got more than one reference run returned\033[00m")
            # get the run instance from the dict
            ref_run = list(ref_runs.values())[0]
            return ref_run

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

    # def load_measurements(self):
    #     msrmsnts = None
    #     with self.runoffdb.get_connection() as dbcon:
    #         with dbcon.cursor(dictionary=True) as thecursor:
    #             query = f"SELECT * FROM {measurements_table} " \
    #                     f"JOIN {measurement_run_table} ON {measurement_run_table}.`measurement_id` = {measurements_table}.`id` " \
    #                     f"WHERE {measurement_run_table}.`run_id` = {self.id}"
    #             # print(query)
    #             thecursor.execute(query)
    #             results = thecursor.fetchall()
    #             thecursor.close()
    #
    #             if len(results) == 0:
    #                 # print(f"\tNo measurement found for run {self.id}")
    #                 return {}
    #             else:
    #                 msrmsnts = {}
    #                 for res in results:
    #                     new_measurement = Measurement(self.runoffdb, **res)
    #                     msrmsnts.update({new_measurement.id: new_measurement})
    #     return msrmsnts

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
            self.measurements = self.runoffdb.load_measurements_of_run(self)
            if self.measurements:
                return self.get_measurements(phenomenon_id)
            else:
                return None
    #
    # def get_project_ids(self):
    #     ids = None
    #     with self.runoffdb.get_connection() as dbcon:
    #         with dbcon.cursor() as thecursor:
    #             # execute the query and fetch the results
    #             query = f"SELECT `project_id` FROM `sequence_project` WHERE `sequence_id` = {self.sequence_id}"
    #
    #             thecursor.execute(query)
    #             results = thecursor.fetchall()
    #             thecursor.close()
    #
    #             if len(results)> 0:
    #                 ids = []
    #                 for r in results:
    #                     ids.append(r[0])
    #     return ids
    #

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
                    data = rec.load_data("plot_x", index_column="plot_x")

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

        candidates = self.runoffdb.get_runs(date_from=self.datetime,
                               date_to=self.datetime,
                               localities=self.locality_id,
                               simulators=self.simulator_id,
                               crops=CULTIVATED_FALLOW_CROP_ID,
                               run_types=self.run_type_id)

        winners = []
        for r in candidates:
            if self.plot.plot_length == r.plot.plot_length and self.plot.plot_width == r.plot.plot_width:
                winners.append(r)

        return winners

