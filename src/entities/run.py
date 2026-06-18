# -*- coding: utf-8 -*-
from ..setup.entity_ids import *

from ..processing.resolution.field import resolve_translated_field

from ..utilities.utilities import *
from src.filters.run_filter import RunFilter
from ..entities.soil_sample import SoilSample
from ..entities.data_owners import MeasurementOwner, RecordOwner


class Run(MeasurementOwner, RecordOwner):
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


    def get_soil_samples(self) -> list[SoilSample]:
        """
        Return all soil samples directly associated with this run.
        """
        return self.runoffdb.get_samples_of_run(self)

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

    def get_crop_condition(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="crop_condition",
            values=self.crop_condition,
            lang=lang,
            source="Run.get_crop_condition",
        )

    def get_note(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="note",
            values=self.note,
            lang=lang,
            source="Run.get_note",
        )

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

