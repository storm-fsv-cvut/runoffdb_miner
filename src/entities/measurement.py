# -*- coding: utf-8 -*-
from collections.abc import Iterable

from ..utilities.utilities import *
from ..setup.entity_ids import *
from ..setup.table_names import *

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

    def show_details(self, indent="", t="- "):
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

    def get_records(
            self,
            unit_id: int | Iterable[int] | None = None,
            record_type_id: int | None = None,
            related_value_x_unit_id: int | Iterable[int] | None = None,
            related_value_y_unit_id: int | Iterable[int] | None = None,
            related_value_z_unit_id: int | Iterable[int] | None = None,
            exclude_missing_records: bool = False,
    ) -> list["Record"]:
        """
        The core method to obtain records.

        :param unit_id: main unit of the record to be found
        :param record_type_id: the record type to be found
        :param related_value_x_unit_id: related value X unit id
        :param related_value_y_unit_id: related value Y unit id
        :param related_value_z_unit_id: related value Z unit id
        :param exclude_missing_records: do not return records with type = MISSING_RECORD_TYPE_ID
        :return:
        """
        if self.records is not None:
            out = []

            if self.records is not None:
                out = []

                # normalize unit_id
                if unit_id is None:
                    allowed_units = None
                elif isinstance(unit_id, Iterable) and not isinstance(unit_id, (str, bytes)):
                    allowed_units = set(unit_id)
                else:
                    allowed_units = {unit_id}

                for rec in self.records:
                    if allowed_units is not None and rec.unit_id not in allowed_units:
                        continue

                    if record_type_id and rec.record_type_id != record_type_id:
                        continue

                    if exclude_missing_records and rec.record_type_id == MISSING_RECORD_TYPE_ID:
                        continue

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
            self.records = self.runoffdb.load_records_of_measurement(self)
            if self.records:
                return self.get_records(unit_id,
                                        record_type_id,
                                        related_value_x_unit_id,
                                        related_value_y_unit_id,
                                        related_value_z_unit_id)
            else:
                return []

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
