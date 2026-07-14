# -*- coding: utf-8 -*-

# column name constants
DB_1ST_DIM_VALUE_COLUMN = "value"
DB_2ND_DIM_VALUE_COLUMN = "related_value_x"
DB_3RD_DIM_VALUE_COLUMN = "related_value_y"
DB_4TH_DIM_VALUE_COLUMN = "related_value_z"
DB_TIME_COLUMN = "time"


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
        self.unit_rel_x = self.runoffdb.units[self.related_value_x_unit_id] \
            if self.related_value_x_unit_id is not None else None
        self.related_value_y_unit_id = kwargs.get("related_value_yunit_id")
        self.unit_rel_y = self.runoffdb.units[self.related_value_y_unit_id] \
            if self.related_value_y_unit_id is not None else None
        self.related_value_z_unit_id = kwargs.get("related_value_zunit_id")
        self.unit_rel_z = self.runoffdb.units[self.related_value_z_unit_id] \
            if self.related_value_z_unit_id is not None else None
        self.quality_index_id = kwargs.get("quality_index_id")
        self.quality_index = self.runoffdb.quality_index[self.quality_index_id] \
            if self.quality_index_id is not None else None
        self.is_timeline = kwargs.get("is_timeline")
        self.source_ids = kwargs.get("source_ids")
        self.methodics_id = kwargs.get("methodics_id")
        self.methodics = self.runoffdb.methodics[self.methodics_id] if self.methodics_id is not None else None

        self.note_cz = kwargs.get("note_cz")
        self.note_en = kwargs.get("note_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.note = {"cz": self.note_cz, "en": self.note_en}

        self._data = None  # dataframe cache
        self._data_loaded = False

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

    def get_data(
            self,
            *,
            value_label="value",
            related_x_label="rel_value_x",
            related_y_label="rel_value_y",
            related_z_label="rel_value_z",
            index_column=None,
            order_by=None,
    ):
        if not self._data_loaded:
            self._data = self.runoffdb.load_record_data(
                self,
                value_label=value_label,
                related_x_label=related_x_label,
                related_y_label=related_y_label,
                related_z_label=related_z_label,
                index_column=index_column,
                order_by=order_by,
            )
            self._data_loaded = True

        return self._data.copy()

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
        meta.update({"source records ids": [r for r in self.source_ids]}) if self.source_ids is not None else None

        return meta

    def get_notes(self, lang="en"):
        notes = {self.id: self.note[lang]}
        return notes
