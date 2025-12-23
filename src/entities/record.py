# -*- coding: utf-8 -*-

from ..setup.unit_ids import *
from ..setup.table_names import *
from ..exceptions import DataframeEmptyError, DataframeNotTimeIndexed
from ..utilities.utilities import *


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
        import pandas as pd

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
        query = f"SELECT {select_time}`value` AS {value_name} {more} FROM {data_table} WHERE `record_id` = {self.id}{order_by}"



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
        import pandas as pd
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
                print(multipliers)
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