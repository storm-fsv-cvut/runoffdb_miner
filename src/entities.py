import pandas as pd
import numpy as np
from pint import UnitRegistry
# import pint_pandas

from src.db_access import DBconnector

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

# multipliers for different units to convert between each other
multipliers = {1: {1: 1}, 2: {2: 1, 3: 0.001}, 3: {2: 1000, 3: 1}, 18: {27: 0.001}, 27: {18: 1000}}
ureg = UnitRegistry()
units = {1: ureg.liter/ureg.minute, 2: ureg.milligram/ureg.liter, 3: ureg.gram/ureg.liter}


# def get_value_in_time(dataframe, timedelta, zero_time=False, extrapolate=False, timekey = 'time', valuekey = 'value'):
#     """
#     Returns interpolated value of dataseries in time specified as timedelta
#
#     :param timedelta:
#     :param zero_time: presumed time of start of the series (value = 0)
#     :param extrapolate: range of extrapolation specified as multipliction of last complete interval length
#     :returns: interpolated/extrapolated value if available based on specified inputs otherwise False
#     """
#
#     # if timedelta is before the first value
#     if dataframe[timekey].min() > timedelta:
#         # if the zero time was specified
#         if zero_time:
#             print(" - extrapolating to zero\n\n")
#             return dataframe[valuekey].iloc[0] * (timedelta - zero_time) / (
#                         dataframe[timekey].iloc[0] - zero_time)
#         else:  # or return None if extrapolation not intended
#             print("Requested time is before the first record in provided data series.\n")
#             return None
#
#     # if time is after the last value
#     elif timedelta > dataframe[timekey].max():
#         if extrapolate:
#             # duration of last step in series
#             last_step = dataframe[timekey].iloc[-1] - dataframe[timekey].iloc[-2]
#
#             # if the desired timedelta is within 'extrapolate' times last interval duration from series end
#             if (timedelta - dataframe[timekey].max()) < extrapolate * last_step:
#                 print(" - extrapolating after series end\n")
#
#                 v1 = dataframe[valuekey].iloc[-2]
#                 v2 = dataframe[valuekey].iloc[-1]
#                 t1 = dataframe[timekey].iloc[-2]
#                 t2 = dataframe[timekey].iloc[-1]
#                 t3 = timedelta
#
#                 if (t3 - t2) < extrapolate * (t2 - t1):
#                     result = v2 + (t3 - t2) * ((v2 - v1) / (t2 - t1))
#                     # print(f"{v2} + ({t3} - {t2}) * (({v2} - {v1})) / ({t2} - {t1}))")
#                     # print(f"= {result}\n")
#                     return result
#
#         else:  # or return False if extrapolation not intended
#             print("Requested time is after last record of provided data series.\n")
#             return None
#
#     else:
#         print(valuekey)
#         print(timedelta)
#         print(dataframe)
#         return dataframe[valuekey].loc[timedelta].interpolate(method='linear')

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

class Run:
    def __init__(self, run_id, sequence_id = None, datetime = None, simulator_id = None, run_group_id = None, ttr = None, run_type_id = None, plot_id = None, locality_id = None, crop_id = None, crop_type_id = None, initmoist_recid = None, surface_cover_recid = None, bbch = None, rainfall_recid = None, texture_ss_id=None, bulkd_ss_id=None, corg_ss_id=None):
        self.dbc = DBconnector()

        self.id = run_id
        self.sequence_id = sequence_id
        self.run_group_id = run_group_id
        self.datetime = datetime
        self.simulator_id = simulator_id
        self.run_type_id = run_type_id
        self.ttr = ttr
        self.measurements = None
        self.brothers = None
        self.plot_id = plot_id
        self.plot = None
        self.locality_id = locality_id
        self.crop_id = crop_id
        self.crop_type_id = crop_type_id
        self.crop_name = None
        self.initmoist_recid = initmoist_recid
        self.surface_cover_recid = surface_cover_recid
        self.bbch = bbch
        self.rain_intensity_recid = rainfall_recid

        self.bulkd_ss_id = bulkd_ss_id
        self.texture_ss_id = texture_ss_id
        self.corg_ss_id = corg_ss_id

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
        dbcon = self.dbc.connect()
        if dbcon:
            thecursor = dbcon.cursor()
            # execute the query and fetch the results
            query = f"SELECT `run`.`id` FROM `run` WHERE `run_group_id` = {self.rungroup_id}"

            thecursor.execute(query)
            results = thecursor.fetchall()

            if thecursor.rowcount > 0:
                brothers = []
                for r in results:
                    if r[0] != self.id:
                        brothers.append(r[0])
                dbcon.close()
                return brothers

            dbcon.close()
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

    def load_plot(self):
        dbcon = self.dbc.connect()
        if dbcon:
            thecursor = dbcon.cursor()
            # execute the query and fetch the results
            thecursor.execute(f"SELECT `run`.`plot_id`, `plot`.`locality_id` FROM `run` "\
                              f"JOIN `plot` ON `plot`.`id` = `run`.`plot_id`"\
                              f"WHERE `run`.`id` = {self.id}")
            results = thecursor.fetchall()

            if thecursor.rowcount > 0:
                for r in results:
                    self.plot_id = r[0]
                    self.locality_id = r[1]
                dbcon.close()
                return True

            dbcon.close()
        return None

    def get_crop_name(self, lang):
        if self.crop_id:
            dbcon = self.dbc.connect()
            if dbcon:
                thecursor = dbcon.cursor()

                query = f"SELECT `name_{lang}` FROM {crops_table} "\
                        f"WHERE `id` = {self.crop_id}"
                # print(query)
                # execute the query and fetch the results
                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    for res in results:
                        name = res[0]
                    dbcon.close()
                    self.crop_name = name
                    return name

                else:
                    print(f"No data were found for query '{query}'")
                    dbcon.close()
                    return None
            else:
                print ("Connection to database couldn't be established")
                return None
        else:
            # print(f"Run {self.id} doesn't have crop ID assigned.")
            return None

    def get_initial_moisture_value(self):
        if self.initmoist_recid:
            dbcon = self.dbc.connect()
            if dbcon:
                thecursor = dbcon.cursor()

                query = f"SELECT `value` FROM {data_table} "\
                        f"WHERE `record_id` = {self.initmoist_recid}"
                # print(query)
                # execute the query and fetch the results
                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount == 0:
                    print(f"No data were found for query '{query}'")
                    dbcon.close()
                    return None
                elif thecursor.rowcount == 1:
                    initmoist = results[0][0]
                    dbcon.close()
                    self.initmoist = initmoist
                    return initmoist
                else:
                    print("initial moisture is average from values")
                    initmoist = 0
                    for res in results:
                        initmoist += res[0]
                    initmoist /= thecursor.rowcount()
                    dbcon.close()
                    self.initmoist = initmoist
                    return initmoist
            else:
                print ("Connection to database couldn't be established")
                return None
        else:
            print(f"run {self.id} doesn't have initial moisture record ID assigned.")
            return None

    def get_surface_cover_value(self):
        if self.surface_cover_recid:
            dbcon = self.dbc.connect()
            if dbcon:
                thecursor = dbcon.cursor()

                query = f"SELECT `value` FROM {data_table} "\
                        f"WHERE `record_id` = {self.surface_cover_recid}"
                # print(query)
                # execute the query and fetch the results
                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount == 0:
                    print(f"No data were found for query '{query}'")
                    dbcon.close()
                    return None
                elif thecursor.rowcount == 1:
                    value = results[0][0]
                    dbcon.close()
                    self.surface_cover = value
                    return value
                else:
                    print(f"more than one surface cover values found for run #{self.id}")
                    dbcon.close()
                    return None
            else:
                print ("Connection to database couldn't be established")
                return None
        else:
            print(f"run {self.id} doesn't have surface cover record ID assigned.")
            return None

    def get_rainfall_intensity(self):
        if self.rain_intensity_recid:
            dbcon = self.dbc.connect()
            if dbcon:
                thecursor = dbcon.cursor()

                query = f"SELECT `value` FROM {data_table} "\
                        f"WHERE `record_id` = {self.rain_intensity_recid}"
                # print(query)
                # execute the query and fetch the results
                thecursor.execute(query)
                data = thecursor.fetchall()

                if thecursor.rowcount == 0:
                    print(f"No data were found for query '{query}'")
                    dbcon.close()
                    return None
                elif thecursor.rowcount == 1:
                    print(f"Proper rainfall intensity has at least two data points. Record '{self.rain_intensity_recid}' contains only one.")
                    dbcon.close()
                    return None
                elif thecursor.rowcount == 2:
                    value = data[0][0]
                    dbcon.close()
                    self.surface_cover = value
                    return value
                else:
                    # interrupted or variable intensity rainfall
                    numzeros = 0
                    for datapoint in data:
                        if datapoint[0] == 0:
                            numzeros += 1
                    if numzeros > 1:
                        dbcon.close()
                        return "interrupted"
                    else:
                        dbcon.close()
                        return "variable"
                    return None
            else:
                print ("Connection to database couldn't be established")
                return None
        else:
            print(f"run {self.id} doesn't have rainfall intensity record ID assigned.")
            return None

    def get_records(self, phenomenon_id = None, unit_id = None, record_type_id = None):
        out = []
        # get the measurements related to Run instance, pass on the argument
        print(f"getting records of measurements of ph_id {phenomenon_id}")
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
        dbcon = self.dbc.connect()
        if dbcon:
            thecursor = dbcon.cursor(dictionary=True)

            query = f"SELECT * FROM {measurements_table} " \
                    f"JOIN {measurement_run_table} ON {measurement_run_table}.`measurement_id` = {measurements_table}.`id` " \
                    f"WHERE {measurement_run_table}.`run_id` = {self.id}"
            thecursor.execute(query)
            results = thecursor.fetchall()

            if thecursor.rowcount == 0:
                # print(f"\tNo measurement found for run {self.id}")
                dbcon.close()
                return []
            else:
                msrmsnt = []
                for res in results:
                    new_measurements = Measurement(**res)
                    msrmsnt.append(new_measurements)
                self.measurements = msrmsnt
                dbcon.close()
                return msrmsnt
        else:
            return None

    def get_measurements(self, phenomenon_id = None):
        if self.measurements:
            out = []
            for meas in self.measurements:
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

    # def get_record(self, phenomenon_id, unit_id, record_type_id = None):
    #     # get measurements of given phenomenon id
    #     measurements = self.get_measurements(phenomenon_id)
    #     if measurements:
    #         if len(measurements) > 0:
    #
    #             for measr in measurements:
    #                 recs = measr.get_records(unit_id, record_type_id)
    #
    #                 # if any records found
    #                 if recs:
    #                     if len(recs) > 0:
    #                         return recs

        return None

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
    def __init__(self, **kwargs):
        self.dbc = DBconnector()

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
        self.records = self.load_records()

    def show_details(self, indent = "", t = "- "):
        indent += t
        print(indent+f"measurement_id: {self.id}")
        indent += t
        print(indent+f"phenomenon_id: {self.phenomenon_id}")
        if self.date:
            print(indent+f"date: {self.date.strftime('%d. %m. %Y')}")
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
        dbcon = self.dbc.connect()
        if dbcon:
            thecursor = dbcon.cursor(dictionary=True)

            query = f"SELECT * FROM {records_table} WHERE `measurement_id` = {self.id}"
            # print(query)
            thecursor.execute(query)
            results = thecursor.fetchall()

            if thecursor.rowcount == 0:
                # print(f"\tNo record found for measurement {self.id}")
                dbcon.close()
                return []
            else:
                rcrds = []
                for r in results:
                    new_record = Record(**r)
                    rcrds.append(new_record)
                self.records = rcrds
                dbcon.close()
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

    def __init__(self, **kwargs):
        self.dbc = DBconnector()

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

        self.data = None

        # the 'unit' of unit as a string
        self.unit = None

        if multipliers.get(self.unit_id):
            self.multiplier_to_SI = multipliers.get(self.unit_id)
        else:
            self.multiplier_to_SI = None

    def show_details(self, indent = "", t = ". "):
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

        dbcon = self.dbc.connect()
        if dbcon:
            try:
                select_time = "`time`, " if self.is_timeline else ""
                if order_by is not None:
                    order_by = f" ORDER BY `{order_by}` ASC"
                elif self.is_timeline:
                    order_by = f" ORDER BY `time` ASC"
                else:
                    order_by = ""

                query = f"SELECT {select_time}`value` AS {value_name} {more} FROM {data_table} WHERE `record_id` = {self.id}{order_by}"
                result_dataFrame = pd.read_sql(query, dbcon)

                if self.is_timeline and index_column is None:
                    result_dataFrame.set_index('time', inplace=True)
                elif index_column is not None:
                    result_dataFrame.set_index(index_column, inplace=True)

                dbcon.close()
                self.data = result_dataFrame
                return result_dataFrame
            except UserWarning:
                dbcon.close()
                pass
            except Exception as e:
                dbcon.close()
                print(str(e))
                return None
        else:
            return None


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
                data_out[output_column_label] = data_out[value_name]*multiply_by
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

    def get_unit(self):
        if self.unit_id:
            dbcon = self.dbc.connect()
            if dbcon:
                thecursor = dbcon.cursor()
                query = f"SELECT `unit` FROM {units_table} "\
                        f"WHERE `id` = {self.unit_id}"
                thecursor.execute(query)
                results = thecursor.fetchall()

                if thecursor.rowcount > 0:
                    unit = results[0][0]
                    dbcon.close()
                    self.unit = unit
                    return unit
                else:
                    print(f"No unit with id == '{query}' found.")
                    dbcon.close()
                    return None
            else:
                print ("Connection to database couldn't be established")
                return None
        else:
            # print(f"Run {self.id} doesn't have crop ID assigned.")
            return None



class Locality:
    def __init__(self):
        pass

class SoilSample:
    def __init__(self, kwargs):
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

        # verbal description of sampling location (e.g. related to experimental plot)
        self.sample_location = kwargs.get("sample_location")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.sample_depth_m = kwargs.get("sample_depth_m")
        self.raw_data_path = kwargs.get("raw_data_path")
        self.deleted = kwargs.get("deleted")
        self.user_id = kwargs.get("user_id")


class Plot:
    def __init__(self, kwargs):
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
        dbcon = DBconnector().connect()
        if dbcon:
            thecursor = dbcon.cursor()
            query = f"SELECT max(`datetime`) FROM `run_group` JOIN `run` ON `run`. `run_group_id` = `run_group`. `id` " \
                    f"WHERE `run`. `plot_id` = {self.id}"
            # execute the query and fetch the results
            thecursor.execute(query)

            results = thecursor.fetchone()
            if thecursor.rowcount > 0:
                dbcon.close()
                return results[0]
            else:
                dbcon.close()
                return None
        return None



class Crop:
    def __init__(self, kwargs):
        self.id = kwargs.get("id")
        self.crop_type_id = kwargs.get("crop_type_id")
        self.crop_er_type_id = kwargs.get("croper_type_id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.name =  {"cz": self.name_cz, "en": self.name_en}
        self.variety = kwargs.get("variety")
        self.is_catch_crop = kwargs.get("is_catch_crop")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.description = {"cz": self.description_cz, "en": self.description_en}


class Agrotechnology:

    operations = None

    @classmethod
    def load_all_operations(cls):
        print("... loading tillage operations to establish agrotechnologies\n")
        dbcon = DBconnector().connect()
        if dbcon:
            thecursor = dbcon.cursor(dictionary=True)
            # execute the query and fetch the results
            thecursor.execute(f"SELECT * FROM {operations_table}")
            results = thecursor.fetchall()
            if thecursor.rowcount > 0:
                operations = {}
                for r in results:
                    new_op = Operation(**r)
                    operations.update({new_op.id: new_op})
            dbcon.close()
            Agrotechnology.operations = operations
            return operations
        return None



    def __init__(self, **kwargs):
        # the first Agrotechnology instance induces the agrotechnology DB load of all operations
        if self.operations is None:
            Agrotechnology.load_all_operations()

        self.id = kwargs.get("id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.note_cz = kwargs.get("note_cz")
        self.note_en = kwargs.get("note_en")

        self.operation_sequence = self.load_operation_sequence()

    def load_operation_sequence(self):
        dbcon = DBconnector().connect()
        if dbcon:
            thecursor = dbcon.cursor(dictionary=True)

            query = f"SELECT `operation_id`, `date` FROM {tillageseq_table} WHERE `agrotechnology_id` = {self.id}"
            # print(query)
            thecursor.execute(query)
            results = thecursor.fetchall()

            if thecursor.rowcount == 0:
                print(f"No tillage sequence records were found for agrotechnology ID {self.id}")
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
        self.machinery_type_en = kwargs.get("machinery_type_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

class Unit:
    def __init__(self, kwargs):
        self.id = kwargs.get("id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.unit = kwargs.get("unit")
        self.decimals = kwargs.get("decimals")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
