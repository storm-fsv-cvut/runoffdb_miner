# -*- coding: utf-8 -*-
from ..setup.entity_ids import *
from ..setup.unit_ids import *
from ..setup.table_names import *

from ..utilities.utilities import *
from ..run_filter import RunFilter
from ..entities.measurement import Measurement
from ..exceptions import DataframeEmptyError, RecordSetNotComplete, DataframeNotTimeIndexed

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
        self.brothers = self.get_group_brothers_ids()
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

    def get_group_brothers_ids(self):
        """
        Returns a list of IDs of simulation runs from the same group
        :return:
        """
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor() as thecursor:
                # execute the query and fetch the results
                query = f"SELECT `run`.`id` FROM {runs_table} WHERE `run_group_id` = {self.run_group_id}"

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
                # self.runoffdb.log(self.id, f"dedicated initial moisture record {initmoist_rec.id} contains more than one value")
                if multi_value:
                    return initmoist_data["initial_moisture"].toList()
                else:
                    print(f"Initial moisture record {initmoist_rec.id} of run {self.id} has more then one value!")
                    print(f"Mean value of all {len(initmoist_data.index)} data points was returned.")
                    return initmoist_data["initial_moisture"].mean()
        else:
            # self.runoffdb.log(self.id, "initial moisture dedicated record not assigned")
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
            # self.runoffdb.log(self.id,f"surface cover dedicated record not assigned")
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
                # self.runoffdb.log(self.id, f"no record of surface cover found - value derived from crop type")
                print(f"\trun #{self.id} has no surface cover record - value derived from crop type")
                return 0
            else:
                # self.runoffdb.log(self.id, f"no record of surface cover found")
                print(f"\trun #{self.id} has no surface cover record")
                return None


    def get_crop_height_value(self):
        crop_height_rec = self.get_best_record_of_unit(CROP_HEIGHT_CM_UNIT_ID)
        if crop_height_rec is not None:
            try:
                crop_height_data = crop_height_rec.get_data("crop_height")
            except DataframeEmptyError:
                # self.runoffdb.log(self.id, f"crop heigh record #{crop_height_rec.id} returned empty dataframe")
                print(f"\tcrop heigh dataframe of record {crop_height_rec.id} is empty")
                return None
            else:
                if crop_height_data is not None:
                    return crop_height_data["crop_height"].mean()
                else:
                    # self.runoffdb.log(self.id, f"plant density data of record {crop_height_rec.id} is None")
                    print(f"\tplant density data of record {crop_height_rec.id} is None")
                    return None
        else:
            # self.runoffdb.log(self.id, f"no crop height record found")
            print(f"\trun #{self.id} has no crop height record")
            return None


    def get_plant_density_value(self):
        plant_density_rec = self.get_best_record_of_unit(CROP_DENSITY_M_2_UNIT_ID)
        if plant_density_rec is not None:
            try:
                plant_density_data = plant_density_rec.get_data("plant_density")
            except DataframeEmptyError:
                # self.runoffdb.log(self.id, f"plant density record #{plant_density_rec.id} returned empty dataframe")
                print(f"\tplant density dataframe of record {plant_density_rec.id} is empty")
                return None
            else:
                if plant_density_data is not None:
                    return plant_density_data["plant_density"].mean()
                else:
                    # self.runoffdb.log(self.id, f"plant density data of record {plant_density_rec.id} is None")
                    print(f"\tplant density data of record {plant_density_rec.id} is None")
                    return None
        else:
            # self.runoffdb.log(self.id, f"no plant density record found")
            print(f"\trun #{self.id} has no plant density record")
        return

    def get_rainfall_intensity_timeline(self, target_unit_id=None, series_label="rainfall_intensity"):
        if self.rain_intensity_recid is not None:
            intensity_rec = self.runoffdb.load_record_by_id(self.rain_intensity_recid)
            try:
                intensity_data = intensity_rec.get_data_in_unit(target_unit_id, series_label, demand_timeline=True)
            except DataframeEmptyError:
                raise
            else:
                # constant intensity series has exactly 2 rows, any other number is some exception or non-standard rainfall
                if len(intensity_data.index) == 1:
                    # self.runoffdb.log(self.id, f"rainfall intensity series of record {intensity_rec.id} contains only one data point")
                    print(f"Rainfall intensity record {intensity_rec.id} of run {self.id} contains only one data point. Proper rainfall intensity must have at least two data points.")
                    return None
                elif len(intensity_data.index) == 2:
                    if intensity_data["rain_intensity"].iloc[-1] != 0:
                        # self.runoffdb.log(self.id, f"rainfall intensity timeline record {intensity_rec} not ending with 0")
                        print(f"Rainfall intensity record {self.rain_intensity_recid} of run {self.id} doesn't end with zero value!")
                        return None
                    else:
                        return intensity_data
        else:
            # self.runoffdb.log(self.id, f"dedicated rainfall intensity record not assigned")
            print(f"\trun #{self.id} doesn't have dedicated rainfall intensity record ID assigned")
            return None

    def get_rainfall_intensity_value(self, target_unit_id=None):
        try:
            intensity_data = self.get_rainfall_intensity_timeline(target_unit_id, "rain_intensity")
        except DataframeEmptyError:
            return None
        else:
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
                # self.runoffdb.log(self.id, f"\tdedicated texture soil sample #{self.texture_ss.id} doesn't have dedicated texture record assigned.")
                print(f"\tdedicated texture soil sample of run #{self.id} doesn't have dedicated texture record assigned.")
                return None
        else:
            # self.runoffdb.log(self.id, f"dedicated texture soil sample not assigned")
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
                # self.runoffdb.log(self.id, f"\tdedicated bulk density soil sample #{self.bulkd_ss.id} doesn't have dedicated bulk density record assigned.")

                print(f"\tdedicated bulk density soil sample of run #{self.id} doesn't have dedicated bulk density record assigned.")
                return None
        else:
            # self.runoffdb.log(self.id, f"dedicated bulk density soil sample not assigned")
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

    def load_measurements(self):
        msrmsnts = None
        with self.runoffdb.get_connection() as dbcon:
            with dbcon.cursor(dictionary=True) as thecursor:
                query = f"SELECT * FROM {measurements_table} " \
                        f"JOIN {measurement_run_table} ON {measurement_run_table}.`measurement_id` = {measurements_table}.`id` " \
                        f"WHERE {measurement_run_table}.`run_id` = {self.id}"
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

