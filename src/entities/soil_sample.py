# -*- coding: utf-8 -*-

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

