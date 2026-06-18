# -*- coding: utf-8 -*-
from ..processing.resolution.field import resolve_translated_field
from ..setup.entity_ids import *
from ..utilities.utilities import *
from ..diagnostics.trace import DataTrace
from ..diagnostics.issue import DataIssue
from ..diagnostics.severity import IssueSeverity
from ..diagnostics.absence_reasons import *

from datetime import datetime, date
from typing import Optional, Tuple


class Locality:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.organization_id = kwargs.get("organization_id")
        self.name = kwargs.get("name")
        self.lat = kwargs.get("lat")
        self.lng = kwargs.get("lng")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.wrb_soil_class_id = kwargs.get("wrb_soil_class_id")

        self.description = {"cz": self.description_cz, "en": self.description_en}

    def __str__(self):
        outstring = f"#{self.id} - {self.name}"
        if self.description_cz is not None:
            outstring += f" ({self.description_cz})"
        outstring += f"[{self.lat}N, {self.lng}E]"
        loc_plots = []
        first_plot_date = None
        last_experiment_date = None
        for pl in self.runoffdb.plots.values():
            if pl.locality_id == self.id:
                loc_plots.append(pl.id)
                if len(pl.get_runs_on_plot()) > 0:
                    # print(f"runs on plot: {', '.join([str(rid) for rid in pl.get_runs_on_plot()])}")
                    first_plot_date = pl.established if first_plot_date is None else min(pl.established, first_plot_date)
                    last_experiment_date = pl.get_last_run_datetime() if last_experiment_date is None else max(pl.get_last_run_datetime(), last_experiment_date)
        if first_plot_date is not None and last_experiment_date is not None:
            outstring += f"\n\t{czech_date(first_plot_date)} - {czech_date(last_experiment_date)}"
        elif first_plot_date is not None and last_experiment_date is None:
            outstring += f"\n\t{czech_date(first_plot_date)} - /"
        else:
            outstring += f"\n\tno plot was ever established on the locality"

        outstring += f"\n\tplots: {', '.join([str(plid) for plid in loc_plots])}"
        outstring += "\n"
        return outstring


class Plot:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb
        self.id = kwargs.get("id")
        self.locality_id = kwargs.get("locality_id")
        self.soil_origin_locality_id = kwargs.get("soil_origin_locality_id")
        self.name = kwargs.get("name")
        self.crop_id = kwargs.get("crop_id")
        self.crop = self.runoffdb.crops[self.crop_id] if self.crop_id is not None else None
        self.agrotechnology_id = kwargs.get("agrotechnology_id")
        self.agrotechnology = runoffdb.agrotechnologies[self.agrotechnology_id] if self.agrotechnology_id is not None else None
        self.established = kwargs.get("established")
        self.plot_width = kwargs.get("plot_width")
        self.plot_length = kwargs.get("plot_length")
        self.plot_slope = kwargs.get("plot_slope")
        self.protection_measure_ids, self.protection_measures = self.runoffdb.get_protection_measures(self.id)

        self.note = {"cz": kwargs.get("note_cz"), "en": kwargs.get("note_en")}

    def get_protection_measures_names(self, lang: str):

        trace = DataTrace(
            source="Plot.get_protection_measures_names",
            variable="protection_measure_names",
            success=True,
            owner_id=self.id,
            owner_class=type(self).__name__,
            traces=[],
        )

        if not self.protection_measures:
            trace.details = f"no protection measure assigned to plot #{self.id}"
            return None, trace

        values = []

        for measure in self.protection_measures:

            value, child_trace = measure.get_name(lang)

            if child_trace:
                trace.traces.append(child_trace)

            if value:
                values.append(value)
            else:
                values.append(f"#{measure.id}")

        return ", ".join(values), trace

    def days_since_seeding(
            self,
            datetime: datetime,
            main_crop_only: bool = False,
    ) -> tuple[Optional[int], DataTrace]:
        """
        Returns number of days since seeding + provenance trace.
        """

        trace = DataTrace(
            source="Plot.days_since_seeding",
            variable="days_since_seeding",
            success=True,
            owner_id=self.id,
            owner_class=type(self).__name__,
            traces=[],
        )

        # -----------------------------------------
        # cultivated fallow shortcut
        # -----------------------------------------
        if self.crop_id == CULTIVATED_FALLOW_CROP_ID:
            trace.details="days since seeding irrelevant for cultivated fallow"
            return None, trace

        # -----------------------------------------
        # agrotechnology path
        # -----------------------------------------
        if self.agrotechnology is not None:
            days = self.agrotechnology.days_since_seeding(
                datetime,
                main_crop_only=main_crop_only,
            )

            trace.details = "derived from agrotechnology"

            return days, trace

        # -----------------------------------------
        # missing dependency
        # -----------------------------------------
        issue = DataIssue(
            reason=DataAbsenceReason.MISSING_PROPERTY,
            source="Plot.get_days_since_seeding",
            details=f"plot #{self.id} has no agrotechnology assigned",
        )

        trace.success=False
        trace.details="unable to compute days since seeding",
        trace.issues.append(issue)

        return None, trace

    def days_since_last_operation(self) -> tuple[Optional[int], DataTrace]:
        trace = DataTrace(
            source="Plot.days_since_last_operation",
            variable="days_since_last_operation",
            success=True,
            owner_id=self.id,
            owner_class=type(self).__name__,
            traces=[],
        )

        # for the cultivated fallow always return 0 since it is assumed prepared right before the simulation
        if self.crop_id == CULTIVATED_FALLOW_CROP_ID:
            trace.details = f"days since last operation implicitly assumed = 0 for 'cultivated fallow'"
            return 0, trace

        else:
            if self.agrotechnology is not None:
                trace.details = "derived from agrotechnology"

                return self.agrotechnology.days_since_last_operation(date), trace

            else:
                issue = DataIssue(
                    reason=DataAbsenceReason.MISSING_PROPERTY,
                    source="Plot.days_since_last_operation",
                    details=f"plot #{self.id} has no agrotechnology assigned",
                )

                trace.success = False
                trace.details = "unable to compute days since last operation",
                trace.issues.append(issue)

                return None, trace

    def get_metadata(self, lang="en"):
        meta = {"plot ID": self.id,
                "name": self.name,
                "established": self.established.strftime('%Y-%m-%d'),
                "plot length": self.plot_length,
                "plot length unit": "m",
                "plot width": self.plot_width,
                "plot width unit": "m",
                "slope steepness": self.plot_slope,
                "slope steepness unit": "%"}
        if self.soil_origin_locality_id is not None and self.soil_origin_locality_id != self.locality_id:
            meta.update({"soil origin locality": self.runoffdb.localities[self.soil_origin_locality_id].name})
        return meta

    def show_properties(self, lang="en"):
        print(f"\n{self.id} - {self.name}")
        print(f"\testablished: {czech_date(self.established)}")
        print(f"\tlength: {self.plot_length}m")
        print(f"\tplot width: {self.plot_width}m")
        print(f"\tslope steepness: {self.plot_slope}%")

        print(f"\tprotection measures: {'none' if not self.protection_measures else ''}")
        for measure in self.protection_measures:
            print(f"\t\t{measure.id} - {measure.name['cz']}")
        return

    def get_note(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="note",
            values=self.note,
            lang=lang,
            source="Plot.get_note",
        )


class Crop:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.crop_type_id = kwargs.get("crop_type_id")
        self.crop_type = self.runoffdb.crop_types[self.crop_type_id] if self.crop_type_id is not None else None
        self.crop_er_type_id = kwargs.get("croper_type_id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.variety = kwargs.get("variety")
        self.is_catch_crop = kwargs.get("is_catch_crop")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_metadata(self, lang="en"):
        meta = {"crop ID": self.id,
                "name": self.name[lang]
                }
        meta.update({"description": self.description[lang]}) if self.description[lang] is not None else None
        meta.update({"crop type": self.crop_type.name[lang]}) if self.crop_type is not None else None
        meta.update({"variety": self.variety}) if self.variety is not None else None
        meta.update({"is catch crop": "True" if self.is_catch_crop == 1 else "False"}) if self.is_catch_crop is not None else None
        return meta


class Agrotechnology:

    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.note_cz = kwargs.get("note_cz")
        self.note_en = kwargs.get("note_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.note = {"cz": self.note_cz, "en": self.note_en}

        self.operation_sequence = self.runoffdb.get_operation_sequence(self.id)

    def get_maximum_disturbance_level(self):
        trace = DataTrace(
            source="Agrotechnology.get_maximum_disturbance_level",
            variable="maximum_soil_disturbance_level",
            success=True,
            details="calculating maximum soil disturbance level from tillage operations sequence",
            owner_id=self.id,
            owner_class=type(self).__name__,
            traces=[],
        )
        if self.operation_sequence is None or self.operation_sequence == {}:
            issue = DataIssue(
                reason=DataAbsenceReason.MISSING_PROPERTY,
                source="Agrotechnology.get_maximum_disturbance_level",
                details=f"agrotechnology #{self.id} has empty operation sequence",
                )
            trace.success = False
            trace.issues.append(issue)
            return None, trace

        return max([op.operation_intensity_id for op in self.operation_sequence.values()]), trace

    def get_maximum_disturbance_depth(self):
        trace = DataTrace(
            source="Agrotechnology.get_maximum_disturbance_depth",
            variable="maximum_soil_disturbance_depth",
            success=True,
            details="calculating maximum soil disturbance depth from tillage operations sequence",
            owner_id=self.id,
            owner_class=type(self).__name__,
            traces=[],
        )
        if self.operation_sequence is None or self.operation_sequence == {}:
            issue = DataIssue(
                reason=DataAbsenceReason.MISSING_PROPERTY,
                source="Agrotechnology.get_maximum_disturbance_depth",
                details=f"agrotechnology #{self.id} has empty operation sequence",
            )
            trace.success = False
            trace.issues.append(issue)
            return None, trace

        return max([op.operation_depth_m for op in self.operation_sequence.values()]), trace

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

    def days_since_seeding(self, input_datetime, main_crop_only=False):
        # check if the input is a datetime or a date
        if isinstance(input_datetime, datetime):
            # extract just the date if it's a datetime
            the_date = input_datetime.date()
        elif isinstance(input_datetime, date):
            the_date = input_datetime
        else:
            raise ValueError("The input must be a date or datetime object.")

        # sort the operation_sequence by date in reverse order (most recent first)
        for operation_date in sorted(self.operation_sequence.keys(), reverse=True):
            if operation_date <= the_date:
                # Check if the operation type is "seeding"
                if main_crop_only and self.operation_sequence[operation_date].operation_type_id == MAIN_CROP_SEEDING_OPERATION_TYPE_ID:
                    # Return the number of days since the last seeding
                    return (the_date - operation_date).days
                elif self.operation_sequence[operation_date].operation_type_id == MAIN_CROP_SEEDING_OPERATION_TYPE_ID or self.operation_sequence[operation_date].operation_type_id == AUX_CROP_SEEDING_OPERATION_TYPE_ID:
                    return (the_date - operation_date).days
        # if no seeding operation was found, return None
        return None

    def days_since_last_operation(self, input_datetime):
        # check if the input is a datetime or a date
        if isinstance(input_datetime, datetime):
            # extract just the date if it's a datetime
            the_date = input_datetime.date()
        elif isinstance(input_datetime, date):
            the_date = input_datetime
        else:
            raise ValueError("The input must be a date or datetime object.")
        # Sort the operation_sequence by date in reverse order (most recent first)
        for operation_date in sorted(self.operation_sequence.keys(), reverse=True):
            if operation_date <= the_date:
                # Return the number of days since the last seeding
                return (the_date - operation_date).days
        # If no "seeding" operation was found, return None or a suitable value (e.g. -1)
        return None

    def get_metadata(self, lang="en"):
        meta = {"name": self.name[lang]}
        if self.description[lang] is not None:
            meta.update({"description": self.description[lang]})
        if self.note[lang] is not None:
            meta.update({"note": self.note[lang]})
        if self.operation_sequence is not None:
            if len(self.operation_sequence) > 0:
                sequence = {}
                for date, operation in self.operation_sequence.items():
                    sequence.update({date.strftime('%Y-%m-%d'): operation.get_metadata(lang)})
                meta.update({"operation sequence": sequence})
                meta.update({"maximum disturbance depth": self.get_maximum_disturbance_depth()})
                meta.update({"maximum disturbance intensity": self.runoffdb.operation_intensities[self.get_maximum_disturbance_level()].description[lang]})
        return meta

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_note(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="note",
            values=self.note,
            lang=lang,
            source=f"{type(self).__name__}.get_note",
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class Operation:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs.get("id")
        self.operation_intensity_id = kwargs.get("operation_intensity_id")
        self.operation_depth_m = kwargs.get("operation_depth_m")
        self.operation_type_id = kwargs.get("operation_type_id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.machinery_type_cz = kwargs.get("machinery_type_cz")
        self.machinery_type_en = kwargs.get("machinery_type_en")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}
        self.machinery_type = {"cz": self.machinery_type_cz, "en": self.machinery_type_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )

    def get_metadata(self, lang="en"):
        meta = {"name": self.name[lang]}
        if self.description[lang] is not None:
            meta.update({"description": self.description[lang]})
        if self.machinery_type[lang] is not None:
            meta.update({"machinery type": self.machinery_type[lang]})
        meta.update({"operation type": self.runoffdb.operation_types[self.operation_type_id].description[lang],
                "operation depth": self.operation_depth_m,
                "operation depth unit": "m",
                "operation intensity": self.runoffdb.operation_intensities[self.operation_intensity_id].description[lang]})
        return meta


class Unit:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.name_cz = kwargs.get("name_cz")
        self.name_en = kwargs.get("name_en")
        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.unit = kwargs.get("unit")
        self.decimals = kwargs.get("decimals")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class Project:
    project_leaders = {1: "Dostál T.",
                       2: "Krása J., Stašek J., Roudnická A.",
                       3: "Devátý J.",
                       4: "Kavka P., Kubínová R.",
                       5: "Krása J.",
                       6: "Dostál T.",
                       7: "Kavka P.",
                       8: "Neumann M., Mistr M."}

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.project_name = kwargs.get("project_name")
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.funding_agency = kwargs.get("funding_agency")
        self.project_code = kwargs.get("project_code")

        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class Simulator:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.organization_id = kwargs["organization_id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.reference = kwargs.get("reference")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

        self.organization = self.runoffdb.organizations[self.organization_id]

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )

    def show_properties(self, lang="en"):
        print(f"\n{self.id} - {self.name[lang]}")
        print(f"\tdescription: {self.description[lang]}")
        print(f"\torganization: {self.organization.name}")

        print(f"\tliterature references: {'none' if not self.reference else ''}")

        return

    def get_metadata(self, lang="en"):
        meta = {"name": self.name[lang]}
        if self.description is not None :
            meta.update({"description": self.description[lang],
                         "organization": self.organization.get_metadata()})
        return meta


class Organization:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name = kwargs["name"]
        self.contact_person = kwargs["contact_person"]
        self.contact_number = kwargs.get("contact_number")
        self.contact_email = kwargs.get("contact_email")
        self.name_code = kwargs.get("name_code")

    def get_metadata(self, lang="en"):
        meta = {"name": self.name}
        if self.contact_person is not None:
            if self.contact_person != "":
                meta.update({"contact person": self.contact_person})
                # add the contact info if contact person is in DB
                if self.contact_email is not None:
                    if self.contact_email != "":
                        meta.update({"contact email": self.contact_email})
                    else:
                        meta.update({"contact email": "NA"})

                if self.contact_number is not None:
                    if self.contact_number != "":
                        meta.update({"contact number": self.contact_number})
                    else:
                        meta.update({"contact number": "NA"})
            else:
                meta.update({"contact person": "NA"})
        else:
            meta.update({"contact person": "NA"})

        return meta


class ProtectionMeasure:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class RunType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class CropType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class OperationType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class OperationIntensity:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class Phenomenon:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.model_parameter_set_id = kwargs.get("model_parameter_set_id")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class RecordType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.model_parameter_set_id = kwargs.get("model_parameter_set_id")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class QualityIndex:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class AssignmentType:
    def __init__(self, **kwargs):

        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )


class Method:

    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        # ordered list of processing steps included in the method
        self.processing_steps_sequence = self.runoffdb.get_processing_steps_sequence(self.id)
        # self.instruments_map = {} # mapping local indexes to instrument class instances

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )

    def show_details(self, lang="en", indent=""):
        print(f"\nMethodics {self.id} - {self.name[lang]}")
        print(f"{self.description[lang] if self.description[lang] else 'no description'}")
        print(f"\nprocessing steps:") if self.processing_steps_sequence else print(f"no processing steps defined")

        for i, prs in enumerate(self.processing_steps_sequence, start=1):
            print(f"\t{i})")
            prs.show_details(lang, indent)

    def export_to_json(self, lang="en", include_ids=False):
        export = {}
        if include_ids:
            export["id"] = self.id
        export["name"] = self.name[lang]
        export["description"] = self.description[lang] if self.description[lang] else "NA"
        steps = {}
        for i, ps in enumerate(self.processing_steps_sequence, start=1):
            steps.update({i:ps.export_to_json(lang, include_ids)})
        # export["processing steps"] = [ps.export_to_json(lang, include_ids) for ps in self.processing_steps_sequence]
        export["processing steps"] = steps
        return export


class ProcessingStep:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.link = kwargs.get("link")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

        self.instruments = self.runoffdb.get_instruments(self.id)

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )

    def show_details(self, lang="en", indent=""):
        indent += "\t"
        print(f"{indent}{self.name[lang]}{(' - '+self.description[lang]) if self.description[lang] else ''}")
        if self.instruments:
            print(f"{indent}instruments used:")
            indent += "\t"
            for instr in self.instruments:
                instr.show_details(lang, indent)

    def export_to_json(self, lang="en", include_ids=False):
        export = {}
        if include_ids:
            export["id"] = self.id
        export["name"] = self.name[lang]
        export["description"] = self.description[lang] if self.description[lang] else "NA"
        export["instruments"] = [instr.export_to_json(lang, include_ids) for instr in self.instruments]
        return export


class Instrument:
    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.link = kwargs.get("link")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

    def get_name(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="name",
            values=self.name,
            lang=lang,
            source=f"{type(self).__name__}.get_name",
            mandatory=True,
        )

    def get_description(self, lang="en"):
        return resolve_translated_field(
            owner=self,
            field_name="description",
            values=self.description,
            lang=lang,
            source=f"{type(self).__name__}.get_description",
        )

    def show_details(self, lang="en", indent=""):
        indent += "\t"
        print(f"{indent}{self.name[lang]} - {self.description[lang]}")

    def export_to_json(self, lang="en", include_ids=False):
        export = {}
        if include_ids:
            export["id"] = self.id
        export["name"] = self.name[lang]
        export["description"] = self.description[lang] if self.description[lang] else "NA"

        return export
