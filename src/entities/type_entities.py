# -*- coding: utf-8 -*-
from ..setup.entity_ids import *
from ..utilities.utilities import *
from ..diagnostics.trace import *

from datetime import date, datetime

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

    def get_protection_measures_names(self, lang, return_trace: bool = False):
        issues = []
        if self.protection_measures:
            outlist = []
            for m in self.protection_measures:
                if m.name[lang] is not None:
                    outlist.append(m.name[lang])
                    # if the protection measure name is missing in requested language it must be resolved
                else:
                    outlist.append(f"missing name in '{lang}'")
                    issues.append(DataIssue(
                                reason=DataAbsenceReason.MISSING_PROPERTY_TRANSLATION,
                                source="Plot.get_protection_measures_names",
                                details=f"protection measure ID {m.id} is missing name in language '{lang}'",
                            ))
            outstr = ", ".join([msr for msr in outlist])
            return (outstr, tuple(issues)) if return_trace else outstr

        return (None, None) if return_trace else None

    def days_since_seeding(self, datetime: datetime, main_crop_only: bool = False, return_trace: bool = False):
        """

        :param datetime:
        :param main_crop_only:
        :param return_trace:
        :return:
        """
        # for the cultivated fallow always return None
        if self.crop_id == CULTIVATED_FALLOW_CROP_ID:
            issue = (DataIssue(
                reason=DataAbsenceReason.INVALID_REQUEST,
                source="get_days_since_seeding",
                details=f"days since seeding irrelevant for 'cultivated fallow'",
            ), )
            return (None, issue) if return_trace else None

        else:
            if self.agrotechnology is not None:
                days = self.agrotechnology.days_since_seeding(datetime, main_crop_only=main_crop_only)
                return (days, None) if return_trace else days
            else:
                issue = (DataIssue(
                    reason=DataAbsenceReason.MISSING_ENTITY_PROPERTY,
                    source="get_days_since_seeding",
                    details=f"plot ID {self.id} doesn't have agrotechnology assigned",
                ), )
                return (None, issue) if return_trace else None

    def days_since_last_operation(self, return_trace: bool = False):
        # for the cultivated fallow always return 0 since it is assumed prepared right before the simulation
        if self.crop_id == CULTIVATED_FALLOW_CROP_ID:
            issue = DataIssue(
                reason=DataAbsenceReason.IMPLICIT_VALUE,
                source="days_since_last_operation",
                details=f"days since last operation implicitly assumed = 0 for 'cultivated fallow'",
            )
            return (None, issue) if return_trace else None
        else:
            if self.agrotechnology is not None:
                return self.agrotechnology.days_since_last_operation(date)
            else:
                issue = DataIssue(
                    reason=DataAbsenceReason.MISSING_ENTITY_PROPERTY,
                    source="days_since_last_operation",
                    details=f"plot ID {self.id} doesn't have agrotechnology assigned",
                )
                return (None, issue) if return_trace else None

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

    def get_note(self, lang="en", remove=None):
        """
        Return the note for the given language, with characters replaced
        according to the `remove` mapping. Handles \n, \r\n, \\n safely.
        """
        note = self.note.get(lang)

        if not note:  # None or empty string
            return None

        # Default forbidden characters
        default_remove = {
            ";": ",",
            "\r\n": ".",
            "\n": ".",
            "\r": ".",
            "\\n": ".",  # literal backslash-n if stored as escaped
        }

        # Merge user-provided mapping
        if remove:
            default_remove.update(remove)

        # Apply all replacements
        for bad, replacement in default_remove.items():
            note = note.replace(bad, replacement)

        return note

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
        if self.operation_sequence is None or self.operation_sequence == {}:
            return None
        return max([op.operation_intensity_id for op in self.operation_sequence.values()])

    def get_maximum_disturbance_depth(self):
        if self.operation_sequence is None or self.operation_sequence == {}:
            return None
        return max([op.operation_depth_m for op in self.operation_sequence.values()])

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

class RunType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class CropType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class OperationType:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

class OperationIntensity:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

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

class QualityIndex:
    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}

class AssignmentType:
    def __init__(self, **kwargs):

        self.id = kwargs["id"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")

        self.description = {"cz": self.description_cz, "en": self.description_en}

class Method:

    def __init__(self, runoffdb, **kwargs):
        self.runoffdb = runoffdb

        self.id = kwargs["id"]
        self.name_cz = kwargs["name_cz"]
        self.name_en = kwargs["name_en"]
        self.description_cz = kwargs.get("description_cz")
        self.description_en = kwargs.get("description_en")
        self.processing_steps_sequence = self.runoffdb.get_processing_steps_sequence(self.id) # ordered list of processing steps included in the method
        # self.instruments_map = {} # mapping local indexes to instrument class instances

        self.name = {"cz": self.name_cz, "en": self.name_en}
        self.description = {"cz": self.description_cz, "en": self.description_en}


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
