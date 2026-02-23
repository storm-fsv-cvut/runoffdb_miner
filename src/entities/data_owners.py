from .measurement import Measurement

class MeasurementOwner:
    """
    Mixin for entities that provide a measurement context.
    """

    id: int
    runoffdb: "RunoffDB"
    measurements: dict[int, "Measurement"] | None = None

    def get_measurements(self, phenomenon_id: int | None = None):
        """
        Return measurements associated with this entity.
        Lazy-loaded via RunoffDB.
        """
        if self.measurements is not None:
            out = [
                m for m in self.measurements.values()
                if phenomenon_id is None or m.phenomenon_id == phenomenon_id
            ]
            return out or None

        self.measurements = self.runoffdb.load_measurements(self)
        return self.get_measurements(phenomenon_id)


class RecordOwner:
    """
    Mixin for entities that can resolve records
    via a measurement context.
    """

    runoffdb: "RunoffDB"

    def get_records(self, unit_id=None,
                    phenomenon_id=None,
                    record_type_id=None,
                    related_value_x_unit_id=None,
                    related_value_y_unit_id=None,
                    related_value_z_unit_id=None,
                    exclude_missing_records=False):

        out = []
        # get the measurements related to RecordOwner instance, pass on the argument
        # all measurements are returned if phenomenon_id is None
        measurements = self.get_measurements(phenomenon_id)
        if not measurements:
            return None

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

    def best_record_of_unit(self, **kwargs):
        """
        Convenient access wrapper
        """
        # import at call to avoid circular import!
        from ..services.record_resolution import get_best_record_of_unit
        return get_best_record_of_unit(owner=self, **kwargs)

    def resolve_record(self, **kwargs):
        """
        Convenient access wrapper
        """
        # import at call to avoid circular import!
        from ..services.record_resolution import resolve_dedicated_or_generic_record
        return resolve_dedicated_or_generic_record(owner=self, **kwargs)