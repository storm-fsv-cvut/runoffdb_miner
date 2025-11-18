
class DataframeEmptyError(Exception):
    """
    This exception is raised when the dataframe of data exists but is empty
    """
    def __init__(self, value_name, record_id):
        self.message = f"DataFrame obtained for record #{record_id} as '{value_name}' is empty."
        self.record_id = record_id

class RecordSetNotComplete(Exception):
    """
    This exception is raised when certain combination of records is requested but some of the records are missing
    """
    def __init__(self, request_list, missing_record_names):
        self.message = f"Requested set of records ({', '.join([rr for rr in request_list])}) is not available for the run.\nFollowing records are missing:\n{', '.join([mr for mr in missing_record_names])}"
        self.requested_records = request_list
        self.missing_records = missing_record_names

class DataframeNotTimeIndexed(Exception):
    """
    This exception is raised when timeline data are expected (requested) but the obtained dataframe is not TimeDelta indexed
    """
    def __init__(self, record_id):
        self.message = f"data of record #{record_id} are not timeline data and should be"

class RequestedTimeDeltaValueMissing(Exception):
    """
    This exception is raised when value for specific TimeDelta is requested from a dataframe but no value exists for that index.
    Raise this exception when interpolation and/or extrapolation in the data is not desired.
    """
    def __init__(self, series_name, timedelta_index, message=None):
        self.message = f"Data value is not available in requested time ({timedelta_index}) without using interpolation/extrapolation for series '{series_name}'."

        if message:
            self.message += f"\n{message}"
