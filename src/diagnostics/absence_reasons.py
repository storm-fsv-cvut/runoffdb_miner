from enum import Enum, auto


class DataAbsenceReason(Enum):
    NO_RUN_SELECTED = auto()
    NO_RECORD = auto()
    RECORD_NOT_ASSIGNED = auto()
    REFERENCED_RECORD_NOT_FOUND = auto()
    REFERENCED_ENTITY_NOT_FOUND = auto()
    NO_GENERIC_RECORD = auto()
    RECORD_SET_NOT_AVAILABLE = auto()
    NO_DATA_IN_RECORD = auto()
    DATA_NOT_AVAILABLE = auto()
    DEDICATION_MISSING = auto()  # for properties that should be assigned as dedicated but are not (desired unit record found but is not as dedicated
    FILTERED_OUT = auto()
    MISSING_REQUIRED_INPUT = auto()
    MISSING_DATA = auto()
    MISSING_PROPERTY = auto()
    MISSING_PROPERTY_TRANSLATION = auto()  # descriptive property is empty in requested language but not empty in other language - missing translation
    INTERPOLATION_NO_EFFECT = auto()
    INTERPOLATION_FAILED = auto()
    EXTRAPOLATION_FAILED = auto()
    INTERPOLATION_NO_DATA = auto()
    BEFORE_FIRST_VALUE = auto()
    AFTER_LAST_VALUE = auto()
    INVALID_VALUES = auto()
    NOT_REQUESTED = auto()
    INVALID_RECORD_TYPE = auto()
    UNIT_CONVERSION_FAILED = auto()
    DATABASE_RECORD_INVALID = auto()
    INVALID_REQUEST = auto()  # requested value does not make sense in given context
    IMPLICIT_VALUE = auto()  # the value was not derived from DB values but was casted an implicit value
    DERIVED_MEAN = auto()
    INCOMPATIBLE_UNIT_SET = auto()

    PROCESSING_ERROR = auto()
    UNKNOWN = auto()
