from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Callable
from enum import Enum, auto

from .unit_ids import *
from .derivations import *

class VariableGroup(Enum):
    HYDRO_SEDIMENT = auto()
    SOIL_PROPERTIES = auto()
    CROP_PROPERTIES = auto()


@dataclass(frozen=True)
class VariableDefinition:
    key: str
    phenomenon_id: Optional[int]
    # main units
    default_unit_id: Optional[int]
    allowed_unit_ids: Tuple[int, ...]
    # related units
    default_relx_unit_id: Optional[int] = None
    default_rely_unit_id: Optional[int] = None
    default_relz_unit_id: Optional[int] = None
    allowed_relx_unit_ids: Tuple[int, ...] = None
    allowed_rely_unit_ids: Tuple[int, ...] = None
    allowed_relz_unit_ids: Tuple[int, ...] = None

    aggregation: Optional[str] = None
    default_interpolation: Optional[str] = None
    dependencies: Tuple[dict, ...] = ()        # e.g., {"record": True} or {"derived_from": [...]}
    base_labels: Optional[Dict[str, str]] = None  # injected at runtime
    groups: tuple[VariableGroup, ...] = ()

    # derivation instructions
    derivation_func: Optional[Callable[[pd.DataFrame, "VariableRegistry"], None]] = None

    # dedicated records
    dedicated_record_attr: Optional[str] = None
    dedicated_soil_sample_attr: Optional[str] = None
    dedicated_soil_record_attr: Optional[str] = None

    def __str__(self) -> str:
        parts = [f"key = '{self.key}'\n"]

        if self.phenomenon_id is not None:
            parts.append(f"phenomenon_id = {self.phenomenon_id}\n")

        if self.default_unit_id is not None:
            parts.append(f"default_unit_id = {self.default_unit_id}\n")

        if self.default_relx_unit_id is not None:
            parts.append(f"default_relx_unit_id = {self.default_relx_unit_id}\n")

        if self.default_rely_unit_id is not None:
            parts.append(f"default_rely_unit_id = {self.default_rely_unit_id}\n")

        if self.default_relz_unit_id is not None:
            parts.append(f"default_relz_unit_id = {self.default_relz_unit_id}\n")

        if self.allowed_unit_ids:
            parts.append(
                f"allowed_unit_ids = {list(self.allowed_unit_ids)}\n"
            )

        if self.allowed_relx_unit_ids:
            parts.append(
                f"allowed_relx_unit_ids = {list(self.allowed_relx_unit_ids)}\n"
            )

        if self.allowed_rely_unit_ids:
            parts.append(
                f"allowed_rely_unit_ids = {list(self.allowed_rely_unit_ids)}\n"
            )

        if self.allowed_relz_unit_ids:
            parts.append(
                f"allowed_relz_unit_ids = {list(self.allowed_relz_unit_ids)}\n"
            )

        if self.aggregation:
            parts.append(f"aggregation = '{self.aggregation}'\n")

        if self.default_interpolation:
            parts.append(f"default_interpolation = '{self.default_interpolation}'\n")

        if self.dependencies:
            parts.append(f"dependencies = {list(self.dependencies)}\n")

        if self.base_labels:
            parts.append(f"base_labels = {self.base_labels}\n")

        return f"VariableDefinition:\n{''.join(parts)}"

# ---------------------------------------------------------
# HYDRO & SEDIMENT
# ------------------------------------------------------------------

SURFACE_RUNOFF = VariableDefinition(
    key="surface_runoff",
    phenomenon_id=SURFACE_RUNOFF_PHEN_ID,
    groups=(VariableGroup.HYDRO_SEDIMENT, ),
    default_unit_id=RUNOFF_RATE_LMIN_UNIT_ID,
    allowed_unit_ids=tuple(RUNOFF_RATE_UNITS),
    aggregation=None,
    default_interpolation="linear",
    dependencies=({"record": True}, ),
)

DISCHARGE = VariableDefinition(
    key="discharge",
    phenomenon_id=SURFACE_RUNOFF_PHEN_ID,
    groups=(VariableGroup.HYDRO_SEDIMENT, ),
    default_unit_id=DISCHARGE_VOLUME_L_UNIT_ID,
    allowed_unit_ids=(DISCHARGE_VOLUME_L_UNIT_ID, ),
    aggregation=None,
    default_interpolation="linear",
    dependencies=({"derived_from": ["surface_runoff"]}, ),
    derivation_func=derive_discharge
)

SEDIMENT_CONCENTRATION = VariableDefinition(
    key="sediment_concentration",
    phenomenon_id=SEDIMENT_QUANTITY_PHEN_ID,
    groups=(VariableGroup.HYDRO_SEDIMENT, ),
    default_unit_id=SS_CONCENTRATION_GL_UNIT_ID,
    allowed_unit_ids=tuple(SS_CONCENTRATION_UNITS),
    aggregation="mean",
    default_interpolation="linear",
    dependencies=({"record": True}, )
)


SEDIMENT_FLUX = VariableDefinition(
    key="sediment_flux",
    phenomenon_id=SEDIMENT_QUANTITY_PHEN_ID,
    groups=(VariableGroup.HYDRO_SEDIMENT, ),
    default_unit_id=SEDIMENT_FLUX_GMIN_UNIT_ID,
    allowed_unit_ids=(SEDIMENT_FLUX_GMIN_UNIT_ID, ),
    aggregation=None,
    default_interpolation="linear",
    dependencies=({"record": True}, {"derived_from": ["surface_runoff", "sediment_concentration"]}),
    derivation_func=derive_sediment_flux,
)


SEDIMENT_YIELD = VariableDefinition(
    key="sediment_yield",
    phenomenon_id=SEDIMENT_QUANTITY_PHEN_ID,
    groups=(VariableGroup.HYDRO_SEDIMENT, ),
    default_unit_id=SEDIMENT_YIELD_G_UNIT_ID,
    allowed_unit_ids=tuple(SEDIMENT_YIELD_UNITS),
    aggregation=None,
    default_interpolation="linear",
    dependencies=({"derived_from": ["sediment_flux"]}, ),
    derivation_func=derive_sediment_yield
)

RAINFALL_INTENSITY = VariableDefinition(
    key="rainfall_intensity",
    phenomenon_id=RAINFALL_PHEN_ID,
    groups=(VariableGroup.HYDRO_SEDIMENT, ),
    default_unit_id=RAINFALL_INTENSITY_MMH_UNIT_ID,
    allowed_unit_ids=tuple(RAINFALL_INTENSITY_UNITS),
    aggregation=None,
    default_interpolation="ffill",
    dependencies=({"record": True}, ),
    dedicated_record_attr="rain_intensity_recid"
)


RAINFALL_TOTAL = VariableDefinition(
    key="rainfall_total",
    phenomenon_id=RAINFALL_PHEN_ID,
    groups=(VariableGroup.HYDRO_SEDIMENT, ),
    default_unit_id=RAINFALL_TOTAL_MM_UNIT_ID,
    allowed_unit_ids=(RAINFALL_TOTAL_MM_UNIT_ID, ),
    aggregation=None,
    default_interpolation="linear",
    dependencies=({"derived_from": ["rainfall_intensity"]}, ),
    derivation_func=derive_rainfall_total,
)


# ------------------------------------------------------------------
# SOIL
# ------------------------------------------------------------------

SOIL_MOISTURE = VariableDefinition(
    key="soil_moisture",
    phenomenon_id=SOIL_MOISTURE_PHEN_ID,
    groups=(VariableGroup.SOIL_PROPERTIES,),
    default_unit_id=SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,
    allowed_unit_ids=(SOIL_MOISTURE_VOLUME_PERC_UNIT_ID,),
    aggregation="mean",
    default_interpolation=None,
    dedicated_record_attr="initmoist_recid",
)

BULK_DENSITY = VariableDefinition(
    key="bulk_density",
    phenomenon_id=PHYSICAL_SOIL_PROPERTIES_PHEN_ID,
    groups=(VariableGroup.SOIL_PROPERTIES,),
    default_unit_id=BULK_DENSITY_KGM_UNIT_ID,
    allowed_unit_ids=tuple(BULK_DENSITY_UNITS),
    aggregation="mean",
    default_interpolation=None,
    dedicated_soil_sample_attr="bulk_ss_id",
    dedicated_soil_record_attr="bulk_density_id",
)

PARTICLE_SIZE_DISTRIBUTION = VariableDefinition(
    key="particle_size_distribution",
    phenomenon_id=PARTICLE_SIZE_DISTRIBUTION_PHEN_ID,
    groups=(VariableGroup.SOIL_PROPERTIES,),
    default_unit_id=CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID,
    allowed_unit_ids=(CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID,),
    default_relx_unit_id=PARTICLE_SIZE_THRESHOLD_MM_UNIT_ID,
    allowed_relx_unit_ids=(PARTICLE_SIZE_THRESHOLD_MM_UNIT_ID,),
)

# ------------------------------------------------------------------
# CROP
# ------------------------------------------------------------------

CROP_HEIGHT = VariableDefinition(
    key="crop_height",
    phenomenon_id=None,  # assign if you have crop phenomenon ID
    groups=(VariableGroup.CROP_PROPERTIES,),
    default_unit_id=CROP_HEIGHT_CM_UNIT_ID,
    allowed_unit_ids=tuple(CROP_HEIGHT_UNITS),
    aggregation="mean",
)

CROP_DENSITY = VariableDefinition(
    key="crop_density",
    phenomenon_id=None,
    groups=(VariableGroup.CROP_PROPERTIES,),
    default_unit_id=CROP_DENSITY_M_2_UNIT_ID,
    allowed_unit_ids=(CROP_DENSITY_M_2_UNIT_ID,),
)

SURFACE_COVER = VariableDefinition(
    key="surface_cover",
    phenomenon_id=None,
    groups=(VariableGroup.CROP_PROPERTIES,),
    default_unit_id=SURFACE_COVER_PERC_UNIT_ID,
    allowed_unit_ids=(SURFACE_COVER_PERC_UNIT_ID,),
)

# ------------------------------------------------------------------
# MASTER REGISTRY (authoritative source)
# ------------------------------------------------------------------

VARIABLE_REGISTRY = (
    RAINFALL_INTENSITY,
    RAINFALL_TOTAL,
    SURFACE_RUNOFF,
    DISCHARGE,
    SEDIMENT_CONCENTRATION,
    SEDIMENT_FLUX,
    SEDIMENT_YIELD,
    SOIL_MOISTURE,
    BULK_DENSITY,
    PARTICLE_SIZE_DISTRIBUTION,
    CROP_HEIGHT,
    CROP_DENSITY,
    SURFACE_COVER,
)