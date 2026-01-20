# this file contains some special IDs that are induced by intended functionalities and calculations

# IDs of units
RUNOFF_RATE_LMIN_UNIT_ID = 1  # in l.min-1
SS_CONCENTRATION_GL_UNIT_ID = 3  # in g.l-1
SS_CONCENTRATION_MGL_UNIT_ID = 2  # in mg.l-1
RAINFALL_INTENSITY_MMH_UNIT_ID = 6  # in mm.hour-1
RAINFALL_INTENSITY_MMMIN_UNIT_ID = 6  # in mm.minute-1
SEDIMENT_FLUX_GMIN_UNIT_ID = 23  # in g.min-1

SOIL_MOISTURE_VOLUME_PERC_UNIT_ID = 16  # in volume %

CROP_HEIGHT_CM_UNIT_ID = 31  # in cm
CROP_DENSITY_M_2_UNIT_ID = 30  # in pieces.m-1

SURFACE_COVER_PERC_UNIT_ID = 10  # in percent

BULK_DENSITY_GCM_UNIT_ID = 27 # in g.cm-3
BULK_DENSITY_KGM_UNIT_ID = 18 # in kg.m-3

CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID = 20  # cumulative particle mass content in %

# sets of units of the same quiantity
BULK_DENSITY_UNITS = [BULK_DENSITY_KGM_UNIT_ID, BULK_DENSITY_GCM_UNIT_ID]
SS_CONCENTRATION_UNITS = [SS_CONCENTRATION_MGL_UNIT_ID, SS_CONCENTRATION_GL_UNIT_ID]
RUNOFF_RATE_UNITS = [RUNOFF_RATE_LMIN_UNIT_ID]
RAINFALL_INTENSITY_UNITS = [RAINFALL_INTENSITY_MMH_UNIT_ID, RAINFALL_INTENSITY_MMMIN_UNIT_ID]
CROP_HEIGHT_UNITS = [CROP_HEIGHT_CM_UNIT_ID]

# IDs of phenomena
SURFACE_RUNOFF_PHEN_ID = 1
SEDIMENT_QUANTITY_PHEN_ID = 2
RAINFALL_PHEN_ID = 3
SOIL_MOISTURE_PHEN_ID = 6
PHYSICAL_SOIL_PROPERTIES_PHEN_ID = 9

# multipliers for different units to convert between each other
UNITS_CONVERSION = {1: {1: 1},
               2: {2: 1, 3: 0.001},
               3: {2: 1000, 3: 1},
               6: {6: 1, 32: 1/60},
               18: {18: 1, 27: 0.001},
               27: {18: 1000, 27: 1},
               32: {6: 60, 32: 1}}