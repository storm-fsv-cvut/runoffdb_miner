
# IDs of units
RUNOFF_RATE_LMIN_UNIT_ID = 1  # in l.min-1
SS_CONCENTRATION_GL_UNIT_ID = 3  # in g.l-1
RAINFALL_INTENSITY_MMH_UNIT_ID = 6  # in mm.hour-1
SEDIMENT_FLUX_GMIN_UNIT_ID = 23  # in g.min-1

CROP_HEIGHT_CM_UNIT_ID = 31 # in cm
CROP_DENSITY_M_2_UNIT_ID = 30 # in pieces.m-1

# multipliers for different units to convert between each other
multipliers = {1: {1: 1},
               2: {2: 1, 3: 0.001},
               3: {2: 1000, 3: 1},
               6: {6: 1, 32: 1/60},
               18: {18: 1, 27: 0.001},
               27: {18: 1000, 27: 1},
               32: {6: 60, 32: 1}}