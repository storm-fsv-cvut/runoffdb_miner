# this file contains some special IDs that are induced by intended functionalities and calculations

# IDs of units
RUNOFF_RATE_LMIN_UNIT_ID = 1  # in liters per minute
DISCHARGE_VOLUME_L_UNIT_ID = 5  # in liters

RAINFALL_INTENSITY_MMH_UNIT_ID = 6  # in mm.hour-1
RAINFALL_INTENSITY_MMMIN_UNIT_ID = 32  # in mm.minute-1

SS_CONCENTRATION_GL_UNIT_ID = 3  # in grams per liter
SS_CONCENTRATION_MGL_UNIT_ID = 2  # in mg.l-1
RAINFALL_TOTAL_MM_UNIT_ID = 24  # in millimeters
SEDIMENT_FLUX_GMIN_UNIT_ID = 23  # in g.min-1
SEDIMENT_YIELD_KG_UNIT_ID = 4  # in kilograms
SEDIMENT_YIELD_G_UNIT_ID = 39  # in grams

SOIL_MOISTURE_VOLUME_PERC_UNIT_ID = 16  # in volume %

CROP_HEIGHT_CM_UNIT_ID = 31  # in cm
CROP_DENSITY_M_2_UNIT_ID = 30  # in pieces.m-1

SURFACE_COVER_PERC_UNIT_ID = 10  # in percent

BULK_DENSITY_GCM_UNIT_ID = 27  # in g.cm-3
BULK_DENSITY_KGM_UNIT_ID = 18  # in kg.m-3

CUMULATIVE_MASS_CONTENT_PERC_UNIT_ID = 20  # cumulative particle mass content in %
PARTICLE_SIZE_THRESHOLD_MM_UNIT_ID = 19  # threshold for the particle size fraction in mm

SATURATED_HYDRAULIC_CONDUCTIVITY_UNIT_ID = 33  # in mm.hour-1
EFFECTIVE_CAPILARY_DRIVE_UNIT_ID = 34  # in mm
PHILLIPS_SORPTIVITY_UNIT_ID = 35  # mm.s-1


# sets of units of the same quantity
BULK_DENSITY_UNITS = [BULK_DENSITY_KGM_UNIT_ID, BULK_DENSITY_GCM_UNIT_ID]
SS_CONCENTRATION_UNITS = [SS_CONCENTRATION_MGL_UNIT_ID, SS_CONCENTRATION_GL_UNIT_ID]
SEDIMENT_YIELD_UNITS = [SEDIMENT_YIELD_G_UNIT_ID, SEDIMENT_YIELD_KG_UNIT_ID]
RUNOFF_RATE_UNITS = [RUNOFF_RATE_LMIN_UNIT_ID]

RAINFALL_INTENSITY_UNITS = [RAINFALL_INTENSITY_MMH_UNIT_ID, RAINFALL_INTENSITY_MMMIN_UNIT_ID]
CROP_HEIGHT_UNITS = [CROP_HEIGHT_CM_UNIT_ID]

# IDs of phenomena
SURFACE_RUNOFF_PHEN_ID = 1
SEDIMENT_QUANTITY_PHEN_ID = 2
RAINFALL_PHEN_ID = 3
SOIL_MOISTURE_PHEN_ID = 6
PHYSICAL_SOIL_PROPERTIES_PHEN_ID = 9
PARTICLE_SIZE_DISTRIBUTION_PHEN_ID = 10
MODELED_HYDROPEDOLOGICAL_CHARACTERISTICS = 15

# multipliers for different units to convert between each other
UNITS_CONVERSION = {
                    1: {1: 1},
                    2: {2: 1, 3: 0.001},
                    3: {2: 1000, 3: 1},
                    4: {4: 1, 39: 1000},
                    6: {6: 1, 32: 1/60},
                    18: {18: 1, 27: 0.001},
                    27: {18: 1000, 27: 1},
                    32: {6: 60, 32: 1},
                    39: {4: 0.001, 39: 1}
                }