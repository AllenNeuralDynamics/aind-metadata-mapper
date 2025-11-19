"""Constants for FIP (Fiber Photometry) mapper.

Hardware specifications, device naming conventions, and default configuration values.
"""

# ==============================================================================
# LED Excitation Wavelengths (nm)
# ==============================================================================
EXCITATION_UV = 415  # UV LED → green emission (isosbestic control)
EXCITATION_BLUE = 470  # Blue LED → green emission (GFP signal)
EXCITATION_YELLOW = 565  # Yellow/Lime LED → red emission (RFP signal)

# ==============================================================================
# Emission Wavelengths (nm)
# ==============================================================================
EMISSION_GREEN = 520  # Green emission: center of 490-540nm bandpass, ~510nm GFP peak
EMISSION_RED = 590  # Red emission: ~590nm RFP peak

# ==============================================================================
# Camera Exposure Time
# ==============================================================================
CAMERA_EXPOSURE_TIME_MICROSECONDS_PER_MILLISECOND = 1000


# ==============================================================================
# ROI Keywords
# ==============================================================================
ROI_KEYWORD_GREEN = "green"
ROI_KEYWORD_ISO = "iso"
ROI_KEYWORD_RED = "red"
ROI_KEYWORD_ROI = "_roi"
ROI_KEYWORD_BACKGROUND = "_background"

# ==============================================================================
# Default Values
# ==============================================================================
DEFAULT_OUTPUT_FILENAME = "acquisition.json"
DEFAULT_LED_POWER = 1.0


# ==============================================================================
# Acquisition Type
# ==============================================================================
# Hardcoded acquisition type for VrForaging-FIP experiments
# NOTE: Ideally this would come from the extracted metadata, but we don't have it yet.
ACQUISITION_TYPE_AIND_VR_FORAGING = "AindVrForaging"


# ==============================================================================
# Device Name Transformations
# ==============================================================================
# Maps rig config keys to historical standard device names
# These are the keys from the fip.json rig dictionary that get transformed
# to match historical naming conventions
DEVICE_NAME_MAP = {
    "camera_green_iso": "Green CMOS",
    "camera_red": "Red CMOS",
    "light_source_uv": "415nm LED",
    "light_source_blue": "470nm LED",
    "light_source_lime": "560nm LED",
}


# ==============================================================================
# Code Repository
# ==============================================================================
# URL for the VrForaging-FIP experiment repository used to track code versions
# NOTE: Ideally this would come from the extracted metadata, but we don't have it yet.
VR_FORAGING_FIP_REPO_URL = "https://github.com/AllenNeuralDynamics/Aind.Experiment.VrForaging-Fip"


# ==============================================================================
# Coordinate System
# ==============================================================================
# Default coordinate system for VrForaging-FIP experiments
# NOTE: Ideally this would come from the extracted metadata, but we don't have it yet.
# Using standard Bregma-based coordinate system with anatomical axes
DEFAULT_COORDINATE_SYSTEM = {
    "object_type": "Coordinate system",
    "name": "origin",
    "origin": "Bregma",
    "axes": [
        {"object_type": "Axis", "name": "X", "direction": "Left_to_right"},
        {"object_type": "Axis", "name": "Y", "direction": "Anterior_to_posterior"},
        {"object_type": "Axis", "name": "Z", "direction": "Inferior_to_superior"},
    ],
    "axis_unit": "millimeter",
}
