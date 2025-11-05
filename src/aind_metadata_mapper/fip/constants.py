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
EMISSION_GREEN = (
    520  # Green emission: center of 490-540nm bandpass, ~510nm GFP peak
)
EMISSION_RED = 590  # Red emission: ~590nm RFP peak

# ==============================================================================
# Camera Exposure Time
# ==============================================================================
CAMERA_EXPOSURE_TIME_MICROSECONDS_PER_MILLISECOND = 1000

# ==============================================================================
# Device Names
# ==============================================================================
CONTROLLER_NAME = "cuTTLefishFip"

# ==============================================================================
# Device Name Prefixes and Keywords
# ==============================================================================
LED_PREFIX = "LED_"
PATCH_CORD_PREFIX = "Patch Cord"
FIBER_PREFIX = "Fiber"
LIGHT_SOURCE_PREFIX = "light_source_"
CAMERA_PREFIX = "camera_"

# ==============================================================================
# Channel Types
# ==============================================================================
CHANNEL_TYPE_GREEN = "Green"
CHANNEL_TYPE_ISOSBESTIC = "Isosbestic"
CHANNEL_TYPE_RED = "Red"

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
DEFAULT_ACQUISITION_TYPE = "FIP"
DEFAULT_LED_POWER = 1.0

# ==============================================================================
# LED Wavelength Mapping
# ==============================================================================
# Maps LED name keywords to their excitation wavelengths
LED_WAVELENGTH_MAP = {
    "uv": EXCITATION_UV,
    "415": EXCITATION_UV,
    "blue": EXCITATION_BLUE,
    "470": EXCITATION_BLUE,
    "yellow": EXCITATION_YELLOW,
    "lime": EXCITATION_YELLOW,
    "565": EXCITATION_YELLOW,
    "560": EXCITATION_YELLOW,
}
