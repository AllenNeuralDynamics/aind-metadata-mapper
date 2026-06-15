"""Constants for the exaSPIM metadata mapper."""

# Schema version threshold — anything below this must be upgraded.
V2_THRESHOLD = "2.0.0"

# Case-insensitive keywords used to detect exaSPIM instruments.
EXASPIM_INSTRUMENT_KEYWORDS: tuple[str, ...] = ("exaspim",)

# Instrument JSON keys that carry a ``manufacturer`` dict.
DEVICE_LISTS: tuple[str, ...] = (
    "objectives",
    "detectors",
    "light_sources",
    "fluorescence_filters",
    "lenses",
    "scanning_stages",
    "motorized_stages",
    "additional_devices",
    "daqs",
    "optical_tables",
)

# Fields on MotorizedStage that were valid in v1 but removed in v2.
DEPRECATED_MOTORIZED_FIELDS: tuple[str, ...] = (
    "stage_axis_direction",
    "stage_axis_name",
)

# Allowed ``travel_unit`` values for stages in v2.
VALID_TRAVEL_UNITS: set[str] = {
    "meter",
    "centimeter",
    "millimeter",
    "micrometer",
    "nanometer",
    "inch",
    "pixel",
}

# Canonical ``stage_axis_direction`` values in v2.
VALID_AXIS_DIRECTIONS: set[str] = {
    "Detection axis",
    "Illumination axis",
    "Perpendicular axis",
}

# Maps lowercase keywords in free-form direction strings → canonical values.
# Order matters: more specific keywords must come first (e.g.
# "perpendicular" before "detection" so that "Perpendicular to detection"
# maps to "Perpendicular axis", not "Detection axis").
AXIS_DIRECTION_MAP: dict[str, str] = {
    "perpendicular": "Perpendicular axis",
    "illumination": "Illumination axis",
    "detection": "Detection axis",
}

# Maps legacy v1 filter_type strings to the v2 canonical forms accepted
# by aind-data-schema-models.  The upgrader does not normalise these.
FILTER_TYPE_MAP: dict[str, str] = {
    "Bandpass": "Band pass",
    "Band Pass": "Band pass",
    "Longpass": "Long pass",
    "Long Pass": "Long pass",
    "Shortpass": "Short pass",
    "Short Pass": "Short pass",
}

# Allowed ``stage_axis_name`` values for scanning stages in v2.
VALID_STAGE_AXIS_NAMES: set[str] = {
    "X", "Y", "Z", "AP", "ML", "SI", "Depth",
}

# Maps ``instrument.id`` from ``instrument_config.yaml`` to the
# corresponding reference instrument JSON filename bundled in
# ``src/aind_metadata_mapper/exaspim/instruments/``.
INSTRUMENT_ID_MAP: dict[str, str] = {
    "exaspim-01": "beta02_instrument.json",
    "exaspim-1x": "1x_instrument.json",
}
