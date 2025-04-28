"""Module defining JobSettings for Fiber Photometry ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional, Union

from aind_metadata_mapper.core_models import BaseJobSettings

# Map of color channel names to their common usage in the metadata
CHANNEL_COLOR_MAP = {"R": "red", "G": "green", "B": "blue", "Iso": "isosbestic"}

# Map of emission colors to their corresponding excitation wavelengths
EMISSION_TO_EXCITATION_MAP = {
    "red": 565,  # Red emission uses 565nm (yellow) excitation
    "green": 470,  # Green emission uses 470nm (blue) excitation
    "isosbestic": 415,  # Isosbestic emission uses 415nm (UV) excitation
}

# Map of LED wavelengths to their color names
LED_WAVELENGTH_NAMES = {
    "415nm": "uv",  # 415nm LED is the UV LED
    "470nm": "blue",  # 470nm LED is the Blue LED
    "565nm": "yellow",  # 565nm LED is the Yellow LED
}

# Update default channel props with proper filter info
DEFAULT_CHANNEL_PROPS = {
    "light_source_name": "LED",
    "filter_names": [],  # Will be populated based on emission path
    "detector_name": "FLIR CMOS",  # Will be set specifically based on color
    "excitation_power": None,  # Using None to indicate unknown power level
}

# Define filter mappings based on the rig definition
FILTER_MAPPINGS = {
    "red": [
        "Red emission bandpass filter",  # FF01-630/69-25 for red emission
        "Emission Dichroic",  # FF562-Di03-25x36
        "dual-edge standard epi-fluorescence dichroic beamsplitter",  # FF493/574-Di01-25x36
    ],
    "green": [
        "Green emission bandpass filter",  # FF01-520/35-25 for green emission
        "dual-edge standard epi-fluorescence dichroic beamsplitter",  # FF493/574-Di01-25x36
    ],
    "isosbestic": [
        "Green emission bandpass filter",  # FF01-520/35-25 (also used for isosbestic)
        "dual-edge standard epi-fluorescence dichroic beamsplitter",  # FF493/574-Di01-25x36
    ],
}

# Define excitation filter mappings
EXCITATION_FILTER_MAPPINGS = {
    "red": "Excitation filter 560nm",  # FB560-10
    "green": "Excitation filter 470nm",  # FB470-10
    "isosbestic": "Excitation filter 410nm",  # FB410-10
}

# Define detector mappings
DETECTOR_MAPPINGS = {
    "red": "FLIR CMOS for Red Channel",
    "green": "FLIR CMOS for Green Channel",
    "isosbestic": "FLIR CMOS for Green Channel",  # Isosbestic uses the same detector as green
}

# Define correct emission wavelengths for each channel type
EMISSION_WAVELENGTHS = {
    "red": 590,  # Red emission peak is 590nm
    "green": 510,  # Green emission peak is 510nm
    "isosbestic": 510,  # Isosbestic emission uses the same wavelength as green
}

# Define descriptions for each channel type with the correct emission peaks
CHANNEL_DESCRIPTIONS = {
    "red": "Red emission (590nm) with yellow excitation (565nm)",
    "green": "Green emission (510nm) with blue excitation (470nm)",
    "isosbestic": "Isosbestic control emission (510nm) with UV excitation (415nm)",
}

# Default values for disconnected fibers
DISCONNECTED_FIBER = {
    "fiber_name": "disconnected",
    "patch_cord_name": "disconnected",
    "patch_cord_output_power": None,
}

# Default Channel for disconnected fibers
DEFAULT_DISCONNECTED_CHANNEL = {
    "channel_name": "disconnected",
    "intended_measurement": None,
    "light_source_name": "None",
    "filter_names": ["None"],
    "detector_name": "None",
    "excitation_wavelength": 0,
    "excitation_power": 0,
}

class JobSettings(BaseJobSettings):
    """Settings for generating Fiber Photometry session metadata.

    Parameters
    ----------
    job_settings_name : Literal["FiberPhotometry"]
        Name of the job settings type, must be "FiberPhotometry"
    experimenter_full_name : List[str]
        List of experimenter names
    session_start_time : Optional[datetime], optional
        Start time of the session, by default None
    session_end_time : Optional[datetime], optional
        End time of the session, by default None
    subject_id : str
        Subject identifier
    rig_id : str
        Identifier for the experimental rig
    mouse_platform_name : str
        Name of the mouse platform used
    active_mouse_platform : bool
        Whether the mouse platform was active during the session
    data_streams : List[dict]
        List of data stream configurations
    session_type : str
        Type of session, defaults to "FIB"
    iacuc_protocol : str
        IACUC protocol identifier
    notes : str
        Session notes
    protocol_id : List[str], optional
        List of protocol identifiers, defaults to empty list
    data_directory : Optional[Union[str, Path]], optional
        Path to data directory containing fiber photometry files,
        by default None
    output_directory : Optional[Union[str, Path]], optional
        Output directory for generated files, by default None
    output_filename : str
        Name of output file, by default "session_fip.json"
    metadata_service_path : str
        Path to the intended measurements endpoint of the metadata service
    """

    job_settings_name: Literal["FiberPhotometry"] = "FiberPhotometry"

    experimenter_full_name: List[str]
    session_start_time: Optional[datetime] = None
    session_end_time: Optional[datetime] = None
    subject_id: str
    rig_id: str
    mouse_platform_name: str
    active_mouse_platform: bool
    data_streams: List[dict]
    session_type: str = "FIB"
    iacuc_protocol: str
    notes: str

    # Optional Session fields with defaults
    protocol_id: List[str] = []

    # Path to data directory containing fiber photometry files
    data_directory: Optional[Union[str, Path]] = None

    # Output directory and filename for generated files
    output_directory: Optional[Union[str, Path]] = None
    output_filename: str = "session_fip.json"

    # Fields for metadata-service
    metadata_service_path: str = "https://aind-metadata-service/intended_measurements"
