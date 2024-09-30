"""Module defining JobSettings for Mesoscope ETL"""

from typing import Literal, Union
from pathlib import Path
from aind_metadata_mapper.core_models import BaseJobSettings

DEFAULT_OPTO_CONDITIONS = {
    "0": {
        "duration": 0.01,
        "name": "1Hz_10ms",
        "condition": "10 ms pulse at 1 Hz",
    },
    "1": {
        "duration": 0.002,
        "name": "1Hz_2ms",
        "condition": "2 ms pulse at 1 Hz",
    },
    "2": {
        "duration": 1.0,
        "name": "5Hz_2ms",
        "condition": "2 ms pulses at 5 Hz",
    },
    "3": {
        "duration": 1.0,
        "name": "10Hz_2ms",
        "condition": "2 ms pulses at 10 Hz",
    },
    "4": {
        "duration": 1.0,
        "name": "20Hz_2ms",
        "condition": "2 ms pulses at 20 Hz",
    },
    "5": {
        "duration": 1.0,
        "name": "30Hz_2ms",
        "condition": "2 ms pulses at 30 Hz",
    },
    "6": {
        "duration": 1.0,
        "name": "40Hz_2ms",
        "condition": "2 ms pulses at 40 Hz",
    },
    "7": {
        "duration": 1.0,
        "name": "50Hz_2ms",
        "condition": "2 ms pulses at 50 Hz",
    },
    "8": {
        "duration": 1.0,
        "name": "60Hz_2ms",
        "condition": "2 ms pulses at 60 Hz",
    },
    "9": {
        "duration": 1.0,
        "name": "80Hz_2ms",
        "condition": "2 ms pulses at 80 Hz",
    },
    "10": {
        "duration": 1.0,
        "name": "square_1s",
        "condition": "1 second square pulse: continuously on for 1s",
    },
    "11": {"duration": 1.0, "name": "cosine_1s", "condition": "cosine pulse"},
}


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["OpenEphys"] = "OpenEphys"
    session_type: str
    project_name: str
    iacuc_protocol: str
    description: str
    sessions_root: Union[Path, str] = "//allen/programs/mindscope/workgroups/np-exp"
    opto_conditions_map: dict = DEFAULT_OPTO_CONDITIONS
    overwrite_tables: bool = False
    mtrain_server: str = "http://mtrain:5000"
    #TODO: refactor sessions_root and camstimephys getfolder functions out and use this instead
    input_source: str = 'blah'
    active_mouse_platform: bool = False
    mouse_platform_name: str = 'Mouse Platform'

    # experimenter_full_name: List[str] = Field(
    #     ..., title="Full name of the experimenter"
    # )
    mouse_platform_name: str = "disc"
