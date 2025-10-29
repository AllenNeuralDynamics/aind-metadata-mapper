"""Module defining JobSettings for Mesoscope ETL"""

from typing import Literal

from aind_metadata_mapper.core_models import BaseJobSettings

DEFAULT_OPTO_CONDITIONS = {
    "0": {
        "duration": 1.0,
        "name": "fast_pulses",
        "condition": "2.5 ms pulses at 10 Hz",
    },
    "1": {
        "duration": 0.005,
        "name": "pulse",
        "condition": "a single square pulse",
    },
    "2": {
        "duration": 0.01,
        "name": "pulse",
        "condition": "a single square pulse",
    },
    "3": {
        "duration": 1.0,
        "name": "raised_cosine",
        "condition": "half-period of a cosine wave",
    },
    "4": {
        "duration": 1.0,
        "name": "5 hz pulse train",
        "condition": "Each pulse is 10 ms wide",
    },
    "5": {
        "duration": 1.0,
        "name": "40 hz pulse train",
        "condition": "Each pulse is 6 ms wide",
    },
}


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["OpenEphys"] = "OpenEphys"
    session_type: str
    project_name: str
    iacuc_protocol: str
    description: str
    opto_conditions_map: dict = DEFAULT_OPTO_CONDITIONS
    overwrite_tables: bool = False
    mtrain_server: str
    session_id: str
    active_mouse_platform: bool = False
    mouse_platform_name: str = "Mouse Platform"
