"""Module defining JobSettings for OpenEphys"""
from pathlib import Path
from typing import List, Literal
from aind_metadata_mapper.core_models import BaseJobSettings

class JobSettings(BaseJobSettings):
    """Data from openephys."""

    job_settings_name: Literal["OpenEphys"] = "OpenEphys"
    stage_logs: List[str]
    openephys_logs: List[str]
    experiment_data: dict
    input_source: str
    output_directory: Path