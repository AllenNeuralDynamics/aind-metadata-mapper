"""Module defining JobSettings for SmartSPIM ETL"""

from pathlib import Path
from typing import Literal, Optional

from aind_metadata_mapper.core import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    # Field can be used to switch between different acquisition etl jobs
    job_settings_name: Literal["SmartSPIM"] = "SmartSPIM"

    subject_id: str
    raw_dataset_path: Path
    output_directory: Optional[Path] = None

    # Metadata names
    asi_filename: str = "derivatives/ASI_logging.txt"
    mdata_filename_json: str = "derivatives/metadata.json"

    # Metadata provided by microscope operators
    processing_manifest_path: str = "derivatives/processing_manifest.json"
