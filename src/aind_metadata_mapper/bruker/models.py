"""Module defining JobSettings for Bruker ETL"""
from pathlib import Path
from typing import List, Literal, Optional, Union

from aind_data_schema.components.devices import (
    MagneticStrength,
    ScannerLocation,
)
from pydantic import Field

from aind_metadata_mapper.core import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data that needs to be input by user."""

    job_settings_name: Literal["Bruker"] = "Bruker"
    data_path: Optional[Union[Path, str]] = Field(
        default=None, description=("Deprecated, use input_source instead.")
    )
    experimenter_full_name: List[str]
    protocol_id: str = Field(default="", description="Protocol ID")
    collection_tz: str = Field(
        default="America/Los_Angeles",
        description="Timezone string of the collection site",
    )
    session_type: str
    primary_scan_number: int
    setup_scan_number: int
    scanner_name: str
    scan_location: ScannerLocation
    magnetic_strength: MagneticStrength
    subject_id: str
    iacuc_protocol: str
    session_notes: str
