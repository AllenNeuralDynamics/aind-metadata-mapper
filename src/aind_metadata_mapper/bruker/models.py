"""Module defining JobSettings for Bruker ETL"""

from pathlib import Path
from typing import List, Literal, Optional

from aind_data_schema.components.devices import (
    MagneticStrength,
    ScannerLocation,
)
from pydantic import Field
from pydantic_settings import BaseSettings


class JobSettings(BaseSettings):
    """Data that needs to be input by user."""

    job_settings_name: Literal["Bruker"] = "Bruker"
    data_path: Path
    output_directory: Optional[Path] = Field(
        default=None,
        description=(
            "Directory where to save the json file to. If None, then json"
            " contents will be returned in the Response message."
        ),
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
