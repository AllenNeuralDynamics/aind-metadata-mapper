"""Module defining JobSettings for FIP ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional

from pydantic import Field

from aind_metadata_mapper.core import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data that needs to be input by user."""

    job_settings_name: Literal["FIP"] = "FIP"
    output_directory: Optional[Path] = Field(
        default=None,
        description=(
            "Directory where to save the json file to. If None, then json"
            " contents will be returned in the Response message."
        ),
    )

    string_to_parse: str
    experimenter_full_name: List[str]
    session_start_time: datetime
    notes: str
    labtracks_id: str
    iacuc_protocol: str
    light_source_list: List[dict]
    detector_list: List[dict]
    fiber_connections_list: List[dict]

    rig_id: str = "ophys_rig"
    session_type: str = "Foraging_Photometry"
    mouse_platform_name: str = "Disc"
    active_mouse_platform: bool = False
