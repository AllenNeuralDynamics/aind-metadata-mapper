"""Module defining JobSettings for FIP ETL"""

from datetime import datetime
from typing import List, Literal

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data that needs to be input by user."""

    job_settings_name: Literal["FIP"] = "FIP"

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
