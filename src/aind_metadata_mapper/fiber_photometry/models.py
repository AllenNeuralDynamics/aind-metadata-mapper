"""Module defining JobSettings for Fiber Photometry ETL"""

from datetime import datetime
from typing import List, Literal, Optional

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Settings for generating Fiber Photometry session metadata."""

    job_settings_name: Literal["fib"] = "fib"

    experimenter_full_name: List[str]
    session_start_time: datetime
    session_end_time: Optional[datetime] = None
    subject_id: str
    rig_id: str
    mouse_platform_name: str
    active_mouse_platform: bool
    data_streams: List[dict]
    session_type: str = "Fiber_Photometry"
    iacuc_protocol: str
    notes: str

    # Optional Session fields with defaults
    protocol_id: List[str] = []
