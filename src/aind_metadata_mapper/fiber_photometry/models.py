"""Module defining JobSettings for Fiber Photometry ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional, Union

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Settings for generating Fiber Photometry session metadata."""

    job_settings_name: Literal["FiberPhotometry"] = "FiberPhotometry"

    experimenter_full_name: List[str]
    session_start_time: Optional[datetime] = None
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

    # Path to data directory containing fiber photometry files
    data_directory: Optional[Union[str, Path]] = None

    # Output directory and filename for generated files
    output_directory: Optional[Union[str, Path]] = None
    output_filename: Optional[str] = None


# Debug print to verify class definition
print("\nDebug - JobSettings class definition:")
print(
    f"  job_settings_name: {getattr(JobSettings, 'job_settings_name', 'Not found')}"
)
print(
    f"  session_start_time is optional: {JobSettings.__annotations__['session_start_time']}"
)
