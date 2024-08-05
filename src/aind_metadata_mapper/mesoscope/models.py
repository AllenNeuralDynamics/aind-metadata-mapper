"""Module defining JobSettings for Mesoscope ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal

from pydantic import Field
from pydantic_settings import BaseSettings


class JobSettings(BaseSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["Mesoscope"] = "Mesoscope"
    input_source: Path
    behavior_source: Path
    output_directory: Path
    session_start_time: datetime
    session_end_time: datetime
    subject_id: str
    project: str
    iacuc_protocol: str = "2115"
    magnification: str = "16x"
    fov_coordinate_ml: float = 1.5
    fov_coordinate_ap: float = 1.5
    fov_reference: str = "Bregma"
    experimenter_full_name: List[str] = Field(
        ..., title="Full name of the experimenter"
    )
    mouse_platform_name: str = "disc"
