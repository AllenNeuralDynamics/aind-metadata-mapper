"""Module defining JobSettings for Mesoscope ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Union

from pydantic import Field

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["OpenEphys"] = "OpenEphys"
    session_type: str
    subject_id: str
    project_name: str
    iacuc_protocol: str
    description: str
    # experimenter_full_name: List[str] = Field(
    #     ..., title="Full name of the experimenter"
    # )
    mouse_platform_name: str = "disc"
