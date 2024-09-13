"""Module defining JobSettings for Mesoscope ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Union

from pydantic import Field

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["Mesoscope"] = "Mesoscope"
    # TODO: Can probably put this as part of input_source
    behavior_source: Union[Path, str]
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
