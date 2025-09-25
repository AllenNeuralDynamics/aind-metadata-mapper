"""Module defining JobSettings for Mesoscope ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal

from pydantic import Field

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["ISI"] = Field(
        default="ISI", title="Name of the job settings"
    )
    session_start_time: datetime = Field(description="Session starttime")
    session_end_time: datetime = Field(description="Session end time")
    input_source: Path = Field(description="Path to input file")
    experimenter_full_name: List[str] = Field(
        description="First and last name of user"
    )
    subject_id: str = Field(description="Mouse ID")
    local_timezone: str = Field(
        default="America/Los_Angeles", description="Timezone for the session"
    )
