"""Module defining JobSettings for Mesoscope ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional

from pydantic import Field, field_validator

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["ISI"] = Field(
        default="ISI", title="Name of the job settings"
    )
    input_source: Path = Field(description="Path to input file")
    experimenter_full_name: List[str] = Field(description="First and last name of user")
    subject_id: str = Field(description="Mouse ID")
    local_timezone: str = Field(
        default="America/Los_Angeles",
        description="Timezone for the session"
    )