"""Module defining JobSettings for Mesoscope ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional

from pydantic import Field, field_validator

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data to be entered by the user."""

    job_settings_name: Literal["Mesoscope"] = Field(
        default="Mesoscope", title="Name of the job settings"
    )
    input_source: Path = Field(..., title="Path to the input source")
    session_id: str = Field(..., title="ID of the session")
    behavior_source: Path = Field(..., title="Path to the behavior source")
    make_camsitm_dir: bool = Field(
        default=False, title="Make camsitm directory"
    )
    output_directory: Path = Field(..., title="Path to the output directory")
    session_start_time: datetime = Field(
        ..., title="Start time of the session"
    )
    session_end_time: datetime = Field(..., title="End time of the session")
    subject_id: str = Field(..., title="ID of the subject")
    project: str = Field(..., title="Name of the project")
    iacuc_protocol: str = Field(default="2115", title="IACUC protocol number")
    magnification: str = Field(default="16x", title="Magnification")
    fov_coordinate_ml: float = Field(
        default=1.5, title="Coordinate in ML direction"
    )
    fov_coordinate_ap: float = Field(
        default=1.5, title="Coordinate in AL direction"
    )
    fov_reference: str = Field(
        default="Bregma", title="Reference point for the FOV"
    )
    experimenter_full_name: List[str] = Field(
        title="Full name of the experimenter"
    )
    mouse_platform_name: str = Field(
        default="disc", title="Name of the mouse platform"
    )
    optional_output: Optional[Path] = Field(
        default=None, title="Optional output path"
    )

    @field_validator("input_source", "behavior_source", "output_directory")
    @classmethod
    def validate_path_is_dir(cls, v):
        """Validate that the input source is a directory"""
        if not v.is_dir():
            raise ValueError(f"{v} is not a directory")
        return v
