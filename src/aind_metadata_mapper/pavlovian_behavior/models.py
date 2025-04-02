"""Models for Pavlovian Behavior session metadata generation."""

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Literal, Dict, Any, Union

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Settings for generating Pavlovian Behavior session metadata.

    This model defines all required and optional parameters for creating
    standardized session metadata for Pavlovian conditioning experiments.
    Timing information can be extracted from data files if not explicitly
    provided.
    """

    job_settings_name: Literal["PavlovianBehavior"] = "PavlovianBehavior"

    # Required fields for session identification
    experimenter_full_name: List[str]
    subject_id: str
    rig_id: str
    iacuc_protocol: str

    # Session timing (can be extracted from data files)
    session_start_time: Optional[datetime] = None
    session_end_time: Optional[datetime] = None

    # Platform configuration
    mouse_platform_name: str
    active_mouse_platform: bool
    session_type: str = "Pavlovian_Conditioning"

    # Data paths
    data_directory: Union[str, Path]  # Required for data extraction
    output_directory: Optional[Union[str, Path]] = None
    output_filename: Optional[str] = None

    # Optional configuration
    notes: str = ""
    protocol_id: List[str] = []
    reward_units_per_trial: float = 2.0  # Default reward amount

    # Data containers (populated during ETL)
    data_streams: List[Dict[str, Any]] = []
    stimulus_epochs: List[Dict[str, Any]] = []

