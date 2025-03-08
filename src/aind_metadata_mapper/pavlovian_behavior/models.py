"""Models for Pavlovian Behavior session metadata generation."""

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Literal, Dict, Any, Union

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Settings for generating Pavlovian Behavior session metadata."""

    job_settings_name: Literal["PavlovianBehavior"] = "PavlovianBehavior"

    # Required fields
    experimenter_full_name: List[str]
    session_start_time: Optional[datetime] = (
        None  # Can be extracted from data files
    )
    session_end_time: Optional[datetime] = None
    subject_id: str
    rig_id: str
    task_version: str
    iacuc_protocol: str
    mouse_platform_name: str
    active_mouse_platform: bool
    session_type: str

    # Path to data directory containing behavior files
    data_directory: Union[str, Path]

    # Output directory and filename for generated files
    output_directory: Optional[Union[str, Path]] = None
    output_filename: Optional[str] = None

    # Optional fields with defaults
    notes: str = ""
    protocol_id: List[str] = []

    # Stimulus epoch information - can be populated from data files
    stimulus_epochs: List[Dict[str, Any]] = []

    @classmethod
    def from_args(cls, args: List[str]):
        """Create JobSettings from command line arguments.

        Args:
            args: Command line arguments

        Returns:
            JobSettings object
        """
        if len(args) != 1:
            raise ValueError(
                "Expected a single argument: path to job settings JSON file"
            )

        import json
        from pathlib import Path

        settings_path = Path(args[0])
        if not settings_path.exists():
            raise FileNotFoundError(
                f"Settings file not found: {settings_path}"
            )

        with open(settings_path, "r") as f:
            settings_data = json.load(f)

        return cls(**settings_data)
