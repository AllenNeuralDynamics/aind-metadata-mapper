"""Models for Pavlovian Behavior session metadata generation.

This module provides data models used in the ETL process for generating
standardized session metadata from Pavlovian conditioning experiments.

The models define the structure and validation rules for:
- Job configuration settings
- Required and optional parameters
- Data containers for extracted information
"""

from datetime import datetime
from pathlib import Path
from typing import List, Optional, Literal, Dict, Any, Union

from aind_data_schema_models.units import VolumeUnit
from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Settings for generating Pavlovian Behavior session metadata.

    This model defines all required and optional parameters for creating
    standardized session metadata for Pavlovian conditioning experiments.
    Inherits from BaseJobSettings to provide core functionality.

    Parameters
    ----------
    job_settings_name : Literal["PavlovianBehavior"]
        Identifier for this job settings type, fixed as "PavlovianBehavior"
    experimenter_full_name : List[str]
        List of full names of experimenters involved in the session
    subject_id : str
        Unique identifier for the experimental subject
    rig_id : str
        Identifier for the experimental apparatus
    iacuc_protocol : str
        IACUC protocol number for the experiment
    session_start_time : Optional[datetime], optional
        Start time of the session, can be extracted from data files
    session_end_time : Optional[datetime], optional
        End time of the session, can be extracted from data files
    mouse_platform_name : str
        Name of the mouse platform used
    active_mouse_platform : bool
        Whether the mouse platform was active during the session
    session_type : str, optional
        Type of session, defaults to "Pavlovian_Conditioning"
    data_directory : Union[str, Path]
        Directory containing the raw data files
    output_directory : Optional[Union[str, Path]], optional
        Directory where output files should be written
    output_filename : Optional[str], optional
        Name for the output file
    notes : str, optional
        Additional notes about the session
    protocol_id : List[str], optional
        List of protocol identifiers
    reward_units_per_trial : float, optional
        Amount of reward given per successful trial, defaults to 2.0
    reward_consumed_unit : VolumeUnit, optional
        Unit for reward measurement, defaults to microliters
    data_streams : List[Dict[str, Any]], optional
        Container for data stream configurations
    stimulus_epochs : List[Dict[str, Any]], optional
        Container for stimulus epoch information

    Notes
    -----
    This model is used throughout the ETL process to:
    - Validate input parameters
    - Store extracted timing information
    - Configure output file locations
    - Track reward and stimulus configurations

    The model supports both manual configuration and automatic extraction
    of certain fields from data files.
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

    # Reward configuration
    reward_units_per_trial: float = 2.0  # Default reward amount
    reward_consumed_unit: VolumeUnit = VolumeUnit.UL  # Default to microliters

    # Data containers (populated during ETL)
    data_streams: List[Dict[str, Any]] = []
    stimulus_epochs: List[Dict[str, Any]] = []
