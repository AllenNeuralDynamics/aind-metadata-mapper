"""Module defining JobSettings for Fiber Photometry ETL"""

from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional, Union

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Settings for generating Fiber Photometry session metadata.

    Parameters
    ----------
    job_settings_name : Literal["FiberPhotometry"]
        Name of the job settings type, must be "FiberPhotometry"
    experimenter_full_name : List[str]
        List of experimenter names
    session_start_time : Optional[datetime], optional
        Start time of the session, by default None
    session_end_time : Optional[datetime], optional
        End time of the session, by default None
    subject_id : str
        Subject identifier
    rig_id : str
        Identifier for the experimental rig
    mouse_platform_name : str
        Name of the mouse platform used
    active_mouse_platform : bool
        Whether the mouse platform was active during the session
    data_streams : List[dict]
        List of data stream configurations
    session_type : str
        Type of session, defaults to "FIB"
    iacuc_protocol : str
        IACUC protocol identifier
    notes : str
        Session notes
    protocol_id : List[str], optional
        List of protocol identifiers, defaults to empty list
    data_directory : Optional[Union[str, Path]], optional
        Path to data directory containing fiber photometry files,
        by default None
    output_directory : Optional[Union[str, Path]], optional
        Output directory for generated files, by default None
    output_filename : str
        Name of output file, by default "session_fip.json"
    """

    job_settings_name: Literal["FiberPhotometry"] = "FiberPhotometry"

    experimenter_full_name: List[str]
    session_start_time: Optional[datetime] = None
    session_end_time: Optional[datetime] = None
    subject_id: str
    rig_id: str
    mouse_platform_name: str
    active_mouse_platform: bool
    data_streams: List[dict]
    session_type: str = "FIB"
    iacuc_protocol: str
    notes: str

    # Optional Session fields with defaults
    protocol_id: List[str] = []

    # Path to data directory containing fiber photometry files
    data_directory: Optional[Union[str, Path]] = None

    # Output directory and filename for generated files
    output_directory: Optional[Union[str, Path]] = None
    output_filename: str = "session_fip.json"
