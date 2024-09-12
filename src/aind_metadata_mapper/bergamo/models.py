"""Module defining JobSettings for Bergamo ETL"""

from decimal import Decimal
from typing import List, Literal, Optional

from aind_metadata_mapper.core import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data that needs to be input by user. Can be pulled from env vars with
    BERGAMO prefix or set explicitly."""

    job_settings_name: Literal["Bergamo"] = "Bergamo"
    # mandatory fields:
    experimenter_full_name: List[str]
    subject_id: str
    imaging_laser_wavelength: int  # user defined
    fov_imaging_depth: int
    fov_targeted_structure: str
    notes: Optional[str]

    # fields with default values
    mouse_platform_name: str = "Standard Mouse Tube"  # FROM RIG JSON
    active_mouse_platform: bool = False
    session_type: str = "BCI"
    iacuc_protocol: str = "2109"
    # should match rig json:
    rig_id: str = "442 Bergamo 2p photostim"
    behavior_camera_names: List[str] = [
        "Side Face Camera",
        "Bottom Face Camera",
    ]
    ch1_filter_names: List[str] = [
        "Green emission filter",
        "Emission dichroic",
    ]
    ch1_detector_name: str = "Green PMT"
    ch1_daq_name: str = "PXI"
    ch2_filter_names: List[str] = ["Red emission filter", "Emission dichroic"]
    ch2_detector_name: str = "Red PMT"
    ch2_daq_name: str = "PXI"
    imaging_laser_name: str = "Chameleon Laser"

    photostim_laser_name: str = "Monaco Laser"
    stimulus_device_names: List[str] = ["speaker", "lickport"]  # FROM RIG JSON
    photostim_laser_wavelength: int = 1040
    fov_coordinate_ml: Decimal = Decimal("1.5")
    fov_coordinate_ap: float = Decimal("1.5")
    fov_reference: str = "Bregma"

    starting_lickport_position: List[float] = [
        0,
        -6,
        0,
    ]  # in mm from face of the mouse
    behavior_task_name: str = "single neuron BCI conditioning"
    hit_rate_trials_0_10: Optional[float] = None
    hit_rate_trials_20_40: Optional[float] = None
    total_hits: Optional[float] = None
    average_hit_rate: Optional[float] = None
    trial_num: Optional[float] = None
    # ZoneInfo object doesn't serialize well, so we can define it as a str
    timezone: str = "US/Pacific"

    class Config:
        """Config to set env var prefix to BERGAMO"""

        env_prefix = "BERGAMO_"
