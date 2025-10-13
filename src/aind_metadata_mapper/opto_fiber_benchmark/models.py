"""Job Settings for Opto Fiber Benchmark"""

from typing import List, Literal, Optional

from pydantic import Field

from aind_metadata_mapper.core_models import BaseJobSettings
from aind_metadata_mapper.fip.models import JobSettings as FiberJobSettings


class OptoJobSettings(BaseJobSettings):
    """Parameters for extracting from raw data."""

    data_directory: str = Field(
        ...,
        description="Path to data directory",
    )

    job_settings_name: Literal["Optogenetics"] = "Optogenetics"

    # Optogenetics parameters
    stimulus_name: str = Field(default="OptoStim", description="Stimulus name")
    pulse_shape: str = Field(default="Square", title="Pulse shape")
    pulse_frequency: List[float] = Field(..., title="Pulse frequency (Hz)")
    number_pulse_trains: List[int] = Field(..., title="Number of pulse trains")
    pulse_width: List[int] = Field(..., title="Pulse width (ms)")

    pulse_train_duration: List[float] = Field(
        ..., title="Pulse train duration (s)"
    )
    fixed_pulse_train_interval: bool = Field(
        default=True, title="Fixed pulse train interval"
    )
    pulse_train_interval: Optional[float] = Field(
        ...,
        title="Pulse train interval (s)",
        description="Time between pulse trains",
    )
    baseline_duration: float = Field(
        ...,
        title="Baseline duration (s)",
        description="Duration of baseline recording prior to first pulse",
    )

    # Stimulus epoch laser configs
    wavelength: int = Field(..., title="Wavelength (nm)")
    laser_name: str = Field(..., title="Name of laser model")
    power: float = Field(..., title="Excitation power")


class JobSettings(BaseJobSettings):
    """Job Settings for combined opto fiber benchmark"""

    job_settings_name: Literal["Opto_Fiber_Benchmark"] = Field(
        default="Opto_Fiber_Benchmark", title="Name of the job settings"
    )
    fiber: FiberJobSettings = Field(default_factory=FiberJobSettings)
    opto: OptoJobSettings = Field(default_factory=OptoJobSettings)
