"""Session mapper for opto fiber benchmark"""

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
from aind_data_schema.components.devices import Software
from aind_data_schema.components.stimulus import OptoStimulation
from aind_data_schema.core.session import (
    DetectorConfig,
    FiberConnectionConfig,
    LaserConfig,
    LightEmittingDiodeConfig,
    Session,
    StimulusEpoch,
    Stream,
)
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.fip.session import FiberData
from aind_metadata_mapper.opto_fiber_benchmark.models import JobSettings


@dataclass
class OptoFiberBenchmarkModel:
    """
    Intermediate class to hold opto + fiber for metadata transformation
    """

    fiber_data: FiberData

    # Optogenetics stimulus epoch
    stimulus_epoch: StimulusEpoch


class OptoFiberBenchmark(GenericEtl[JobSettings]):
    """
    Extracts opto and fiber benchmark and transforms
    to session metadata
    """

    def __init__(self, job_settings: Union[JobSettings, str]):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        job_settings: Union[JobSettings, str]
          Variables for a particular session
        """

        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        else:
            job_settings_model = job_settings

        super().__init__(job_settings=job_settings_model)

    def _extract_session_start_time(
        self, data_files: List[Path]
    ) -> Optional[datetime]:
        """Extract session start  time from data files."""
        # Read the first data file to get timing information
        signal_file = data_files[0]
        df_signal = pd.read_csv(signal_file)

        # Try to find timestamp column
        timestamp_cols = [
            col for col in df_signal.columns if "ts" in col.lower()
        ]
        if timestamp_cols:
            start_time = datetime.strptime(
                signal_file.stem, "Signal_%Y-%m-%dT%H_%M_%S"
            )

            timestamps_signal = df_signal[timestamp_cols[0]]

            start_time = start_time + pd.to_timedelta(timestamps_signal.min())
            return start_time

        return None

    def _extract_stimulus_epochs(self, data_files: List[Path]) -> dict:
        """Extracts stimulus epoch information"""
        stim_file = data_files[1].stem

        # Parse directly with the format
        start_time = datetime.strptime(stim_file, "Stim_%Y-%m-%dT%H_%M_%S")
        start_time = start_time + pd.to_timedelta(
            self.job_settings.opto.baseline_duration, unit="s"
        )
        # Compute end time

        time = start_time
        # take into account multiple durations
        for duration in self.job_settings.opto.pulse_train_duration:
            time = time + pd.to_timedelta(
                (duration + self.job_settings.opto.pulse_train_interval)
                * self.job_settings.opto.number_pulse_trains[0]
                * len(self.job_settings.opto.pulse_frequency),
                unit="s",
            )

        end_time = time.isoformat()

        return {
            "stimulus_start_time": start_time.isoformat(),
            "stimulus_end_time": end_time,
            "stimulus_name": "OptoStim",
            "stimulus_modalities": ["Optogenetics"],
            "wavelength": self.job_settings.opto.wavelength,
            "power": self.job_settings.opto.power,
        }

    def _extract(self) -> OptoFiberBenchmarkModel:
        """Extracts data to intemediate model"""
        # Look for relevant data files
        if isinstance(self.job_settings.fiber.data_directory, str):
            self.job_settings.fiber.data_directory = Path(
                self.job_settings.fiber.data_directory
            )

        data_files = list(
            self.job_settings.fiber.data_directory.glob("*Signal*.csv")
        ) + list(self.job_settings.fiber.data_directory.glob("*Stim*.csv"))

        if not data_files:
            raise FileNotFoundError(
                "No data files found in "
                f"{self.job_settings.fiber.data_directory}"
            )

        stim_epoch_information = self._extract_stimulus_epochs(data_files)
        session_start_time = self._extract_session_start_time(data_files)

        stream_data = self.job_settings.fiber.data_streams[0]
        fiber_data = FiberData(
            start_time=session_start_time,
            end_time=stim_epoch_information["stimulus_end_time"],
            data_files=data_files,
            timestamps=[],
            light_source_configs=stream_data["light_sources"],
            detector_configs=stream_data["detectors"],
            fiber_configs=stream_data["fiber_connections"],
            subject_id=self.job_settings.fiber.subject_id,
            experimenter_full_name=(
                self.job_settings.fiber.experimenter_full_name
            ),
            rig_id=self.job_settings.fiber.rig_id,
            iacuc_protocol=self.job_settings.fiber.iacuc_protocol,
            notes=self.job_settings.fiber.notes,
            mouse_platform_name=self.job_settings.fiber.mouse_platform_name,
            active_mouse_platform=(
                self.job_settings.fiber.active_mouse_platform
            ),
            session_type=self.job_settings.fiber.session_type,
            anaesthesia=self.job_settings.fiber.anaesthesia,
            animal_weight_post=self.job_settings.fiber.animal_weight_post,
            animal_weight_prior=self.job_settings.fiber.animal_weight_prior,
        )

        stimulus_epoch = StimulusEpoch(
            stimulus_start_time=stim_epoch_information["stimulus_start_time"],
            stimulus_end_time=stim_epoch_information["stimulus_end_time"],
            stimulus_name=self.job_settings.opto.stimulus_name,
            stimulus_modalities=stim_epoch_information["stimulus_modalities"],
            stimulus_parameters=[
                OptoStimulation(
                    stimulus_name=self.job_settings.opto.stimulus_name,
                    pulse_shape=self.job_settings.opto.pulse_shape,
                    pulse_frequency=self.job_settings.opto.pulse_frequency,
                    number_pulse_trains=[
                        self.job_settings.opto.number_pulse_trains[0]
                        * len(self.job_settings.opto.pulse_frequency)
                        * len(self.job_settings.opto.pulse_train_duration)
                    ],
                    pulse_width=self.job_settings.opto.pulse_width,
                    pulse_train_duration=(
                        self.job_settings.opto.pulse_train_duration
                    ),
                    fixed_pulse_train_interval=(
                        self.job_settings.opto.fixed_pulse_train_interval
                    ),
                    pulse_train_interval=(
                        self.job_settings.opto.pulse_train_interval
                    ),
                    baseline_duration=self.job_settings.opto.baseline_duration,
                )
            ],
            light_source_config=[
                LaserConfig(
                    name=self.job_settings.opto.laser_name,
                    wavelength=self.job_settings.opto.wavelength,
                    excitation_power=self.job_settings.opto.power,
                )
            ],
            software=[
                Software(
                    name="FIP_DAQ_Control_IndicatorBenchmarking",
                    version="0.1.0",
                    url=str(
                        "https://github.com/AllenNeuralDynamics/"
                        "FIP_DAQ_Control_IndicatorBenchmarking"
                    ),
                )
            ],
            trials_total=self.job_settings.opto.number_pulse_trains[0],
        )

        return OptoFiberBenchmarkModel(
            fiber_data=fiber_data, stimulus_epoch=stimulus_epoch
        )

    def _transfrom(self, model: OptoFiberBenchmarkModel) -> Session:
        """Transform to session metadata schema object"""
        stream = Stream(
            stream_start_time=model.fiber_data.start_time,
            stream_end_time=model.fiber_data.end_time,
            light_sources=[
                LightEmittingDiodeConfig(**ls)
                for ls in model.fiber_data.light_source_configs
            ],
            stream_modalities=[Modality.FIB],
            detectors=[
                DetectorConfig(**d) for d in model.fiber_data.detector_configs
            ],
            fiber_connections=[
                FiberConnectionConfig(**fc)
                for fc in model.fiber_data.fiber_configs
            ],
            software=[
                Software(
                    name="FIP_DAQ_Control_IndicatorBenchmarking",
                    version="0.1.0",
                    url=str(
                        "https://github.com/AllenNeuralDynamics/"
                        "FIP_DAQ_Control_IndicatorBenchmarking"
                    ),
                )
            ],
        )

        session = Session(
            experimenter_full_name=model.fiber_data.experimenter_full_name,
            session_start_time=model.fiber_data.start_time,
            session_end_time=model.fiber_data.end_time,
            session_type=model.fiber_data.session_type,
            rig_id=model.fiber_data.rig_id,
            subject_id=model.fiber_data.subject_id,
            iacuc_protocol=model.fiber_data.iacuc_protocol,
            notes=model.fiber_data.notes,
            data_streams=[stream],
            mouse_platform_name=model.fiber_data.mouse_platform_name,
            active_mouse_platform=model.fiber_data.active_mouse_platform,
            animal_weight_post=model.fiber_data.animal_weight_post,
            animal_weight_prior=model.fiber_data.animal_weight_prior,
            stimulus_epochs=[model.stimulus_epoch],
        )
        return session

    def run_job(self) -> JobResponse:
        """Run ETL job for session metadata"""
        extracted_data = self._extract()
        transformed_data = self._transfrom(extracted_data)
        transformed_data.write_standard_file(
            output_directory=Path(
                self.job_settings.fiber.data_directory.parent
            )
        )

        return JobResponse(
            status_code=200,
            message=str(
                "Wrote model to "
                f"{self.job_settings.fiber.data_directory.parent}"
            ),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_json_path",
        type=str,
        required=True,
        help="Path to the config json file with parameters",
    )
    args = parser.parse_args()
    with open(Path(args.config_json_path), "r") as f:
        config = json.load(f)

    job_settings = JobSettings(**config)
    OptoFiberBenchmark(job_settings).run_job()
