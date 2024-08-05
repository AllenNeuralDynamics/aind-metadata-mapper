"""Module to write valid OptoStim and Subject schemas"""

import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Union

from aind_data_schema.components.stimulus import OptoStimulation, PulseShape
from aind_data_schema.core.session import (
    DetectorConfig,
    FiberConnectionConfig,
    LightEmittingDiodeConfig,
    Session,
    StimulusEpoch,
    StimulusModality,
    Stream,
)
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl, JobResponse
from aind_metadata_mapper.fip.models import JobSettings


@dataclass(frozen=True)
class ParsedMetadata:
    """RawImageInfo gets parsed into this data"""

    teensy_str: str


class FIBEtl(GenericEtl[JobSettings]):
    """This class contains the methods to write OphysScreening data"""

    _dictionary_mapping = {
        "o": "OptoStim10Hz",
        "p": "OptoStim20Hz",
        "q": "OptoStim5Hz",
    }

    # Define regular expressions to extract the values
    command_regex = re.compile(r"Received command (\w)")
    frequency_regex = re.compile(r"OptoStim\s*([0-9.]+)")
    trial_regex = re.compile(r"OptoTrialN:\s*([0-9.]+)")
    pulse_regex = re.compile(r"PulseW\(um\):\s*([0-9.]+)")
    duration_regex = re.compile(r"OptoDuration\(s\):\s*([0-9.]+)")
    interval_regex = re.compile(r"OptoInterval\(s\):\s*([0-9.]+)")
    base_regex = re.compile(r"OptoBase\(s\):\s*([0-9.]+)")

    def __init__(
        self,
        job_settings: Union[JobSettings, str],
    ):
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

    def _transform(self, extracted_source: ParsedMetadata) -> Session:
        """
        Parses params from teensy string and creates ophys session model
        Parameters
        ----------
        extracted_source : ParsedInformation

        Returns
        -------
        Session

        """
        # Process data from dictionary keys

        string_to_parse = extracted_source.teensy_str

        session_start_time = self.job_settings.session_start_time
        labtracks_id = self.job_settings.labtracks_id
        iacuc_protocol = self.job_settings.iacuc_protocol
        rig_id = self.job_settings.rig_id
        experimenter_full_name = self.job_settings.experimenter_full_name
        mouse_platform_name = self.job_settings.mouse_platform_name
        active_mouse_platform = self.job_settings.active_mouse_platform
        light_source_list = self.job_settings.light_source_list
        detector_list = self.job_settings.detector_list
        fiber_connections_list = self.job_settings.fiber_connections_list
        session_type = self.job_settings.session_type
        notes = self.job_settings.notes

        # Use regular expressions to extract the values
        frequency_match = re.search(self.frequency_regex, string_to_parse)
        trial_match = re.search(self.trial_regex, string_to_parse)
        pulse_match = re.search(self.pulse_regex, string_to_parse)
        duration_match = re.search(self.duration_regex, string_to_parse)
        interval_match = re.search(self.interval_regex, string_to_parse)
        base_match = re.search(self.base_regex, string_to_parse)
        command_match = re.search(self.command_regex, string_to_parse)

        # Store the float values as variables
        frequency = int(frequency_match.group(1))
        trial_num = int(trial_match.group(1))
        pulse_width = int(pulse_match.group(1))
        opto_duration = float(duration_match.group(1))
        opto_interval = float(interval_match.group(1))
        opto_base = float(base_match.group(1))

        # maps stimulus_name from command
        command = command_match.group(1)
        stimulus_name = self._dictionary_mapping.get(command, "")

        # create opto stim instance
        opto_stim = OptoStimulation(
            stimulus_name=stimulus_name,
            pulse_shape=PulseShape.SQUARE,
            pulse_frequency=[
                frequency,
            ],
            number_pulse_trains=[
                trial_num,
            ],
            pulse_width=[
                pulse_width,
            ],
            pulse_train_duration=[
                opto_duration,
            ],
            pulse_train_interval=opto_interval,
            baseline_duration=opto_base,
            fixed_pulse_train_interval=True,  # TODO: Check this is right
        )

        # create stimulus presentation instance
        experiment_duration = (
            opto_base + opto_duration + (opto_interval * trial_num)
        )
        end_datetime = session_start_time + timedelta(
            seconds=experiment_duration
        )
        stimulus_epochs = StimulusEpoch(
            stimulus_name=stimulus_name,
            stimulus_modalities=[StimulusModality.OPTOGENETICS],
            stimulus_parameters=[
                opto_stim,
            ],
            stimulus_start_time=session_start_time,
            stimulus_end_time=end_datetime,
        )

        # create light source instance
        light_source = []
        for ls in light_source_list:
            diode = LightEmittingDiodeConfig(**ls)
            light_source.append(diode)

        # create detector instance
        detectors = []
        for d in detector_list:
            camera = DetectorConfig(**d)
            detectors.append(camera)

        # create fiber connection instance
        fiber_connections = []
        for fc in fiber_connections_list:
            cord = FiberConnectionConfig(**fc)
            fiber_connections.append(cord)
        data_stream = [
            Stream(
                stream_start_time=session_start_time,
                stream_end_time=end_datetime,
                light_sources=light_source,
                stream_modalities=[Modality.FIB],
                detectors=detectors,
                fiber_connections=fiber_connections,
            )
        ]
        # and finally, create ophys session
        ophys_session = Session(
            stimulus_epochs=[stimulus_epochs],
            subject_id=labtracks_id,
            iacuc_protocol=iacuc_protocol,
            session_start_time=session_start_time,
            session_end_time=end_datetime,
            rig_id=rig_id,
            experimenter_full_name=experimenter_full_name,
            session_type=session_type,
            notes=notes,
            data_streams=data_stream,
            mouse_platform_name=mouse_platform_name,
            active_mouse_platform=active_mouse_platform,
        )

        return ophys_session

    def _extract(self) -> ParsedMetadata:
        """Extract metadata from fip session."""

        tensy_str = self.job_settings.string_to_parse

        return ParsedMetadata(
            teensy_str=tensy_str,
        )

    def run_job(self) -> JobResponse:
        """Run the etl job and return a JobResponse."""
        extracted = self._extract()
        transformed = self._transform(extracted_source=extracted)
        job_response = self._load(
            transformed, self.job_settings.output_directory
        )
        return job_response
