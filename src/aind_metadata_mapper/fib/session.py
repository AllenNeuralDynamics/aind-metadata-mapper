"""Module to write valid OptoStim and Subject schemas"""

import datetime
import re
from dataclasses import dataclass
from pathlib import Path

from aind_data_schema.core.session import (
    DetectorConfig,
    FiberConnectionConfig,
    LightEmittingDiodeConfig,
    Session,
    Stream,
)
from aind_data_schema.models.modalities import Modality
from aind_data_schema.models.stimulus import (
    OptoStimulation,
    PulseShape,
    StimulusEpoch,
)

from aind_metadata_mapper.core import BaseEtl


@dataclass(frozen=True)
class ParsedInformation:
    """RawImageInfo gets parsed into this data"""

    teensy_str: str
    experiment_data: dict
    start_datetime: datetime


class FIBEtl(BaseEtl):
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
        output_directory: Path,
        teensy_str: str,
        experiment_data: dict,
        start_datetime: datetime,
        input_source: str = "",
    ):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        input_source : Union[str, PathLike]
          Can be a string or a Path
        output_directory : Path
          The directory where to save the json files.
        user_settings: UserSettings
          Variables for a particular session
        """
        super().__init__(input_source, output_directory)
        self.teensy_str = teensy_str
        self.experiment_data = experiment_data
        self.start_datetime = start_datetime

    def _transform(self, extracted_source: ParsedInformation) -> Session:
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

        experiment_data = extracted_source.experiment_data
        string_to_parse = extracted_source.teensy_str
        start_datetime = extracted_source.start_datetime

        labtracks_id = experiment_data["labtracks_id"]
        iacuc_protocol = experiment_data["iacuc"]
        rig_id = experiment_data["rig_id"]
        experimenter_full_name = experiment_data["experimenter_name"]
        mouse_platform_name = experiment_data["mouse_platform_name"]
        active_mouse_platform = experiment_data["active_mouse_platform"]
        light_source_list = experiment_data["light_source"]
        detector_list = experiment_data["detectors"]
        fiber_connections_list = experiment_data["fiber_connections"]
        session_type = experiment_data["session_type"]
        notes = experiment_data["notes"]

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
            pulse_frequency=frequency,
            number_pulse_trains=trial_num,
            pulse_width=pulse_width,
            pulse_train_duration=opto_duration,
            pulse_train_interval=opto_interval,
            baseline_duration=opto_base,
            fixed_pulse_train_interval=True,  # TODO: Check this is right
        )

        # create stimulus presentation instance
        experiment_duration = (
            opto_base + opto_duration + (opto_interval * trial_num)
        )
        end_datetime = start_datetime + datetime.timedelta(
            seconds=experiment_duration
        )
        stimulus_epochs = StimulusEpoch(
            stimulus=opto_stim,
            stimulus_start_time=start_datetime,
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
                stream_start_time=start_datetime,
                stream_end_time=end_datetime,
                light_sources=light_source,
                stream_modalities=[Modality.FIB],
                mouse_platform_name=mouse_platform_name,
                active_mouse_platform=active_mouse_platform,
                detectors=detectors,
                fiber_connections=fiber_connections,
            )
        ]

        # and finally, create ophys session
        ophys_session = Session(
            stimulus_epochs=[stimulus_epochs],
            subject_id=labtracks_id,
            iacuc_protocol=iacuc_protocol,
            session_start_time=start_datetime,
            session_end_time=end_datetime,
            rig_id=rig_id,
            experimenter_full_name=experimenter_full_name,
            session_type=session_type,
            notes=notes,
            data_streams=data_stream,
        )
        return ophys_session

    def _extract(self) -> ParsedInformation:
        """Extract metadata from fib session."""
        return ParsedInformation(
            teensy_str=self.teensy_str,
            experiment_data=self.experiment_data,
            start_datetime=self.start_datetime,
        )
