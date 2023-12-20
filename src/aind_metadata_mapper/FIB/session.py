"""Module to write valid OptoStim and Subject schemas"""

import datetime
import json
import re
from enum import Enum
from pathlib import Path

from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.session import Session
from aind_data_schema.models.stimulus import (
    OptoStimulation,
    PulseShape,
    StimulusEpoch,
)


class StrEnum(str, Enum):
    """Base class for creating enumerated constants that are
    also subclasses of str"""

    pass


SRC_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SRC_DIR.parent


class StimulusName(StrEnum):
    """maps stimulus_name based on command"""

    o = "OptoStim10Hz"
    p = "OptoStim20Hz"
    q = "OptoStim5Hz"


class SchemaWriter:
    """This class contains the methods to write OphysScreening data"""

    @staticmethod
    def map_response_to_ophys_session(
        string_to_parse: str,
        experiment_data: dict,
        start_datetime: datetime,
    ):
        """Parses params from teensy string and creates ophys session model"""
        # Process data from dictionary keys
        labtracks_id = experiment_data["labtracks_id"]
        iacuc_protocol = experiment_data["iacuc"]
        rig_id = experiment_data["rig_id"]
        experimenter_full_name = experiment_data["experimenter_name"]
        output_path = experiment_data["save_dir"]
        session_type = experiment_data["session_type"]
        notes = experiment_data["notes"]

        # Define regular expressions to extract the values
        command_regex = r"Received command (\w)"
        frequency_regex = r"OptoStim\s*([0-9.]+)"
        trial_regex = r"OptoTrialN:\s*([0-9.]+)"
        pulse_regex = r"PulseW\(um\):\s*([0-9.]+)"
        duration_regex = r"OptoDuration\(s\):\s*([0-9.]+)"
        interval_regex = r"OptoInterval\(s\):\s*([0-9.]+)"
        base_regex = r"OptoBase\(s\):\s*([0-9.]+)"

        # Use regular expressions to extract the values
        frequency_match = re.search(frequency_regex, string_to_parse)
        trial_match = re.search(trial_regex, string_to_parse)
        pulse_match = re.search(pulse_regex, string_to_parse)
        duration_match = re.search(duration_regex, string_to_parse)
        interval_match = re.search(interval_regex, string_to_parse)
        base_match = re.search(base_regex, string_to_parse)
        command_match = re.search(command_regex, string_to_parse)

        # Store the float values as variables
        frequency = int(frequency_match.group(1))
        trial_num = int(trial_match.group(1))
        pulse_width = int(pulse_match.group(1))
        opto_duration = float(duration_match.group(1))
        opto_interval = float(interval_match.group(1))
        opto_base = float(base_match.group(1))

        # maps stimulus_name from command
        command = command_match.group(1)
        stimulus_name = getattr(StimulusName, command, "")

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
            data_streams=[],
        )

        # write to ophys session json
        ophys_session_path = str(
            output_path
            + f"/{labtracks_id}_"
            + start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
            + "_ophys_session.json"
        )

        with open(ophys_session_path, "w") as f:
            f.write(ophys_session.model_dump_json(indent=3))
            print(f"Saved session file to {ophys_session_path}")

    @staticmethod
    def map_to_ophys_rig(
        experiment_data: dict, start_datetime: datetime, reference_path: Path
    ):
        """Exports ophys rig based on rig id"""

        rig_id = experiment_data["rig_id"]
        output_path = experiment_data["save_dir"]
        labtracks_id = experiment_data["labtracks_id"]
        file_pattern = f"*{rig_id}*"
        matching_files = list(reference_path.glob(file_pattern))
        print("Log matching files: ", matching_files)

        # creates new instrument json based on local version and
        # renames with labtracks id
        for file_path in list(matching_files):
            ophys_rig_path = (
                output_path
                + f"/{labtracks_id}_"
                + start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
                + "_ophys_rig.json"
            )
            with open(file_path, "r") as f:
                instrument_json = f.read()
            inst = Instrument(**json.loads(instrument_json))
            with open(ophys_rig_path, "w") as f:
                f.write(inst.model_dump_json(indent=3))
            print(f"Saved rig file to {ophys_rig_path}")
