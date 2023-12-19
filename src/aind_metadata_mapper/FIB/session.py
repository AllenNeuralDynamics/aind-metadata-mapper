"""Module to write valid OptoStim and Subject schemas"""

import shutil
from aind_data_schema.models.stimulus import OptoStimulation, PulseShape, StimulusEpoch
from aind_data_schema.models.devices import LightEmittingDiode
from aind_data_schema.core.session import Session
from typing import Optional
import re
import datetime
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SRC_DIR.parent
RIGS_DIR = PROJECT_DIR / 'resources' / 'rigs'


class SchemaWriter:
    """This class contains the methods to write OphysScreening data"""

    @staticmethod
    def _map_stimulus_name(command: str) -> Optional[str]:
        """maps stimulus_name based on command"""
        if command == "o":
            stimulus_name = "OptoStim10Hz"
        elif command == "p":
            stimulus_name = "OptoStim20Hz"
        elif command == "q":
            stimulus_name = "OptoStim5Hz"
        else:
            stimulus_name = None
        return stimulus_name

    def map_response_to_ophys_session(self, string_to_parse: str, experiment_data: dict, start_datetime: datetime):
        """Parses params from teensy string and creates ophys session model"""
        # Process data from dictionary keys
        labtracks_id = experiment_data['labtracks_id']
        iacuc_protocol = experiment_data['iacuc']
        rig_id = experiment_data['rig_id']
        experimenter_full_name = experiment_data['experimenter_name']
        output_path = experiment_data['save_dir']
        light_source_list = experiment_data['light_source']
        excitation_power_list = experiment_data['light_excitation_power']
        session_type = experiment_data['session_type']
        notes = experiment_data['notes']

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
        stimulus_name = self._map_stimulus_name(command)

        # create opto stim instance
        opto_stim = OptoStimulation(
            stimulus_name=stimulus_name,
            pulse_shape=PulseShape.SQUARE,
            pulse_frequency=frequency,
            number_pulse_trains=trial_num,
            pulse_width=pulse_width,
            pulse_train_duration=opto_duration,
            pulse_train_interval=opto_interval,
            baseline_duration=opto_base
        )

        # create stimulus presentation instance
        experiment_duration = opto_base + opto_duration + (opto_interval*trial_num)
        end_datetime = start_datetime + datetime.timedelta(seconds=experiment_duration)
        stimulus_presentation = StimulusEpoch(
            stimulus=opto_stim,
            start_time=start_datetime.time(),
            end_time=end_datetime.time()

        )

        # create light source instance
        light_source=[]
        for ls in light_source_list:
            for ep in excitation_power_list:
                diode = LightEmittingDiode(
                    name=ls,
                    excitation_power=ep,
                )
                light_source.append(diode)

        # and finally, create ophys session
        ophys_session = Session(
            stimulus_presentations=[stimulus_presentation],
            subject_id=labtracks_id,
            iacuc_protocol=iacuc_protocol,
            session_start_time=start_datetime,
            session_end_time=end_datetime,
            rig_id=rig_id,
            experimenter_full_name=experimenter_full_name,
            light_sources=light_source,
            session_type=session_type,
            notes=notes,
        )

        # write to ophys session json
        ophys_session_path = str(output_path + f"/{labtracks_id}_" + start_datetime.strftime('%Y-%m-%d_%H-%M-%S') + "_ophys_session.json")

        with open(ophys_session_path, "w") as f:
            f.write(ophys_session.json(indent=3))
            print(f"Saved session file to {ophys_session_path}")

    @staticmethod
    def map_to_ophys_rig(experiment_data: dict, start_datetime: datetime):
        """Exports ophys rig based on rig id"""
        rig_id = experiment_data['rig_id']
        output_path = experiment_data['save_dir']
        labtracks_id = experiment_data['labtracks_id']
        file_pattern = f"*{rig_id}*"
        matching_files = RIGS_DIR.glob(file_pattern)
        print("Log matching files: ", matching_files)

        # saves copy of ophys rig, renames with labtracks id
        for file_path in matching_files:
            ophys_rig_path = output_path + f"/{labtracks_id}_" + start_datetime.strftime('%Y-%m-%d_%H-%M-%S') + "_ophys_rig.json"
            shutil.copy(str(file_path), ophys_rig_path)
            print(f"Saved rig file to {ophys_rig_path}")