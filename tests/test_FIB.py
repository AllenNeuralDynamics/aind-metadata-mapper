"""Tests parsing of session information from FIB rig."""

import json
import os
import unittest
from datetime import datetime
from pathlib import Path

from aind_metadata_mapper.FIB.session import SchemaWriter

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "resources" / "FIB"
)
EXAMPLE_MD_PATH = RESOURCES_DIR / "example_from_teensy.txt"
EXPECTED_SESSION = RESOURCES_DIR / "000000_ophys_session.json"


class TestSchemaWriter(unittest.TestCase):
    """Test methods in SchemaWriter class."""

    @classmethod
    def setUpClass(cls):
        """Load record object and user settings before running tests."""

        cls.example_experiment_data = {
            "labtracks_id": "000000",
            "experimenter_name": [
                "Lucas Kinsey",
                "Kenta Hagihara",
            ],  # Travis Ramirez
            "notes": "brabrabrabra....",  #
            "experimental_mode": "c",
            "save_dir": str(RESOURCES_DIR),
            "iacuc": "2115",
            "rig_id": "ophys_rig",
            "COMPort": "COM3",
            "light_source": [
                "470nm LED",
                "415nm LED",
                "565nm LED",
            ],  # default light source
            "light_excitation_power": [
                0.020,
                0.020,
                0.020,
            ],  # mW    Set 0 for unused StimLED
            "session_type": "Foraging_Photometry",
        }

        with open(EXAMPLE_MD_PATH, "r") as f:
            raw_md_contents = f.read()
        with open(EXPECTED_SESSION, "r") as f:
            expected_session_contents = json.load(f)

        cls.expected_session = expected_session_contents
        cls.example_metadata = raw_md_contents

    def test_map_response_to_ophys_session(self):
        """Tests that the teensy response maps correctly to ophys session."""

        start_date = datetime.now()

        original_input = ["o", "p", "q", "l"]
        expected_output = ["OptoStim10Hz", "OptoStim20Hz", "OptoStim5Hz", ""]

        for command, stimulus_name in zip(original_input, expected_output):
            command_index = self.example_metadata.index(
                "Received command "
            ) + len("Received command ")
            new_metadata = (
                self.example_metadata[:command_index]
                + command
                + self.example_metadata[command_index + 1:]
            )
            SchemaWriter.map_response_to_ophys_session(
                string_to_parse=new_metadata,
                experiment_data=self.example_experiment_data,
                start_datetime=start_date,
            )

            ophys_session_path = str(
                self.example_experiment_data["save_dir"]
                + f"/{self.example_experiment_data['labtracks_id']}_"
                + start_date.strftime("%Y-%m-%d_%H-%M-%S")
                + "_ophys_session.json"
            )

            with open(ophys_session_path, "r") as f:
                actual_session_contents = json.load(f)
            print(
                actual_session_contents["stimulus_epochs"][0]["stimulus"][
                    "stimulus_name"
                ]
            )
            self.assertEqual(
                actual_session_contents["stimulus_epochs"][0]["stimulus"][
                    "stimulus_name"
                ],
                stimulus_name,
            )

    def test_map_to_ophys_rig(self):
        """Tests that the teensy response maps correctly to ophys rig."""

        start_date = datetime.now()

        SchemaWriter.map_to_ophys_rig(
            experiment_data=self.example_experiment_data,
            start_datetime=start_date,
            reference_path=RESOURCES_DIR,
        )

        # ophys_rig_path = (
        #     self.example_experiment_data["save_dir"]
        #     + f"/{self.example_experiment_data['labtracks_id']}_"
        #     + start_date.strftime("%Y-%m-%d_%H-%M-%S")
        #     + "_ophys_rig.json"
        # )

        # with open(ophys_rig_path, "r") as f:
        #     actual_rig_contents = json.load(f)
        #
        # self.assertEqual(actual_rig_contents, self.expected_rig)


if __name__ == "__main__":
    unittest.main()
