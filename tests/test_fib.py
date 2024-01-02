"""Tests parsing of session information from fib rig."""

import json
import os
import unittest
from datetime import datetime
from pathlib import Path

from aind_data_schema.core.session import Session

from aind_metadata_mapper.fib.session import FIBEtl

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "resources" / "fib"
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
                "john doe",
            ],
            "notes": "brabrabrabra....",  #
            "experimental_mode": "c",
            "save_dir": "",
            "iacuc": "2115",
            "rig_id": "ophys_rig",
            "COMPort": "COM3",
            "mouse_platform_name": "Disc",
            "active_mouse_platform": False,
            "light_source": [
                {
                    "name": "470nm LED",
                    "excitation_power": 0.020,
                    "excitation_power_unit": "milliwatt",
                },
                {
                    "name": "415nm LED",
                    "excitation_power": 0.020,
                    "excitation_power_unit": "milliwatt",
                },
                {
                    "name": "565nm LED",
                    "excitation_power": 0.020,  # Set 0 for unused StimLED
                    "excitation_power_unit": "milliwatt",
                },
            ],  # default light source
            "detectors": [
                {
                    "name": "Hamamatsu Camera",
                    "exposure_time": 10,
                    "trigger_type": "Internal",
                }
            ],
            "fiber_connections": [
                {
                    "patch_cord_name": "Patch Cord A",
                    "patch_cord_output_power": 40,
                    "output_power_unit": "microwatt",
                    "fiber_name": "Fiber A",
                }
            ],
            "session_type": "Foraging_Photometry",
        }

        with open(EXAMPLE_MD_PATH, "r") as f:
            raw_md_contents = f.read()
        with open(EXPECTED_SESSION, "r") as f:
            expected_session_contents = Session(**json.load(f))

        cls.expected_session = expected_session_contents
        cls.example_metadata = raw_md_contents

    def test_extract(self):
        """Tests that the teensy response and experiment
        data is extracted correctly"""

        etl_job1 = FIBEtl(
            output_directory=RESOURCES_DIR,
            teensy_str=self.example_metadata,
            experiment_data=self.example_experiment_data,
            start_datetime=datetime(1999, 10, 4),
        )
        parsed_info = etl_job1._extract()
        self.assertEqual(self.example_metadata, parsed_info.teensy_str)
        self.assertEqual(
            self.example_experiment_data, parsed_info.experiment_data
        )
        self.assertEqual(datetime(1999, 10, 4), parsed_info.start_datetime)

    def test_transform(self):
        """Tests that the teensy response maps correctly to ophys session."""

        etl_job1 = FIBEtl(
            output_directory=RESOURCES_DIR,
            teensy_str=self.example_metadata,
            experiment_data=self.example_experiment_data,
            start_datetime=datetime(1999, 10, 4),
        )
        parsed_info = etl_job1._extract()
        actual_session = etl_job1._transform(parsed_info)
        self.assertEqual(self.expected_session, actual_session)


if __name__ == "__main__":
    unittest.main()
