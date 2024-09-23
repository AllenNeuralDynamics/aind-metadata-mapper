"""Tests parsing of session information from fip rig."""

import json
import os
import unittest
import zoneinfo
from datetime import datetime
from pathlib import Path

from aind_data_schema.core.session import Session

from aind_metadata_mapper.fip.session import FIBEtl, JobSettings

RESOURCES_DIR = (
    Path(os.path.dirname(os.path.realpath(__file__)))
    / ".."
    / "resources"
    / "fip"
)

EXAMPLE_MD_PATH = RESOURCES_DIR / "example_from_teensy.txt"
EXPECTED_SESSION = RESOURCES_DIR / "000000_ophys_session.json"


class TestSchemaWriter(unittest.TestCase):
    """Test methods in SchemaWriter class."""

    @classmethod
    def setUpClass(cls):
        """Load record object and user settings before running tests."""

        with open(EXAMPLE_MD_PATH, "r") as f:
            raw_md_contents = f.read()
        with open(EXPECTED_SESSION, "r") as f:
            expected_session_contents = json.load(f)

        cls.example_job_settings = JobSettings(
            string_to_parse=raw_md_contents,
            experimenter_full_name=["Don Key"],
            session_start_time=datetime(
                1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC")
            ),
            notes="brabrabrabra....",
            labtracks_id="000000",
            iacuc_protocol="2115",
            light_source_list=[
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
            ],
            detector_list=[
                {
                    "name": "Hamamatsu Camera",
                    "exposure_time": 10,
                    "trigger_type": "Internal",
                }
            ],
            fiber_connections_list=[
                {
                    "patch_cord_name": "Patch Cord A",
                    "patch_cord_output_power": 40,
                    "output_power_unit": "microwatt",
                    "fiber_name": "Fiber A",
                }
            ],
            rig_id="ophys_rig",
            session_type="Foraging_Photometry",
            mouse_platform_name="Disc",
            active_mouse_platform=False,
        )

        cls.expected_session = Session.model_validate_json(
            json.dumps(expected_session_contents)
        )

    def test_constructor_from_string(self) -> None:
        """Tests that the settings can be constructed from a json string"""
        job_settings_str = self.example_job_settings.model_dump_json()
        etl0 = FIBEtl(
            job_settings=job_settings_str,
        )
        etl1 = FIBEtl(
            job_settings=self.example_job_settings,
        )
        self.assertEqual(etl1.job_settings, etl0.job_settings)

    def test_extract(self):
        """Tests that the teensy response and experiment
        data is extracted correctly"""

        etl_job1 = FIBEtl(job_settings=self.example_job_settings)
        parsed_info = etl_job1._extract()
        self.assertEqual(
            self.example_job_settings.string_to_parse, parsed_info.teensy_str
        )
        self.assertEqual(
            datetime(1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC")),
            self.example_job_settings.session_start_time,
        )

    def test_transform(self):
        """Tests that the teensy response maps correctly to ophys session."""

        etl_job1 = FIBEtl(job_settings=self.example_job_settings)
        parsed_info = etl_job1._extract()
        actual_session = etl_job1._transform(parsed_info)
        self.assertEqual(self.expected_session, actual_session)

    def test_run_job(self):
        """Tests that the teensy response maps correctly to ophys session."""

        etl_job1 = FIBEtl(job_settings=self.example_job_settings)
        job = etl_job1.run_job()
        self.assertEqual(
            self.expected_session, Session(**json.loads(job.data))
        )


if __name__ == "__main__":
    unittest.main()
