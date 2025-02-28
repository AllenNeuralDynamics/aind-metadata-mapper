"""Tests for fiber photometry session metadata generation."""

import json
import unittest
from datetime import datetime
from pathlib import Path
import zoneinfo

from aind_data_schema.core.session import Session

from aind_metadata_mapper.fip.session import FIBEtl, JobSettings

FIXTURES_DIR = Path(__file__).parent.parent / "resources" / "fip" / "fixtures"
EXAMPLE_SETTINGS = FIXTURES_DIR / "example_fip_settings.json"
EXPECTED_SESSION = FIXTURES_DIR / "example_fip_session.json"


class TestFiberPhotometrySession(unittest.TestCase):
    """Test fiber photometry session metadata generation."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.example_job_settings = JobSettings(
            experimenter_full_name=["Don Key"],
            session_start_time=datetime(
                1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC")
            ),
            subject_id="000000",
            rig_id="fiber_rig_01",
            mouse_platform_name="Disc",
            active_mouse_platform=False,
            light_source_list=[
                {
                    "name": "470nm LED",
                    "excitation_power": 0.020,
                    "excitation_power_unit": "milliwatt",
                }
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
            notes="Test session",
            iacuc_protocol="2115",
        )

        with open(EXPECTED_SESSION, "r") as f:
            expected_session_contents = json.load(f)
        cls.expected_session = Session.model_validate(
            expected_session_contents
        )

    def test_constructor_from_string(self) -> None:
        """Test construction from JSON string."""
        job_settings_str = self.example_job_settings.model_dump_json()
        etl0 = FIBEtl(job_settings=job_settings_str)
        etl1 = FIBEtl(job_settings=self.example_job_settings)
        self.assertEqual(etl1.job_settings, etl0.job_settings)

    def test_transform(self):
        """Test transformation to valid session metadata."""
        etl = FIBEtl(job_settings=self.example_job_settings)
        parsed_info = etl._extract()
        actual_session = etl._transform(parsed_info)
        self.assertEqual(self.expected_session, actual_session)

    def test_run_job(self):
        """Test complete ETL workflow."""
        etl = FIBEtl(job_settings=self.example_job_settings)
        job = etl.run_job()
        self.assertEqual(
            self.expected_session, Session(**json.loads(job.data))
        )


if __name__ == "__main__":
    unittest.main()
