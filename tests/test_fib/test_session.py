"""Tests for fiber photometry session metadata generation."""

import json
import unittest
from datetime import datetime
import zoneinfo
from unittest.mock import patch, mock_open

from aind_data_schema.core.session import (
    Session,
    Stream,
    LightEmittingDiodeConfig,
    DetectorConfig,
    FiberConnectionConfig,
)
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.fib.session import ETL, JobSettings


class TestFiberPhotometrySession(unittest.TestCase):
    """Test fiber photometry session metadata generation."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        session_time = datetime(1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC"))

        # Create job settings
        cls.example_job_settings = JobSettings(
            experimenter_full_name=["Test User"],
            session_start_time=session_time,
            subject_id="000000",
            rig_id="fiber_rig_01",
            mouse_platform_name="Disc",
            active_mouse_platform=False,
            data_streams=[
                {
                    "stream_start_time": session_time,
                    "stream_end_time": session_time,
                    "light_sources": [
                        {
                            "name": "470nm LED",
                            "excitation_power": 0.020,
                            "excitation_power_unit": "milliwatt",
                        }
                    ],
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
                }
            ],
            notes="Test session",
            iacuc_protocol="2115",
        )

        # Create expected session
        cls.expected_session = Session(
            experimenter_full_name=["Test User"],
            session_start_time=session_time,
            session_type="FIB",
            rig_id="fiber_rig_01",
            subject_id="000000",
            iacuc_protocol="2115",
            notes="Test session",
            mouse_platform_name="Disc",
            active_mouse_platform=False,
            data_streams=[
                Stream(
                    stream_start_time=session_time,
                    stream_end_time=session_time,
                    light_sources=[
                        LightEmittingDiodeConfig(
                            name="470nm LED",
                            excitation_power=0.020,
                            excitation_power_unit="milliwatt",
                        )
                    ],
                    stream_modalities=[Modality.FIB],
                    detectors=[
                        DetectorConfig(
                            name="Hamamatsu Camera",
                            exposure_time=10,
                            trigger_type="Internal",
                        )
                    ],
                    fiber_connections=[
                        FiberConnectionConfig(
                            patch_cord_name="Patch Cord A",
                            patch_cord_output_power=40,
                            output_power_unit="microwatt",
                            fiber_name="Fiber A",
                        )
                    ],
                )
            ],
        )

    def test_constructor_from_string(self) -> None:
        """Test construction from JSON string."""
        job_settings_str = self.example_job_settings.model_dump_json()
        etl0 = ETL(job_settings=job_settings_str)
        etl1 = ETL(job_settings=self.example_job_settings)

        # Compare serialized versions to avoid timezone implementation
        # differences
        self.assertEqual(
            etl0.job_settings.model_dump_json(),
            etl1.job_settings.model_dump_json(),
        )

    def test_transform(self):
        """Test transformation to valid session metadata."""
        etl = ETL(job_settings=self.example_job_settings)
        parsed_info = etl._extract()
        actual_session = etl._transform(parsed_info)
        self.assertEqual(self.expected_session, actual_session)

    @patch("pathlib.Path.exists")
    @patch("builtins.open", new_callable=mock_open)
    def test_run_job(self, mock_file, mock_exists):
        """Test complete ETL workflow."""
        # Create job settings with output directory and filename
        job_settings = self.example_job_settings.model_copy()
        job_settings.output_directory = "/dummy/path"
        job_settings.output_filename = "session.json"

        # Make Path.exists() return True
        mock_exists.return_value = True

        # Create ETL and run job
        etl = ETL(job_settings=job_settings)
        etl.run_job()  # Run the job without storing the return value

        # Get the data that was written to the file
        written_data = None
        for call in mock_file().write.call_args_list:
            written_data = call[0][
                0
            ]  # Get the first argument of the write call
            break

        # Verify the written data matches our expected session
        self.assertIsNotNone(written_data)
        written_session = Session(**json.loads(written_data))
        self.assertEqual(self.expected_session, written_session)

        # Verify that write operations were called
        mock_file.assert_called()
        mock_file().write.assert_called()


if __name__ == "__main__":
    unittest.main()
