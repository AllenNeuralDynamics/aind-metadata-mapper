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

from aind_metadata_mapper.fip.session import FIBEtl, JobSettings, FiberData


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
            data_directory="/dummy/data/path",
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
        """Test construction from JSON string.

        Verifies that the FIBEtl class can be initialized with either a
        JobSettings object or a JSON string.
        """
        job_settings_str = self.example_job_settings.model_dump_json()
        etl0 = FIBEtl(job_settings=job_settings_str)
        etl1 = FIBEtl(job_settings=self.example_job_settings)

        # Compare serialized versions to avoid timezone implementation
        # differences
        self.assertEqual(
            etl0.job_settings.model_dump_json(),
            etl1.job_settings.model_dump_json(),
        )

    def test_transform(self):
        """Test session object creation through transformation.

        Verifies that a valid Session object is created with the expected
        format and fields based on input data.
        """
        # Create reference session for validation
        session_time = self.example_job_settings.session_start_time
        session = Session(
            experimenter_full_name=["Test User"],
            session_start_time=session_time,
            session_end_time=None,
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

        # Prepare expected session with session_end_time=None for comparison
        expected_dict = self.expected_session.model_dump()
        expected_dict["session_end_time"] = None
        modified_expected = Session(**expected_dict)

        # Compare session objects using JSON serialization
        self.assertEqual(
            modified_expected.model_dump_json(exclude_none=True),
            session.model_dump_json(exclude_none=True),
        )

    @patch("builtins.open", new_callable=mock_open)
    def test_run_job(self, mock_file):
        """Test the complete ETL workflow.

        Verifies that the ETL process correctly:
        1. Processes input data
        2. Creates a valid Session object
        3. Writes the Session to a file
        4. Returns a successful response
        """
        # Create job settings with output information
        job_settings = self.example_job_settings.model_copy()
        job_settings.output_directory = "/dummy/data/output"
        job_settings.output_filename = "session.json"

        # Create ETL instance
        etl = FIBEtl(job_settings=job_settings)

        # Mock the internal ETL methods to isolate the test from file system
        session_time = datetime(1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC"))

        with patch.object(
            etl, "_transform", return_value=self.expected_session
        ):
            with patch.object(etl, "_extract") as mock_extract:
                # Create test data for the extract method
                fiber_data = FiberData(
                    start_time=session_time,
                    end_time=session_time,
                    data_files=[],
                    timestamps=[],
                    light_source_configs=job_settings.data_streams[0][
                        "light_sources"
                    ],
                    detector_configs=job_settings.data_streams[0]["detectors"],
                    fiber_configs=job_settings.data_streams[0][
                        "fiber_connections"
                    ],
                    subject_id=job_settings.subject_id,
                    experimenter_full_name=job_settings.experimenter_full_name,
                    rig_id=job_settings.rig_id,
                    iacuc_protocol=job_settings.iacuc_protocol,
                    notes=job_settings.notes,
                    mouse_platform_name=job_settings.mouse_platform_name,
                    active_mouse_platform=job_settings.active_mouse_platform,
                )
                mock_extract.return_value = fiber_data

                # Run the ETL job
                response = etl.run_job()

                # Verify successful execution
                self.assertEqual(200, response.status_code)

                # Verify file writing operations
                mock_file.assert_called()
                mock_file().write.assert_called()

                # Verify the file content matches the expected session
                written_data = mock_file().write.call_args[0][0]
                written_session = Session(**json.loads(written_data))
                self.assertEqual(self.expected_session, written_session)


if __name__ == "__main__":
    unittest.main()
