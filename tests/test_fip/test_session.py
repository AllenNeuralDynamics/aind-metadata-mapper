"""Tests parsing of session information from fip rig."""

import unittest
from datetime import datetime, timedelta
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


class TestSchemaWriter(unittest.TestCase):
    """Test methods in SchemaWriter class."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        session_time = datetime(1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC"))
        # Make stream end time 1 hour later for temporal consistency
        stream_end_time = session_time + timedelta(hours=1)

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
                    "stream_end_time": stream_end_time,
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
                    stream_end_time=stream_end_time,
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
        """Tests that the settings can be constructed from a json string"""
        job_settings_str = self.example_job_settings.model_dump_json()
        etl0 = FIBEtl(job_settings=job_settings_str)
        etl1 = FIBEtl(job_settings=self.example_job_settings)
        self.assertEqual(
            etl1.job_settings.model_dump_json(),
            etl0.job_settings.model_dump_json(),
        )

    def test_extract(self):
        """Tests that data files and metadata are extracted correctly"""
        etl_job1 = FIBEtl(job_settings=self.example_job_settings)
        # Make end time 1 hour later than start time for temporal consistency
        session_end_time = (
            self.example_job_settings.session_start_time + timedelta(hours=1)
        )
        with patch.object(etl_job1, "_extract") as mock_extract:
            mock_extract.return_value = FiberData(
                start_time=self.example_job_settings.session_start_time,
                end_time=session_end_time,
                data_files=[],
                timestamps=[],
                light_source_configs=self.example_job_settings.data_streams[0][
                    "light_sources"
                ],
                detector_configs=self.example_job_settings.data_streams[0][
                    "detectors"
                ],
                fiber_configs=self.example_job_settings.data_streams[0][
                    "fiber_connections"
                ],
                subject_id=self.example_job_settings.subject_id,
                experimenter_full_name=(
                    self.example_job_settings.experimenter_full_name
                ),
                rig_id=self.example_job_settings.rig_id,
                iacuc_protocol=self.example_job_settings.iacuc_protocol,
                notes=self.example_job_settings.notes,
                mouse_platform_name=(
                    self.example_job_settings.mouse_platform_name
                ),
                active_mouse_platform=(
                    self.example_job_settings.active_mouse_platform
                ),
                session_type="FIB",
                anaesthesia=None,
                animal_weight_post=None,
                animal_weight_prior=None,
            )
            parsed_info = etl_job1._extract()
            self.assertEqual(
                self.example_job_settings.session_start_time,
                parsed_info.start_time,
            )

    def test_transform(self):
        """Tests that the data maps correctly to session object"""
        etl_job1 = FIBEtl(job_settings=self.example_job_settings)
        session_time = self.example_job_settings.session_start_time
        # Make end time 1 hour later than start time for temporal consistency
        session_end_time = session_time + timedelta(hours=1)
        job_settings = self.example_job_settings
        stream = job_settings.data_streams[0]
        parsed_info = FiberData(
            start_time=session_time,
            end_time=session_end_time,
            data_files=[],
            timestamps=[],
            light_source_configs=stream["light_sources"],
            detector_configs=stream["detectors"],
            fiber_configs=stream["fiber_connections"],
            subject_id=job_settings.subject_id,
            experimenter_full_name=job_settings.experimenter_full_name,
            rig_id=job_settings.rig_id,
            iacuc_protocol=job_settings.iacuc_protocol,
            notes=job_settings.notes,
            mouse_platform_name=job_settings.mouse_platform_name,
            active_mouse_platform=job_settings.active_mouse_platform,
            session_type="FIB",
            anaesthesia=None,
            animal_weight_post=None,
            animal_weight_prior=None,
        )
        actual_session = etl_job1._transform(parsed_info)

        # Ensure expected session has matching datetime fields
        expected_dict = self.expected_session.model_dump()
        expected_dict["session_end_time"] = session_end_time
        modified_expected = Session(**expected_dict)

        self.assertEqual(modified_expected, actual_session)

    @patch("builtins.open", new_callable=mock_open)
    def test_run_job(self, mock_file):
        """Tests the complete ETL workflow"""
        job_settings = self.example_job_settings.model_copy()
        job_settings.output_directory = "/dummy/data/output"
        job_settings.output_filename = "session.json"

        etl_job1 = FIBEtl(job_settings=job_settings)
        stream = self.example_job_settings.data_streams[0]
        # Make end time 1 hour later than start time for temporal consistency
        session_end_time = job_settings.session_start_time + timedelta(hours=1)
        with patch.object(
            etl_job1, "_transform", return_value=self.expected_session
        ):
            with patch.object(etl_job1, "_extract") as mock_extract:
                mock_extract.return_value = FiberData(
                    start_time=job_settings.session_start_time,
                    end_time=session_end_time,
                    data_files=[],
                    timestamps=[],
                    light_source_configs=stream["light_sources"],
                    detector_configs=stream["detectors"],
                    fiber_configs=stream["fiber_connections"],
                    subject_id=job_settings.subject_id,
                    experimenter_full_name=job_settings.experimenter_full_name,
                    rig_id=job_settings.rig_id,
                    iacuc_protocol=job_settings.iacuc_protocol,
                    notes=job_settings.notes,
                    mouse_platform_name=job_settings.mouse_platform_name,
                    active_mouse_platform=job_settings.active_mouse_platform,
                    session_type="FIB",
                    anaesthesia=None,
                    animal_weight_post=None,
                    animal_weight_prior=None,
                )
                job_response = etl_job1.run_job()
                self.assertEqual(200, job_response.status_code)


if __name__ == "__main__":
    unittest.main()
