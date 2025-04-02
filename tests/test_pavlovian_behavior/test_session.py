"""Tests for Pavlovian behavior session metadata generation."""

import json
import unittest
from datetime import datetime
import zoneinfo
import pandas as pd

from aind_data_schema.core.session import Session, Stream, StimulusEpoch
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.pavlovian_behavior.session import ETL, JobSettings


class TestPavlovianBehaviorSession(unittest.TestCase):
    """Test Pavlovian behavior session metadata generation."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        session_time = datetime(1999, 10, 4, tzinfo=zoneinfo.ZoneInfo("UTC"))

        # Create job settings
        cls.example_job_settings = JobSettings(
            experimenter_full_name=["Test User"],
            session_start_time=session_time,
            session_end_time=session_time,
            subject_id="000000",
            rig_id="pav_rig_01",
            mouse_platform_name="mouse_tube_pavlovian",
            active_mouse_platform=False,
            data_streams=[
                {
                    "stream_start_time": session_time,
                    "stream_end_time": session_time,
                    "stream_modalities": [Modality.BEHAVIOR],
                    "light_sources": [
                        {
                            "name": "IR LED",
                            "device_type": "Light emitting diode",
                            "excitation_power": None,
                            "excitation_power_unit": "milliwatt",
                        }
                    ],
                    "software": [
                        {
                            "name": "Bonsai",
                            "version": "",
                            "url": "",
                            "parameters": {},
                        }
                    ],
                }
            ],
            stimulus_epochs=[
                StimulusEpoch(
                    stimulus_start_time=session_time,
                    stimulus_end_time=session_time,
                    stimulus_name="Pavlovian",
                    stimulus_modalities=["Auditory"],
                    trials_total=100,
                    trials_finished=100,
                    trials_rewarded=50,
                    reward_consumed_during_epoch=100.0,
                )
            ],
            notes="Test session",
            iacuc_protocol="2115",
            reward_units_per_trial=2.0,
        )

        # Create expected session
        cls.expected_session = Session(
            experimenter_full_name=["Test User"],
            session_start_time=session_time,
            session_end_time=session_time,
            session_type="Pavlovian_Conditioning",
            rig_id="pav_rig_01",
            subject_id="000000",
            iacuc_protocol="2115",
            notes="Test session",
            mouse_platform_name="mouse_tube_pavlovian",
            active_mouse_platform=False,
            data_streams=[
                Stream(
                    stream_start_time=session_time,
                    stream_end_time=session_time,
                    stream_modalities=[Modality.BEHAVIOR],
                    light_sources=[
                        {
                            "name": "IR LED",
                            "device_type": "Light emitting diode",
                            "excitation_power": None,
                            "excitation_power_unit": "milliwatt",
                        }
                    ],
                    software=[
                        {
                            "name": "Bonsai",
                            "version": "",
                            "url": "",
                            "parameters": {},
                        }
                    ],
                )
            ],
            stimulus_epochs=[
                StimulusEpoch(
                    stimulus_start_time=session_time,
                    stimulus_end_time=session_time,
                    stimulus_name="Pavlovian",
                    stimulus_modalities=["Auditory"],
                    trials_total=100,
                    trials_finished=100,
                    trials_rewarded=50,
                    reward_consumed_during_epoch=100.0,
                )
            ],
            reward_consumed_total=100.0,
            reward_consumed_unit="microliter",
        )

    def test_constructor_from_string(self) -> None:
        """Test construction from JSON string."""
        job_settings_str = self.example_job_settings.model_dump_json()
        etl0 = ETL(job_settings=job_settings_str)
        etl1 = ETL(job_settings=self.example_job_settings)

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

    def test_run_job(self):
        """Test complete ETL workflow."""
        etl = ETL(job_settings=self.example_job_settings)
        job = etl.run_job()
        self.assertEqual(
            self.expected_session, Session(**json.loads(job.data))
        )


if __name__ == "__main__":
    unittest.main()
