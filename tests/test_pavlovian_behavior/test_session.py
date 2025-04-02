"""Tests for Pavlovian behavior session metadata generation."""

import unittest
import tempfile
import pandas as pd
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from aind_data_schema.core.session import Session
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import VolumeUnit

from aind_metadata_mapper.pavlovian_behavior.session import ETL, JobSettings


class TestPavlovianBehaviorSession(unittest.TestCase):
    """Test Pavlovian behavior session metadata generation."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        cls.temp_dir = tempfile.mkdtemp()
        behavior_dir = Path(cls.temp_dir) / "behavior"
        behavior_dir.mkdir()

        # Create test files with time that will convert to 8am UTC from PT
        session_time = datetime(1999, 10, 4, 8, 0, 0, tzinfo=ZoneInfo("UTC"))
        ts_file = (
            behavior_dir / "TS_CS1_1999-10-04T01_00_00.csv"
        )  # 1am PT = 8am UTC
        trial_file = behavior_dir / "TrialN_TrialType_ITI_001.csv"

        # Create and write trial data
        df = pd.DataFrame(
            {
                "TrialNumber": range(1, 101),  # 100 trials
                "TotalRewards": [50] * 100,  # 50 rewards
                "ITI_s": [1.0] * 100,  # 1 second ITI
            }
        )
        df.to_csv(trial_file, index=False)
        ts_file.touch()

        end_time = session_time + pd.Timedelta(
            seconds=100
        )  # 100 trials * 1s ITI

        # Create stimulus epoch as a dictionary
        stimulus_epoch_dict = {
            "stimulus_start_time": session_time,
            "stimulus_end_time": end_time,
            "stimulus_name": "Pavlovian",
            "stimulus_modalities": ["Auditory"],
            "trials_total": 100,
            "trials_finished": 100,
            "trials_rewarded": 50,
            "reward_consumed_during_epoch": 100.0,
        }

        # Create job settings with all required fields
        cls.example_job_settings = JobSettings(
            experimenter_full_name=["Test User"],
            subject_id="000000",
            rig_id="pav_rig_01",
            iacuc_protocol="2115",
            mouse_platform_name="mouse_tube_pavlovian",
            active_mouse_platform=False,
            data_directory=cls.temp_dir,
            local_timezone="UTC",
            notes="Test session",
            data_streams=[
                {
                    "stream_start_time": session_time,
                    "stream_end_time": end_time,
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
        )

        # Create expected session object
        cls.expected_session = Session(
            experimenter_full_name=["Test User"],
            session_start_time=session_time,
            session_end_time=end_time,
            subject_id="000000",
            rig_id="pav_rig_01",
            iacuc_protocol="2115",
            mouse_platform_name="mouse_tube_pavlovian",
            active_mouse_platform=False,
            session_type="Pavlovian_Conditioning",
            data_streams=cls.example_job_settings.data_streams,
            stimulus_epochs=[stimulus_epoch_dict],
            reward_consumed_total=100.0,
            reward_consumed_unit=VolumeUnit.UL,
            notes="Test session",
        )

    @classmethod
    def tearDownClass(cls):
        """Clean up test files."""
        import shutil

        shutil.rmtree(cls.temp_dir)

    def test_constructor_from_string(self):
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
        self.assertEqual(job.status_code, 200)


if __name__ == "__main__":
    unittest.main()
