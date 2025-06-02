"""Tests for Pavlovian behavior session metadata generation."""

import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, Mock
from zoneinfo import ZoneInfo
import pandas as pd

from aind_data_schema.core.session import Session, StimulusEpoch
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.units import VolumeUnit

from aind_metadata_mapper.pavlovian_behavior.session import ETL, JobSettings


class TestPavlovianBehaviorSession(unittest.TestCase):
    """Test Pavlovian behavior session metadata generation."""

    def setUp(self):
        """Set up test fixtures."""
        self.data_dir = Path("/mock/data/dir")
        self.session_time = datetime(
            1999, 10, 4, 8, 0, 0, tzinfo=ZoneInfo("UTC")
        )
        # Make end time 2 hours later than start time for temporal consistency
        self.end_time = self.session_time + timedelta(hours=2)

        # Create stimulus epoch as a proper StimulusEpoch object
        self.stimulus_epoch = StimulusEpoch(
            stimulus_start_time=self.session_time,
            stimulus_end_time=self.end_time,
            stimulus_name="Pavlovian",
            stimulus_modalities=["Auditory"],
            trials_total=100,
            trials_finished=100,
            trials_rewarded=50,
            reward_consumed_during_epoch=100.0,
        )

        # Create job settings with all required fields
        self.example_job_settings = JobSettings(
            experimenter_full_name=["Test User"],
            subject_id="000000",
            rig_id="pav_rig_01",
            iacuc_protocol="2115",
            mouse_platform_name="mouse_tube_pavlovian",
            active_mouse_platform=False,
            data_directory=self.data_dir,
            output_directory=self.data_dir,
            output_filename="session_pavlovian.json",
            local_timezone="UTC",
            notes="Test session",
            data_streams=[
                {
                    "stream_start_time": self.session_time,
                    "stream_end_time": self.end_time,
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
        self.expected_session = Session(
            experimenter_full_name=["Test User"],
            session_start_time=self.session_time,
            session_end_time=self.end_time,
            subject_id="000000",
            rig_id="pav_rig_01",
            iacuc_protocol="2115",
            mouse_platform_name="mouse_tube_pavlovian",
            active_mouse_platform=False,
            session_type="Pavlovian_Conditioning",
            data_streams=self.example_job_settings.data_streams,
            stimulus_epochs=[self.stimulus_epoch.model_dump()],
            reward_consumed_total=100.0,
            reward_consumed_unit=VolumeUnit.UL,
            notes="Test session",
        )

        # Mock file paths
        self.behavior_file = self.data_dir / "TS_CS1_1999-10-04T08_00_00.csv"
        self.trial_file = self.data_dir / "TrialN_TrialType_ITI_001.csv"

        # Create mock trial data
        self.trial_data = pd.DataFrame(
            {
                "TrialNumber": range(1, 101),  # 100 trials
                "TotalRewards": [50] * 100,  # 50 rewards
                "ITI_s": [1.0] * 100,  # 1 second ITI
            }
        )

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
        # Create simple mocks with explicit return values
        mock_exists = Mock(
            return_value=False
        )  # Use data_dir instead of behavior subdir
        mock_glob = Mock(
            side_effect=lambda pattern: {
                "TS_CS1_*.csv": [self.behavior_file],
                "TrialN_TrialType_ITI_*.csv": [self.trial_file],
                "TS_CS*.csv": [self.behavior_file],
                "TS_Reward*.csv": [],
            }[pattern]
        )

        mock_read_csv = Mock(return_value=self.trial_data)
        # Mock extract_session_data to return proper StimulusEpoch object
        mock_extract = Mock(
            return_value=(self.session_time, [self.stimulus_epoch])
        )
        # Mock find_session_end_time to return our expected end time
        mock_find_end_time = Mock(return_value=self.end_time)

        # Patch with explicit mocks
        with (
            patch.multiple(Path, exists=mock_exists, glob=mock_glob),
            patch("pandas.read_csv", mock_read_csv),
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".extract_session_data",
                mock_extract,
            ),
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".find_session_end_time",
                mock_find_end_time,
            ),
        ):
            etl = ETL(job_settings=self.example_job_settings)
            parsed_info = etl._extract()
            actual_session = etl._transform(parsed_info)
            self.assertEqual(self.expected_session, actual_session)

    def test_run_job(self):
        """Test complete ETL workflow."""
        # Create simple mocks with explicit return values
        mock_exists = Mock(return_value=True)
        mock_glob = Mock(
            side_effect=lambda pattern: {
                "TS_CS1_*.csv": [self.behavior_file],
                "TrialN_TrialType_ITI_*.csv": [self.trial_file],
                "TS_CS*.csv": [self.behavior_file],
                "TS_Reward*.csv": [],
            }[pattern]
        )

        # Create explicit mock for file operations that
        # returns an empty list of lines
        mock_file = Mock()
        mock_file.readlines = Mock(
            return_value=[]
        )  # Empty list of lines for tzlocal to iterate
        mock_open = Mock()
        mock_open.return_value.__enter__ = Mock(return_value=mock_file)
        mock_open.return_value.__exit__ = Mock()

        mock_read_csv = Mock(return_value=self.trial_data)
        # Mock extract_session_data to return proper StimulusEpoch object
        mock_extract = Mock(
            return_value=(self.session_time, [self.stimulus_epoch])
        )
        # Mock find_session_end_time to return our expected end time
        mock_find_end_time = Mock(return_value=self.end_time)

        # Patch with explicit mocks
        with (
            patch.multiple(Path, exists=mock_exists, glob=mock_glob),
            patch("builtins.open", mock_open),
            patch("pandas.read_csv", mock_read_csv),
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".extract_session_data",
                mock_extract,
            ),
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".find_session_end_time",
                mock_find_end_time,
            ),
            # Add explicit mock for timezone to avoid file reading
            patch("tzlocal.get_localzone", return_value=ZoneInfo("UTC")),
        ):
            etl = ETL(job_settings=self.example_job_settings)
            job = etl.run_job()

            # Verify the job succeeded and file was written
            self.assertEqual(job.status_code, 200)
            mock_open.assert_any_call(
                self.data_dir / "session_pavlovian.json", "w"
            )


if __name__ == "__main__":
    unittest.main()
