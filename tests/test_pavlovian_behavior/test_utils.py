"""Tests for Pavlovian behavior utility functions."""

import unittest
from datetime import datetime
from pathlib import Path
import tempfile
import pandas as pd
from zoneinfo import ZoneInfo

from aind_metadata_mapper.pavlovian_behavior.utils import (
    find_behavior_files,
    parse_session_start_time,
    extract_trial_data,
    calculate_session_timing,
    create_stimulus_epoch,
    extract_session_data,
)


class TestPavlovianBehaviorUtils(unittest.TestCase):
    """Test Pavlovian behavior utility functions."""

    def test_find_behavior_files(self):
        """Test finding behavior and trial files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test directory structure
            behavior_dir = Path(tmpdir) / "behavior"
            behavior_dir.mkdir()

            # Create test files
            ts_file = behavior_dir / "TS_CS1_2024-01-01T15_49_53.csv"
            trial_file = behavior_dir / "TrialN_TrialType_ITI_001.csv"
            ts_file.touch()
            trial_file.touch()

            # Test with behavior subdirectory
            behavior_files, trial_files = find_behavior_files(Path(tmpdir))
            self.assertEqual(len(behavior_files), 1)
            self.assertEqual(len(trial_files), 1)

            # Test with files in main directory
            ts_file.rename(Path(tmpdir) / ts_file.name)
            trial_file.rename(Path(tmpdir) / trial_file.name)
            behavior_dir.rmdir()

            behavior_files, trial_files = find_behavior_files(Path(tmpdir))
            self.assertEqual(len(behavior_files), 1)
            self.assertEqual(len(trial_files), 1)

            # Test with missing files
            with self.assertRaises(FileNotFoundError):
                find_behavior_files(Path(tmpdir) / "nonexistent")

    def test_parse_session_start_time(self):
        """Test parsing session start time from filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test file
            timestamp = "2024-01-01T15_49_53"
            test_file = Path(tmpdir) / f"TS_CS1_{timestamp}.csv"
            test_file.touch()

            # Test with America/Los_Angeles timezone
            result = parse_session_start_time(
                test_file, local_timezone="America/Los_Angeles"
            )
            self.assertEqual(result.year, 2024)
            self.assertEqual(result.month, 1)
            self.assertEqual(result.day, 1)
            self.assertEqual(result.hour, 23)  # 15:49 PT = 23:49 UTC
            self.assertEqual(result.minute, 49)
            self.assertEqual(result.second, 53)

            # Test with invalid filename
            invalid_file = Path(tmpdir) / "invalid_filename.csv"
            invalid_file.touch()
            with self.assertRaises(ValueError):
                parse_session_start_time(invalid_file)

    def test_extract_trial_data(self):
        """Test extraction of trial data from CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test CSV file
            trial_file = Path(tmpdir) / "trial_data.csv"
            df = pd.DataFrame(
                {
                    "TrialNumber": range(1, 11),
                    "TotalRewards": range(0, 5),
                    "ITI_s": [1.0] * 10,
                }
            )
            df.to_csv(trial_file, index=False)

            # Test with valid file
            result = extract_trial_data(trial_file)
            self.assertEqual(len(result), 10)
            self.assertTrue(
                all(
                    col in result.columns
                    for col in ["TrialNumber", "TotalRewards", "ITI_s"]
                )
            )

            # Test with missing columns
            invalid_df = pd.DataFrame({"Wrong": [1, 2, 3]})
            invalid_df.to_csv(Path(tmpdir) / "invalid.csv", index=False)
            with self.assertRaises(ValueError):
                extract_trial_data(Path(tmpdir) / "invalid.csv")

    def test_calculate_session_timing(self):
        """Test calculation of session timing from trial data."""
        start_time = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))
        trial_data = pd.DataFrame({"ITI_s": [1.0] * 10})  # 10 seconds total

        end_time, duration = calculate_session_timing(start_time, trial_data)
        self.assertEqual(duration, 10.0)
        self.assertEqual((end_time - start_time).total_seconds(), 10.0)

    def test_create_stimulus_epoch(self):
        """Test creation of stimulus epoch from trial data."""
        start_time = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))
        end_time = datetime(2024, 1, 1, 0, 0, 10, tzinfo=ZoneInfo("UTC"))
        trial_data = pd.DataFrame(
            {
                "TrialNumber": range(1, 11),
                "TotalRewards": range(0, 5),
                "ITI_s": [1.0] * 10,
            }
        )

        epoch = create_stimulus_epoch(
            start_time, end_time, trial_data, reward_units_per_trial=2.0
        )
        self.assertEqual(epoch.trials_total, 10)
        self.assertEqual(epoch.trials_rewarded, 4)
        self.assertEqual(epoch.reward_consumed_during_epoch, 8.0)

    def test_extract_session_data(self):
        """Test complete session data extraction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test directory structure and files
            behavior_dir = Path(tmpdir) / "behavior"
            behavior_dir.mkdir()

            # Create behavior file
            ts_file = behavior_dir / "TS_CS1_2024-01-01T15_49_53.csv"
            ts_file.touch()

            # Create trial file with data
            trial_file = behavior_dir / "TrialN_TrialType_ITI_001.csv"
            df = pd.DataFrame(
                {
                    "TrialNumber": range(1, 11),
                    "TotalRewards": range(0, 5),
                    "ITI_s": [1.0] * 10,
                }
            )
            df.to_csv(trial_file, index=False)

            # Test complete extraction
            start_time, epochs = extract_session_data(
                Path(tmpdir),
                reward_units_per_trial=2.0,
                local_timezone="America/Los_Angeles",
            )

            self.assertEqual(start_time.hour, 23)  # 15:49 PT = 23:49 UTC
            self.assertEqual(len(epochs), 1)
            self.assertEqual(epochs[0].trials_total, 10)
            self.assertEqual(epochs[0].trials_rewarded, 4)
            self.assertEqual(epochs[0].reward_consumed_during_epoch, 8.0)


if __name__ == "__main__":
    unittest.main()
