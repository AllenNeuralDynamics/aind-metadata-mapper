"""Tests for Pavlovian behavior utility functions."""

import unittest
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd

from aind_metadata_mapper.pavlovian_behavior.utils import (
    find_behavior_files,
    parse_session_start_time,
    extract_trial_data,
    calculate_session_timing,
    create_stimulus_epoch,
    extract_session_data,
    validate_behavior_file_format,
    validate_trial_file_format,
)


class TestPavlovianBehaviorUtils(unittest.TestCase):
    """Test Pavlovian behavior utility functions."""

    def setUp(self):
        """Set up test data."""
        self.test_time = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

        # Create test DataFrame with matching lengths
        self.test_df = pd.DataFrame(
            {
                "TrialNumber": range(1, 11),  # 10 items
                "TotalRewards": range(0, 10),  # 10 items
                "ITI_s": [1.0] * 10,  # 10 items
            }
        )

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
        # Test with UTC time to avoid timezone dependencies
        filename_actual = Path("TS_CS1_2024-01-01T12_00_00.csv")
        expected = datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("UTC"))
        actual = parse_session_start_time(
            filename_actual, local_timezone="UTC"
        )
        self.assertEqual(expected, actual)

    def test_extract_trial_data(self):
        """Test extraction of trial data from CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test CSV file
            trial_file = Path(tmpdir) / "trial_data.csv"
            df = pd.DataFrame(
                {
                    "TrialNumber": range(1, 11),  # 10 items
                    "TotalRewards": range(0, 10),  # 10 items
                    "ITI_s": [1.0] * 10,  # 10 items
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
                "TotalRewards": range(0, 10),
                "ITI_s": [1.0] * 10,
            }
        )

        epoch = create_stimulus_epoch(
            start_time, end_time, trial_data, reward_units_per_trial=2.0
        )
        self.assertEqual(epoch.trials_total, 10)
        self.assertEqual(
            epoch.trials_rewarded, 9
        )  # range(0,10) has 9 non-zero values
        self.assertEqual(epoch.reward_consumed_during_epoch, 18.0)  # 9 * 2.0

    def test_extract_session_data(self):
        """Test complete session data extraction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test directory structure and files
            behavior_dir = Path(tmpdir) / "behavior"
            behavior_dir.mkdir()

            # Use UTC time to avoid timezone dependencies
            ts_file = behavior_dir / "TS_CS1_2024-01-01T12_00_00.csv"
            ts_file.touch()

            # Create trial file with data
            trial_file = behavior_dir / "TrialN_TrialType_ITI_001.csv"
            df = pd.DataFrame(
                {
                    "TrialNumber": range(1, 11),
                    "TotalRewards": range(0, 10),
                    "ITI_s": [1.0] * 10,
                }
            )
            df.to_csv(trial_file, index=False)

            # Test complete extraction using UTC
            start_time, epochs = extract_session_data(
                Path(tmpdir),
                reward_units_per_trial=2.0,
                local_timezone="UTC",  # Always use UTC in tests
            )

            # Test exact UTC times
            self.assertEqual(start_time.hour, 12)
            self.assertEqual(start_time.minute, 0)
            self.assertEqual(start_time.second, 0)
            self.assertEqual(start_time.tzinfo, ZoneInfo("UTC"))
            self.assertEqual(len(epochs), 1)
            self.assertEqual(epochs[0].trials_total, 10)
            self.assertEqual(epochs[0].trials_rewarded, 9)
            self.assertEqual(epochs[0].reward_consumed_during_epoch, 18.0)

    def test_validate_behavior_file_format(self):
        """Test behavior file name validation."""
        # Test valid format
        valid_file = Path("TS_CS1_2024-01-01T15_49_53.csv")
        validate_behavior_file_format(valid_file)  # Should not raise

        # Test wrong prefix
        with self.assertRaises(ValueError) as cm:
            validate_behavior_file_format(Path("TS_2024-01-01T15_49_53.csv"))
        self.assertIn("must start with 'TS_CS1_'", str(cm.exception))

        # Test wrong number of parts
        with self.assertRaises(ValueError) as cm:
            validate_behavior_file_format(Path("TS.csv"))
        self.assertIn("should have exactly three parts", str(cm.exception))

        # Test wrong extension
        with self.assertRaises(ValueError) as cm:
            validate_behavior_file_format(
                Path("TS_CS1_2024-01-01T15_49_53.txt")
            )
        self.assertIn("must have .csv extension", str(cm.exception))

        # Test invalid datetime format
        with self.assertRaises(ValueError) as cm:
            validate_behavior_file_format(Path("TS_CS1_20240101T154953.csv"))
        self.assertIn(
            "must be in format YYYY-MM-DDThh_mm_ss", str(cm.exception)
        )

    def test_validate_trial_file_format(self):
        """Test trial file name validation."""
        # Test valid format
        valid_file = Path("TrialN_TrialType_ITI_001.csv")
        validate_trial_file_format(valid_file)  # Should not raise

        # Test wrong number of parts
        with self.assertRaises(ValueError) as cm:
            validate_trial_file_format(Path("TrialN_TrialType.csv"))
        self.assertIn(
            "should have at least\nfour parts separated by underscores",
            str(cm.exception),
        )

        # Test wrong prefix
        with self.assertRaises(ValueError) as cm:
            validate_trial_file_format(Path("Trial_Type_ITI_001.csv"))
        self.assertIn(
            "must start with 'TrialN_TrialType_ITI_'", str(cm.exception)
        )

        # Test wrong extension
        with self.assertRaises(ValueError) as cm:
            validate_trial_file_format(Path("TrialN_TrialType_ITI_001.txt"))
        self.assertIn("must have .csv extension", str(cm.exception))

    def test_find_behavior_files_with_invalid_formats(self):
        """Test finding behavior files with invalid formats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            behavior_dir = Path(tmpdir) / "behavior"
            behavior_dir.mkdir()

            # Create files with invalid formats
            invalid_ts = (
                behavior_dir / "TS_CS1_20240101T154953.csv"
            )  # Wrong format but correct prefix
            valid_trial = behavior_dir / "TrialN_TrialType_ITI_001.csv"
            invalid_ts.touch()
            valid_trial.touch()

            # Should raise ValueError due to invalid behavior file format
            with self.assertRaises(ValueError) as cm:
                find_behavior_files(Path(tmpdir))
            self.assertIn(
                "must be in format YYYY-MM-DDThh_mm_ss", str(cm.exception)
            )


if __name__ == "__main__":
    unittest.main()
