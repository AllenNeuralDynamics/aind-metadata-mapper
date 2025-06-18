"""Tests for Pavlovian behavior utility functions."""

import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd
from unittest.mock import patch, Mock

from aind_metadata_mapper.pavlovian_behavior.utils import (
    find_behavior_files,
    parse_session_start_time,
    extract_trial_data,
    calculate_session_timing_from_trials,
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
        # Mock Path objects for files
        mock_ts_file = Mock()
        mock_ts_file.name = "TS_CS1_2024-01-01T15_49_53.csv"
        mock_trial_file = Mock()
        mock_trial_file.name = "TrialN_TrialType_ITI_001.csv"

        # Test with behavior subdirectory
        with (
            patch.object(Path, "exists") as mock_exists,
            patch.object(Path, "glob") as mock_glob,
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".validate_behavior_file_format"
            ),
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".validate_trial_file_format"
            ),
        ):
            # Mock behavior directory exists
            mock_exists.side_effect = lambda: True
            mock_glob.side_effect = lambda pattern: (
                [mock_ts_file] if "TS_CS1_" in pattern else [mock_trial_file]
            )

            behavior_files, trial_files = find_behavior_files(
                Path("/mock/path")
            )
            self.assertEqual(len(behavior_files), 1)
            self.assertEqual(len(trial_files), 1)

        # Test with missing files
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "glob", return_value=[]),  # No files found
        ):
            with self.assertRaises(FileNotFoundError):
                find_behavior_files(Path("/mock/path"))

    def test_parse_session_start_time(self):
        """Test parsing session start time from filename."""
        # Test with UTC time to avoid timezone dependencies
        filename_actual = Path("TS_CS1_2024-01-01T12_00_00.csv")
        expected = datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("UTC"))
        actual = parse_session_start_time(
            filename_actual, local_timezone="UTC"
        )
        self.assertEqual(expected, actual)

    def test_parse_session_start_time_with_default_timezone(self):
        """Test parsing session start time with default timezone."""
        # Test with no timezone specified (uses system default)
        filename_actual = Path("TS_CS1_2024-01-01T12_00_00.csv")
        actual = parse_session_start_time(filename_actual)
        # Should have timezone info from system default
        self.assertIsNotNone(actual.tzinfo)
        self.assertEqual(actual.hour, 12)
        self.assertEqual(actual.minute, 0)
        self.assertEqual(actual.second, 0)

    def test_parse_session_start_time_invalid_format(self):
        """Test parsing session start time with invalid filename format."""
        # Test with invalid filename format (not enough parts)
        with self.assertRaises(ValueError) as cm:
            parse_session_start_time(Path("TS_CS1.csv"))
        self.assertIn("Could not parse datetime", str(cm.exception))

        # Test with invalid datetime format
        with self.assertRaises(ValueError) as cm:
            parse_session_start_time(Path("TS_CS1_invalid-date.csv"))
        self.assertIn("Could not parse datetime", str(cm.exception))

    def test_extract_trial_data(self):
        """Test extraction of trial data from CSV."""
        # Create test DataFrame
        df = pd.DataFrame(
            {
                "TrialNumber": range(1, 11),  # 10 items
                "TotalRewards": range(0, 10),  # 10 items
                "ITI_s": [1.0] * 10,  # 10 items
            }
        )

        # Mock pandas.read_csv to return our test DataFrame
        with patch("pandas.read_csv", return_value=df):
            result = extract_trial_data(Path("/mock/trial_data.csv"))
            self.assertEqual(len(result), 10)
            self.assertTrue(
                all(
                    col in result.columns
                    for col in ["TrialNumber", "TotalRewards", "ITI_s"]
                )
            )

        # Test with missing columns
        invalid_df = pd.DataFrame({"Wrong": [1, 2, 3]})
        with patch("pandas.read_csv", return_value=invalid_df):
            with self.assertRaises(ValueError):
                extract_trial_data(Path("/mock/invalid.csv"))

    def test_extract_trial_data_missing_columns(self):
        """Test extraction of trial data with missing required columns."""
        # Create CSV with missing columns
        df = pd.DataFrame({"TrialNumber": [1, 2, 3]})  # Missing other columns

        with patch("pandas.read_csv", return_value=df):
            with self.assertRaises(ValueError) as cm:
                extract_trial_data(Path("/mock/trial_data.csv"))
            self.assertIn("missing required columns", str(cm.exception))
            self.assertIn("TotalRewards", str(cm.exception))
            self.assertIn("ITI_s", str(cm.exception))

    def test_calculate_session_timing(self):
        """Test calculation of session timing from trial data."""
        start_time = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))
        trial_data = pd.DataFrame({"ITI_s": [1.0] * 10})  # 10 seconds total

        end_time, duration = calculate_session_timing_from_trials(
            start_time, trial_data
        )
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
        # Create trial DataFrame
        df = pd.DataFrame(
            {
                "TrialNumber": range(1, 11),
                "TotalRewards": range(0, 10),
                "ITI_s": [1.0] * 10,
            }
        )

        # Expected session start time
        expected_start_time = datetime(
            2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("UTC")
        )

        with (
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".find_behavior_files",
                return_value=(["mock_ts_file"], ["mock_trial_file"]),
            ),
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".parse_session_start_time",
                return_value=expected_start_time,
            ),
            patch("pandas.read_csv", return_value=df),
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils"
                ".find_session_end_time",
                return_value=None,
            ),  # Force fallback to trial calculation
        ):
            # Test complete extraction using UTC
            start_time, epochs = extract_session_data(
                Path("/mock/path"),
                reward_units_per_trial=2.0,
                local_timezone="UTC",
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
        # Mock file objects
        mock_invalid_ts = Mock()
        mock_invalid_ts.name = "TS_CS1_20240101T154953.csv"  # Wrong format
        mock_valid_trial = Mock()
        mock_valid_trial.name = "TrialN_TrialType_ITI_001.csv"

        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "glob") as mock_glob,
            patch(
                "aind_metadata_mapper.pavlovian_behavior.utils."
                "validate_trial_file_format"
            ),
        ):
            mock_glob.side_effect = lambda pattern: (
                [mock_invalid_ts]
                if "TS_CS1_" in pattern
                else [mock_valid_trial]
            )

            # Should raise ValueError due to invalid behavior file format
            with self.assertRaises(ValueError) as cm:
                find_behavior_files(Path("/mock/path"))
            self.assertIn(
                "must be in format YYYY-MM-DDThh_mm_ss", str(cm.exception)
            )


if __name__ == "__main__":
    unittest.main()
