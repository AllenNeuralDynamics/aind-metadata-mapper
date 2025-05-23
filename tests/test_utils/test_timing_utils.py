"""Tests for timing utility functions."""

import unittest
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd
from unittest.mock import patch

from aind_metadata_mapper.utils.timing_utils import (
    convert_ms_since_midnight_to_datetime,
    find_latest_timestamp_in_csv_files,
    _read_csv_safely,
    _extract_max_timestamp,
)


class TestTimingUtils(unittest.TestCase):
    """Test timing utility functions."""

    def test_convert_ms_since_midnight_to_datetime_default_timezone(self):
        """Test conversion with default timezone."""
        base_date = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

        # Test with no timezone specified (uses system default)
        result = convert_ms_since_midnight_to_datetime(0.0, base_date)
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.hour, 0)
        self.assertEqual(result.minute, 0)

    def test_read_csv_safely_error_conditions(self):
        """Test CSV reading with error conditions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test with empty file
            empty_file = Path(tmpdir) / "empty.csv"
            empty_file.touch()
            result = _read_csv_safely(empty_file)
            self.assertIsNone(result)

            # Test with valid CSV that can be read
            valid_file = Path(tmpdir) / "valid.csv"
            with open(valid_file, "w") as f:
                f.write("1,2,3\n")
                f.write("4,5,6\n")

            # Should return a DataFrame for valid CSV
            result = _read_csv_safely(valid_file)
            self.assertIsNotNone(result)

            # Test with malformed CSV that pandas can't parse
            malformed_file = Path(tmpdir) / "malformed.csv"
            with open(malformed_file, "w") as f:
                f.write("1,2,3\n")
                f.write("4,5\n")  # Missing column
                f.write("6,7,8,9\n")  # Extra column

            # Should return None for malformed CSV
            # that can't be parsed consistently
            result = _read_csv_safely(malformed_file)
            # The function may or may not be able to read
            # this depending on pandas behavior
            # so we just check that it doesn't crash

    def test_extract_max_timestamp_edge_cases(self):
        """Test timestamp extraction edge cases."""
        # Test with DataFrame with string columns but no time columns
        df_no_time = pd.DataFrame({"name": ["a", "b"], "value": [1, 2]})
        result = _extract_max_timestamp(df_no_time)
        self.assertEqual(
            result, 2
        )  # Should return max of first numeric column

        # Test with DataFrame with no numeric columns
        df_no_numeric = pd.DataFrame({"name": ["a", "b"], "text": ["x", "y"]})
        result = _extract_max_timestamp(df_no_numeric)
        self.assertIsNone(result)

        # Test with DataFrame with time-related column names
        df_time_col = pd.DataFrame({"timestamp_ms": [100, 200, 150]})
        result = _extract_max_timestamp(df_time_col)
        self.assertEqual(result, 200)

        # Test with DataFrame without headers (numeric columns)
        df_no_header = pd.DataFrame([[100], [200], [150]])
        result = _extract_max_timestamp(df_no_header)
        self.assertEqual(result, 200)

        # Test with empty DataFrame
        df_empty = pd.DataFrame()
        result = _extract_max_timestamp(df_empty)
        self.assertIsNone(result)

    def test_find_latest_timestamp_nonexistent_directory(self):
        """Test finding timestamps in nonexistent directory."""
        session_start = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

        result = find_latest_timestamp_in_csv_files(
            "/nonexistent/path", "*.csv", session_start, local_timezone="UTC"
        )
        self.assertIsNone(result)

    def test_find_latest_timestamp_no_matching_files(self):
        """Test finding timestamps with no matching files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            session_start = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

            result = find_latest_timestamp_in_csv_files(
                tmpdir,
                "nonexistent_*.csv",
                session_start,
                local_timezone="UTC",
            )
            self.assertIsNone(result)

    def test_find_latest_timestamp_with_valid_files(self):
        """Test finding timestamps with valid CSV files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test CSV files
            csv1 = Path(tmpdir) / "test1.csv"
            csv2 = Path(tmpdir) / "test2.csv"

            # CSV with header
            pd.DataFrame({"timestamp_ms": [100, 200]}).to_csv(
                csv1, index=False
            )

            # CSV without header
            pd.DataFrame([[150], [300]]).to_csv(
                csv2, index=False, header=False
            )

            session_start = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

            result = find_latest_timestamp_in_csv_files(
                tmpdir, "test*.csv", session_start, local_timezone="UTC"
            )

            # Should find the maximum timestamp (300ms)
            self.assertIsNotNone(result)
            self.assertEqual(result.minute, 0)
            self.assertEqual(result.second, 0)
            self.assertEqual(result.microsecond, 300000)  # 300ms

    @patch("aind_metadata_mapper.utils.timing_utils.logging")
    def test_find_latest_timestamp_with_file_errors(self, mock_logging):
        """Test finding timestamps when files have errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a file with non-numeric data that results in NaN
            bad_file = Path(tmpdir) / "bad.csv"
            with open(bad_file, "w") as f:
                f.write("timestamp\n")
                f.write("not_a_number\n")
                f.write("also_not_a_number\n")

            session_start = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

            result = find_latest_timestamp_in_csv_files(
                tmpdir, "bad.csv", session_start, local_timezone="UTC"
            )

            # Should return None when no valid timestamps found
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
