"""Tests for timing utility functions."""

import unittest
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

    def setUp(self):
        """Set up common test data."""
        self.session_start = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

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
        # Mock empty DataFrame
        mock_empty_df = pd.DataFrame()

        # Mock valid DataFrame
        mock_valid_df = pd.DataFrame(
            {"col1": [1, 4], "col2": [2, 5], "col3": [3, 6]}
        )

        # Test with empty file (pandas returns empty DataFrame)
        with patch("pandas.read_csv", return_value=mock_empty_df):
            result = _read_csv_safely(Path("empty.csv"))
            self.assertIsNone(result)

        # Test with valid CSV that can be read
        with patch("pandas.read_csv", return_value=mock_valid_df):
            result = _read_csv_safely(Path("valid.csv"))
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 2)

        # Test with malformed CSV that raises ParserError
        with patch(
            "pandas.read_csv", side_effect=pd.errors.ParserError("Parse error")
        ):
            # Should try reading without header as fallback
            with patch(
                "pandas.read_csv", return_value=mock_valid_df
            ) as mock_fallback:
                result = _read_csv_safely(Path("malformed.csv"))
                # Should call read_csv twice (with and without header)
                self.assertEqual(mock_fallback.call_count, 1)

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
        # Mock Path.exists to return False
        with patch.object(Path, "exists", return_value=False):
            result = find_latest_timestamp_in_csv_files(
                "/nonexistent/path",
                "*.csv",
                self.session_start,
                local_timezone="UTC",
            )
            self.assertIsNone(result)

    def test_find_latest_timestamp_no_matching_files(self):
        """Test finding timestamps with no matching files."""
        # Mock Path methods directly
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "glob", return_value=[]),  # No matching files
        ):
            result = find_latest_timestamp_in_csv_files(
                "/mock/path",
                "nonexistent_*.csv",
                self.session_start,
                local_timezone="UTC",
            )
            self.assertIsNone(result)

    def test_find_latest_timestamp_with_valid_files(self):
        """Test finding timestamps with valid CSV files."""
        # Mock DataFrames
        df1 = pd.DataFrame({"timestamp_ms": [100, 200]})
        df2 = pd.DataFrame([[150], [300]])  # No header

        # Mock Path methods and CSV reading
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(
                Path, "glob", return_value=["test1.csv", "test2.csv"]
            ),
            patch(
                "aind_metadata_mapper.utils.timing_utils._read_csv_safely",
                side_effect=[df1, df2],
            ),
        ):
            result = find_latest_timestamp_in_csv_files(
                "/mock/path",
                "test*.csv",
                self.session_start,
                local_timezone="UTC",
            )

            # Should find the maximum timestamp (300ms)
            self.assertIsNotNone(result)
            self.assertEqual(result.minute, 0)
            self.assertEqual(result.second, 0)
            self.assertEqual(result.microsecond, 300000)  # 300ms

    @patch("aind_metadata_mapper.utils.timing_utils.logging")
    def test_find_latest_timestamp_with_file_errors(self, mock_logging):
        """Test finding timestamps when files have errors."""
        # Mock DataFrame with non-numeric data that results in NaN
        df_with_nan = pd.DataFrame(
            {"timestamp": ["not_a_number", "also_not_a_number"]}
        )

        # Mock Path methods and CSV reading
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "glob", return_value=["bad.csv"]),
            patch(
                "aind_metadata_mapper.utils.timing_utils._read_csv_safely",
                return_value=df_with_nan,
            ),
        ):
            result = find_latest_timestamp_in_csv_files(
                "/mock/path",
                "bad.csv",
                self.session_start,
                local_timezone="UTC",
            )

            # Should return None when no valid timestamps found
            self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
