"""Tests for fiber photometry utility functions."""

import unittest
from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo
from unittest.mock import patch, MagicMock

from aind_metadata_mapper.fip.utils import (
    extract_session_start_time_from_files,
    extract_session_end_time_from_files,
)
from aind_metadata_mapper.utils.timing_utils import (
    convert_ms_since_midnight_to_datetime,
)


class TestFiberPhotometryUtils(unittest.TestCase):
    """Test fiber photometry utility functions."""

    def test_convert_ms_since_midnight_to_datetime(self):
        """Test conversion of milliseconds since midnight to datetime."""
        # Create a base date in UTC
        base_date = datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

        # Test midnight (0 ms)
        result = convert_ms_since_midnight_to_datetime(
            0.0, base_date, local_timezone="America/Los_Angeles"
        )
        self.assertEqual(result.hour, 0)  # midnight PT = 00:00 PT
        self.assertEqual(result.minute, 0)
        self.assertEqual(result.second, 0)
        self.assertEqual(result.microsecond, 0)

        # Test arbitrary time (3723456.789 ms = 01:02:03.456789)
        result = convert_ms_since_midnight_to_datetime(
            3723456.789, base_date, local_timezone="America/Los_Angeles"
        )
        self.assertEqual(result.hour, 1)  # 01:02 PT = 01:02 PT
        self.assertEqual(result.minute, 2)
        self.assertEqual(result.second, 3)
        self.assertEqual(result.microsecond, 456789)

        # Test DST transition (2025-03-09 in America/Los_Angeles)
        # At 2 AM, clock jumps forward to 3 AM
        dst_base_date = datetime(
            2025, 3, 9, tzinfo=ZoneInfo("America/Los_Angeles")
        )

        # Test 2:30 AM (which doesn't exist due to DST jump)
        two_thirty_am = 1000 * 60 * 60 * 2.5  # 2.5 hours in milliseconds
        result_two_thirty = convert_ms_since_midnight_to_datetime(
            two_thirty_am, dst_base_date, local_timezone="America/Los_Angeles"
        )

        # Test 3:30 AM (after DST jump)
        three_thirty_am = 1000 * 60 * 60 * 3.5  # 3.5 hours in milliseconds
        result_three_thirty = convert_ms_since_midnight_to_datetime(
            three_thirty_am,
            dst_base_date,
            local_timezone="America/Los_Angeles",
        )

        # The times should be an hour apart in UTC
        time_difference = (
            result_three_thirty - result_two_thirty
        ).total_seconds()
        self.assertEqual(
            time_difference, 3600
        )  # Should be exactly 1 hour (3600 seconds)

        # Verify specific UTC times
        # 2:30 AM PST (before DST) = 10:30 UTC
        self.assertEqual(result_two_thirty.hour, 2)
        self.assertEqual(result_two_thirty.minute, 30)

        # 3:30 AM PDT (after DST) = 11:30 UTC
        self.assertEqual(result_three_thirty.hour, 3)
        self.assertEqual(result_three_thirty.minute, 30)

    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.exists")
    def test_extract_session_start_time_from_files(
        self, mock_exists, mock_glob
    ):
        """Test extraction of session start time from filenames."""
        # Mock directory existence
        mock_exists.return_value = True

        # Mock file with timestamp in name
        mock_file = MagicMock()
        mock_file.name = "FIP_DataG_2024-01-01T15_49_53.csv"
        mock_glob.return_value = [mock_file]

        # Test with valid file
        result = extract_session_start_time_from_files(
            "/dummy/path", local_timezone="America/Los_Angeles"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 1)
        self.assertEqual(result.hour, 15)  # 15:49 PT = 15:49 PT
        self.assertEqual(result.minute, 49)
        self.assertEqual(result.second, 53)

        # Test with non-existent directory
        mock_exists.return_value = False
        mock_glob.return_value = (
            []
        )  # Reset glob mock for non-existent directory
        result = extract_session_start_time_from_files(
            "/dummy/path", local_timezone="America/Los_Angeles"
        )
        self.assertIsNone(result)

        # Test with directory containing no valid files
        mock_exists.return_value = True
        mock_glob.return_value = []
        result = extract_session_start_time_from_files(
            "/dummy/path", local_timezone="America/Los_Angeles"
        )
        self.assertIsNone(result)

    @patch("pandas.read_csv")
    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.exists")
    def test_extract_session_end_time_from_files(
        self, mock_exists, mock_glob, mock_read_csv
    ):
        """Test extraction of session end time from CSV data."""
        # Mock directory existence
        mock_exists.return_value = True

        # Mock file
        mock_file = MagicMock()
        mock_file.name = "FIP_DataG.csv"
        mock_glob.return_value = [mock_file]

        # Mock CSV data - 0ms start, ~1h end time
        mock_read_csv.return_value = pd.DataFrame({0: [0.0, 3723456.789]})

        # Create session start time in UTC
        session_start = datetime(
            2024, 1, 1, 8, 0, tzinfo=ZoneInfo("UTC")
        )  # 00:00 PT

        # Test with valid data
        result = extract_session_end_time_from_files(
            "/dummy/path", session_start, local_timezone="America/Los_Angeles"
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.hour, 1)  # 01:02 PT = 01:02 PT
        self.assertEqual(result.minute, 2)
        self.assertEqual(result.second, 3)
        self.assertEqual(result.microsecond, 456789)

        # Test with empty file
        mock_read_csv.return_value = pd.DataFrame()
        result = extract_session_end_time_from_files(
            "/dummy/path", session_start, local_timezone="America/Los_Angeles"
        )
        self.assertIsNone(result)

        # Test with non-existent directory
        mock_exists.return_value = False
        result = extract_session_end_time_from_files(
            "/dummy/path", session_start, local_timezone="America/Los_Angeles"
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
