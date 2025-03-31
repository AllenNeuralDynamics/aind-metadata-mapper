"""Tests for fiber photometry utility functions."""

import unittest
from datetime import datetime
from pathlib import Path
import tempfile
import pandas as pd
from zoneinfo import ZoneInfo

from aind_metadata_mapper.fib.utils import (
    convert_ms_since_midnight_to_datetime,
    extract_session_start_time_from_files,
    extract_session_end_time_from_files,
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
        self.assertEqual(result.hour, 8)  # midnight PT = 08:00 UTC
        self.assertEqual(result.minute, 0)
        self.assertEqual(result.second, 0)
        self.assertEqual(result.microsecond, 0)

        # Test arbitrary time (3723456.789 ms = 01:02:03.456789)
        result = convert_ms_since_midnight_to_datetime(
            3723456.789, base_date, local_timezone="America/Los_Angeles"
        )
        self.assertEqual(result.hour, 9)  # 01:02 PT = 09:02 UTC
        self.assertEqual(result.minute, 2)
        self.assertEqual(result.second, 3)
        self.assertEqual(result.microsecond, 456789)

    def test_extract_session_start_time_from_files(self):
        """Test extraction of session start time from filenames."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test directory structure
            fib_dir = Path(tmpdir) / "fib"
            fib_dir.mkdir()

            # Create test files with timestamps
            timestamp = "2024-01-01T15_49_53"
            valid_file = fib_dir / f"FIP_DataG_{timestamp}.csv"
            valid_file.touch()

            invalid_file = fib_dir / "FIP_DataG_invalid.csv"
            invalid_file.touch()

            # Test with valid file
            result = extract_session_start_time_from_files(
                tmpdir, local_timezone="America/Los_Angeles"
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.year, 2024)
            self.assertEqual(result.month, 1)
            self.assertEqual(result.day, 1)
            self.assertEqual(result.hour, 23)  # 15:49 PT = 23:49 UTC
            self.assertEqual(result.minute, 49)
            self.assertEqual(result.second, 53)

            # Test with non-existent directory
            result = extract_session_start_time_from_files(
                "/nonexistent/path", local_timezone="America/Los_Angeles"
            )
            self.assertIsNone(result)

            # Test with directory containing no valid files
            empty_dir = Path(tmpdir) / "empty"
            empty_dir.mkdir()
            result = extract_session_start_time_from_files(
                empty_dir, local_timezone="America/Los_Angeles"
            )
            self.assertIsNone(result)

    def test_extract_session_end_time_from_files(self):
        """Test extraction of session end time from CSV data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test directory structure
            fib_dir = Path(tmpdir) / "fib"
            fib_dir.mkdir()

            # Create test CSV file with millisecond data
            csv_file = fib_dir / "FIP_DataG.csv"
            df = pd.DataFrame(
                {0: [0.0, 3723456.789]}
            )  # Start at 0ms, end at 01:02:03.456789
            df.to_csv(csv_file, index=False, header=False)

            # Create session start time in UTC
            session_start = datetime(
                2024, 1, 1, 8, 0, tzinfo=ZoneInfo("UTC")
            )  # 00:00 PT

            # Test with valid data
            result = extract_session_end_time_from_files(
                tmpdir, session_start, local_timezone="America/Los_Angeles"
            )
            self.assertIsNotNone(result)
            self.assertEqual(result.hour, 9)  # 01:02 PT = 09:02 UTC
            self.assertEqual(result.minute, 2)
            self.assertEqual(result.second, 3)
            self.assertEqual(result.microsecond, 456789)

            # Test with empty CSV
            empty_csv = fib_dir / "FIP_DataG_empty.csv"
            pd.DataFrame().to_csv(empty_csv, index=False, header=False)
            result = extract_session_end_time_from_files(
                tmpdir, session_start, local_timezone="America/Los_Angeles"
            )
            self.assertIsNotNone(
                result
            )  # Should still return result from valid file

            # Test with non-existent directory
            result = extract_session_end_time_from_files(
                "/nonexistent/path",
                session_start,
                local_timezone="America/Los_Angeles",
            )
            self.assertIsNone(result)

            # Test with invalid CSV data
            invalid_csv = fib_dir / "FIP_DataG_invalid.csv"
            invalid_csv.write_text("invalid,data\n")
            result = extract_session_end_time_from_files(
                tmpdir, session_start, local_timezone="America/Los_Angeles"
            )
            self.assertIsNotNone(
                result
            )  # Should still return result from valid file


if __name__ == "__main__":
    unittest.main()
