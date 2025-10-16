"""Tests for utility functions.

Strategy:
- Mock HTTP via @patch on requests.get and drive behavior using MagicMock responses:
  - status_code, json(), and raise_for_status() are set to simulate success, non-200, and exceptions.
- File IO is exercised in isolation using tempfile.TemporaryDirectory(); where cwd matters, tests temporarily chdir.
- Focus on error handling and return contracts (None vs. dict), not on external service availability.
- Keep tests fast by avoiding real network or filesystem side effects outside temp dirs.
"""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from aind_metadata_mapper import utils


class TestUtils(unittest.TestCase):
    def test_ensure_timezone_none(self):
        """Test that ensure_timezone handles None input by returning current time with timezone.

        When None is passed to ensure_timezone, it should return the current datetime
        with timezone information. This is useful for cases where a datetime field
        is optional but we need a timezone-aware datetime for processing.
        """
        dt = utils.ensure_timezone(None)
        self.assertIsNotNone(dt.tzinfo)

    def test_ensure_timezone_naive_datetime(self):
        """Test that ensure_timezone adds timezone info to naive datetime objects.

        When a naive datetime (without timezone info) is passed to ensure_timezone,
        it should add the system's local timezone information. This ensures all
        datetime objects are timezone-aware for consistent processing.
        """
        naive = datetime(2025, 1, 1, 12, 0, 0)
        dt = utils.ensure_timezone(naive)
        self.assertIsNotNone(dt.tzinfo)

    def test_ensure_timezone_iso_string(self):
        """Test that ensure_timezone correctly parses ISO format strings with timezone.

        When an ISO format string with timezone information is passed to ensure_timezone,
        it should parse it correctly and preserve the timezone. This handles the common
        case of datetime strings from APIs or configuration files.
        """
        aware_iso = "2025-01-01T12:00:00+00:00"
        dt = utils.ensure_timezone(aware_iso)
        self.assertEqual(dt.tzinfo, timezone(timedelta(seconds=0)))

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_procedures_success(self, mock_get):
        """Test that get_procedures successfully fetches and returns procedures data.

        When the metadata service returns a successful HTTP response with procedures data,
        get_procedures should return the parsed JSON data. This tests the happy path
        where the API call succeeds and returns valid procedures information.
        """
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"ok": True}
        mock_get.return_value = mock_resp
        result = utils.get_procedures("123")
        self.assertEqual(result, {"ok": True})

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_procedures_http_error(self, mock_get):
        """Test that get_procedures handles HTTP errors gracefully.

        When the metadata service returns an HTTP error (4xx, 5xx), get_procedures
        should catch the exception and return None instead of crashing. This ensures
        the mapper can continue processing even when the procedures service is unavailable.
        """
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("boom")
        mock_get.return_value = mock_resp
        result = utils.get_procedures("123")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_procedures_exception(self, mock_get):
        """Test that get_procedures handles network exceptions gracefully.

        When network issues occur (connection timeouts, DNS failures, etc.), get_procedures
        should catch the exception and return None. This prevents the entire mapping process
        from failing due to temporary network issues with the metadata service.
        """
        mock_get.side_effect = Exception("network")
        result = utils.get_procedures("123")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_intended_measurements_success(self, mock_get):
        """Test that get_intended_measurements successfully fetches measurement data.

        When the metadata service returns a successful HTTP 200 response with intended
        measurements data, get_intended_measurements should return the parsed JSON data.
        This tests the happy path where the API call succeeds and returns valid measurement
        assignments for the subject.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": []}
        mock_get.return_value = mock_resp
        result = utils.get_intended_measurements("123")
        self.assertEqual(result, {"data": []})

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_intended_measurements_non_200(self, mock_get):
        """Test that get_intended_measurements handles non-200 HTTP status codes.

        When the metadata service returns a non-200 status code (like 404 for subject not found),
        get_intended_measurements should return None instead of trying to parse the response.
        This handles cases where the subject doesn't exist or has no measurement assignments.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp
        result = utils.get_intended_measurements("123")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_intended_measurements_exception(self, mock_get):
        """Test that get_intended_measurements handles network exceptions gracefully.

        When network issues occur during the API call, get_intended_measurements should
        catch the exception and return None. This prevents the mapping process from failing
        due to temporary network issues with the intended measurements service.
        """
        mock_get.side_effect = Exception("network")
        result = utils.get_intended_measurements("123")
        self.assertIsNone(result)

    def test_write_acquisition_no_output_directory(self):
        """Test that write_acquisition writes files to current directory when no output directory specified.

        When output_directory is None, write_acquisition should write the acquisition JSON file
        to the current working directory. This tests the else branch of the conditional logic
        and ensures the function works correctly when no specific output location is provided.
        """
        # Create a minimal acquisition using the fixture data
        import json
        from pathlib import Path
        from types import SimpleNamespace

        fixture_path = Path("tests/fixtures/fip_intermediate.json")
        payload = json.loads(fixture_path.read_text())

        # Use the existing mapper to create a proper acquisition
        from aind_metadata_mapper.fip.mapper import FIPMapper

        mapper = FIPMapper()

        # Mock the external calls to avoid network dependencies
        with (
            patch.object(mapper, "_parse_intended_measurements", return_value=None),
            patch.object(mapper, "_parse_implanted_fibers", return_value=None),
        ):
            acquisition = mapper._transform(SimpleNamespace(**payload))

        with tempfile.TemporaryDirectory() as tmpdir:
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)

                result = utils.write_acquisition(acquisition, output_directory=None, filename="test.json")

                self.assertTrue(result.name.endswith("test.json"))
                self.assertTrue(result.exists())
            finally:
                os.chdir(original_cwd)

    def test_write_acquisition_with_output_directory(self):
        """Test that write_acquisition writes files to specified output directory.

        When output_directory is provided, write_acquisition should write the acquisition JSON file
        to that specific directory. This tests the if branch of the conditional logic and ensures
        the function correctly handles custom output locations for the generated metadata files.
        """
        # Create a minimal acquisition using the fixture data
        import json
        from pathlib import Path
        from types import SimpleNamespace

        fixture_path = Path("tests/fixtures/fip_intermediate.json")
        payload = json.loads(fixture_path.read_text())

        # Use the existing mapper to create a proper acquisition
        from aind_metadata_mapper.fip.mapper import FIPMapper

        mapper = FIPMapper()

        # Mock the external calls to avoid network dependencies
        with (
            patch.object(mapper, "_parse_intended_measurements", return_value=None),
            patch.object(mapper, "_parse_implanted_fibers", return_value=None),
        ):
            acquisition = mapper._transform(SimpleNamespace(**payload))

        with tempfile.TemporaryDirectory() as tmpdir:
            result = utils.write_acquisition(acquisition, output_directory=tmpdir, filename="test.json")

            self.assertTrue(result.name.endswith("test.json"))
            self.assertTrue(result.exists())
            self.assertEqual(result.parent, Path(tmpdir))


if __name__ == "__main__":
    unittest.main()
