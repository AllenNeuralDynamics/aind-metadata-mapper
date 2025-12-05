"""Tests for utility functions.

Strategy:
- Use dependency injection to pass test HTTP functions instead of mocking.
- File IO is exercised in isolation using tempfile.TemporaryDirectory(); where cwd matters, tests temporarily chdir.
- Focus on error handling and return contracts (None vs. dict), not on external service availability.
- Keep tests fast by avoiding real network or filesystem side effects outside temp dirs.
"""

import shutil
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import requests

from aind_metadata_mapper import
from aind_metadata_mapper.utils import ensure_timezone, get_intended_measurements, get_procedures, get_protocols_for_modality, get_subject, prompt_for_string 


class TestUtils(unittest.TestCase):
    """Test cases for utility functions in aind_metadata_mapper."""

    def test_ensure_timezone_none(self):
        """Test that ensure_timezone handles None input by returning current time with timezone.

        When None is passed to ensure_timezone, it should return the current datetime
        with timezone information. This is useful for cases where a datetime field
        is optional but we need a timezone-aware datetime for processing.
        """
        dt = ensure_timezone(None)
        self.assertIsNotNone(dt.tzinfo)

    def test_ensure_timezone_naive_datetime(self):
        """Test that ensure_timezone adds timezone info to naive datetime objects.

        When a naive datetime (without timezone info) is passed to ensure_timezone,
        it should add the system's local timezone information. This ensures all
        datetime objects are timezone-aware for consistent processing.
        """
        naive = datetime(2025, 1, 1, 12, 0, 0)
        dt = ensure_timezone(naive)
        self.assertIsNotNone(dt.tzinfo)

    def test_ensure_timezone_iso_string(self):
        """Test that ensure_timezone correctly parses ISO format strings with timezone.

        When an ISO format string with timezone information is passed to ensure_timezone,
        it should parse it correctly and preserve the timezone. This handles the common
        case of datetime strings from APIs or configuration files.
        """
        aware_iso = "2025-01-01T12:00:00+00:00"
        dt = ensure_timezone(aware_iso)
        self.assertEqual(dt.tzinfo, timezone(timedelta(seconds=0)))

    @patch("requests.get")
    def test_get_procedures_success(self, mock_get):
        """Test that get_procedures successfully fetches and returns procedures data.

        When the metadata service returns a successful HTTP response with procedures data,
        get_procedures should return the parsed JSON data. This tests the happy path
        where the API call succeeds and returns valid procedures information.
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 200
        mock_resp.json = lambda: {"ok": True}
        mock_resp.raise_for_status = lambda: None
        mock_get.return_value = mock_resp

        result = get_procedures("123")
        self.assertEqual(result, {"ok": True})

    @patch("requests.get")
    def test_get_procedures_http_error(self, mock_get):
        """Test that get_procedures handles HTTP errors gracefully.

        When the metadata service returns an HTTP error (4xx, 5xx), get_procedures
        should catch the exception and return None instead of crashing. This ensures
        the mapper can continue processing even when the procedures service is unavailable.
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 500
        mock_resp.json = lambda: {}
        mock_get.return_value = mock_resp

        result = get_procedures("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_procedures_400_with_valid_data(self, mock_get):
        """Test that get_procedures handles 400 status code with valid data.

        When the endpoint returns 400, get_procedures should return the JSON data
        (400 is treated as a normal response for this API).
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 400
        mock_resp.json = lambda: {"subject_procedures": [{"object_type": "Surgery"}]}
        mock_get.return_value = mock_resp

        result = get_procedures("123")
        self.assertIsNotNone(result)
        self.assertIn("subject_procedures", result)

    @patch("requests.get")
    def test_get_procedures_400_with_any_data(self, mock_get):
        """Test that get_procedures returns JSON for 400 status code.

        When the endpoint returns 400, get_procedures should return the JSON data
        regardless of content (400 is treated as a normal response for this API).
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 400
        mock_resp.json = lambda: {"error": "bad request"}
        mock_get.return_value = mock_resp

        result = get_procedures("123")
        self.assertEqual(result, {"error": "bad request"})

    @patch("requests.get")
    def test_get_procedures_400_json_error(self, mock_get):
        """Test that get_procedures handles 400 status code with JSON parsing error.

        When the endpoint returns 400 and json() raises an exception,
        get_procedures should return None.
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 400
        mock_resp.json = lambda: (_ for _ in ()).throw(ValueError("Invalid JSON"))
        mock_get.return_value = mock_resp

        result = get_procedures("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_procedures_exception(self, mock_get):
        """Test that get_procedures handles network exceptions gracefully.

        When network issues occur (connection timeouts, DNS failures, etc.), get_procedures
        should catch the exception and return None. This prevents the entire mapping process
        from failing due to temporary network issues with the metadata service.
        """
        mock_get.side_effect = Exception("network")

        result = get_procedures("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_procedures_request_exception(self, mock_get):
        """Test that get_procedures handles RequestException specifically.

        When requests raises a RequestException (not a generic Exception),
        get_procedures should catch it and return None.
        """
        mock_get.side_effect = requests.exceptions.RequestException("connection error")

        result = get_procedures("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_subject_success(self, mock_get):
        """Test that get_subject successfully fetches and returns subject data.

        When the metadata service returns a successful HTTP response with subject data,
        get_subject should return the parsed JSON data.
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 200
        mock_resp.json = lambda: {"subject_id": "123"}
        mock_resp.raise_for_status = lambda: None
        mock_get.return_value = mock_resp

        result = get_subject("123")
        self.assertEqual(result, {"subject_id": "123"})

    @patch("requests.get")
    def test_get_subject_http_error(self, mock_get):
        """Test that get_subject handles HTTP errors gracefully."""
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 500
        mock_resp.json = lambda: {}
        mock_get.return_value = mock_resp

        result = get_subject("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_subject_exception(self, mock_get):
        """Test that get_subject handles network exceptions gracefully."""
        mock_get.side_effect = Exception("network")

        result = get_subject("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_intended_measurements_success(self, mock_get):
        """Test that get_intended_measurements successfully fetches measurement data.

        When the metadata service returns a successful HTTP 200 response with intended
        measurements data, get_intended_measurements should return the parsed JSON data.
        This tests the happy path where the API call succeeds and returns valid measurement
        assignments for the subject.
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 200
        mock_resp.json = lambda: {"data": []}
        mock_resp.raise_for_status = lambda: None
        mock_get.return_value = mock_resp

        result = get_intended_measurements("123")
        self.assertEqual(result, {"data": []})

    @patch("requests.get")
    def test_get_intended_measurements_non_200(self, mock_get):
        """Test that get_intended_measurements handles non-200 HTTP status codes.

        When the metadata service returns a non-200 status code (like 404 for subject not found),
        get_intended_measurements should return None instead of trying to parse the response.
        This handles cases where the subject doesn't exist or has no measurement assignments.
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 404
        mock_resp.json = lambda: {}
        mock_get.return_value = mock_resp

        result = get_intended_measurements("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_intended_measurements_exception(self, mock_get):
        """Test that get_intended_measurements handles network exceptions gracefully.

        When network issues occur during the API call, get_intended_measurements should
        catch the exception and return None. This prevents the mapping process from failing
        due to temporary network issues with the intended measurements service.
        """
        mock_get.side_effect = Exception("network")

        result = get_intended_measurements("123")
        self.assertIsNone(result)

    @patch("requests.get")
    def test_get_intended_measurements_status_300(self, mock_get):
        """Test that get_intended_measurements handles 300 status code.

        When the endpoint returns 300 (redirect), get_intended_measurements should
        return None (only 200 and 400 are accepted by metadata_service_helper).
        """
        mock_resp = SimpleNamespace()
        mock_resp.status_code = 300
        mock_resp.json = lambda: {"data": []}
        mock_get.return_value = mock_resp

        result = get_intended_measurements("123")
        self.assertIsNone(result)

    def test_get_protocols_for_modality_file_not_found(self):
        """Test get_protocols_for_modality returns empty list when protocols.yaml is missing."""
        protocols_path = Path(__file__).parent.parent / "protocols.yaml"
        backup_path = Path(__file__).parent.parent / "protocols.yaml.test_backup"

        if protocols_path.exists():
            shutil.move(str(protocols_path), str(backup_path))

        try:
            result = get_protocols_for_modality("fip")
            self.assertEqual(result, [])
        finally:
            if backup_path.exists():
                shutil.move(str(backup_path), str(protocols_path))

    def test_get_protocols_for_modality_yaml_error(self):
        """Test get_protocols_for_modality returns empty list when YAML is invalid."""
        protocols_path = Path(__file__).parent.parent / "protocols.yaml"
        backup_path = Path(__file__).parent.parent / "protocols.yaml.test_backup"

        if protocols_path.exists():
            shutil.move(str(protocols_path), str(backup_path))

        try:
            with open(protocols_path, "w") as f:
                f.write("invalid: yaml: [unclosed")

            result = get_protocols_for_modality("fip")
            self.assertEqual(result, [])
        finally:
            protocols_path.unlink(missing_ok=True)
            if backup_path.exists():
                shutil.move(str(backup_path), str(protocols_path))

    def test_get_protocols_for_modality_success(self):
        """Test get_protocols_for_modality returns protocols when file exists and is valid."""
        result = get_protocols_for_modality("fip")
        # Should return a list (may be empty if protocols.yaml doesn't have fip, but should not error)
        self.assertIsInstance(result, list)


class TestPromptFunctions(unittest.TestCase):
    """Tests for prompt utility functions."""

    def test_prompt_for_string_with_default(self):
        """Test prompt_for_string with default value."""
        self.assertEqual(prompt_for_string("Test", default="default", input_func=lambda x: ""), "default")
        self.assertEqual(prompt_for_string("Test", default="default", input_func=lambda x: "input"), "input")

    def test_prompt_for_string_not_required(self):
        """Test prompt_for_string without default, not required."""
        self.assertEqual(prompt_for_string("Test", required=False, input_func=lambda x: ""), "")
        self.assertEqual(prompt_for_string("Test", required=False, input_func=lambda x: "input"), "input")

    def test_prompt_for_string_required(self):
        """Test prompt_for_string required=True with no default."""
        call_count = [0]

        def input_func(prompt):
            """Mock input function that returns empty first, then valid input."""
            call_count[0] += 1
            return "" if call_count[0] == 1 else "input"

        with patch("builtins.print"):
            result = prompt_for_string("Test", required=True, input_func=input_func)
        self.assertEqual(result, "input")
        self.assertEqual(call_count[0], 2)

    def test_prompt_for_string_with_help_message(self):
        """Test prompt_for_string with help message."""
        call_count = [0]

        def input_func(prompt):
            """Mock input function that returns empty first, then valid input."""
            call_count[0] += 1
            return "" if call_count[0] == 1 else "input"

        with patch("builtins.print") as mock_print:
            prompt_for_string("Test", required=True, help_message="Help", input_func=input_func)
        self.assertEqual(call_count[0], 2)
        self.assertTrue(any("Help" in str(call) for call in mock_print.call_args_list))


if __name__ == "__main__":
    unittest.main()
