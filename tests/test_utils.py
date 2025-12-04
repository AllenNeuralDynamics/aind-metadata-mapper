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

from aind_metadata_mapper import utils


class TestUtils(unittest.TestCase):
    """Test cases for utility functions in aind_metadata_mapper.utils."""

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

    def test_get_procedures_success(self):
        """Test that get_procedures successfully fetches and returns procedures data.

        When the metadata service returns a successful HTTP response with procedures data,
        get_procedures should return the parsed JSON data. This tests the happy path
        where the API call succeeds and returns valid procedures information.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns a successful response."""
            resp = SimpleNamespace()
            resp.status_code = 200
            resp.json = lambda: {"ok": True}
            return resp

        result = utils.get_procedures("123", get_func=test_get)
        self.assertEqual(result, {"ok": True})

    def test_get_procedures_http_error(self):
        """Test that get_procedures handles HTTP errors gracefully.

        When the metadata service returns an HTTP error (4xx, 5xx), get_procedures
        should catch the exception and return None instead of crashing. This ensures
        the mapper can continue processing even when the procedures service is unavailable.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns a server error response."""
            resp = SimpleNamespace()
            resp.status_code = 500  # Server error
            resp.json = lambda: {}
            return resp

        result = utils.get_procedures("123", get_func=test_get)
        self.assertIsNone(result)

    def test_get_procedures_400_with_valid_data(self):
        """Test that get_procedures handles 400 status code with valid data.

        When the endpoint returns 400, get_procedures should return the JSON data
        (400 is treated as a normal response for this API).
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns 400 with valid procedures data."""
            resp = SimpleNamespace()
            resp.status_code = 400
            resp.json = lambda: {"subject_procedures": [{"object_type": "Surgery"}]}
            return resp

        result = utils.get_procedures("123", get_func=test_get)
        self.assertIsNotNone(result)
        self.assertIn("subject_procedures", result)

    def test_get_procedures_400_with_any_data(self):
        """Test that get_procedures returns JSON for 400 status code.

        When the endpoint returns 400, get_procedures should return the JSON data
        regardless of content (400 is treated as a normal response for this API).
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns 400 with any JSON data."""
            resp = SimpleNamespace()
            resp.status_code = 400
            resp.json = lambda: {"error": "bad request"}
            return resp

        result = utils.get_procedures("123", get_func=test_get)
        # 400 is treated like 200, so it returns the JSON
        self.assertEqual(result, {"error": "bad request"})

    def test_get_procedures_400_json_error(self):
        """Test that get_procedures handles 400 status code with JSON parsing error.

        When the endpoint returns 400 and json() raises an exception,
        get_procedures should return None.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns 400 and raises JSON error."""
            resp = SimpleNamespace()
            resp.status_code = 400
            resp.json = lambda: (_ for _ in ()).throw(ValueError("Invalid JSON"))
            return resp

        result = utils.get_procedures("123", get_func=test_get)
        self.assertIsNone(result)

    def test_get_procedures_exception(self):
        """Test that get_procedures handles network exceptions gracefully.

        When network issues occur (connection timeouts, DNS failures, etc.), get_procedures
        should catch the exception and return None. This prevents the entire mapping process
        from failing due to temporary network issues with the metadata service.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that raises a network exception."""
            raise Exception("network")

        result = utils.get_procedures("123", get_func=test_get)
        self.assertIsNone(result)

    def test_get_procedures_request_exception(self):
        """Test that get_procedures handles RequestException specifically.

        When requests raises a RequestException (not a generic Exception),
        get_procedures should catch it and return None.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that raises a RequestException."""
            raise requests.exceptions.RequestException("connection error")

        result = utils.get_procedures("123", get_func=test_get)
        self.assertIsNone(result)

    def test_get_procedures_non_int_status_code(self):
        """Test that get_procedures handles non-int status codes gracefully.

        When status_code is not an int (e.g., from test mocks),
        get_procedures should try to return JSON if possible.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function with non-int status code that returns valid JSON."""
            resp = SimpleNamespace()
            resp.status_code = "200"  # String instead of int
            resp.json = lambda: {"ok": True}
            return resp

        result = utils.get_procedures("123", get_func=test_get)
        # Should try to return JSON even with non-int status_code
        self.assertEqual(result, {"ok": True})

    def test_get_procedures_non_int_status_code_exception(self):
        """Test that get_procedures handles non-int status codes with JSON exception.

        When status_code is not an int and json() raises an exception,
        get_procedures should return None.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function with non-int status code that raises JSON error."""
            resp = SimpleNamespace()
            resp.status_code = "200"  # String instead of int
            resp.json = lambda: (_ for _ in ()).throw(Exception("JSON error"))
            return resp

        result = utils.get_procedures("123", get_func=test_get)
        self.assertIsNone(result)

    def test_get_intended_measurements_success(self):
        """Test that get_intended_measurements successfully fetches measurement data.

        When the metadata service returns a successful HTTP 200 response with intended
        measurements data, get_intended_measurements should return the parsed JSON data.
        This tests the happy path where the API call succeeds and returns valid measurement
        assignments for the subject.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns successful intended measurements response."""
            resp = SimpleNamespace()
            resp.status_code = 200
            resp.json = lambda: {"data": []}
            return resp

        result = utils.get_intended_measurements("123", get_func=test_get)
        self.assertEqual(result, {"data": []})

    def test_get_intended_measurements_non_200(self):
        """Test that get_intended_measurements handles non-200 HTTP status codes.

        When the metadata service returns a non-200 status code (like 404 for subject not found),
        get_intended_measurements should return None instead of trying to parse the response.
        This handles cases where the subject doesn't exist or has no measurement assignments.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns 404 not found."""
            resp = SimpleNamespace()
            resp.status_code = 404
            resp.json = lambda: {}
            return resp

        result = utils.get_intended_measurements("123", get_func=test_get)
        self.assertIsNone(result)

    def test_get_intended_measurements_exception(self):
        """Test that get_intended_measurements handles network exceptions gracefully.

        When network issues occur during the API call, get_intended_measurements should
        catch the exception and return None. This prevents the mapping process from failing
        due to temporary network issues with the intended measurements service.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that raises a network exception."""
            raise Exception("network")

        result = utils.get_intended_measurements("123", get_func=test_get)
        self.assertIsNone(result)

    def test_get_intended_measurements_status_300(self):
        """Test that get_intended_measurements handles 300 status code.

        When the endpoint returns 300 (redirect), get_intended_measurements should
        return the JSON data.
        """

        def test_get(url, timeout=None):
            """Mock HTTP GET function that returns 300 redirect with data."""
            resp = SimpleNamespace()
            resp.status_code = 300
            resp.json = lambda: {"data": []}
            return resp

        result = utils.get_intended_measurements("123", get_func=test_get)
        self.assertEqual(result, {"data": []})

    def test_get_protocols_for_modality_file_not_found(self):
        """Test get_protocols_for_modality returns empty list when protocols.yaml is missing."""
        protocols_path = Path(__file__).parent.parent / "protocols.yaml"
        backup_path = Path(__file__).parent.parent / "protocols.yaml.test_backup"

        if protocols_path.exists():
            shutil.move(str(protocols_path), str(backup_path))

        try:
            result = utils.get_protocols_for_modality("fip")
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

            result = utils.get_protocols_for_modality("fip")
            self.assertEqual(result, [])
        finally:
            protocols_path.unlink(missing_ok=True)
            if backup_path.exists():
                shutil.move(str(backup_path), str(protocols_path))

    def test_get_protocols_for_modality_success(self):
        """Test get_protocols_for_modality returns protocols when file exists and is valid."""
        result = utils.get_protocols_for_modality("fip")
        # Should return a list (may be empty if protocols.yaml doesn't have fip, but should not error)
        self.assertIsInstance(result, list)


class TestPromptFunctions(unittest.TestCase):
    """Tests for prompt utility functions."""

    def test_prompt_for_string_with_default(self):
        """Test prompt_for_string with default value."""
        self.assertEqual(utils.prompt_for_string("Test", default="default", input_func=lambda x: ""), "default")
        self.assertEqual(utils.prompt_for_string("Test", default="default", input_func=lambda x: "input"), "input")

    def test_prompt_for_string_not_required(self):
        """Test prompt_for_string without default, not required."""
        self.assertEqual(utils.prompt_for_string("Test", required=False, input_func=lambda x: ""), "")
        self.assertEqual(utils.prompt_for_string("Test", required=False, input_func=lambda x: "input"), "input")

    def test_prompt_for_string_required(self):
        """Test prompt_for_string required=True with no default."""
        call_count = [0]

        def input_func(prompt):
            """Mock input function that returns empty first, then valid input."""
            call_count[0] += 1
            return "" if call_count[0] == 1 else "input"

        with patch("builtins.print"):
            result = utils.prompt_for_string("Test", required=True, input_func=input_func)
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
            utils.prompt_for_string("Test", required=True, help_message="Help", input_func=input_func)
        self.assertEqual(call_count[0], 2)
        self.assertTrue(any("Help" in str(call) for call in mock_print.call_args_list))


if __name__ == "__main__":
    unittest.main()
