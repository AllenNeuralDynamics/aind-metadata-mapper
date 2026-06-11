"""Tests for utility functions.

Strategy:
- Use dependency injection to pass test HTTP functions instead of mocking.
- File IO is exercised in isolation using tempfile.TemporaryDirectory(); where cwd matters, tests temporarily chdir.
- Focus on error handling and return contracts (None vs. dict), not on external service availability.
- Keep tests fast by avoiding real network or filesystem side effects outside temp dirs.
"""

import json
import shutil
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

import aind_data_schema.core.instrument as instrument
from aind_metadata_mapper.utils import (
    check_existing_instrument,
    check_instrument_id,
    ensure_timezone,
    get_iacuc_protocol,
    get_instrument,
    get_intended_measurements,
    get_procedures,
    get_protocols_for_modality,
    get_subject,
    metadata_service_helper,
    normalize_utc_timezone,
    prompt_for_string,
    save_instrument,
)

# Path to instrument fixture used by get_instrument/save_instrument tests.
INSTRUMENT_JSON = Path(__file__).parent / "resources" / "v2_metadata" / "instrument.json"


class TestGetInstrument(unittest.TestCase):
    """Tests for get_instrument function."""

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_instrument_not_found(self, mock_get):
        """Test get_instrument returns None when instrument not found."""
        mock_get.return_value = MagicMock(status_code=404)
        result = get_instrument("nonexistent_id")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_instrument_returns_latest(self, mock_helper):
        """Test get_instrument returns latest record as Instrument."""
        with open(INSTRUMENT_JSON) as f:
            data = json.load(f)
        data["modification_date"] = "2024-01-03"
        mock_helper.return_value = [data]
        result = get_instrument("422_MESO2_20241017")
        self.assertIsNotNone(result)
        self.assertEqual(str(result.modification_date), "2024-01-03")

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_instrument_by_date(self, mock_helper):
        """Test get_instrument returns specific date if provided."""
        with open(INSTRUMENT_JSON) as f:
            data = json.load(f)
        records = [
            {**data, "modification_date": "2024-01-01"},
            {**data, "modification_date": "2024-01-02"},
        ]
        mock_helper.return_value = records
        result = get_instrument("422_MESO2_20241017", modification_date="2024-01-01")
        self.assertIsNotNone(result)
        self.assertEqual(str(result.modification_date), "2024-01-01")

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_instrument_writes_to_file_when_output_directory_given(self, mock_helper):
        """get_instrument saves to file when output_directory is provided."""
        with open(INSTRUMENT_JSON) as f:
            mock_helper.return_value = [json.load(f)]
        with tempfile.TemporaryDirectory() as tmpdir:
            result = get_instrument("422_MESO2_20241017", output_directory=tmpdir)
            self.assertIsNotNone(result)
            self.assertTrue((Path(tmpdir) / "instrument.json").exists())

    @patch("aind_metadata_mapper.utils.get_instrument")
    @patch("aind_metadata_mapper.utils.requests.post")
    def test_save_instrument_loads_from_filepath(self, mock_post, mock_get):
        """save_instrument loads from file when given a path."""
        with open(INSTRUMENT_JSON) as f:
            instrument_data = json.load(f)
        instrument_data["modification_date"] = date.today().isoformat()
        mock_get.return_value = instrument.Instrument.model_validate(instrument_data)
        mock_post.return_value = MagicMock(status_code=201)
        save_instrument(str(INSTRUMENT_JSON))
        mock_post.assert_called_once()

    @patch("aind_metadata_mapper.utils.get_instrument")
    @patch("aind_metadata_mapper.utils.requests.post")
    def test_save_instrument_updates_modification_date_by_default(self, mock_post, mock_get):
        """save_instrument sets modification_date to today when update_modification_date=True."""
        with open(INSTRUMENT_JSON) as f:
            instrument_data = json.load(f)
        instrument_data["modification_date"] = date.today().isoformat()
        mock_get.return_value = instrument.Instrument.model_validate(instrument_data)
        mock_post.return_value = MagicMock(status_code=201)
        save_instrument(str(INSTRUMENT_JSON))
        posted_json = mock_post.call_args[1]["json"]
        today = date.today().isoformat()
        self.assertEqual(posted_json["modification_date"], today)

    @patch("aind_metadata_mapper.utils.get_instrument")
    @patch("aind_metadata_mapper.utils.requests.post")
    def test_save_instrument_preserves_modification_date_when_false(self, mock_post, mock_get):
        """save_instrument keeps original modification_date when update_modification_date=False."""
        with open(INSTRUMENT_JSON) as f:
            instrument_data = json.load(f)
        original_date = instrument_data["modification_date"]
        mock_get.return_value = instrument.Instrument.model_validate(instrument_data)
        mock_post.return_value = MagicMock(status_code=201)
        save_instrument(str(INSTRUMENT_JSON), update_modification_date=False)
        posted_json = mock_post.call_args[1]["json"]
        self.assertEqual(posted_json["modification_date"], original_date)

    @patch("aind_metadata_mapper.utils.get_instrument")
    def test_check_existing_instrument(self, mock_get):
        """check_existing_instrument returns True when instrument exists."""
        with open(INSTRUMENT_JSON) as f:
            mock_get.return_value = instrument.Instrument.model_validate(json.load(f))
        self.assertTrue(check_existing_instrument("422_MESO2_20241017", "2024-10-28"))
        mock_get.return_value = None
        self.assertFalse(check_existing_instrument("test_instrument", "2024-01-01"))

    @patch("aind_metadata_mapper.utils.get_instrument")
    def test_check_instrument_id(self, mock_get):
        """check_instrument_id returns existing instrument or None."""
        with open(INSTRUMENT_JSON) as f:
            mock_get.return_value = instrument.Instrument.model_validate(json.load(f))
        result = check_instrument_id("422_MESO2_20241017", skip_confirmation=True)
        self.assertIsNotNone(result)
        self.assertEqual(result.instrument_id, "422_MESO2_20241017")
        mock_get.return_value = None
        self.assertIsNone(check_instrument_id("test_instrument_new", skip_confirmation=True))

    @patch("aind_metadata_mapper.utils.get_instrument")
    def test_check_instrument_id_exits_when_user_declines(self, mock_get):
        """check_instrument_id exits when user declines new instrument."""
        mock_get.return_value = None

        def user_declines(prompt):
            """Mock input that returns no to decline."""
            return "n"

        with self.assertRaises(SystemExit):
            check_instrument_id("test_instrument_new", skip_confirmation=False, input_func=user_declines)

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_instrument_returns_instrument_model(self, mock_helper):
        """get_instrument returns Instrument model."""
        with open(INSTRUMENT_JSON) as f:
            mock_helper.return_value = [json.load(f)]
        result = get_instrument("422_MESO2_20241017")
        self.assertIsNotNone(result)
        self.assertEqual(result.instrument_id, "422_MESO2_20241017")

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_instrument_returns_none_when_helper_fails(self, mock_helper):
        """get_instrument returns None when metadata_service_helper returns None."""
        mock_helper.return_value = None
        self.assertIsNone(get_instrument("test_instrument"))

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_instrument_warns_when_modification_date_not_found(self, mock_helper):
        """get_instrument logs warning when modification_date not in records."""
        mock_helper.return_value = [
            {"instrument_id": "test_instrument", "modification_date": "2024-01-01"},
        ]
        with self.assertLogs("aind_metadata_mapper.utils", level="WARNING"):
            result = get_instrument("test_instrument", modification_date="2024-01-02")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.get_instrument")
    @patch("aind_metadata_mapper.utils.requests.post")
    def test_save_instrument_error_paths(self, mock_post, mock_get):
        """save_instrument raises on 400, 500, not found, and round-trip failure."""
        with open(INSTRUMENT_JSON) as f:
            instrument_data = json.load(f)
        instrument_model = instrument.Instrument.model_validate(instrument_data)

        for status_code, get_return, expected_msg in [
            (400, instrument_model, "exists"),
            (201, None, "not found"),
        ]:
            with self.subTest(status_code=status_code, expected=expected_msg):
                mock_post.return_value = MagicMock(status_code=status_code, json=lambda: {"message": "exists"})
                mock_get.return_value = get_return
                with self.assertRaises(ValueError) as cm:
                    save_instrument(str(INSTRUMENT_JSON))
                self.assertIn(expected_msg, str(cm.exception))

        instrument_data["location"] = "different"
        mock_post.return_value = MagicMock(status_code=201)
        mock_get.return_value = instrument.Instrument.model_validate(instrument_data)
        with self.assertRaises(ValueError) as cm:
            save_instrument(str(INSTRUMENT_JSON))
        self.assertIn("Round-trip", str(cm.exception))

        mock_post.return_value = MagicMock(status_code=500)
        mock_post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError()
        with self.assertRaises(requests.exceptions.HTTPError):
            save_instrument(str(INSTRUMENT_JSON))

    @patch("aind_metadata_mapper.utils._write_instrument_to_path")
    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_instrument_returns_none_on_exception(self, mock_helper, mock_write):
        """get_instrument returns None when an exception occurs."""
        with open(INSTRUMENT_JSON) as f:
            mock_helper.return_value = [json.load(f)]
        mock_write.side_effect = ValueError("write failed")
        with tempfile.TemporaryDirectory() as tmpdir:
            result = get_instrument("422_MESO2_20241017", output_directory=tmpdir)
        self.assertIsNone(result)


class TestUtils(unittest.TestCase):
    """Test cases for utility functions in aind_metadata_mapper."""

    def test_replace_z_with_offset(self):
        """Test that a trailing 'Z' timezone shorthand is replaced with '+00:00'."""
        self.assertEqual(normalize_utc_timezone("2025-11-16T23:00:22Z"), "2025-11-16T23:00:22+00:00")

    def test_no_replacement(self):
        """Test that the original string is returned unchanged when 'old' is not found."""
        self.assertEqual(normalize_utc_timezone("2025-11-16T23:00:22"), "2025-11-16T23:00:22")

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

    @patch("aind_metadata_mapper.utils.time.sleep")
    @patch("requests.get")
    def test_metadata_service_helper_retry_on_exception(self, mock_get, mock_sleep):
        """Test that metadata_service_helper retries when exceptions occur.

        When requests raises an exception, metadata_service_helper should
        retry up to max_retries times before giving up.
        """
        from aind_metadata_mapper.utils import metadata_service_helper

        # First 2 calls raise exception, third succeeds
        mock_resp_success = SimpleNamespace()
        mock_resp_success.status_code = 200
        mock_resp_success.json = lambda: {"test": "data"}
        mock_resp_success.raise_for_status = lambda: None

        mock_get.side_effect = [
            requests.exceptions.RequestException("network error"),
            requests.exceptions.RequestException("network error"),
            mock_resp_success,
        ]

        result = metadata_service_helper("http://test.com")
        self.assertEqual(result, {"test": "data"})
        self.assertEqual(mock_get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("aind_metadata_mapper.utils.time.sleep")
    @patch("requests.get")
    def test_metadata_service_helper_max_retries_exceeded(self, mock_get, mock_sleep):
        """Test that metadata_service_helper gives up after max_retries.

        When the metadata service consistently fails, metadata_service_helper should
        retry max_retries times and then return None.
        """
        from aind_metadata_mapper.utils import metadata_service_helper

        mock_get.side_effect = requests.exceptions.RequestException("persistent error")

        result = metadata_service_helper("http://test.com", max_retries=3)
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("aind_metadata_mapper.utils.time.sleep")
    @patch("requests.get")
    def test_metadata_service_helper_custom_retry_delay(self, mock_get, mock_sleep):
        """Test that metadata_service_helper respects custom retry_delay.

        When a custom retry_delay is provided, metadata_service_helper should
        use that delay between retries.
        """
        from aind_metadata_mapper.utils import metadata_service_helper

        mock_get.side_effect = [
            requests.exceptions.RequestException("error"),
            requests.exceptions.RequestException("error"),
        ]

        result = metadata_service_helper("http://test.com", max_retries=2, retry_delay=0.5)
        self.assertIsNone(result)
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        mock_sleep.assert_called_with(0.5)

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

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_subject_returns_none_after_retries(self, mock_helper):
        """Test get_subject when metadata_service_helper returns None after retries."""
        mock_helper.return_value = None
        with self.assertLogs("aind_metadata_mapper.utils", level="WARNING") as log:
            result = get_subject("test_subject")
        self.assertIsNone(result)
        self.assertTrue(any("Could not fetch subject test_subject" in msg for msg in log.output))

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_get_intended_measurements_returns_none_after_retries(self, mock_helper):
        """Test get_intended_measurements when metadata_service_helper returns None after retries."""
        mock_helper.return_value = None
        with self.assertLogs("aind_metadata_mapper.utils", level="WARNING") as log:
            result = get_intended_measurements("test_subject")
        self.assertIsNone(result)
        self.assertTrue(
            any("Could not fetch intended measurements for subject test_subject" in msg for msg in log.output)
        )

    def test_metadata_service_helper_zero_retries(self):
        """Test metadata_service_helper with max_retries=0."""
        result = metadata_service_helper("http://test.com", max_retries=0)
        self.assertIsNone(result)


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


class TestGetIacucProtocol(unittest.TestCase):
    """Tests for get_iacuc_protocol function."""

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_returns_protocol_from_group_name(self, mock_helper):
        """Trailing dash-delimited number of the group name is returned."""
        mock_helper.return_value = [{"group_name": "Exp-ND-01-001-2414"}]
        self.assertEqual(get_iacuc_protocol("818908"), "2414")

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_drops_trailing_site_tag(self, mock_helper):
        """A trailing site tag (e.g. ' AIND') is ignored."""
        mock_helper.return_value = [{"group_name": "Exp-ND-01-020-2414 AIND"}]
        self.assertEqual(get_iacuc_protocol("x"), "2414")

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_non_numeric_group_returns_none(self, mock_helper):
        """A group with no protocol number (e.g. 'Practice Mice') yields None."""
        mock_helper.return_value = [{"group_name": "Practice Mice"}]
        self.assertIsNone(get_iacuc_protocol("x"))

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_missing_group_name_returns_none(self, mock_helper):
        """A record without a group_name yields None."""
        mock_helper.return_value = [{}]
        self.assertIsNone(get_iacuc_protocol("x"))

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_no_records_returns_none(self, mock_helper):
        """No records (helper returns None) yields None."""
        mock_helper.return_value = None
        self.assertIsNone(get_iacuc_protocol("x"))

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_dict_response_is_normalized(self, mock_helper):
        """A non-list (dict) response is normalized and handled gracefully."""
        mock_helper.return_value = {"detail": "Not Found"}
        self.assertIsNone(get_iacuc_protocol("x"))

    @patch("aind_metadata_mapper.utils.metadata_service_helper")
    def test_exception_returns_none(self, mock_helper):
        """An unexpected error is caught and yields None."""
        mock_helper.side_effect = ValueError("boom")
        self.assertIsNone(get_iacuc_protocol("x"))


if __name__ == "__main__":
    unittest.main()
