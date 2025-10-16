import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from aind_metadata_mapper import utils


class TestUtils(unittest.TestCase):
    def test_ensure_timezone_none(self):
        dt = utils.ensure_timezone(None)
        self.assertIsNotNone(dt.tzinfo)

    def test_ensure_timezone_naive_datetime(self):
        naive = datetime(2025, 1, 1, 12, 0, 0)
        dt = utils.ensure_timezone(naive)
        self.assertIsNotNone(dt.tzinfo)

    def test_ensure_timezone_iso_string(self):
        aware_iso = "2025-01-01T12:00:00+00:00"
        dt = utils.ensure_timezone(aware_iso)
        self.assertEqual(dt.tzinfo, timezone(timedelta(seconds=0)))

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_procedures_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"ok": True}
        mock_get.return_value = mock_resp
        result = utils.get_procedures("123")
        self.assertEqual(result, {"ok": True})

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_procedures_http_error(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("boom")
        mock_get.return_value = mock_resp
        result = utils.get_procedures("123")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_procedures_exception(self, mock_get):
        mock_get.side_effect = Exception("network")
        result = utils.get_procedures("123")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_intended_measurements_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": []}
        mock_get.return_value = mock_resp
        result = utils.get_intended_measurements("123")
        self.assertEqual(result, {"data": []})

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_intended_measurements_non_200(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp
        result = utils.get_intended_measurements("123")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_intended_measurements_exception(self, mock_get):
        mock_get.side_effect = Exception("network")
        result = utils.get_intended_measurements("123")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
