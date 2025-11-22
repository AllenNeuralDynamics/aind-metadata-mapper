"""Tests for utils module."""

import unittest
from unittest.mock import patch, MagicMock

from aind_metadata_mapper.utils import get_instrument, prompt_for_string


class TestGetInstrument(unittest.TestCase):
    """Tests for get_instrument function."""

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_instrument_not_found(self, mock_get):
        """Test get_instrument returns None when instrument not found."""
        mock_get.return_value = MagicMock(status_code=404)
        result = get_instrument("nonexistent_id")
        self.assertIsNone(result)

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_instrument_returns_latest(self, mock_get):
        """Test get_instrument returns latest record."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"instrument_id": "test_id", "modification_date": "2024-01-01"},
            {"instrument_id": "test_id", "modification_date": "2024-01-03"},
            {"instrument_id": "test_id", "modification_date": "2024-01-02"},
        ]
        mock_get.return_value = mock_response
        result = get_instrument("test_id")
        self.assertEqual(result["modification_date"], "2024-01-03")

    @patch("aind_metadata_mapper.utils.requests.get")
    def test_get_instrument_by_date(self, mock_get):
        """Test get_instrument returns specific date if provided."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"instrument_id": "test_id", "modification_date": "2024-01-01"},
            {"instrument_id": "test_id", "modification_date": "2024-01-02"},
        ]
        mock_get.return_value = mock_response
        result = get_instrument("test_id", modification_date="2024-01-01")
        self.assertEqual(result["modification_date"], "2024-01-01")


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
