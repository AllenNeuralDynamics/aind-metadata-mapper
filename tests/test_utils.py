"""Tests for utils module."""

import unittest
from unittest.mock import patch

from aind_metadata_mapper.utils import prompt_for_string, prompt_yes_no


class TestPromptFunctions(unittest.TestCase):
    """Tests for prompt utility functions."""

    def test_prompt_yes_no_default_yes(self):
        """Test prompt_yes_no with default=True."""
        # Empty input should return default (True)
        result = prompt_yes_no("Test prompt", default=True, input_func=lambda x: "")
        self.assertTrue(result)

        # "y" should return True
        result = prompt_yes_no("Test prompt", default=True, input_func=lambda x: "y")
        self.assertTrue(result)

        # "yes" should return True
        result = prompt_yes_no("Test prompt", default=True, input_func=lambda x: "yes")
        self.assertTrue(result)

        # "n" should return False
        result = prompt_yes_no("Test prompt", default=True, input_func=lambda x: "n")
        self.assertFalse(result)

    def test_prompt_yes_no_default_no(self):
        """Test prompt_yes_no with default=False."""
        # Empty input should return default (False)
        result = prompt_yes_no("Test prompt", default=False, input_func=lambda x: "")
        self.assertFalse(result)

        # "y" should return True
        result = prompt_yes_no("Test prompt", default=False, input_func=lambda x: "y")
        self.assertTrue(result)

        # "n" should return False
        result = prompt_yes_no("Test prompt", default=False, input_func=lambda x: "n")
        self.assertFalse(result)

    def test_prompt_for_string_with_default(self):
        """Test prompt_for_string with default value."""
        # Empty input should return default
        result = prompt_for_string("Test prompt", default="default_value", input_func=lambda x: "")
        self.assertEqual(result, "default_value")

        # Non-empty input should return input
        result = prompt_for_string("Test prompt", default="default_value", input_func=lambda x: "user_input")
        self.assertEqual(result, "user_input")

    def test_prompt_for_string_without_default_not_required(self):
        """Test prompt_for_string without default, not required."""
        # Empty input should return empty string
        result = prompt_for_string("Test prompt", required=False, input_func=lambda x: "")
        self.assertEqual(result, "")

        # Non-empty input should return input
        result = prompt_for_string("Test prompt", required=False, input_func=lambda x: "user_input")
        self.assertEqual(result, "user_input")

    def test_prompt_for_string_required_no_default(self):
        """Test prompt_for_string required=True with no default."""
        # First empty, then valid input
        call_count = [0]

        def input_func(prompt):
            """Mock input function that returns empty first, then valid input."""
            call_count[0] += 1
            if call_count[0] == 1:
                return ""  # First call: empty (should prompt again)
            return "valid_input"  # Second call: valid

        result = prompt_for_string("Test prompt", required=True, input_func=input_func)
        self.assertEqual(result, "valid_input")
        self.assertEqual(call_count[0], 2)  # Should have prompted twice

    def test_prompt_for_string_with_help_message(self):
        """Test prompt_for_string with help message."""
        call_count = [0]
        help_printed = [False]

        def input_func(prompt):
            """Mock input function that returns empty first, then valid input."""
            call_count[0] += 1
            if call_count[0] == 1:
                return ""  # First call: empty
            return "valid_input"

        def print_func(msg):
            """Mock print function that tracks if help message was printed."""
            if "help" in msg.lower() or "required" in msg.lower():
                help_printed[0] = True

        # Mock print to capture help message
        with patch("builtins.print", side_effect=print_func):
            result = prompt_for_string(
                "Test prompt",
                required=True,
                help_message="This is a help message",
                input_func=input_func,
            )
        self.assertEqual(result, "valid_input")
        self.assertTrue(help_printed[0])


if __name__ == "__main__":
    unittest.main()
