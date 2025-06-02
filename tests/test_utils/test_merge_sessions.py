"""Tests for session merging functionality."""

import pytest
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import unittest
import logging
from unittest.mock import patch, mock_open

from aind_metadata_mapper.utils.merge_sessions import merge_sessions


def test_basic_merge(caplog):
    """Test merging of basic fields."""
    file1_data = {
        "subject_id": "mouse1",
        "experimenter_full_name": ["John Doe"],
    }
    file2_data = {"subject_id": "mouse1", "rig_id": "rig1"}

    # Mock file operations
    def mock_open_side_effect(filename, mode="r"):
        """Mock file opening for different test files."""
        if "file1.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file1_data)).return_value
        elif "file2.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file2_data)).return_value
        elif "output.json" in str(filename):
            return mock_open().return_value
        return mock_open().return_value

    with (
        patch("builtins.open", side_effect=mock_open_side_effect),
        patch("builtins.input", return_value=""),  # Always take first value
    ):
        result = merge_sessions("file1.json", "file2.json", "output.json")

    assert result["subject_id"] == "mouse1"
    assert result["experimenter_full_name"] == ["John Doe"]
    assert result["rig_id"] == "rig1"


def test_merge_timestamps(caplog):
    """Test merging of timestamp fields."""
    # Set logging level to INFO to capture our messages
    caplog.set_level(logging.INFO)

    now = datetime.now(ZoneInfo("UTC"))
    earlier = (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    later = now.isoformat().replace("+00:00", "Z")

    file1_data = {"session_start_time": earlier, "session_end_time": earlier}
    file2_data = {"session_start_time": later, "session_end_time": later}

    # Mock file operations
    def mock_open_side_effect(filename, mode="r"):
        """Mock file opening for timestamp test files."""
        if "file1.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file1_data)).return_value
        elif "file2.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file2_data)).return_value
        elif "output.json" in str(filename):
            return mock_open().return_value
        return mock_open().return_value

    with patch("builtins.open", side_effect=mock_open_side_effect):
        result = merge_sessions("file1.json", "file2.json", "output.json")

    assert result["session_start_time"] == earlier  # Should take earlier time
    assert result["session_end_time"] == later  # Should take later time

    # Check for the presence of our log messages in a more flexible way
    log_text = (
        caplog.text.lower()
    )  # Convert to lowercase for case-insensitive matching
    assert "earlier timestamp" in log_text
    assert "later timestamp" in log_text


def test_merge_lists():
    """Test merging of lists with both simple types and dictionaries."""
    file1_data = {
        "data_streams": [
            {"name": "stream1", "type": "behavior"},
            {"name": "stream2", "type": "ephys"},
        ],
        "tags": ["tag1", "tag2"],
    }
    file2_data = {
        "data_streams": [{"name": "stream3", "type": "imaging"}],
        "tags": ["tag2", "tag3"],
    }

    # Mock file operations
    def mock_open_side_effect(filename, mode="r"):
        """Mock file opening for list merge test files."""
        if "file1.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file1_data)).return_value
        elif "file2.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file2_data)).return_value
        elif "output.json" in str(filename):
            return mock_open().return_value
        return mock_open().return_value

    with patch("builtins.open", side_effect=mock_open_side_effect):
        result = merge_sessions("file1.json", "file2.json", "output.json")

    assert (
        len(result["data_streams"]) == 3
    )  # Lists of dicts should concatenate
    assert len(result["tags"]) == 3  # Simple lists should deduplicate


def test_merge_with_none_values():
    """Test merging when one file has None values."""
    file1_data = {"subject_id": "mouse1", "reward_consumed_total": None}
    file2_data = {"subject_id": "mouse1", "reward_consumed_total": 0.5}

    # Mock file operations
    def mock_open_side_effect(filename, mode="r"):
        """Mock file opening for None value test files."""
        if "file1.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file1_data)).return_value
        elif "file2.json" in str(filename):
            if "w" in mode:
                return mock_open().return_value
            return mock_open(read_data=json.dumps(file2_data)).return_value
        elif "output.json" in str(filename):
            return mock_open().return_value
        return mock_open().return_value

    with patch("builtins.open", side_effect=mock_open_side_effect):
        result = merge_sessions("file1.json", "file2.json", "output.json")

    assert result["reward_consumed_total"] == 0.5


def test_file_errors():
    """Test error handling for file operations."""
    # Test with file that raises FileNotFoundError
    with patch(
        "builtins.open", side_effect=FileNotFoundError("File not found")
    ):
        with pytest.raises(ValueError, match="Error reading session files"):
            merge_sessions("non_existent.json", "file2.json", "output.json")

    # Test with invalid JSON
    with patch("builtins.open", mock_open(read_data="invalid json")):
        with pytest.raises(ValueError, match="Error reading session files"):
            merge_sessions("file1.json", "file2.json", "output.json")


if __name__ == "__main__":
    unittest.main()
