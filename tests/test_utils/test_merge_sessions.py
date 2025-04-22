"""Tests for session merging functionality."""

import pytest
from pathlib import Path
import json
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import unittest
import logging

from aind_metadata_mapper.utils.merge_sessions import (
    merge_sessions,
    _merge_timestamps,
)


@pytest.fixture
def temp_json_file():
    """Create a temporary JSON file and return its path."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        yield Path(f.name)
        Path(f.name).unlink()  # Cleanup after test


def create_test_file(data: dict, file_path: Path):
    """Helper to create a test JSON file with given data."""
    with open(file_path, "w") as f:
        json.dump(data, f)


def test_basic_merge(temp_json_file, monkeypatch, caplog):
    """Test merging of basic fields."""
    # Mock user input to always take first value
    monkeypatch.setattr("builtins.input", lambda _: "")

    file1_data = {
        "subject_id": "mouse1",
        "experimenter_full_name": ["John Doe"],
    }
    file2_data = {"subject_id": "mouse1", "rig_id": "rig1"}

    file1 = temp_json_file
    file2 = Path(str(temp_json_file).replace(".json", "_2.json"))
    output = Path(str(temp_json_file).replace(".json", "_merged.json"))

    create_test_file(file1_data, file1)
    create_test_file(file2_data, file2)

    result = merge_sessions(file1, file2, output)

    assert result["subject_id"] == "mouse1"
    assert result["experimenter_full_name"] == ["John Doe"]
    assert result["rig_id"] == "rig1"


def test_merge_timestamps(temp_json_file, caplog):
    """Test merging of timestamp fields."""
    # Set logging level to INFO to capture our messages
    caplog.set_level(logging.INFO)

    now = datetime.now(ZoneInfo("UTC"))
    earlier = (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    later = now.isoformat().replace("+00:00", "Z")

    file1_data = {"session_start_time": earlier, "session_end_time": earlier}
    file2_data = {"session_start_time": later, "session_end_time": later}

    file1 = temp_json_file
    file2 = Path(str(temp_json_file).replace(".json", "_2.json"))
    output = Path(str(temp_json_file).replace(".json", "_merged.json"))

    create_test_file(file1_data, file1)
    create_test_file(file2_data, file2)

    result = merge_sessions(file1, file2, output)

    assert result["session_start_time"] == earlier  # Should take earlier time
    assert result["session_end_time"] == later  # Should take later time

    # Check for the presence of our log messages in a more flexible way
    log_text = (
        caplog.text.lower()
    )  # Convert to lowercase for case-insensitive matching
    assert "earlier timestamp" in log_text
    assert "later timestamp" in log_text


def test_merge_lists(temp_json_file):
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

    file1 = temp_json_file
    file2 = Path(str(temp_json_file).replace(".json", "_2.json"))
    output = Path(str(temp_json_file).replace(".json", "_merged.json"))

    create_test_file(file1_data, file1)
    create_test_file(file2_data, file2)

    result = merge_sessions(file1, file2, output)

    assert (
        len(result["data_streams"]) == 3
    )  # Lists of dicts should concatenate
    assert len(result["tags"]) == 3  # Simple lists should deduplicate


def test_merge_with_none_values(temp_json_file):
    """Test merging when one file has None values."""
    file1_data = {"subject_id": "mouse1", "reward_consumed_total": None}
    file2_data = {"subject_id": "mouse1", "reward_consumed_total": 0.5}

    file1 = temp_json_file
    file2 = Path(str(temp_json_file).replace(".json", "_2.json"))
    output = Path(str(temp_json_file).replace(".json", "_merged.json"))

    create_test_file(file1_data, file1)
    create_test_file(file2_data, file2)

    result = merge_sessions(file1, file2, output)

    assert result["reward_consumed_total"] == 0.5


def test_merge_timestamp_tolerance():
    """Test timestamp merging tolerance."""
    now = datetime.now(ZoneInfo("UTC"))
    time1 = now.isoformat().replace("+00:00", "Z")
    time2 = (now + timedelta(hours=2)).isoformat().replace("+00:00", "Z")

    # Should raise error when difference exceeds default 1-hour tolerance
    with pytest.raises(ValueError, match="exceeding tolerance"):
        _merge_timestamps("session_start_time", time1, time2)

    # Should succeed with custom tolerance
    result = _merge_timestamps(
        "session_start_time", time1, time2, tolerance_hours=3
    )
    assert result == time1  # Should take earlier time for start times


def test_file_errors(temp_json_file):
    """Test error handling for file operations."""
    non_existent_file = Path("non_existent.json")

    with pytest.raises(ValueError, match="Error reading session files"):
        merge_sessions(non_existent_file, temp_json_file, "output.json")

    # Test with invalid JSON
    with open(temp_json_file, "w") as f:
        f.write("invalid json")

    with pytest.raises(ValueError, match="Error reading session files"):
        merge_sessions(temp_json_file, temp_json_file, "output.json")


if __name__ == "__main__":
    unittest.main()
