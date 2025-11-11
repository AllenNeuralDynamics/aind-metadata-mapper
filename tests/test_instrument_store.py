"""Tests for instrument_store module."""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

import aind_metadata_mapper.instrument_store as instrument_store_module
from aind_metadata_mapper.instrument_store import (
    DEFAULT_INSTRUMENT_STORE_PATH,
    InstrumentStore,
    get_instrument,
    initialize_store,
    list_instrument_ids,
    save_instrument,
)


def test_instrument_store_init_default():
    """Test InstrumentStore initialization with default path."""
    store = InstrumentStore()
    assert store.base_path == Path(DEFAULT_INSTRUMENT_STORE_PATH)


def test_instrument_store_init_custom_path():
    """Test InstrumentStore initialization with custom path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = InstrumentStore(base_path=tmpdir)
        assert store.base_path == Path(tmpdir)
        assert store.base_path.exists()


def test_save_instrument_new(tmp_path):
    """Test saving a new instrument in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))

    # Create a test instrument file
    instrument_file = tmp_path / "test_instrument.json"
    instrument_data = {
        "instrument_id": "test_rig",
        "modification_date": "2025-01-15",
        "location": "428",
    }
    with open(instrument_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    # Save instrument
    saved_path = store.save_instrument(str(instrument_file), "test_rig")

    # Verify it was saved
    assert saved_path.exists()
    assert saved_path == tmp_path / "test_rig" / "instrument.json"

    # Verify content
    with open(saved_path, "r", encoding="utf-8") as f:
        saved_data = json.load(f)
    assert saved_data["instrument_id"] == "test_rig"


def test_save_instrument_archives_existing(tmp_path):
    """Test that saving archives existing instrument in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    # Create existing instrument.json
    existing_file = rig_dir / "instrument.json"
    existing_data = {
        "instrument_id": "test_rig",
        "modification_date": "2025-01-15",
        "location": "428",
    }
    with open(existing_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f)

    # Create new instrument file
    new_instrument_file = tmp_path / "new_instrument.json"
    new_data = {
        "instrument_id": "test_rig",
        "modification_date": "2025-01-16",
        "location": "428",
    }
    with open(new_instrument_file, "w", encoding="utf-8") as f:
        json.dump(new_data, f)

    # Save new instrument
    store.save_instrument(str(new_instrument_file), "test_rig")

    # Verify archive was created
    archive_files = list(rig_dir.glob("instrument_*.json"))
    assert len(archive_files) == 1
    assert archive_files[0].name == "instrument_20250115.json"

    # Verify current file was updated
    with open(existing_file, "r", encoding="utf-8") as f:
        current_data = json.load(f)
    assert current_data["modification_date"] == "2025-01-16"


def test_save_instrument_file_not_found(tmp_path):
    """Test that save_instrument raises FileNotFoundError for missing file in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))

    with pytest.raises(FileNotFoundError):
        store.save_instrument("nonexistent.json", "test_rig")


def test_save_instrument_invalid_json(tmp_path):
    """Test that save_instrument raises JSONDecodeError for invalid JSON in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))

    invalid_file = tmp_path / "invalid.json"
    with open(invalid_file, "w", encoding="utf-8") as f:
        f.write("not valid json")

    with pytest.raises(json.JSONDecodeError):
        store.save_instrument(str(invalid_file), "test_rig")


def test_get_instrument_exists(tmp_path):
    """Test getting an existing instrument from a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    instrument_file = rig_dir / "instrument.json"
    instrument_data = {
        "instrument_id": "test_rig",
        "modification_date": "2025-01-15",
        "location": "428",
    }
    with open(instrument_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    result = store.get_instrument("test_rig")
    assert result is not None
    assert result["instrument_id"] == "test_rig"


def test_get_instrument_not_exists(tmp_path):
    """Test getting a non-existent instrument from a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    result = store.get_instrument("nonexistent")
    assert result is None


def test_get_instrument_path_exists(tmp_path):
    """Test getting path to existing instrument in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    instrument_file = rig_dir / "instrument.json"
    instrument_file.touch()

    result = store.get_instrument_path("test_rig")
    assert result == instrument_file


def test_get_instrument_path_not_exists(tmp_path):
    """Test getting path to non-existent instrument in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    result = store.get_instrument_path("nonexistent")
    assert result is None


def test_list_versions(tmp_path):
    """Test listing archived versions in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    # Create archived files
    archive1 = rig_dir / "instrument_20250115.json"
    archive2 = rig_dir / "instrument_20250116.json"
    archive1.touch()
    archive2.touch()

    versions = store.list_versions("test_rig")
    assert len(versions) == 2
    assert versions[0].name == "instrument_20250115.json"
    assert versions[1].name == "instrument_20250116.json"


def test_list_versions_empty(tmp_path):
    """Test listing versions when none exist in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    versions = store.list_versions("nonexistent")
    assert versions == []


def test_list_instrument_ids(tmp_path):
    """Test listing instrument IDs in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))

    # Create multiple rig directories with instrument.json
    for rig_id in ["rig1", "rig2", "rig3"]:
        rig_dir = tmp_path / rig_id
        rig_dir.mkdir()
        (rig_dir / "instrument.json").touch()

    # Create a directory without instrument.json (should be ignored)
    (tmp_path / "empty_dir").mkdir()

    ids = store.list_instrument_ids()
    assert len(ids) == 3
    assert "rig1" in ids
    assert "rig2" in ids
    assert "rig3" in ids
    assert ids == sorted(ids)


def test_list_instrument_ids_empty(tmp_path):
    """Test listing instrument IDs when store is empty in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    ids = store.list_instrument_ids()
    assert ids == []


def test_list_instrument_ids_nonexistent_base(tmp_path):
    """Test listing instrument IDs when base path doesn't exist in a temporary path."""
    nonexistent_path = tmp_path / "nonexistent"
    store = InstrumentStore(base_path=str(nonexistent_path))
    # Delete the directory to test the case where base_path doesn't exist
    import shutil

    if nonexistent_path.exists():
        shutil.rmtree(nonexistent_path)
    ids = store.list_instrument_ids()
    assert ids == []


def test_get_archive_path_from_modification_date(tmp_path):
    """Test archive path generation from modification_date in JSON in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    current_file = rig_dir / "instrument.json"
    instrument_data = {
        "instrument_id": "test_rig",
        "modification_date": "2025-01-15",
    }
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    archive_path = store._get_archive_path(rig_dir, current_file)
    assert archive_path.name == "instrument_20250115.json"


def test_get_archive_path_from_mtime(tmp_path):
    """Test archive path generation from file mtime when no modification_date in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    current_file = rig_dir / "instrument.json"
    instrument_data = {"instrument_id": "test_rig"}
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    # Set a known mtime
    test_date = datetime(2025, 1, 15, 12, 30, 0)
    mtime = test_date.timestamp()
    current_file.touch()
    os.utime(current_file, (mtime, mtime))

    archive_path = store._get_archive_path(rig_dir, current_file)
    assert archive_path.name == "instrument_20250115.json"


def test_parse_modification_date_date_only():
    """Test parsing date-only modification_date."""
    store = InstrumentStore()
    result = store._parse_modification_date("2025-01-15")
    assert result == datetime(2025, 1, 15, 0, 0, 0)


def test_parse_modification_date_datetime():
    """Test parsing datetime modification_date."""
    store = InstrumentStore()
    result = store._parse_modification_date("2025-01-15T14:30:00")
    assert result == datetime(2025, 1, 15, 14, 30, 0)


def test_parse_modification_date_datetime_z():
    """Test parsing datetime with Z timezone."""
    store = InstrumentStore()
    result = store._parse_modification_date("2025-01-15T14:30:00Z")
    assert result == datetime(2025, 1, 15, 14, 30, 0)


def test_parse_modification_date_invalid():
    """Test parsing invalid modification_date."""
    store = InstrumentStore()
    result = store._parse_modification_date("invalid")
    assert result is None


def test_parse_modification_date_none():
    """Test parsing None modification_date."""
    store = InstrumentStore()
    result = store._parse_modification_date(None)
    assert result is None


def test_save_instrument_convenience_function(tmp_path):
    """Test save_instrument convenience function in a temporary path."""
    # Reset the global store to ensure we use the provided base_path
    instrument_store_module._default_store = None

    instrument_file = tmp_path / "test_instrument.json"
    instrument_data = {"instrument_id": "test_rig", "modification_date": "2025-01-15"}
    with open(instrument_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    saved_path = save_instrument(str(instrument_file), "test_rig", base_path=str(tmp_path))
    assert saved_path.exists()


def test_get_instrument_convenience_function(tmp_path):
    """Test get_instrument convenience function in a temporary path."""
    # Reset the global store to ensure we use the provided base_path
    instrument_store_module._default_store = None

    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()
    instrument_file = rig_dir / "instrument.json"
    instrument_data = {"instrument_id": "test_rig", "modification_date": "2025-01-15"}
    with open(instrument_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    result = get_instrument("test_rig", base_path=str(tmp_path))
    assert result is not None
    assert result["instrument_id"] == "test_rig"


def test_list_instrument_ids_convenience_function(tmp_path):
    """Test list_instrument_ids convenience function in a temporary path."""
    # Reset the global store to ensure we use the provided base_path
    instrument_store_module._default_store = None

    for rig_id in ["rig1", "rig2"]:
        rig_dir = tmp_path / rig_id
        rig_dir.mkdir()
        (rig_dir / "instrument.json").touch()

    ids = list_instrument_ids(base_path=str(tmp_path))
    assert len(ids) == 2
    assert "rig1" in ids
    assert "rig2" in ids


def test_initialize_store():
    """Test initialize_store function in a temporary path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = initialize_store(base_path=tmpdir)
        assert isinstance(store, InstrumentStore)
        assert store.base_path == Path(tmpdir)


def test_get_archive_path_invalid_json(tmp_path):
    """Test archive path generation with invalid JSON falls back to mtime in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    current_file = rig_dir / "instrument.json"
    with open(current_file, "w", encoding="utf-8") as f:
        f.write("invalid json")

    # Should fall back to mtime
    archive_path = store._get_archive_path(rig_dir, current_file)
    assert archive_path.name.startswith("instrument_")
    assert archive_path.name.endswith(".json")


def test_get_archive_path_no_modification_date(tmp_path):
    """Test archive path generation when modification_date is missing in a temporary path."""
    store = InstrumentStore(base_path=str(tmp_path))
    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()

    current_file = rig_dir / "instrument.json"
    instrument_data = {"instrument_id": "test_rig"}  # No modification_date
    with open(current_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    # Should fall back to mtime
    archive_path = store._get_archive_path(rig_dir, current_file)
    assert archive_path.name.startswith("instrument_")
    assert archive_path.name.endswith(".json")


def test_parse_modification_date_with_timezone():
    """Test parsing datetime with timezone offset."""
    store = InstrumentStore()
    result = store._parse_modification_date("2025-01-15T14:30:00+00:00")
    assert result is not None
    assert result.year == 2025
    assert result.month == 1
    assert result.day == 15


def test_parse_modification_date_with_microseconds():
    """Test parsing datetime with microseconds."""
    store = InstrumentStore()
    result = store._parse_modification_date("2025-01-15T14:30:00.123456")
    assert result is not None
    assert result.microsecond == 123456


def test_save_instrument_convenience_function_initializes_store(tmp_path):
    """Test that save_instrument convenience function initializes store when None in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    instrument_file = tmp_path / "test_instrument.json"
    instrument_data = {"instrument_id": "test_rig", "modification_date": "2025-01-15"}
    with open(instrument_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    saved_path = save_instrument(str(instrument_file), "test_rig", base_path=str(tmp_path))
    assert saved_path.exists()
    # Verify store was initialized
    assert instrument_store_module._default_store is not None


def test_get_instrument_convenience_function_initializes_store(tmp_path):
    """Test that get_instrument convenience function initializes store when None in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    rig_dir = tmp_path / "test_rig"
    rig_dir.mkdir()
    instrument_file = rig_dir / "instrument.json"
    instrument_data = {"instrument_id": "test_rig", "modification_date": "2025-01-15"}
    with open(instrument_file, "w", encoding="utf-8") as f:
        json.dump(instrument_data, f)

    result = get_instrument("test_rig", base_path=str(tmp_path))
    assert result is not None
    # Verify store was initialized
    assert instrument_store_module._default_store is not None


def test_list_instrument_ids_convenience_function_initializes_store(tmp_path):
    """Test that list_instrument_ids convenience function initializes store when None in a temporary path."""
    # Reset the global store
    instrument_store_module._default_store = None

    for rig_id in ["rig1"]:
        rig_dir = tmp_path / rig_id
        rig_dir.mkdir()
        (rig_dir / "instrument.json").touch()

    ids = list_instrument_ids(base_path=str(tmp_path))
    assert len(ids) == 1
    # Verify store was initialized
    assert instrument_store_module._default_store is not None
