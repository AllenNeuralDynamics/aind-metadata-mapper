"""Tests for instrument_store module."""

import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import aind_metadata_mapper.instrument_store as instrument_store_module
from aind_metadata_mapper.instrument_store import InstrumentStore, get_instrument, list_instrument_ids, save_instrument


class TestInstrumentStore(unittest.TestCase):
    """Tests for InstrumentStore class."""

    def setUp(self):
        """Set up test fixtures with temporary directory."""
        self.tmpdir = tempfile.mkdtemp()
        self.tmp_path = Path(self.tmpdir)

    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_instrument_store_init_default(self):
        """Test InstrumentStore initialization with default path."""
        original_default_path = instrument_store_module.DEFAULT_INSTRUMENT_STORE_PATH
        try:
            instrument_store_module.DEFAULT_INSTRUMENT_STORE_PATH = str(self.tmp_path)
            store = InstrumentStore()
            self.assertEqual(store.base_path, Path(self.tmp_path))
            self.assertTrue(store.base_path.exists())
        finally:
            instrument_store_module.DEFAULT_INSTRUMENT_STORE_PATH = original_default_path

    def test_instrument_store_init_custom_path(self):
        """Test InstrumentStore initialization with custom path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = InstrumentStore(base_path=tmpdir)
            self.assertEqual(store.base_path, Path(tmpdir))
            self.assertTrue(store.base_path.exists())

    def test_save_instrument_new(self):
        """Test saving a new instrument in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))

        # Create a test instrument file
        instrument_file = self.tmp_path / "test_instrument.json"
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
        self.assertTrue(saved_path.exists())
        self.assertEqual(saved_path, self.tmp_path / "test_rig" / "instrument.json")

        # Verify content
        with open(saved_path, "r", encoding="utf-8") as f:
            saved_data = json.load(f)
        self.assertEqual(saved_data["instrument_id"], "test_rig")

    def test_save_instrument_archives_existing(self):
        """Test that saving archives existing instrument in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
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
        new_instrument_file = self.tmp_path / "new_instrument.json"
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
        self.assertEqual(len(archive_files), 1)
        self.assertEqual(archive_files[0].name, "instrument_20250115.json")

        # Verify current file was updated
        with open(existing_file, "r", encoding="utf-8") as f:
            current_data = json.load(f)
        self.assertEqual(current_data["modification_date"], "2025-01-16")

    def test_save_instrument_file_not_found(self):
        """Test that save_instrument raises FileNotFoundError for missing file in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))

        with self.assertRaises(FileNotFoundError):
            store.save_instrument("nonexistent.json", "test_rig")

    def test_save_instrument_invalid_json(self):
        """Test that save_instrument raises JSONDecodeError for invalid JSON in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))

        invalid_file = self.tmp_path / "invalid.json"
        with open(invalid_file, "w", encoding="utf-8") as f:
            f.write("not valid json")

        with self.assertRaises(json.JSONDecodeError):
            store.save_instrument(str(invalid_file), "test_rig")

    def test_get_instrument_exists(self):
        """Test getting an existing instrument from a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
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
        self.assertIsNotNone(result)
        self.assertEqual(result["instrument_id"], "test_rig")

    def test_get_instrument_not_exists(self):
        """Test getting a non-existent instrument from a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store.get_instrument("nonexistent")
        self.assertIsNone(result)

    def test_get_instrument_path_exists(self):
        """Test getting path to existing instrument in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
        rig_dir.mkdir()

        instrument_file = rig_dir / "instrument.json"
        instrument_file.touch()

        result = store.get_instrument_path("test_rig")
        self.assertEqual(result, instrument_file)

    def test_get_instrument_path_not_exists(self):
        """Test getting path to non-existent instrument in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store.get_instrument_path("nonexistent")
        self.assertIsNone(result)

    def test_list_versions(self):
        """Test listing archived versions in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
        rig_dir.mkdir()

        # Create archived files
        archive1 = rig_dir / "instrument_20250115.json"
        archive2 = rig_dir / "instrument_20250116.json"
        archive1.touch()
        archive2.touch()

        versions = store.list_versions("test_rig")
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[0].name, "instrument_20250115.json")
        self.assertEqual(versions[1].name, "instrument_20250116.json")

    def test_list_versions_empty(self):
        """Test listing versions when none exist in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        versions = store.list_versions("nonexistent")
        self.assertEqual(versions, [])

    def test_list_instrument_ids(self):
        """Test listing instrument IDs in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))

        # Create multiple rig directories with instrument.json
        for rig_id in ["rig1", "rig2", "rig3"]:
            rig_dir = self.tmp_path / rig_id
            rig_dir.mkdir()
            (rig_dir / "instrument.json").touch()

        # Create a directory without instrument.json (should be ignored)
        (self.tmp_path / "empty_dir").mkdir()

        ids = store.list_instrument_ids()
        self.assertEqual(len(ids), 3)
        self.assertIn("rig1", ids)
        self.assertIn("rig2", ids)
        self.assertIn("rig3", ids)
        self.assertEqual(ids, sorted(ids))

    def test_list_instrument_ids_empty(self):
        """Test listing instrument IDs when store is empty in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        ids = store.list_instrument_ids()
        self.assertEqual(ids, [])

    def test_list_instrument_ids_nonexistent_base(self):
        """Test listing instrument IDs when base path doesn't exist in a temporary path."""
        nonexistent_path = self.tmp_path / "nonexistent"
        store = InstrumentStore(base_path=str(nonexistent_path))
        # Delete the directory to test the case where base_path doesn't exist
        if nonexistent_path.exists():
            shutil.rmtree(nonexistent_path)
        ids = store.list_instrument_ids()
        self.assertEqual(ids, [])

    def test_get_archive_path_from_modification_date(self):
        """Test archive path generation from modification_date in JSON in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
        rig_dir.mkdir()

        current_file = rig_dir / "instrument.json"
        instrument_data = {
            "instrument_id": "test_rig",
            "modification_date": "2025-01-15",
        }
        with open(current_file, "w", encoding="utf-8") as f:
            json.dump(instrument_data, f)

        archive_path = store._get_archive_path(rig_dir, current_file)
        self.assertEqual(archive_path.name, "instrument_20250115.json")

    def test_get_archive_path_from_mtime(self):
        """Test archive path generation from file mtime when no modification_date in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
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
        self.assertEqual(archive_path.name, "instrument_20250115.json")

    def test_parse_modification_date_date_only(self):
        """Test parsing date-only modification_date."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store._parse_modification_date("2025-01-15")
        self.assertEqual(result, datetime(2025, 1, 15, 0, 0, 0))

    def test_parse_modification_date_datetime(self):
        """Test parsing datetime modification_date."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store._parse_modification_date("2025-01-15T14:30:00")
        self.assertEqual(result, datetime(2025, 1, 15, 14, 30, 0))

    def test_parse_modification_date_datetime_z(self):
        """Test parsing datetime with Z timezone."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store._parse_modification_date("2025-01-15T14:30:00Z")
        self.assertEqual(result, datetime(2025, 1, 15, 14, 30, 0))

    def test_parse_modification_date_invalid(self):
        """Test parsing invalid modification_date."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store._parse_modification_date("invalid")
        self.assertIsNone(result)

    def test_parse_modification_date_none(self):
        """Test parsing None modification_date."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store._parse_modification_date(None)
        self.assertIsNone(result)

    def test_save_instrument_convenience_function(self):
        """Test save_instrument convenience function in a temporary path."""
        instrument_file = self.tmp_path / "test_instrument.json"
        instrument_data = {"instrument_id": "test_rig", "modification_date": "2025-01-15"}
        with open(instrument_file, "w", encoding="utf-8") as f:
            json.dump(instrument_data, f)

        saved_path = save_instrument(str(instrument_file), "test_rig", base_path=str(self.tmp_path))
        self.assertTrue(saved_path.exists())

    def test_get_instrument_convenience_function(self):
        """Test get_instrument convenience function in a temporary path."""
        rig_dir = self.tmp_path / "test_rig"
        rig_dir.mkdir()
        instrument_file = rig_dir / "instrument.json"
        instrument_data = {"instrument_id": "test_rig", "modification_date": "2025-01-15"}
        with open(instrument_file, "w", encoding="utf-8") as f:
            json.dump(instrument_data, f)

        result = get_instrument("test_rig", base_path=str(self.tmp_path))
        self.assertIsNotNone(result)
        self.assertEqual(result["instrument_id"], "test_rig")

    def test_list_instrument_ids_convenience_function(self):
        """Test list_instrument_ids convenience function in a temporary path."""
        for rig_id in ["rig1", "rig2"]:
            rig_dir = self.tmp_path / rig_id
            rig_dir.mkdir()
            (rig_dir / "instrument.json").touch()

        ids = list_instrument_ids(base_path=str(self.tmp_path))
        self.assertEqual(len(ids), 2)
        self.assertIn("rig1", ids)
        self.assertIn("rig2", ids)

    def test_get_archive_path_invalid_json(self):
        """Test archive path generation with invalid JSON falls back to mtime in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
        rig_dir.mkdir()

        current_file = rig_dir / "instrument.json"
        with open(current_file, "w", encoding="utf-8") as f:
            f.write("invalid json")

        # Should fall back to mtime
        archive_path = store._get_archive_path(rig_dir, current_file)
        self.assertTrue(archive_path.name.startswith("instrument_"))
        self.assertTrue(archive_path.name.endswith(".json"))

    def test_get_archive_path_no_modification_date(self):
        """Test archive path generation when modification_date is missing in a temporary path."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        rig_dir = self.tmp_path / "test_rig"
        rig_dir.mkdir()

        current_file = rig_dir / "instrument.json"
        instrument_data = {"instrument_id": "test_rig"}  # No modification_date
        with open(current_file, "w", encoding="utf-8") as f:
            json.dump(instrument_data, f)

        # Should fall back to mtime
        archive_path = store._get_archive_path(rig_dir, current_file)
        self.assertTrue(archive_path.name.startswith("instrument_"))
        self.assertTrue(archive_path.name.endswith(".json"))

    def test_parse_modification_date_with_timezone(self):
        """Test parsing datetime with timezone offset."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store._parse_modification_date("2025-01-15T14:30:00+00:00")
        self.assertIsNotNone(result)
        self.assertEqual(result.year, 2025)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)

    def test_parse_modification_date_with_microseconds(self):
        """Test parsing datetime with microseconds."""
        store = InstrumentStore(base_path=str(self.tmp_path))
        result = store._parse_modification_date("2025-01-15T14:30:00.123456")
        self.assertIsNotNone(result)
        self.assertEqual(result.microsecond, 123456)


if __name__ == "__main__":
    unittest.main()
