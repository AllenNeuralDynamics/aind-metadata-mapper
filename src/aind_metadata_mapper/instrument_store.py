"""Filesystem-based caching for instrument.json files."""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

# Default base path for instrument store
DEFAULT_INSTRUMENT_STORE_PATH = "/allen/aind/scratch/instrument_store"


class InstrumentStore:
    """Filesystem-based store for instrument.json files with versioning."""

    def __init__(self, base_path: Optional[str] = None) -> None:
        """Initialize the instrument store.

        Parameters
        ----------
        base_path : Optional[str]
            Base directory path for the store. Defaults to DEFAULT_INSTRUMENT_STORE_PATH.
        """
        if base_path is None:
            base_path = DEFAULT_INSTRUMENT_STORE_PATH
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_instrument(self, path: str, rig_id: str) -> Path:
        """Save instrument.json to store with versioning.

        If a current instrument.json exists, it will be archived first.
        Archive filename uses modification_date from JSON if available,
        otherwise falls back to file mtime.

        Parameters
        ----------
        path : str
            Path to the instrument.json file to save.
        rig_id : str
            Rig identifier (e.g., "323_EPHYS1_20231201").

        Returns
        -------
        Path
            Path to the saved instrument.json file in the store.

        Raises
        ------
        FileNotFoundError
            If the source file doesn't exist.
        json.JSONDecodeError
            If the file contains invalid JSON.
        """
        source_path = Path(path)
        if not source_path.exists():
            raise FileNotFoundError(f"Instrument file not found: {path}")

        # Validate JSON by loading it
        with open(source_path, "r", encoding="utf-8") as f:
            _ = json.load(f)

        # Get rig directory
        rig_dir = self.base_path / rig_id
        rig_dir.mkdir(parents=True, exist_ok=True)

        current_path = rig_dir / "instrument.json"

        # Archive existing instrument.json if it exists
        if current_path.exists():
            archive_path = self._get_archive_path(rig_dir, current_path)
            shutil.copy2(current_path, archive_path)

        # Copy new file as current instrument.json
        shutil.copy2(source_path, current_path)

        return current_path

    def get_instrument(self, rig_id: str) -> Optional[dict]:
        """Get current instrument data for a rig.

        Parameters
        ----------
        rig_id : str
            Rig identifier.

        Returns
        -------
        Optional[dict]
            Instrument data as dict, or None if not found.
        """
        path = self.get_instrument_path(rig_id)
        if path is None or not path.exists():
            return None

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_instrument_path(self, rig_id: str) -> Optional[Path]:
        """Get path to current instrument.json for a rig.

        Parameters
        ----------
        rig_id : str
            Rig identifier.

        Returns
        -------
        Optional[Path]
            Path to instrument.json, or None if rig directory doesn't exist.
        """
        rig_dir = self.base_path / rig_id
        current_path = rig_dir / "instrument.json"
        if current_path.exists():
            return current_path
        return None

    def list_versions(self, rig_id: str) -> list[Path]:
        """List all archived versions for a rig.

        Parameters
        ----------
        rig_id : str
            Rig identifier.

        Returns
        -------
        list[Path]
            List of paths to archived instrument.json files, sorted by name.
        """
        rig_dir = self.base_path / rig_id
        if not rig_dir.exists():
            return []

        # Find all archived files (instrument_*.json, excluding instrument.json)
        archived = [p for p in rig_dir.glob("instrument_*.json") if p.name != "instrument.json"]
        return sorted(archived)

    def list_instrument_ids(self) -> list[str]:
        """List all instrument IDs in the store.

        Returns
        -------
        list[str]
            List of instrument IDs (folder names), sorted alphabetically.
        """
        if not self.base_path.exists():
            return []

        # Get all directories in base_path (these are the instrument IDs)
        instrument_ids = [d.name for d in self.base_path.iterdir() if d.is_dir() and (d / "instrument.json").exists()]
        return sorted(instrument_ids)

    def _get_archive_path(self, rig_dir: Path, current_path: Path) -> Path:
        """Generate archive path for existing instrument.json.

        Uses modification_date from JSON if available, otherwise file mtime.
        Archive filename uses date only (YYYYMMDD) since schema only stores dates.

        Parameters
        ----------
        rig_dir : Path
            Directory for the rig.
        current_path : Path
            Path to current instrument.json.

        Returns
        -------
        Path
            Path for archived file.
        """
        # Try to get modification_date from JSON
        try:
            with open(current_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            mod_date_str = data.get("modification_date")
            if mod_date_str:
                timestamp = self._parse_modification_date(mod_date_str)
                if timestamp:
                    filename = f"instrument_{timestamp.strftime('%Y%m%d')}.json"
                    return rig_dir / filename
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

        # Fall back to file mtime (use date only)
        mtime = datetime.fromtimestamp(current_path.stat().st_mtime)
        filename = f"instrument_{mtime.strftime('%Y%m%d')}.json"
        return rig_dir / filename

    def _parse_modification_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse modification_date string to datetime.

        Handles various ISO formats:
        - "2025-11-07" (date only, primary format - uses 00:00:00)
        - "2025-11-07T14:30:00" (datetime format, for backwards compatibility)
        - "2025-11-07T14:30:00Z"
        - "2025-11-07T14:30:00+00:00"

        Parameters
        ----------
        date_str : Optional[str]
            Date string from JSON.

        Returns
        -------
        Optional[datetime]
            Parsed datetime, or None if parsing fails.
        """
        if date_str is None:
            return None

        # Try full ISO datetime formats
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # Try date-only format
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            pass

        return None


# Module-level convenience functions
_default_store: Optional[InstrumentStore] = None


def initialize_store(base_path: Optional[str] = None) -> InstrumentStore:
    """Initialize the default instrument store.

    Parameters
    ----------
    base_path : Optional[str]
        Base directory path for the store. Defaults to DEFAULT_INSTRUMENT_STORE_PATH.

    Returns
    -------
    InstrumentStore
        Initialized store instance.
    """
    global _default_store
    _default_store = InstrumentStore(base_path)
    return _default_store


def save_instrument(path: str, rig_id: str, base_path: Optional[str] = None) -> Path:
    """Save instrument.json to store (convenience function).

    Parameters
    ----------
    path : str
        Path to the instrument.json file to save.
    rig_id : str
        Rig identifier.
    base_path : Optional[str]
        Base directory path for the store. Only used if store not initialized.
        Defaults to /allen/aind/scratch/instrument_store.

    Returns
    -------
    Path
        Path to the saved instrument.json file in the store.
    """
    global _default_store
    if _default_store is None:
        _default_store = InstrumentStore(base_path)
    return _default_store.save_instrument(path, rig_id)


def get_instrument(rig_id: str, base_path: Optional[str] = None) -> Optional[dict]:
    """Get current instrument data for a rig (convenience function).

    Parameters
    ----------
    rig_id : str
        Rig identifier.
    base_path : Optional[str]
        Base directory path for the store. Only used if store not initialized.
        Defaults to /allen/aind/scratch/instrument_store.

    Returns
    -------
    Optional[dict]
        Instrument data as dict, or None if not found.
    """
    global _default_store
    if _default_store is None:
        _default_store = InstrumentStore(base_path)
    return _default_store.get_instrument(rig_id)


def list_instrument_ids(base_path: Optional[str] = None) -> list[str]:
    """List all instrument IDs in the store (convenience function).

    Parameters
    ----------
    base_path : Optional[str]
        Base directory path for the store. Only used if store not initialized.
        Defaults to /allen/aind/scratch/instrument_store.

    Returns
    -------
    list[str]
        List of instrument IDs, sorted alphabetically.
    """
    global _default_store
    if _default_store is None:
        _default_store = InstrumentStore(base_path)
    return _default_store.list_instrument_ids()
