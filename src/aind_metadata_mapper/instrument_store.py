"""Filesystem-based caching for instrument.json files."""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

# Default base path for instrument store
# On Linux: /allen/aind/scratch/instrument_store
# On Windows: \\allen\aind\scratch\instrument_store
# Path class handles platform-specific separators automatically
DEFAULT_INSTRUMENT_STORE_PATH = os.getenv("AIND_INSTRUMENT_STORE_PATH", "/allen/aind/scratch/instrument_store")


class InstrumentStore:
    """Filesystem-based store for instrument.json files with versioning."""

    def __init__(self, base_path: Optional[str] = None, confirm_create: bool = True) -> None:
        """Initialize the instrument store.

        Parameters
        ----------
        base_path : Optional[str]
            Base directory path for the store. Defaults to DEFAULT_INSTRUMENT_STORE_PATH.
        confirm_create : bool
            If True, prompt for confirmation before creating a new directory.
            Defaults to True.

        Raises
        ------
        FileNotFoundError
            If the parent directory (one level up) does not exist, indicating
            the network path is not accessible.
        """
        if base_path is None:
            base_path = DEFAULT_INSTRUMENT_STORE_PATH
        self.base_path = Path(base_path)

        # Verify parent directory exists (one level up) to ensure network connectivity
        parent_path = self.base_path.parent
        if not parent_path.exists():
            raise FileNotFoundError(
                f"Parent directory does not exist: {parent_path}\n"
                f"This likely indicates the network path is not accessible on this machine."
            )

        # If the full path doesn't exist, confirm before creating
        if not self.base_path.exists():
            if confirm_create:
                response = (
                    input(
                        f"The instrument store directory does not exist:\n  {self.base_path}\n"
                        f"Create this directory? [y/N]: "
                    )
                    .strip()
                    .lower()
                )
                if response not in ("y", "yes"):
                    raise ValueError(f"Directory creation cancelled by user: {self.base_path}")

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
                    filename = f"instrument_{timestamp.date().isoformat().replace('-', '')}.json"
                    return rig_dir / filename
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

        # Fall back to file mtime (use date only)
        mtime_dt = datetime.fromtimestamp(current_path.stat().st_mtime)
        mtime_date = mtime_dt.date()
        filename = f"instrument_{mtime_date.isoformat().replace('-', '')}.json"
        return rig_dir / filename

    def _parse_modification_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse modification_date string to datetime using ISO format.

        Parameters
        ----------
        date_str : Optional[str]
            Date string from JSON (ISO format).

        Returns
        -------
        Optional[datetime]
            Parsed datetime, or None if parsing fails.
        """
        if date_str is None:
            return None

        try:
            # Replace Z with +00:00 for fromisoformat compatibility
            iso_str = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_str)
            # Return naive datetime (remove timezone info)
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except (ValueError, AttributeError):
            return None


def save_instrument(path: str, rig_id: str, base_path: Optional[str] = None, confirm_create: bool = True) -> Path:
    """Save instrument.json to store (convenience function).

    Parameters
    ----------
    path : str
        Path to the instrument.json file to save.
    rig_id : str
        Rig identifier.
    base_path : Optional[str]
        Base directory path for the store. Only used if store not initialized.
        Defaults to /allen/aind/scratch/instrument_store (or AIND_INSTRUMENT_STORE_PATH env var).
    confirm_create : bool
        If True, prompt for confirmation before creating a new directory.
        Defaults to True.

    Returns
    -------
    Path
        Path to the saved instrument.json file in the store.
    """
    store = InstrumentStore(base_path, confirm_create=confirm_create)
    return store.save_instrument(path, rig_id)


def get_instrument(rig_id: str, base_path: Optional[str] = None, confirm_create: bool = True) -> Optional[dict]:
    """Get current instrument data for a rig (convenience function).

    Parameters
    ----------
    rig_id : str
        Rig identifier.
    base_path : Optional[str]
        Base directory path for the store. Only used if store not initialized.
        Defaults to /allen/aind/scratch/instrument_store (or AIND_INSTRUMENT_STORE_PATH env var).
    confirm_create : bool
        If True, prompt for confirmation before creating a new directory.
        Defaults to True.

    Returns
    -------
    Optional[dict]
        Instrument data as dict, or None if not found.
    """
    store = InstrumentStore(base_path, confirm_create=confirm_create)
    return store.get_instrument(rig_id)


def list_instrument_ids(base_path: Optional[str] = None, confirm_create: bool = True) -> list[str]:
    """List all instrument IDs in the store (convenience function).

    Parameters
    ----------
    base_path : Optional[str]
        Base directory path for the store. Only used if store not initialized.
        Defaults to /allen/aind/scratch/instrument_store (or AIND_INSTRUMENT_STORE_PATH env var).
    confirm_create : bool
        If True, prompt for confirmation before creating a new directory.
        Defaults to True.

    Returns
    -------
    list[str]
        List of instrument IDs, sorted alphabetically.
    """
    store = InstrumentStore(base_path, confirm_create=confirm_create)
    return store.list_instrument_ids()
