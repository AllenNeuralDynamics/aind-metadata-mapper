"""Utility functions for fiber photometry metadata extraction.

This module provides functions for handling timestamps and file operations
specific to fiber photometry data, including conversion between milliseconds
and datetime objects, and extraction of session times from data files.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Union, Optional
from pathlib import Path
from tzlocal import get_localzone
import logging
import re

from aind_metadata_mapper.utils.timing_utils import (
    find_latest_timestamp_in_csv_files,
)


def extract_session_start_time_from_files(
    data_dir: Union[str, Path], local_timezone: str = "America/Los_Angeles"
) -> Optional[datetime]:
    """Extract session start time from fiber photometry data files.

    Parameters
    ----------
    data_dir : Union[str, Path]
        Path to the directory containing fiber photometry data
    local_timezone : str, optional
        Timezone string, by default "America/Los_Angeles"

    Returns
    -------
    Optional[datetime]
        Extracted session time in local timezone with
        offset format (+/-HH:MM) or None if not found
    """
    data_dir = Path(data_dir)

    # Look for FIP data files in the fib subdirectory
    fib_dir = data_dir / "fib"
    if not fib_dir.exists():
        # If no fib subdirectory, look in the main directory
        fib_dir = data_dir

    # Look for CSV or bin files with timestamps in their names
    file_patterns = ["FIP_Data*.csv", "FIP_Raw*.bin", "FIP_Raw*.bin.*"]

    for pattern in file_patterns:
        files = list(fib_dir.glob(pattern))
        if files:
            # Extract timestamp from the first matching file
            for file in files:
                # Extract timestamp from filename
                # (format: FIP_DataG_2024-12-31T15_49_53.csv)
                filename = file.name
                # Find the timestamp pattern in the filename
                match = re.search(
                    r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2})", filename
                )
                if match:
                    timestamp_str = match.group(1)
                    # Convert to datetime
                    # (replace _ with : for proper ISO format)
                    timestamp_str = timestamp_str.replace("_", ":")
                    try:
                        # Warn if auto-detected timezone differs
                        # from provided timezone
                        auto_tz = get_localzone()
                        if str(auto_tz) != local_timezone:
                            logging.warning(
                                f"Auto-detected timezone ({auto_tz}) "
                                f"differs from specified "
                                f"timezone ({local_timezone}). "
                                f"Using {local_timezone} timezone. "
                                f"Specify local_timezone parameter "
                                f"if this is incorrect."
                            )

                        tz = ZoneInfo(local_timezone)
                        local_time = datetime.fromisoformat(
                            timestamp_str
                        ).replace(tzinfo=tz)
                        return local_time
                    except ValueError:
                        continue

    return None


def extract_session_end_time_from_files(
    data_dir: Union[str, Path],
    session_start_time: datetime,
    local_timezone: str = "America/Los_Angeles",
) -> Optional[datetime]:
    """Extract session end time from fiber photometry data files.

    Parameters
    ----------
    data_dir : Union[str, Path]
        Path to the directory containing fiber photometry data
    session_start_time : datetime
        Previously determined session start time (in local timezone)
    local_timezone : str, optional
        Timezone string, by default "America/Los_Angeles"

    Returns
    -------
    Optional[datetime]
        Extracted session end time in local timezone or None if not found
    """
    data_dir = Path(data_dir)
    fib_dir = data_dir / "fib"
    if not fib_dir.exists():
        fib_dir = data_dir

    # Find the latest timestamp in FIP data files
    latest_time = find_latest_timestamp_in_csv_files(
        directory=fib_dir,
        file_pattern="FIP_Data*.csv",
        session_start_time=session_start_time,
        local_timezone=local_timezone,
    )

    # Calculate the session duration if we found a valid end time
    if latest_time is not None:
        # Ensure session_start_time and latest_time are in the same timezone
        tz = ZoneInfo(local_timezone)
        local_session_start = session_start_time.astimezone(tz)
        latest_time = latest_time.astimezone(tz)

        session_duration = latest_time - local_session_start
        logging.info(f"FIP session duration: {session_duration}")

    return latest_time
