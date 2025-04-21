"""Utility functions for fiber photometry data processing.

This module provides functions for handling timestamps and file operations
specific to fiber photometry data, including conversion between milliseconds
and datetime objects, and extraction of session times from data files.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Union, Optional
from pathlib import Path
from tzlocal import get_localzone
import logging
import pandas as pd
import re


def convert_ms_since_midnight_to_datetime(
    ms_since_midnight: float,
    base_date: datetime,
    local_timezone: Optional[str] = None,
) -> datetime:
    """Convert milliseconds since midnight to a datetime object in UTC.

    Parameters
    ----------
    ms_since_midnight : float
        Float representing milliseconds since midnight in local timezone
    base_date : datetime
        Reference datetime to get the date from (must have tzinfo)
    local_timezone : Optional[str], optional
        Timezone string (e.g., "America/Los_Angeles").
        If not provided, will use system timezone.

    Returns
    -------
    datetime
        datetime object in UTC with the same date as base_date but time from
        ms_since_midnight
    """
    # Get timezone (either specified or system default)
    tz = ZoneInfo(local_timezone) if local_timezone else get_localzone()

    # Get midnight of base_date in local time
    base_date_local = base_date.astimezone(tz)
    base_midnight_local = datetime.combine(
        base_date_local.date(), datetime.min.time()
    )
    base_midnight_local = base_midnight_local.replace(tzinfo=tz)

    # Convert local midnight to UTC
    base_midnight_utc = base_midnight_local.astimezone(ZoneInfo("UTC"))

    # Add milliseconds as timedelta
    delta = timedelta(milliseconds=ms_since_midnight)

    return base_midnight_utc + delta


def extract_session_start_time_from_files(
    data_dir: Union[str, Path], local_timezone: Optional[str] = None
) -> Optional[datetime]:
    """Extract session start time from fiber photometry data files.

    Parameters
    ----------
    data_dir : Union[str, Path]
        Path to the directory containing fiber photometry data
    local_timezone : Optional[str], optional
        Timezone string (e.g., "America/Los_Angeles").
        If not provided, will use system timezone.

    Returns
    -------
    Optional[datetime]
        Extracted session time in UTC or None if not found
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
                        # Parse the timestamp in local time zone
                        # and convert to UTC
                        tz = (
                            ZoneInfo(local_timezone)
                            if local_timezone
                            else get_localzone()
                        )
                        local_time = datetime.fromisoformat(
                            timestamp_str
                        ).replace(tzinfo=tz)
                        return local_time.astimezone(ZoneInfo("UTC"))
                    except ValueError:
                        continue

    return None


def extract_session_end_time_from_files(
    data_dir: Union[str, Path],
    session_start_time: datetime,
    local_timezone: Optional[str] = None,
) -> Optional[datetime]:
    """Extract session end time from fiber photometry data files.

    Parameters
    ----------
    data_dir : Union[str, Path]
        Path to the directory containing fiber photometry data
    session_start_time : datetime
        Previously determined session start time (in UTC)
    local_timezone : Optional[str], optional
        Timezone string (e.g., "America/Los_Angeles").
        If not provided, will use system timezone.

    Returns
    -------
    Optional[datetime]
        Extracted session end time in UTC or None if not found
    """
    data_dir = Path(data_dir)
    fib_dir = data_dir / "fib"
    if not fib_dir.exists():
        fib_dir = data_dir

    earliest_time = None
    latest_time = None

    # Get timezone
    tz = ZoneInfo(local_timezone) if local_timezone else get_localzone()

    # Convert session_start_time to local time for comparison
    local_session_start = session_start_time.astimezone(tz)

    # Look for CSV files
    for csv_file in fib_dir.glob("FIP_Data*.csv"):
        try:
            # Read CSV file using pandas - with no header
            df = pd.read_csv(csv_file, header=None)
            if df.empty:
                continue

            # Use first column (index 0) for time data
            first_ms = df[0].min()
            last_ms = df[0].max()

            # Convert to datetime objects (will be in UTC)
            first_time = convert_ms_since_midnight_to_datetime(
                first_ms, local_session_start, local_timezone=local_timezone
            )
            last_time = convert_ms_since_midnight_to_datetime(
                last_ms, local_session_start, local_timezone=local_timezone
            )

            # Update earliest and latest times
            if earliest_time is None or first_time < earliest_time:
                earliest_time = first_time
            if latest_time is None or last_time > latest_time:
                latest_time = last_time

        except Exception as e:
            logging.warning(f"Error processing file {csv_file}: {str(e)}")
            continue

    # Validate earliest time against session start time (both in UTC)
    if earliest_time is not None and session_start_time is not None:
        time_diff = abs((earliest_time - session_start_time).total_seconds())
        if time_diff > 300:  # 5 minutes = 300 seconds
            logging.warning(
                f"First timestamp in CSV "
                f"({earliest_time.isoformat()}) "
                f"differs from session start time "
                f"({session_start_time.isoformat()}) "
                f"by more than 5 minutes"
            )
            return None

    return latest_time
