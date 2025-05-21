"""Shared timing utility functions for AIND metadata modules.

This module provides common functions for handling timestamps and time conversions
used by multiple metadata modules including Pavlovian behavior and FIP (fiber photometry).
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Union, Optional
from pathlib import Path
from tzlocal import get_localzone
import pandas as pd
import logging


def convert_ms_since_midnight_to_datetime(
    ms_since_midnight: float,
    base_date: datetime,
    local_timezone: Optional[str] = None,
) -> datetime:
    """Convert milliseconds since midnight to a datetime object in local timezone.

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
        datetime object in local timezone with the same date as base_date but time from
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

    # Add milliseconds as timedelta
    delta = timedelta(milliseconds=ms_since_midnight)

    return base_midnight_local + delta


def find_latest_timestamp_in_csv_files(
    directory: Union[str, Path],
    file_pattern: str,
    session_start_time: datetime,
    local_timezone: Optional[str] = None,
) -> Optional[datetime]:
    """Find the latest timestamp in a set of CSV files.

    Parameters
    ----------
    directory : Union[str, Path]
        Directory containing CSV files to search
    file_pattern : str
        Glob pattern to match CSV files
    session_start_time : datetime
        Session start time with timezone info, used as base date for timestamp conversion
    local_timezone : Optional[str], optional
        Timezone string. If not provided, system timezone is used.

    Returns
    -------
    Optional[datetime]
        Datetime object representing the latest timestamp found, or None if no valid timestamps
    """
    directory = Path(directory)
    if not directory.exists():
        logging.warning(f"Directory not found: {directory}")
        return None

    latest_time = None
    latest_ms = None

    # Get all matching CSV files
    files = list(directory.glob(file_pattern))
    if not files:
        logging.warning(
            f"No files matching pattern '{file_pattern}' in {directory}"
        )
        return None

    for csv_file in files:
        try:
            # Try reading with header first
            try:
                df = pd.read_csv(csv_file)
                # Look for time columns
                time_cols = [
                    col
                    for col in df.columns
                    if any(
                        term in col.lower()
                        for term in ["time", "timestamp", "ms"]
                    )
                ]

                if time_cols:
                    # Use first identified time column
                    col = time_cols[0]
                    max_ms = df[col].max()
                    if latest_ms is None or max_ms > latest_ms:
                        latest_ms = max_ms
                else:
                    # No time columns found, try first numeric column
                    numeric_cols = df.select_dtypes(include=["number"]).columns
                    if len(numeric_cols) > 0:
                        max_ms = df[numeric_cols[0]].max()
                        if latest_ms is None or max_ms > latest_ms:
                            latest_ms = max_ms
            except (pd.errors.EmptyDataError, pd.errors.ParserError):
                # Try reading with no header
                df = pd.read_csv(csv_file, header=None)
                if df.empty:
                    continue

                # Use first column assuming it's time data
                if df.shape[1] == 1:  # Single column file
                    max_ms = df[0].max()
                    if latest_ms is None or max_ms > latest_ms:
                        latest_ms = max_ms

        except Exception as e:
            logging.warning(f"Error processing file {csv_file}: {str(e)}")
            continue

    # Convert maximum timestamp found to datetime
    if latest_ms is not None:
        latest_time = convert_ms_since_midnight_to_datetime(
            latest_ms, session_start_time, local_timezone=local_timezone
        )

    return latest_time
