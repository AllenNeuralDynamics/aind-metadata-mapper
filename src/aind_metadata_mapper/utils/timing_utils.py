"""Shared timing utility functions for AIND metadata modules.

This module provides common functions for handling
timestamps and time conversions
used by multiple metadata modules including
Pavlovian behavior and FIP (fiber photometry).
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Union, Optional
from pathlib import Path
import pandas as pd
import logging
import math


def validate_session_temporal_consistency(session) -> None:
    """Validate that all timestamps in a Session are in proper temporal order.

    Parameters
    ----------
    session : Session
        AIND Session object to validate

    Raises
    ------
    AssertionError
        If any temporal consistency checks fail

    Notes
    -----
    Validates:
    - session_end_time > session_start_time
    - stream_end_time > stream_start_time for all data streams
    """
    # Validate session-level temporal consistency
    assert session.session_end_time > session.session_start_time, (
        f"Session end time ({session.session_end_time}) must be greater than "
        f"session start time ({session.session_start_time})"
    )

    # Validate data stream temporal consistency
    if session.data_streams:
        for i, stream in enumerate(session.data_streams):
            assert stream.stream_end_time > stream.stream_start_time, (
                f"Data stream {i} end time ({stream.stream_end_time}) "
                f"must be greater than "
                f"stream start time ({stream.stream_start_time})"
            )


def convert_ms_since_midnight_to_datetime(
    ms_since_midnight: float,
    base_date: datetime,
    local_timezone: str = "America/Los_Angeles",
) -> datetime:
    """
    Convert milliseconds since midnight
    to a datetime object in local timezone.

    Parameters
    ----------
    ms_since_midnight : float
        Float representing milliseconds since midnight in local timezone
    base_date : datetime
        Reference datetime to get the date from (must have tzinfo)
    local_timezone : str, optional
        Timezone string, by default "America/Los_Angeles"

    Returns
    -------
    datetime
        datetime object in local timezone with the
        same date as base_date but time from
        ms_since_midnight
    """
    # Check for NaN or invalid values
    if math.isnan(ms_since_midnight) or math.isinf(ms_since_midnight):
        raise ValueError(f"Invalid timestamp value: {ms_since_midnight}")

    # Use provided timezone
    tz = ZoneInfo(local_timezone)

    # Get midnight of base_date in local time
    base_date_local = base_date.astimezone(tz)
    base_midnight_local = datetime.combine(
        base_date_local.date(), datetime.min.time()
    )
    base_midnight_local = base_midnight_local.replace(tzinfo=tz)

    # Add milliseconds as timedelta
    delta = timedelta(milliseconds=float(ms_since_midnight))

    return base_midnight_local + delta


def _read_csv_safely(csv_file: Path) -> Optional[pd.DataFrame]:
    """
    Read CSV file with fallback if the header is not present.
    """
    # Try reading with header first
    try:
        df = pd.read_csv(csv_file)
        if not df.empty:
            return df
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        pass

    # Try reading without header as fallback
    try:
        df = pd.read_csv(csv_file, header=None)
        if not df.empty:
            return df
    except Exception:
        pass

    return None


def _extract_max_timestamp(df: pd.DataFrame) -> Optional[float]:
    """Extract the maximum timestamp value from a DataFrame."""

    def _validate_max_value(max_val):
        """Helper to validate a max value is not NaN."""
        if pd.isna(max_val):
            return None
        try:
            if math.isnan(max_val):
                return None
        except (TypeError, ValueError):
            return None
        return max_val

    # Handle empty DataFrame
    if df.empty:
        return None

    # Handle files without headers first (simpler case)
    if df.columns.dtype != "object" and df.shape[1] >= 1:
        max_val = df[0].max()
        return _validate_max_value(max_val)

    # Handle files with headers - try time-related columns
    time_cols = [
        col
        for col in df.columns
        if any(term in col.lower() for term in ["time", "timestamp", "ms"])
    ]
    if time_cols:
        max_val = df[time_cols[0]].max()
        return _validate_max_value(max_val)

    # Fallback to first numeric column
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if len(numeric_cols) > 0:
        max_val = df[numeric_cols[0]].max()
        return _validate_max_value(max_val)

    return None


def find_latest_timestamp_in_csv_files(
    directory: Union[str, Path],
    file_pattern: str,
    session_start_time: datetime,
    local_timezone: str = "America/Los_Angeles",
) -> Optional[datetime]:
    """Find the latest timestamp across multiple CSV files.

    Parameters
    ----------
    directory : Union[str, Path]
        Directory containing CSV files to search
    file_pattern : str
        Glob pattern to match CSV files
    session_start_time : datetime
        Session start time with timezone info,
        used as base date for timestamp conversion
    local_timezone : str, optional
        Timezone string, by default "America/Los_Angeles"

    Returns
    -------
    Optional[datetime]
        Datetime object representing the latest timestamp found,
        or None if no valid timestamps
    """
    directory = Path(directory)
    if not directory.exists():
        logging.warning(f"Directory not found: {directory}")
        return None

    files = list(directory.glob(file_pattern))
    if not files:
        logging.warning(
            f"No files matching pattern '{file_pattern}' in {directory}"
        )
        return None

    latest_ms = None

    for csv_file in files:
        try:
            df = _read_csv_safely(csv_file)
            if df is None:
                continue

            max_ms = _extract_max_timestamp(df)
            if max_ms is not None and (
                latest_ms is None or max_ms > latest_ms
            ):
                latest_ms = max_ms

        except Exception as e:
            logging.warning(f"Error processing file {csv_file}: {str(e)}")
            continue

    # Convert maximum timestamp found to datetime
    if latest_ms is not None:
        return convert_ms_since_midnight_to_datetime(
            latest_ms, session_start_time, local_timezone=local_timezone
        )

    return None
