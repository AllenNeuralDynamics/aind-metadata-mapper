"""Module for creating Fiber Photometry session metadata.

This module demonstrates a simple ETL pattern for creating session metadata,
with hooks for future extension to fetch additional data from external
services.
"""

import sys
import json
from typing import Union, Optional
from datetime import datetime
from pathlib import Path
import logging
from zoneinfo import ZoneInfo
import pandas as pd

from aind_data_schema.core.session import (
    DetectorConfig,
    FiberConnectionConfig,
    LightEmittingDiodeConfig,
    Session,
    Stream,
)
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.fiber_photometry.models import JobSettings

# Add timezone constant near top of file after imports
LOCAL_TIMEZONE = ZoneInfo("America/Los_Angeles")  # Seattle is in Pacific Time


class ETL(GenericEtl[JobSettings]):
    """Creates fiber photometry session metadata with ETL pattern."""

    def __init__(self, job_settings: Union[str, JobSettings]):
        """Initialize ETL with job settings.

        Args:
            job_settings: Either a JobSettings object or a JSON string that can
                be parsed into one
        """
        if isinstance(job_settings, str):
            job_settings = JobSettings(**json.loads(job_settings))
        super().__init__(job_settings)

    def _convert_ms_since_midnight_to_datetime(
        self, ms_since_midnight: float, base_date: datetime
    ) -> datetime:
        """Convert milliseconds since midnight to a datetime object in UTC.

        Args:
            ms_since_midnight: Float representing milliseconds since midnight in LOCAL_TIMEZONE
            base_date: Reference datetime to get the date from (must have tzinfo)

        Returns:
            datetime object in UTC with the same date as base_date but time from ms_since_midnight
        """
        # Convert milliseconds to hours, minutes, seconds, microseconds
        total_seconds = ms_since_midnight / 1000
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = int(total_seconds % 60)
        microseconds = int((total_seconds * 1000000) % 1000000)

        # Create new datetime in local timezone first
        local_dt = datetime(
            year=base_date.year,
            month=base_date.month,
            day=base_date.day,
            hour=hours,
            minute=minutes,
            second=seconds,
            microsecond=microseconds,
            tzinfo=LOCAL_TIMEZONE,  # Set to Pacific Time
        )

        # Convert to UTC
        return local_dt.astimezone(ZoneInfo("UTC"))

    def _extract_session_start_time_from_files(
        self, data_dir: Union[str, Path]
    ) -> Optional[datetime]:
        """Extract session start time from fiber photometry data files.

        Args:
            data_dir: Path to the directory containing fiber photometry data

        Returns:
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
                    # Extract timestamp from filename (format: FIP_DataG_2024-12-31T15_49_53.csv)
                    filename = file.name
                    # Find the timestamp pattern in the filename
                    import re

                    match = re.search(
                        r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2})", filename
                    )
                    if match:
                        timestamp_str = match.group(1)
                        # Convert to datetime (replace _ with : for proper ISO format)
                        timestamp_str = timestamp_str.replace("_", ":")
                        try:
                            # Parse the timestamp in local time zone and convert to UTC
                            local_time = datetime.fromisoformat(
                                timestamp_str
                            ).replace(tzinfo=LOCAL_TIMEZONE)
                            return local_time.astimezone(ZoneInfo("UTC"))
                        except ValueError:
                            continue

        return None

    def _extract_session_end_time_from_files(
        self, data_dir: Union[str, Path], session_start_time: datetime
    ) -> Optional[datetime]:
        """Extract session end time from fiber photometry data files.

        Args:
            data_dir: Path to the directory containing fiber photometry data
            session_start_time: Previously determined session start time for validation (in UTC)

        Returns:
            Extracted session end time in UTC or None if not found
        """
        data_dir = Path(data_dir)
        fib_dir = data_dir / "fib"
        if not fib_dir.exists():
            fib_dir = data_dir

        earliest_time = None
        latest_time = None

        # Convert session_start_time to local time for comparison
        local_session_start = session_start_time.astimezone(LOCAL_TIMEZONE)

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
                first_time = self._convert_ms_since_midnight_to_datetime(
                    first_ms, local_session_start
                )
                last_time = self._convert_ms_since_midnight_to_datetime(
                    last_ms, local_session_start
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
            time_diff = abs(
                (earliest_time - session_start_time).total_seconds()
            )
            if time_diff > 300:  # 5 minutes = 300 seconds
                logging.warning(
                    f"First timestamp in CSV ({earliest_time.isoformat()}) differs from "
                    f"session start time ({session_start_time.isoformat()}) by more than 5 minutes"
                )
                return None

        return latest_time

    def _extract(self) -> JobSettings:
        """Extract metadata from job settings and external sources."""
        settings = self.job_settings
        logging.info("Starting metadata extraction")

        # Extract session start time if not provided
        if (
            not hasattr(settings, "session_start_time")
            or settings.session_start_time is None
        ):
            if hasattr(settings, "data_directory") and settings.data_directory:
                logging.info(
                    f"Extracting session start time from {settings.data_directory}"
                )
                session_start_time = (
                    self._extract_session_start_time_from_files(
                        settings.data_directory
                    )
                )
                if session_start_time:
                    logging.info(
                        f"Found session start time: {session_start_time.isoformat()}"
                    )
                    # Create a new JobSettings object with the extracted session_start_time
                    settings_dict = settings.model_dump()
                    settings_dict["session_start_time"] = session_start_time

                    # Now extract session end time
                    logging.info("Extracting session end time")
                    session_end_time = (
                        self._extract_session_end_time_from_files(
                            settings.data_directory, session_start_time
                        )
                    )
                    if session_end_time:
                        logging.info(
                            f"Found session end time: {session_end_time.isoformat()}"
                        )
                        settings_dict["session_end_time"] = session_end_time
                    else:
                        logging.warning("Could not extract session end time")

                    return JobSettings(**settings_dict)
                else:
                    logging.warning(
                        "Could not extract session start time from data files"
                    )
            else:
                logging.warning(
                    "No data_directory provided and no session_start_time specified"
                )

        return settings

    def _create_stream(
        self, stream_data: dict, settings: JobSettings
    ) -> Stream:
        """Create a Stream object from stream configuration data."""
        # Ensure stream_start_time and stream_end_time are valid datetime objects
        stream_start_time = stream_data.get("stream_start_time")
        if stream_start_time is None:
            stream_start_time = settings.session_start_time

        stream_end_time = stream_data.get("stream_end_time")
        if stream_end_time is None:
            stream_end_time = settings.session_end_time
            # If session_end_time is also None, set it to session_start_time
            if stream_end_time is None:
                stream_end_time = settings.session_start_time

        return Stream(
            stream_start_time=stream_start_time,
            stream_end_time=stream_end_time,
            light_sources=[
                LightEmittingDiodeConfig(**ls)
                for ls in stream_data["light_sources"]
            ],
            stream_modalities=[Modality.FIB],
            detectors=[DetectorConfig(**d) for d in stream_data["detectors"]],
            fiber_connections=[
                FiberConnectionConfig(**fc)
                for fc in stream_data["fiber_connections"]
            ],
        )

    def _transform(self, settings: JobSettings) -> Session:
        """Transform extracted data into a valid Session object."""
        # Create data streams from configuration
        data_streams = [
            self._create_stream(stream, settings)
            for stream in settings.data_streams
        ]

        # Future: Add any data from external sources here such as calls to
        # endpoints to fetch additional data

        # Create session with all available data
        session = Session(
            experimenter_full_name=settings.experimenter_full_name,
            session_start_time=settings.session_start_time,
            session_end_time=settings.session_end_time,
            session_type=settings.session_type,
            rig_id=settings.rig_id,
            subject_id=settings.subject_id,
            iacuc_protocol=settings.iacuc_protocol,
            notes=settings.notes,
            data_streams=data_streams,
            mouse_platform_name=settings.mouse_platform_name,
            active_mouse_platform=settings.active_mouse_platform,
            # Add any optional fields gathered from services here
        )

        return session

    def run_job(self) -> JobResponse:
        """Run the ETL job and return a JobResponse."""
        extracted = self._extract()
        transformed = self._transform(extracted)
        return self._load(transformed, self.job_settings.output_directory)


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = ETL(job_settings=main_job_settings)
    etl.run_job()
