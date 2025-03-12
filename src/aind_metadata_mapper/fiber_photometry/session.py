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


class FIBEtl(GenericEtl[JobSettings]):
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

    def _extract_session_time_from_files(
        self, data_dir: Union[str, Path]
    ) -> Optional[datetime]:
        """Extract session start time from fiber photometry data files.

        Args:
            data_dir: Path to the directory containing fiber photometry data

        Returns:
            Extracted session time or None if not found
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
                            # Parse the timestamp and set timezone to UTC
                            session_time = datetime.fromisoformat(
                                timestamp_str
                            ).replace(tzinfo=ZoneInfo("UTC"))
                            return session_time
                        except ValueError:
                            continue

        return None

    def _extract(self) -> JobSettings:
        """Extract metadata from job settings and external sources."""
        print("\nDebug - _extract method called")
        settings = self.job_settings
        print(
            f"Debug - settings.session_start_time: {getattr(settings, 'session_start_time', 'Not found')}"
        )
        print(
            f"Debug - settings.data_directory: {getattr(settings, 'data_directory', 'Not found')}"
        )

        # If session_start_time is not provided, try to extract it from data files
        if (
            not hasattr(settings, "session_start_time")
            or settings.session_start_time is None
        ):
            print("Debug - session_start_time is None or not present")
            if hasattr(settings, "data_directory") and settings.data_directory:
                print(
                    f"Debug - Extracting session time from {settings.data_directory}"
                )
                session_time = self._extract_session_time_from_files(
                    settings.data_directory
                )
                if session_time:
                    print(f"Debug - Extracted session time: {session_time}")
                    # Create a new JobSettings object with the extracted session_start_time
                    settings_dict = settings.model_dump()
                    settings_dict["session_start_time"] = session_time
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
    etl = FIBEtl(job_settings=main_job_settings)
    etl.run_job()
