"""Module for creating Fiber Photometry session metadata.

This module demonstrates a simple ETL pattern for creating session metadata,
with hooks for future extension to fetch additional data from external
services.
"""

import sys
import json
from typing import Union
import logging

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
from aind_metadata_mapper.fib.models import JobSettings
from aind_metadata_mapper.fib.utils import (
    extract_session_start_time_from_files,
    extract_session_end_time_from_files,
)


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
                    "Extracting session start time from "
                    f"{settings.data_directory}"
                )
                session_start_time = extract_session_start_time_from_files(
                    settings.data_directory
                )
                if session_start_time:
                    logging.info(
                        "Found session start time: "
                        f"{session_start_time.isoformat()}"
                    )
                    # Create a new JobSettings object
                    # with the extracted session_start_time
                    settings_dict = settings.model_dump()
                    settings_dict["session_start_time"] = session_start_time

                    # Now extract session end time
                    logging.info("Extracting session end time")
                    session_end_time = extract_session_end_time_from_files(
                        settings.data_directory, session_start_time
                    )
                    if session_end_time:
                        logging.info(
                            "Found session end time: "
                            f"{session_end_time.isoformat()}"
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
                    "No data_directory provided and "
                    "no session_start_time specified"
                )

        return settings

    def _create_stream(
        self, stream_data: dict, settings: JobSettings
    ) -> Stream:
        """Create a Stream object from stream configuration data."""
        # Ensure stream_start_time and stream_end_time
        # are valid datetime objects
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
