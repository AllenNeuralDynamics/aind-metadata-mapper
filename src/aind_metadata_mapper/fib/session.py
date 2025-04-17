"""Module for creating Fiber Photometry session metadata.

This module implements an ETL (Extract, Transform, Load) pattern for generating
standardized session metadata from fiber photometry experiments. It handles:

- Extraction of session times from data files
- Transformation of raw data into standardized session objects
- Loading/saving of session metadata in a standard format

The ETL class provides hooks for future extension to fetch additional data from
external services or handle new data formats.
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
    verify_output_file,
)


class ETL(GenericEtl[JobSettings]):
    """Creates fiber photometry session metadata using an ETL pattern.

    This class handles the full lifecycle of session metadata creation:
    - Extracting timing information from data files
    - Transforming raw data into standardized session objects
    - Loading/saving session metadata in a standard format

    The ETL process ensures that all required metadata fields are populated
    and validates the output against the AIND data schema.
    """

    def __init__(self, job_settings: Union[str, JobSettings]):
        """Initialize ETL with job settings.

        Parameters
        ----------
        job_settings : Union[str, JobSettings]
            Either a JobSettings object or a JSON string that can
            be parsed into one. The settings define all required parameters
            for the session metadata, including experimenter info, subject
            ID, data paths, etc.

        Raises
        ------
        ValidationError
            If the provided settings fail schema validation
        JSONDecodeError
            If job_settings is a string but not valid JSON
        """
        if isinstance(job_settings, str):
            job_settings = JobSettings(**json.loads(job_settings))
        super().__init__(job_settings)

    def _extract(self) -> JobSettings:
        """Extract metadata from job settings and data files.

        This method attempts to extract session start and end times from the
        data files if they are not explicitly provided in the job settings.
        It looks for timestamps in file names and CSV data to determine the
        temporal bounds of the session.

        Returns
        -------
        JobSettings
            Updated settings object with extracted timing info

        Notes
        -----
        If data_directory is not provided or no valid timestamps are found,
        the original settings are returned unchanged.
        """
        settings = self.job_settings
        logging.info("Starting metadata extraction")

        if (
            not hasattr(settings, "session_start_time")
            or settings.session_start_time is None
        ):
            if hasattr(settings, "data_directory") and settings.data_directory:
                logging.info(
                    "Extracting session start time from "
                    f"{settings.data_directory}"
                )
                return update_job_settings_with_times(
                    settings, settings.data_directory
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
        """Create a Stream object from stream configuration data.

        Parameters
        ----------
        stream_data : dict
            Dictionary containing stream configuration including
            light sources, detectors, and fiber connections
        settings : JobSettings
            JobSettings object containing session-level settings
            that may be used as defaults for stream-level attributes

        Returns
        -------
        Stream
            A fully configured Stream object representing a single
            data stream within the session

        Notes
        -----
        If stream timing information is not provided in stream_data,
        it falls back to session-level timing. If session end time is
        also not available, it defaults to the session start time.
        """
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
        """Transform extracted data into a valid Session object.

        This method creates a standardized Session object from the provided
        settings, including all data streams and their configurations. It
        ensures that all required fields are populated and properly formatted.

        Args:
            settings: JobSettings object containing all necessary session
                configuration data

        Returns:
            Session: A fully configured Session object that conforms to the
                AIND data schema

        Notes:
            The resulting Session object includes:
            - Experimenter information
            - Session timing
            - Subject and protocol IDs
            - Data stream configurations
            - Platform settings
        """
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
        """Run the complete ETL job and return a JobResponse.

        This method orchestrates the full ETL process:
        1. Extracts metadata from files and settings
        2. Transforms the data into a valid Session object
        3. Saves the session metadata to the specified output location
        4. Verifies the output file was written correctly

        Returns
        -------
        JobResponse
            Object containing status code, message, and optional data.
            Status codes:
            - 200: Success
            - 406: Validation errors
            - 500: File writing errors

        Notes
        -----
        If a custom output filename is specified in job_settings, it will
        be used instead of the default filename.
        """
        extracted = self._extract()
        transformed = self._transform(extracted)

        # If a custom filename is specified, save it directly
        if self.job_settings.output_filename:
            output_path = (
                self.job_settings.output_directory
                / self.job_settings.output_filename
            )
            try:
                with open(output_path, "w") as f:
                    f.write(transformed.model_dump_json(indent=2))
                if verify_output_file(
                    self.job_settings.output_directory,
                    self.job_settings.output_filename,
                ):
                    return JobResponse(
                        status_code=200,
                        message=f"Successfully wrote file to {output_path}",
                        data=None,
                    )
                else:
                    return JobResponse(
                        status_code=500,
                        message=f"File verification failed for {output_path}",
                        data=None,
                    )
            except Exception as e:
                return JobResponse(
                    status_code=500,
                    message=f"Error writing to {output_path}: {str(e)}",
                    data=None,
                )

        # Otherwise use the default write_standard_file method
        return self._load(transformed, self.job_settings.output_directory)


def update_job_settings_with_times(
    settings: JobSettings, data_directory: str
) -> JobSettings:
    """Update JobSettings with extracted session start and end times.

    This helper function attempts to extract session timing information from
    data files and updates the provided settings accordingly.

    Parameters
    ----------
    settings : JobSettings
        Original JobSettings object
    data_directory : str
        Directory containing the fiber photometry data files

    Returns
    -------
    JobSettings
        Updated settings object with extracted timing information

    Notes
    -----
    If timing information cannot be extracted, the original settings are
    returned unchanged. Successful extraction will update both start and
    end times if available.
    """
    settings_dict = settings.model_dump()
    session_start_time = extract_session_start_time_from_files(data_directory)

    if session_start_time:
        settings_dict["session_start_time"] = session_start_time
        session_end_time = extract_session_end_time_from_files(
            data_directory, session_start_time
        )
        if session_end_time:
            settings_dict["session_end_time"] = session_end_time

    return JobSettings(**settings_dict)


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = ETL(job_settings=main_job_settings)
    etl.run_job()
