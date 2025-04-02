"""Module for creating Pavlovian Behavior session metadata.

This module implements an ETL (Extract, Transform, Load) pattern for generating
standardized session metadata from Pavlovian conditioning experiments. It
handles:

- Extraction of session times and trial data from behavior files
- Transformation of raw data into standardized session objects
- Loading/saving of session metadata in a standard format

The ETL class provides hooks for future extension to fetch additional data from
external services or handle new data formats.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Union

from aind_data_schema.core.session import Session
from aind_data_schema_models.units import VolumeUnit

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.pavlovian_behavior.models import JobSettings
from aind_metadata_mapper.pavlovian_behavior.utils import extract_session_data


class ETL(GenericEtl[JobSettings]):
    """Creates Pavlovian behavior session metadata using an ETL pattern.

    This class handles the full lifecycle of session metadata creation:
    - Extracting timing and trial information from behavior files
    - Transforming raw data into standardized session objects
    - Loading/saving session metadata in a standard format

    The ETL process ensures that all required metadata fields are populated
    and validates the output against the AIND data schema.
    """

    def __init__(self, job_settings: Union[str, JobSettings]):
        """Initialize ETL with job settings.

        Args:
            job_settings: Either a JobSettings object or a JSON string that can
                be parsed into one. The settings define all required parameters
                for the session metadata.

        Raises:
            ValidationError: If the provided settings fail schema validation
            JSONDecodeError: If job_settings is a string but not valid JSON
        """
        if isinstance(job_settings, str):
            job_settings = JobSettings(**json.loads(job_settings))
        super().__init__(job_settings)

    def _extract(self) -> JobSettings:
        """Extract metadata from job settings and behavior files.

        Attempts to extract session timing and trial information from behavior
        files if not explicitly provided in job settings.

        Returns:
            JobSettings: Updated settings object with extracted data

        Raises:
            ValueError: If required files are missing
                or data cannot be extracted
        """
        settings = self.job_settings
        logging.info("Starting metadata extraction")

        if (
            not hasattr(settings, "data_directory")
            or not settings.data_directory
        ):
            raise ValueError(
                "data_directory is required for metadata extraction"
            )

        try:
            data_dir = Path(settings.data_directory)
            reward_units = getattr(settings, "reward_units_per_trial", 2.0)

            session_time, stimulus_epochs = extract_session_data(
                data_dir, reward_units
            )

            # Update settings with extracted data
            settings.session_start_time = session_time
            if stimulus_epochs:
                settings.session_end_time = stimulus_epochs[
                    0
                ].stimulus_end_time
                settings.stimulus_epochs = stimulus_epochs

        except (FileNotFoundError, ValueError) as e:
            raise ValueError(f"Failed to extract data from files: {str(e)}")

        return settings

    def _transform(self, settings: JobSettings) -> Session:
        """Transform extracted data into a valid Session object.

        Creates a standardized Session object from the provided settings,
        including all stimulus epochs and their configurations.

        Args:
            settings: Job settings containing session data

        Returns:
            Session: A fully configured Session object that conforms to the
                AIND data schema
        """
        # Get stimulus epochs directly from settings
        stimulus_epochs = settings.stimulus_epochs
        reward_consumed_total = sum(
            epoch.reward_consumed_during_epoch for epoch in stimulus_epochs
        )

        # Process data streams
        data_streams = []
        if hasattr(settings, "data_streams") and settings.data_streams:
            for stream in settings.data_streams:
                if stream.get("stream_start_time") is None:
                    stream["stream_start_time"] = settings.session_start_time
                if stream.get("stream_end_time") is None:
                    stream["stream_end_time"] = settings.session_end_time
                data_streams.append(stream)

        # Format timestamps as ISO with Z suffix
        start_time = (
            settings.session_start_time.replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        end_time = (
            settings.session_end_time.replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

        session = Session(
            experimenter_full_name=settings.experimenter_full_name,
            session_start_time=start_time,
            session_end_time=end_time,
            session_type=settings.session_type,
            rig_id=settings.rig_id,
            subject_id=settings.subject_id,
            iacuc_protocol=settings.iacuc_protocol,
            notes=settings.notes,
            mouse_platform_name=settings.mouse_platform_name,
            active_mouse_platform=settings.active_mouse_platform,
            data_streams=data_streams,
            stimulus_epochs=stimulus_epochs,
            reward_consumed_total=reward_consumed_total,
            reward_consumed_unit=VolumeUnit.UL,
        )

        return session

    def run_job(self) -> JobResponse:
        """Run the complete ETL job and return a JobResponse.

        This method orchestrates the full ETL process:
        1. Extracts metadata from files and settings
        2. Transforms the data into a valid Session object
        3. Saves the session metadata to the specified output location

        Returns:
            JobResponse: Object containing status code and message:
                - 200: Success
                - 406: Validation errors
                - 500: File writing errors
        """
        extracted = self._extract()
        transformed = self._transform(extracted)

        output_directory = extracted.output_directory
        output_filename = extracted.output_filename

        if output_directory and output_filename:
            output_directory = Path(output_directory)
            output_path = output_directory / output_filename

            try:
                output_directory.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w") as f:
                    f.write(transformed.model_dump_json(indent=2))

                # Verify the file was written correctly
                try:
                    with open(output_path, "r") as f:
                        json.load(f)
                    return JobResponse(
                        status_code=200,
                        message=f"Session metadata saved to: {output_path}",
                        data=None,
                    )
                except json.JSONDecodeError:
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

        return self._load(transformed, output_directory)


if __name__ == "__main__":  # pragma: no cover
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = ETL(job_settings=main_job_settings)
    etl.run_job()
