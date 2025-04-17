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
from typing import Union, Optional, List
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
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
from aind_metadata_mapper.fib.models import JobSettings
from aind_metadata_mapper.fib.utils import (
    extract_session_start_time_from_files,
    extract_session_end_time_from_files,
    verify_output_file,
)


@dataclass
class FiberData:
    """Intermediate data model for fiber photometry data."""

    start_time: datetime
    end_time: Optional[datetime]
    data_files: List[Path]
    timestamps: List[float]
    light_source_configs: List[dict]
    detector_configs: List[dict]
    fiber_configs: List[dict]
    subject_id: str
    experimenter_full_name: List[str]
    rig_id: str
    iacuc_protocol: str
    notes: str
    mouse_platform_name: str
    active_mouse_platform: bool


class ETL(GenericEtl[JobSettings]):
    """Creates fiber photometry session metadata using an ETL pattern.

    This class handles the full lifecycle of session metadata creation:
    - Extracting timing information from data files
    - Transforming raw data into standardized session objects
    - Loading/saving session metadata in a standard format

    The ETL process ensures that all required metadata fields are populated
    and validates the output against the AIND data schema.

    This class inherits from GenericEtl which provides the _load method
    for writing session metadata to a JSON file using a standard filename
    format (session_fib.json).
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

    def _extract(self) -> FiberData:
        """Extract metadata and raw data from fiber photometry files.

        This method parses the raw data files to create an
        intermediate data model containing all necessary
        information for creating a Session object.

        Returns
        -------
        FiberData
            Intermediate data model containing parsed file data and metadata
        """
        settings = self.job_settings
        data_dir = Path(settings.data_directory)

        data_files = list(data_dir.glob("FIP_Data*.csv"))
        start_time = extract_session_start_time_from_files(data_dir)
        end_time = (
            extract_session_end_time_from_files(data_dir, start_time)
            if start_time
            else None
        )

        timestamps = []
        for file in data_files:
            df = pd.read_csv(file, header=None)
            timestamps.extend(df[0].tolist())

        stream_data = settings.data_streams[0]

        return FiberData(
            start_time=start_time,
            end_time=end_time,
            data_files=data_files,
            timestamps=timestamps,
            light_source_configs=stream_data["light_sources"],
            detector_configs=stream_data["detectors"],
            fiber_configs=stream_data["fiber_connections"],
            subject_id=settings.subject_id,
            experimenter_full_name=settings.experimenter_full_name,
            rig_id=settings.rig_id,
            iacuc_protocol=settings.iacuc_protocol,
            notes=settings.notes,
            mouse_platform_name=settings.mouse_platform_name,
            active_mouse_platform=settings.active_mouse_platform,
        )

    def _transform(self, fiber_data: FiberData) -> Session:
        """Transform extracted data into a valid Session object.

        Parameters
        ----------
        fiber_data : FiberData
            Intermediate data model containing parsed file data and metadata

        Returns
        -------
        Session
            A fully configured Session object that
            conforms to the AIND data schema
        """
        stream = Stream(
            stream_start_time=fiber_data.start_time,
            stream_end_time=fiber_data.end_time,
            light_sources=[
                LightEmittingDiodeConfig(**ls)
                for ls in fiber_data.light_source_configs
            ],
            stream_modalities=[Modality.FIB],
            detectors=[
                DetectorConfig(**d) for d in fiber_data.detector_configs
            ],
            fiber_connections=[
                FiberConnectionConfig(**fc) for fc in fiber_data.fiber_configs
            ],
        )

        session = Session(
            experimenter_full_name=fiber_data.experimenter_full_name,
            session_start_time=fiber_data.start_time,
            session_end_time=fiber_data.end_time,
            session_type="FIB",
            rig_id=fiber_data.rig_id,
            subject_id=fiber_data.subject_id,
            iacuc_protocol=fiber_data.iacuc_protocol,
            notes=fiber_data.notes,
            data_streams=[stream],
            mouse_platform_name=fiber_data.mouse_platform_name,
            active_mouse_platform=fiber_data.active_mouse_platform,
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
        be used instead of the default filename. Otherwise, uses the parent
        class's _load method which saves to 'session_fib.json'.
        """
        fiber_data = self._extract()
        transformed = self._transform(fiber_data)

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

        # Use parent class's _load method to write with default filename (session_fib.json)
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
