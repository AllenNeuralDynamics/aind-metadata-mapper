"""Module for creating Fiber Photometry session metadata.

This module demonstrates a simple ETL pattern for creating session metadata,
with hooks for future extension to fetch additional data from external
services.
"""

import sys
import json

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
from aind_metadata_mapper.fip.job_settings import JobSettings


class FIBEtl(GenericEtl[JobSettings]):
    """Creates fiber photometry session metadata with ETL pattern."""

    def __init__(self, job_settings: str | JobSettings):
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
        return self.job_settings

    def _create_stream(
        self, stream_data: dict, settings: JobSettings
    ) -> Stream:
        """Create a Stream object from stream configuration data."""
        return Stream(
            stream_start_time=stream_data.get(
                "stream_start_time", settings.session_start_time
            ),
            stream_end_time=stream_data.get(
                "stream_end_time", settings.session_end_time
            ),
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

        # Create base session
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

        # Future: Add any data from external sources here such as calls to
        # endpoints to fetch additional data

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
