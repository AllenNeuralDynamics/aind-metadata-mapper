"""Module to write valid OptoStim and Subject schemas"""

import re
import sys
from dataclasses import dataclass
from datetime import timedelta
from typing import Union

from aind_data_schema.components.stimulus import OptoStimulation, PulseShape
from aind_data_schema.core.session import (
    DetectorConfig,
    FiberConnectionConfig,
    LightEmittingDiodeConfig,
    Session,
    StimulusEpoch,
    StimulusModality,
    Stream,
)
from aind_data_schema_models.modalities import Modality

from aind_metadata_mapper.core import GenericEtl
from aind_metadata_mapper.core_models import JobResponse
from aind_metadata_mapper.fip.models import JobSettings


@dataclass(frozen=True)
class ParsedMetadata:
    """Holds data from job settings and potentially external sources"""

    # Basic fields from job settings
    settings: JobSettings
    # Future: Add fields for data from external endpoints
    # previous_procedures: Optional[List[dict]] = None
    # etc...


class FIBEtl(GenericEtl[JobSettings]):
    """Generates fiber photometry session metadata"""

    def _extract(self) -> ParsedMetadata:
        """
        Extract metadata from job settings and potentially external sources
        Future: Add calls to external endpoints here
        """
        return ParsedMetadata(settings=self.job_settings)

    def _transform(self, extracted_source: ParsedMetadata) -> Session:
        """Transform extracted data into a valid Session object"""
        settings = extracted_source.settings

        # Create data streams from the provided data_streams list
        data_streams = [
            Stream(
                stream_start_time=stream.get(
                    "stream_start_time", settings.session_start_time
                ),
                stream_end_time=stream.get(
                    "stream_end_time", settings.session_end_time
                ),
                light_sources=[
                    LightEmittingDiodeConfig(**ls)
                    for ls in stream["light_sources"]
                ],
                stream_modalities=[Modality.FIB],
                detectors=[DetectorConfig(**d) for d in stream["detectors"]],
                fiber_connections=[
                    FiberConnectionConfig(**fc)
                    for fc in stream["fiber_connections"]
                ],
            )
            for stream in settings.data_streams
        ]

        # Create and return the session object
        return Session(
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
            # Other optional fields would be added here
        )

    def run_job(self) -> JobResponse:
        """Run the etl job and return a JobResponse."""
        extracted = self._extract()
        transformed = self._transform(extracted_source=extracted)
        job_response = self._load(
            transformed, self.job_settings.output_directory
        )
        return job_response


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    main_job_settings = JobSettings.from_args(sys_args)
    etl = FIBEtl(job_settings=main_job_settings)
    etl.run_job()
