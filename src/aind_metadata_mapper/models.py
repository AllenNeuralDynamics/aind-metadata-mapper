"""Module to define models for Gather Metadata Job"""

from pathlib import Path
from typing import List, Optional, Union
from typing_extensions import Annotated

from aind_data_schema.core.processing import PipelineProcess
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from pydantic import Field, BaseModel, ConfigDict
from pydantic_settings import BaseSettings

from aind_metadata_mapper.bergamo.models import (
    JobSettings as BergamoSessionJobSettings,
)
from aind_metadata_mapper.bruker.models import (
    JobSettings as BrukerSessionJobSettings,
)
from aind_metadata_mapper.fip.models import (
    JobSettings as FipSessionJobSettings,
)
from aind_metadata_mapper.mesoscope.models import (
    JobSettings as MesoscopeSessionJobSettings,
)
from aind_metadata_mapper.smartspim.models import (
    JobSettings as SmartSpimAcquisitionJobSettings,
)


class JobResponse(BaseModel):
    """Standard model of a JobResponse."""

    model_config = ConfigDict(extra="forbid")
    status_code: int
    message: Optional[str] = Field(None)
    data: Optional[str] = Field(None)


class SessionSettings(BaseSettings):
    """Settings needed to retrieve session metadata"""

    job_settings: Annotated[
        Union[
            BergamoSessionJobSettings,
            BrukerSessionJobSettings,
            FipSessionJobSettings,
            MesoscopeSessionJobSettings,
        ],
        Field(discriminator="job_settings_name"),
    ]


class AcquisitionSettings(BaseSettings):
    """Fields needed to retrieve acquisition metadata"""

    # TODO: we can change this to a tagged union once more acquisition settings
    #  are added
    job_settings: SmartSpimAcquisitionJobSettings


class SubjectSettings(BaseSettings):
    """Fields needed to retrieve subject metadata"""

    subject_id: str
    metadata_service_path: str = "subject"


class ProceduresSettings(BaseSettings):
    """Fields needed to retrieve procedures metadata"""

    subject_id: str
    metadata_service_path: str = "procedures"


class RawDataDescriptionSettings(BaseSettings):
    """Fields needed to retrieve data description metadata"""

    name: str
    project_name: str
    modality: List[Modality.ONE_OF]
    institution: Optional[Organization.ONE_OF] = Organization.AIND
    metadata_service_path: str = "funding"


class ProcessingSettings(BaseSettings):
    """Fields needed to retrieve processing metadata"""

    pipeline_process: PipelineProcess


class MetadataSettings(BaseSettings):
    """Fields needed to retrieve main Metadata"""

    name: str
    location: str
    subject_filepath: Optional[Path] = None
    data_description_filepath: Optional[Path] = None
    procedures_filepath: Optional[Path] = None
    session_filepath: Optional[Path] = None
    rig_filepath: Optional[Path] = None
    processing_filepath: Optional[Path] = None
    acquisition_filepath: Optional[Path] = None
    instrument_filepath: Optional[Path] = None


class JobSettings(BaseSettings):
    """Fields needed to gather all metadata"""

    metadata_service_domain: Optional[str] = None
    subject_settings: Optional[SubjectSettings] = None
    session_settings: Optional[SessionSettings] = None
    acquisition_settings: Optional[AcquisitionSettings] = None
    raw_data_description_settings: Optional[RawDataDescriptionSettings] = None
    procedures_settings: Optional[ProceduresSettings] = None
    processing_settings: Optional[ProcessingSettings] = None
    metadata_settings: Optional[MetadataSettings] = None
    directory_to_write_to: Path
    metadata_dir: Optional[Path] = Field(
        default=None,
        description="Optional path where user defined metadata files might be",
    )
    metadata_dir_force: bool = Field(
        default=False,
        description=(
            "Whether to override the user defined files in metadata_dir with "
            "those pulled from metadata service"
        ),
    )
