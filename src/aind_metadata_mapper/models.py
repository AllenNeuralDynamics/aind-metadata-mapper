"""Module to define models for Gather Metadata Job"""

from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from pydantic import Field
from pydantic_settings import BaseSettings
from typing_extensions import Annotated

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
from aind_metadata_mapper.open_ephys.models import (
    JobSettings as OpenEphysJobSettings,
)
from aind_metadata_mapper.smartspim.models import (
    JobSettings as SmartSpimAcquisitionJobSettings,
)


class SessionSettings(BaseSettings, extra="allow"):
    """Settings needed to retrieve session metadata"""

    job_settings: Annotated[
        Union[
            BergamoSessionJobSettings,
            BrukerSessionJobSettings,
            FipSessionJobSettings,
            MesoscopeSessionJobSettings,
            OpenEphysJobSettings,
        ],
        Field(discriminator="job_settings_name"),
    ]


class AcquisitionSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve acquisition metadata"""

    # TODO: we can change this to a tagged union once more acquisition settings
    #  are added
    job_settings: SmartSpimAcquisitionJobSettings


class SubjectSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve subject metadata"""

    subject_id: str
    metadata_service_path: str = "subject"


class ProceduresSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve procedures metadata"""

    subject_id: str
    metadata_service_path: str = "procedures"


class RawDataDescriptionSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve data description metadata"""

    name: str
    project_name: str
    modality: List[Modality.ONE_OF]
    institution: Optional[Organization.ONE_OF] = Organization.AIND
    metadata_service_path: str = "funding"


class ProcessingSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve processing metadata"""

    pipeline_process: dict = Field(
        ...,
        description=(
            "Pipeline processes as a dict object. Will be converted to "
            "PipelineProcess model downstream."
        ),
    )


class MetadataSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve main Metadata"""

    name: str
    location: Optional[str] = Field(
        default=None,
        description=(
            "S3 location where data will be written to. "
            "This will override the location_map field."
        ),
    )
    location_map: Optional[Dict[str, str]] = Field(
        default=None, description="Maps metadata status to an s3 location."
    )
    subject_filepath: Optional[Path] = None
    data_description_filepath: Optional[Path] = None
    procedures_filepath: Optional[Path] = None
    session_filepath: Optional[Path] = None
    rig_filepath: Optional[Path] = None
    processing_filepath: Optional[Path] = None
    acquisition_filepath: Optional[Path] = None
    instrument_filepath: Optional[Path] = None
    quality_control_filepath: Optional[Path] = None


class JobSettings(BaseSettings, extra="allow"):
    """Fields needed to gather all metadata"""

    job_settings_name: Literal["GatherMetadata"] = "GatherMetadata"
    metadata_service_domain: Optional[str] = None
    subject_settings: Optional[SubjectSettings] = None
    session_settings: Optional[SessionSettings] = None
    acquisition_settings: Optional[AcquisitionSettings] = None
    raw_data_description_settings: Optional[RawDataDescriptionSettings] = None
    procedures_settings: Optional[ProceduresSettings] = None
    processing_settings: Optional[ProcessingSettings] = None
    metadata_settings: Optional[MetadataSettings] = None
    directory_to_write_to: Path
    metadata_dir: Optional[Union[Path, str]] = Field(
        default=None,
        description="Optional path where user defined metadata files might be",
    )
    metadata_dir_force: Optional[bool] = Field(
        default=None,
        description=(
            "Whether to override the user defined files in metadata_dir with "
            "those pulled from metadata service"
        ),
    )
