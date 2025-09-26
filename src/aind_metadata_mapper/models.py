"""Module to define models for Gather Metadata Job"""

from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization
from pydantic import Field
from pydantic_settings import BaseSettings


class AcquisitionSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve acquisition metadata"""

    acquisition_path: str


class SubjectSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve subject metadata"""

    subject_id: str
    metadata_service_path: str = "api/v2/subject"


class ProceduresSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve procedures metadata"""

    subject_id: str
    metadata_service_path: str = "api/v2/procedures"


class DataDescriptionSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve data description metadata"""

    name: str
    project_name: str
    modality: List[Modality.ONE_OF]
    institution: Optional[Organization.ONE_OF] = Organization.AIND
    metadata_service_path_funding: str = "funding"


class InstrumentSettings(BaseSettings, extra="allow"):
    """Fields needed to retrieve instrument metadata"""

    instrument_id: str
    metadata_service_path: str = "instrument"


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
    processing_filepath: Optional[Path] = None
    acquisition_filepath: Optional[Path] = None
    instrument_filepath: Optional[Path] = None
    quality_control_filepath: Optional[Path] = None


class JobSettings(BaseSettings, extra="allow"):
    """Fields needed to gather all metadata"""

    job_settings_name: Literal["GatherMetadata"] = "GatherMetadata"
    metadata_service_url: Optional[str] = None
    subject_settings: Optional[SubjectSettings] = None
    acquisition_settings: Optional[AcquisitionSettings] = None
    data_description_settings: Optional[DataDescriptionSettings] = None
    procedures_settings: Optional[ProceduresSettings] = None
    instrument_settings: Optional[InstrumentSettings] = None
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
