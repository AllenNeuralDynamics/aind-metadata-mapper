"""Module to define models for Gather Metadata Job"""

from typing import List, Optional

from aind_data_schema.base import AwareDatetimeWithDefault
from aind_data_schema_models.data_name_patterns import Group
from aind_data_schema_models.modalities import Modality
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class DataDescriptionSettings(BaseSettings):
    """Settings specific to data description metadata"""

    project_name: str = Field(
        default=...,
        description=("Project name. Will be used to download metadata from a service."),
    )
    modalities: List[Modality.ONE_OF] = Field(
        default=...,
        description=("List of data modalities for this dataset."),
    )
    tags: Optional[List[str]] = Field(
        default=None,
        description="Descriptive strings to help categorize and search for data.",
    )
    group: Optional[Group] = Field(
        default=None,
        description="A short name for the group of individuals that collected this data.",
    )
    restrictions: Optional[str] = Field(
        default=None,
        description="Detail any restrictions on publishing or sharing these data.",
    )
    data_summary: Optional[str] = Field(
        default=None,
        description="Semantic summary of experimental goal.",
    )

    @field_validator("modalities", mode="before")
    def convert_modalities_from_string(cls, v):
        """Convert modalities from string to list if necessary"""
        if isinstance(v, str):
            return [Modality.from_abbreviation(v)]
        elif isinstance(v, list):
            return [Modality.from_abbreviation(mod) if isinstance(mod, str) else mod for mod in v]
        return v


class InstrumentSettings(BaseSettings):
    """Settings specific to instrument metadata"""

    instrument_id: str = Field(
        ...,
        description="Identifier for the instrument used in data collection.",
    )


class JobSettings(BaseSettings, cli_parse_args=True, cli_ignore_unknown_args=True):
    """Settings required to fetch metadata from metadata service and construct the data_description"""

    # Path settings
    metadata_dir: Optional[str] = Field(
        default=None,
        description=(
            "Optional location of metadata files. If a file is not found in this "
            "directory, an attempt will be made to create it."
        ),
    )
    output_dir: str = Field(
        ...,
        description=("Location to save metadata."),
    )

    # Job settings
    raise_if_invalid: bool = Field(
        default=False,
        description=(
            "If True, GatherMetadataJob will raise an error if the fetched metadata is invalid. "
            "If False, log a warning and continue."
        ),
    )
    raise_if_mapper_errors: bool = Field(
        default=True,
        description=(
            "If True, GatherMetadataJob will raise an error if any automated mappers fail. "
            "If False, log a warning and continue."
        ),
    )

    # Metadata settings
    subject_id: Optional[str] = Field(
        default=None,
        description=(
            "Subject ID. If acquisition.json is present, this will be overriden by the value"
            " in acquisition.json. If raise_if_invalid is True, the subject_id in both locations must match."
            "The subject_id is used to download the subject and procedures from the metadata-service."
        ),
    )
    acquisition_start_time: Optional[AwareDatetimeWithDefault] = Field(
        default=None,
        description=(
            "Acquisition start time. If acquisition.json is present, this will be overridden by the value"
            " in acquisition.json. If raise_if_invalid is True, this time must match the start time provided"
            " by the acquisition.json."
        ),
    )

    # Core metadata settings
    data_description_settings: DataDescriptionSettings = Field(
        ...,
        description="Settings specific to data description metadata.",
    )
    instrument_settings: Optional[InstrumentSettings] = Field(
        default=None,
        description="Settings specific to instrument metadata.",
    )

    # Metadata service settings
    metadata_service_url: str = Field(
        default="http://aind-metadata-service",
        description="Metadata service URL to download metadata info.",
    )
    metadata_service_subject_endpoint: str = Field(
        default="/api/v2/subject/",
        description="Metadata service endpoint for subject metadata.",
    )
    metadata_service_procedures_endpoint: str = Field(
        default="/api/v2/procedures/",
        description="Metadata service endpoint for procedures metadata.",
    )
    metadata_service_instrument_endpoint: str = Field(
        default="/api/v2/instrument/",
        description="Metadata service endpoint for instrument metadata.",
    )
