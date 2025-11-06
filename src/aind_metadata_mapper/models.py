"""Module to define models for Gather Metadata Job"""

from typing import List, Optional

from aind_data_schema_models.data_name_patterns import Group
from aind_data_schema_models.modalities import Modality
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings


class JobSettings(BaseSettings, cli_parse_args=True, cli_ignore_unknown_args=True):
    """Settings required to fetch metadata from metadata service"""

    # Path settings
    input_metadata_path: str = Field(
        ...,
        description=(
            "Location of metadata directory. If a file is not found in this "
            "directory, an attempt will be made to create it and save it here."
        ),
    )
    output_metadata_path: Optional[str] = Field(
        default=None,
        description=(
            "Location to save updated metadata. If the directory does not exist, "
            "it will be created. Defaults to input_metadata_path if not specified."
        ),
    )
    metadata_service_url: Optional[str] = Field(
        default="http://aind-metadata-service",
        description="Metadata service URL to download metadata info.",
    )

    # Job settings
    location: Optional[str] = Field(
        default=None,
        description=("Location to be set in the metadata. If None, location will not be set."),
    )
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
    subject_id: str = Field(
        default=...,
        description=("Subject ID. Will be used to download metadata from a service."),
    )
    project_name: str = Field(
        default=...,
        description=("Project Name. Will be used to download metadata from a service."),
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

    @model_validator(mode="after")
    def set_output_path_default(self):
        """Set output_metadata_path to input_metadata_path if not provided"""
        if self.output_metadata_path is None:
            self.output_metadata_path = self.input_metadata_path
        return self

