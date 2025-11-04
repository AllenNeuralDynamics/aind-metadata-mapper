"""Module to define models for Gather Metadata Job"""

from typing import List, Optional

from aind_data_schema_models.data_name_patterns import Group
from aind_data_schema_models.modalities import Modality
from pydantic import Field
from pydantic_settings import BaseSettings


class JobSettings(BaseSettings, cli_parse_args=True, cli_ignore_unknown_args=True):
    """Settings required to fetch metadata from metadata service"""

    # Job settings
    metadata_dir: str = Field(
        ...,
        description=(
            "Location of metadata directory. If a file is not found in this "
            "directory, an attempt will be made to create it and save it here."
        ),
    )
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
    metadata_service_url: Optional[str] = Field(
        default="http://aind-metadata-service",
        description="Metadata service URL to download metadata info.",
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
