"""Module to define models for the metadata mapper"""

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class JobSettings(
    BaseSettings, cli_parse_args=True, cli_ignore_unknown_args=True
):
    """Settings required to fetch metadata from metadata service"""

    metadata_dir: str = Field(
        ...,
        description=(
            "Location of metadata directory. If a file is not found in this "
            "directory, an attempt will be made to create it and save it here."
        ),
    )
    subject_id: Optional[str] = Field(
        default=None,
        description=(
            "Subject ID. Will be used to download metadata from a service."
        ),
    )
    instrument_id: Optional[str] = Field(
        default=None,
        description=(
            "Instrument ID. Will be used to download metadata from a service."
        ),
    )
    project_name: Optional[str] = Field(
        default=None,
        description=(
            "Project Name. Will be used to download metadata from a service."
        ),
    )
    metadata_service_url: Optional[str] = Field(
        default="http://aind-metadata-service",
        description="Metadata service URL to download metadata info.",
    )


class Mapper:
    """Base class for all mappers"""

    def transform(self, metadata: dict):
        """Transforms raw metadata into a complete model."""
        raise NotImplementedError(
            "This method should be overridden in subclass"
        )
