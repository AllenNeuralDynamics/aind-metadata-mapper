"""Defines Job Settings for U19 ETL"""

from typing import List, Literal

from pydantic import Field

from aind_metadata_mapper.core import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data that needs to be input by user."""

    job_settings_name: Literal["U19"] = "U19"
    tissue_sheet_names: List[str]
    experimenter_full_name: List[str]
    subject_to_ingest: str = Field(
        default=None,
        description=(
            "subject ID to ingest. If None,"
            " then all subjects in spreadsheet will be ingested."
        ),
    )
    procedures_download_link: str = Field(
        description="Link to download the relevant procedures "
        "from metadata service",
    )
    allow_validation_errors: bool = Field(
        False, description="Whether or not to allow validation errors."
    )
