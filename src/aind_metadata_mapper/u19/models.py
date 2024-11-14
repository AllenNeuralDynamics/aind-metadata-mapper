"""Defines Job Settings for U19 ETL"""

from pathlib import Path
from typing import List, Literal, Optional, Union

from pydantic import Field

from aind_metadata_mapper.core_models import BaseJobSettings


class JobSettings(BaseJobSettings):
    """Data that needs to be input by user."""

    job_settings_name: Literal["U19"] = "U19"
    tissue_sheet_path: Optional[Union[Path, str]] = Field(
        default=None, description=("Deprecated, use input_source instead.")
    )
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
