"""Sets up the U19 ingest ETL"""

import logging
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Union

from pydantic import Field
from pydantic_settings import BaseSettings
from aind_metadata_mapper.core import GenericEtl, JobResponse

import pandas as pd
import requests
import json

from aind_data_schema.core.procedures import Surgery, Procedures, Injection, ViralMaterial, TarsVirusIdentifiers, NonViralMaterial, NanojectInjection, Perfusion, Anaesthetic
from datetime import datetime
from aind_data_schema.core.procedures import VolumeUnit, SizeUnit
import glob
from enum import Enum
from decimal import Decimal



class JobSettings(BaseSettings):
    """Data that needs to be input by user."""

    data_path: Path
    output_directory: Optional[Path] = Field(
        default=None,
        description=(
            "Directory where to save the json file to. If None, then json"
            " contents will be returned in the Response message."
        ),
    )
    experimenter_full_name: List[str]
    subjects_to_ingest: List[str] = Field(
        default=None,
        description=(
            "List of subject IDs to ingest. If None,"
            " then all subjects in spreadsheet will be ingested."
        )
    )



DATETIME_FORMAT = "%H:%M:%S %d %b %Y"
LENGTH_FORMAT = "%Hh%Mm%Ss%fms"


class U19Etl(GenericEtl[JobSettings]):
    """Class for MRI ETL process."""

    def __init__(self, job_settings: Union[JobSettings, str]):
        """
        Class constructor for Base etl class.
        Parameters
        ----------
        job_settings: Union[JobSettings, str]
          Variables for a particular session
        """

        if isinstance(job_settings, str):
            job_settings_model = JobSettings.model_validate_json(job_settings)
        else:
            job_settings_model = job_settings
        super().__init__(job_settings=job_settings_model)

    def _extract(self) -> :
        """Extract the data from the bruker files."""


        return metadata

    def _transform(self, input_metadata: BrukerMetadata) -> Session:
        """Transform the data into the AIND data schema."""

        return self.load_mri_session(
            experimenter=self.job_settings.experimenter_full_name,
            primary_scan_number=self.job_settings.primary_scan_number,
            setup_scan_number=self.job_settings.setup_scan_number,
            scan_data=input_metadata.scan_data,
            subject_data=input_metadata.subject_data,
        )

    def run_job(self) -> JobResponse:
        """Run the job and return the response."""

        extracted = self._extract()
        transformed = self._transform(extracted)

        job_response = self._load(
            transformed, self.job_settings.output_directory
        )

        return job_response